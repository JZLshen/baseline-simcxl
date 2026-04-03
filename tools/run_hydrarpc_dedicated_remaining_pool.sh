#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_dedicated_remaining_pool.sh [options]

Options:
  --root-outdir <dir>      Root output directory. Required.
  --checkpoint <dir>       Existing classic checkpoint directory. Required.
  --parallel-jobs <N>      Max concurrent gem5 jobs in this pool. Default: 15
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected dedicated guest binaries.
  --skip-existing          Reuse completed outdirs when possible.
  --continue-on-failure    Log a failed point and continue the remaining queue.
  --help                   Show this message.

This launcher only covers the remaining dedicated single-point work items:
  - sensitivity/respsize_req64_resp8192
  - sensitivity/ringsize_s{16,32,64,128,256,512}
  - sensitivity/sparse32_*
  - sensitivity/cxl_latency_*
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
CHECKPOINT_DIR=""
PARALLEL_JOBS=15
COUNT_PER_CLIENT=30
NUM_CPUS=34
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
CONTINUE_ON_FAILURE=0
ANY_FAILURES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
      shift 2
      ;;
    --checkpoint)
      CHECKPOINT_DIR="$2"
      shift 2
      ;;
    --parallel-jobs)
      PARALLEL_JOBS="$2"
      shift 2
      ;;
    --count-per-client)
      COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift 1
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift 1
      ;;
    --skip-existing)
      SKIP_EXISTING=1
      shift 1
      ;;
    --continue-on-failure)
      CONTINUE_ON_FAILURE=1
      shift 1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ROOT_OUTDIR" ]]; then
  echo "--root-outdir is required" >&2
  exit 1
fi
if [[ -z "$CHECKPOINT_DIR" ]]; then
  echo "--checkpoint is required" >&2
  exit 1
fi
if [[ ! -d "$CHECKPOINT_DIR" ]]; then
  echo "checkpoint not found: $CHECKPOINT_DIR" >&2
  exit 1
fi
if [[ "$PARALLEL_JOBS" -lt 1 ]]; then
  echo "--parallel-jobs must be >= 1" >&2
  exit 1
fi

ROOT_OUTDIR="$(mkdir -p "$ROOT_OUTDIR" && cd "$ROOT_OUTDIR" && pwd)"
CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"
POOL_LOG="$ROOT_OUTDIR/remaining_pool.log"
touch "$POOL_LOG"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for dedicated remaining pool" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
fi

COMMON_DEDICATED=(
  --kinds dedicated
  --count-per-client "$COUNT_PER_CLIENT"
  --slot-count 1024
  --cpu-type TIMING
  --boot-cpu KVM
  --num-cpus "$NUM_CPUS"
  --restore-checkpoint "$CHECKPOINT_DIR"
  --skip-build
  --skip-image-setup
)
if [[ "$SKIP_EXISTING" -eq 1 ]]; then
  COMMON_DEDICATED+=(--skip-existing)
fi

log_msg() {
  local msg="$1"
  printf '[%s] %s\n' "$(date '+%F %T')" "$msg" | tee -a "$POOL_LOG"
}

wait_for_background_or_fail() {
  local wait_rc=0

  set +e
  wait -n
  wait_rc=$?
  set -e

  if [[ "$wait_rc" -ne 0 ]]; then
    ANY_FAILURES=1
    if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
      log_msg "CONTINUE-FAIL remaining pool job rc=${wait_rc}; continuing queue"
      return 0
    fi
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    exit "$wait_rc"
  fi
}

wait_for_all_background() {
  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
}

wait_for_slot() {
  while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
    wait_for_background_or_fail
  done
}

launch_task() {
  local label="$1"
  shift

  (
    log_msg "TASK-START ${label}"
    set +e
    "$@" >>"$POOL_LOG" 2>&1
    local rc=$?
    set -e
    log_msg "TASK-END ${label} rc=${rc}"
    exit "$rc"
  ) &
  wait_for_slot
}

launch_respsize_8192() {
  launch_task \
    "respsize_req64_resp8192" \
    bash tools/run_hydrarpc_sweep.sh \
      --root-outdir "$ROOT_OUTDIR/sensitivity/respsize_req64_resp8192" \
      --parallel-jobs 1 \
      "${COMMON_DEDICATED[@]}" \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes 8192 \
      --request-transfer-mode staging \
      --response-transfer-mode staging
}

launch_ring_point() {
  local ring_size="$1"

  launch_task \
    "ringsize_s${ring_size}" \
    bash tools/run_hydrarpc_sweep.sh \
      --root-outdir "$ROOT_OUTDIR/sensitivity/ringsize_s${ring_size}" \
      --parallel-jobs 1 \
      "${COMMON_DEDICATED[@]}" \
      --client-counts "32" \
      --slot-count "$ring_size" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --request-transfer-mode staging \
      --response-transfer-mode staging
}

launch_sparse_point() {
  local slow_client_count="$1"
  local slow_count_per_client="$2"

  launch_task \
    "sparse32_sc${slow_client_count}_sq${slow_count_per_client}" \
    bash tools/run_hydrarpc_sweep.sh \
      --root-outdir "$ROOT_OUTDIR/sensitivity/sparse32_sc${slow_client_count}_sq${slow_count_per_client}" \
      --parallel-jobs 1 \
      "${COMMON_DEDICATED[@]}" \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --slow-client-count "$slow_client_count" \
      --slow-count-per-client "$slow_count_per_client" \
      --slow-send-gap-ns 20000 \
      --request-transfer-mode staging \
      --response-transfer-mode staging
}

launch_latency_point() {
  local latency="$1"

  launch_task \
    "cxl_latency_${latency}" \
    bash tools/run_hydrarpc_sweep.sh \
      --root-outdir "$ROOT_OUTDIR/sensitivity/cxl_latency_${latency}" \
      --parallel-jobs 1 \
      "${COMMON_DEDICATED[@]}" \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --request-transfer-mode staging \
      --response-transfer-mode staging \
      --cxl-bridge-extra-latency "$latency"
}

log_msg "ROOT=${ROOT_OUTDIR}"
log_msg "CHECKPOINT=${CHECKPOINT_DIR}"
log_msg "PARALLEL_JOBS=${PARALLEL_JOBS}"

launch_respsize_8192
for ring_size in 16 32 64 128 256 512; do
  launch_ring_point "$ring_size"
done
for latency in 100ns 200ns 300ns; do
  launch_latency_point "$latency"
done
for slow_count_per_client in 8 15; do
  for slow_client_count in 4 8 16 20 24 28; do
    launch_sparse_point "$slow_client_count" "$slow_count_per_client"
  done
done

wait_for_all_background

if [[ "$ANY_FAILURES" -ne 0 ]]; then
  log_msg "Dedicated remaining pool complete with failures."
else
  log_msg "Dedicated remaining pool complete."
fi
