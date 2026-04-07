#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_dedicated_flat_queue.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
                           Default: output/dedicated_flat_queue_<timestamp>
  --checkpoint <dir>       Existing classic checkpoint directory to reuse.
  --checkpoint-root <dir>  Root directory used to create/reuse one classic
                           checkpoint when --checkpoint is not given.
                           Default: <root-outdir>/checkpoints
  --refresh-checkpoint     Recreate the managed classic checkpoint bundle.
  --max-procs <N>          Global concurrent gem5 processes. Default: 17
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --disk-image <path>      Disk image to reuse. Default: files/parsec.img
  --guest-cflags <flags>   Host gcc flags used for injected guest binaries.
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected dedicated/app binaries.
  --skip-existing          Reuse completed outdirs when possible.
  --continue-on-failure    Record failed runs and continue remaining work.
  --help                   Show this message.

Current dedicated-only deduped run set:
  - overall: 18
  - sensitivity: 37
    req-size 5, resp-size 5, ring-size 6, sparse32 18, cxl-latency 3
  - application: 5

Physical runs launched by this queue runner: 60
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
CHECKPOINT_DIR=""
CHECKPOINT_ROOT=""
REFRESH_CHECKPOINT=0
MAX_PROCS=17
COUNT_PER_CLIENT=30
NUM_CPUS=34
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
GUEST_CFLAGS=""
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
CONTINUE_ON_FAILURE=0
ANY_FAILURES=0
SUBMITTED_RUNS=0

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
    --checkpoint-root)
      CHECKPOINT_ROOT="$2"
      shift 2
      ;;
    --refresh-checkpoint)
      REFRESH_CHECKPOINT=1
      shift 1
      ;;
    --max-procs)
      MAX_PROCS="$2"
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
    --disk-image)
      DISK_IMAGE="$2"
      shift 2
      ;;
    --guest-cflags)
      GUEST_CFLAGS="$2"
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

if [[ "$MAX_PROCS" -lt 1 ]]; then
  echo "--max-procs must be >= 1" >&2
  exit 1
fi

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/dedicated_flat_queue_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"

if [[ -z "$CHECKPOINT_ROOT" ]]; then
  CHECKPOINT_ROOT="$ROOT_OUTDIR/checkpoints"
fi
mkdir -p "$CHECKPOINT_ROOT"
CHECKPOINT_ROOT="$(cd "$CHECKPOINT_ROOT" && pwd)"

RUN_LOG="$ROOT_OUTDIR/run.log"
touch "$RUN_LOG"

run_cmd() {
  printf '[%s] ' "$(date '+%F %T')" | tee -a "$RUN_LOG"
  printf '%q ' "$@" | tee -a "$RUN_LOG"
  printf '\n' | tee -a "$RUN_LOG"
  "$@"
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
      echo "[$(date '+%F %T')] CONTINUE-FAIL flat queue background rc=${wait_rc}" \
        | tee -a "$RUN_LOG"
      return 0
    fi
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    echo "[$(date '+%F %T')] FAIL-FAST aborting flat queue after background failure" \
      | tee -a "$RUN_LOG"
    exit "$wait_rc"
  fi
}

wait_for_cap() {
  local cap="$1"
  while [[ "$(jobs -pr | wc -l)" -ge "$cap" ]]; do
    wait_for_background_or_fail
  done
}

wait_for_all_background() {
  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
}

ensure_build() {
  if [[ "$SKIP_BUILD" -eq 0 ]]; then
    run_cmd scons build/X86/gem5.opt -j"$(nproc)"
    return 0
  fi

  run_cmd bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for dedicated flat queue" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
}

ensure_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi

  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_noncc_all_disk_image.sh "$DISK_IMAGE"
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_noncc_all_disk_image.sh "$DISK_IMAGE"
  fi
}

ensure_checkpoint() {
  local managed_outdir="$CHECKPOINT_ROOT/classic_n${NUM_CPUS}"

  if [[ -n "$CHECKPOINT_DIR" ]]; then
    if [[ ! -d "$CHECKPOINT_DIR" ]]; then
      echo "checkpoint not found: $CHECKPOINT_DIR" >&2
      exit 1
    fi
    CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"
    return 0
  fi

  if [[ "$REFRESH_CHECKPOINT" -eq 1 ]]; then
    rm -rf "$managed_outdir"
  fi

  run_cmd bash tools/create_hydrarpc_boot_checkpoint.sh \
    --mode classic \
    --outdir "$managed_outdir" \
    --binary build/X86/gem5.opt \
    --boot-cpu KVM \
    --num-cpus "$NUM_CPUS" \
    --disk-image "$DISK_IMAGE" \
    --skip-build

  CHECKPOINT_DIR="$managed_outdir/latest"
  CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"
}

submit() {
  local label="$1"
  shift

  printf '[%s] submit %s\n' "$(date '+%F %T')" "$label" | tee -a "$RUN_LOG"
  "$@" &
  SUBMITTED_RUNS=$((SUBMITTED_RUNS + 1))
  wait_for_cap "$MAX_PROCS"
}

common_sweep_args() {
  local -n out_ref=$1

  out_ref+=(
    --kinds dedicated
    --client-counts "32"
    --count-per-client "$COUNT_PER_CLIENT"
    --slot-count 1024
    --cpu-type TIMING
    --boot-cpu KVM
    --num-cpus "$NUM_CPUS"
    --restore-checkpoint "$CHECKPOINT_DIR"
    --request-transfer-mode staging
    --response-transfer-mode staging
    --skip-build
    --skip-image-setup
    --parallel-jobs 1
  )

  if [[ -n "$GUEST_CFLAGS" ]]; then
    out_ref+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    out_ref+=(--skip-existing)
  fi
  if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
    out_ref+=(--continue-on-failure)
  fi
}

submit_sweep_point() {
  local label="$1"
  local outdir="$2"
  shift 2
  local args=(bash tools/run_hydrarpc_sweep.sh --root-outdir "$outdir")

  common_sweep_args args
  args+=("$@")
  submit "$label" "${args[@]}"
}

submit_overall_point() {
  local load_name="$1"
  local client_count="$2"
  shift 2

  submit_sweep_point \
    "overall/${load_name}/c${client_count}" \
    "$ROOT_OUTDIR/overall/${load_name}/c${client_count}" \
    --client-counts "$client_count" \
    "$@"
}

submit_app_point() {
  local profile="$1"
  local args=(bash tools/run_hydrarpc_app_sweep.sh
    --root-outdir "$ROOT_OUTDIR/application/${profile}"
    --client-counts "32"
    --profiles "$profile"
    --kinds dedicated
    --count-per-client "$COUNT_PER_CLIENT"
    --cpu-type TIMING
    --boot-cpu KVM
    --num-cpus "$NUM_CPUS"
    --restore-checkpoint "$CHECKPOINT_DIR"
    --skip-build
    --skip-image-setup
    --parallel-jobs 1
  )

  if [[ -n "$GUEST_CFLAGS" ]]; then
    args+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    args+=(--skip-existing)
  fi
  if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
    args+=(--continue-on-failure)
  fi

  submit "application/${profile}" "${args[@]}"
}

ensure_build
ensure_image
ensure_checkpoint

for client_count in 1 2 4 8 16 32; do
  submit_overall_point \
    req64_resp64 \
    "$client_count" \
    --req-bytes 64 \
    --resp-bytes 64
  submit_overall_point \
    req1530_resp315_uniform \
    "$client_count" \
    --req-bytes 1530 \
    --resp-bytes 315 \
    --req-min-bytes 765 \
    --req-max-bytes 2295 \
    --resp-min-bytes 158 \
    --resp-max-bytes 472
  submit_overall_point \
    req38_resp230_uniform \
    "$client_count" \
    --req-bytes 38 \
    --resp-bytes 230 \
    --req-min-bytes 19 \
    --req-max-bytes 57 \
    --resp-min-bytes 115 \
    --resp-max-bytes 345
done

for req_bytes in 8 256 1024 4096 8192; do
  submit_sweep_point \
    "sensitivity/reqsize/req${req_bytes}" \
    "$ROOT_OUTDIR/sensitivity/reqsize/req${req_bytes}" \
    --req-bytes "$req_bytes" \
    --resp-bytes 64
done

for resp_bytes in 8 256 1024 4096 8192; do
  submit_sweep_point \
    "sensitivity/respsize/resp${resp_bytes}" \
    "$ROOT_OUTDIR/sensitivity/respsize/resp${resp_bytes}" \
    --req-bytes 64 \
    --resp-bytes "$resp_bytes"
done

for ring_size in 16 32 64 128 256 512; do
  submit_sweep_point \
    "sensitivity/ringsize/s${ring_size}" \
    "$ROOT_OUTDIR/sensitivity/ringsize/s${ring_size}" \
    --slot-count "$ring_size" \
    --req-bytes 64 \
    --resp-bytes 64
done

for slow_count_per_client in 8 15; do
  for slow_client_count in 4 8 16 20 24 28; do
    submit_sweep_point \
      "sensitivity/sparse32/sc${slow_client_count}_sq${slow_count_per_client}" \
      "$ROOT_OUTDIR/sensitivity/sparse32/sc${slow_client_count}_sq${slow_count_per_client}" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --slow-client-count "$slow_client_count" \
      --slow-count-per-client "$slow_count_per_client" \
      --slow-send-gap-ns 20000
  done
done

for latency in 100ns 200ns 300ns; do
  submit_sweep_point \
    "sensitivity/cxl_latency/${latency}" \
    "$ROOT_OUTDIR/sensitivity/cxl_latency/${latency}" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --cxl-bridge-extra-latency "$latency"
done

for profile in ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_d_1k udb_a udb_b udb_c udb_d; do
  submit_app_point "$profile"
done

wait_for_all_background

echo
echo "checkpoint=$CHECKPOINT_DIR"
echo "root_outdir=$ROOT_OUTDIR"
echo "submitted_runs=$SUBMITTED_RUNS"
if [[ "$ANY_FAILURES" -ne 0 ]]; then
  echo "Dedicated flat queue completed with failures."
else
  echo "Dedicated flat queue completed."
fi
