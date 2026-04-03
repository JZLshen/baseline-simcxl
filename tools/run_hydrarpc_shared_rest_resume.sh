#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_shared_rest_resume.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
                           Default: output/shared_rest_<timestamp>
  --checkpoint <dir>       Existing classic checkpoint directory. Required.
  --parallel-jobs <N>      Max concurrent gem5 jobs across shared phases.
                           Default: 4
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected shared/shared-app binaries.
  --skip-existing          Reuse completed outdirs when possible.
  --continue-on-failure    Record failures and continue remaining work.
  --help                   Show this message.

Supported exp.txt-aligned groups:
  - motivation shared competition
  - overall
  - req-size
  - resp-size
  - ring-size
  - sparse32
  - cxl-latency
  - application

Not included here:
  - sync-head sensitivity
  - DMA lane sensitivity
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
CHECKPOINT_DIR=""
PARALLEL_JOBS=4
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

if [[ -z "$CHECKPOINT_DIR" ]]; then
  echo "--checkpoint is required" >&2
  exit 1
fi
if [[ ! -d "$CHECKPOINT_DIR" ]]; then
  echo "checkpoint not found: $CHECKPOINT_DIR" >&2
  exit 1
fi
if [[ "$PARALLEL_JOBS" -le 0 ]]; then
  echo "--parallel-jobs must be positive" >&2
  exit 1
fi
if [[ "$COUNT_PER_CLIENT" -le 0 ]]; then
  echo "--count-per-client must be positive" >&2
  exit 1
fi
if [[ "$NUM_CPUS" -le 0 ]]; then
  echo "--num-cpus must be positive" >&2
  exit 1
fi
CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/shared_rest_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for shared rest resume" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
  bash tools/setup_hydrarpc_shared_app_disk_image.sh files/parsec.img
fi

COMMON_SHARED=(
  --kinds shared
  --count-per-client "$COUNT_PER_CLIENT"
  --slot-count 1024
  --cpu-type TIMING
  --boot-cpu KVM
  --num-cpus "$NUM_CPUS"
  --restore-checkpoint "$CHECKPOINT_DIR"
  --skip-build
  --skip-image-setup
)
EXTRA_SWEEP_ARGS=()
EXTRA_APP_ARGS=()
if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
  EXTRA_SWEEP_ARGS+=(--continue-on-failure)
  EXTRA_APP_ARGS+=(--continue-on-failure)
fi
if [[ "$SKIP_EXISTING" -eq 1 ]]; then
  EXTRA_SWEEP_ARGS+=(--skip-existing)
  EXTRA_APP_ARGS+=(--skip-existing)
fi

echo "ROOT=$ROOT_OUTDIR"
echo "CHECKPOINT=$CHECKPOINT_DIR"

wait_for_background_or_fail() {
  local wait_rc=0

  set +e
  wait -n
  wait_rc=$?
  set -e

  if [[ "$wait_rc" -ne 0 ]]; then
    ANY_FAILURES=1
    if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
      echo "CONTINUE-FAIL background group rc=${wait_rc}" >&2
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

wait_for_onepoint_cap() {
  local cap="$1"
  while [[ "$(jobs -pr | wc -l)" -ge "$cap" ]]; do
    wait_for_background_or_fail
  done
}

run_shared_sweep() {
  local per_jobs="$1"
  local root_outdir="$2"
  shift 2

  bash tools/run_hydrarpc_sweep.sh \
    --root-outdir "$root_outdir" \
    --parallel-jobs "$per_jobs" \
    "${COMMON_SHARED[@]}" \
    "$@" \
    "${EXTRA_SWEEP_ARGS[@]}"
}

run_shared_app() {
  local per_jobs="$1"

  bash tools/run_hydrarpc_app_sweep.sh \
    --root-outdir "$ROOT_OUTDIR/application" \
    --client-counts "32" \
    --profiles "ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_f_1k udb_ro" \
    --kinds shared \
    --count-per-client "$COUNT_PER_CLIENT" \
    --cpu-type TIMING \
    --boot-cpu KVM \
    --num-cpus "$NUM_CPUS" \
    --restore-checkpoint "$CHECKPOINT_DIR" \
    --skip-build \
    --skip-image-setup \
    --parallel-jobs "$per_jobs" \
    "${EXTRA_APP_ARGS[@]}"
}

run_motivation_phase() {
  run_shared_sweep "$PARALLEL_JOBS" \
    "$ROOT_OUTDIR/motivation/moti_shared_competition_req64_resp64_pow2" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64
}

run_overall_phase() {
  local per_sweep_jobs=1

  if [[ "$PARALLEL_JOBS" -ge 3 ]]; then
    per_sweep_jobs=$((PARALLEL_JOBS / 3))
    if [[ "$per_sweep_jobs" -lt 1 ]]; then
      per_sweep_jobs=1
    fi

    run_shared_sweep "$per_sweep_jobs" \
      "$ROOT_OUTDIR/overall/req64_resp64" \
      --client-counts "1 2 4 8 16 32" \
      --req-bytes 64 \
      --resp-bytes 64 &
    run_shared_sweep "$per_sweep_jobs" \
      "$ROOT_OUTDIR/overall/req1530_resp315_uniform" \
      --client-counts "1 2 4 8 16 32" \
      --req-bytes 1530 \
      --resp-bytes 315 \
      --req-min-bytes 765 \
      --req-max-bytes 2295 \
      --resp-min-bytes 158 \
      --resp-max-bytes 472 &
    run_shared_sweep "$per_sweep_jobs" \
      "$ROOT_OUTDIR/overall/req38_resp230_uniform" \
      --client-counts "1 2 4 8 16 32" \
      --req-bytes 38 \
      --resp-bytes 230 \
      --req-min-bytes 19 \
      --req-max-bytes 57 \
      --resp-min-bytes 115 \
      --resp-max-bytes 345 &
    wait_for_all_background
    return 0
  fi

  run_shared_sweep "$PARALLEL_JOBS" \
    "$ROOT_OUTDIR/overall/req64_resp64" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64
  run_shared_sweep "$PARALLEL_JOBS" \
    "$ROOT_OUTDIR/overall/req1530_resp315_uniform" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 1530 \
    --resp-bytes 315 \
    --req-min-bytes 765 \
    --req-max-bytes 2295 \
    --resp-min-bytes 158 \
    --resp-max-bytes 472
  run_shared_sweep "$PARALLEL_JOBS" \
    "$ROOT_OUTDIR/overall/req38_resp230_uniform" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 38 \
    --resp-bytes 230 \
    --req-min-bytes 19 \
    --req-max-bytes 57 \
    --resp-min-bytes 115 \
    --resp-max-bytes 345
}

run_reqsize_point() {
  local req_bytes="$1"
  run_shared_sweep 1 \
    "$ROOT_OUTDIR/sensitivity/reqsize_req${req_bytes}_resp64" \
    --client-counts "32" \
    --req-bytes "$req_bytes" \
    --resp-bytes 64
}

run_respsize_point() {
  local resp_bytes="$1"
  run_shared_sweep 1 \
    "$ROOT_OUTDIR/sensitivity/respsize_req64_resp${resp_bytes}" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes "$resp_bytes"
}

run_latency_point() {
  local latency="$1"
  run_shared_sweep 1 \
    "$ROOT_OUTDIR/sensitivity/cxl_latency_${latency}" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --cxl-bridge-extra-latency "$latency"
}

run_sparse_point() {
  local slow_client_count="$1"
  local slow_count_per_client="$2"

  run_shared_sweep 1 \
    "$ROOT_OUTDIR/sensitivity/sparse32_sc${slow_client_count}_sq${slow_count_per_client}" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$slow_count_per_client" \
    --slow-send-gap-ns 20000
}

run_ring_sweep() {
  local per_jobs="$1"
  local ring_size=""

  for ring_size in 16 32 64 128 256 512 1024; do
    run_shared_sweep "$per_jobs" \
      "$ROOT_OUTDIR/sensitivity/ringsize_s${ring_size}" \
      --client-counts "32" \
      --slot-count "$ring_size" \
      --req-bytes 64 \
      --resp-bytes 64
  done
}

run_size_points_phase() {
  local point_cap="$1"
  local req_bytes=""
  local resp_bytes=""

  for req_bytes in 8 64 256 1024 4096 8192; do
    run_reqsize_point "$req_bytes" &
    wait_for_onepoint_cap "$point_cap"
  done
  for resp_bytes in 8 64 256 1024 4096 8192; do
    run_respsize_point "$resp_bytes" &
    wait_for_onepoint_cap "$point_cap"
  done
  wait_for_all_background
}

run_latency_points_phase() {
  local point_cap="$1"
  local latency=""

  for latency in 100ns 200ns 300ns; do
    run_latency_point "$latency" &
    wait_for_onepoint_cap "$point_cap"
  done
  wait_for_all_background
}

run_sparse_points_phase() {
  local point_cap="$1"
  local slow_count_per_client=""
  local slow_client_count=""

  for slow_count_per_client in 8 15 30; do
    for slow_client_count in 4 8 16 20 24 28; do
      run_sparse_point "$slow_client_count" "$slow_count_per_client" &
      wait_for_onepoint_cap "$point_cap"
    done
  done
  wait_for_all_background
}

run_size_and_ring_phase() {
  local ring_jobs=0
  local size_cap="$PARALLEL_JOBS"

  if [[ "$PARALLEL_JOBS" -gt 10 ]]; then
    size_cap=10
    ring_jobs=$((PARALLEL_JOBS - size_cap))
    if [[ "$ring_jobs" -gt 7 ]]; then
      ring_jobs=7
    fi
  fi

  if [[ "$ring_jobs" -gt 0 ]]; then
    run_ring_sweep "$ring_jobs" &
  fi
  run_size_points_phase "$size_cap"
  wait_for_all_background

  if [[ "$ring_jobs" -eq 0 ]]; then
    run_ring_sweep "$PARALLEL_JOBS"
  fi
}

run_sparse_latency_app_phase() {
  local app_jobs=0
  local latency_cap=0
  local sparse_cap=0

  if [[ "$PARALLEL_JOBS" -ge 5 ]]; then
    app_jobs=5
  else
    app_jobs="$PARALLEL_JOBS"
  fi

  if [[ "$PARALLEL_JOBS" -gt "$app_jobs" ]]; then
    latency_cap=$((PARALLEL_JOBS - app_jobs))
    if [[ "$latency_cap" -gt 3 ]]; then
      latency_cap=3
    fi
  fi

  if [[ "$PARALLEL_JOBS" -gt $((app_jobs + latency_cap)) ]]; then
    sparse_cap=$((PARALLEL_JOBS - app_jobs - latency_cap))
  fi

  if [[ "$app_jobs" -gt 0 ]]; then
    run_shared_app "$app_jobs" &
  fi
  if [[ "$latency_cap" -gt 0 ]]; then
    run_latency_points_phase "$latency_cap" &
  fi
  if [[ "$sparse_cap" -gt 0 ]]; then
    run_sparse_points_phase "$sparse_cap" &
  else
    run_sparse_points_phase 1 &
  fi
  wait_for_all_background
}

run_motivation_phase
run_overall_phase
run_size_and_ring_phase
run_sparse_latency_app_phase

if [[ "$ANY_FAILURES" -ne 0 ]]; then
  echo "Shared rest resume complete with failures."
else
  echo "Shared rest resume complete."
fi
echo "ROOT=$ROOT_OUTDIR"
