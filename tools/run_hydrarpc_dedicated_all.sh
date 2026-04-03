#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_dedicated_all.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
                           Default: output/dedicated_all_<timestamp>
  --parallel-jobs <N>      Per-line gem5 parallelism. Default: 2
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --disk-image <path>      Disk image to reuse. Default: files/parsec.img
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected dedicated/coherent/app binaries.
  --continue-on-failure    Let sub-sweeps continue collecting failures.
  --skip-existing          Reuse completed outdirs when possible.
  --help                   Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
PARALLEL_JOBS=2
COUNT_PER_CLIENT=30
NUM_CPUS=34
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
CONTINUE_ON_FAILURE=0
SKIP_EXISTING=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
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
    --disk-image)
      DISK_IMAGE="$2"
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
    --continue-on-failure)
      CONTINUE_ON_FAILURE=1
      shift 1
      ;;
    --skip-existing)
      SKIP_EXISTING=1
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
  ROOT_OUTDIR="output/dedicated_all_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"

export DISK_IMAGE

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for dedicated_all" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-ruby-local.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  bash tools/setup_hydrarpc_dedicated_all_disk_image.sh "$DISK_IMAGE"
fi

motivation_args=(
  --root-outdir "$ROOT_OUTDIR/motivation"
  --cpu-type TIMING
  --boot-cpu KVM
  --num-cpus "$NUM_CPUS"
  --parallel-jobs "$PARALLEL_JOBS"
  --count-per-client "$COUNT_PER_CLIENT"
  --skip-build
  --skip-image-setup
)
if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
  motivation_args+=(--continue-on-failure)
fi
if [[ "$SKIP_EXISTING" -eq 1 ]]; then
  motivation_args+=(--skip-existing)
fi

for machine_index in 1 2 3; do
  bash tools/run_hydrarpc_dedicated_motivation_kvm3.sh \
    --machine-index "$machine_index" \
    "${motivation_args[@]}"
done

CKPT_CLASSIC="$ROOT_OUTDIR/motivation/checkpoints/classic_n${NUM_CPUS}/latest"
if [[ ! -d "$CKPT_CLASSIC" ]]; then
  echo "classic checkpoint not found after motivation run: $CKPT_CLASSIC" >&2
  exit 1
fi

COMMON_DEDICATED=(
  --kinds dedicated
  --count-per-client "$COUNT_PER_CLIENT"
  --slot-count 1024
  --cpu-type TIMING
  --boot-cpu KVM
  --num-cpus "$NUM_CPUS"
  --restore-checkpoint "$CKPT_CLASSIC"
  --skip-build
  --skip-image-setup
  --parallel-jobs "$PARALLEL_JOBS"
)
EXTRA_SWEEP_ARGS=()
EXTRA_APP_ARGS=()
OVERALL_CLIENT_COUNTS="1 2 4 8 16 32"
APP_PROFILES="ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_f_1k udb_ro"
OVERALL_REQ1530_RESP315_UNIFORM_ARGS=(
  --req-bytes 1530
  --resp-bytes 315
  --req-min-bytes 765
  --req-max-bytes 2295
  --resp-min-bytes 158
  --resp-max-bytes 472
)
OVERALL_REQ38_RESP230_UNIFORM_ARGS=(
  --req-bytes 38
  --resp-bytes 230
  --req-min-bytes 19
  --req-max-bytes 57
  --resp-min-bytes 115
  --resp-max-bytes 345
)
if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
  EXTRA_SWEEP_ARGS+=(--continue-on-failure)
  EXTRA_APP_ARGS+=(--continue-on-failure)
fi
if [[ "$SKIP_EXISTING" -eq 1 ]]; then
  EXTRA_SWEEP_ARGS+=(--skip-existing)
  EXTRA_APP_ARGS+=(--skip-existing)
fi

echo "ROOT=$ROOT_OUTDIR"
echo "DISK_IMAGE=$DISK_IMAGE"
echo "CKPT_CLASSIC=$CKPT_CLASSIC"

bash tools/run_hydrarpc_sweep.sh \
  --root-outdir "$ROOT_OUTDIR/overall/req64_resp64" \
  --client-counts "$OVERALL_CLIENT_COUNTS" \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode staging \
  --response-transfer-mode staging \
  "${COMMON_DEDICATED[@]}" \
  "${EXTRA_SWEEP_ARGS[@]}"

bash tools/run_hydrarpc_sweep.sh \
  --root-outdir "$ROOT_OUTDIR/overall/req1530_resp315_uniform" \
  --client-counts "$OVERALL_CLIENT_COUNTS" \
  "${OVERALL_REQ1530_RESP315_UNIFORM_ARGS[@]}" \
  --request-transfer-mode staging \
  --response-transfer-mode staging \
  "${COMMON_DEDICATED[@]}" \
  "${EXTRA_SWEEP_ARGS[@]}"

bash tools/run_hydrarpc_sweep.sh \
  --root-outdir "$ROOT_OUTDIR/overall/req38_resp230_uniform" \
  --client-counts "$OVERALL_CLIENT_COUNTS" \
  "${OVERALL_REQ38_RESP230_UNIFORM_ARGS[@]}" \
  --request-transfer-mode staging \
  --response-transfer-mode staging \
  "${COMMON_DEDICATED[@]}" \
  "${EXTRA_SWEEP_ARGS[@]}"

for req_bytes in 8 256 1024 4096 8192; do
  bash tools/run_hydrarpc_sweep.sh \
    --root-outdir "$ROOT_OUTDIR/sensitivity/reqsize_req${req_bytes}_resp64" \
    --client-counts "32" \
    --req-bytes "$req_bytes" \
    --resp-bytes 64 \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    "${COMMON_DEDICATED[@]}" \
    "${EXTRA_SWEEP_ARGS[@]}"
done

for resp_bytes in 8 256 1024 4096 8192; do
  bash tools/run_hydrarpc_sweep.sh \
    --root-outdir "$ROOT_OUTDIR/sensitivity/respsize_req64_resp${resp_bytes}" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes "$resp_bytes" \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    "${COMMON_DEDICATED[@]}" \
    "${EXTRA_SWEEP_ARGS[@]}"
done

for ring_size in 16 32 64 128 256 512; do
  bash tools/run_hydrarpc_sweep.sh \
    --root-outdir "$ROOT_OUTDIR/sensitivity/ringsize_s${ring_size}" \
    --client-counts "32" \
    --slot-count "$ring_size" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    --kinds dedicated \
    --count-per-client "$COUNT_PER_CLIENT" \
    --cpu-type TIMING \
    --boot-cpu KVM \
    --num-cpus "$NUM_CPUS" \
    --restore-checkpoint "$CKPT_CLASSIC" \
    --skip-build \
    --skip-image-setup \
    --parallel-jobs "$PARALLEL_JOBS" \
    "${EXTRA_SWEEP_ARGS[@]}"
done

for slow_count_per_client in 8 15; do
  for slow_client_count in 4 8 16 20 24 28; do
    bash tools/run_hydrarpc_sweep.sh \
      --root-outdir "$ROOT_OUTDIR/sensitivity/sparse32_sc${slow_client_count}_sq${slow_count_per_client}" \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --slow-client-count "$slow_client_count" \
      --slow-count-per-client "$slow_count_per_client" \
      --slow-send-gap-ns 20000 \
      --request-transfer-mode staging \
      --response-transfer-mode staging \
      "${COMMON_DEDICATED[@]}" \
      "${EXTRA_SWEEP_ARGS[@]}"
  done
done

for latency in 100ns 200ns 300ns; do
  bash tools/run_hydrarpc_sweep.sh \
    --root-outdir "$ROOT_OUTDIR/sensitivity/cxl_latency_${latency}" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --request-transfer-mode staging \
    --response-transfer-mode staging \
    --cxl-bridge-extra-latency "$latency" \
    "${COMMON_DEDICATED[@]}" \
    "${EXTRA_SWEEP_ARGS[@]}"
done

bash tools/run_hydrarpc_app_sweep.sh \
  --root-outdir "$ROOT_OUTDIR/application" \
  --client-counts "32" \
  --profiles "$APP_PROFILES" \
  --kinds dedicated \
  --count-per-client "$COUNT_PER_CLIENT" \
  --cpu-type TIMING \
  --boot-cpu KVM \
  --num-cpus "$NUM_CPUS" \
  --restore-checkpoint "$CKPT_CLASSIC" \
  --skip-build \
  --skip-image-setup \
  --parallel-jobs "$PARALLEL_JOBS" \
  "${EXTRA_APP_ARGS[@]}"

echo "All dedicated submissions finished."
echo "ROOT=$ROOT_OUTDIR"
