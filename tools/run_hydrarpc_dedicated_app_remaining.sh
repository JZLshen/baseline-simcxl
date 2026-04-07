#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_dedicated_app_remaining.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
                           Default: output/dedicated_app_remaining_<timestamp>
  --checkpoint <dir>       Existing classic checkpoint directory. Required.
  --parallel-jobs <N>      Concurrent application gem5 jobs. Default: 5
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --disk-image <path>      Disk image to reuse. Default: repo-local files/parsec.img
  --guest-cflags <flags>   Host gcc flags used for injected guest binaries.
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected app guest binaries.
  --skip-existing          Reuse completed outdirs when possible.
  --continue-on-failure    Record failed runs and continue the sweep.
  --help                   Show this message.

This launcher covers the current dedicated application profiles:
  - ycsb_a_1k
  - ycsb_b_1k
  - ycsb_c_1k
  - ycsb_d_1k
  - udb_a
  - udb_b
  - udb_c
  - udb_d
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR=""
CHECKPOINT_DIR=""
PARALLEL_JOBS=5
COUNT_PER_CLIENT=30
NUM_CPUS=34
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
GUEST_CFLAGS=""
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
CONTINUE_ON_FAILURE=0

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
      shift
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift
      ;;
    --skip-existing)
      SKIP_EXISTING=1
      shift
      ;;
    --continue-on-failure)
      CONTINUE_ON_FAILURE=1
      shift
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
if [[ "$PARALLEL_JOBS" -lt 1 ]]; then
  echo "--parallel-jobs must be >= 1" >&2
  exit 1
fi

CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"
if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/dedicated_app_remaining_$(date +%Y%m%d_%H%M%S)"
fi
ROOT_OUTDIR="$(mkdir -p "$ROOT_OUTDIR" && cd "$ROOT_OUTDIR" && pwd)"
DISK_IMAGE="$(cd "$(dirname "$DISK_IMAGE")" && pwd)/$(basename "$DISK_IMAGE")"
if [[ ! -f "$DISK_IMAGE" ]]; then
  echo "disk image not found: $DISK_IMAGE" >&2
  exit 1
fi

export SCONSFLAGS="--ignore-style ${SCONSFLAGS:-}"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "build/X86/gem5.opt" \
    --label "gem5 binary for dedicated app remaining" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if [[ -n "$GUEST_CFLAGS" ]]; then
    HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_app_disk_image.sh "$DISK_IMAGE"
  else
    bash tools/setup_hydrarpc_dedicated_app_disk_image.sh "$DISK_IMAGE"
  fi
fi

export DISK_IMAGE

args=(
  bash tools/run_hydrarpc_app_sweep.sh
  --root-outdir "$ROOT_OUTDIR"
  --client-counts "32"
  --profiles "ycsb_a_1k ycsb_b_1k ycsb_c_1k ycsb_d_1k udb_a udb_b udb_c udb_d"
  --kinds dedicated
  --count-per-client "$COUNT_PER_CLIENT"
  --record-count 10000
  --cpu-type TIMING
  --boot-cpu KVM
  --num-cpus "$NUM_CPUS"
  --restore-checkpoint "$CHECKPOINT_DIR"
  --parallel-jobs "$PARALLEL_JOBS"
)

if [[ "$SKIP_BUILD" -eq 1 ]]; then
  args+=(--skip-build)
fi
if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
  args+=(--skip-image-setup)
fi
if [[ "$SKIP_EXISTING" -eq 1 ]]; then
  args+=(--skip-existing)
fi
if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
  args+=(--continue-on-failure)
fi
if [[ -n "$GUEST_CFLAGS" ]]; then
  args+=(--guest-cflags "$GUEST_CFLAGS")
fi

printf 'ROOT=%s\n' "$ROOT_OUTDIR"
printf 'CHECKPOINT=%s\n' "$CHECKPOINT_DIR"
printf 'DISK_IMAGE=%s\n' "$DISK_IMAGE"
printf 'PARALLEL_JOBS=%s\n' "$PARALLEL_JOBS"
printf '%q ' "${args[@]}"
printf '\n'
"${args[@]}"
