#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_m1_respsize_resume.sh [options]

Options:
  --root-outdir <dir>      Root motivation m1 directory.
                           Default: output/motivation_m1
  --checkpoint <dir>       Existing classic checkpoint directory. Required.
  --count-per-client <N>   Requests per client. Default: 30
  --num-cpus <N>           Guest CPU count. Default: 34
  --skip-build             Reuse existing gem5 binary after freshness checks.
  --skip-image-setup       Reuse already injected guest binaries.
  --skip-existing          Reuse completed outdirs when possible.
  --continue-on-failure    Record failures and continue remaining work.
  --help                   Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

ROOT_OUTDIR="output/motivation_m1"
CHECKPOINT_DIR=""
COUNT_PER_CLIENT=30
NUM_CPUS=34
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
CHECKPOINT_DIR="$(cd "$CHECKPOINT_DIR" && pwd)"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons build/X86/gem5.opt -j"$(nproc)"
fi

for resp_bytes in 8 256 1024 4096 8192; do
  args=(
    bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$ROOT_OUTDIR/respsize_c32_resp${resp_bytes}"
    --kinds dedicated
    --client-counts "32"
    --req-bytes 64
    --resp-bytes "$resp_bytes"
    --count-per-client "$COUNT_PER_CLIENT"
    --slot-count 1024
    --cpu-type TIMING
    --boot-cpu KVM
    --num-cpus "$NUM_CPUS"
    --restore-checkpoint "$CHECKPOINT_DIR"
    --skip-build
    --parallel-jobs 1
  )

  if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
    args+=(--skip-image-setup)
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    args+=(--skip-existing)
  fi
  if [[ "$CONTINUE_ON_FAILURE" -eq 1 ]]; then
    args+=(--continue-on-failure)
  fi

  "${args[@]}"
done
