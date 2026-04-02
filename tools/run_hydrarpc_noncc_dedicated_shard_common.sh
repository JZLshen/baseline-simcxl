#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MACHINE_TAG:-}" ]]; then
  echo "MACHINE_TAG must be set before sourcing run_hydrarpc_noncc_dedicated_shard_common.sh" >&2
  return 1 2>/dev/null || exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STAMP="${STAMP:-$(date +%Y%m%d_%H%M%S)}"
ROOT_OUTDIR="${ROOT_OUTDIR:-${REPO_ROOT}/output/hydrarpc_noncc_dedicated_${MACHINE_TAG}_${STAMP}}"
COUNT_PER_CLIENT="${COUNT_PER_CLIENT:-30}"
SLOT_COUNT="${SLOT_COUNT:-1024}"
CPU_TYPE="${CPU_TYPE:-TIMING}"
GUEST_CFLAGS="${GUEST_CFLAGS:-}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_IMAGE_SETUP="${SKIP_IMAGE_SETUP:-0}"
SKIP_EXISTING="${SKIP_EXISTING:-0}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-${ROOT_OUTDIR}/checkpoints}"
mkdir -p "$CHECKPOINT_ROOT"
CHECKPOINT_ROOT="$(cd "$CHECKPOINT_ROOT" && pwd)"

RUN_LOG="$ROOT_OUTDIR/run.log"
: >"$RUN_LOG"

BUILD_DONE=0
DEDICATED_IMAGE_READY=0

run_cmd() {
  printf '[%s] ' "$(date '+%F %T')" | tee -a "$RUN_LOG"
  printf '%q ' "$@" | tee -a "$RUN_LOG"
  printf '\n' | tee -a "$RUN_LOG"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi

  "$@"
}

ensure_build() {
  if [[ "$SKIP_BUILD" -eq 1 || "$BUILD_DONE" -eq 1 ]]; then
    return 0
  fi

  run_cmd scons build/X86/gem5.opt -j"$(nproc)"
  BUILD_DONE=1
}

ensure_dedicated_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 || "$DEDICATED_IMAGE_READY" -eq 1 ]]; then
    return 0
  fi

  ensure_build
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  fi
  DEDICATED_IMAGE_READY=1
}

run_case() {
  local name="$1"
  shift
  local outdir="${ROOT_OUTDIR}/${name}"
  local args=(
    bash tools/run_e2e_hydrarpc_dedicated.sh
    --outdir "$outdir"
    --skip-build
    --skip-image-setup
    --cpu-type "$CPU_TYPE"
    --count-per-client "$COUNT_PER_CLIENT"
    --slot-count "$SLOT_COUNT"
    --atomic-checkpoint
    --checkpoint-root "$CHECKPOINT_ROOT"
  )

  if [[ "$SKIP_EXISTING" -eq 1 && -f "$outdir/board.pc.com_1.device" ]]; then
    printf '[%s] REUSE %s\n' "$(date '+%F %T')" "$outdir" | tee -a "$RUN_LOG"
    return 0
  fi

  if [[ -n "$GUEST_CFLAGS" ]]; then
    args+=(--guest-cflags "$GUEST_CFLAGS")
  fi

  args+=("$@")
  run_cmd "${args[@]}"
}

print_footer() {
  echo
  echo "Non-coherent dedicated shard complete."
  echo "machine_tag=$MACHINE_TAG"
  echo "root_outdir=$ROOT_OUTDIR"
  echo "checkpoint_root=$CHECKPOINT_ROOT"
  echo "run_log=$RUN_LOG"
}
