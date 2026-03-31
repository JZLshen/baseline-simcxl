#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVER_TAG:-}" ]]; then
  echo "SERVER_TAG must be set before sourcing run_hydrarpc_motivation_common.sh" >&2
  return 1 2>/dev/null || exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STAMP="${STAMP:-$(date +%Y%m%d_%H%M%S)}"
ROOT_OUTDIR="${ROOT_OUTDIR:-${REPO_ROOT}/output/hydrarpc_motivation_${SERVER_TAG}_${STAMP}}"
COUNT_PER_CLIENT="${COUNT_PER_CLIENT:-30}"
SLOT_COUNT="${SLOT_COUNT:-1024}"
CPU_TYPE="${CPU_TYPE:-TIMING}"
BOOT_CPU="${BOOT_CPU:-KVM}"
NUM_CPUS="${NUM_CPUS:-0}"
PARALLEL_JOBS="${PARALLEL_JOBS:-1}"
GUEST_CFLAGS="${GUEST_CFLAGS:-}"
SLOW_SEND_GAP_NS="${SLOW_SEND_GAP_NS:-20000}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_IMAGE_SETUP="${SKIP_IMAGE_SETUP:-0}"
SKIP_EXISTING="${SKIP_EXISTING:-0}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"
RUN_LOG="$ROOT_OUTDIR/run.log"
: >"$RUN_LOG"

BUILD_DONE=0
DEDICATED_READY=0
SHARED_READY=0
COHERENT_READY=0

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
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 || "$DEDICATED_READY" -eq 1 ]]; then
    return 0
  fi

  ensure_build
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
  fi
  DEDICATED_READY=1
}

ensure_shared_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 || "$SHARED_READY" -eq 1 ]]; then
    return 0
  fi

  ensure_build
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
  else
    run_cmd bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
  fi
  SHARED_READY=1
}

ensure_coherent_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 || "$COHERENT_READY" -eq 1 ]]; then
    return 0
  fi

  ensure_build
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh files/parsec.img
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh files/parsec.img
  fi
  COHERENT_READY=1
}

append_common_sweep_args() {
  local -n out_ref=$1

  out_ref+=(--count-per-client "$COUNT_PER_CLIENT")
  out_ref+=(--slot-count "$SLOT_COUNT")
  out_ref+=(--cpu-type "$CPU_TYPE")
  out_ref+=(--boot-cpu "$BOOT_CPU")
  out_ref+=(--parallel-jobs "$PARALLEL_JOBS")
  out_ref+=(--skip-build --skip-image-setup)

  if [[ "$NUM_CPUS" -gt 0 ]]; then
    out_ref+=(--num-cpus "$NUM_CPUS")
  fi
  if [[ -n "$GUEST_CFLAGS" ]]; then
    out_ref+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    out_ref+=(--skip-existing)
  fi
}

run_noncc_sweep() {
  local name="$1"
  shift
  local args=(bash tools/run_hydrarpc_sweep.sh --root-outdir "$ROOT_OUTDIR/$name")
  args+=("$@")
  append_common_sweep_args args
  run_cmd "${args[@]}"
}

run_cc_sweep() {
  local name="$1"
  shift
  local args=(bash tools/run_hydrarpc_coherent_sweep.sh --root-outdir "$ROOT_OUTDIR/$name")
  args+=("$@")
  append_common_sweep_args args
  run_cmd "${args[@]}"
}

run_shared_competition_pow2() {
  run_noncc_sweep \
    "moti_shared_competition_req64_resp64_pow2" \
    --kinds "shared" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64
}

run_dedicated_noncc_staging_pow2() {
  run_noncc_sweep \
    "moti_dedicated_noncc_staging_req64_resp64_pow2" \
    --kinds "dedicated" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --request-transfer-mode staging \
    --response-transfer-mode staging
}

run_dedicated_noncc_direct_pow2() {
  run_noncc_sweep \
    "moti_dedicated_noncc_direct_req64_resp64_pow2" \
    --kinds "dedicated" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --request-transfer-mode direct \
    --response-transfer-mode direct
}

run_dedicated_cc_direct_pow2() {
  run_cc_sweep \
    "moti_dedicated_cc_direct_req64_resp64_pow2" \
    --client-counts "1 2 4 8 16 32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --request-transfer-mode direct \
    --response-transfer-mode direct
}

run_dedicated_respsize() {
  local resp_bytes="$1"

  run_noncc_sweep \
    "moti_dedicated_respsize_c32_resp${resp_bytes}" \
    --kinds "dedicated" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes "$resp_bytes" \
    --request-transfer-mode staging \
    --response-transfer-mode staging
}

run_dedicated_sparse32() {
  local slow_client_count="$1"
  local slow_count_per_client="$2"

  run_noncc_sweep \
    "moti_dedicated_sparse32_sc${slow_client_count}_sq${slow_count_per_client}" \
    --kinds "dedicated" \
    --client-counts "32" \
    --req-bytes 64 \
    --resp-bytes 64 \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$slow_count_per_client" \
    --slow-send-gap-ns "$SLOW_SEND_GAP_NS" \
    --request-transfer-mode staging \
    --response-transfer-mode staging
}

print_motivation_footer() {
  echo
  echo "Motivation shard complete."
  echo "server_tag=$SERVER_TAG"
  echo "root_outdir=$ROOT_OUTDIR"
  echo "run_log=$RUN_LOG"
}
