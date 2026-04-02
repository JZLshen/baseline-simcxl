#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_dedicated_motivation_kvm3.sh [options]

Options:
  --machine-index <1|2|3>  Which shard to run. Required.
  --root-outdir <dir>      Root output directory.
                           Default: output/hydrarpc_dedicated_motivation_kvm3_m<idx>_<timestamp>
  --binary <path>          gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>        Benchmark CPU type. Default: TIMING
  --boot-cpu <type>        Boot CPU type for checkpoint creation. Default: KVM
  --num-cpus <N>           Fixed guest CPU count used by all runs.
                           Default: 34
  --parallel-jobs <N>      Max concurrent single-run sweeps. Default: 2
  --count-per-client <N>   Requests per client. Default: 30
  --slot-count <N>         Ring depth. Default: 1024
  --guest-cflags <flags>   Host gcc flags for guest binaries.
  --skip-build             Skip scons build.
  --skip-image-setup       Reuse already injected guest binaries.
  --skip-existing          Reuse existing sweep outdirs when possible.
  --dry-run                Print commands only.
  --help                   Show this message.

Shard layout:
  machine 1: coherent direct pow2 + dedicated response-size motivation
  machine 2: non-coherent staging pow2 + half of sparse32
  machine 3: non-coherent direct pow2 + half of sparse32

All runs use num-cpus=34 by default so each machine can reuse one Classic
checkpoint and, when needed, one Ruby checkpoint.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export DISK_IMAGE="${DISK_IMAGE:-${REPO_ROOT}/files/parsec.img}"

MACHINE_INDEX=""
ROOT_OUTDIR=""
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=34
PARALLEL_JOBS=2
COUNT_PER_CLIENT=30
SLOT_COUNT=1024
GUEST_CFLAGS=""
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
DRY_RUN=0

run_cmd() {
  printf '[%s] ' "$(date '+%F %T')" | tee -a "$RUN_LOG"
  printf '%q ' "$@" | tee -a "$RUN_LOG"
  printf '\n' | tee -a "$RUN_LOG"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi

  "$@"
}

wait_for_background_or_fail() {
  local wait_rc=0

  set +e
  wait -n
  wait_rc=$?
  set -e

  if [[ "$wait_rc" -ne 0 ]]; then
    jobs -pr | xargs -r kill 2>/dev/null || true
    wait || true
    echo "[$(date '+%F %T')] FAIL-FAST aborting motivation shard after background failure" \
      | tee -a "$RUN_LOG"
    exit "$wait_rc"
  fi
}

checkpoint_path() {
  local kind="$1"
  printf '%s\n' "$CHECKPOINT_ROOT/${kind}_n${NUM_CPUS}/latest"
}

ensure_build() {
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    return 0
  fi
  run_cmd scons "$BINARY" -j"$(nproc)"
}

ensure_dedicated_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_disk_image.sh "$DISK_IMAGE"
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_disk_image.sh "$DISK_IMAGE"
  fi
}

ensure_coherent_image() {
  if [[ "$SKIP_IMAGE_SETUP" -eq 1 ]]; then
    return 0
  fi
  if [[ -n "$GUEST_CFLAGS" ]]; then
    run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
      bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh "$DISK_IMAGE"
  else
    run_cmd bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh "$DISK_IMAGE"
  fi
}

ensure_checkpoint() {
  local mode="$1"
  local outdir="$CHECKPOINT_ROOT/${mode}_n${NUM_CPUS}"
  local cmd=(bash tools/create_hydrarpc_boot_checkpoint.sh
    --mode "$mode"
    --outdir "$outdir"
    --binary "$BINARY"
    --boot-cpu "$BOOT_CPU"
    --num-cpus "$NUM_CPUS"
    --disk-image "$DISK_IMAGE"
    --skip-build
  )

  run_cmd "${cmd[@]}"
}

append_common_noncc_args() {
  local -n out_ref=$1

  out_ref+=(
    --count-per-client "$COUNT_PER_CLIENT"
    --slot-count "$SLOT_COUNT"
    --cpu-type "$CPU_TYPE"
    --boot-cpu "$BOOT_CPU"
    --num-cpus "$NUM_CPUS"
    --restore-checkpoint "$(checkpoint_path classic)"
    --skip-build
    --skip-image-setup
  )

  if [[ -n "$GUEST_CFLAGS" ]]; then
    out_ref+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    out_ref+=(--skip-existing)
  fi
}

append_common_cc_args() {
  local -n out_ref=$1

  out_ref+=(
    --count-per-client "$COUNT_PER_CLIENT"
    --slot-count "$SLOT_COUNT"
    --cpu-type "$CPU_TYPE"
    --boot-cpu "$BOOT_CPU"
    --num-cpus "$NUM_CPUS"
    --restore-checkpoint "$(checkpoint_path ruby)"
    --skip-build
    --skip-image-setup
  )

  if [[ -n "$GUEST_CFLAGS" ]]; then
    out_ref+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    out_ref+=(--skip-existing)
  fi
}

run_noncc_single() {
  local root_outdir="$1"
  shift
  local args=(bash tools/run_hydrarpc_sweep.sh --root-outdir "$root_outdir")

  args+=("$@")
  append_common_noncc_args args
  args+=(--parallel-jobs 1)
  run_cmd "${args[@]}"
}

run_cc_pow2() {
  local root_outdir="$1"
  local args=(bash tools/run_hydrarpc_coherent_sweep.sh
    --root-outdir "$root_outdir"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 64
    --resp-bytes 64
    --request-transfer-mode direct
    --response-transfer-mode direct
  )

  append_common_cc_args args
  args+=(--parallel-jobs "$PARALLEL_JOBS")
  run_cmd "${args[@]}"
}

run_noncc_pow2() {
  local root_outdir="$1"
  local request_mode="$2"
  local response_mode="$3"
  local args=(bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$root_outdir"
    --kinds dedicated
    --client-counts "1 2 4 8 16 32"
    --req-bytes 64
    --resp-bytes 64
    --request-transfer-mode "$request_mode"
    --response-transfer-mode "$response_mode"
  )

  append_common_noncc_args args
  args+=(--parallel-jobs "$PARALLEL_JOBS")
  run_cmd "${args[@]}"
}

run_sparse_batch() {
  local -n points_ref=$1
  local pair=""
  local sc=""
  local sq=""

  for pair in "${points_ref[@]}"; do
    read -r sc sq <<<"$pair"
    run_noncc_single \
      "$ROOT_OUTDIR/sparse32_sc${sc}_sq${sq}" \
      --kinds dedicated \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes 64 \
      --slow-client-count "$sc" \
      --slow-count-per-client "$sq" \
      --slow-send-gap-ns 20000 &
    while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
      wait_for_background_or_fail
    done
  done

  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
}

run_respsize_batch() {
  local resp_bytes=""

  for resp_bytes in 8 256 1024 4096 8192; do
    run_noncc_single \
      "$ROOT_OUTDIR/respsize_c32_resp${resp_bytes}" \
      --kinds dedicated \
      --client-counts "32" \
      --req-bytes 64 \
      --resp-bytes "$resp_bytes" &
    while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL_JOBS" ]]; do
      wait_for_background_or_fail
    done
  done

  while [[ "$(jobs -pr | wc -l)" -gt 0 ]]; do
    wait_for_background_or_fail
  done
}

run_machine1() {
  ensure_dedicated_image
  ensure_coherent_image
  ensure_checkpoint classic
  ensure_checkpoint ruby

  run_cc_pow2 "$ROOT_OUTDIR/cc_direct_req64_resp64_pow2"
  run_respsize_batch
}

run_machine2() {
  local sparse_points=(
    "4 30"
    "16 30"
    "24 30"
    "4 15"
    "8 8"
    "16 15"
    "24 15"
    "20 8"
    "28 8"
  )

  ensure_dedicated_image
  ensure_checkpoint classic

  run_noncc_pow2 \
    "$ROOT_OUTDIR/noncc_staging_req64_resp64_pow2" \
    staging \
    staging
  run_sparse_batch sparse_points
}

run_machine3() {
  local sparse_points=(
    "8 30"
    "20 30"
    "28 30"
    "4 8"
    "8 15"
    "20 15"
    "16 8"
    "28 15"
    "24 8"
  )

  ensure_dedicated_image
  ensure_checkpoint classic

  run_noncc_pow2 \
    "$ROOT_OUTDIR/noncc_direct_req64_resp64_pow2" \
    direct \
    direct
  run_sparse_batch sparse_points
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --machine-index)
      MACHINE_INDEX="$2"
      shift 2
      ;;
    --root-outdir)
      ROOT_OUTDIR="$2"
      shift 2
      ;;
    --binary)
      BINARY="$2"
      shift 2
      ;;
    --cpu-type)
      CPU_TYPE="$2"
      shift 2
      ;;
    --boot-cpu)
      BOOT_CPU="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
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
    --slot-count)
      SLOT_COUNT="$2"
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
    --dry-run)
      DRY_RUN=1
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

case "$MACHINE_INDEX" in
  1|2|3)
    ;;
  *)
    echo "--machine-index must be one of 1, 2, 3" >&2
    exit 1
    ;;
esac

if [[ "$PARALLEL_JOBS" -lt 1 ]]; then
  echo "--parallel-jobs must be >= 1" >&2
  exit 1
fi

cd "$REPO_ROOT"

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/hydrarpc_dedicated_motivation_kvm3_m${MACHINE_INDEX}_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"
CHECKPOINT_ROOT="$ROOT_OUTDIR/checkpoints"
RUN_LOG="$ROOT_OUTDIR/run.log"
: >"$RUN_LOG"

ensure_build

case "$MACHINE_INDEX" in
  1)
    run_machine1
    ;;
  2)
    run_machine2
    ;;
  3)
    run_machine3
    ;;
esac

echo
echo "Dedicated motivation shard complete."
echo "machine_index=$MACHINE_INDEX"
echo "root_outdir=$ROOT_OUTDIR"
echo "checkpoint_root=$CHECKPOINT_ROOT"
echo "run_log=$RUN_LOG"
