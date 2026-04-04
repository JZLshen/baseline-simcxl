#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_baseline_minset.sh [options]

Options:
  --root-outdir <dir>      Root output directory.
                           Default: output/paper_baseline_minset_<timestamp>
  --groups <list>          Quoted list from:
                           "overall coherence req-size resp-size ring-size sparse32 cxl-latency"
                           Default: all groups
  --cpu-type <type>        Passed to sub-runners. Default: TIMING
  --boot-cpu <type>        Passed to sub-runners. Default: KVM
  --num-cpus <N>           Passed to sub-runners. Default: auto
  --parallel-jobs <N>      Passed to non-coherent/coherent sweep runners. Default: 1
  --guest-cflags <flags>   Host gcc flags used for injected guest binaries.
  --slow-send-gap-ns <N>   Sparse32 slow-client gap. Default: 20000
  --skip-build             Skip top-level scons build.
  --skip-image-setup       Reuse already injected guest binaries in the disk image.
  --skip-existing          Reuse existing sub-run outdirs when possible.
  --dry-run                Print commands only.
  --help                   Show this message.

Fixed experiment contract:
  - count-per-client = 30
  - slot-count = 1024
  - no 3-client point
  - no slow16 group
  - sparse32 slow-count-per-client in {8, 15}
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ROOT_OUTDIR=""
SELECTED_GROUPS="overall coherence req-size resp-size ring-size sparse32 cxl-latency"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=0
PARALLEL_JOBS=1
GUEST_CFLAGS=""
SLOW_SEND_GAP_NS=20000
SKIP_BUILD=0
SKIP_IMAGE_SETUP=0
SKIP_EXISTING=0
DRY_RUN=0

COUNT_PER_CLIENT=30
SLOT_COUNT=1024

run_cmd() {
  printf '[%s] ' "$(date '+%F %T')" | tee -a "$RUN_LOG"
  printf '%q ' "$@" | tee -a "$RUN_LOG"
  printf '\n' | tee -a "$RUN_LOG"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi

  "$@"
}

append_common_sweep_args() {
  local -n out_ref=$1
  local include_slot_count="${2:-1}"

  out_ref+=(--count-per-client "$COUNT_PER_CLIENT")
  if [[ "$include_slot_count" -eq 1 ]]; then
    out_ref+=(--slot-count "$SLOT_COUNT")
  fi
  out_ref+=(--cpu-type "$CPU_TYPE")
  out_ref+=(--boot-cpu "$BOOT_CPU")
  out_ref+=(--parallel-jobs "$PARALLEL_JOBS")

  if [[ "$NUM_CPUS" -gt 0 ]]; then
    out_ref+=(--num-cpus "$NUM_CPUS")
  fi
  if [[ -n "$GUEST_CFLAGS" ]]; then
    out_ref+=(--guest-cflags "$GUEST_CFLAGS")
  fi
  if [[ "$SKIP_EXISTING" -eq 1 ]]; then
    out_ref+=(--skip-existing)
  fi
  out_ref+=(--skip-build --skip-image-setup)
}

run_overall_group() {
  local args=()

  args=(bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$ROOT_OUTDIR/overall_dedicated_shared_req64_resp64_pow2"
    --kinds "dedicated shared"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 64
    --resp-bytes 64
  )
  append_common_sweep_args args
  run_cmd "${args[@]}"

  args=(bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$ROOT_OUTDIR/overall_dedicated_shared_req1530_resp315_pow2"
    --kinds "dedicated shared"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 1530
    --resp-bytes 315
    --req-min-bytes 765
    --req-max-bytes 2295
    --resp-min-bytes 158
    --resp-max-bytes 472
  )
  append_common_sweep_args args
  run_cmd "${args[@]}"

  args=(bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$ROOT_OUTDIR/overall_dedicated_shared_req38_resp230_pow2"
    --kinds "dedicated shared"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 38
    --resp-bytes 230
    --req-min-bytes 19
    --req-max-bytes 57
    --resp-min-bytes 115
    --resp-max-bytes 345
  )
  append_common_sweep_args args
  run_cmd "${args[@]}"
}

run_coherence_group() {
  local args=()

  args=(bash tools/run_hydrarpc_sweep.sh
    --root-outdir "$ROOT_OUTDIR/coherence_dedicated_noncc_staging_req64_resp64_pow2"
    --kinds "dedicated"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 64
    --resp-bytes 64
    --request-transfer-mode staging
    --response-transfer-mode staging
  )
  append_common_sweep_args args
  run_cmd "${args[@]}"

  args=(bash tools/run_hydrarpc_coherent_sweep.sh
    --root-outdir "$ROOT_OUTDIR/coherence_dedicated_cc_staging_req64_resp64_pow2"
    --client-counts "1 2 4 8 16 32"
    --req-bytes 64
    --resp-bytes 64
    --request-transfer-mode staging
    --response-transfer-mode staging
  )
  append_common_sweep_args args
  run_cmd "${args[@]}"
}

run_req_size_group() {
  local req_bytes=""
  local args=()

  for req_bytes in 8 256 1024 4096 8192; do
    args=(bash tools/run_hydrarpc_sweep.sh
      --root-outdir "$ROOT_OUTDIR/reqsize_dedicated_shared_c32_req${req_bytes}_resp64"
      --kinds "dedicated shared"
      --client-counts "32"
      --req-bytes "$req_bytes"
      --resp-bytes 64
    )
    append_common_sweep_args args
    run_cmd "${args[@]}"
  done
}

run_resp_size_group() {
  local resp_bytes=""
  local args=()

  for resp_bytes in 8 256 1024 4096 8192; do
    args=(bash tools/run_hydrarpc_sweep.sh
      --root-outdir "$ROOT_OUTDIR/respsize_dedicated_shared_c32_req64_resp${resp_bytes}"
      --kinds "dedicated shared"
      --client-counts "32"
      --req-bytes 64
      --resp-bytes "$resp_bytes"
    )
    append_common_sweep_args args
    run_cmd "${args[@]}"
  done
}

run_ring_size_group() {
  local ring_size=""
  local args=()

  for ring_size in 16 32 64 128 256 512 1024; do
    args=(bash tools/run_hydrarpc_sweep.sh
      --root-outdir "$ROOT_OUTDIR/ringsize_dedicated_shared_c32_req64_resp64_s${ring_size}"
      --kinds "dedicated shared"
      --client-counts "32"
      --slot-count "$ring_size"
      --req-bytes 64
      --resp-bytes 64
    )
    append_common_sweep_args args 0
    run_cmd "${args[@]}"
  done
}

run_cxl_latency_group() {
  local latency=""
  local args=()

  for latency in 100ns 200ns 300ns; do
    args=(bash tools/run_hydrarpc_sweep.sh
      --root-outdir "$ROOT_OUTDIR/cxl_latency_dedicated_shared_c32_req64_resp64_${latency}"
      --kinds "dedicated shared"
      --client-counts "32"
      --req-bytes 64
      --resp-bytes 64
      --cxl-bridge-extra-latency "$latency"
    )
    append_common_sweep_args args
    run_cmd "${args[@]}"
  done
}

run_sparse32_group() {
  local slow_client_count=""
  local slow_count_per_client=""
  local args=()

  for slow_count_per_client in 8 15; do
    for slow_client_count in 4 8 16 20 24 28; do
      args=(bash tools/run_hydrarpc_sweep.sh
        --root-outdir "$ROOT_OUTDIR/sparse32_dedicated_shared_c32_sc${slow_client_count}_sq${slow_count_per_client}"
        --kinds "dedicated shared"
        --client-counts "32"
        --req-bytes 64
        --resp-bytes 64
        --slow-client-count "$slow_client_count"
        --slow-count-per-client "$slow_count_per_client"
        --slow-send-gap-ns "$SLOW_SEND_GAP_NS"
      )
      append_common_sweep_args args
      run_cmd "${args[@]}"
    done
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-outdir)
      ROOT_OUTDIR="$2"
      shift 2
      ;;
    --groups)
      SELECTED_GROUPS="$2"
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
    --guest-cflags)
      GUEST_CFLAGS="$2"
      shift 2
      ;;
    --slow-send-gap-ns)
      SLOW_SEND_GAP_NS="$2"
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

cd "$REPO_ROOT"

group_enabled() {
  local group="$1"
  [[ " $SELECTED_GROUPS " == *" $group "* ]]
}

if [[ -z "$ROOT_OUTDIR" ]]; then
  ROOT_OUTDIR="output/hydrarpc_baseline_minset_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$ROOT_OUTDIR"
ROOT_OUTDIR="$(cd "$ROOT_OUTDIR" && pwd)"
RUN_LOG="$ROOT_OUTDIR/top_level.log"
: >"$RUN_LOG"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  run_cmd scons build/X86/gem5.opt -j"$(nproc)"
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if group_enabled overall || group_enabled req-size || group_enabled resp-size || \
     group_enabled ring-size || group_enabled sparse32 || group_enabled coherence || \
     group_enabled cxl-latency; then
    if [[ -n "$GUEST_CFLAGS" ]]; then
      run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
        bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
    else
      run_cmd bash tools/setup_hydrarpc_dedicated_disk_image.sh files/parsec.img
    fi
  fi

  if group_enabled overall || group_enabled req-size || group_enabled resp-size || \
     group_enabled ring-size || group_enabled sparse32 || group_enabled cxl-latency; then
    if [[ -n "$GUEST_CFLAGS" ]]; then
      run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
        bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
    else
      run_cmd bash tools/setup_hydrarpc_shared_disk_image.sh files/parsec.img
    fi
  fi

  if group_enabled coherence; then
    if [[ -n "$GUEST_CFLAGS" ]]; then
      run_cmd env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
        bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh files/parsec.img
    else
      run_cmd bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh files/parsec.img
    fi
  fi
fi

case " $SELECTED_GROUPS " in
  *" overall "*) run_overall_group ;;
esac

case " $SELECTED_GROUPS " in
  *" coherence "*) run_coherence_group ;;
esac

case " $SELECTED_GROUPS " in
  *" req-size "*) run_req_size_group ;;
esac

case " $SELECTED_GROUPS " in
  *" resp-size "*) run_resp_size_group ;;
esac

case " $SELECTED_GROUPS " in
  *" ring-size "*) run_ring_size_group ;;
esac

case " $SELECTED_GROUPS " in
  *" sparse32 "*) run_sparse32_group ;;
esac

case " $SELECTED_GROUPS " in
  *" cxl-latency "*) run_cxl_latency_group ;;
esac

echo
echo "Baseline min-set submission complete."
echo "root_outdir=$ROOT_OUTDIR"
echo "top_level_log=$RUN_LOG"
