#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_cxl_load_store_baseline.sh [options]

Options:
  --outdir <dir>        Output directory. Default: output/cxl_load_store_baseline_r1
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>     Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>     Boot CPU type: KVM or ATOMIC. Default: KVM
  --count <N>           Number of store/load samples. Default: 1
  --cpu <N>             Guest CPU id. Default: 0
  --debug-flags <list>  Optional gem5 debug flags.
  --skip-build          Skip scons build.
  --help                Show this message.

Fixed resources:
  - kernel: repo-local files/vmlinux
  - disk:   repo-local files/parsec.img

Notes:
  - `store_delta_tsc` measures: store + clflushopt + sfence.
  - `load_delta_tsc` measures: clflushopt + mfence + load.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KERNEL="${REPO_ROOT}/files/vmlinux"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"

OUTDIR="output/cxl_load_store_baseline_r1"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
COUNT=1
CPU_ID=0
DEBUG_FLAGS=""
SKIP_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir)
      OUTDIR="$2"
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
    --count)
      COUNT="$2"
      shift 2
      ;;
    --cpu)
      CPU_ID="$2"
      shift 2
      ;;
    --debug-flags)
      DEBUG_FLAGS="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
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

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
fi

mkdir -p "$OUTDIR"

B64_SRC="$(base64 -w0 tools/cxl_load_store_baseline.c)"
GUEST_CMD="numactl -N 0 -m 1 /tmp/cxl_load_store_baseline --count ${COUNT} --cpu ${CPU_ID}"
WORKLOAD_CMD="printf '%s' \"$B64_SRC\" | base64 -d > /tmp/cxl_load_store_baseline.c; gcc -O3 /tmp/cxl_load_store_baseline.c -o /tmp/cxl_load_store_baseline; m5 exit; m5 resetstats; ${GUEST_CMD}; m5 dumpstats; m5 exit"

gem5_args=(
  -d "$OUTDIR"
)

if [[ -n "$DEBUG_FLAGS" ]]; then
  gem5_args+=(--debug-flags="$DEBUG_FLAGS" --debug-file=cxl_trace.log)
fi

gem5_args+=(
  configs/example/gem5_library/x86-cxl-type3-with-classic.py
  --is_asic True
  --cpu_type "$CPU_TYPE"
  --boot_cpu "$BOOT_CPU"
  --num_cpus 2
  --kernel "$KERNEL"
  --disk-image "$DISK_IMAGE"
  --workload-cmd "$WORKLOAD_CMD"
)

"$BINARY" "${gem5_args[@]}"

echo
echo "=== CXL single-op baseline output ==="
rg -n "^(iter_[0-9]+_(store|load)_(start_tsc|end_tsc|delta_tsc|delta_ns)|iter_[0-9]+_(store|load)_correctness_fail)" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"
