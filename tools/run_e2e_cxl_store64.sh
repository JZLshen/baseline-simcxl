#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_cxl_store64.sh --kernel <path> --disk-image <path> [options]

Required:
  --kernel <path>       Linux kernel image (vmlinux).
  --disk-image <path>   Disk image for full-system boot.

Options:
  --outdir <dir>        Output directory. Default: output/e2e_cxl_store64
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --skip-build          Skip scons build.
  --help                Show this message.

Notes:
  - No pre-installed guest binary is required.
  - This script injects `tools/store64_lat.c` into the guest, compiles it
    with gcc, and runs under `numactl -N 0 -m 1`.
EOF
}

KERNEL=""
DISK_IMAGE=""
OUTDIR="output/e2e_cxl_store64"
BINARY="build/X86/gem5.opt"
SKIP_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kernel)
      KERNEL="$2"
      shift 2
      ;;
    --disk-image)
      DISK_IMAGE="$2"
      shift 2
      ;;
    --outdir)
      OUTDIR="$2"
      shift 2
      ;;
    --binary)
      BINARY="$2"
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

if [[ -z "$KERNEL" || -z "$DISK_IMAGE" ]]; then
  echo "Error: --kernel and --disk-image are required." >&2
  usage
  exit 1
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
fi

mkdir -p "$OUTDIR"

B64_SRC="$(base64 -w0 tools/store64_lat.c)"
# Compile under the fast boot core first, then switch to TIMING/O3 only for
# the actual latency probe to avoid spending simulator time in gcc.
WORKLOAD_CMD="numactl -H; printf '%s' \"$B64_SRC\" | base64 -d > /tmp/store64_lat.c; gcc -O2 /tmp/store64_lat.c -o /tmp/store64_lat; m5 exit; m5 resetstats; numactl -N 0 -m 1 /tmp/store64_lat; m5 dumpstats; m5 exit"

"$BINARY" \
  -d "$OUTDIR" \
  --debug-flags=Bridge,CXLMemCtrl,MemCtrl \
  --debug-file=cxl_trace.log \
  configs/example/gem5_library/x86-cxl-type3-with-classic.py \
  --is_asic True \
  --cpu_type TIMING \
  --boot_cpu KVM \
  --kernel "$KERNEL" \
  --disk-image "$DISK_IMAGE" \
  --workload-cmd "$WORKLOAD_CMD"

echo
echo "=== Guest end-to-end 64B CXL store metrics ==="
rg -n "^(cpu_mhz|samples|first_cycles|avg_cycles|min_cycles|max_cycles|first_ns|avg_ns|min_ns|max_ns)=" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"
