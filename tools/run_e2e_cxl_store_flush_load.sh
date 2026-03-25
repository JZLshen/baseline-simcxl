#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_cxl_store_flush_load.sh --kernel <path> --disk-image <path> [options]

Required:
  --kernel <path>       Linux kernel image (vmlinux).
  --disk-image <path>   Disk image for full-system boot.

Options:
  --outdir <dir>        Output directory. Default: output/e2e_cxl_store_flush_load
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>     Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>     Boot CPU type: KVM or ATOMIC. Default: KVM
  --debug-flags <list>  Optional gem5 debug flags, e.g. Bridge,CXLMemCtrl.
  --skip-build          Skip scons build.
  --help                Show this message.

Notes:
  - The guest program performs:
      CXL store + clflushopt + sfence
      then loop: clflush + load until the new value is observed.
EOF
}

KERNEL=""
DISK_IMAGE=""
OUTDIR="output/e2e_cxl_store_flush_load"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
DEBUG_FLAGS=""
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
    --cpu-type)
      CPU_TYPE="$2"
      shift 2
      ;;
    --boot-cpu)
      BOOT_CPU="$2"
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

if [[ -z "$KERNEL" || -z "$DISK_IMAGE" ]]; then
  echo "Error: --kernel and --disk-image are required." >&2
  usage
  exit 1
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
fi

mkdir -p "$OUTDIR"

B64_SRC="$(base64 -w0 tools/store_flush_load_retry.c)"
WORKLOAD_CMD="numactl -H; printf '%s' \"$B64_SRC\" | base64 -d > /tmp/store_flush_load_retry.c; gcc -O2 /tmp/store_flush_load_retry.c -o /tmp/store_flush_load_retry; m5 exit; m5 resetstats; numactl -N 0 -m 1 /tmp/store_flush_load_retry; m5 dumpstats; m5 exit"

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
  --kernel "$KERNEL"
  --disk-image "$DISK_IMAGE"
  --workload-cmd "$WORKLOAD_CMD"
)

"$BINARY" "${gem5_args[@]}"

echo
echo "=== Guest sfence/load visibility metrics ==="
rg -n "^(cpu_mhz|used_clflushopt|target_value|t_sfence_done_cycles|t_sfence_done_ns|attempt1_value|attempt1_load_start_cycles|attempt1_load_done_cycles|attempt1_load_latency_cycles|attempt1_load_done_ns|success|success_attempt|success_value|success_load_start_cycles|success_load_done_cycles|success_load_latency_cycles|success_load_done_ns|sfence_to_success_load_cycles|sfence_to_success_load_ns)=" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"
