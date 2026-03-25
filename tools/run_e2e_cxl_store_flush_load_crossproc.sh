#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_cxl_store_flush_load_crossproc.sh --kernel <path> --disk-image <path> [options]

Required:
  --kernel <path>       Linux kernel image (vmlinux).
  --disk-image <path>   Disk image for full-system boot.

Options:
  --outdir <dir>        Output directory. Default: output/e2e_cxl_store_flush_load_crossproc
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>     Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>     Boot CPU type: KVM or ATOMIC. Default: KVM
  --num-cpus <N>        Number of guest CPUs. Default: 2
  --debug-flags <list>  Optional gem5 debug flags, e.g. Bridge,CXLMemCtrl.
  --skip-build          Skip scons build.
  --help                Show this message.

Experiment:
  - Writer process on CPU0: store + clflushopt + sfence
  - Reader process on CPU1: clflush + load loop until new value observed
  - Test process is run with numactl -N 0 -m 1 so data pages come from node1.
EOF
}

KERNEL=""
DISK_IMAGE=""
OUTDIR="output/e2e_cxl_store_flush_load_crossproc"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=2
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
    --num-cpus)
      NUM_CPUS="$2"
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

B64_SRC="$(base64 -w0 tools/store_flush_load_crossproc.c)"
WORKLOAD_CMD="numactl -H; printf '%s' \"$B64_SRC\" | base64 -d > /tmp/store_flush_load_crossproc.c; gcc -O2 /tmp/store_flush_load_crossproc.c -o /tmp/store_flush_load_crossproc; m5 exit; m5 resetstats; numactl -N 0 -m 1 /tmp/store_flush_load_crossproc; m5 dumpstats; m5 exit"

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
  --num_cpus "$NUM_CPUS"
  --kernel "$KERNEL"
  --disk-image "$DISK_IMAGE"
  --workload-cmd "$WORKLOAD_CMD"
)

"$BINARY" "${gem5_args[@]}"

echo
echo "=== Cross-process sfence/load visibility metrics ==="
rg -n "^(online_cpus|cpu_mhz|used_clflushopt|writer_cpu|reader_cpu|writer_pid|reader_pid|target_value|writer_store_start_cycles|writer_sfence_done_cycles|writer_sfence_done_ns|reader_first_value|reader_loop_start_cycles|reader_loop_start_ns|reader_first_load_start_cycles|reader_first_load_done_cycles|reader_first_load_latency_cycles|reader_first_load_done_ns|reader_success|reader_success_attempt|reader_total_attempts|reader_success_value|reader_success_load_start_cycles|reader_success_load_done_cycles|reader_success_load_latency_cycles|reader_success_load_done_ns|reader_loop_to_reader_success_load_cycles|reader_loop_to_reader_success_load_ns|crossproc_store_to_reader_success_load_cycles|crossproc_store_to_reader_success_load_ns|crossproc_sfence_to_reader_success_load_cycles|crossproc_sfence_to_reader_success_load_ns)=" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"

if [[ -f "$OUTDIR/cxl_trace.log" ]]; then
  echo
  echo "=== CXL trace transaction summary (heuristic) ==="
  python3 tools/parse_crosscore_cxl_trace.py "$OUTDIR/cxl_trace.log" || true
fi
