#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_hydrarpc_baseline.sh [options]

Options:
  --outdir <dir>        Output directory. Default: output/hydrarpc_baseline_w0_r1
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>     Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>     Boot CPU type: KVM or ATOMIC. Default: KVM
  --num-cpus <N>        Number of guest CPUs. Default: 2
  --window-mode <N>     Window size as a non-negative integer. Default: 0
  --slot-count <N>      Slot ring depth for windowed mode. Default: 32
  --cxl-node <N>        NUMA node used for CXL mappings inside guest. Default: 1
  --count <N>           Request count. Default: 1
  --client-cpu <N>      Client CPU id. Default: 0
  --server-cpu <N>      Server CPU id. Default: 1
  --dump-layout         Print guest VA/PA layout for slot 0 and last slot.
  --debug-flags <list>  Optional gem5 debug flags.
  --skip-build          Skip scons build.
  --help                Show this message.

Fixed resources:
  - kernel: repo-local files/vmlinux
  - disk:   repo-local files/parsec.img
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KERNEL="${REPO_ROOT}/files/vmlinux"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"

OUTDIR="output/hydrarpc_baseline_w0_r1"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=2
WINDOW_MODE="0"
SLOT_COUNT=32
CXL_NODE=1
COUNT=1
CLIENT_CPU=0
SERVER_CPU=1
DUMP_LAYOUT=0
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
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --window-mode)
      WINDOW_MODE="$2"
      shift 2
      ;;
    --slot-count)
      SLOT_COUNT="$2"
      shift 2
      ;;
    --cxl-node)
      CXL_NODE="$2"
      shift 2
      ;;
    --count)
      COUNT="$2"
      shift 2
      ;;
    --client-cpu)
      CLIENT_CPU="$2"
      shift 2
      ;;
    --server-cpu)
      SERVER_CPU="$2"
      shift 2
      ;;
    --dump-layout)
      DUMP_LAYOUT=1
      shift 1
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
OUTDIR="$(cd "$OUTDIR" && pwd)"

HOST_BENCH_BIN="$OUTDIR/hydrarpc_baseline_rtc.hostbin"
gcc -O1 -pthread -static tools/hydrarpc_baseline_rtc.c -o "$HOST_BENCH_BIN"
BINARY_B64="$(base64 -w 76 "$HOST_BENCH_BIN")"
GUEST_CMD="numactl -N 0 -m 0 /tmp/hydrarpc_baseline_rtc --count ${COUNT} --window-mode ${WINDOW_MODE} --slot-count ${SLOT_COUNT} --cxl-node ${CXL_NODE} --client-cpu ${CLIENT_CPU} --server-cpu ${SERVER_CPU}"
if [[ "$DUMP_LAYOUT" -eq 1 ]]; then
  GUEST_CMD+=" --dump-layout"
fi
WORKLOAD_FILE="$OUTDIR/hydrarpc_baseline_rtc.runscript"
{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "cat <<'EOF_BIN' | base64 -d > /tmp/hydrarpc_baseline_rtc\n"
  printf "%s\n" "$BINARY_B64"
  printf "EOF_BIN\n"
  printf "chmod +x /tmp/hydrarpc_baseline_rtc\n"
  printf "printf 'guest_stage=pre_switch\\n'\n"
  printf "/sbin/m5 exit\n"
  printf "printf 'guest_stage=post_switch\\n'\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$WORKLOAD_FILE"

gem5_launcher=("$BINARY")
if [[ "$BOOT_CPU" == "KVM" ]]; then
  if ! sudo -n true >/dev/null 2>&1; then
    echo "KVM boot requires passwordless sudo or a live sudo ticket" >&2
    exit 1
  fi
  gem5_launcher=(sudo -n "$BINARY")
fi

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
  --workload-file "$WORKLOAD_FILE"
)

"${gem5_launcher[@]}" "${gem5_args[@]}"

echo
echo "=== HydraRPC baseline output ==="
rg -n "^(req_[0-9]+_(start_ns|end_ns)|req_[0-9]+_correctness_fail)" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"
