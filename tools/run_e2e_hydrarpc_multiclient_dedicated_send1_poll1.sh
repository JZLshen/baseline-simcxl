#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_hydrarpc_multiclient_dedicated_send1_poll1.sh [options]

Options:
  --outdir <dir>           Output directory.
  --binary <path>          gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>        Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>        Boot CPU type before the first m5 exit: KVM or ATOMIC. Default: KVM
  --client-count <N>       Number of client processes. Default: 1
  --count-per-client <N>   Requests per client. Default: 8
  --window-size <N>        Max outstanding requests per client. Default: 16
  --slot-count <N>         Per-client ring depth. Default: min(window-size, count-per-client)
  --slow-client-count <N>  Mark the first N client ids as slow. Default: 0
  --slow-count-per-client <N>
                           Request count used by each slow client.
  --slow-send-gap-ns <N>   Uniform inter-request gap used by slow clients.
  --send-mode <mode>       Client send pacing: greedy, uniform, staggered, or uneven. Default: greedy
  --send-gap-ns <N>        Inter-request gap used by all paced modes. Default: 0
  --record-breakdown <m>   Guest instrumentation level: none or basic. Default: none
  --cxl-node <N>           NUMA node used for CXL mappings inside guest. Default: 1
  --num-cpus <N>           Guest CPU count. Default: client-count + 2
  --server-cpu <N>         Server CPU id. Default: client-count
  --terminal-port <N>      Host TCP port reserved for the guest COM1 listener. Default: auto-pick
  --guest-cflags <flags>   Host gcc flags used to build the injected guest binary.
                           Default: -O2 -Wall -static -g -pthread
  --skip-image-setup       Reuse the dedicated guest binary already injected into the disk image.
  --debug-flags <list>     Optional gem5 debug flags.
  --skip-build             Skip scons build.
  --help                   Show this message.

Fixed resources:
  - kernel: repo-local files/vmlinux
  - disk:   repo-local files/parsec.img

The dedicated guest binary is built on the host, injected into the disk image,
then launched from a small readfile workload after the first m5 exit switches
from BOOT_CPU to CPU_TYPE.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KERNEL="${REPO_ROOT}/files/vmlinux"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"

OUTDIR="output/hydrarpc_multiclient_dedicated_send1_poll1"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
CLIENT_COUNT=1
COUNT_PER_CLIENT=8
WINDOW_SIZE=16
SLOT_COUNT=0
SLOW_CLIENT_COUNT=0
SLOW_COUNT_PER_CLIENT=0
SLOW_SEND_GAP_NS=0
SEND_MODE="greedy"
SEND_GAP_NS=0
RECORD_BREAKDOWN="none"
CXL_NODE=1
NUM_CPUS=0
SERVER_CPU=-1
TERMINAL_PORT=0
GUEST_CFLAGS=""
SKIP_IMAGE_SETUP=0
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
    --client-count)
      CLIENT_COUNT="$2"
      shift 2
      ;;
    --count-per-client)
      COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --window-size)
      WINDOW_SIZE="$2"
      shift 2
      ;;
    --slot-count)
      SLOT_COUNT="$2"
      shift 2
      ;;
    --slow-client-count)
      SLOW_CLIENT_COUNT="$2"
      shift 2
      ;;
    --slow-count-per-client)
      SLOW_COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --slow-send-gap-ns)
      SLOW_SEND_GAP_NS="$2"
      shift 2
      ;;
    --send-mode)
      SEND_MODE="$2"
      shift 2
      ;;
    --send-gap-ns)
      SEND_GAP_NS="$2"
      shift 2
      ;;
    --record-breakdown)
      RECORD_BREAKDOWN="$2"
      shift 2
      ;;
    --cxl-node)
      CXL_NODE="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --server-cpu)
      SERVER_CPU="$2"
      shift 2
      ;;
    --terminal-port)
      TERMINAL_PORT="$2"
      shift 2
      ;;
    --guest-cflags)
      GUEST_CFLAGS="$2"
      shift 2
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
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

if [[ -z "$GUEST_CFLAGS" ]]; then
  GUEST_CFLAGS="-O2 -Wall -static -g -pthread"
fi

if [[ "$NUM_CPUS" -eq 0 ]]; then
  # Leave one extra guest CPU so the parent barrier/waitpid process does not
  # contend with the pinned client/server workers at higher client counts.
  # Keep at least four guest CPUs; odd-sized auto layouts have not been stable.
  NUM_CPUS=$((CLIENT_COUNT + 2))
  if [[ "$NUM_CPUS" -lt 4 ]]; then
    NUM_CPUS=4
  fi
  if (( NUM_CPUS % 2 != 0 )); then
    NUM_CPUS=$((NUM_CPUS + 1))
  fi
fi

if [[ "$SERVER_CPU" -lt 0 ]]; then
  SERVER_CPU="$CLIENT_COUNT"
fi

if [[ "$SLOT_COUNT" -eq 0 ]]; then
  SLOT_COUNT="$WINDOW_SIZE"
  if [[ "$SLOT_COUNT" -gt "$COUNT_PER_CLIENT" ]]; then
    SLOT_COUNT="$COUNT_PER_CLIENT"
  fi
fi

EXPECTED_TOTAL_REQUESTS=$((CLIENT_COUNT * COUNT_PER_CLIENT))
if [[ "$SLOW_CLIENT_COUNT" -gt 0 ]]; then
  EXPECTED_TOTAL_REQUESTS=$((EXPECTED_TOTAL_REQUESTS - SLOW_CLIENT_COUNT * (COUNT_PER_CLIENT - SLOW_COUNT_PER_CLIENT)))
fi

cd "$REPO_ROOT"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
fi

if [[ "$TERMINAL_PORT" -eq 0 ]]; then
  TERMINAL_PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
fi

if [[ "$BOOT_CPU" == "KVM" || "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  if ! sudo -n true >/dev/null 2>&1; then
    echo "dedicated runner requires passwordless sudo or a live sudo ticket" >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"

GEM5_LOG="$OUTDIR/gem5.stdout"
LOG_PATH="$OUTDIR/board.pc.com_1.device"

gem5_launcher=("$BINARY")
if [[ "$BOOT_CPU" == "KVM" ]]; then
  gem5_launcher=(sudo -n "$BINARY")
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
    bash tools/setup_hydrarpc_multiclient_dedicated_disk_image.sh "$DISK_IMAGE"
fi

GUEST_CMD="/home/test_code/run_hydrarpc_multiclient_dedicated_send1_poll1.sh --client-count ${CLIENT_COUNT} --count-per-client ${COUNT_PER_CLIENT} --window-size ${WINDOW_SIZE} --slot-count ${SLOT_COUNT} --slow-client-count ${SLOW_CLIENT_COUNT} --slow-count-per-client ${SLOW_COUNT_PER_CLIENT} --slow-send-gap-ns ${SLOW_SEND_GAP_NS} --send-mode ${SEND_MODE} --send-gap-ns ${SEND_GAP_NS} --record-breakdown ${RECORD_BREAKDOWN} --cxl-node ${CXL_NODE} --server-cpu ${SERVER_CPU}"
WORKLOAD_FILE="$OUTDIR/hydrarpc_multiclient_dedicated_send1_poll1.runscript"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "/sbin/m5 exit\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$WORKLOAD_FILE"

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
  --terminal-port "$TERMINAL_PORT"
)

if ! "${gem5_launcher[@]}" "${gem5_args[@]}" >"$GEM5_LOG" 2>&1; then
  echo "gem5 run failed" >&2
  echo "=== gem5 log ===" >&2
  tail -n 200 "$GEM5_LOG" >&2 || true
  exit 1
fi

echo
echo "=== Multi-client dedicated raw output ==="
rg -n "^(client_[0-9]+_req_[0-9]+_(start_ns|end_ns)|guest_command_rc=|benchmark_rc=)" \
  "$LOG_PATH" | sed 's/^[0-9]*://' || true
echo
echo "=== Multi-client dedicated summary ==="
python3 tools/summarize_hydrarpc_multiclient.py \
  --log "$LOG_PATH" \
  --experiment multiclient_dedicated_send1_poll1 \
  --client-count "$CLIENT_COUNT" \
  --count-per-client "$COUNT_PER_CLIENT" \
  --expected-total-requests "$EXPECTED_TOTAL_REQUESTS"
echo
echo "Output dir: $OUTDIR"
