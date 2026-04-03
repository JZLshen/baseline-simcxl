#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_hydrarpc_dedicated_coherent.sh [options]

Options:
  --outdir <dir>           Output directory.
  --binary <path>          gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>        Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>        Boot CPU type before the first m5 exit: KVM or ATOMIC. Default: KVM
  --client-count <N>       Number of client processes. Default: 1
  --count-per-client <N>   Requests per client. Default: 30
  --window-size <N>        Max outstanding requests per client. Default: 16
  --slot-count <N>         Per-client ring depth. Default: 1024
  --req-bytes <N>          Request payload bytes. Default: 64
  --resp-bytes <N>         Response payload bytes. Default: 64
  --slow-client-count <N>  Mark the first N client ids as slow. Default: 0
  --slow-count-per-client <N>
                           Request count used by each slow client.
  --slow-send-gap-ns <N>   Uniform inter-request gap used by slow clients.
  --send-mode <mode>       Client send pacing: greedy, uniform, staggered, or uneven. Default: greedy
  --send-gap-ns <N>        Inter-request gap used by all paced modes. Default: 0
  --request-transfer-mode <mode>
                           Request publish mode: staging or direct. Default: staging
  --response-transfer-mode <mode>
                           Response publish mode: staging or direct. Default: staging
  --cxl-node <N>           NUMA node used for CXL mappings inside guest. Default: 1
  --num-cpus <N>           Guest CPU count. Default: client-count + 2
  --server-cpu <N>         Server CPU id. Default: client-count
  --restore-checkpoint <dir>
                           Restore from an existing boot checkpoint and start
                           directly with CPU_TYPE cores.
  --restore-dispatch <mode>
                           Restored workload dispatch: readfile or guest-file.
                           Default: readfile
  --guest-file-disk-image <path>
                           Reusable private disk image used by guest-file
                           restore dispatch. When omitted, guest-file mode
                           creates a per-outdir copy.
  --terminal-port <N>      Host TCP port reserved for the guest COM1 listener. Default: auto-pick
  --guest-cflags <flags>   Host gcc flags used to build the injected guest binary.
                           Default: -O2 -Wall -static -g -pthread
  --guest-trace            Enable extra guest-side stage traces on ttyS0.
  --skip-image-setup       Reuse the coherent dedicated guest binary already injected into the disk image.
  --debug-flags <list>     Optional gem5 debug flags.
  --skip-build             Skip scons build.
  --help                   Show this message.

Fixed resources:
  - kernel: repo-local files/vmlinux
  - disk:   repo-local files/parsec.img

The coherent dedicated guest binary is built on the host, injected into the
disk image, then launched from a small readfile workload after the first m5
exit switches from BOOT_CPU to CPU_TYPE. Ruby restore can hang in m5 readfile,
so guest-file dispatch instead runs a guest-resident autorun wrapper from a
private disk image copy.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
KERNEL="${REPO_ROOT}/files/vmlinux"
export DISK_IMAGE="${DISK_IMAGE:-${REPO_ROOT}/files/parsec.img}"

OUTDIR="output/hydrarpc_dedicated_coherent_run"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
CLIENT_COUNT=1
COUNT_PER_CLIENT=30
WINDOW_SIZE=16
SLOT_COUNT=1024
REQ_BYTES=64
RESP_BYTES=64
SLOW_CLIENT_COUNT=0
SLOW_COUNT_PER_CLIENT=0
SLOW_SEND_GAP_NS=0
SEND_MODE="greedy"
SEND_GAP_NS=0
REQUEST_TRANSFER_MODE="staging"
RESPONSE_TRANSFER_MODE="staging"
CXL_NODE=1
NUM_CPUS=0
SERVER_CPU=-1
RESTORE_CHECKPOINT=""
RESTORE_DISPATCH="readfile"
GUEST_FILE_DISK_IMAGE=""
TERMINAL_PORT=0
GUEST_CFLAGS=""
GUEST_TRACE=0
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
    --req-bytes)
      REQ_BYTES="$2"
      shift 2
      ;;
    --resp-bytes)
      RESP_BYTES="$2"
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
    --request-transfer-mode)
      REQUEST_TRANSFER_MODE="$2"
      shift 2
      ;;
    --response-transfer-mode)
      RESPONSE_TRANSFER_MODE="$2"
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
    --restore-checkpoint)
      RESTORE_CHECKPOINT="$2"
      shift 2
      ;;
    --restore-dispatch)
      RESTORE_DISPATCH="$2"
      shift 2
      ;;
    --guest-file-disk-image)
      GUEST_FILE_DISK_IMAGE="$2"
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
    --guest-trace)
      GUEST_TRACE=1
      shift 1
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

if [[ -n "$RESTORE_CHECKPOINT" ]]; then
  if [[ ! -d "$RESTORE_CHECKPOINT" ]]; then
    echo "restore checkpoint not found: $RESTORE_CHECKPOINT" >&2
    exit 1
  fi
  RESTORE_CHECKPOINT="$(cd "$RESTORE_CHECKPOINT" && pwd)"
fi

case "$RESTORE_DISPATCH" in
  readfile|guest-file)
    ;;
  *)
    echo "unsupported --restore-dispatch: $RESTORE_DISPATCH" >&2
    exit 1
    ;;
esac

if [[ "$RESTORE_DISPATCH" == "guest-file" && -z "$RESTORE_CHECKPOINT" ]]; then
  echo "--restore-dispatch guest-file requires --restore-checkpoint" >&2
  exit 1
fi

EXPECTED_TOTAL_REQUESTS=$((CLIENT_COUNT * COUNT_PER_CLIENT))
if [[ "$SLOW_CLIENT_COUNT" -gt 0 ]]; then
  EXPECTED_TOTAL_REQUESTS=$((EXPECTED_TOTAL_REQUESTS - SLOW_CLIENT_COUNT * (COUNT_PER_CLIENT - SLOW_COUNT_PER_CLIENT)))
fi

cd "$REPO_ROOT"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "$BINARY" \
    --label "gem5 binary for coherent dedicated runner" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-ruby-local.py" \
    --monitor "src/python/gem5/components/boards/x86_board.py"
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

if [[ "$SKIP_IMAGE_SETUP" -eq 0 || ( -z "$RESTORE_CHECKPOINT" && "$BOOT_CPU" == "KVM" ) || "$RESTORE_DISPATCH" == "guest-file" ]]; then
  if ! sudo -n true >/dev/null 2>&1; then
    echo "coherent dedicated runner requires passwordless sudo or a live sudo ticket" >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"

if [[ -n "$GUEST_FILE_DISK_IMAGE" ]]; then
  mkdir -p "$(dirname "$GUEST_FILE_DISK_IMAGE")"
  GUEST_FILE_DISK_IMAGE="$(readlink -f "$GUEST_FILE_DISK_IMAGE")"
fi

GEM5_LOG="$OUTDIR/gem5.stdout"
LOG_PATH="$OUTDIR/board.pc.com_1.device"
RESULT_LOG_PATH="$OUTDIR/hydrarpc_dedicated_coherent.result.log"
RUNTIME_DISK_IMAGE="$DISK_IMAGE"

gem5_launcher=("$BINARY")
if [[ -z "$RESTORE_CHECKPOINT" && "$BOOT_CPU" == "KVM" ]]; then
  gem5_launcher=(sudo -n "$BINARY")
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 && "$RESTORE_DISPATCH" != "guest-file" ]]; then
  HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
    bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh "$DISK_IMAGE"
fi

GUEST_CMD="/home/test_code/run_hydrarpc_dedicated_coherent.sh --client-count ${CLIENT_COUNT} --count-per-client ${COUNT_PER_CLIENT} --window-size ${WINDOW_SIZE} --slot-count ${SLOT_COUNT} --req-bytes ${REQ_BYTES} --resp-bytes ${RESP_BYTES} --slow-client-count ${SLOW_CLIENT_COUNT} --slow-count-per-client ${SLOW_COUNT_PER_CLIENT} --slow-send-gap-ns ${SLOW_SEND_GAP_NS} --send-mode ${SEND_MODE} --send-gap-ns ${SEND_GAP_NS} --request-transfer-mode ${REQUEST_TRANSFER_MODE} --response-transfer-mode ${RESPONSE_TRANSFER_MODE} --cxl-node ${CXL_NODE} --server-cpu ${SERVER_CPU}"
GUEST_RESTORE_CMD="/home/test_code/hydrarpc_dedicated_coherent --client-count ${CLIENT_COUNT} --count-per-client ${COUNT_PER_CLIENT} --window-size ${WINDOW_SIZE} --slot-count ${SLOT_COUNT} --req-bytes ${REQ_BYTES} --resp-bytes ${RESP_BYTES} --slow-client-count ${SLOW_CLIENT_COUNT} --slow-count-per-client ${SLOW_COUNT_PER_CLIENT} --slow-send-gap-ns ${SLOW_SEND_GAP_NS} --send-mode ${SEND_MODE} --send-gap-ns ${SEND_GAP_NS} --request-transfer-mode ${REQUEST_TRANSFER_MODE} --response-transfer-mode ${RESPONSE_TRANSFER_MODE} --cxl-node ${CXL_NODE} --server-cpu ${SERVER_CPU}"
if [[ "$GUEST_TRACE" -eq 1 ]]; then
  GUEST_CMD="env HYDRARPC_TRACE=1 ${GUEST_CMD}"
  GUEST_RESTORE_CMD="env HYDRARPC_TRACE=1 ${GUEST_RESTORE_CMD}"
fi
WORKLOAD_FILE="$OUTDIR/hydrarpc_dedicated_coherent.runscript"
RESTORE_WORKLOAD_FILE="$OUTDIR/hydrarpc_dedicated_coherent.restore.runscript"
RESTORE_AUTORUN_CMD_FILE="$OUTDIR/hydrarpc_dedicated_coherent.restore.autorun.sh"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "printf 'guest_workload_bootstrap_enter\\n'\n"
  printf "ready_tries=300\n"
  printf "while [ \"\$ready_tries\" -gt 0 ]; do\n"
  printf "  state=\$(systemctl is-system-running 2>/dev/null || true)\n"
  printf "  if systemctl is-active --quiet multi-user.target; then\n"
  printf "    case \"\$state\" in\n"
  printf "      running|degraded)\n"
  printf "        printf 'guest_workload_ready state=%%s\\n' \"\$state\"\n"
  printf "        break\n"
  printf "        ;;\n"
  printf "    esac\n"
  printf "  fi\n"
  printf "  ready_tries=\$((ready_tries - 1))\n"
  printf "  if [ \"\$ready_tries\" -eq 0 ]; then\n"
  printf "    printf 'guest_ready_timeout state=%%s\\n' \"\$state\"\n"
  printf "    exit 1\n"
  printf "  fi\n"
  printf "  sleep 1\n"
  printf "done\n"
  if [[ -z "$RESTORE_CHECKPOINT" ]]; then
    printf "printf 'guest_workload_pre_switch_exit\\n'\n"
    printf "/sbin/m5 exit\n"
    printf "printf 'guest_workload_post_switch_resume\\n'\n"
  fi
  printf "printf 'guest_workload_before_cmd\\n'\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_workload_after_cmd rc=%%s\\n' \"\$rc\"\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$WORKLOAD_FILE"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "printf 'guest_restore_workload_start\\n'\n"
  printf "printf 'guest_restore_workload_before_cmd\\n'\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_restore_workload_after_cmd rc=%%s\\n' \"\$rc\"\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$RESTORE_WORKLOAD_FILE"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "printf 'guest_restore_cmd_start\\n'\n"
  printf "sync\n"
  printf "echo 3 > /proc/sys/vm/drop_caches || true\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_RESTORE_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_restore_cmd_after_run rc=%%s\\\\n' \"\$rc\"\n"
  printf "printf 'guest_restore_cmd_rc=%%s\\\\n' \"\$rc\"\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$RESTORE_AUTORUN_CMD_FILE"

chmod 755 "$WORKLOAD_FILE" "$RESTORE_WORKLOAD_FILE" "$RESTORE_AUTORUN_CMD_FILE"

prepare_guest_file_restore_image() {
  local image_copy="$OUTDIR/parsec.img"

  if [[ -n "$GUEST_FILE_DISK_IMAGE" ]]; then
    image_copy="$GUEST_FILE_DISK_IMAGE"
  else
    rm -f "$image_copy"
  fi

  if [[ ! -f "$image_copy" ]]; then
    cp --reflink=auto "$DISK_IMAGE" "$image_copy"
  fi

  if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
    if [[ -n "$GUEST_CFLAGS" ]]; then
      env HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
        bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh "$image_copy"
    else
      bash tools/setup_hydrarpc_dedicated_coherent_disk_image.sh "$image_copy"
    fi
  fi

  bash tools/install_hydrarpc_restore_autorun_in_image.sh \
    "$image_copy" \
    "$RESTORE_AUTORUN_CMD_FILE"

  RUNTIME_DISK_IMAGE="$image_copy"
}

if [[ "$RESTORE_DISPATCH" == "guest-file" ]]; then
  prepare_guest_file_restore_image
fi

gem5_args=(
  -d "$OUTDIR"
)

if [[ -n "$DEBUG_FLAGS" ]]; then
  gem5_args+=(--debug-flags="$DEBUG_FLAGS" --debug-file=cxl_trace.log)
fi

gem5_args+=(
  configs/example/gem5_library/x86-cxl-type3-with-ruby-local.py
  --is_asic True
  --cpu_type "$CPU_TYPE"
  --num_cpus "$NUM_CPUS"
  --kernel "$KERNEL"
  --disk-image "$RUNTIME_DISK_IMAGE"
  --terminal-port "$TERMINAL_PORT"
)

if [[ -n "$RESTORE_CHECKPOINT" ]]; then
  if [[ "$RESTORE_DISPATCH" == "guest-file" ]]; then
    gem5_args+=(
      --restore-checkpoint "$RESTORE_CHECKPOINT"
      --disable-workload
    )
  else
    gem5_args+=(
      --restore-checkpoint "$RESTORE_CHECKPOINT"
      --workload-file "$RESTORE_WORKLOAD_FILE"
    )
  fi
else
  gem5_args+=(
    --boot_cpu "$BOOT_CPU"
    --workload-file "$WORKLOAD_FILE"
  )
fi

if ! "${gem5_launcher[@]}" "${gem5_args[@]}" >"$GEM5_LOG" 2>&1; then
  echo "gem5 run failed" >&2
  echo "=== gem5 log ===" >&2
  tail -n 200 "$GEM5_LOG" >&2 || true
  exit 1
fi

if [[ ! -f "$RESULT_LOG_PATH" ]]; then
  echo "missing result log: $RESULT_LOG_PATH" >&2
  echo "=== board log tail ===" >&2
  tail -n 80 "$LOG_PATH" >&2 || true
  echo "=== gem5 log tail ===" >&2
  tail -n 80 "$GEM5_LOG" >&2 || true
  exit 1
fi

echo
echo "=== Multi-client dedicated coherent raw output ==="
rg -n "^(server_loop_start_ts_ns=|client_[0-9]+_req_[0-9]+_(client_req_start_ts_ns|client_resp_done_ts_ns|server_req_observe_ts_ns|server_exec_done_ts_ns|server_resp_done_ts_ns)=|guest_command_rc=|benchmark_rc=)" \
  "$RESULT_LOG_PATH" | sed 's/^[0-9]*://' || true
echo
echo "=== Multi-client dedicated coherent summary ==="
python3 tools/summarize_hydrarpc_multiclient.py \
  --log "$RESULT_LOG_PATH" \
  --experiment dedicated_coherent \
  --client-count "$CLIENT_COUNT" \
  --count-per-client "$COUNT_PER_CLIENT" \
  --expected-total-requests "$EXPECTED_TOTAL_REQUESTS"
echo
echo "Output dir: $OUTDIR"
