#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_hydrarpc_dedicated.sh [options]

Options:
  --outdir <dir>           Output directory.
  --binary <path>          gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>        Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>        Boot CPU type before the first m5 exit: KVM or ATOMIC. Default: KVM
  --atomic-checkpoint      Boot once with ATOMIC, save/reuse a boot checkpoint,
                           then restore directly with CPU_TYPE to run the guest workload.
  --checkpoint-root <dir>  Root directory for reusable boot checkpoints.
                           Default with --atomic-checkpoint: <outdir>/checkpoints
  --checkpoint-id <id>     Checkpoint bundle name under --checkpoint-root.
                           Default: atomic_n<num-cpus>
  --refresh-checkpoint     Regenerate the selected boot checkpoint bundle.
  --client-count <N>       Number of client processes. Default: 1
  --count-per-client <N>   Requests per client. Default: 30
  --window-size <N>        Max outstanding requests per client. Default: 16
  --slot-count <N>         Per-client ring depth. Default: 1024
  --req-bytes <N>          Request payload bytes. Default: 64
  --resp-bytes <N>         Response payload bytes. Default: 64
  --req-min-bytes <N>      Optional minimum request payload size for per-request uniform bins.
  --req-max-bytes <N>      Optional maximum request payload size for per-request uniform bins.
  --resp-min-bytes <N>     Optional minimum response payload size for per-request uniform bins.
  --resp-max-bytes <N>     Optional maximum response payload size for per-request uniform bins.
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
  --cxl-bridge-extra-latency <lat>
                           Extra host-side CXL bridge latency passed to gem5.
                           Default: 0ns
  --restore-checkpoint <dir>
                           Restore from an existing boot checkpoint and start
                           directly with CPU_TYPE cores.
  --guest-cmd <cmd>        Override the guest-side command launched after boot.
                           Default: run_hydrarpc_dedicated.sh with the current
                           microbenchmark arguments.
  --result-log-name <name> Guest-visible result log filename copied into OUTDIR.
                           Default: hydrarpc_dedicated.result.log
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
export DISK_IMAGE="${DISK_IMAGE:-${REPO_ROOT}/files/parsec.img}"

OUTDIR="output/hydrarpc_dedicated_run"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
ATOMIC_CHECKPOINT=0
CHECKPOINT_ROOT=""
CHECKPOINT_ID=""
REFRESH_CHECKPOINT=0
CLIENT_COUNT=1
COUNT_PER_CLIENT=30
WINDOW_SIZE=16
SLOT_COUNT=1024
REQ_BYTES=64
RESP_BYTES=64
REQ_MIN_BYTES=""
REQ_MAX_BYTES=""
RESP_MIN_BYTES=""
RESP_MAX_BYTES=""
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
CXL_BRIDGE_EXTRA_LATENCY="0ns"
RESTORE_CHECKPOINT=""
GUEST_CMD_OVERRIDE=""
RESULT_LOG_NAME="hydrarpc_dedicated.result.log"
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
    --atomic-checkpoint)
      ATOMIC_CHECKPOINT=1
      shift 1
      ;;
    --checkpoint-root)
      CHECKPOINT_ROOT="$2"
      shift 2
      ;;
    --checkpoint-id)
      CHECKPOINT_ID="$2"
      shift 2
      ;;
    --refresh-checkpoint)
      REFRESH_CHECKPOINT=1
      shift 1
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
    --req-min-bytes)
      REQ_MIN_BYTES="$2"
      shift 2
      ;;
    --req-max-bytes)
      REQ_MAX_BYTES="$2"
      shift 2
      ;;
    --resp-min-bytes)
      RESP_MIN_BYTES="$2"
      shift 2
      ;;
    --resp-max-bytes)
      RESP_MAX_BYTES="$2"
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
    --cxl-bridge-extra-latency)
      CXL_BRIDGE_EXTRA_LATENCY="$2"
      shift 2
      ;;
    --restore-checkpoint)
      RESTORE_CHECKPOINT="$2"
      shift 2
      ;;
    --guest-cmd)
      GUEST_CMD_OVERRIDE="$2"
      shift 2
      ;;
    --result-log-name)
      RESULT_LOG_NAME="$2"
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

if [[ "$ATOMIC_CHECKPOINT" -eq 1 ]]; then
  BOOT_CPU="ATOMIC"
fi

if [[ "$ATOMIC_CHECKPOINT" -eq 1 && -n "$RESTORE_CHECKPOINT" ]]; then
  echo "--atomic-checkpoint and --restore-checkpoint are mutually exclusive" >&2
  exit 1
fi

if [[ -n "$RESTORE_CHECKPOINT" ]]; then
  if [[ ! -d "$RESTORE_CHECKPOINT" ]]; then
    echo "restore checkpoint not found: $RESTORE_CHECKPOINT" >&2
    exit 1
  fi
  RESTORE_CHECKPOINT="$(cd "$RESTORE_CHECKPOINT" && pwd)"
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
    --label "gem5 binary for dedicated runner" \
    --monitor "configs/example/gem5_library/x86-cxl-type3-with-classic.py" \
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

if [[ "$SKIP_IMAGE_SETUP" -eq 0 || ( -z "$RESTORE_CHECKPOINT" && "$BOOT_CPU" == "KVM" ) ]]; then
  if ! sudo -n true >/dev/null 2>&1; then
    echo "dedicated runner requires passwordless sudo or a live sudo ticket" >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"

GEM5_LOG="$OUTDIR/gem5.stdout"
LOG_PATH="$OUTDIR/board.pc.com_1.device"
RESULT_LOG_PATH="$OUTDIR/$RESULT_LOG_NAME"

if [[ -z "$CHECKPOINT_ROOT" ]]; then
  CHECKPOINT_ROOT="$OUTDIR/checkpoints"
fi

mkdir -p "$CHECKPOINT_ROOT"
CHECKPOINT_ROOT="$(cd "$CHECKPOINT_ROOT" && pwd)"

if [[ -z "$CHECKPOINT_ID" ]]; then
  CHECKPOINT_ID="atomic_n${NUM_CPUS}"
fi

CHECKPOINT_BUNDLE="$CHECKPOINT_ROOT/$CHECKPOINT_ID"
CHECKPOINT_BOOT_OUTDIR="$CHECKPOINT_BUNDLE/boot_run"
CHECKPOINT_MARKER="$CHECKPOINT_BUNDLE/checkpoint.path"

gem5_launcher=("$BINARY")
if [[ -z "$RESTORE_CHECKPOINT" && "$BOOT_CPU" == "KVM" ]]; then
  gem5_launcher=(sudo -n "$BINARY")
fi

if [[ "$SKIP_IMAGE_SETUP" -eq 0 ]]; then
  HYDRARPC_GUEST_CFLAGS="$GUEST_CFLAGS" \
    bash tools/setup_hydrarpc_dedicated_disk_image.sh "$DISK_IMAGE"
fi

if [[ -n "$GUEST_CMD_OVERRIDE" ]]; then
  GUEST_CMD="$GUEST_CMD_OVERRIDE"
else
  GUEST_CMD="/home/test_code/run_hydrarpc_dedicated.sh --client-count ${CLIENT_COUNT} --count-per-client ${COUNT_PER_CLIENT} --window-size ${WINDOW_SIZE} --slot-count ${SLOT_COUNT} --req-bytes ${REQ_BYTES} --resp-bytes ${RESP_BYTES} --slow-client-count ${SLOW_CLIENT_COUNT} --slow-count-per-client ${SLOW_COUNT_PER_CLIENT} --slow-send-gap-ns ${SLOW_SEND_GAP_NS} --send-mode ${SEND_MODE} --send-gap-ns ${SEND_GAP_NS} --request-transfer-mode ${REQUEST_TRANSFER_MODE} --response-transfer-mode ${RESPONSE_TRANSFER_MODE} --cxl-node ${CXL_NODE} --server-cpu ${SERVER_CPU}"
  if [[ -n "$REQ_MIN_BYTES" || -n "$REQ_MAX_BYTES" ]]; then
    GUEST_CMD+=" --req-min-bytes ${REQ_MIN_BYTES:-$REQ_BYTES} --req-max-bytes ${REQ_MAX_BYTES:-$REQ_BYTES}"
  fi
  if [[ -n "$RESP_MIN_BYTES" || -n "$RESP_MAX_BYTES" ]]; then
    GUEST_CMD+=" --resp-min-bytes ${RESP_MIN_BYTES:-$RESP_BYTES} --resp-max-bytes ${RESP_MAX_BYTES:-$RESP_BYTES}"
  fi
fi
WORKLOAD_FILE="$OUTDIR/hydrarpc_dedicated.runscript"
RESTORE_WORKLOAD_FILE="$OUTDIR/hydrarpc_dedicated.restore.runscript"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "ready_tries=300\n"
  printf "while [ \"\$ready_tries\" -gt 0 ]; do\n"
  printf "  state=\$(systemctl is-system-running 2>/dev/null || true)\n"
  printf "  if systemctl is-active --quiet multi-user.target; then\n"
  printf "    case \"\$state\" in\n"
  printf "      running|degraded)\n"
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
    printf "/sbin/m5 exit\n"
  fi
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$WORKLOAD_FILE"

{
  printf "#!/bin/sh\n"
  printf "set -eu\n"
  printf "exec >/dev/ttyS0 2>&1\n"
  printf "set +e\n"
  printf "%s\n" "$GUEST_CMD"
  printf "rc=\$?\n"
  printf "printf 'guest_command_rc=%%s\\\\n' \"\$rc\"\n"
  printf "/sbin/m5 exit\n"
} > "$RESTORE_WORKLOAD_FILE"

resolve_checkpoint_dir() {
  local checkpoint_dir=""

  if [[ -f "$CHECKPOINT_MARKER" ]]; then
    checkpoint_dir="$(<"$CHECKPOINT_MARKER")"
    if [[ -d "$checkpoint_dir" ]]; then
      printf '%s\n' "$checkpoint_dir"
      return 0
    fi
  fi

  shopt -s nullglob
  local candidates=("$CHECKPOINT_BOOT_OUTDIR"/cpt.*)
  shopt -u nullglob

  if [[ "${#candidates[@]}" -eq 0 ]]; then
    return 1
  fi

  checkpoint_dir="$(printf '%s\n' "${candidates[@]}" | sort | tail -n 1)"
  if [[ -d "$checkpoint_dir" ]]; then
    printf '%s\n' "$checkpoint_dir"
    return 0
  fi

  return 1
}

write_checkpoint_marker() {
  local checkpoint_dir="$1"
  mkdir -p "$CHECKPOINT_BUNDLE"
  printf '%s\n' "$checkpoint_dir" > "$CHECKPOINT_MARKER"
}

ensure_boot_checkpoint() {
  local checkpoint_dir=""
  local boot_log=""
  local boot_terminal_port=0
  local boot_args=()

  if [[ "$ATOMIC_CHECKPOINT" -eq 0 ]]; then
    return 0
  fi

  if [[ "$REFRESH_CHECKPOINT" -eq 1 ]]; then
    rm -rf "$CHECKPOINT_BUNDLE"
  fi

  if checkpoint_dir="$(resolve_checkpoint_dir)"; then
    printf 'Reusing boot checkpoint: %s\n' "$checkpoint_dir"
    write_checkpoint_marker "$checkpoint_dir"
    return 0
  fi

  mkdir -p "$CHECKPOINT_BOOT_OUTDIR"
  boot_log="$CHECKPOINT_BOOT_OUTDIR/gem5.stdout"
  boot_terminal_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"

  boot_args=(
    -d "$CHECKPOINT_BOOT_OUTDIR"
    configs/example/gem5_library/x86-cxl-type3-with-classic.py
    --is_asic True
    --cpu_type "$CPU_TYPE"
    --boot_cpu ATOMIC
    --checkpoint-boot
    --num_cpus "$NUM_CPUS"
    --kernel "$KERNEL"
    --disk-image "$DISK_IMAGE"
    --terminal-port "$boot_terminal_port"
    --cxl-bridge-extra-latency "$CXL_BRIDGE_EXTRA_LATENCY"
  )

  if ! "$BINARY" "${boot_args[@]}" >"$boot_log" 2>&1; then
    echo "boot checkpoint generation failed" >&2
    echo "=== boot checkpoint gem5 log ===" >&2
    tail -n 200 "$boot_log" >&2 || true
    exit 1
  fi

  if ! checkpoint_dir="$(resolve_checkpoint_dir)"; then
    echo "boot checkpoint generation completed but no checkpoint directory was found" >&2
    exit 1
  fi

  write_checkpoint_marker "$checkpoint_dir"
  printf 'Created boot checkpoint: %s\n' "$checkpoint_dir"
}

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
  --num_cpus "$NUM_CPUS"
  --kernel "$KERNEL"
  --disk-image "$DISK_IMAGE"
  --terminal-port "$TERMINAL_PORT"
  --cxl-bridge-extra-latency "$CXL_BRIDGE_EXTRA_LATENCY"
)

if [[ "$ATOMIC_CHECKPOINT" -eq 1 ]]; then
  ensure_boot_checkpoint
  RESTORE_CHECKPOINT_DIR="$(<"$CHECKPOINT_MARKER")"
  gem5_args+=(
    --restore-checkpoint "$RESTORE_CHECKPOINT_DIR"
    --workload-file "$RESTORE_WORKLOAD_FILE"
  )
elif [[ -n "$RESTORE_CHECKPOINT" ]]; then
  gem5_args+=(
    --restore-checkpoint "$RESTORE_CHECKPOINT"
    --workload-file "$RESTORE_WORKLOAD_FILE"
  )
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
echo "=== Multi-client dedicated raw output ==="
rg -n "^(server_loop_start_ts_ns=|client_[0-9]+_req_[0-9]+_(client_req_start_ts_ns|client_resp_done_ts_ns|server_req_observe_ts_ns|server_exec_done_ts_ns|server_resp_done_ts_ns)=|guest_command_rc=|benchmark_rc=)" \
  "$RESULT_LOG_PATH" | sed 's/^[0-9]*://' || true
echo
echo "=== Multi-client dedicated summary ==="
python3 tools/summarize_hydrarpc_multiclient.py \
  --log "$RESULT_LOG_PATH" \
  --experiment dedicated \
  --client-count "$CLIENT_COUNT" \
  --count-per-client "$COUNT_PER_CLIENT" \
  --expected-total-requests "$EXPECTED_TOTAL_REQUESTS"
echo
if [[ "$ATOMIC_CHECKPOINT" -eq 1 ]]; then
  echo "Checkpoint bundle: $CHECKPOINT_BUNDLE"
fi
echo "Output dir: $OUTDIR"
