#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/create_hydrarpc_boot_checkpoint.sh [options]

Options:
  --mode <classic|ruby>    Full-system config family. Default: classic
  --outdir <dir>           Checkpoint output directory. Required.
  --binary <path>          gem5 binary. Default: build/X86/gem5.opt
  --boot-cpu <type>        Boot CPU type used to create the checkpoint.
                           Default: KVM
  --num-cpus <N>           Guest CPU count baked into the checkpoint.
                           Default: 34
  --kernel <path>          Kernel image. Default: repo-local files/vmlinux
  --disk-image <path>      Disk image. Default: repo-local files/parsec.img
  --terminal-port <N>      Terminal port. Default: auto-pick
  --skip-build             Skip scons build.
  --help                   Show this message.

This helper creates one boot checkpoint and then reuses it on subsequent
invocations via <outdir>/latest.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE="classic"
OUTDIR=""
BINARY="build/X86/gem5.opt"
BOOT_CPU="KVM"
NUM_CPUS=34
KERNEL="${REPO_ROOT}/files/vmlinux"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
TERMINAL_PORT=0
SKIP_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
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
    --boot-cpu)
      BOOT_CPU="$2"
      shift 2
      ;;
    --num-cpus)
      NUM_CPUS="$2"
      shift 2
      ;;
    --kernel)
      KERNEL="$2"
      shift 2
      ;;
    --disk-image)
      DISK_IMAGE="$2"
      shift 2
      ;;
    --terminal-port)
      TERMINAL_PORT="$2"
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

case "$MODE" in
  classic)
    CONFIG_PATH="configs/example/gem5_library/x86-cxl-type3-with-classic.py"
    ;;
  ruby)
    CONFIG_PATH="configs/example/gem5_library/x86-cxl-type3-with-ruby-local.py"
    ;;
  *)
    echo "Unsupported checkpoint mode: $MODE" >&2
    exit 1
    ;;
esac

if [[ -z "$OUTDIR" ]]; then
  echo "--outdir is required" >&2
  exit 1
fi

cd "$REPO_ROOT"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  scons "$BINARY" -j"$(nproc)"
else
  bash tools/check_gem5_binary_freshness.sh \
    --binary "$BINARY" \
    --label "gem5 binary for checkpoint mode=$MODE" \
    --monitor "$CONFIG_PATH" \
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

if [[ "$BOOT_CPU" == "KVM" ]]; then
  if ! sudo -n true >/dev/null 2>&1; then
    echo "checkpoint creation with KVM requires passwordless sudo or a live sudo ticket" >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"

if [[ -L "$OUTDIR/latest" || -d "$OUTDIR/latest" ]]; then
  if [[ -d "$OUTDIR/latest" ]]; then
    printf '%s\n' "$OUTDIR/latest"
    exit 0
  fi
fi

GEM5_LOG="$OUTDIR/gem5.stdout"
BOARD_LOG="$OUTDIR/board.pc.com_1.device"

gem5_launcher=("$BINARY")
if [[ "$BOOT_CPU" == "KVM" ]]; then
  gem5_launcher=(sudo -n "$BINARY")
fi

gem5_args=(
  -d "$OUTDIR"
  "$CONFIG_PATH"
  --is_asic True
  --cpu_type TIMING
  --boot_cpu "$BOOT_CPU"
  --num_cpus "$NUM_CPUS"
  --kernel "$KERNEL"
  --disk-image "$DISK_IMAGE"
  --checkpoint-boot
  --terminal-port "$TERMINAL_PORT"
)

if ! "${gem5_launcher[@]}" "${gem5_args[@]}" >"$GEM5_LOG" 2>&1; then
  echo "checkpoint creation failed" >&2
  echo "=== board log tail ===" >&2
  tail -n 80 "$BOARD_LOG" >&2 || true
  echo "=== gem5 log tail ===" >&2
  tail -n 80 "$GEM5_LOG" >&2 || true
  exit 1
fi

checkpoint_dir="$(find "$OUTDIR" -maxdepth 1 -mindepth 1 -type d -name 'cpt.*' | sort | tail -n 1)"
if [[ -z "$checkpoint_dir" ]]; then
  echo "checkpoint directory not found under $OUTDIR" >&2
  echo "=== board log tail ===" >&2
  tail -n 80 "$BOARD_LOG" >&2 || true
  echo "=== gem5 log tail ===" >&2
  tail -n 80 "$GEM5_LOG" >&2 || true
  exit 1
fi

ln -sfn "$(basename "$checkpoint_dir")" "$OUTDIR/latest"
printf '%s\n' "$checkpoint_dir" >"$OUTDIR/checkpoint.path"
printf '%s\n' "$OUTDIR/latest"
