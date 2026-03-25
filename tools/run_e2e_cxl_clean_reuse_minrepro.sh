#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_cxl_clean_reuse_minrepro.sh [options]

Options:
  --outdir <dir>        Output directory. Default: output/cxl_clean_reuse_minrepro
  --binary <path>       gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>     Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>     Boot CPU type: KVM or ATOMIC. Default: KVM
  --num-cpus <N>        Number of guest CPUs. Default: 2
  --count <N>           Request count. Default: 40
  --slots <N>           Ring slot count. Default: 32
  --inflight <N>        Max in-flight requests. Default: 2
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

OUTDIR="output/cxl_clean_reuse_minrepro"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
NUM_CPUS=2
COUNT=40
SLOTS=32
INFLIGHT=2
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
    --count)
      COUNT="$2"
      shift 2
      ;;
    --slots)
      SLOTS="$2"
      shift 2
      ;;
    --inflight)
      INFLIGHT="$2"
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

B64_SRC="$(base64 -w0 tools/cxl_clean_reuse_minrepro.c)"
GUEST_CMD="numactl -N 0 -m 1 /tmp/cxl_clean_reuse_minrepro --count ${COUNT} --slots ${SLOTS} --inflight ${INFLIGHT} --client-cpu ${CLIENT_CPU} --server-cpu ${SERVER_CPU}"
if [[ "$DUMP_LAYOUT" -eq 1 ]]; then
  GUEST_CMD+=" --dump-layout"
fi
WORKLOAD_CMD="printf '%s' \"$B64_SRC\" | base64 -d > /tmp/cxl_clean_reuse_minrepro.c; gcc -O3 -pthread /tmp/cxl_clean_reuse_minrepro.c -o /tmp/cxl_clean_reuse_minrepro; m5 exit; ${GUEST_CMD}; m5 exit"

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
echo "=== CXL clean-reuse minrepro output ==="
rg -n "^(layout_|correctness_fail|minrepro_done_count=)" \
  "$OUTDIR/board.pc.com_1.device" | sed 's/^[0-9]*://'
echo
echo "Output dir: $OUTDIR"
