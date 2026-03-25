#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_e2e_write_size_sweep.sh [options]

Options:
  --outdir <dir>          Output directory. Default: output/write_size_sweep
  --binary <path>         gem5 binary. Default: build/X86/gem5.opt
  --kernel <path>         Kernel image. Default: files/vmlinux
  --disk-image <path>     Disk image. Default: files/parsec.img
  --cpu-type <type>       Switch CPU type: TIMING or O3. Default: TIMING
  --boot-cpu <type>       Boot CPU type: KVM or ATOMIC. Default: KVM
  --cpu <N>               Guest CPU id used by the benchmark. Default: 0
  --mode <all|dram|cxl>   Which memory mode(s) to run. Default: all
  --min-size <bytes>      Optional lower bound passed to the guest workload.
  --max-size <bytes>      Optional upper bound passed to the guest workload.
  --skip-build            Skip scons build.
  --help                  Show this message.

Notes:
  - DRAM path: numactl -N 0 -m 0
  - CXL path:  numactl -N 0 -m 1
  - The guest workload measures:
      1) latency: write + clflushopt(all touched lines) + sfence
      2) bandwidth: batched writes with per-line clflushopt and one final sfence
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUTDIR="output/write_size_sweep"
BINARY="build/X86/gem5.opt"
KERNEL="${REPO_ROOT}/files/vmlinux"
DISK_IMAGE="${REPO_ROOT}/files/parsec.img"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
CPU_ID=0
MODE="all"
MIN_SIZE=""
MAX_SIZE=""
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
    --kernel)
      KERNEL="$2"
      shift 2
      ;;
    --disk-image)
      DISK_IMAGE="$2"
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
    --cpu)
      CPU_ID="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --min-size)
      MIN_SIZE="$2"
      shift 2
      ;;
    --max-size)
      MAX_SIZE="$2"
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

SRC_B64="$(base64 -w0 tools/write_size_sweep.c)"

run_mode() {
  local mode_label="$1"
  local mem_node="$2"
  local run_dir="${OUTDIR}/${mode_label}"
  local guest_args="--cpu ${CPU_ID} --mode-label ${mode_label}"
  local workload_cmd=""

  mkdir -p "$run_dir"

  if [[ -n "$MIN_SIZE" ]]; then
    guest_args+=" --min-size ${MIN_SIZE}"
  fi
  if [[ -n "$MAX_SIZE" ]]; then
    guest_args+=" --max-size ${MAX_SIZE}"
  fi

  workload_cmd="numactl -H; printf '%s' \"$SRC_B64\" | base64 -d > /tmp/write_size_sweep.c; gcc -O3 /tmp/write_size_sweep.c -o /tmp/write_size_sweep; m5 exit; m5 resetstats; numactl -N 0 -m ${mem_node} /tmp/write_size_sweep ${guest_args}; m5 dumpstats; m5 exit"

  echo "[run] mode=${mode_label} mem_node=${mem_node} outdir=${run_dir}"
  "$BINARY" \
    -d "$run_dir" \
    configs/example/gem5_library/x86-cxl-type3-with-classic.py \
    --is_asic True \
    --cpu_type "$CPU_TYPE" \
    --boot_cpu "$BOOT_CPU" \
    --num_cpus 2 \
    --kernel "$KERNEL" \
    --disk-image "$DISK_IMAGE" \
    --workload-cmd "$workload_cmd"

  python3 tools/parse_write_size_sweep.py \
    "$run_dir/board.pc.com_1.device" \
    "$run_dir/results.csv"
}

case "$MODE" in
  all)
    run_mode dram 0
    run_mode cxl 1
    ;;
  dram)
    run_mode dram 0
    ;;
  cxl)
    run_mode cxl 1
    ;;
  *)
    echo "invalid --mode: ${MODE}" >&2
    exit 1
    ;;
esac

python3 - <<'PY' "$OUTDIR"
import csv
import sys
from pathlib import Path

outdir = Path(sys.argv[1])
combined = []
for path in sorted(outdir.glob("*/results.csv")):
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        combined.extend(reader)

combined.sort(key=lambda row: (row["metric"], int(row["size_bytes"]), row["mode"]))
combined_path = outdir / "combined_results.csv"
with combined_path.open("w", newline="", encoding="utf-8") as fh:
    if combined:
        writer = csv.DictWriter(fh, fieldnames=list(combined[0].keys()))
        writer.writeheader()
        writer.writerows(combined)

summary_path = outdir / "summary.txt"
with summary_path.open("w", encoding="utf-8") as fh:
    for metric in ("latency", "bandwidth"):
        fh.write(f"[{metric}]\n")
        for mode in ("dram", "cxl"):
            rows = [row for row in combined if row["metric"] == metric and row["mode"] == mode]
            if not rows:
                continue
            fh.write(f"mode={mode}\n")
            for row in rows:
                if metric == "latency":
                    fh.write(
                        "  size_bytes={size_bytes} avg_ns={avg_ns} p50_ns={p50_ns} "
                        "p95_ns={p95_ns} target_node_hint={target_node_hint}\n".format(**row)
                    )
                else:
                    fh.write(
                        "  size_bytes={size_bytes} payload_gib_s={payload_gib_s} "
                        "effective_gib_s={effective_gib_s} target_node_hint={target_node_hint}\n".format(**row)
                    )
        fh.write("\n")

print(f"combined_csv={combined_path}")
print(f"summary_txt={summary_path}")
PY

echo
echo "=== Write size sweep summary ==="
sed -n '1,120p' "$OUTDIR/summary.txt"
echo
echo "Output dir: $OUTDIR"
