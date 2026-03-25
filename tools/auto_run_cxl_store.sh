#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/auto_run_cxl_store.sh --kernel <path> --disk-image <path> [options]

Required:
  --kernel <path>         Linux kernel image used by gem5 full-system.
  --disk-image <path>     Disk image used by gem5 full-system.

Options:
  --cpu-type <TIMING|O3>  Switched CPU model. Default: TIMING
  --boot-cpu <KVM|ATOMIC> Boot CPU model before switch. Default: KVM
  --is-asic <True|False>  CXL device model. Default: True
  --runs <N>              Number of runs. Default: 3
  --outdir <dir>          Output directory. Default: output/auto_cxl_store
  --jobs <N>              Parallel build jobs. Default: nproc
  --binary <path>         gem5 binary path. Default: build/X86/gem5.opt
  --skip-build            Skip scons build step.
  --help                  Show this message.

Example:
  sudo tools/auto_run_cxl_store.sh \
    --kernel /home/you/fs_image/vmlinux \
    --disk-image /home/you/fs_image/parsec.img \
    --runs 3

Notes:
  - The parser filters by the measured cacheline physical address and the
    benchmark trace window emitted by the guest workload.
EOF
}

KERNEL=""
DISK_IMAGE=""
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
IS_ASIC="True"
RUNS=3
OUTDIR="output/auto_cxl_store"
BINARY="build/X86/gem5.opt"
JOBS="$(nproc)"
SKIP_BUILD=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
    --cpu-type)
      CPU_TYPE="$2"
      shift 2
      ;;
    --boot-cpu)
      BOOT_CPU="$2"
      shift 2
      ;;
    --is-asic)
      IS_ASIC="$2"
      shift 2
      ;;
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --outdir)
      OUTDIR="$2"
      shift 2
      ;;
    --jobs)
      JOBS="$2"
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

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: --runs must be a positive integer." >&2
  exit 1
fi

cd "${REPO_ROOT}"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "[1/4] Building ${BINARY}"
  scons "${BINARY}" -j"${JOBS}"
else
  echo "[1/4] Skipping build step"
fi

mkdir -p "${OUTDIR}"

declare -a KV_FILES=()
B64_SRC="$(base64 -w0 tools/store64_lat.c)"
WORKLOAD_CMD="numactl -H; printf '%s' \"$B64_SRC\" | base64 -d > /tmp/store64_lat.c; gcc -O2 /tmp/store64_lat.c -o /tmp/store64_lat; m5 exit; m5 resetstats; numactl -N 0 -m 1 /tmp/store64_lat; m5 dumpstats; m5 exit"

for run_id in $(seq 1 "${RUNS}"); do
  run_dir="${OUTDIR}/run${run_id}"
  mkdir -p "${run_dir}"

  echo "[2/4] Run ${run_id}/${RUNS}: Running gem5 full-system CXL simulation"
  echo "       Guest workload: injected tools/store64_lat.c"
  "${BINARY}" \
    -d "${run_dir}" \
    --debug-flags=Bridge,CXLMemCtrl,MemCtrl \
    --debug-file=cxl_trace.log \
    configs/example/gem5_library/x86-cxl-type3-with-classic.py \
    --is_asic "${IS_ASIC}" \
    --cpu_type "${CPU_TYPE}" \
    --boot_cpu "${BOOT_CPU}" \
    --kernel "${KERNEL}" \
    --disk-image "${DISK_IMAGE}" \
    --workload-cmd "${WORKLOAD_CMD}"

  echo "[3/4] Run ${run_id}/${RUNS}: Parsing CXL store latency"
  kv_path="${run_dir}/store_latency.kv"
  target_pa="$(
    python3 - "${run_dir}/board.pc.com_1.device" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
text = log_path.read_text(encoding="utf-8", errors="ignore")
matches = re.findall(r"^target_pa=(0x[0-9a-fA-F]+)$", text, flags=re.M)
if not matches:
    raise SystemExit("missing target_pa in board log")
print(matches[-1])
PY
  )"
  trace_window_start_ns="$(
    python3 - "${run_dir}/board.pc.com_1.device" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
text = log_path.read_text(encoding="utf-8", errors="ignore")
matches = re.findall(r"^trace_window_start_ns=(\d+)$", text, flags=re.M)
if not matches:
    raise SystemExit("missing trace_window_start_ns in board log")
print(matches[-1])
PY
  )"
  trace_window_end_ns="$(
    python3 - "${run_dir}/board.pc.com_1.device" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
text = log_path.read_text(encoding="utf-8", errors="ignore")
matches = re.findall(r"^trace_window_end_ns=(\d+)$", text, flags=re.M)
if not matches:
    raise SystemExit("missing trace_window_end_ns in board log")
print(matches[-1])
PY
  )"
  samples="$(
    python3 - "${run_dir}/board.pc.com_1.device" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
text = log_path.read_text(encoding="utf-8", errors="ignore")
matches = re.findall(r"^samples=(\d+)$", text, flags=re.M)
if not matches:
    raise SystemExit("missing samples in board log")
print(matches[-1])
PY
  )"
  python3 tools/parse_cxl_store_latency.py \
    --trace "${run_dir}/cxl_trace.log" \
    --stats "${run_dir}/stats.txt" \
    --component-substr bridge \
    --addr-exact "${target_pa}" \
    --time-start-ns "${trace_window_start_ns}" \
    --time-end-ns "${trace_window_end_ns}" \
    --cmd-filter Write \
    --strict \
    --expect-pairs "${samples}" \
    --require-clean-window \
    --output-kv "${kv_path}"
  KV_FILES+=("${kv_path}")
done

echo "[4/4] Aggregating run statistics"
summary_path="${OUTDIR}/summary_store_latency.txt"
python3 - "${summary_path}" "${KV_FILES[@]}" <<'PY'
import statistics
import sys
from pathlib import Path

summary = Path(sys.argv[1])
kv_files = [Path(p) for p in sys.argv[2:]]
required = [
    "matched_pairs",
    "first_store_latency_ns",
    "p50_latency_ns",
    "p95_latency_ns",
    "max_latency_ns",
]

rows = []
for idx, kv in enumerate(kv_files, start=1):
    data = {}
    with kv.open("r", encoding="utf-8") as fh:
        for line in fh:
            if "=" not in line:
                continue
            k, v = line.strip().split("=", 1)
            data[k] = v
    missing = [k for k in required if k not in data]
    if missing:
        raise RuntimeError(f"{kv} missing keys: {missing}")
    rows.append(
        {
            "run": idx,
            "matched_pairs": int(data["matched_pairs"]),
            "first_store_latency_ns": float(data["first_store_latency_ns"]),
            "p50_latency_ns": float(data["p50_latency_ns"]),
            "p95_latency_ns": float(data["p95_latency_ns"]),
            "max_latency_ns": float(data["max_latency_ns"]),
        }
    )

def med(key: str) -> float:
    return statistics.median([r[key] for r in rows])

with summary.open("w", encoding="utf-8") as out:
    out.write("run,matched_pairs,first_ns,p50_ns,p95_ns,max_ns\n")
    for r in rows:
        out.write(
            f"{r['run']},{r['matched_pairs']},"
            f"{r['first_store_latency_ns']:.3f},{r['p50_latency_ns']:.3f},"
            f"{r['p95_latency_ns']:.3f},{r['max_latency_ns']:.3f}\n"
        )
    out.write("\n")
    out.write("median_first_ns={:.3f}\n".format(med("first_store_latency_ns")))
    out.write("median_p50_ns={:.3f}\n".format(med("p50_latency_ns")))
    out.write("median_p95_ns={:.3f}\n".format(med("p95_latency_ns")))
    out.write("median_max_ns={:.3f}\n".format(med("max_latency_ns")))

print(summary)
PY

echo "Done. Output directory: ${OUTDIR}"
echo "Summary file: ${summary_path}"
