#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  tools/run_hydrarpc_multiclient_16client_slowmix_sweep.sh [options]

Options:
  --outdir <dir>                Output root.
  --binary <path>               gem5 binary. Default: build/X86/gem5.opt
  --cpu-type <type>             Switch CPU type. Default: TIMING
  --boot-cpu <type>             Boot CPU type before first m5 exit. Default: KVM
  --client-count <N>            Total client count. Default: 16
  --count-per-client <N>        Normal-client request count. Default: 20
  --slow-count-per-client <N>   Request count for slow clients. Default: 2
  --slow-client-counts <list>   Quoted list, e.g. "1 2 4 8 16"
  --slow-send-gap-ns <N>        Uniform gap used by slow clients. Default: 20000
  --window-size <N>             Per-client window size. Default: 16
  --slot-count <N>              Per-client ring depth. Default: min(window-size, count-per-client)
  --cxl-node <N>                NUMA node used for CXL mappings. Default: 1
  --num-cpus <N>                Guest CPU count. Default: client-count + 2
  --server-cpu <N>              Server CPU id. Default: client-count
  --drop-first-per-client <N>   Steady-state trimming. Default: 1
  --resume                      Reuse an existing outdir/run.log/CSV set.
  --skip-successful             Skip slowN subdirs whose result.json already reports success.
  --skip-image-setup            Reuse the already injected dedicated guest binary.
  --skip-build                  Skip scons build.
  --help                        Show this message.

Behavior:
  - Total clients stay fixed.
  - The first N client ids are marked slow.
  - Slow clients issue only slow-count-per-client requests and use uniform
    pacing with slow-send-gap-ns.
  - Normal clients remain on the default greedy dedicated path.
  - Guest instrumentation stays at start/end only (record-breakdown=none).
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUTDIR="output/hydrarpc_multiclient_16client_slowmix_$(date +%Y%m%d_%H%M%S)"
BINARY="build/X86/gem5.opt"
CPU_TYPE="TIMING"
BOOT_CPU="KVM"
CLIENT_COUNT=16
COUNT_PER_CLIENT=20
SLOW_COUNT_PER_CLIENT=2
SLOW_CLIENT_COUNTS="1 2 4 8 16"
SLOW_SEND_GAP_NS=20000
WINDOW_SIZE=16
SLOT_COUNT=0
CXL_NODE=1
NUM_CPUS=0
SERVER_CPU=-1
DROP_FIRST_PER_CLIENT=1
SKIP_IMAGE_SETUP=0
SKIP_BUILD=0
RESUME=0
SKIP_SUCCESSFUL=0

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
    --slow-count-per-client)
      SLOW_COUNT_PER_CLIENT="$2"
      shift 2
      ;;
    --slow-client-counts)
      SLOW_CLIENT_COUNTS="$2"
      shift 2
      ;;
    --slow-send-gap-ns)
      SLOW_SEND_GAP_NS="$2"
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
    --drop-first-per-client)
      DROP_FIRST_PER_CLIENT="$2"
      shift 2
      ;;
    --resume)
      RESUME=1
      shift 1
      ;;
    --skip-successful)
      SKIP_SUCCESSFUL=1
      shift 1
      ;;
    --skip-image-setup)
      SKIP_IMAGE_SETUP=1
      shift 1
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

mkdir -p "$OUTDIR"
OUTDIR="$(cd "$OUTDIR" && pwd)"
SUMMARY_CSV="$OUTDIR/summary.csv"
STEADY_CSV="$OUTDIR/steady_summary.csv"
IMPACT_CSV="$OUTDIR/impact.csv"
RUNLOG="$OUTDIR/run.log"

if [[ "$RESUME" -eq 1 ]]; then
  touch "$RUNLOG"
else
  : > "$RUNLOG"
  rm -f "$SUMMARY_CSV" "$STEADY_CSV" "$IMPACT_CSV"
fi

for slow_client_count in $SLOW_CLIENT_COUNTS; do
  out="$OUTDIR/slow${slow_client_count}"
  expected_total=$(( CLIENT_COUNT * COUNT_PER_CLIENT - slow_client_count * (COUNT_PER_CLIENT - SLOW_COUNT_PER_CLIENT) ))

  if [[ "$SKIP_SUCCESSFUL" -eq 1 && -f "$out/result.json" ]]; then
    if python3 - <<'PY' "$out/result.json"
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
sys.exit(0 if payload.get("status") == "success" else 1)
PY
    then
      echo "[$(date '+%F %T')] SKIP slow_client_count=${slow_client_count} outdir=${out} reason=existing-success" | tee -a "$RUNLOG"
      continue
    fi
  fi

  mkdir -p "$out"
  echo "[$(date '+%F %T')] START slow_client_count=${slow_client_count} outdir=${out}" | tee -a "$RUNLOG"

  set +e
  bash "$REPO_ROOT/tools/run_e2e_hydrarpc_multiclient_dedicated_send1_poll1.sh" \
    --binary "$BINARY" \
    --cpu-type "$CPU_TYPE" \
    --boot-cpu "$BOOT_CPU" \
    --client-count "$CLIENT_COUNT" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --window-size "$WINDOW_SIZE" \
    --slot-count "$SLOT_COUNT" \
    --slow-client-count "$slow_client_count" \
    --slow-count-per-client "$SLOW_COUNT_PER_CLIENT" \
    --slow-send-gap-ns "$SLOW_SEND_GAP_NS" \
    --send-mode greedy \
    --send-gap-ns 0 \
    --record-breakdown none \
    --cxl-node "$CXL_NODE" \
    --num-cpus "$NUM_CPUS" \
    --server-cpu "$SERVER_CPU" \
    --outdir "$out" \
    $([[ "$SKIP_IMAGE_SETUP" -eq 1 ]] && printf '%s ' --skip-image-setup) \
    $([[ "$SKIP_BUILD" -eq 1 ]] && printf '%s ' --skip-build) \
    >"$out/console.log" 2>&1
  runner_rc=$?

  python3 "$REPO_ROOT/tools/summarize_hydrarpc_multiclient.py" \
    --log "$out/board.pc.com_1.device" \
    --experiment multiclient_dedicated_send1_poll1_slowmix \
    --client-count "$CLIENT_COUNT" \
    --count-per-client "$COUNT_PER_CLIENT" \
    --expected-total-requests "$expected_total" \
    --drop-first-per-client "$DROP_FIRST_PER_CLIENT" \
    --csv "$SUMMARY_CSV" \
    --steady-csv "$STEADY_CSV" \
    --outdir "$out" \
    --result-json "$out/result.json" \
    >"$out/summarize.log" 2>&1
  summarize_rc=$?
  set -e

  if [[ "$runner_rc" -ne 0 || "$summarize_rc" -ne 0 ]]; then
    echo "[$(date '+%F %T')] FAIL slow_client_count=${slow_client_count} runner_rc=${runner_rc} summarize_rc=${summarize_rc} outdir=${out}" | tee -a "$RUNLOG"
    tail -n 40 "$out/console.log" | tee -a "$RUNLOG" || true
    tail -n 40 "$out/summarize.log" | tee -a "$RUNLOG" || true
    continue
  fi

  echo "[$(date '+%F %T')] SUCCESS slow_client_count=${slow_client_count} outdir=${out}" | tee -a "$RUNLOG"
  cat "$out/summarize.log" | tee -a "$RUNLOG"
done

python3 - <<'PY' "$OUTDIR" "$COUNT_PER_CLIENT" "$SLOW_COUNT_PER_CLIENT" "$SLOW_SEND_GAP_NS" "$IMPACT_CSV"
import csv
import json
import pathlib
import re
import sys

outdir = pathlib.Path(sys.argv[1])
normal_count = int(sys.argv[2])
slow_count = int(sys.argv[3])
slow_gap_ns = int(sys.argv[4])
impact_csv = pathlib.Path(sys.argv[5])
summary_csv = outdir / "summary.csv"
steady_csv = outdir / "steady_summary.csv"

summary_fieldnames = [
    "experiment",
    "client_count",
    "count_per_client",
    "total_requests",
    "first_start_ns",
    "last_end_ns",
    "total_e2e_ns",
    "avg_latency_ns",
    "median_latency_ns",
    "aggregate_throughput_mrps",
    "correctness_fail_count",
    "outdir",
    "log_path",
]

steady_fieldnames = [
    "experiment",
    "client_count",
    "count_per_client",
    "drop_first_requests_per_client",
    "dropped_requests_total",
    "steady_requests",
    "first_kept_start_ns",
    "steady_total_e2e_ns",
    "steady_avg_latency_ns",
    "steady_median_latency_ns",
    "steady_avg_reqresp_latency_ns",
    "steady_avg_gap_ns",
    "steady_median_gap_ns",
    "steady_avg_throughput_mrps",
    "steady_avg_gap_throughput_mrps",
    "steady_median_throughput_mrps",
    "first_kept_end_ns",
    "last_kept_end_ns",
    "outdir",
    "log_path",
]

summary_rows = []
steady_rows = []
rows = []
for result_path in sorted(outdir.glob("slow*/result.json")):
    match = re.fullmatch(r"slow(\d+)", result_path.parent.name)
    if match is None:
        continue
    payload = json.loads(result_path.read_text())
    if payload.get("status") != "success":
        continue
    slow_client_count = int(match.group(1))
    experiment = payload["experiment"]
    client_count = payload["client_count"]
    count_per_client = payload["count_per_client"]
    log_path = payload["log_path"]
    stats = payload["stats"]
    steady = payload["steady_stats"]
    summary_rows.append(
        {
            "experiment": experiment,
            "client_count": client_count,
            "count_per_client": count_per_client,
            "total_requests": stats["total_requests"],
            "first_start_ns": stats["first_start_ns"],
            "last_end_ns": stats["last_end_ns"],
            "total_e2e_ns": stats["total_e2e_ns"],
            "avg_latency_ns": f"{stats['avg_latency_ns']:.3f}",
            "median_latency_ns": f"{stats['median_latency_ns']:.3f}",
            "aggregate_throughput_mrps": f"{stats['aggregate_throughput_mrps']:.6f}",
            "correctness_fail_count": stats["correctness_fail_count"],
            "outdir": str(result_path.parent),
            "log_path": log_path,
        }
    )
    steady_rows.append(
        {
            "experiment": experiment,
            "client_count": client_count,
            "count_per_client": count_per_client,
            "drop_first_requests_per_client": steady["drop_first_requests_per_client"],
            "dropped_requests_total": steady["dropped_requests_total"],
            "steady_requests": steady["steady_requests"],
            "first_kept_start_ns": steady["first_kept_start_ns"],
            "steady_total_e2e_ns": steady["steady_total_e2e_ns"],
            "steady_avg_latency_ns": (
                f"{steady['steady_avg_latency_ns']:.3f}"
                if steady["steady_avg_latency_ns"] is not None
                else ""
            ),
            "steady_median_latency_ns": (
                f"{steady['steady_median_latency_ns']:.3f}"
                if steady["steady_median_latency_ns"] is not None
                else ""
            ),
            "steady_avg_reqresp_latency_ns": (
                f"{steady['steady_avg_reqresp_latency_ns']:.3f}"
                if steady["steady_avg_reqresp_latency_ns"] is not None
                else ""
            ),
            "steady_avg_gap_ns": (
                f"{steady['steady_avg_gap_ns']:.3f}"
                if steady["steady_avg_gap_ns"] is not None
                else ""
            ),
            "steady_median_gap_ns": (
                f"{steady['steady_median_gap_ns']:.3f}"
                if steady["steady_median_gap_ns"] is not None
                else ""
            ),
            "steady_avg_throughput_mrps": (
                f"{steady['steady_avg_throughput_mrps']:.6f}"
                if steady["steady_avg_throughput_mrps"] is not None
                else ""
            ),
            "steady_avg_gap_throughput_mrps": (
                f"{steady['steady_avg_gap_throughput_mrps']:.6f}"
                if steady["steady_avg_gap_throughput_mrps"] is not None
                else ""
            ),
            "steady_median_throughput_mrps": (
                f"{steady['steady_median_throughput_mrps']:.6f}"
                if steady["steady_median_throughput_mrps"] is not None
                else ""
            ),
            "first_kept_end_ns": steady["first_kept_end_ns"],
            "last_kept_end_ns": steady["last_kept_end_ns"],
            "outdir": str(result_path.parent),
            "log_path": log_path,
        }
    )
    rows.append(
        {
            "slow_client_count": slow_client_count,
            "normal_count_per_client": normal_count,
            "slow_count_per_client": slow_count,
            "slow_send_gap_ns": slow_gap_ns,
            "total_requests": stats["total_requests"],
            "aggregate_throughput_mrps": f"{stats['aggregate_throughput_mrps']:.6f}",
            "avg_latency_ns": f"{stats['avg_latency_ns']:.3f}",
            "median_latency_ns": f"{stats['median_latency_ns']:.3f}",
            "steady_requests": steady["steady_requests"],
            "steady_avg_throughput_mrps": (
                f"{steady['steady_avg_throughput_mrps']:.6f}"
                if steady["steady_avg_throughput_mrps"] is not None
                else ""
            ),
            "steady_avg_reqresp_latency_ns": (
                f"{steady['steady_avg_reqresp_latency_ns']:.3f}"
                if steady["steady_avg_reqresp_latency_ns"] is not None
                else ""
            ),
            "steady_median_latency_ns": (
                f"{steady['steady_median_latency_ns']:.3f}"
                if steady["steady_median_latency_ns"] is not None
                else ""
            ),
            "outdir": str(result_path.parent),
        }
    )

def sort_key_for_row(row):
    match = re.fullmatch(r"slow(\d+)", pathlib.Path(row["outdir"]).name)
    if match is None:
        return pathlib.Path(row["outdir"]).name
    return int(match.group(1))


summary_rows.sort(key=sort_key_for_row)
steady_rows.sort(key=sort_key_for_row)
rows.sort(key=lambda row: row["slow_client_count"])

with summary_csv.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=summary_fieldnames)
    writer.writeheader()
    writer.writerows(summary_rows)

with steady_csv.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=steady_fieldnames)
    writer.writeheader()
    writer.writerows(steady_rows)

fieldnames = [
    "slow_client_count",
    "normal_count_per_client",
    "slow_count_per_client",
    "slow_send_gap_ns",
    "total_requests",
    "aggregate_throughput_mrps",
    "avg_latency_ns",
    "median_latency_ns",
    "steady_requests",
    "steady_avg_throughput_mrps",
    "steady_avg_reqresp_latency_ns",
    "steady_median_latency_ns",
    "outdir",
]

with impact_csv.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
PY

echo "OUTDIR=$OUTDIR"
echo "SUMMARY_CSV=$SUMMARY_CSV"
echo "STEADY_CSV=$STEADY_CSV"
echo "IMPACT_CSV=$IMPACT_CSV"
