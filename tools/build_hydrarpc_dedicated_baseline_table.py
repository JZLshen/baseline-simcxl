#!/usr/bin/env python3
import csv
from pathlib import Path

from summarize_hydrarpc_multiclient import (
    compute_steady_stats,
    compute_window_stats,
    parse_log,
)


ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output" / "figures"
OUTCSV = OUTDIR / "hydrarpc_dedicated_baseline_table.csv"
OUTMD = OUTDIR / "hydrarpc_dedicated_baseline_table.md"

AUTHORITATIVE_RUNS = [
    {
        "client_count": 1,
        "label": "c1_serialized_final",
        "log_path": ROOT / "output" / "c1_first_rpc_serialized_20260322" / "board.pc.com_1.device",
        "note": "first request fully serialized before normal steady sending",
    },
    {
        "client_count": 2,
        "label": "c2_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c2_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "client_count": 4,
        "label": "c4_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c4_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "client_count": 8,
        "label": "c8_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c8_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "client_count": 16,
        "label": "c16_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c16_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
]

MISSING_RUNS = [
    {
        "client_count": 32,
        "label": "c32_missing_final",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_retry_20260321" / "c32_try1" / "board.pc.com_1.device",
        "note": "run directory exists, but no successful summarized final result was produced",
    }
]


def fmt(value, digits=6):
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def fmt_ns(value):
    if value is None:
        return ""
    return f"{value:.3f}"


def build_row(item):
    stats, request_rows, _ = parse_log(item["log_path"])
    window = compute_window_stats(request_rows)
    steady1 = compute_steady_stats(request_rows, 1)
    steady5 = compute_steady_stats(request_rows, 5)

    req_ids = {row["req_id"] for row in request_rows}
    count_per_client = max(req_ids) + 1 if req_ids else 0

    return {
        "client_count": item["client_count"],
        "label": item["label"],
        "status": "ok",
        "count_per_client": count_per_client,
        "total_requests": window["total_requests"],
        "aggregate_throughput_mrps": fmt(window["aggregate_throughput_mrps"]),
        "aggregate_avg_latency_ns": fmt_ns(window["avg_latency_ns"]),
        "aggregate_median_latency_ns": fmt_ns(window["median_latency_ns"]),
        "steady_drop1_requests": steady1["steady_requests"],
        "steady_drop1_throughput_mrps": fmt(steady1["steady_avg_throughput_mrps"]),
        "steady_drop1_avg_reqresp_latency_ns": fmt_ns(steady1["steady_avg_reqresp_latency_ns"]),
        "steady_drop1_avg_latency_ns": fmt_ns(steady1["steady_avg_latency_ns"]),
        "steady_drop5_requests": steady5["steady_requests"],
        "steady_drop5_throughput_mrps": fmt(steady5["steady_avg_throughput_mrps"]),
        "steady_drop5_avg_reqresp_latency_ns": fmt_ns(steady5["steady_avg_reqresp_latency_ns"]),
        "steady_drop5_avg_latency_ns": fmt_ns(steady5["steady_avg_latency_ns"]),
        "log_path": str(item["log_path"]),
        "note": item["note"],
    }


def build_missing_row(item):
    return {
        "client_count": item["client_count"],
        "label": item["label"],
        "status": "missing",
        "count_per_client": "",
        "total_requests": "",
        "aggregate_throughput_mrps": "",
        "aggregate_avg_latency_ns": "",
        "aggregate_median_latency_ns": "",
        "steady_drop1_requests": "",
        "steady_drop1_throughput_mrps": "",
        "steady_drop1_avg_reqresp_latency_ns": "",
        "steady_drop1_avg_latency_ns": "",
        "steady_drop5_requests": "",
        "steady_drop5_throughput_mrps": "",
        "steady_drop5_avg_reqresp_latency_ns": "",
        "steady_drop5_avg_latency_ns": "",
        "log_path": str(item["log_path"]),
        "note": item["note"],
    }


def write_csv(rows):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with OUTCSV.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "client_count",
                "label",
                "status",
                "count_per_client",
                "total_requests",
                "aggregate_throughput_mrps",
                "aggregate_avg_latency_ns",
                "aggregate_median_latency_ns",
                "steady_drop1_requests",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop1_avg_latency_ns",
                "steady_drop5_requests",
                "steady_drop5_throughput_mrps",
                "steady_drop5_avg_reqresp_latency_ns",
                "steady_drop5_avg_latency_ns",
                "log_path",
                "note",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_md(rows):
    visible_rows = [row for row in rows if row["status"] == "ok"]
    lines = [
        "| Clients | Aggregate Mops | Steady Mops (drop=1) | Req-Resp Latency ns (drop=1) | Steady Mops (drop=5) | Req-Resp Latency ns (drop=5) |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in visible_rows:
        lines.append(
            "| "
            f"{row['client_count']} | "
            f"{row['aggregate_throughput_mrps']} | "
            f"{row['steady_drop1_throughput_mrps']} | "
            f"{row['steady_drop1_avg_reqresp_latency_ns']} | "
            f"{row['steady_drop5_throughput_mrps']} | "
            f"{row['steady_drop5_avg_reqresp_latency_ns']} |"
        )

    missing_rows = [row for row in rows if row["status"] != "ok"]
    if missing_rows:
        lines.append("")
        lines.append("Missing or incomplete runs:")
        for row in missing_rows:
            lines.append(f"- c{row['client_count']}: {row['note']}")

    OUTMD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    rows = [build_row(item) for item in AUTHORITATIVE_RUNS]
    rows.extend(build_missing_row(item) for item in MISSING_RUNS)
    rows.sort(key=lambda row: row["client_count"])
    write_csv(rows)
    write_md(rows)
    print(f"wrote {OUTCSV}")
    print(f"wrote {OUTMD}")


if __name__ == "__main__":
    main()
