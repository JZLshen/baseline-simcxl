#!/usr/bin/env python3
import csv
from collections import Counter
from pathlib import Path

from plot_hydrarpc_slowmix_steady_compare_svg import (
    BASELINE_LOG,
    ROOT,
    SLOW10_LOGS,
    SLOW30_LOGS,
)
from summarize_hydrarpc_multiclient import (
    compute_steady_stats,
    compute_window_stats,
    parse_log,
)


OUTDIR = ROOT / "output" / "figures"
OUTCSV = OUTDIR / "hydrarpc_current_complete_table.csv"
OUTMD = OUTDIR / "hydrarpc_current_complete_table.md"

DEDICATED_RUNS = [
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 1,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c1_serialized_final",
        "log_path": ROOT / "output" / "c1_first_rpc_serialized_20260322" / "board.pc.com_1.device",
        "note": "first request fully serialized before normal steady sending",
    },
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 2,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c2_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c2_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 4,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c4_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c4_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 8,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c8_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c8_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 16,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c16_pow2_clean",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_rerun_clean_20260322_0200" / "c16_try1" / "board.pc.com_1.device",
        "note": "pow2 rerun after first-request serialization change",
    },
]

MISSING_ROWS = [
    {
        "dataset": "dedicated",
        "series": "baseline",
        "client_count": 32,
        "slow_rate_pct": "",
        "slow_client_count": 0,
        "label": "c32_missing_final",
        "log_path": ROOT / "output" / "hydrarpc_multiclient_pow2_retry_20260321" / "c32_try1" / "board.pc.com_1.device",
        "note": "run directory exists, but no successful summarized final result was produced",
    }
]


def fmt(value, digits=6):
    if value is None or value == "":
        return ""
    return f"{value:.{digits}f}"


def fmt_ns(value):
    if value is None or value == "":
        return ""
    return f"{value:.3f}"


def requests_per_client_profile(request_rows):
    counts = Counter()
    for row in request_rows:
        counts[row["client_id"]] += 1

    if not counts:
        return ""

    profile = Counter(counts.values())
    parts = []
    for req_count in sorted(profile):
        parts.append(f"{req_count}x{profile[req_count]}")
    return ";".join(parts)


def stats_for_drop(request_rows, drop_first_per_client):
    steady = compute_steady_stats(request_rows, drop_first_per_client)
    return {
        f"steady_drop{drop_first_per_client}_requests": steady["steady_requests"],
        f"steady_drop{drop_first_per_client}_throughput_mrps": fmt(
            steady["steady_avg_throughput_mrps"]
        ),
        f"steady_drop{drop_first_per_client}_avg_reqresp_latency_ns": fmt_ns(
            steady["steady_avg_reqresp_latency_ns"]
        ),
        f"steady_drop{drop_first_per_client}_avg_latency_ns": fmt_ns(
            steady["steady_avg_latency_ns"]
        ),
    }


def build_row(item):
    _, request_rows, _ = parse_log(item["log_path"])
    window = compute_window_stats(request_rows)

    row = {
        "dataset": item["dataset"],
        "series": item["series"],
        "label": item["label"],
        "status": "ok",
        "client_count": item["client_count"],
        "slow_rate_pct": item["slow_rate_pct"],
        "slow_client_count": item["slow_client_count"],
        "requests_per_client_profile": requests_per_client_profile(request_rows),
        "total_requests": window["total_requests"],
        "aggregate_throughput_mrps": fmt(window["aggregate_throughput_mrps"]),
        "aggregate_avg_latency_ns": fmt_ns(window["avg_latency_ns"]),
        "aggregate_median_latency_ns": fmt_ns(window["median_latency_ns"]),
        "log_path": str(item["log_path"]),
        "note": item["note"],
    }
    row.update(stats_for_drop(request_rows, 1))
    row.update(stats_for_drop(request_rows, 2))
    row.update(stats_for_drop(request_rows, 5))
    return row


def build_missing_row(item):
    row = {
        "dataset": item["dataset"],
        "series": item["series"],
        "label": item["label"],
        "status": "missing",
        "client_count": item["client_count"],
        "slow_rate_pct": item["slow_rate_pct"],
        "slow_client_count": item["slow_client_count"],
        "requests_per_client_profile": "",
        "total_requests": "",
        "aggregate_throughput_mrps": "",
        "aggregate_avg_latency_ns": "",
        "aggregate_median_latency_ns": "",
        "log_path": str(item["log_path"]),
        "note": item["note"],
    }
    for drop in (1, 2, 5):
        row[f"steady_drop{drop}_requests"] = ""
        row[f"steady_drop{drop}_throughput_mrps"] = ""
        row[f"steady_drop{drop}_avg_reqresp_latency_ns"] = ""
        row[f"steady_drop{drop}_avg_latency_ns"] = ""
    return row


def write_csv(rows):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with OUTCSV.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "dataset",
                "series",
                "label",
                "status",
                "client_count",
                "slow_rate_pct",
                "slow_client_count",
                "requests_per_client_profile",
                "total_requests",
                "aggregate_throughput_mrps",
                "aggregate_avg_latency_ns",
                "aggregate_median_latency_ns",
                "steady_drop1_requests",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop1_avg_latency_ns",
                "steady_drop2_requests",
                "steady_drop2_throughput_mrps",
                "steady_drop2_avg_reqresp_latency_ns",
                "steady_drop2_avg_latency_ns",
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


def md_table(header, rows, columns):
    lines = [header, "", "| " + " | ".join(columns) + " |"]
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    lines.append("")
    return lines


def write_md(rows):
    dedicated_rows = [row for row in rows if row["dataset"] == "dedicated" and row["status"] == "ok"]
    slowmix_baseline_rows = [row for row in rows if row["dataset"] == "slowmix_baseline"]
    slow10_rows = [row for row in rows if row["dataset"] == "slowmix" and row["series"] == "slow10"]
    slow30_rows = [row for row in rows if row["dataset"] == "slowmix" and row["series"] == "slow30"]
    missing_rows = [row for row in rows if row["status"] != "ok"]

    lines = []
    lines.extend(
        md_table(
            "Dedicated Baseline",
            dedicated_rows,
            [
                "client_count",
                "aggregate_throughput_mrps",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop2_throughput_mrps",
                "steady_drop5_throughput_mrps",
            ],
        )
    )
    lines.extend(
        md_table(
            "16-Client 100% Baseline",
            slowmix_baseline_rows,
            [
                "slow_client_count",
                "aggregate_throughput_mrps",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop2_throughput_mrps",
                "steady_drop5_throughput_mrps",
            ],
        )
    )
    lines.extend(
        md_table(
            "16-Client Slow 10%",
            slow10_rows,
            [
                "slow_client_count",
                "requests_per_client_profile",
                "total_requests",
                "aggregate_throughput_mrps",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop2_throughput_mrps",
            ],
        )
    )
    lines.extend(
        md_table(
            "16-Client Slow 30%",
            slow30_rows,
            [
                "slow_client_count",
                "requests_per_client_profile",
                "total_requests",
                "aggregate_throughput_mrps",
                "steady_drop1_throughput_mrps",
                "steady_drop1_avg_reqresp_latency_ns",
                "steady_drop2_throughput_mrps",
            ],
        )
    )
    if missing_rows:
        lines.append("Missing or incomplete runs")
        lines.append("")
        for row in missing_rows:
            lines.append(f"- {row['label']}: {row['note']}")
        lines.append("")

    OUTMD.write_text("\n".join(lines), encoding="utf-8")


def build_all_rows():
    rows = [build_row(item) for item in DEDICATED_RUNS]
    rows.extend(build_missing_row(item) for item in MISSING_ROWS)

    rows.append(
        build_row(
            {
                "dataset": "slowmix_baseline",
                "series": "100pct",
                "client_count": 16,
                "slow_rate_pct": 100,
                "slow_client_count": 0,
                "label": "slowmix_100pct_x0_final",
                "log_path": BASELINE_LOG,
                "note": "16-client all-full-rate x=0 baseline used by the slowmix figure",
            }
        )
    )

    for slow_client_count, log_path in sorted(SLOW10_LOGS.items()):
        rows.append(
            build_row(
                {
                    "dataset": "slowmix",
                    "series": "slow10",
                    "client_count": 16,
                    "slow_rate_pct": 10,
                    "slow_client_count": slow_client_count,
                    "label": f"slow10_n{slow_client_count}",
                    "log_path": log_path,
                    "note": "16-client mixed run with a subset of clients slowed to 10%",
                }
            )
        )

    for slow_client_count, log_path in sorted(SLOW30_LOGS.items()):
        rows.append(
            build_row(
                {
                    "dataset": "slowmix",
                    "series": "slow30",
                    "client_count": 16,
                    "slow_rate_pct": 30,
                    "slow_client_count": slow_client_count,
                    "label": f"slow30_n{slow_client_count}",
                    "log_path": log_path,
                    "note": "16-client mixed run with a subset of clients slowed to 30%",
                }
            )
        )

    rows.sort(
        key=lambda row: (
            {"dedicated": 0, "slowmix_baseline": 1, "slowmix": 2}.get(row["dataset"], 9),
            row["client_count"],
            row["slow_rate_pct"] if row["slow_rate_pct"] != "" else -1,
            row["slow_client_count"],
        )
    )
    return rows


def main():
    rows = build_all_rows()
    write_csv(rows)
    write_md(rows)
    print(f"wrote {OUTCSV}")
    print(f"wrote {OUTMD}")


if __name__ == "__main__":
    main()
