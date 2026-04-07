#!/usr/bin/env python3

import argparse
import csv
import json
from pathlib import Path


SLOW_CLIENT_COUNTS = (1, 2, 4, 8)
SLOW_REQUEST_PCTS = (0, 25, 50, 100)

CSV_FIELDS = [
    "slow_client_count",
    "slow_request_pct",
    "throughput_kops",
    "latency_median_ns",
    "latency_p90_ns",
    "latency_p99_ns",
    "server_poll_median_ns",
    "server_poll_p90_ns",
    "server_poll_p99_ns",
    "server_execute_median_ns",
    "server_execute_p90_ns",
    "server_execute_p99_ns",
    "server_response_median_ns",
    "server_response_p90_ns",
    "server_response_p99_ns",
]


def fmt_metric(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def load_drop1_stats(result_json_path: Path):
    payload = json.loads(result_json_path.read_text())
    stats = payload.get("steady_drop1_stats")
    if not stats:
        raise ValueError(f"missing steady_drop1_stats in {result_json_path}")
    return stats


def build_row(slow_client_count: int, slow_request_pct: int, stats):
    return {
        "slow_client_count": slow_client_count,
        "slow_request_pct": slow_request_pct,
        "throughput_kops": fmt_metric(stats.get("steady_avg_throughput_kops")),
        "latency_median_ns": fmt_metric(stats.get("steady_median_latency_ns")),
        "latency_p90_ns": fmt_metric(stats.get("steady_p90_latency_ns")),
        "latency_p99_ns": fmt_metric(stats.get("steady_p99_latency_ns")),
        "server_poll_median_ns": fmt_metric(
            stats.get("steady_median_server_poll_notify_ns")
        ),
        "server_poll_p90_ns": fmt_metric(stats.get("steady_p90_server_poll_notify_ns")),
        "server_poll_p99_ns": fmt_metric(stats.get("steady_p99_server_poll_notify_ns")),
        "server_execute_median_ns": fmt_metric(
            stats.get("steady_median_server_execute_ns")
        ),
        "server_execute_p90_ns": fmt_metric(stats.get("steady_p90_server_execute_ns")),
        "server_execute_p99_ns": fmt_metric(stats.get("steady_p99_server_execute_ns")),
        "server_response_median_ns": fmt_metric(
            stats.get("steady_median_server_response_ns")
        ),
        "server_response_p90_ns": fmt_metric(stats.get("steady_p90_server_response_ns")),
        "server_response_p99_ns": fmt_metric(stats.get("steady_p99_server_response_ns")),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build the dedicated sparse moti drop1 table from a single batch root."
    )
    parser.add_argument(
        "--moti-root",
        required=True,
        help="Path to the moti batch root containing sparse16_d*_c*/result.json.",
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Directory where moti_sparse_polling_dedicated.csv is written.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    moti_root = Path(args.moti_root).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    rows = []
    manifest_rows = []

    for slow_client_count in SLOW_CLIENT_COUNTS:
        for slow_request_pct in SLOW_REQUEST_PCTS:
            case_dir = moti_root / f"sparse16_d{slow_request_pct}_c{slow_client_count}"
            result_json_path = case_dir / "result.json"
            if not result_json_path.is_file():
                raise FileNotFoundError(result_json_path)

            stats = load_drop1_stats(result_json_path)
            rows.append(build_row(slow_client_count, slow_request_pct, stats))
            manifest_rows.append(
                {
                    "table": "moti_sparse_polling_dedicated",
                    "row_key": f"sc{slow_client_count}:pct{slow_request_pct}",
                    "source_type": "result_json",
                    "source_path": str(result_json_path),
                    "note": "recomputed from moti sparse result.json at drop1",
                }
            )

    csv_path = outdir / "moti_sparse_polling_dedicated.csv"
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    manifest_path = outdir / "source_manifest.csv"
    with manifest_path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["table", "row_key", "source_type", "source_path", "note"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    print(csv_path)
    print(manifest_path)


if __name__ == "__main__":
    main()
