#!/usr/bin/env python3
import csv
import sys
from pathlib import Path


def parse_line(line: str):
    line = line.strip()
    if not line.startswith("WRITE_SWEEP,"):
        return None

    record = {}
    for field in line.split(",")[1:]:
        if "=" not in field:
            continue
        key, value = field.split("=", 1)
        record[key] = value
    return record


def metric_sort_key(metric: str):
    if metric == "latency":
        return 0
    if metric == "bandwidth":
        return 1
    return 2


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: parse_write_size_sweep.py <board-log> <out-csv>")

    board_log = Path(sys.argv[1])
    out_csv = Path(sys.argv[2])

    rows = []
    for line in board_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        record = parse_line(line)
        if record is not None:
            rows.append(record)

    rows.sort(
        key=lambda row: (
            row.get("mode", ""),
            metric_sort_key(row.get("metric", "")),
            int(row.get("size_bytes", "0")),
        )
    )

    fieldnames = [
        "metric",
        "mode",
        "size_bytes",
        "aligned_bytes",
        "iterations",
        "sample_target_pa",
        "target_node_hint",
        "avg_ns",
        "p50_ns",
        "p95_ns",
        "min_ns",
        "max_ns",
        "elapsed_ns",
        "total_payload_bytes",
        "total_effective_bytes",
        "payload_gib_s",
        "effective_gib_s",
    ]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    print(f"parsed_rows={len(rows)}")
    print(f"output_csv={out_csv}")


if __name__ == "__main__":
    main()
