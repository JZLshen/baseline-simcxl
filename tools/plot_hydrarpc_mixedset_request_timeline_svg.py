#!/usr/bin/env python3
import argparse
import csv
from datetime import datetime
from pathlib import Path

from plot_hydrarpc_request_timeline_svg import parse_log, render_experiment_svg
from plot_hydrarpc_slowmix_steady_compare_svg import (
    BASELINE_LOG,
    ROOT,
    SLOW10_LOGS,
    SLOW30_LOGS,
)


def build_experiments():
    experiments = [
        {
            "series": "100%",
            "slow_client_count": 0,
            "label": "100pct_x0",
            "title": "16 clients, all at full rate",
            "subtitle_tag": "baseline",
            "log_path": BASELINE_LOG,
        }
    ]

    for slow_client_count, log_path in sorted(SLOW10_LOGS.items()):
        experiments.append(
            {
                "series": "slow10",
                "slow_client_count": slow_client_count,
                "label": f"slow10_n{slow_client_count}",
                "title": f"16 clients, {slow_client_count} slow clients at 10% rate",
                "subtitle_tag": f"slow10%, N={slow_client_count}",
                "log_path": log_path,
            }
        )

    for slow_client_count, log_path in sorted(SLOW30_LOGS.items()):
        experiments.append(
            {
                "series": "slow30",
                "slow_client_count": slow_client_count,
                "label": f"slow30_n{slow_client_count}",
                "title": f"16 clients, {slow_client_count} slow clients at 30% rate",
                "subtitle_tag": f"slow30%, N={slow_client_count}",
                "log_path": log_path,
            }
        )

    return experiments


def default_output_dir():
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "output" / f"hydrarpc_multiclient_16client_mixedset_timeline_paper_{stamp}"


def summarize_rows(rows):
    if not rows:
        raise ValueError("no complete request-response rows found")

    client_ids = {row["client_id"] for row in rows}
    req_ids = {row["req_id"] for row in rows}
    count_per_client = max(req_ids) + 1
    client_count = max(client_ids) + 1
    return client_count, count_per_client


def main():
    parser = argparse.ArgumentParser(
        description="Render request-response timeline SVGs for the 16-client mixed-load experiments"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir(),
        help="Directory to write SVGs and manifest into",
    )
    parser.add_argument(
        "--drop-first-per-client",
        type=int,
        default=1,
        help="Color req_id < N as warmup requests",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    manifest_rows = []
    for item in build_experiments():
        rows = parse_log(item["log_path"])
        client_count, count_per_client = summarize_rows(rows)
        if item["slow_client_count"] == 0:
            slow_note = "all clients run at the full request rate"
        elif item["slow_client_count"] == client_count:
            slow_note = "all client rows are slow-client rows"
        else:
            slow_note = f"slow clients are c0-c{item['slow_client_count'] - 1}"

        subtitle = (
            f"req 0 is warm-up and excluded from steady-state metrics; {slow_note}"
        )
        svg_text = render_experiment_svg(
            rows=rows,
            client_count=client_count,
            count_per_client=count_per_client,
            drop_first_per_client=args.drop_first_per_client,
            title=item["title"],
            subtitle=subtitle,
            slow_client_count=item["slow_client_count"],
            paper_style=True,
            show_gap_panel=False,
        )
        svg_path = args.output_dir / f"hydrarpc_timeline_{item['label']}.svg"
        svg_path.write_text(svg_text, encoding="utf-8")
        manifest_rows.append(
            {
                "series": item["series"],
                "slow_client_count": item["slow_client_count"],
                "label": item["label"],
                "client_count": client_count,
                "count_per_client": count_per_client,
                "total_requests": len(rows),
                "log_path": str(item["log_path"]),
                "svg_path": str(svg_path),
            }
        )
        print(f"wrote {svg_path}")

    manifest_path = args.output_dir / "manifest.csv"
    with manifest_path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "series",
                "slow_client_count",
                "label",
                "client_count",
                "count_per_client",
                "total_requests",
                "log_path",
                "svg_path",
            ],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)
    print(f"wrote {manifest_path}")


if __name__ == "__main__":
    main()
