#!/usr/bin/env python3
import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path

from plot_hydrarpc_slowmix_steady_compare_svg import SLOW10_LOGS, SLOW30_LOGS
from summarize_hydrarpc_multiclient import (
    compute_steady_stats,
    compute_window_stats,
    parse_log,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLIENT_COUNTS = [1, 2, 4, 8, 16, 32]


def parse_counts(raw: str):
    return [int(part) for part in raw.split() if part.strip()]


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
    return ";".join(f"{req_count}x{profile[req_count]}" for req_count in sorted(profile))


def discover_series_log(root: Path, series: str, client_count: int):
    if series == "coherent":
        candidates = [
            root
            / f"c{client_count}"
            / f"coherent_mgreedy_g0_bnone_c{client_count}_r20"
            / "board.pc.com_1.device",
            root / f"coherent_mgreedy_g0_bnone_c{client_count}_r20" / "board.pc.com_1.device",
            root / f"c{client_count}" / "board.pc.com_1.device",
        ]
    elif series == "noncoherent":
        candidates = [
            root
            / f"c{client_count}"
            / f"dedicated_mgreedy_g0_bnone_c{client_count}_r20"
            / "board.pc.com_1.device",
            root / f"dedicated_mgreedy_g0_bnone_c{client_count}_r20" / "board.pc.com_1.device",
            root / f"c{client_count}" / "board.pc.com_1.device",
        ]
    else:
        raise ValueError(f"unknown series: {series}")

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def build_row_from_log(
    dataset: str,
    series: str,
    x_kind: str,
    x_value: int,
    client_count: int,
    slow_client_count,
    slow_rate_pct,
    log_path: Path,
):
    _, request_rows, _ = parse_log(log_path)
    window = compute_window_stats(request_rows)
    row = {
        "dataset": dataset,
        "series": series,
        "status": "ok",
        "x_kind": x_kind,
        "x_value": x_value,
        "client_count": client_count,
        "slow_client_count": slow_client_count,
        "slow_rate_pct": slow_rate_pct,
        "requests_per_client_profile": requests_per_client_profile(request_rows),
        "total_requests": window["total_requests"],
        "aggregate_throughput_mrps": fmt(window["aggregate_throughput_mrps"]),
        "aggregate_avg_latency_ns": fmt_ns(window["avg_latency_ns"]),
        "aggregate_median_latency_ns": fmt_ns(window["median_latency_ns"]),
        "log_path": str(log_path),
    }

    for drop in (1, 2, 5):
        steady = compute_steady_stats(request_rows, drop)
        row[f"steady_drop{drop}_requests"] = steady["steady_requests"]
        row[f"steady_drop{drop}_throughput_mrps"] = fmt(
            steady["steady_avg_throughput_mrps"]
        )
        row[f"steady_drop{drop}_avg_reqresp_latency_ns"] = fmt_ns(
            steady["steady_avg_reqresp_latency_ns"]
        )
        row[f"steady_drop{drop}_avg_latency_ns"] = fmt_ns(
            steady["steady_avg_latency_ns"]
        )

    return row


def build_missing_row(
    dataset: str,
    series: str,
    x_kind: str,
    x_value: int,
    client_count: int,
    slow_client_count,
    slow_rate_pct,
):
    row = {
        "dataset": dataset,
        "series": series,
        "status": "missing",
        "x_kind": x_kind,
        "x_value": x_value,
        "client_count": client_count,
        "slow_client_count": slow_client_count,
        "slow_rate_pct": slow_rate_pct,
        "requests_per_client_profile": "",
        "total_requests": "",
        "aggregate_throughput_mrps": "",
        "aggregate_avg_latency_ns": "",
        "aggregate_median_latency_ns": "",
        "log_path": "",
    }
    for drop in (1, 2, 5):
        row[f"steady_drop{drop}_requests"] = ""
        row[f"steady_drop{drop}_throughput_mrps"] = ""
        row[f"steady_drop{drop}_avg_reqresp_latency_ns"] = ""
        row[f"steady_drop{drop}_avg_latency_ns"] = ""
    return row


def build_series_rows(root: Path, series: str, client_counts):
    rows = []
    for client_count in client_counts:
        log_path = discover_series_log(root, series, client_count)
        if log_path is None:
            rows.append(
                build_missing_row(
                    dataset="coherence_compare",
                    series=series,
                    x_kind="client_count",
                    x_value=client_count,
                    client_count=client_count,
                    slow_client_count="",
                    slow_rate_pct="",
                )
            )
            continue

        rows.append(
            build_row_from_log(
                dataset="coherence_compare",
                series=series,
                x_kind="client_count",
                x_value=client_count,
                client_count=client_count,
                slow_client_count="",
                slow_rate_pct="",
                log_path=log_path,
            )
        )
    return rows


def build_slow_rows(series: str, log_map: dict, slow_rate_pct: int):
    rows = []
    for slow_client_count, log_path in sorted(log_map.items()):
        rows.append(
            build_row_from_log(
                dataset="slowmix",
                series=series,
                x_kind="slow_client_count",
                x_value=slow_client_count,
                client_count=16,
                slow_client_count=slow_client_count,
                slow_rate_pct=slow_rate_pct,
                log_path=log_path.resolve(),
            )
        )
    return rows


def write_csv(rows, csv_path: Path):
    fieldnames = [
        "dataset",
        "series",
        "status",
        "x_kind",
        "x_value",
        "client_count",
        "slow_client_count",
        "slow_rate_pct",
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
    ]
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows, json_path: Path):
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, ensure_ascii=True)
        fh.write("\n")


def fmt_tick(value: float):
    return f"{value:.2f}".rstrip("0").rstrip(".")


def text(x, y, value, klass="", anchor="middle"):
    extra = f' class="{klass}"' if klass else ""
    return f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}"{extra}>{value}</text>'


def circle(cx, cy, radius, stroke, fill, stroke_width):
    return (
        f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="{radius:.2f}" '
        f'stroke="{stroke}" fill="{fill}" stroke-width="{stroke_width:.2f}"/>'
    )


def line_path(points):
    if not points:
        return ""
    parts = [f"M {points[0][0]:.2f} {points[0][1]:.2f}"]
    for x, y in points[1:]:
        parts.append(f"L {x:.2f} {y:.2f}")
    return " ".join(parts)


def build_plot_rows(rows, drop_first_per_client: int):
    key = f"steady_drop{drop_first_per_client}_throughput_mrps"
    plot_rows = []
    for row in rows:
        if row["dataset"] != "coherence_compare":
            continue
        if row["status"] != "ok":
            continue
        value = row.get(key, "")
        if value == "":
            continue
        plot_rows.append(
            {
                "series": row["series"],
                "client_count": int(row["client_count"]),
                "throughput_mrps": float(value),
            }
        )
    return plot_rows


def write_plot_csv(rows, csv_path: Path):
    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["series", "client_count", "throughput_mrps"]
        )
        writer.writeheader()
        writer.writerows(rows)


def build_svg(plot_rows, svg_path: Path, paper: bool = False):
    if paper:
        width = 520
        height = 332
        margin_left = 76
        margin_right = 18
        margin_top = 18
        margin_bottom = 58
        axis_font = 18
        tick_font = 13
        legend_font = 13
        grid_width = 0.9
        x_grid_width = 0.8
        axis_width = 1.2
        line_width = 2.6
        marker_radius = 4.2
        marker_stroke = 1.8
        legend_sample = 34
        legend_gap = 22
        legend_x_offset = 160
        xlabel_y = height - 14
        y_label_x = 24.0
        tick_x_offset = 12
        tick_y_offset = 4
        x_tick_y = 22
        legend_text_dx = 46
        legend_marker_x = 17
        legend_y = margin_top + 14
    else:
        width = 1024
        height = 620
        margin_left = 118
        margin_right = 44
        margin_top = 38
        margin_bottom = 92
        axis_font = 24
        tick_font = 18
        legend_font = 18
        grid_width = 1.1
        x_grid_width = 1.0
        axis_width = 1.6
        line_width = 3.2
        marker_radius = 5.4
        marker_stroke = 2.4
        legend_sample = 42
        legend_gap = 30
        legend_x_offset = 200
        xlabel_y = height - 24
        y_label_x = 38.0
        tick_x_offset = 18
        tick_y_offset = 6
        x_tick_y = 32
        legend_text_dx = 56
        legend_marker_x = 21
        legend_y = margin_top + 18
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_values = [row["throughput_mrps"] for row in plot_rows]
    if not all_values:
        raise ValueError("no coherent/noncoherent rows available for plotting")

    client_counts = sorted({row["client_count"] for row in plot_rows})
    y_max_data = max(all_values)
    y_max = max(0.6, math.ceil((y_max_data + 0.05) / 0.2) * 0.2)
    y_ticks = []
    tick = 0.0
    while tick <= y_max + 1e-9:
        y_ticks.append(round(tick, 2))
        tick += 0.2

    def x_map(client_count: int):
        idx = client_counts.index(client_count)
        if len(client_counts) == 1:
            return margin_left + plot_w / 2.0
        return margin_left + idx * (plot_w / (len(client_counts) - 1))

    def y_map(value: float):
        return margin_top + plot_h - (value / y_max) * plot_h

    series_meta = {
        "noncoherent": {
            "label": "Non-coherent",
            "color": "#c2410c",
            "dasharray": None,
        },
        "coherent": {
            "label": "Coherent",
            "color": "#2563eb",
            "dasharray": "10 6",
        },
    }

    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img" '
            f'aria-label="HydraRPC throughput: coherent vs non-coherent">'
        ),
        "<style>"
        ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{axis_font}px;fill:#1f2937" + "}"
        ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{tick_font}px;fill:#374151" + "}"
        ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{legend_font}px;fill:#1f2937" + "}"
        "</style>",
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>',
    ]

    for tick_value in y_ticks:
        y = y_map(tick_value)
        parts.append(
            f'<line x1="{margin_left:.2f}" y1="{y:.2f}" '
            f'x2="{margin_left + plot_w:.2f}" y2="{y:.2f}" '
            f'stroke="#d9dee7" stroke-width="{grid_width:.1f}"/>'
        )
        parts.append(
            text(
                margin_left - tick_x_offset,
                y + tick_y_offset,
                fmt_tick(tick_value),
                "tick",
                "end",
            )
        )

    for client_count in client_counts:
        x = x_map(client_count)
        parts.append(
            f'<line x1="{x:.2f}" y1="{margin_top:.2f}" '
            f'x2="{x:.2f}" y2="{margin_top + plot_h:.2f}" '
            f'stroke="#edf1f6" stroke-width="{x_grid_width:.1f}"/>'
        )
        parts.append(text(x, margin_top + plot_h + x_tick_y, str(client_count), "tick"))

    parts.append(
        f'<line x1="{margin_left:.2f}" y1="{margin_top:.2f}" '
        f'x2="{margin_left:.2f}" y2="{margin_top + plot_h:.2f}" '
        f'stroke="#111827" stroke-width="{axis_width:.1f}"/>'
    )
    parts.append(
        f'<line x1="{margin_left:.2f}" y1="{margin_top + plot_h:.2f}" '
        f'x2="{margin_left + plot_w:.2f}" y2="{margin_top + plot_h:.2f}" '
        f'stroke="#111827" stroke-width="{axis_width:.1f}"/>'
    )

    series_to_points = {}
    for series in ("noncoherent", "coherent"):
        series_rows = sorted(
            [row for row in plot_rows if row["series"] == series],
            key=lambda row: row["client_count"],
        )
        points = [
            (x_map(row["client_count"]), y_map(row["throughput_mrps"]))
            for row in series_rows
        ]
        series_to_points[series] = points
        meta = series_meta[series]
        dash_attr = (
            f' stroke-dasharray="{meta["dasharray"]}"'
            if meta["dasharray"]
            else ""
        )
        parts.append(
            f'<path d="{line_path(points)}" fill="none" stroke="{meta["color"]}" '
            f'stroke-width="{line_width:.1f}" stroke-linecap="round" stroke-linejoin="round"{dash_attr}/>'
        )
        for x, y in points:
            parts.append(circle(x, y, marker_radius, meta["color"], "#ffffff", marker_stroke))

    parts.append(
        text(
            margin_left + plot_w / 2.0,
            xlabel_y,
            "Client",
            "axis",
        )
    )
    parts.append(
        f'<text x="{y_label_x:.2f}" y="{margin_top + plot_h / 2.0:.2f}" text-anchor="middle" '
        f'transform="rotate(-90 {y_label_x:.2f} {margin_top + plot_h / 2.0:.2f})" class="axis">'
        "Throughput (Mops)"
        "</text>"
    )

    legend_x = margin_left + plot_w - legend_x_offset
    for index, series in enumerate(("noncoherent", "coherent")):
        meta = series_meta[series]
        y = legend_y + index * legend_gap
        dash_attr = (
            f' stroke-dasharray="{meta["dasharray"]}"'
            if meta["dasharray"]
            else ""
        )
        parts.append(
            f'<line x1="{legend_x:.2f}" y1="{y:.2f}" x2="{legend_x + legend_sample:.2f}" y2="{y:.2f}" '
            f'stroke="{meta["color"]}" stroke-width="{line_width:.1f}" stroke-linecap="round"{dash_attr}/>'
        )
        parts.append(
            circle(
                legend_x + legend_marker_x,
                y,
                marker_radius - 0.4,
                meta["color"],
                "#ffffff",
                max(marker_stroke - 0.2, 1.4),
            )
        )
        parts.append(text(legend_x + legend_text_dx, y + tick_y_offset, meta["label"], "legend", "start"))

    parts.append("</svg>")
    svg_path.write_text("\n".join(parts), encoding="utf-8")


def build_bundle(coherent_root: Path, noncoherent_root: Path, outdir: Path, client_counts):
    rows = []
    rows.extend(build_series_rows(noncoherent_root, "noncoherent", client_counts))
    rows.extend(build_series_rows(coherent_root, "coherent", client_counts))
    rows.extend(build_slow_rows("slow10", SLOW10_LOGS, 10))
    rows.extend(build_slow_rows("slow30", SLOW30_LOGS, 30))

    rows.sort(
        key=lambda row: (
            {"coherence_compare": 0, "slowmix": 1}.get(row["dataset"], 9),
            {"noncoherent": 0, "coherent": 1, "slow10": 2, "slow30": 3}.get(
                row["series"], 9
            ),
            int(row["x_value"]),
        )
    )

    outdir.mkdir(parents=True, exist_ok=True)
    write_csv(rows, outdir / "combined_metrics.csv")
    write_json(rows, outdir / "combined_metrics.json")

    plot_rows = build_plot_rows(rows, drop_first_per_client=5)
    write_plot_csv(plot_rows, outdir / "coherent_vs_noncoherent_plot_points.csv")
    build_svg(plot_rows, outdir / "coherent_vs_noncoherent_steady_drop5.svg", paper=False)
    build_svg(
        plot_rows,
        outdir / "coherent_vs_noncoherent_steady_drop5_paper.svg",
        paper=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coherent-root", required=True)
    parser.add_argument("--noncoherent-root", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument(
        "--client-counts",
        default="1 2 4 8 16 32",
        help="Whitespace-separated list of client counts for coherent/noncoherent runs",
    )
    args = parser.parse_args()

    build_bundle(
        coherent_root=Path(args.coherent_root),
        noncoherent_root=Path(args.noncoherent_root),
        outdir=Path(args.outdir),
        client_counts=parse_counts(args.client_counts),
    )


if __name__ == "__main__":
    main()
