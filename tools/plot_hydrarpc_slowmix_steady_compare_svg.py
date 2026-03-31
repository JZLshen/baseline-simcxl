#!/usr/bin/env python3
import argparse
import csv
import math
from pathlib import Path

from summarize_hydrarpc_multiclient import compute_steady_stats, parse_log


ROOT = Path(__file__).resolve().parents[1]
X_VALUES = [0, 1, 2, 4, 8, 16]

BASELINE_LOG = (
    ROOT
    / "output"
    / "hydrarpc_multiclient_16client_slowmix_100pct_x0_final_20260322_184055"
    / "board.pc.com_1.device"
)

SLOW10_LOGS = {
    1: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_20260322_032117" / "slow1" / "board.pc.com_1.device",
    2: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_resume_20260322_040651" / "slow2" / "board.pc.com_1.device",
    4: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_resume_20260322_040651" / "slow4" / "board.pc.com_1.device",
    8: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_resume_20260322_040651" / "slow8" / "board.pc.com_1.device",
    16: ROOT / "output" / "hydrarpc_multiclient_16client_slow16_retry_20260322_052135" / "board.pc.com_1.device",
}

SLOW30_LOGS = {
    1: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_30pct_20260322_152926" / "slow1" / "board.pc.com_1.device",
    2: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_30pct_retrymanual_20260322_161315" / "slow2_try1" / "board.pc.com_1.device",
    4: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_30pct_tail_20260322_163500" / "slow4" / "board.pc.com_1.device",
    8: ROOT / "output" / "hydrarpc_multiclient_16client_slowmix_30pct_tail_20260322_163500" / "slow8" / "board.pc.com_1.device",
    16: ROOT / "output" / "hydrarpc_multiclient_16client_slow16_30pct_rerun_20260322_173813" / "board.pc.com_1.device",
}

OUTDIR = ROOT / "output" / "figures"


def request_rows_from_log(log_path: Path):
    parsed = parse_log(log_path)
    rows = []

    for (client_id, req_id), entry in parsed["timing_by_request"].items():
        start_ns = entry.get("client_req_start_ts_ns", 0)
        end_ns = entry.get("client_resp_done_ts_ns", 0)

        if start_ns == 0 or end_ns == 0:
            continue

        rows.append(
            {
                "client_id": client_id,
                "req_id": req_id,
                "start_ns": start_ns,
                "end_ns": end_ns,
                "latency_ns": end_ns - start_ns,
            }
        )

    rows.sort(key=lambda row: (row["client_id"], row["req_id"]))
    return rows


def output_paths(drop_first_per_client: int, paper: bool = False):
    if drop_first_per_client == 1:
        suffix = ""
    else:
        suffix = f"_drop{drop_first_per_client}"
    if paper:
        suffix += "_paper"
    outcsv = OUTDIR / f"hydrarpc_16client_slowmix_steady_compare{suffix}.csv"
    outsvg = OUTDIR / f"hydrarpc_16client_slowmix_steady_compare{suffix}.svg"
    return outcsv, outsvg


def build_rows_from_logs(label: str, log_map: dict, drop_first_per_client: int):
    rows = []
    for slow_client_count, log_path in sorted(log_map.items()):
        if not log_path.exists():
            continue
        request_rows = request_rows_from_log(log_path)
        steady_stats = compute_steady_stats(request_rows, drop_first_per_client)
        rows.append(
            {
                "series": label,
                "slow_client_count": slow_client_count,
                "throughput_mrps": steady_stats["steady_avg_throughput_mrps"],
                "source": str(log_path),
            }
        )
    return rows


def build_baseline_rows(drop_first_per_client: int):
    if not BASELINE_LOG.exists():
        return []

    request_rows = request_rows_from_log(BASELINE_LOG)
    steady_stats = compute_steady_stats(request_rows, drop_first_per_client)
    return [
        {
            "series": "All 100%",
            "slow_client_count": 0,
            "throughput_mrps": steady_stats["steady_avg_throughput_mrps"],
            "source": str(BASELINE_LOG),
        }
    ]


def fmt(value: float):
    return f"{value:.3f}".rstrip("0").rstrip(".")


def line_path(points):
    if not points:
        return ""
    parts = [f"M {points[0][0]:.2f} {points[0][1]:.2f}"]
    for x, y in points[1:]:
        parts.append(f"L {x:.2f} {y:.2f}")
    return " ".join(parts)


def circle(cx, cy, r, stroke, fill, stroke_width):
    return (
        f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="{r:.2f}" '
        f'stroke="{stroke}" fill="{fill}" stroke-width="{stroke_width:.2f}"/>'
    )


def square(cx, cy, size, stroke, fill, stroke_width):
    x = cx - size / 2.0
    y = cy - size / 2.0
    return (
        f'<rect x="{x:.2f}" y="{y:.2f}" width="{size:.2f}" height="{size:.2f}" '
        f'stroke="{stroke}" fill="{fill}" stroke-width="{stroke_width:.2f}"/>'
    )


def diamond(cx, cy, size, stroke, fill, stroke_width):
    half = size / 2.0
    points = [
        (cx, cy - half),
        (cx + half, cy),
        (cx, cy + half),
        (cx - half, cy),
    ]
    point_text = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    return (
        f'<polygon points="{point_text}" stroke="{stroke}" fill="{fill}" '
        f'stroke-width="{stroke_width:.2f}"/>'
    )


def text(x, y, value, klass="", anchor="middle"):
    extra = f' class="{klass}"' if klass else ""
    return f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}"{extra}>{value}</text>'


def build_svg(series_rows, drop_first_per_client: int, paper: bool = False):
    if paper:
        width = 520
        height = 340
        margin_left = 76
        margin_right = 18
        margin_top = 18
        margin_bottom = 60
        axis_font = 18
        tick_font = 13
        legend_font = 12
        y_grid_width = 0.9
        x_grid_width = 0.8
        axis_width = 1.2
        line_width = 2.6
        response_line_width = 1.6
        circle_radius = 4.2
        square_size = 8.0
        diamond_size = 12.0
        baseline_diamond_size = 14.0
        marker_stroke = 1.8
        xlabel_y = height - 14
        x_tick_y = 22
        tick_x_offset = 12
        tick_y_offset = 4
        y_label_x = 24.0
        legend_x_offset = 68
        legend_y = margin_top + 12
        legend_gap = 20
        legend_sample = 28
        legend_marker_x = 14
        legend_text_dx = 38
    else:
        width = 1120
        height = 648
        margin_left = 132
        margin_right = 36
        margin_top = 44
        margin_bottom = 104
        axis_font = 24
        tick_font = 18
        legend_font = 18
        y_grid_width = 1.1
        x_grid_width = 1.0
        axis_width = 1.6
        line_width = 4.2
        response_line_width = 2.4
        circle_radius = 6.8
        square_size = 12.8
        diamond_size = 18.0
        baseline_diamond_size = 18.0
        marker_stroke = 3.0
        xlabel_y = height - 18
        x_tick_y = 32
        tick_x_offset = 18
        tick_y_offset = 6
        y_label_x = 36.0
        legend_x_offset = 132
        legend_y = margin_top + 28
        legend_gap = 34
        legend_sample = 28
        legend_marker_x = 18
        legend_text_dx = 48
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_rows = [
        row
        for rows in series_rows.values()
        for row in rows
        if row["throughput_mrps"] is not None
    ]
    y_max_data = max(row["throughput_mrps"] for row in all_rows)
    y_max = max(0.26, math.ceil((y_max_data + 0.002) / 0.02) * 0.02)
    y_tick_step = 0.05
    y_ticks = []
    tick_value = 0.0
    while tick_value <= y_max + 1e-9:
        y_ticks.append(round(tick_value, 2))
        tick_value += y_tick_step

    def x_map(slow_client_count: int):
        idx = X_VALUES.index(slow_client_count)
        if len(X_VALUES) == 1:
            return margin_left + plot_w / 2.0
        return margin_left + idx * (plot_w / (len(X_VALUES) - 1))

    def y_map(value: float):
        if y_max == 0:
            return margin_top + plot_h
        return margin_top + plot_h - (value / y_max) * plot_h

    series_meta = {
        "All 100%": {
            "color": "#111827",
            "marker": "diamond",
            "dasharray": None,
            "legend_label": "0%",
        },
        "Slow 10%": {
            "color": "#c2410c",
            "marker": "square",
            "dasharray": "12 8",
            "legend_label": "10%",
        },
        "Slow 30%": {
            "color": "#2563eb",
            "marker": "circle",
            "dasharray": None,
            "legend_label": "30%",
        },
    }

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="HydraRPC 16-client steady throughput under slow-client mixes">'
    )
    parts.append(
        "<style>"
        ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{axis_font}px;fill:#1f2937" + "}"
        ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{tick_font}px;fill:#374151" + "}"
        ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        f"font-size:{legend_font}px;fill:#1f2937" + "}"
        "</style>"
    )
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>')

    for tick in y_ticks:
        y = y_map(tick)
        parts.append(
            f'<line x1="{margin_left:.2f}" y1="{y:.2f}" '
            f'x2="{margin_left + plot_w:.2f}" y2="{y:.2f}" '
            f'stroke="#d9dee7" stroke-width="{y_grid_width:.1f}"/>'
        )
        parts.append(
            text(margin_left - tick_x_offset, y + tick_y_offset, fmt(tick), "tick", "end")
        )

    for slow_client_count in X_VALUES:
        x = x_map(slow_client_count)
        parts.append(
            f'<line x1="{x:.2f}" y1="{margin_top:.2f}" '
            f'x2="{x:.2f}" y2="{margin_top + plot_h:.2f}" '
            f'stroke="#edf1f6" stroke-width="{x_grid_width:.1f}"/>'
        )
        parts.append(text(x, margin_top + plot_h + x_tick_y, str(slow_client_count), "tick"))

    parts.append(
        f'<rect x="{margin_left:.2f}" y="{margin_top:.2f}" width="{plot_w:.2f}" '
        f'height="{plot_h:.2f}" fill="none" stroke="#1f2937" stroke-width="{axis_width:.1f}"/>'
    )

    baseline_row = series_rows["All 100%"][0]
    baseline_x = x_map(baseline_row["slow_client_count"])
    baseline_y = y_map(baseline_row["throughput_mrps"])

    connector_targets = {}
    for label in ["All 100%", "Slow 10%", "Slow 30%"]:
        rows = [
            row for row in series_rows[label] if row["throughput_mrps"] is not None
        ]
        points = [(x_map(row["slow_client_count"]), y_map(row["throughput_mrps"])) for row in rows]
        meta = series_meta[label]
        if len(points) >= 2:
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
            if meta["marker"] == "circle":
                parts.append(circle(x, y, circle_radius, meta["color"], "#ffffff", marker_stroke))
            elif meta["marker"] == "square":
                parts.append(square(x, y, square_size, meta["color"], "#ffffff", marker_stroke))
            else:
                parts.append(diamond(x, y, diamond_size, meta["color"], meta["color"], max(marker_stroke - 0.4, 1.4)))
        if rows and label != "All 100%":
            connector_targets[label] = rows[0]

    for label in ["Slow 10%", "Slow 30%"]:
        row = connector_targets[label]
        target_x = x_map(row["slow_client_count"])
        target_y = y_map(row["throughput_mrps"])
        color = series_meta[label]["color"]
        parts.append(
            f'<line x1="{baseline_x:.2f}" y1="{baseline_y:.2f}" '
            f'x2="{target_x:.2f}" y2="{target_y:.2f}" '
            f'stroke="{color}" stroke-width="{response_line_width:.1f}" stroke-opacity="0.45" '
            f'stroke-dasharray="4 7"/>'
        )
    parts.append(
        diamond(
            baseline_x,
            baseline_y,
            baseline_diamond_size,
            "#111827",
            "#111827",
            max(marker_stroke - 0.4, 1.4),
        )
    )

    parts.append(text(margin_left + plot_w / 2.0, xlabel_y, "Client", "axis"))
    parts.append(
        f'<text x="{y_label_x:.2f}" y="{margin_top + plot_h / 2.0:.2f}" text-anchor="middle" '
        f'transform="rotate(-90 {y_label_x:.2f} {margin_top + plot_h / 2.0:.2f})" class="axis">'
        "Throughput (Mops)"
        "</text>"
    )

    legend_x = margin_left + plot_w - legend_x_offset

    legend_rows = [
        ("All 100%", 0),
        ("Slow 10%", legend_gap),
        ("Slow 30%", legend_gap * 2),
    ]
    for label, offset in legend_rows:
        y = legend_y + offset
        mx = legend_x + legend_marker_x
        meta = series_meta[label]
        if label != "All 100%" and meta["dasharray"]:
            parts.append(
                f'<line x1="{legend_x:.2f}" y1="{y:.2f}" x2="{legend_x + legend_sample:.2f}" y2="{y:.2f}" '
                f'stroke="{meta["color"]}" stroke-width="{line_width:.1f}" stroke-dasharray="{meta["dasharray"]}"/>'
            )
        elif label != "All 100%":
            parts.append(
                f'<line x1="{legend_x:.2f}" y1="{y:.2f}" x2="{legend_x + legend_sample:.2f}" y2="{y:.2f}" '
                f'stroke="{meta["color"]}" stroke-width="{line_width:.1f}"/>'
            )
        if meta["marker"] == "circle":
            parts.append(circle(mx, y, circle_radius, meta["color"], "#ffffff", marker_stroke))
        elif meta["marker"] == "square":
            parts.append(square(mx, y, square_size, meta["color"], "#ffffff", marker_stroke))
        else:
            parts.append(diamond(mx, y, max(diamond_size - 2.0, 10.0), meta["color"], meta["color"], max(marker_stroke - 0.4, 1.4)))
        parts.append(text(legend_x + legend_text_dx, y + tick_y_offset, meta["legend_label"], "legend", "start"))

    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def build_empty_svg(message: str):
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="720" height="160" '
        'viewBox="0 0 720 160" role="img" aria-label="HydraRPC slowmix steady compare unavailable">\n'
        '<rect x="0" y="0" width="720" height="160" fill="#ffffff"/>\n'
        '<text x="360" y="86" text-anchor="middle" '
        'style="font-family:Times New Roman,serif;font-size:24px;fill:#1f2937">'
        f"{message}</text>\n"
        "</svg>\n"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop-first-per-client", type=int, default=1)
    parser.add_argument("--paper", action="store_true")
    args = parser.parse_args()

    baseline_rows = build_baseline_rows(args.drop_first_per_client)
    rows_10pct = build_rows_from_logs("Slow 10%", SLOW10_LOGS, args.drop_first_per_client)
    rows_30pct = build_rows_from_logs("Slow 30%", SLOW30_LOGS, args.drop_first_per_client)

    series_rows = {
        "All 100%": baseline_rows,
        "Slow 10%": rows_10pct,
        "Slow 30%": rows_30pct,
    }

    outcsv, outsvg = output_paths(args.drop_first_per_client, paper=args.paper)
    OUTDIR.mkdir(parents=True, exist_ok=True)

    with outcsv.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["series", "slow_client_count", "throughput_mrps", "source"],
        )
        writer.writeheader()
        for label in ["All 100%", "Slow 10%", "Slow 30%"]:
            for row in series_rows[label]:
                writer.writerow(row)

    if not baseline_rows:
        outsvg.write_text(
            build_empty_svg("Slowmix baseline log unavailable"),
            encoding="utf-8",
        )
    else:
        outsvg.write_text(
            build_svg(series_rows, args.drop_first_per_client, paper=args.paper),
            encoding="utf-8",
        )

    print(f"wrote {outcsv}")
    print(f"wrote {outsvg}")


if __name__ == "__main__":
    main()
