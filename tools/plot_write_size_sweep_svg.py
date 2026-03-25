#!/usr/bin/env python3
import csv
import math
import sys
from pathlib import Path


DRAM_COLOR = "#1f4e79"
CXL_COLOR = "#c66a1c"
BG = "#ffffff"
AXIS = "#1f1f1f"
GRID = "#d8d8d8"
LABEL = "#2b2b2b"


def fmt_size(size_bytes: int) -> str:
    if size_bytes >= 1024 * 1024 and size_bytes % (1024 * 1024) == 0:
        return f"{size_bytes // (1024 * 1024)}M"
    if size_bytes >= 1024 and size_bytes % 1024 == 0:
        return f"{size_bytes // 1024}K"
    return str(size_bytes)


def text(x, y, value, klass="", anchor="middle"):
    extra = f' class="{klass}"' if klass else ""
    return f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}"{extra}>{value}</text>'


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


def build_panel(parts, title, x0, y0, width, height, x_ticks, y_ticks, x_map, y_map):
    parts.append(
        f'<rect x="{x0:.2f}" y="{y0:.2f}" width="{width:.2f}" height="{height:.2f}" '
        f'fill="none" stroke="{AXIS}" stroke-width="1.7"/>'
    )

    for tick in y_ticks:
        y = y_map(tick)
        parts.append(
            f'<line x1="{x0:.2f}" y1="{y:.2f}" x2="{x0 + width:.2f}" y2="{y:.2f}" '
            f'stroke="{GRID}" stroke-width="1.0"/>'
        )

    for tick in x_ticks:
        x = x_map(tick)
        parts.append(
            f'<line x1="{x:.2f}" y1="{y0:.2f}" x2="{x:.2f}" y2="{y0 + height:.2f}" '
            f'stroke="{GRID}" stroke-width="1.0"/>'
        )

    parts.append(text(x0 + width / 2.0, y0 - 14, title, "panel"))


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: plot_write_size_sweep_svg.py <combined-results.csv> <output.svg>")

    csv_path = Path(sys.argv[1])
    out_svg = Path(sys.argv[2])

    rows = []
    with csv_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)

    latency_rows = [row for row in rows if row["metric"] == "latency"]
    bandwidth_rows = [row for row in rows if row["metric"] == "bandwidth"]

    x_values = sorted({int(row["size_bytes"]) for row in rows})
    latency_values = [float(row["p50_ns"]) for row in latency_rows]
    bandwidth_values = [float(row["payload_gib_s"]) for row in bandwidth_rows]

    latency_min = min(latency_values)
    latency_max = max(latency_values)
    bw_max = max(bandwidth_values)

    latency_ticks = [100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0, 1000000.0, 3000000.0, 10000000.0]
    latency_ticks = [tick for tick in latency_ticks if latency_min * 0.8 <= tick <= latency_max * 1.2]
    if not latency_ticks:
        latency_ticks = [latency_min, latency_max]

    bandwidth_ticks = [round(i * 0.05, 2) for i in range(0, int(math.ceil((bw_max + 0.02) / 0.05)) + 1)]

    label_x_ticks = []
    for tick in x_values:
        if tick in {8, 64, 512, 4096, 32768, 262144, 1048576, 4194304}:
            label_x_ticks.append(tick)
    if x_values[-1] not in label_x_ticks:
        label_x_ticks.append(x_values[-1])

    width = 1320
    height = 620
    margin_top = 58
    margin_bottom = 90
    margin_left = 92
    margin_right = 36
    gap = 80
    panel_w = (width - margin_left - margin_right - gap) / 2.0
    panel_h = height - margin_top - margin_bottom

    panel1_x = margin_left
    panel2_x = margin_left + panel_w + gap
    panel_y = margin_top

    log_x_min = math.log2(x_values[0])
    log_x_max = math.log2(x_values[-1])

    def x_map_factory(panel_x):
        def mapper(size_bytes: int):
            if log_x_max == log_x_min:
                return panel_x + panel_w / 2.0
            return panel_x + (math.log2(size_bytes) - log_x_min) / (log_x_max - log_x_min) * panel_w
        return mapper

    latency_log_min = math.log10(latency_ticks[0])
    latency_log_max = math.log10(latency_ticks[-1])

    def latency_y_map(value: float):
        if latency_log_max == latency_log_min:
            return panel_y + panel_h / 2.0
        return panel_y + panel_h - (math.log10(value) - latency_log_min) / (latency_log_max - latency_log_min) * panel_h

    bw_min = 0.0
    bw_max_axis = max(0.45, math.ceil((bw_max + 0.02) / 0.05) * 0.05)

    def bandwidth_y_map(value: float):
        if bw_max_axis == bw_min:
            return panel_y + panel_h / 2.0
        return panel_y + panel_h - (value - bw_min) / (bw_max_axis - bw_min) * panel_h

    latency_series = {"dram": [], "cxl": []}
    bandwidth_series = {"dram": [], "cxl": []}

    x_map1 = x_map_factory(panel1_x)
    x_map2 = x_map_factory(panel2_x)

    for row in latency_rows:
        latency_series[row["mode"]].append((x_map1(int(row["size_bytes"])), latency_y_map(float(row["p50_ns"]))))
    for row in bandwidth_rows:
        bandwidth_series[row["mode"]].append((x_map2(int(row["size_bytes"])), bandwidth_y_map(float(row["payload_gib_s"]))))

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="DRAM versus CXL write latency and bandwidth by write size">'
    )
    parts.append(
        "<style>"
        ".title{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:28px;fill:#2b2b2b}"
        ".panel{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:20px;fill:#2b2b2b}"
        ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:20px;fill:#2b2b2b}"
        ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:16px;fill:#2b2b2b}"
        ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:18px;fill:#2b2b2b}"
        ".note{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;font-size:14px;fill:#5a5a5a}"
        "</style>"
    )
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="{BG}"/>')
    parts.append(text(width / 2.0, 34, "DRAM vs CXL Write Size Sweep", "title"))

    build_panel(parts, "Latency (p50, ns)", panel1_x, panel_y, panel_w, panel_h, label_x_ticks, latency_ticks, x_map1, latency_y_map)
    build_panel(parts, "Bandwidth (payload GiB/s)", panel2_x, panel_y, panel_w, panel_h, label_x_ticks, bandwidth_ticks, x_map2, bandwidth_y_map)

    for tick in latency_ticks:
        parts.append(text(panel1_x - 14, latency_y_map(tick) + 6, f"{tick:g}", "tick", "end"))
    for tick in bandwidth_ticks:
        parts.append(text(panel2_x - 14, bandwidth_y_map(tick) + 6, f"{tick:.2f}".rstrip("0").rstrip("."), "tick", "end"))
    for tick in label_x_ticks:
        parts.append(text(x_map1(tick), panel_y + panel_h + 28, fmt_size(tick), "tick"))
        parts.append(text(x_map2(tick), panel_y + panel_h + 28, fmt_size(tick), "tick"))

    parts.append(text(panel1_x + panel_w / 2.0, height - 24, "Write size", "axis"))
    parts.append(text(panel2_x + panel_w / 2.0, height - 24, "Write size", "axis"))

    parts.append(
        f'<g transform="translate({34:.2f},{panel_y + panel_h / 2.0:.2f}) rotate(-90)">{text(0, 0, "Latency (ns)", "axis")}</g>'
    )
    parts.append(
        f'<g transform="translate({panel2_x - 58:.2f},{panel_y + panel_h / 2.0:.2f}) rotate(-90)">{text(0, 0, "Payload GiB/s", "axis")}</g>'
    )

    parts.append(
        f'<path d="{line_path(latency_series["dram"])}" fill="none" stroke="{DRAM_COLOR}" stroke-width="3.2"/>'
    )
    parts.append(
        f'<path d="{line_path(latency_series["cxl"])}" fill="none" stroke="{CXL_COLOR}" stroke-width="3.2" stroke-dasharray="10 7"/>'
    )
    parts.append(
        f'<path d="{line_path(bandwidth_series["dram"])}" fill="none" stroke="{DRAM_COLOR}" stroke-width="3.2"/>'
    )
    parts.append(
        f'<path d="{line_path(bandwidth_series["cxl"])}" fill="none" stroke="{CXL_COLOR}" stroke-width="3.2" stroke-dasharray="10 7"/>'
    )

    for x, y in latency_series["dram"]:
        parts.append(circle(x, y, 5.5, DRAM_COLOR, BG, 2.5))
    for x, y in latency_series["cxl"]:
        parts.append(square(x, y, 10.5, CXL_COLOR, BG, 2.5))
    for x, y in bandwidth_series["dram"]:
        parts.append(circle(x, y, 5.5, DRAM_COLOR, BG, 2.5))
    for x, y in bandwidth_series["cxl"]:
        parts.append(square(x, y, 10.5, CXL_COLOR, BG, 2.5))

    legend_x = width - 220
    legend_y = 78
    parts.append(
        f'<rect x="{legend_x - 16:.2f}" y="{legend_y - 20:.2f}" width="190" height="72" '
        f'fill="#ffffff" fill-opacity="0.92" stroke="#d0d0d0" stroke-width="1.0" rx="8"/>'
    )
    parts.append(
        f'<line x1="{legend_x:.2f}" y1="{legend_y:.2f}" x2="{legend_x + 42:.2f}" y2="{legend_y:.2f}" '
        f'stroke="{DRAM_COLOR}" stroke-width="3.2"/>'
    )
    parts.append(circle(legend_x + 21, legend_y, 5.5, DRAM_COLOR, BG, 2.5))
    parts.append(text(legend_x + 56, legend_y + 6, "DRAM", "legend", "start"))
    parts.append(
        f'<line x1="{legend_x:.2f}" y1="{legend_y + 30:.2f}" x2="{legend_x + 42:.2f}" y2="{legend_y + 30:.2f}" '
        f'stroke="{CXL_COLOR}" stroke-width="3.2" stroke-dasharray="10 7"/>'
    )
    parts.append(square(legend_x + 21, legend_y + 30, 10.5, CXL_COLOR, BG, 2.5))
    parts.append(text(legend_x + 56, legend_y + 36, "CXL", "legend", "start"))

    parts.append(
        text(
            width - 24,
            height - 52,
            "Latency uses p50 write+clflushopt+sfence; bandwidth uses payload GiB/s.",
            "note",
            "end",
        )
    )

    parts.append("</svg>")
    out_svg.write_text("\n".join(parts) + "\n", encoding="utf-8")
    print(f"wrote_svg={out_svg}")


if __name__ == "__main__":
    main()
