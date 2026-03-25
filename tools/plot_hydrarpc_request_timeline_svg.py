#!/usr/bin/env python3
import argparse
import csv
import math
import re
from pathlib import Path


TIMING_PATTERN = re.compile(r"^client_(\d+)_req_(\d+)_(start_ns|end_ns)=(\d+)$")


def parse_log(log_path: Path):
    starts = {}
    ends = {}

    with log_path.open("r", errors="ignore") as fh:
        for raw_line in fh:
            line = raw_line.strip().replace("\r", "")
            match = TIMING_PATTERN.match(line)
            if not match:
                continue

            client_id = int(match.group(1))
            req_id = int(match.group(2))
            kind = match.group(3)
            value = int(match.group(4))
            key = (client_id, req_id)

            if kind == "start_ns":
                starts[key] = value
            else:
                ends[key] = value

    rows = []
    for key in sorted(starts):
        if key not in ends:
            continue
        rows.append(
            {
                "client_id": key[0],
                "req_id": key[1],
                "start_ns": starts[key],
                "end_ns": ends[key],
                "latency_ns": ends[key] - starts[key],
            }
        )
    return rows


def load_experiments(summary_csv: Path):
    experiments = []
    repo_root = summary_csv.parents[2]

    with summary_csv.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            outdir = Path(row["outdir"])
            log_path = Path(row["log_path"])
            if not outdir.is_absolute():
                summary_relative = (summary_csv.parent / outdir).resolve()
                repo_relative = (repo_root / outdir).resolve()
                outdir = summary_relative if summary_relative.exists() else repo_relative
            if not log_path.is_absolute():
                summary_relative = (summary_csv.parent / log_path).resolve()
                repo_relative = (repo_root / log_path).resolve()
                log_path = summary_relative if summary_relative.exists() else repo_relative

            experiments.append(
                {
                    "client_count": int(row["client_count"]),
                    "count_per_client": int(row["count_per_client"]),
                    "outdir": outdir,
                    "log_path": log_path,
                }
            )

    experiments.sort(key=lambda item: item["client_count"])
    return experiments


def fmt_float(value: float, digits: int = 3):
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def xml_escape(value: str):
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def text(x, y, value, css_class="", anchor="middle", rotate=None):
    attrs = [f'x="{x:.2f}"', f'y="{y:.2f}"', f'text-anchor="{anchor}"']
    if css_class:
        attrs.append(f'class="{css_class}"')
    if rotate is not None:
        attrs.append(f'transform="rotate({rotate:.2f} {x:.2f} {y:.2f})"')
    return f"<text {' '.join(attrs)}>{xml_escape(value)}</text>"


def client_color(client_id: int, client_count: int):
    if client_count <= 1:
        hue = 210.0
    else:
        hue = 210.0 + (280.0 * client_id / (client_count - 1))
    return f"hsl({hue:.1f}, 66%, 43%)"


def interval_color(
    client_id: int,
    client_count: int,
    slow_client_count: int = 0,
    paper_style: bool = False,
):
    if paper_style:
        if client_id < slow_client_count:
            return "#b45309"
        return "#1d4f91"
    return client_color(client_id, client_count)


def render_experiment_svg(
    rows,
    client_count: int,
    count_per_client: int,
    drop_first_per_client: int,
    title: str,
    subtitle: str,
    slow_client_count: int = 0,
    paper_style: bool = False,
    show_gap_panel: bool = True,
):
    if not rows:
        raise ValueError("no complete timing rows found")

    rows_by_client = {}
    for row in rows:
        rows_by_client.setdefault(row["client_id"], []).append(row)

    for client_rows in rows_by_client.values():
        client_rows.sort(key=lambda item: item["req_id"])

    first_start_ns = min(row["start_ns"] for row in rows)
    last_end_ns = max(row["end_ns"] for row in rows)
    total_span_ns = max(1, last_end_ns - first_start_ns)

    completion_rows = sorted(rows, key=lambda row: (row["end_ns"], row["client_id"], row["req_id"]))
    gaps = []
    for prev, curr in zip(completion_rows, completion_rows[1:]):
        gaps.append(curr["end_ns"] - prev["end_ns"])

    if paper_style:
        width = 1680
        margin_left = 146
        margin_right = 40
        margin_top = 106
        margin_bottom = 74
        legend_h = 42
        lane_gap = 9
        row_gap = 1
        row_h = 6
    else:
        width = 1480
        margin_left = 110
        margin_right = 36
        margin_top = 78
        margin_bottom = 72
        legend_h = 30
        lane_gap = 12
        row_gap = 3
        row_h = 8

    client_band_h = count_per_client * row_h + max(0, count_per_client - 1) * row_gap
    timeline_h = client_count * client_band_h + max(0, client_count - 1) * lane_gap

    panel_gap = 48 if (gaps and show_gap_panel) else 0
    gap_panel_h = 270 if (gaps and show_gap_panel) else 0

    height = margin_top + legend_h + timeline_h + panel_gap + gap_panel_h + margin_bottom
    plot_x0 = margin_left
    plot_x1 = width - margin_right
    plot_w = plot_x1 - plot_x0

    timeline_y0 = margin_top + legend_h
    timeline_y1 = timeline_y0 + timeline_h

    gap_y0 = timeline_y1 + panel_gap if gap_panel_h else timeline_y1
    gap_y1 = gap_y0 + gap_panel_h

    def x_map(ns_value: int):
        rel = ns_value - first_start_ns
        return plot_x0 + (rel / total_span_ns) * plot_w

    def lane_center(client_id: int):
        return timeline_y0 + client_id * (client_band_h + lane_gap) + client_band_h / 2.0

    def row_y(client_id: int, req_id: int):
        band_y0 = timeline_y0 + client_id * (client_band_h + lane_gap)
        return band_y0 + req_id * (row_h + row_gap) + row_h / 2.0

    gap_max_ns = max(gaps) if gaps else 1

    def gap_y_map(ns_value: int):
        if gap_max_ns <= 0:
            return gap_y1
        return gap_y1 - (ns_value / gap_max_ns) * gap_panel_h

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="{xml_escape(title)}">'
    )
    if paper_style:
        parts.append(
            "<style>"
            ".title{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:30px;font-weight:700;fill:#111827}"
            ".subtitle{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:18px;fill:#374151}"
            ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:24px;fill:#1f2937}"
            ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:17px;fill:#374151}"
            ".small{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:16px;fill:#4b5563}"
            ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:16px;fill:#1f2937}"
            "</style>"
        )
    else:
        parts.append(
            "<style>"
            ".title{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:28px;font-weight:700;fill:#1f1f1f}"
            ".subtitle{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:17px;fill:#4b4b4b}"
            ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:20px;fill:#2b2b2b}"
            ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:15px;fill:#333333}"
            ".small{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:14px;fill:#505050}"
            ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
            "font-size:15px;fill:#2f2f2f}"
            "</style>"
        )
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>')

    if paper_style:
        parts.append(text(plot_x0, 40, title, "title", "start"))
        parts.append(text(plot_x0, 70, subtitle, "subtitle", "start"))
        legend_y = margin_top - 18

        legend_items = [
            ("line", "#b8bec8", "req 0 warm-up"),
        ]
        if slow_client_count < client_count:
            legend_items.append(("line", "#1d4f91", "normal-client interval"))
        if slow_client_count > 0:
            legend_items.append(("line", "#b45309", "slow-client interval"))
        legend_items.extend(
            [
                ("circle", "#b91c1c", "request issued"),
                ("square", "#065f46", "response received"),
            ]
        )

        legend_x = plot_x0
        for kind, color, label in legend_items:
            if kind == "line":
                parts.append(
                    f'<line x1="{legend_x:.2f}" y1="{legend_y:.2f}" '
                    f'x2="{legend_x + 34:.2f}" y2="{legend_y:.2f}" stroke="{color}" stroke-width="4.6"/>'
                )
                text_x = legend_x + 42
                legend_x = text_x + len(label) * 7.8 + 34
            elif kind == "circle":
                parts.append(
                    f'<circle cx="{legend_x + 4.5:.2f}" cy="{legend_y:.2f}" r="4.5" fill="{color}"/>'
                )
                text_x = legend_x + 18
                legend_x = text_x + len(label) * 7.8 + 28
            else:
                parts.append(
                    f'<rect x="{legend_x:.2f}" y="{legend_y - 4.5:.2f}" width="9" height="9" fill="{color}"/>'
                )
                text_x = legend_x + 16
                legend_x = text_x + len(label) * 7.8 + 28
            parts.append(text(text_x, legend_y + 5, label, "legend", "start"))
    else:
        parts.append(text(plot_x0, 34, title, "title", "start"))
        parts.append(text(plot_x0, 60, subtitle, "subtitle", "start"))

        legend_y = margin_top - 18
        parts.append('<rect x="0" y="0" width="0" height="0" fill="none"/>')
        parts.append(
            f'<line x1="{plot_x0:.2f}" y1="{legend_y:.2f}" '
            f'x2="{plot_x0 + 34:.2f}" y2="{legend_y:.2f}" stroke="#9ca3af" stroke-width="4"/>'
        )
        parts.append(text(plot_x0 + 42, legend_y + 5, f"warmup req_id < {drop_first_per_client}", "legend", "start"))
        parts.append(
            f'<line x1="{plot_x0 + 250:.2f}" y1="{legend_y:.2f}" '
            f'x2="{plot_x0 + 284:.2f}" y2="{legend_y:.2f}" stroke="#1f4e79" stroke-width="4"/>'
        )
        parts.append(text(plot_x0 + 292, legend_y + 5, "steady request-response interval", "legend", "start"))
        parts.append(
            f'<circle cx="{plot_x0 + 560:.2f}" cy="{legend_y:.2f}" r="4" fill="#b91c1c"/>'
        )
        parts.append(text(plot_x0 + 572, legend_y + 5, "request start", "legend", "start"))
        parts.append(
            f'<rect x="{plot_x0 + 706:.2f}" y="{legend_y - 4:.2f}" width="8" height="8" fill="#065f46"/>'
        )
        parts.append(text(plot_x0 + 722, legend_y + 5, "response observed", "legend", "start"))

    x_ticks = 6 if paper_style else 8
    for tick_idx in range(x_ticks + 1):
        ratio = tick_idx / x_ticks
        x = plot_x0 + ratio * plot_w
        ns_value = first_start_ns + int(round(ratio * total_span_ns))
        us_value = (ns_value - first_start_ns) / 1000.0
        parts.append(
            f'<line x1="{x:.2f}" y1="{timeline_y0:.2f}" x2="{x:.2f}" y2="{timeline_y1:.2f}" '
            f'stroke="#e5e7eb" stroke-width="1"/>'
        )
        if gaps and show_gap_panel:
            parts.append(
                f'<line x1="{x:.2f}" y1="{gap_y0:.2f}" x2="{x:.2f}" y2="{gap_y1:.2f}" '
                f'stroke="#f0f1f4" stroke-width="1"/>'
            )
        parts.append(text(x, timeline_y1 + 24, fmt_float(us_value, 1), "tick"))

    axis_label = "Time from first request issue (us)" if paper_style else "Time since first request start (us)"
    parts.append(text((plot_x0 + plot_x1) / 2.0, height - 20, axis_label, "axis"))

    parts.append(
        f'<rect x="{plot_x0:.2f}" y="{timeline_y0:.2f}" width="{plot_w:.2f}" height="{timeline_h:.2f}" '
        f'fill="none" stroke="#1f2937" stroke-width="1.6"/>'
    )

    for client_id in range(client_count):
        band_y0 = timeline_y0 + client_id * (client_band_h + lane_gap)
        band_y1 = band_y0 + client_band_h
        is_slow_client = client_id < slow_client_count
        if paper_style:
            band_fill = "#fff7ed" if is_slow_client else "#f7fbff"
        else:
            band_fill = "#fbfbfc" if client_id % 2 == 0 else "#f4f6f8"
        parts.append(
            f'<rect x="{plot_x0:.2f}" y="{band_y0:.2f}" width="{plot_w:.2f}" height="{client_band_h:.2f}" '
            f'fill="{band_fill}" stroke="none"/>'
        )
        if paper_style:
            stripe_fill = "#f59e0b" if is_slow_client else "#60a5fa"
            parts.append(
                f'<rect x="{plot_x0 - 10:.2f}" y="{band_y0:.2f}" width="6" height="{client_band_h:.2f}" '
                f'fill="{stripe_fill}" stroke="none"/>'
            )
        parts.append(
            f'<line x1="{plot_x0:.2f}" y1="{band_y1:.2f}" x2="{plot_x1:.2f}" y2="{band_y1:.2f}" '
            f'stroke="#d1d5db" stroke-width="1"/>'
        )
        label_class = "tick"
        if paper_style:
            label_fill = "#b45309" if is_slow_client else "#1d4f91"
            parts.append(
                f'<text x="{plot_x0 - 16:.2f}" y="{lane_center(client_id) + 5:.2f}" '
                f'text-anchor="end" class="{label_class}" fill="{label_fill}">c{client_id}</text>'
            )
        else:
            parts.append(text(plot_x0 - 14, lane_center(client_id) + 5, f"c{client_id}", "tick", "end"))

        color = interval_color(
            client_id,
            client_count,
            slow_client_count=slow_client_count,
            paper_style=paper_style,
        )
        for row in rows_by_client.get(client_id, []):
            y = row_y(client_id, row["req_id"])
            x_start = x_map(row["start_ns"])
            x_end = x_map(row["end_ns"])
            if row["req_id"] < drop_first_per_client:
                stroke = "#b8bec8" if paper_style else "#9ca3af"
                stroke_opacity = 0.75
                start_fill = "#9ca3af"
                end_fill = "#6b7280"
            else:
                stroke = color
                stroke_opacity = 0.9 if paper_style else 0.88
                start_fill = "#b91c1c"
                end_fill = "#065f46"
            parts.append(
                f'<line x1="{x_start:.2f}" y1="{y:.2f}" x2="{x_end:.2f}" y2="{y:.2f}" '
                f'stroke="{stroke}" stroke-width="{"2.8" if paper_style else "2.4"}" stroke-opacity="{stroke_opacity:.3f}" '
                f'stroke-linecap="round"/>'
            )
            parts.append(
                f'<circle cx="{x_start:.2f}" cy="{y:.2f}" r="{"3.0" if paper_style else "2.7"}" fill="{start_fill}" fill-opacity="0.95"/>'
            )
            parts.append(
                f'<rect x="{x_end - (3.0 if paper_style else 2.5):.2f}" y="{y - (3.0 if paper_style else 2.5):.2f}" '
                f'width="{"6" if paper_style else "5"}" height="{"6" if paper_style else "5"}" '
                f'fill="{end_fill}" fill-opacity="0.95"/>'
            )

    parts.append(text(28, timeline_y0 + timeline_h / 2.0, "Client", "axis", "middle", -90))

    if gaps and show_gap_panel:
        avg_gap = sum(gaps) / len(gaps)
        median_gap = sorted(gaps)[len(gaps) // 2] if len(gaps) % 2 == 1 else (
            sorted(gaps)[len(gaps) // 2 - 1] + sorted(gaps)[len(gaps) // 2]
        ) / 2.0

        parts.append(
            f'<rect x="{plot_x0:.2f}" y="{gap_y0:.2f}" width="{plot_w:.2f}" height="{gap_panel_h:.2f}" '
            f'fill="none" stroke="#1f2937" stroke-width="1.6"/>'
        )

        for gap_tick_ratio in (0.0, 0.25, 0.5, 0.75, 1.0):
            gap_tick_value = gap_tick_ratio * gap_max_ns
            y = gap_y_map(gap_tick_value)
            parts.append(
                f'<line x1="{plot_x0:.2f}" y1="{y:.2f}" x2="{plot_x1:.2f}" y2="{y:.2f}" '
                f'stroke="#e5e7eb" stroke-width="1"/>'
            )
            parts.append(text(plot_x0 - 14, y + 5, fmt_float(gap_tick_value, 0), "tick", "end"))

        gap_points = []
        gap_count = len(gaps)
        for idx, gap_ns in enumerate(gaps):
            x = plot_x0 if gap_count == 1 else plot_x0 + idx * (plot_w / (gap_count - 1))
            y = gap_y_map(gap_ns)
            gap_points.append((x, y))

        if gap_points:
            path = [f"M {gap_points[0][0]:.2f} {gap_points[0][1]:.2f}"]
            for x, y in gap_points[1:]:
                path.append(f"L {x:.2f} {y:.2f}")
            parts.append(
                f'<path d="{" ".join(path)}" fill="none" stroke="#7c3aed" '
                f'stroke-width="2.0" stroke-linejoin="round" stroke-linecap="round"/>'
            )
            for x, y in gap_points:
                parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="2.2" fill="#7c3aed"/>')

        avg_y = gap_y_map(avg_gap)
        med_y = gap_y_map(median_gap)
        parts.append(
            f'<line x1="{plot_x0:.2f}" y1="{avg_y:.2f}" x2="{plot_x1:.2f}" y2="{avg_y:.2f}" '
            f'stroke="#b45309" stroke-width="1.6" stroke-dasharray="7 6"/>'
        )
        parts.append(
            f'<line x1="{plot_x0:.2f}" y1="{med_y:.2f}" x2="{plot_x1:.2f}" y2="{med_y:.2f}" '
            f'stroke="#0f766e" stroke-width="1.6" stroke-dasharray="3 5"/>'
        )

        parts.append(text(plot_x0, gap_y0 - 14, "Completion gaps (all clients merged, sorted by end_ns)", "axis", "start"))
        parts.append(text(plot_x1, gap_y0 - 14, f"avg={fmt_float(avg_gap, 1)} ns, median={fmt_float(median_gap, 1)} ns", "small", "end"))
        parts.append(text(28, gap_y0 + gap_panel_h / 2.0, "Gap (ns)", "axis", "middle", -90))
        parts.append(text((plot_x0 + plot_x1) / 2.0, gap_y1 + 28, "Completion order", "small"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(
        description="Render per-experiment HydraRPC request/response timeline SVGs"
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        required=True,
        help="Summary CSV listing completed experiment outdirs/log paths",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write one SVG per experiment",
    )
    parser.add_argument(
        "--drop-first-per-client",
        type=int,
        default=5,
        help="Color req_id < N as warmup requests",
    )
    args = parser.parse_args()

    experiments = load_experiments(args.summary_csv.resolve())
    args.output_dir.mkdir(parents=True, exist_ok=True)

    manifest_rows = []
    for item in experiments:
        rows = parse_log(item["log_path"])
        title = f"HydraRPC dedicated timeline: {item['client_count']} clients"
        subtitle = (
            f"outdir={item['outdir'].name}, count_per_client={item['count_per_client']}, "
            f"warmup req_id < {args.drop_first_per_client}"
        )
        svg_text = render_experiment_svg(
            rows=rows,
            client_count=item["client_count"],
            count_per_client=item["count_per_client"],
            drop_first_per_client=args.drop_first_per_client,
            title=title,
            subtitle=subtitle,
        )
        out_name = f"hydrarpc_timeline_c{item['client_count']}.svg"
        out_path = args.output_dir / out_name
        out_path.write_text(svg_text, encoding="utf-8")
        manifest_rows.append(
            {
                "client_count": item["client_count"],
                "count_per_client": item["count_per_client"],
                "outdir": str(item["outdir"]),
                "log_path": str(item["log_path"]),
                "svg_path": str(out_path),
            }
        )
        print(f"wrote {out_path}")

    manifest_path = args.output_dir / "manifest.csv"
    with manifest_path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "client_count",
                "count_per_client",
                "outdir",
                "log_path",
                "svg_path",
            ],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)
    print(f"wrote {manifest_path}")


if __name__ == "__main__":
    main()
