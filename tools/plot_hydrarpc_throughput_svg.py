#!/usr/bin/env python3
import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SHARED_CSV = ROOT / "output" / "hydrarpc_multiclient_sweep_shared_final_20260318" / "steady_summary.csv"
DEDICATED_CSV = ROOT / "output" / "hydrarpc_multiclient_sweep_dedicated_20260314_fix" / "steady_summary_recovered_c1_c2_c4_c8_c16_c32.csv"
OUTDIR = ROOT / "output" / "figures"
OUTSVG = OUTDIR / "hydrarpc_shared_vs_dedicated_throughput.svg"
OUTCSV = OUTDIR / "hydrarpc_shared_vs_dedicated_throughput.csv"
OUTTEX = OUTDIR / "hydrarpc_shared_vs_dedicated_throughput_pgfplots.tex"


def read_series(path: Path, label: str):
    rows = []
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                {
                    "series": label,
                    "client_count": int(row["client_count"]),
                    "throughput_mrps": float(row["steady_avg_throughput_mrps"]),
                }
            )
    rows.sort(key=lambda row: row["client_count"])
    return rows


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


def text(x, y, value, klass="", anchor="middle"):
    extra = f' class="{klass}"' if klass else ""
    return f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}"{extra}>{value}</text>'


def main():
    shared = read_series(SHARED_CSV, "Shared")
    dedicated = read_series(DEDICATED_CSV, "Dedicated")
    combined = shared + dedicated
    OUTDIR.mkdir(parents=True, exist_ok=True)

    with OUTCSV.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["series", "client_count", "throughput_mrps"],
        )
        writer.writeheader()
        for row in combined:
            writer.writerow(row)

    dedicated_coords = " ".join(
        f"({row['client_count']},{row['throughput_mrps']:.6f})" for row in dedicated
    )
    shared_coords = " ".join(
        f"({row['client_count']},{row['throughput_mrps']:.6f})" for row in shared
    )

    OUTTEX.write_text(
        "\n".join(
            [
                "% Requires \\usepackage{pgfplots}",
                "% and ideally \\pgfplotsset{compat=1.18}",
                "\\begin{tikzpicture}",
                "\\begin{axis}[",
                "  width=0.95\\linewidth,",
                "  height=0.58\\linewidth,",
                "  xlabel={Number of clients},",
                "  ylabel={Throughput (Mrps)},",
                "  xmin=1, xmax=32,",
                "  ymin=0, ymax=0.45,",
                "  xtick={1,2,4,8,16,32},",
                "  ytick={0,0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45},",
                "  grid=both,",
                "  major grid style={draw=gray!35},",
                "  minor grid style={draw=gray!20},",
                "  tick label style={font=\\small},",
                "  label style={font=\\small},",
                "  legend style={font=\\small, draw=gray!30, fill=white, fill opacity=0.92, at={(0.98,0.98)}, anchor=north east},",
                "  line width=1.05pt,",
                "  mark size=2.7pt,",
                "]",
                f"\\addplot[color={{rgb,255:red,31; green,78; blue,121}}, mark=o] coordinates {{{dedicated_coords}}};",
                "\\addlegendentry{Dedicated}",
                f"\\addplot[color={{rgb,255:red,198; green,106; blue,28}}, dashed, mark=square*, mark options={{fill=white}}] coordinates {{{shared_coords}}};",
                "\\addlegendentry{Shared}",
                "\\end{axis}",
                "\\end{tikzpicture}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    x_values = [1, 2, 4, 8, 16, 32]
    y_max_data = max(row["throughput_mrps"] for row in combined)
    y_max = math.ceil((y_max_data + 0.02) / 0.05) * 0.05
    y_ticks = [round(i * 0.05, 2) for i in range(0, int(round(y_max / 0.05)) + 1)]

    width = 980
    height = 560
    margin_left = 108
    margin_right = 38
    margin_top = 34
    margin_bottom = 88
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    def x_map(client_count: int):
        idx = x_values.index(client_count)
        if len(x_values) == 1:
            return margin_left + plot_w / 2.0
        return margin_left + idx * (plot_w / (len(x_values) - 1))

    def y_map(value: float):
        if y_max == 0:
            return margin_top + plot_h
        return margin_top + plot_h - (value / y_max) * plot_h

    shared_points = [(x_map(row["client_count"]), y_map(row["throughput_mrps"])) for row in shared]
    dedicated_points = [(x_map(row["client_count"]), y_map(row["throughput_mrps"])) for row in dedicated]

    blue = "#1f4e79"
    orange = "#c66a1c"
    axis = "#1f1f1f"
    grid = "#d9d9d9"
    label = "#2b2b2b"
    bg = "#ffffff"

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="HydraRPC throughput versus client count">'
    )
    parts.append(
        "<style>"
        ".axis{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        "font-size:22px;fill:#2b2b2b}"
        ".tick{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        "font-size:18px;fill:#2b2b2b}"
        ".legend{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        "font-size:18px;fill:#2b2b2b}"
        ".note{font-family:'Times New Roman','Liberation Serif','Nimbus Roman',serif;"
        "font-size:15px;fill:#5a5a5a}"
        "</style>"
    )
    parts.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="{bg}"/>')

    for tick in y_ticks:
        y = y_map(tick)
        parts.append(
            f'<line x1="{margin_left:.2f}" y1="{y:.2f}" '
            f'x2="{margin_left + plot_w:.2f}" y2="{y:.2f}" '
            f'stroke="{grid}" stroke-width="1.1"/>'
        )
        parts.append(text(margin_left - 18, y + 6, fmt(tick), "tick", "end"))

    for client_count in x_values:
        x = x_map(client_count)
        parts.append(
            f'<line x1="{x:.2f}" y1="{margin_top:.2f}" '
            f'x2="{x:.2f}" y2="{margin_top + plot_h:.2f}" '
            f'stroke="{grid}" stroke-width="1.0"/>'
        )
        parts.append(text(x, margin_top + plot_h + 30, str(client_count), "tick"))

    parts.append(
        f'<rect x="{margin_left:.2f}" y="{margin_top:.2f}" width="{plot_w:.2f}" '
        f'height="{plot_h:.2f}" fill="none" stroke="{axis}" stroke-width="1.8"/>'
    )

    parts.append(
        f'<path d="{line_path(dedicated_points)}" fill="none" stroke="{blue}" '
        f'stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"/>'
    )
    parts.append(
        f'<path d="{line_path(shared_points)}" fill="none" stroke="{orange}" '
        f'stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round" '
        f'stroke-dasharray="11 8"/>'
    )

    for x, y in dedicated_points:
        parts.append(circle(x, y, 6.2, blue, bg, 3.0))
    for x, y in shared_points:
        parts.append(square(x, y, 12.0, orange, bg, 3.0))

    parts.append(text(margin_left + plot_w / 2.0, height - 24, "Number of clients", "axis"))

    cy = margin_top + plot_h / 2.0
    cx = 32
    parts.append(
        f'<g transform="translate({cx:.2f},{cy:.2f}) rotate(-90)">'
        f'{text(0, 0, "Throughput (Mrps)", "axis")}'
        "</g>"
    )

    legend_x = margin_left + plot_w - 220
    legend_y = margin_top + 20
    parts.append(
        f'<rect x="{legend_x - 16:.2f}" y="{legend_y - 18:.2f}" width="210" height="78" '
        f'fill="#ffffff" fill-opacity="0.9" stroke="#d0d0d0" stroke-width="1.0" rx="8"/>'
    )
    parts.append(
        f'<line x1="{legend_x:.2f}" y1="{legend_y:.2f}" x2="{legend_x + 42:.2f}" y2="{legend_y:.2f}" '
        f'stroke="{blue}" stroke-width="3.6"/>'
    )
    parts.append(circle(legend_x + 21, legend_y, 6.2, blue, bg, 3.0))
    parts.append(text(legend_x + 58, legend_y + 6, "Dedicated", "legend", "start"))
    parts.append(
        f'<line x1="{legend_x:.2f}" y1="{legend_y + 32:.2f}" '
        f'x2="{legend_x + 42:.2f}" y2="{legend_y + 32:.2f}" '
        f'stroke="{orange}" stroke-width="3.6" stroke-dasharray="11 8"/>'
    )
    parts.append(square(legend_x + 21, legend_y + 32, 12.0, orange, bg, 3.0))
    parts.append(text(legend_x + 58, legend_y + 38, "Shared", "legend", "start"))

    parts.append(
        text(
            margin_left + plot_w,
            margin_top + plot_h + 58,
            "Shared currently has data up to c16; dedicated includes c32.",
            "note",
            "end",
        )
    )

    parts.append("</svg>")
    OUTSVG.write_text("\n".join(parts) + "\n", encoding="utf-8")
    print(f"wrote_svg={OUTSVG}")
    print(f"wrote_csv={OUTCSV}")
    print(f"wrote_tex={OUTTEX}")


if __name__ == "__main__":
    main()
