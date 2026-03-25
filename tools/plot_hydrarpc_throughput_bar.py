#!/usr/bin/env python3
import csv
import math
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator


ROOT = Path(__file__).resolve().parents[1]
SHARED_CSV = ROOT / "output" / "hydrarpc_multiclient_sweep_shared_final_20260318" / "steady_summary.csv"
DEDICATED_CSV = ROOT / "output" / "hydrarpc_multiclient_sweep_dedicated_20260314_fix" / "steady_summary_recovered_c1_c2_c4_c8_c16_c32.csv"
OUTDIR = ROOT / "output" / "figures"
BASE_NAME = "hydrarpc_shared_vs_dedicated_throughput_bar"
OUTCSV = OUTDIR / f"{BASE_NAME}_data.csv"
CLIENT_COUNTS = [1, 2, 4, 8, 16, 32]


@dataclass(frozen=True)
class Variant:
    suffix: str
    figsize: tuple[float, float]
    include_width: str
    axis_width: str
    axis_height: str
    bar_offset: float
    pgf_bar_width: str
    label_fontsize: float
    tick_fontsize: float
    legend_fontsize: float
    note_fontsize: float
    legend_bbox_y: float
    grid_color: str
    dedicated_fill: str
    dedicated_edge: str
    dedicated_hatch: str
    shared_fill: str
    shared_edge: str
    shared_hatch: str
    pgf_dedicated_draw: str
    pgf_dedicated_fill: str
    pgf_dedicated_extra: str
    pgf_shared_draw: str
    pgf_shared_fill: str
    pgf_shared_extra: str


VARIANTS = [
    Variant(
        suffix="",
        figsize=(7.2, 4.1),
        include_width="0.98\\linewidth",
        axis_width="0.97\\linewidth",
        axis_height="0.60\\linewidth",
        bar_offset=0.34,
        pgf_bar_width="10pt",
        label_fontsize=13,
        tick_fontsize=11,
        legend_fontsize=11,
        note_fontsize=9.5,
        legend_bbox_y=1.10,
        grid_color="#cfcfcf",
        dedicated_fill="#1d4c77",
        dedicated_edge="#1f1f1f",
        dedicated_hatch="",
        shared_fill="#c3661d",
        shared_edge="#1f1f1f",
        shared_hatch="",
        pgf_dedicated_draw="{rgb,255:red,29; green,76; blue,119}",
        pgf_dedicated_fill="{rgb,255:red,29; green,76; blue,119}!88",
        pgf_dedicated_extra="",
        pgf_shared_draw="{rgb,255:red,195; green,102; blue,29}",
        pgf_shared_fill="{rgb,255:red,195; green,102; blue,29}!82",
        pgf_shared_extra="",
    ),
    Variant(
        suffix="_twocol",
        figsize=(3.45, 2.32),
        include_width="0.99\\columnwidth",
        axis_width="0.99\\columnwidth",
        axis_height="0.72\\columnwidth",
        bar_offset=0.28,
        pgf_bar_width="6pt",
        label_fontsize=10,
        tick_fontsize=8,
        legend_fontsize=8,
        note_fontsize=6.4,
        legend_bbox_y=1.16,
        grid_color="#d7d7d7",
        dedicated_fill="#1d4c77",
        dedicated_edge="#202020",
        dedicated_hatch="",
        shared_fill="#c3661d",
        shared_edge="#202020",
        shared_hatch="",
        pgf_dedicated_draw="{rgb,255:red,29; green,76; blue,119}",
        pgf_dedicated_fill="{rgb,255:red,29; green,76; blue,119}!88",
        pgf_dedicated_extra="",
        pgf_shared_draw="{rgb,255:red,195; green,102; blue,29}",
        pgf_shared_fill="{rgb,255:red,195; green,102; blue,29}!82",
        pgf_shared_extra="",
    ),
    Variant(
        suffix="_bw",
        figsize=(7.0, 4.0),
        include_width="0.98\\linewidth",
        axis_width="0.97\\linewidth",
        axis_height="0.60\\linewidth",
        bar_offset=0.34,
        pgf_bar_width="10pt",
        label_fontsize=13,
        tick_fontsize=11,
        legend_fontsize=11,
        note_fontsize=9.5,
        legend_bbox_y=1.10,
        grid_color="#d5d5d5",
        dedicated_fill="#5b5b5b",
        dedicated_edge="#111111",
        dedicated_hatch="",
        shared_fill="#d9d9d9",
        shared_edge="#111111",
        shared_hatch="",
        pgf_dedicated_draw="black",
        pgf_dedicated_fill="black!65",
        pgf_dedicated_extra="",
        pgf_shared_draw="black",
        pgf_shared_fill="black!18",
        pgf_shared_extra="",
    ),
]


def read_series(path: Path) -> dict[int, float]:
    rows: dict[int, float] = {}
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows[int(row["client_count"])] = float(row["steady_avg_throughput_mrps"])
    return rows


def fmt(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def output_paths(variant: Variant) -> dict[str, Path]:
    base = OUTDIR / f"{BASE_NAME}{variant.suffix}"
    return {
        "base": base,
        "pdf": base.with_suffix(".pdf"),
        "svg": base.with_suffix(".svg"),
        "png": base.with_suffix(".png"),
        "tex": base.with_name(base.name + "_pgfplots.tex"),
        "include": base.with_name(base.name + "_include.tex"),
    }


def write_combined_csv(shared: dict[int, float], dedicated: dict[int, float]) -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with OUTCSV.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["client_count", "dedicated_throughput_mrps", "shared_throughput_mrps"],
        )
        writer.writeheader()
        for client_count in CLIENT_COUNTS:
            writer.writerow(
                {
                    "client_count": client_count,
                    "dedicated_throughput_mrps": fmt(dedicated[client_count]) if client_count in dedicated else "",
                    "shared_throughput_mrps": fmt(shared[client_count]) if client_count in shared else "",
                }
            )


def write_pgfplots(
    shared: dict[int, float],
    dedicated: dict[int, float],
    ymax: float,
    variant: Variant,
    paths: dict[str, Path],
) -> None:
    dedicated_coords = " ".join(
        f"({client_count},{dedicated[client_count]:.6f})"
        for client_count in CLIENT_COUNTS
        if client_count in dedicated
    )
    shared_coords = " ".join(
        f"({client_count},{shared[client_count]:.6f})"
        for client_count in CLIENT_COUNTS
        if client_count in shared
    )

    paths["tex"].write_text(
        "\n".join(
            [
                "% Requires \\usepackage{pgfplots}",
                "% and ideally \\pgfplotsset{compat=1.18}",
                "\\begin{tikzpicture}",
                "\\begin{axis}[",
                f"  width={variant.axis_width},",
                f"  height={variant.axis_height},",
                "  ybar=6pt,",
                f"  bar width={variant.pgf_bar_width},",
                "  xlabel={Number of clients},",
                "  ylabel={Throughput (Mrps)},",
                "  symbolic x coords={1,2,4,8,16,32},",
                "  xtick=data,",
                f"  ymin=0, ymax={ymax:.2f},",
                "  enlarge x limits=0.08,",
                "  ymajorgrids,",
                "  grid style={draw=gray!22},",
                "  tick label style={font=\\small},",
                "  label style={font=\\small},",
                f"  legend style={{font=\\small, draw=gray!25, fill=white, fill opacity=0.96, at={{(0.5,{variant.legend_bbox_y - 0.08:.2f})}}, anchor=south, legend columns=2}},",
                "  axis line style={draw=black!70},",
                "  tick style={draw=black!70},",
                "]",
                f"\\addplot+[draw={variant.pgf_dedicated_draw}, fill={variant.pgf_dedicated_fill}{variant.pgf_dedicated_extra}] coordinates {{{dedicated_coords}}};",
                "\\addlegendentry{Dedicated}",
                f"\\addplot+[draw={variant.pgf_shared_draw}, fill={variant.pgf_shared_fill}{variant.pgf_shared_extra}] coordinates {{{shared_coords}}};",
                "\\addlegendentry{Shared}",
                "\\end{axis}",
                "\\end{tikzpicture}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_include_snippet(paths: dict[str, Path], variant: Variant) -> None:
    figure_spec = "[t]" if variant.suffix != "_twocol" else "[tb]"
    label_suffix = variant.suffix.replace("_", "-") if variant.suffix else ""
    label = "fig:hydrarpc-shared-dedicated-throughput-bar"
    if label_suffix:
        label = f"{label}{label_suffix}"

    paths["include"].write_text(
        "\n".join(
            [
                f"\\begin{{figure}}{figure_spec}",
                "  \\centering",
                f"  \\includegraphics[width={variant.include_width}]{{output/figures/{paths['pdf'].name}}}",
                "  \\caption{HydraRPC throughput versus client count under shared and dedicated configurations.}",
                f"  \\label{{{label}}}",
                "\\end{figure}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def compute_bar_layout(client_counts, shared, dedicated, offset):
    dedicated_x = []
    dedicated_y = []
    shared_x = []
    shared_y = []
    for idx, client_count in enumerate(client_counts):
        has_dedicated = client_count in dedicated
        has_shared = client_count in shared
        if has_dedicated and has_shared:
            dedicated_x.append(idx - offset / 2.0)
            dedicated_y.append(dedicated[client_count])
            shared_x.append(idx + offset / 2.0)
            shared_y.append(shared[client_count])
        elif has_dedicated:
            dedicated_x.append(idx)
            dedicated_y.append(dedicated[client_count])
        elif has_shared:
            shared_x.append(idx)
            shared_y.append(shared[client_count])
    return offset, dedicated_x, dedicated_y, shared_x, shared_y


def create_figure(
    shared: dict[int, float],
    dedicated: dict[int, float],
    ymax: float,
    variant: Variant,
) -> None:
    paths = output_paths(variant)

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Liberation Serif", "Nimbus Roman", "Times New Roman", "DejaVu Serif"],
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "hatch.linewidth": 1.2,
        }
    )

    fig, ax = plt.subplots(figsize=variant.figsize, constrained_layout=True)
    x = np.arange(len(CLIENT_COUNTS))
    bar_width, dedicated_x, dedicated_y, shared_x, shared_y = compute_bar_layout(
        CLIENT_COUNTS, shared, dedicated, variant.bar_offset
    )

    ax.bar(
        dedicated_x,
        dedicated_y,
        width=bar_width,
        color=variant.dedicated_fill,
        edgecolor=variant.dedicated_edge,
        linewidth=0.8,
        hatch=variant.dedicated_hatch,
        zorder=3,
    )
    ax.bar(
        shared_x,
        shared_y,
        width=bar_width,
        color=variant.shared_fill,
        edgecolor=variant.shared_edge,
        linewidth=0.8,
        hatch=variant.shared_hatch,
        zorder=3,
    )

    ax.set_xticks(x)
    ax.set_xticklabels([str(client_count) for client_count in CLIENT_COUNTS], fontsize=variant.tick_fontsize)
    ax.set_xlabel("Number of clients", fontsize=variant.label_fontsize)
    ax.set_ylabel("Throughput (Mrps)", fontsize=variant.label_fontsize)
    ax.set_ylim(0.0, ymax)
    ax.yaxis.set_major_locator(MultipleLocator(0.05))
    ax.grid(axis="y", color=variant.grid_color, linewidth=0.8, alpha=0.85)
    ax.set_axisbelow(True)
    ax.margins(x=0.04)
    ax.tick_params(axis="y", labelsize=variant.tick_fontsize)
    ax.tick_params(axis="both", width=0.9, length=4)

    for spine_name in ("left", "bottom"):
        ax.spines[spine_name].set_color("#444444")
        ax.spines[spine_name].set_linewidth(0.9)

    legend_handles = [
        Patch(
            facecolor=variant.dedicated_fill,
            edgecolor=variant.dedicated_edge,
            linewidth=0.8,
            hatch=variant.dedicated_hatch,
            label="Dedicated",
        ),
        Patch(
            facecolor=variant.shared_fill,
            edgecolor=variant.shared_edge,
            linewidth=0.8,
            hatch=variant.shared_hatch,
            label="Shared",
        ),
    ]
    ax.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, variant.legend_bbox_y),
        ncol=2,
        frameon=False,
        fontsize=variant.legend_fontsize,
        handlelength=1.4,
        columnspacing=1.3,
        borderpad=0.2,
    )

    fig.savefig(paths["pdf"], dpi=300, bbox_inches="tight")
    fig.savefig(paths["svg"], bbox_inches="tight")
    fig.savefig(paths["png"], dpi=300, bbox_inches="tight")
    plt.close(fig)

    write_pgfplots(shared, dedicated, ymax, variant, paths)
    write_include_snippet(paths, variant)


def main() -> None:
    shared = read_series(SHARED_CSV)
    dedicated = read_series(DEDICATED_CSV)
    values = [*shared.values(), *dedicated.values()]
    ymax = math.ceil((max(values) + 0.03) / 0.05) * 0.05

    write_combined_csv(shared, dedicated)
    for variant in VARIANTS:
        create_figure(shared, dedicated, ymax, variant)


if __name__ == "__main__":
    main()
