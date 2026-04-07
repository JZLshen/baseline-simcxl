#!/usr/bin/env python3
import argparse
import csv
from datetime import date
from pathlib import Path

from build_moti_overall_drop2_tables import (
    BACKUP_ROOT,
    METRIC_FIELDS,
    ROOT,
    compute_drop2_metrics_from_result_json,
    write_csv,
)


OUTPUT_ROOT = ROOT / "output"
SHARED_ROOT = BACKUP_ROOT / "SimCXL" / "output" / "SHARED"

DEDICATED_APP_ROOT = OUTPUT_ROOT / "dedicated_rest_20260403_120905" / "application"
SHARED_APP_ROOT = SHARED_ROOT / "application"


APPLICATION_PROFILES = (
    "ycsb_a_1k",
    "ycsb_b_1k",
    "ycsb_c_1k",
    "ycsb_d_1k",
    "udb_a",
    "udb_b",
    "udb_c",
    "udb_d",
)


APPLICATION_SPECS = [
    {
        "table_name": f"application_{profile}.csv",
        "profile": profile,
        "dedicated_result_json": (
            DEDICATED_APP_ROOT / f"dedicated_{profile}_c32_r30" / "result.json"
        ),
        "shared_result_json": (
            SHARED_APP_ROOT
            / profile
            / f"shared_profile_{profile}_s1024_c32_r30"
            / "result.json"
        ),
    }
    for profile in APPLICATION_PROFILES
]


def build_application_tables(outdir: Path):
    manifest_rows = []

    for spec in APPLICATION_SPECS:
        rows = []
        for project, result_json in (
            ("dedicated", spec["dedicated_result_json"]),
            ("shared", spec["shared_result_json"]),
        ):
            metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
            row = {
                "project": project,
                "client_count": 32,
            }
            row.update(metrics)
            rows.append(row)
            manifest_rows.append(
                {
                    "table": spec["table_name"].replace(".csv", ""),
                    "row_key": f"{project}:c32",
                    "profile": spec["profile"],
                    "source_type": "result_log",
                    "source_path": str(source_path),
                    "note": (
                        f"recomputed from application {spec['profile']} "
                        f"{project} result log at drop2"
                    ),
                }
            )

        write_csv(
            outdir / spec["table_name"],
            ["project", "client_count", *METRIC_FIELDS],
            rows,
        )

    write_csv(
        outdir / "source_manifest.csv",
        ["table", "row_key", "profile", "source_type", "source_path", "note"],
        manifest_rows,
    )


def parse_args():
    today = date.today().strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=Path,
        default=OUTPUT_ROOT / f"application_tables_drop2_canonical_{today}",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    build_application_tables(args.outdir)
    print(f"wrote {args.outdir}")


if __name__ == "__main__":
    main()
