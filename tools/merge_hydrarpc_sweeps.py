#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


CSV_NAMES = ("summary.csv", "steady_summary.csv", "failures.csv")


def read_csv_rows(path: Path):
    if not path.exists():
        return [], []

    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return fieldnames, rows


def sort_rows(rows):
    def key(row):
        experiment = row.get("experiment", "")
        client_count = int(row.get("client_count", "0") or 0)
        outdir = row.get("outdir", "")
        return (experiment, client_count, outdir)

    return sorted(rows, key=key)


def merge_one_csv(outdir: Path, input_dirs: list[Path], name: str):
    merged_rows = []
    fieldnames = None

    for input_dir in input_dirs:
        current_fields, current_rows = read_csv_rows(input_dir / name)
        if not current_rows:
            continue
        if fieldnames is None:
            fieldnames = current_fields
        elif current_fields != fieldnames:
            raise ValueError(
                f"CSV schema mismatch for {name}: {input_dir / name} has "
                f"{current_fields}, expected {fieldnames}"
            )
        merged_rows.extend(current_rows)

    if fieldnames is None:
        return

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / name
    with outpath.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in sort_rows(merged_rows):
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Merge HydraRPC sweep CSV outputs.")
    parser.add_argument(
        "--outdir",
        required=True,
        help="Directory to write merged CSV files into.",
    )
    parser.add_argument(
        "input_dirs",
        nargs="+",
        help="Sweep output directories to merge.",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    input_dirs = [Path(path).resolve() for path in args.input_dirs]

    for name in CSV_NAMES:
        merge_one_csv(outdir, input_dirs, name)

    print(f"merged_outdir={outdir}")
    for name in CSV_NAMES:
        path = outdir / name
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
