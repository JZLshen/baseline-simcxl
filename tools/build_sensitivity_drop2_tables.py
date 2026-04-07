#!/usr/bin/env python3
import argparse
import csv
from datetime import date
from pathlib import Path

from build_moti_overall_drop2_tables import (
    METRIC_FIELDS,
    SHARED_ROOT as SHARED_OUTPUT_ROOT,
    blank_metrics,
    compute_drop2_metrics_from_result_json,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output"
DEDICATED_SENSITIVITY_ROOT = (
    OUTPUT_ROOT / "noncc_dedicated_sensitivity_aligned_multirepo" / "sensitivity"
)
SHARED_SENSITIVITY_ROOT = SHARED_OUTPUT_ROOT / "sensitivity"

REQ_SIZES = (8, 64, 256, 1024, 4096, 8192)
RESP_SIZES = (8, 64, 256, 1024, 4096, 8192)
RING_SIZES = (16, 32, 64, 128, 256, 512)
SPARSE_CLIENT_COUNT = 16
SLOW_CLIENT_COUNTS = (1, 2, 4, 8)
SLOW_REQUEST_PCTS = (0, 25, 50, 100)
CXL_LATENCIES_NS = (100, 200, 300, 400, 500, 600)


def write_csv(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def add_manifest(manifest_rows, table, row_key, source_type, source_path, note=""):
    manifest_rows.append(
        {
            "table": table,
            "row_key": row_key,
            "source_type": source_type,
            "source_path": str(source_path) if source_path else "",
            "note": note,
        }
    )


def nested_single_result_json(root: Path):
    candidates = sorted(root.glob("**/result.json"))
    if not candidates:
        return None
    if len(candidates) > 1:
        raise RuntimeError(f"multiple result.json under {root}: {candidates}")
    return candidates[0]


def dedicated_result_json(dirname: str):
    path = DEDICATED_SENSITIVITY_ROOT / dirname / "result.json"
    return path if path.exists() else None


def shared_result_json(dirname: str):
    return nested_single_result_json(SHARED_SENSITIVITY_ROOT / dirname)


def latest_existing_result_json(candidates):
    existing = [path for path in candidates if path is not None and path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def dedicated_overall_req38_resp230_result_json(client_count: int):
    pattern = (
        f"*/overall/req38_resp230_uniform/*_c{client_count}_r30/result.json"
    )
    return latest_existing_result_json(sorted(OUTPUT_ROOT.glob(pattern)))


def shared_overall_req38_resp230_result_json(client_count: int):
    return latest_existing_result_json(
        [
            SHARED_OUTPUT_ROOT
            / "overall"
            / "req38_resp230_uniform"
            / f"shared_s1024_requ19-57_respu115-345_c{client_count}_r30"
            / "result.json"
        ]
    )


def sparse_result_json(project: str, slow_request_pct: int, slow_client_count: int):
    if slow_request_pct == 100:
        if project == "dedicated":
            return dedicated_overall_req38_resp230_result_json(SPARSE_CLIENT_COUNT)
        return shared_overall_req38_resp230_result_json(SPARSE_CLIENT_COUNT)

    dirnames = [
        f"sparse16_d{slow_request_pct}_c{slow_client_count}",
        f"sparse_d{slow_request_pct}_c{slow_client_count}",
    ]
    for dirname in dirnames:
        result_json = (
            dedicated_result_json(dirname)
            if project == "dedicated"
            else shared_result_json(dirname)
        )
        if result_json is not None:
            return result_json
    return None


def load_metrics(
    manifest_rows,
    table_name: str,
    row_key: str,
    result_json_path: Path | None,
    note: str,
):
    if result_json_path is None:
        add_manifest(manifest_rows, table_name, row_key, "missing", "", note)
        return blank_metrics()

    metrics, source_path = compute_drop2_metrics_from_result_json(result_json_path)
    add_manifest(
        manifest_rows,
        table_name,
        row_key,
        "result_log",
        source_path,
        note,
    )
    return metrics


def build_reqsize_rows(manifest_rows):
    rows = []
    for project in ("dedicated", "shared"):
        for req_size in REQ_SIZES:
            dirname = f"reqsize_req{req_size}_resp230u"
            result_json = (
                dedicated_result_json(dirname)
                if project == "dedicated"
                else shared_result_json(dirname)
            )
            row = {"project": project, "req_size_bytes": req_size}
            row.update(
                load_metrics(
                    manifest_rows,
                    "sensitivity_reqsize",
                    f"{project}:req{req_size}",
                    result_json,
                    f"recomputed from {project} req-size sensitivity result log at drop2",
                )
            )
            rows.append(row)
    return rows


def build_respsize_rows(manifest_rows):
    rows = []
    for project in ("dedicated", "shared"):
        for resp_size in RESP_SIZES:
            dirname = f"respsize_req38u_resp{resp_size}"
            result_json = (
                dedicated_result_json(dirname)
                if project == "dedicated"
                else shared_result_json(dirname)
            )
            row = {"project": project, "resp_size_bytes": resp_size}
            row.update(
                load_metrics(
                    manifest_rows,
                    "sensitivity_respsize",
                    f"{project}:resp{resp_size}",
                    result_json,
                    f"recomputed from {project} resp-size sensitivity result log at drop2",
                )
            )
            rows.append(row)
    return rows


def build_ringsize_rows(manifest_rows):
    rows = []
    for project in ("dedicated", "shared"):
        for ring_size in RING_SIZES:
            dirname = f"ringsize_s{ring_size}"
            result_json = (
                dedicated_result_json(dirname)
                if project == "dedicated"
                else shared_result_json(dirname)
            )
            note = (
                f"recomputed from {project} ring-size sensitivity result log at drop2"
            )
            if project == "shared" and ring_size == 16 and result_json is None:
                note = (
                    "missing shared ringsize_s16 result.json; no successful reusable raw "
                    "run found"
                )
            row = {"project": project, "ring_slots": ring_size}
            row.update(
                load_metrics(
                    manifest_rows,
                    "sensitivity_ringsize",
                    f"{project}:s{ring_size}",
                    result_json,
                    note,
                )
            )
            rows.append(row)
    return rows


def build_sparse_rows(manifest_rows):
    rows = []
    for project in ("dedicated", "shared"):
        for slow_request_pct in SLOW_REQUEST_PCTS:
            for slow_client_count in SLOW_CLIENT_COUNTS:
                result_json = sparse_result_json(
                    project, slow_request_pct, slow_client_count
                )
                note = (
                    f"recomputed from {project} sparse sensitivity result log at drop2"
                )
                if slow_request_pct == 100:
                    note = (
                        f"canonicalized to {project} overall req38/resp230 c16 "
                        "result log at drop2"
                    )
                row = {
                    "project": project,
                    "client_count": SPARSE_CLIENT_COUNT,
                    "slow_request_pct": slow_request_pct,
                    "slow_client_count": slow_client_count,
                }
                row.update(
                    load_metrics(
                        manifest_rows,
                        "sensitivity_sparse",
                        f"{project}:pct{slow_request_pct}:sc{slow_client_count}",
                        result_json,
                        note,
                    )
                )
                rows.append(row)
    return rows


def build_cxl_latency_rows(manifest_rows):
    rows = []
    for project in ("dedicated", "shared"):
        for extra_latency_ns in CXL_LATENCIES_NS:
            dirname = f"cxl_latency_{extra_latency_ns}ns"
            result_json = (
                dedicated_result_json(dirname)
                if project == "dedicated"
                else shared_result_json(dirname)
            )
            row = {"project": project, "cxl_extra_latency_ns": extra_latency_ns}
            row.update(
                load_metrics(
                    manifest_rows,
                    "sensitivity_cxl_latency",
                    f"{project}:lat{extra_latency_ns}",
                    result_json,
                    f"recomputed from {project} cxl-latency sensitivity result log at drop2",
                )
            )
            rows.append(row)
    return rows


def parse_args():
    today = date.today().strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=Path,
        default=OUTPUT_ROOT / f"sensitivity_tables_drop2_canonical_{today}",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    manifest_rows = []

    reqsize_rows = build_reqsize_rows(manifest_rows)
    respsize_rows = build_respsize_rows(manifest_rows)
    ringsize_rows = build_ringsize_rows(manifest_rows)
    sparse_rows = build_sparse_rows(manifest_rows)
    cxl_latency_rows = build_cxl_latency_rows(manifest_rows)

    write_csv(
        args.outdir / "sensitivity_reqsize.csv",
        ["project", "req_size_bytes", *METRIC_FIELDS],
        reqsize_rows,
    )
    write_csv(
        args.outdir / "sensitivity_respsize.csv",
        ["project", "resp_size_bytes", *METRIC_FIELDS],
        respsize_rows,
    )
    write_csv(
        args.outdir / "sensitivity_ringsize.csv",
        ["project", "ring_slots", *METRIC_FIELDS],
        ringsize_rows,
    )
    write_csv(
        args.outdir / "sensitivity_sparse.csv",
        [
            "project",
            "client_count",
            "slow_request_pct",
            "slow_client_count",
            *METRIC_FIELDS,
        ],
        sparse_rows,
    )
    write_csv(
        args.outdir / "sensitivity_cxl_latency.csv",
        ["project", "cxl_extra_latency_ns", *METRIC_FIELDS],
        cxl_latency_rows,
    )
    write_csv(
        args.outdir / "source_manifest.csv",
        ["table", "row_key", "source_type", "source_path", "note"],
        manifest_rows,
    )
    print(f"wrote {args.outdir}")


if __name__ == "__main__":
    main()
