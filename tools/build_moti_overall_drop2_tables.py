#!/usr/bin/env python3
import argparse
import csv
import json
import re
from datetime import date
from pathlib import Path

from summarize_hydrarpc_multiclient import (
    build_request_rows,
    compute_server_phase_durations,
    compute_steady_stats_with_server_phase,
    parse_log,
    validate_rows,
)


ROOT = Path(__file__).resolve().parents[1]
BACKUP_ROOT = ROOT.parent
SHARED_ROOT = BACKUP_ROOT / "SimCXL" / "output" / "SHARED"
OUTPUT_ROOT = ROOT / "output"
DEDICATED_ROOT = OUTPUT_ROOT / "DEDICATED"

DEDICATED_OVERALL_ROOT = (
    DEDICATED_ROOT / "overall" / "dedicated_rest_20260403_120905"
)
DEDICATED_SENSITIVITY_ROOT = (
    DEDICATED_ROOT / "sensitivity" / "dedicated_rest_20260403_120905"
)
DIRECT_COMPARE_ROOT = (
    DEDICATED_ROOT
    / "_batches"
    / "20260406"
    / "noncc_direct_vs_staging_ab_20260406_150136"
    / "direct"
)
STAGING_COMPARE_ROOT = (
    DEDICATED_ROOT
    / "_batches"
    / "20260406"
    / "noncc_direct_vs_staging_ab_20260406_150136"
    / "staging"
)
DIRECT_C1_RERUN_ROOT = (
    DEDICATED_ROOT
    / "_batches"
    / "20260406"
    / "noncc_direct_c1_rerun_20260406_154605"
)
MOTIVATION_M2_ROOT = DEDICATED_ROOT / "motivation" / "non-CC" / "motivation_m2"
MOTIVATION_M3_ROOT = DEDICATED_ROOT / "motivation" / "non-CC" / "motivation_m3"
SPARSE_RETEST_ROOT = (
    DEDICATED_ROOT / "sensitivity" / "noncc_dedicated_sparse_retest_multirepo" / "sensitivity"
)
ALIGNED_MOTI_SPARSE_ROOT = (
    DEDICATED_ROOT
    / "_batches"
    / "20260406"
    / "noncc_dedicated_sensitivity_aligned_multirepo"
    / "moti"
)
LATEST_ALIGNED_MOTI_SPARSE_ROOT = (
    DEDICATED_ROOT
    / "_batches"
    / "20260407"
    / "noncc_dedicated_sparse_aligned_seq_20260407_003347"
    / "moti"
)
LEGACY_MOTI_DROP2_CSV = (
    OUTPUT_ROOT / "moti_tables_drop2_20260406" / "moti_noncc_polling_dedicated.csv"
)

CLIENT_COUNTS = (1, 2, 4, 8, 16, 32)
SLOW_CLIENT_COUNTS = (1, 2, 4, 8)
SLOW_REQUEST_PCTS = (0, 25, 50, 100)
RESPONSE_SIZES = (8, 64, 256, 1024, 4096, 8192)

METRIC_FIELDS = [
    "throughput_kops",
    "latency_median_ns",
    "latency_p90_ns",
    "latency_p99_ns",
    "server_poll_median_ns",
    "server_poll_p90_ns",
    "server_poll_p99_ns",
    "server_execute_median_ns",
    "server_execute_p90_ns",
    "server_execute_p99_ns",
    "server_response_median_ns",
    "server_response_p90_ns",
    "server_response_p99_ns",
]


def fmt(value):
    if value is None:
        return ""
    return f"{value:.3f}"


def blank_metrics():
    return {field: "" for field in METRIC_FIELDS}


def row_from_steady(steady):
    return {
        "throughput_kops": fmt(steady.get("steady_avg_throughput_kops")),
        "latency_median_ns": fmt(steady.get("steady_median_latency_ns")),
        "latency_p90_ns": fmt(steady.get("steady_p90_latency_ns")),
        "latency_p99_ns": fmt(steady.get("steady_p99_latency_ns")),
        "server_poll_median_ns": fmt(
            steady.get("steady_median_server_poll_notify_ns")
        ),
        "server_poll_p90_ns": fmt(steady.get("steady_p90_server_poll_notify_ns")),
        "server_poll_p99_ns": fmt(steady.get("steady_p99_server_poll_notify_ns")),
        "server_execute_median_ns": fmt(
            steady.get("steady_median_server_execute_ns")
        ),
        "server_execute_p90_ns": fmt(steady.get("steady_p90_server_execute_ns")),
        "server_execute_p99_ns": fmt(steady.get("steady_p99_server_execute_ns")),
        "server_response_median_ns": fmt(
            steady.get("steady_median_server_response_ns")
        ),
        "server_response_p90_ns": fmt(steady.get("steady_p90_server_response_ns")),
        "server_response_p99_ns": fmt(steady.get("steady_p99_server_response_ns")),
    }


def legacy_row_from_csv(row):
    return {
        "throughput_kops": row["throughput_kops"],
        "latency_median_ns": row["latency_median_ns"],
        "latency_p90_ns": row["latency_p90_ns"],
        "latency_p99_ns": row["latency_p99_ns"],
        "server_poll_median_ns": row["server_poll_median_ns"],
        "server_poll_p90_ns": row["server_poll_p90_ns"],
        "server_poll_p99_ns": row["server_poll_p99_ns"],
        "server_execute_median_ns": row["server_execute_median_ns"],
        "server_execute_p90_ns": row["server_execute_p90_ns"],
        "server_execute_p99_ns": row["server_execute_p99_ns"],
        "server_response_median_ns": row["server_response_median_ns"],
        "server_response_p90_ns": row["server_response_p90_ns"],
        "server_response_p99_ns": row["server_response_p99_ns"],
    }


def expected_total_requests_for_run(
    result_json_path: Path, client_count: int, count_per_client: int
):
    name = result_json_path.parent.name
    match = re.search(r"_slow(\d+)_n(\d+)_", name)
    if not match:
        sparse_match = re.fullmatch(r"sparse(?:16)?_d(\d+)_c(\d+)", name)
        if sparse_match:
            slow_request_pct = int(sparse_match.group(1))
            slow_client_count = int(sparse_match.group(2))
            sparse_count_map = {0: 0, 25: 8, 50: 15, 100: count_per_client}
            slow_count_per_client = sparse_count_map.get(
                slow_request_pct,
                round(count_per_client * slow_request_pct / 100.0),
            )
            normal_client_count = client_count - slow_client_count
            return (
                normal_client_count * count_per_client
                + slow_client_count * slow_count_per_client
            )

        shared_sparse_match = re.search(r"_sd(\d+)_sc(\d+)_", name)
        if shared_sparse_match:
            slow_request_pct = int(shared_sparse_match.group(1))
            slow_client_count = int(shared_sparse_match.group(2))
            sparse_count_map = {0: 0, 25: 8, 50: 15, 100: count_per_client}
            slow_count_per_client = sparse_count_map.get(
                slow_request_pct,
                round(count_per_client * slow_request_pct / 100.0),
            )
            normal_client_count = client_count - slow_client_count
            return (
                normal_client_count * count_per_client
                + slow_client_count * slow_count_per_client
            )

        return client_count * count_per_client

    slow_client_count = int(match.group(1))
    slow_count_per_client = int(match.group(2))
    normal_client_count = client_count - slow_client_count
    return normal_client_count * count_per_client + slow_client_count * slow_count_per_client


def compute_drop2_metrics_from_result_json(result_json_path: Path):
    payload = json.loads(result_json_path.read_text())
    client_count = int(payload["client_count"])
    count_per_client = int(payload["count_per_client"])
    log_path = resolve_result_log_path(result_json_path, payload)

    parsed = parse_log(log_path)
    benchmark_rc = parsed["benchmark_rc"]
    guest_command_rc = parsed["guest_command_rc"]
    if benchmark_rc != 0 or guest_command_rc != 0:
        raise RuntimeError(
            f"benchmark failed for {log_path}: "
            f"benchmark_rc={benchmark_rc} guest_command_rc={guest_command_rc}"
        )

    rows, missing_start, missing_end, partial_start_only, partial_end_only = (
        build_request_rows(
            parsed["timing_by_request"],
            client_count,
            count_per_client,
        )
    )
    validation_error = validate_rows(
        rows,
        missing_start,
        missing_end,
        partial_start_only,
        partial_end_only,
        expected_total_requests_for_run(
            result_json_path,
            client_count,
            count_per_client,
        ),
    )
    if validation_error is not None:
        raise RuntimeError(f"{log_path}: {validation_error}")

    server_phase_durations = compute_server_phase_durations(
        rows, parsed["server_loop_start_ts_ns"]
    )
    steady = compute_steady_stats_with_server_phase(rows, server_phase_durations, 2)
    return row_from_steady(steady), log_path


def resolve_result_log_path(result_json_path: Path, payload):
    raw_log_path = Path(payload["log_path"])
    sibling_log_path = result_json_path.parent / raw_log_path.name
    if sibling_log_path.exists():
        return sibling_log_path
    if raw_log_path.exists():
        return raw_log_path
    raise FileNotFoundError(
        f"missing result log: sibling={sibling_log_path} raw={raw_log_path}"
    )


def dedicated_64_result_json(client_count: int):
    latest_result = (
        STAGING_COMPARE_ROOT
        / (
            f"dedicated_s1024_qb64_pb64_reqstaging_respstaging_mgreedy_g0_"
            f"c{client_count}_r30"
        )
        / "result.json"
    )
    if latest_result.exists():
        return latest_result
    return (
        DEDICATED_OVERALL_ROOT
        / "req64_resp64"
        / (
            f"dedicated_s1024_qb64_pb64_reqstaging_respstaging_mgreedy_g0_"
            f"c{client_count}_r30"
        )
        / "result.json"
    )


def dedicated_uniform1530_result_json(client_count: int):
    return (
        DEDICATED_OVERALL_ROOT
        / "req1530_resp315_uniform"
        / (
            f"dedicated_s1024_qu765-2295_pu158-472_reqstaging_respstaging_"
            f"mgreedy_g0_c{client_count}_r30"
        )
        / "result.json"
    )


def dedicated_uniform38_result_json(client_count: int):
    return (
        DEDICATED_OVERALL_ROOT
        / "req38_resp230_uniform"
        / (
            f"dedicated_s1024_qu19-57_pu115-345_reqstaging_respstaging_"
            f"mgreedy_g0_c{client_count}_r30"
        )
        / "result.json"
    )


def shared_64_result_json(client_count: int):
    return (
        SHARED_ROOT
        / "moti"
        / "req64_resp64"
        / f"shared_s1024_reqb64_respb64_c{client_count}_r30"
        / "result.json"
    )


def shared_uniform1530_result_json(client_count: int):
    return (
        SHARED_ROOT
        / "overall"
        / "req1530_resp315_uniform"
        / f"shared_s1024_requ765-2295_respu158-472_c{client_count}_r30"
        / "result.json"
    )


def shared_uniform38_result_json(client_count: int):
    return (
        SHARED_ROOT
        / "overall"
        / "req38_resp230_uniform"
        / f"shared_s1024_requ19-57_respu115-345_c{client_count}_r30"
        / "result.json"
    )


def noncc_direct_result_json(client_count: int):
    if client_count == 1:
        return (
            DIRECT_C1_RERUN_ROOT
            / "dedicated_s1024_qb64_pb64_reqdirect_respdirect_mgreedy_g0_c1_r30"
            / "result.json"
        )
    return (
        DIRECT_COMPARE_ROOT
        / (
            f"dedicated_s1024_qb64_pb64_reqdirect_respdirect_mgreedy_g0_"
            f"c{client_count}_r30"
        )
        / "result.json"
    )


def response_size_result_json(response_size: int):
    if response_size == 64:
        return dedicated_64_result_json(32)
    return (
        DEDICATED_SENSITIVITY_ROOT
        / f"respsize_req64_resp{response_size}"
        / (
            f"dedicated_s1024_qb64_pb{response_size}_reqstaging_respstaging_"
            "mgreedy_g0_c32_r30"
        )
        / "result.json"
    )


def find_sparse_result_json(slow_client_count: int, slow_request_count: int):
    pct_by_count = {
        0: 0,
        8: 25,
        15: 50,
        30: 100,
    }
    sparse_pct = pct_by_count.get(slow_request_count)
    if sparse_pct is not None:
        candidates = [
            LATEST_ALIGNED_MOTI_SPARSE_ROOT
            / f"sparse16_d{sparse_pct}_c{slow_client_count}"
            / "result.json",
            ALIGNED_MOTI_SPARSE_ROOT
            / f"sparse16_d{sparse_pct}_c{slow_client_count}"
            / "result.json",
        ]
        existing = [candidate for candidate in candidates if candidate.exists()]
        if existing:
            return max(existing, key=lambda path: path.stat().st_mtime)

    pattern = (
        f"sparse32_sc{slow_client_count}_sq{slow_request_count}"
        f"/dedicated_s1024_qb64_pb64_reqstaging_respstaging_mgreedy_g0_"
        f"slow{slow_client_count}_n{slow_request_count}_sg20000_c32_r30/result.json"
    )
    candidates = []
    for root in (MOTIVATION_M2_ROOT, MOTIVATION_M3_ROOT):
        candidate = root / pattern
        if candidate.exists():
            candidates.append(candidate)
    if len(candidates) > 1:
        raise RuntimeError(f"multiple sparse sources for {pattern}: {candidates}")
    if len(candidates) == 1:
        return candidates[0]
    return None


def find_sparse_zero_result_json(slow_client_count: int):
    candidates = [
        LATEST_ALIGNED_MOTI_SPARSE_ROOT
        / f"sparse16_d0_c{slow_client_count}"
        / "result.json",
        ALIGNED_MOTI_SPARSE_ROOT
        / f"sparse16_d0_c{slow_client_count}"
        / "result.json",
    ]
    existing = [candidate for candidate in candidates if candidate.exists()]
    if existing:
        return max(existing, key=lambda path: path.stat().st_mtime)

    candidates = [
        (
            SPARSE_RETEST_ROOT
            / f"sparse32_sc{slow_client_count}_sq0"
            / (
                "dedicated_s1024_qb64_pb64_reqstaging_respstaging_mgreedy_g0_"
                f"slow{slow_client_count}_n0_sg20000_c32_r30"
            )
            / "result.json"
        ),
        (
            SPARSE_RETEST_ROOT
            / f"sparse32_sc{slow_client_count}_sq0"
            / "result.json"
        ),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def load_legacy_staging_cc_rows():
    rows = {}
    with LEGACY_MOTI_DROP2_CSV.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row["case"] != "staging_cc_cxl":
                continue
            rows[int(row["client_count"])] = legacy_row_from_csv(row)
    missing = [count for count in CLIENT_COUNTS if count not in rows]
    if missing:
        raise RuntimeError(
            f"legacy staging_cc_cxl rows missing client counts: {missing}"
        )
    return rows


def is_latest_staging_64_source(result_json: Path):
    try:
        result_json.relative_to(STAGING_COMPARE_ROOT)
        return True
    except ValueError:
        return False


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


def build_moti_tables(moti_outdir: Path):
    legacy_staging_cc_rows = load_legacy_staging_cc_rows()
    manifest_rows = []

    noncc_polling_rows = []
    for case in ("staging_noncc_cxl", "direct_noncc_cxl", "staging_cc_cxl"):
        for client_count in CLIENT_COUNTS:
            if case == "staging_noncc_cxl":
                result_json = dedicated_64_result_json(client_count)
                metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
                note = (
                    "canonicalized to latest 20260406 staging req64/resp64 drop2"
                    if is_latest_staging_64_source(result_json)
                    else "canonicalized to dedicated overall req64/resp64 drop2"
                )
                add_manifest(
                    manifest_rows,
                    "moti_noncc_polling_dedicated",
                    f"{case}:c{client_count}",
                    "result_log",
                    source_path,
                    note,
                )
            elif case == "direct_noncc_cxl":
                result_json = noncc_direct_result_json(client_count)
                metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
                note = (
                    "recomputed from 20260406 direct c1 rerun at drop2"
                    if client_count == 1
                    else "recomputed from noncc direct result log at drop2"
                )
                add_manifest(
                    manifest_rows,
                    "moti_noncc_polling_dedicated",
                    f"{case}:c{client_count}",
                    "result_log",
                    source_path,
                    note,
                )
            else:
                metrics = legacy_staging_cc_rows[client_count]
                add_manifest(
                    manifest_rows,
                    "moti_noncc_polling_dedicated",
                    f"{case}:c{client_count}",
                    "legacy_csv",
                    LEGACY_MOTI_DROP2_CSV,
                    "raw coherent result log unavailable; preserved legacy drop2 row",
                )

            row = {"case": case, "client_count": client_count}
            row.update(metrics)
            noncc_polling_rows.append(row)

    response_size_rows = []
    for response_size in RESPONSE_SIZES:
        result_json = response_size_result_json(response_size)
        metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
        note = (
            "canonicalized to latest 20260406 staging req64/resp64 drop2"
            if response_size == 64 and is_latest_staging_64_source(result_json)
            else (
                "canonicalized to dedicated overall req64/resp64 drop2"
                if response_size == 64
                else "recomputed from response-size dedicated result log at drop2"
            )
        )
        add_manifest(
            manifest_rows,
            "moti_response_size_dedicated",
            f"resp{response_size}",
            "result_log",
            source_path,
            note,
        )
        row = {"response_size_bytes": response_size}
        row.update(metrics)
        response_size_rows.append(row)

    sparse_rows = []
    slow_request_count_by_pct = {25: 8, 50: 15, 100: 30}
    for slow_client_count in SLOW_CLIENT_COUNTS:
        for slow_request_pct in SLOW_REQUEST_PCTS:
            row = {
                "slow_client_count": slow_client_count,
                "slow_request_pct": slow_request_pct,
            }
            if slow_request_pct == 100:
                result_json = dedicated_64_result_json(16)
                metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
                note = (
                    "canonicalized to latest 20260406 staging req64/resp64 c16 drop2"
                    if is_latest_staging_64_source(result_json)
                    else "canonicalized to dedicated overall req64/resp64 c16 drop2"
                )
                add_manifest(
                    manifest_rows,
                    "moti_sparse_polling_dedicated",
                    f"sc{slow_client_count}:pct{slow_request_pct}",
                    "result_log",
                    source_path,
                    note,
                )
            elif slow_request_pct in (25, 50):
                result_json = find_sparse_result_json(
                    slow_client_count, slow_request_count_by_pct[slow_request_pct]
                )
                if result_json is None:
                    raise RuntimeError(
                        "missing sparse result for "
                        f"slow_client_count={slow_client_count} "
                        f"slow_request_pct={slow_request_pct}"
                    )
                metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
                add_manifest(
                    manifest_rows,
                    "moti_sparse_polling_dedicated",
                    f"sc{slow_client_count}:pct{slow_request_pct}",
                    "result_log",
                    source_path,
                    "recomputed from sparse dedicated result log at drop2",
                )
            else:
                result_json = find_sparse_zero_result_json(slow_client_count)
                if result_json is None:
                    metrics = blank_metrics()
                    add_manifest(
                        manifest_rows,
                        "moti_sparse_polling_dedicated",
                        f"sc{slow_client_count}:pct{slow_request_pct}",
                        "missing",
                        "",
                        "no successful 64/64 sparse 0% raw run available",
                    )
                else:
                    metrics, source_path = compute_drop2_metrics_from_result_json(
                        result_json
                    )
                    add_manifest(
                        manifest_rows,
                        "moti_sparse_polling_dedicated",
                        f"sc{slow_client_count}:pct{slow_request_pct}",
                        "result_log",
                        source_path,
                        "recomputed from sparse 0% dedicated result log at drop2",
                    )

            row.update(metrics)
            sparse_rows.append(row)

    contention_rows = []
    for client_count in CLIENT_COUNTS:
        result_json = shared_64_result_json(client_count)
        metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
        add_manifest(
            manifest_rows,
            "moti_contention_shared",
            f"c{client_count}",
            "result_log",
            source_path,
            "canonicalized to shared req64/resp64 moti result log at drop2",
        )
        row = {"client_count": client_count}
        row.update(metrics)
        contention_rows.append(row)

    write_csv(
        moti_outdir / "moti_noncc_polling_dedicated.csv",
        ["case", "client_count", *METRIC_FIELDS],
        noncc_polling_rows,
    )
    write_csv(
        moti_outdir / "moti_response_size_dedicated.csv",
        ["response_size_bytes", *METRIC_FIELDS],
        response_size_rows,
    )
    write_csv(
        moti_outdir / "moti_sparse_polling_dedicated.csv",
        ["slow_client_count", "slow_request_pct", *METRIC_FIELDS],
        sparse_rows,
    )
    write_csv(
        moti_outdir / "moti_contention_shared.csv",
        ["client_count", *METRIC_FIELDS],
        contention_rows,
    )
    write_csv(
        moti_outdir / "source_manifest.csv",
        ["table", "row_key", "source_type", "source_path", "note"],
        manifest_rows,
    )


def build_overall_tables(overall_outdir: Path):
    manifest_rows = []

    overall_specs = [
        (
            "overall_req64_resp64.csv",
            dedicated_64_result_json,
            shared_64_result_json,
            "req64_resp64",
        ),
        (
            "overall_req1530_resp315_uniform.csv",
            dedicated_uniform1530_result_json,
            shared_uniform1530_result_json,
            "req1530_resp315_uniform",
        ),
        (
            "overall_req38_resp230_uniform.csv",
            dedicated_uniform38_result_json,
            shared_uniform38_result_json,
            "req38_resp230_uniform",
        ),
    ]

    for filename, dedicated_fn, shared_fn, series_name in overall_specs:
        rows = []
        for project, result_fn in (
            ("dedicated", dedicated_fn),
            ("shared", shared_fn),
        ):
            for client_count in CLIENT_COUNTS:
                result_json = result_fn(client_count)
                metrics, source_path = compute_drop2_metrics_from_result_json(result_json)
                add_manifest(
                    manifest_rows,
                    filename.replace(".csv", ""),
                    f"{project}:c{client_count}",
                    "result_log",
                    source_path,
                    f"recomputed from {series_name} {project} result log at drop2",
                )
                row = {"project": project, "client_count": client_count}
                row.update(metrics)
                rows.append(row)

        write_csv(
            overall_outdir / filename,
            ["project", "client_count", *METRIC_FIELDS],
            rows,
        )

    write_csv(
        overall_outdir / "source_manifest.csv",
        ["table", "row_key", "source_type", "source_path", "note"],
        manifest_rows,
    )


def parse_args():
    today = date.today().strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--moti-outdir",
        type=Path,
        default=OUTPUT_ROOT / f"moti_tables_drop2_canonical_{today}",
    )
    parser.add_argument(
        "--overall-outdir",
        type=Path,
        default=OUTPUT_ROOT / f"overall_tables_drop2_canonical_{today}",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    build_moti_tables(args.moti_outdir)
    build_overall_tables(args.overall_outdir)
    print(f"wrote {args.moti_outdir}")
    print(f"wrote {args.overall_outdir}")


if __name__ == "__main__":
    main()
