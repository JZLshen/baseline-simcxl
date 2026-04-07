#!/usr/bin/env python3

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


CLIENT_COUNTS = (1, 2, 4, 8, 16, 32)
SLOW_CLIENT_COUNTS = (1, 2, 4, 8)
SLOW_REQUEST_PCTS = (0, 25, 50, 100)
RESPONSE_SIZES = (8, 64, 256, 1024, 4096, 8192)
COUNT_PER_CLIENT = 30
WINDOW_SIZE = 1
QUEUE_SLOTS = 1024
DROP_POLICY = "drop1_per_client"

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

UNIFORM_1530_315 = {
    "req_bytes": 1530,
    "resp_bytes": 315,
    "req_min_bytes": 765,
    "req_max_bytes": 2295,
    "resp_min_bytes": 158,
    "resp_max_bytes": 472,
    "message_profile": "uniform_1530_315",
}

UNIFORM_38_230 = {
    "req_bytes": 38,
    "resp_bytes": 230,
    "req_min_bytes": 19,
    "req_max_bytes": 57,
    "resp_min_bytes": 115,
    "resp_max_bytes": 345,
    "message_profile": "uniform_38_230",
}

FIXED_64_64 = {
    "req_bytes": 64,
    "resp_bytes": 64,
    "req_min_bytes": 0,
    "req_max_bytes": 0,
    "resp_min_bytes": 0,
    "resp_max_bytes": 0,
    "message_profile": "fixed",
}


@dataclass(frozen=True)
class Experiment:
    experiment_id: str
    project: str
    case_kind: str
    transport: str
    req_bytes: int
    resp_bytes: int
    req_min_bytes: int
    req_max_bytes: int
    resp_min_bytes: int
    resp_max_bytes: int
    message_profile: str
    client_count: int
    count_per_client: int
    window_size: int
    request_queue_slots: int
    response_queue_slots: int
    slow_client_count: int = 0
    slow_request_pct: int = 100
    cxl_extra_latency_ns: int = 0


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def metric_stub() -> Dict[str, str]:
    return {field: "" for field in METRIC_FIELDS}


def fixed_case_kwargs(req_bytes: int, resp_bytes: int) -> Dict[str, int | str]:
    return {
        "req_bytes": req_bytes,
        "resp_bytes": resp_bytes,
        "req_min_bytes": 0,
        "req_max_bytes": 0,
        "resp_min_bytes": 0,
        "resp_max_bytes": 0,
        "message_profile": "fixed",
    }


def make_experiment_id(
    project: str,
    transport: str,
    req_bytes: int,
    resp_bytes: int,
    client_count: int,
    slow_client_count: int = 0,
    slow_request_pct: int = 100,
    req_min_bytes: int = 0,
    req_max_bytes: int = 0,
    resp_min_bytes: int = 0,
    resp_max_bytes: int = 0,
) -> str:
    req_tag = (
        f"requ{req_min_bytes}-{req_max_bytes}"
        if req_min_bytes and req_max_bytes and req_min_bytes != req_max_bytes
        else f"req{req_bytes}"
    )
    resp_tag = (
        f"respu{resp_min_bytes}-{resp_max_bytes}"
        if resp_min_bytes and resp_max_bytes and resp_min_bytes != resp_max_bytes
        else f"resp{resp_bytes}"
    )
    suffix = (
        f"_slowc{slow_client_count}_pct{slow_request_pct}"
        if slow_client_count > 0 or slow_request_pct != 100
        else ""
    )
    return f"{project}__{transport}__{req_tag}_{resp_tag}_c{client_count}_r{COUNT_PER_CLIENT}{suffix}"


def add_experiment(experiments: Dict[str, Experiment], **kwargs) -> str:
    experiment = Experiment(**kwargs)
    experiments.setdefault(experiment.experiment_id, experiment)
    return experiment.experiment_id


def build_experiments() -> Dict[str, Experiment]:
    experiments: Dict[str, Experiment] = {}

    for client_count in CLIENT_COUNTS:
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "noncc_dedicated", "staging", 64, 64, client_count
            ),
            project="noncc_dedicated",
            case_kind="bare_rpc",
            transport="staging",
            client_count=client_count,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **FIXED_64_64,
        )
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "noncc_dedicated", "direct", 64, 64, client_count
            ),
            project="noncc_dedicated",
            case_kind="bare_rpc",
            transport="direct",
            client_count=client_count,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **FIXED_64_64,
        )
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "cc_dedicated", "staging_cc", 64, 64, client_count
            ),
            project="cc_dedicated",
            case_kind="bare_rpc",
            transport="staging_cc",
            client_count=client_count,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **FIXED_64_64,
        )
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "shared_5908", "shared", 64, 64, client_count
            ),
            project="shared_5908",
            case_kind="bare_rpc",
            transport="shared",
            client_count=client_count,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **FIXED_64_64,
        )
        add_experiment(
            experiments,
            experiment_id=make_experiment_id("simcxl", "simcxl", 64, 64, client_count),
            project="simcxl",
            case_kind="bare_rpc",
            transport="simcxl",
            client_count=client_count,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **FIXED_64_64,
        )

        for project, transport in (
            ("noncc_dedicated", "staging"),
            ("shared_5908", "shared"),
            ("simcxl", "simcxl"),
        ):
            add_experiment(
                experiments,
                experiment_id=make_experiment_id(
                    project,
                    transport,
                    UNIFORM_1530_315["req_bytes"],
                    UNIFORM_1530_315["resp_bytes"],
                    client_count,
                    req_min_bytes=UNIFORM_1530_315["req_min_bytes"],
                    req_max_bytes=UNIFORM_1530_315["req_max_bytes"],
                    resp_min_bytes=UNIFORM_1530_315["resp_min_bytes"],
                    resp_max_bytes=UNIFORM_1530_315["resp_max_bytes"],
                ),
                project=project,
                case_kind="bare_rpc",
                transport=transport,
                client_count=client_count,
                count_per_client=COUNT_PER_CLIENT,
                window_size=WINDOW_SIZE,
                request_queue_slots=QUEUE_SLOTS,
                response_queue_slots=QUEUE_SLOTS,
                **UNIFORM_1530_315,
            )
            add_experiment(
                experiments,
                experiment_id=make_experiment_id(
                    project,
                    transport,
                    UNIFORM_38_230["req_bytes"],
                    UNIFORM_38_230["resp_bytes"],
                    client_count,
                    req_min_bytes=UNIFORM_38_230["req_min_bytes"],
                    req_max_bytes=UNIFORM_38_230["req_max_bytes"],
                    resp_min_bytes=UNIFORM_38_230["resp_min_bytes"],
                    resp_max_bytes=UNIFORM_38_230["resp_max_bytes"],
                ),
                project=project,
                case_kind="bare_rpc",
                transport=transport,
                client_count=client_count,
                count_per_client=COUNT_PER_CLIENT,
                window_size=WINDOW_SIZE,
                request_queue_slots=QUEUE_SLOTS,
                response_queue_slots=QUEUE_SLOTS,
                **UNIFORM_38_230,
            )

    for slow_client_count in SLOW_CLIENT_COUNTS:
        for slow_request_pct in (0, 25, 50):
            add_experiment(
                experiments,
                experiment_id=make_experiment_id(
                    "noncc_dedicated",
                    "staging",
                    64,
                    64,
                    16,
                    slow_client_count=slow_client_count,
                    slow_request_pct=slow_request_pct,
                ),
                project="noncc_dedicated",
                case_kind="bare_rpc",
                transport="staging",
                client_count=16,
                count_per_client=COUNT_PER_CLIENT,
                window_size=WINDOW_SIZE,
                request_queue_slots=QUEUE_SLOTS,
                response_queue_slots=QUEUE_SLOTS,
                slow_client_count=slow_client_count,
                slow_request_pct=slow_request_pct,
                **FIXED_64_64,
            )

    for response_size in RESPONSE_SIZES:
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "noncc_dedicated", "staging", 64, response_size, 16
            ),
            project="noncc_dedicated",
            case_kind="bare_rpc",
            transport="staging",
            client_count=16,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **fixed_case_kwargs(64, response_size),
        )
        add_experiment(
            experiments,
            experiment_id=make_experiment_id(
                "noncc_dedicated", "direct", 64, response_size, 16
            ),
            project="noncc_dedicated",
            case_kind="bare_rpc",
            transport="direct",
            client_count=16,
            count_per_client=COUNT_PER_CLIENT,
            window_size=WINDOW_SIZE,
            request_queue_slots=QUEUE_SLOTS,
            response_queue_slots=QUEUE_SLOTS,
            **fixed_case_kwargs(64, response_size),
        )

    return experiments


def build_table_rows(experiments: Dict[str, Experiment]) -> Dict[str, List[dict]]:
    rows: Dict[str, List[dict]] = {
        "moti_sparse_request_dedicated.csv": [],
        "moti_mode_compare_dedicated.csv": [],
        "moti_response_size_dedicated.csv": [],
        "moti_shared_client_scaling.csv": [],
        "overall_req64_resp64.csv": [],
        "overall_req1530_resp315_uniform.csv": [],
        "overall_req38_resp230_uniform.csv": [],
    }

    for slow_client_count in SLOW_CLIENT_COUNTS:
        for slow_request_pct in SLOW_REQUEST_PCTS:
            if slow_request_pct == 100:
                experiment_id = make_experiment_id(
                    "noncc_dedicated", "staging", 64, 64, 16
                )
                source_note = "reused from canonical noncc_dedicated staging req64/resp64 c16"
            else:
                experiment_id = make_experiment_id(
                    "noncc_dedicated",
                    "staging",
                    64,
                    64,
                    16,
                    slow_client_count=slow_client_count,
                    slow_request_pct=slow_request_pct,
                )
                source_note = ""
            row = {
                "slow_client_count": slow_client_count,
                "slow_request_pct": slow_request_pct,
                "canonical_experiment_id": experiment_id,
                "drop_policy": DROP_POLICY,
                "source_note": source_note,
            }
            row.update(metric_stub())
            rows["moti_sparse_request_dedicated.csv"].append(row)

    for case, project, transport in (
        ("staging_noncc_dedicated", "noncc_dedicated", "staging"),
        ("direct_noncc_dedicated", "noncc_dedicated", "direct"),
        ("staging_cc_dedicated", "cc_dedicated", "staging_cc"),
    ):
        for client_count in CLIENT_COUNTS:
            row = {
                "case": case,
                "client_count": client_count,
                "canonical_experiment_id": make_experiment_id(
                    project, transport, 64, 64, client_count
                ),
                "drop_policy": DROP_POLICY,
            }
            row.update(metric_stub())
            rows["moti_mode_compare_dedicated.csv"].append(row)

    for case, transport in (
        ("staging_noncc_dedicated", "staging"),
        ("direct_noncc_dedicated", "direct"),
    ):
        for response_size in RESPONSE_SIZES:
            if response_size == 64:
                experiment_id = make_experiment_id(
                    "noncc_dedicated", transport, 64, 64, 16
                )
                source_note = (
                    f"reused from canonical noncc_dedicated {transport} req64/resp64 c16"
                )
            else:
                experiment_id = make_experiment_id(
                    "noncc_dedicated", transport, 64, response_size, 16
                )
                source_note = ""
            row = {
                "case": case,
                "response_size_bytes": response_size,
                "canonical_experiment_id": experiment_id,
                "drop_policy": DROP_POLICY,
                "source_note": source_note,
            }
            row.update(metric_stub())
            rows["moti_response_size_dedicated.csv"].append(row)

    for client_count in CLIENT_COUNTS:
        row = {
            "client_count": client_count,
            "canonical_experiment_id": make_experiment_id(
                "shared_5908", "shared", 64, 64, client_count
            ),
            "drop_policy": DROP_POLICY,
        }
        row.update(metric_stub())
        rows["moti_shared_client_scaling.csv"].append(row)

    for client_count in CLIENT_COUNTS:
        for project, transport, table_name, workload, source_note in (
            (
                "noncc_dedicated",
                "staging",
                "overall_req64_resp64.csv",
                FIXED_64_64,
                "reused from moti staging_noncc_dedicated",
            ),
            (
                "shared_5908",
                "shared",
                "overall_req64_resp64.csv",
                FIXED_64_64,
                "reused from moti shared client scaling",
            ),
            (
                "simcxl",
                "simcxl",
                "overall_req64_resp64.csv",
                FIXED_64_64,
                "",
            ),
            (
                "noncc_dedicated",
                "staging",
                "overall_req1530_resp315_uniform.csv",
                UNIFORM_1530_315,
                "",
            ),
            (
                "shared_5908",
                "shared",
                "overall_req1530_resp315_uniform.csv",
                UNIFORM_1530_315,
                "",
            ),
            (
                "simcxl",
                "simcxl",
                "overall_req1530_resp315_uniform.csv",
                UNIFORM_1530_315,
                "",
            ),
            (
                "noncc_dedicated",
                "staging",
                "overall_req38_resp230_uniform.csv",
                UNIFORM_38_230,
                "",
            ),
            (
                "shared_5908",
                "shared",
                "overall_req38_resp230_uniform.csv",
                UNIFORM_38_230,
                "",
            ),
            (
                "simcxl",
                "simcxl",
                "overall_req38_resp230_uniform.csv",
                UNIFORM_38_230,
                "",
            ),
        ):
            row = {
                "project": project,
                "client_count": client_count,
                "canonical_experiment_id": make_experiment_id(
                    project,
                    transport,
                    workload["req_bytes"],
                    workload["resp_bytes"],
                    client_count,
                    req_min_bytes=workload["req_min_bytes"],
                    req_max_bytes=workload["req_max_bytes"],
                    resp_min_bytes=workload["resp_min_bytes"],
                    resp_max_bytes=workload["resp_max_bytes"],
                ),
                "drop_policy": DROP_POLICY,
                "source_note": source_note,
            }
            row.update(metric_stub())
            rows[table_name].append(row)

    return rows


def build_summary(experiments: Dict[str, Experiment], table_rows: Dict[str, List[dict]]) -> dict:
    project_counts: Dict[str, int] = {}
    for experiment in experiments.values():
        project_counts[experiment.project] = project_counts.get(experiment.project, 0) + 1

    raw_table_rows = sum(len(values) for values in table_rows.values())
    return {
        "drop_policy": DROP_POLICY,
        "window_size": WINDOW_SIZE,
        "count_per_client": COUNT_PER_CLIENT,
        "queue_slots": QUEUE_SLOTS,
        "raw_table_row_count": raw_table_rows,
        "unique_experiment_count_with_cc_moti": len(experiments),
        "unique_experiment_count_three_main_projects": (
            project_counts.get("noncc_dedicated", 0)
            + project_counts.get("shared_5908", 0)
            + project_counts.get("simcxl", 0)
        ),
        "unique_experiment_count_by_project": project_counts,
        "table_row_count_by_csv": {name: len(values) for name, values in table_rows.items()},
    }


def write_experiment_csvs(outdir: Path, experiments: Dict[str, Experiment]) -> None:
    fieldnames = list(asdict(next(iter(experiments.values()))).keys())
    rows = [asdict(experiment) for experiment in experiments.values()]
    rows.sort(key=lambda row: (row["project"], row["experiment_id"]))
    write_csv(outdir / "canonical_experiments.csv", fieldnames, rows)

    for project in ("noncc_dedicated", "cc_dedicated", "shared_5908", "simcxl"):
        project_rows = [row for row in rows if row["project"] == project]
        write_csv(
            outdir / f"canonical_experiments__{project}.csv",
            fieldnames,
            project_rows,
        )


def write_table_templates(outdir: Path, table_rows: Dict[str, List[dict]]) -> None:
    for filename, rows in table_rows.items():
        fieldnames = list(rows[0].keys()) if rows else []
        write_csv(outdir / filename, fieldnames, rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the canonical moti/overall experiment matrix, deduplicated "
            "experiment manifests, and drop1 table templates."
        )
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("output") / "moti_overall_matrix_plan",
        help="Output directory for manifests and table templates.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outdir = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    experiments = build_experiments()
    table_rows = build_table_rows(experiments)
    summary = build_summary(experiments, table_rows)

    write_experiment_csvs(outdir, experiments)
    write_table_templates(outdir / "table_templates", table_rows)

    with (outdir / "summary.json").open("w") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print(outdir)


if __name__ == "__main__":
    main()
