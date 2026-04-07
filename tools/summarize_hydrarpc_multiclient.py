#!/usr/bin/env python3
import argparse
import csv
import json
import math
import pathlib
import re
import statistics as st
import sys


TIMING_PATTERN = re.compile(
    r"^client_(\d+)_req_(\d+)_"
    r"(client_req_start_ts_ns|client_resp_done_ts_ns|"
    r"server_req_observe_ts_ns|server_exec_done_ts_ns|server_resp_done_ts_ns|"
    r"start_ns|end_ns)=(\d+)$"
)
SERVER_LOOP_PATTERN = re.compile(r"^server_loop_start_ts_ns=(\d+)$")
BENCHMARK_RC_PATTERN = re.compile(r"^benchmark_rc=(-?\d+)$")
GUEST_RC_PATTERN = re.compile(r"^guest_command_rc=(-?\d+)$")


def canonical_field_name(name: str):
    if name == "start_ns":
        return "client_req_start_ts_ns"
    if name == "end_ns":
        return "client_resp_done_ts_ns"
    return name


def percentile_nearest_rank(values, percentile: int):
    if not values:
        return None

    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100.0) * len(ordered)))
    return ordered[min(len(ordered) - 1, rank - 1)]


def avg_or_none(values):
    if not values:
        return None
    return sum(values) / len(values)


def safe_median(values):
    if not values:
        return None
    return st.median(values)


def mrps_from_ns(request_count: int, total_ns):
    if total_ns is None:
        return None
    if total_ns <= 0:
        return float("inf")
    return request_count * 1000.0 / total_ns


def kops_from_ns(request_count: int, total_ns):
    mrps = mrps_from_ns(request_count, total_ns)
    if mrps is None:
        return None
    return mrps * 1000.0


def stats_from_distribution(prefix: str, values):
    if not values:
        return {}

    return {
        f"avg_{prefix}_ns": avg_or_none(values),
        f"median_{prefix}_ns": safe_median(values),
        f"p50_{prefix}_ns": percentile_nearest_rank(values, 50),
        f"p90_{prefix}_ns": percentile_nearest_rank(values, 90),
        f"p99_{prefix}_ns": percentile_nearest_rank(values, 99),
    }


def parse_log(log_path: pathlib.Path):
    timing_by_request = {}
    server_loop_start_ts_ns = None
    benchmark_rc = None
    guest_command_rc = None

    with log_path.open("r", errors="ignore") as fh:
        for raw_line in fh:
            line = raw_line.strip().replace("\r", "")
            if not line:
                continue

            match = TIMING_PATTERN.match(line)
            if match:
                client_id = int(match.group(1))
                req_id = int(match.group(2))
                field = canonical_field_name(match.group(3))
                value = int(match.group(4))
                key = (client_id, req_id)
                entry = timing_by_request.setdefault(key, {})
                entry[field] = value
                continue

            match = SERVER_LOOP_PATTERN.match(line)
            if match:
                server_loop_start_ts_ns = int(match.group(1))
                continue

            match = BENCHMARK_RC_PATTERN.match(line)
            if match:
                benchmark_rc = int(match.group(1))
                continue

            match = GUEST_RC_PATTERN.match(line)
            if match:
                guest_command_rc = int(match.group(1))
                continue

    return {
        "timing_by_request": timing_by_request,
        "server_loop_start_ts_ns": server_loop_start_ts_ns,
        "benchmark_rc": benchmark_rc,
        "guest_command_rc": guest_command_rc,
    }


def build_request_rows(timing_by_request: dict, client_count: int, count_per_client: int):
    rows = []
    missing_start = 0
    missing_end = 0
    partial_start_only = 0
    partial_end_only = 0

    for client_id in range(client_count):
        for req_id in range(count_per_client):
            entry = timing_by_request.get((client_id, req_id), {})
            start_ns = entry.get("client_req_start_ts_ns", 0)
            end_ns = entry.get("client_resp_done_ts_ns", 0)
            server_req_observe_ts_ns = entry.get("server_req_observe_ts_ns", 0)
            server_exec_done_ts_ns = entry.get("server_exec_done_ts_ns", 0)
            server_resp_done_ts_ns = entry.get("server_resp_done_ts_ns", 0)

            if start_ns == 0 and end_ns == 0:
                continue

            if start_ns == 0:
                missing_start += 1
                partial_end_only += 1
                continue

            if end_ns == 0:
                missing_end += 1
                partial_start_only += 1
                continue

            rows.append(
                {
                    "client_id": client_id,
                    "req_id": req_id,
                    "start_ns": start_ns,
                    "end_ns": end_ns,
                    "latency_ns": end_ns - start_ns,
                    "server_req_observe_ts_ns": (
                        server_req_observe_ts_ns if server_req_observe_ts_ns != 0 else None
                    ),
                    "server_exec_done_ts_ns": (
                        server_exec_done_ts_ns if server_exec_done_ts_ns != 0 else None
                    ),
                    "server_resp_done_ts_ns": (
                        server_resp_done_ts_ns if server_resp_done_ts_ns != 0 else None
                    ),
                }
            )

    rows.sort(key=lambda row: (row["client_id"], row["req_id"]))
    return rows, missing_start, missing_end, partial_start_only, partial_end_only


def validate_rows(
    rows,
    missing_start: int,
    missing_end: int,
    partial_start_only: int,
    partial_end_only: int,
    expected_total_requests: int,
):
    got_total = len(rows)

    if got_total != expected_total_requests or missing_start != 0 or missing_end != 0:
        return (
            "incomplete timing records: "
            f"got_total={got_total} "
            f"expected_total={expected_total_requests} "
            f"missing_start={missing_start} "
            f"missing_end={missing_end} "
            f"partial_start_only={partial_start_only} "
            f"partial_end_only={partial_end_only}"
        )

    for row in rows:
        if row["latency_ns"] < 0:
            return (
                "invalid timing order: "
                f"client={row['client_id']} req={row['req_id']} "
                f"start_ns={row['start_ns']} end_ns={row['end_ns']}"
            )

    return None


def compute_window_stats(rows):
    if not rows:
        return {
            "total_requests": 0,
            "first_start_ns": None,
            "last_end_ns": None,
            "total_e2e_ns": None,
            "avg_latency_ns": None,
            "median_latency_ns": None,
            "p50_latency_ns": None,
            "p90_latency_ns": None,
            "p99_latency_ns": None,
            "aggregate_throughput_mrps": None,
            "aggregate_throughput_kops": None,
        }

    latencies = [row["latency_ns"] for row in rows]
    first_start_ns = min(row["start_ns"] for row in rows)
    last_end_ns = max(row["end_ns"] for row in rows)
    total_e2e_ns = last_end_ns - first_start_ns

    return {
        "total_requests": len(rows),
        "first_start_ns": first_start_ns,
        "last_end_ns": last_end_ns,
        "total_e2e_ns": total_e2e_ns,
        "avg_latency_ns": avg_or_none(latencies),
        "median_latency_ns": safe_median(latencies),
        "p50_latency_ns": percentile_nearest_rank(latencies, 50),
        "p90_latency_ns": percentile_nearest_rank(latencies, 90),
        "p99_latency_ns": percentile_nearest_rank(latencies, 99),
        "aggregate_throughput_mrps": mrps_from_ns(len(rows), total_e2e_ns),
        "aggregate_throughput_kops": kops_from_ns(len(rows), total_e2e_ns),
    }


def select_steady_rows(rows, drop_first_per_client: int):
    by_client = {}
    kept_rows = []
    dropped_requests_total = 0

    for row in rows:
        by_client.setdefault(row["client_id"], []).append(row)

    for client_rows in by_client.values():
        ordered = sorted(client_rows, key=lambda row: (row["req_id"], row["start_ns"], row["end_ns"]))
        dropped_requests_total += min(drop_first_per_client, len(ordered))
        kept_rows.extend(ordered[drop_first_per_client:])

    kept_rows.sort(key=lambda row: (row["end_ns"], row["client_id"], row["req_id"]))
    return kept_rows, dropped_requests_total


def compute_steady_stats(rows, drop_first_per_client: int):
    kept_rows, dropped_requests_total = select_steady_rows(rows, drop_first_per_client)
    kept_end_ns = [row["end_ns"] for row in kept_rows]
    kept_start_ns = [row["start_ns"] for row in kept_rows]
    kept_latencies = [row["latency_ns"] for row in kept_rows]

    if kept_rows:
        first_kept_start_ns = min(kept_start_ns)
        first_kept_end_ns = kept_end_ns[0]
        last_kept_end_ns = kept_end_ns[-1]
        steady_total_e2e_ns = last_kept_end_ns - first_kept_start_ns
        steady_avg_latency_ns = (
            steady_total_e2e_ns / len(kept_rows)
            if steady_total_e2e_ns > 0
            else float("inf")
        )
        steady_avg_throughput_mrps = mrps_from_ns(len(kept_rows), steady_total_e2e_ns)
    else:
        first_kept_start_ns = None
        first_kept_end_ns = None
        last_kept_end_ns = None
        steady_total_e2e_ns = None
        steady_avg_latency_ns = None
        steady_avg_throughput_mrps = None

    if len(kept_end_ns) >= 2:
        gaps = [
            curr_end_ns - prev_end_ns
            for prev_end_ns, curr_end_ns in zip(kept_end_ns, kept_end_ns[1:])
        ]
        steady_avg_gap_ns = avg_or_none(gaps)
        steady_median_gap_ns = safe_median(gaps)
        steady_p99_gap_ns = percentile_nearest_rank(gaps, 99)
        steady_avg_gap_throughput_mrps = (
            1000.0 / steady_avg_gap_ns if steady_avg_gap_ns and steady_avg_gap_ns > 0 else float("inf")
        )
        steady_median_throughput_mrps = (
            1000.0 / steady_median_gap_ns
            if steady_median_gap_ns and steady_median_gap_ns > 0
            else float("inf")
        )
    else:
        steady_avg_gap_ns = None
        steady_median_gap_ns = None
        steady_p99_gap_ns = None
        steady_avg_gap_throughput_mrps = None
        steady_median_throughput_mrps = None

    return {
        "drop_first_requests_per_client": drop_first_per_client,
        "dropped_requests_total": dropped_requests_total,
        "steady_requests": len(kept_rows),
        "first_kept_start_ns": first_kept_start_ns,
        "first_kept_end_ns": first_kept_end_ns,
        "last_kept_end_ns": last_kept_end_ns,
        "steady_total_e2e_ns": steady_total_e2e_ns,
        "steady_avg_latency_ns": steady_avg_latency_ns,
        "steady_avg_reqresp_latency_ns": avg_or_none(kept_latencies),
        "steady_median_latency_ns": safe_median(kept_latencies),
        "steady_p50_latency_ns": percentile_nearest_rank(kept_latencies, 50),
        "steady_p90_latency_ns": percentile_nearest_rank(kept_latencies, 90),
        "steady_p99_latency_ns": percentile_nearest_rank(kept_latencies, 99),
        "steady_avg_gap_ns": steady_avg_gap_ns,
        "steady_median_gap_ns": steady_median_gap_ns,
        "steady_p90_gap_ns": percentile_nearest_rank(gaps, 90) if len(kept_end_ns) >= 2 else None,
        "steady_p99_gap_ns": steady_p99_gap_ns,
        "steady_avg_throughput_mrps": steady_avg_throughput_mrps,
        "steady_avg_throughput_kops": (
            steady_avg_throughput_mrps * 1000.0
            if steady_avg_throughput_mrps is not None
            else None
        ),
        "steady_avg_gap_throughput_mrps": steady_avg_gap_throughput_mrps,
        "steady_avg_gap_throughput_kops": (
            steady_avg_gap_throughput_mrps * 1000.0
            if steady_avg_gap_throughput_mrps is not None
            else None
        ),
        "steady_median_throughput_mrps": steady_median_throughput_mrps,
        "steady_median_throughput_kops": (
            steady_median_throughput_mrps * 1000.0
            if steady_median_throughput_mrps is not None
            else None
        ),
    }


def compute_server_phase_durations(rows, server_loop_start_ts_ns):
    phase_rows = []
    durations = {}

    for row in rows:
        observe = row.get("server_req_observe_ts_ns")
        exec_done = row.get("server_exec_done_ts_ns")
        resp_done = row.get("server_resp_done_ts_ns")

        if observe is None or exec_done is None or resp_done is None:
            continue

        if exec_done < observe or resp_done < exec_done:
            continue

        key = (row["client_id"], row["req_id"])
        durations[key] = {
            "server_execute_ns": exec_done - observe,
            "server_response_ns": resp_done - exec_done,
        }
        phase_rows.append(row)

    phase_rows.sort(
        key=lambda row: (
            row["server_req_observe_ts_ns"],
            row["client_id"],
            row["req_id"],
        )
    )

    if phase_rows:
        first_row = phase_rows[0]
        first_observe = first_row["server_req_observe_ts_ns"]
        if (
            server_loop_start_ts_ns is not None
            and first_observe >= server_loop_start_ts_ns
        ):
            durations[(first_row["client_id"], first_row["req_id"])][
                "server_poll_notify_ns"
            ] = first_observe - server_loop_start_ts_ns

        for prev_row, row in zip(phase_rows, phase_rows[1:]):
            prev_resp_done = prev_row["server_resp_done_ts_ns"]
            curr_observe = row["server_req_observe_ts_ns"]
            if curr_observe >= prev_resp_done:
                durations[(row["client_id"], row["req_id"])][
                    "server_poll_notify_ns"
                ] = curr_observe - prev_resp_done

    return durations


def summarize_server_phase_durations(rows, phase_durations):
    poll_notify_durations = []
    execute_durations = []
    response_durations = []

    for row in rows:
        key = (row["client_id"], row["req_id"])
        entry = phase_durations.get(key)

        if not entry:
            continue

        if entry.get("server_poll_notify_ns") is not None:
            poll_notify_durations.append(entry["server_poll_notify_ns"])
        if entry.get("server_execute_ns") is not None:
            execute_durations.append(entry["server_execute_ns"])
        if entry.get("server_response_ns") is not None:
            response_durations.append(entry["server_response_ns"])

    stats = {}
    stats.update(stats_from_distribution("server_poll_notify", poll_notify_durations))
    stats.update(stats_from_distribution("server_execute", execute_durations))
    stats.update(stats_from_distribution("server_response", response_durations))
    return stats


def prefix_stats(prefix: str, stats: dict):
    return {f"{prefix}{key}": value for key, value in stats.items()}


def ordered_unique_drops(*drop_values):
    ordered = []
    seen = set()

    for drop in drop_values:
        if drop in seen:
            continue
        seen.add(drop)
        ordered.append(drop)

    return ordered


def compute_steady_stats_with_server_phase(rows, server_phase_durations, drop_first_per_client: int):
    steady_stats = compute_steady_stats(rows, drop_first_per_client)
    steady_rows, _ = select_steady_rows(rows, drop_first_per_client)
    steady_stats.update(
        prefix_stats(
            "steady_",
            summarize_server_phase_durations(steady_rows, server_phase_durations),
        )
    )
    return steady_stats


def print_float(name: str, value):
    if value is None:
        print(f"{name}=")
    elif math.isinf(value):
        print(f"{name}=inf")
    else:
        print(f"{name}={value:.6f}")


def append_csv_row(csv_path: pathlib.Path, fieldnames, row):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def build_summary_csv_row(
    experiment: str,
    client_count: int,
    count_per_client: int,
    outdir: str,
    log_path: str,
    stats: dict,
):
    row = {
        "experiment": experiment,
        "client_count": client_count,
        "count_per_client": count_per_client,
        "total_requests": stats["total_requests"],
        "first_start_ns": stats["first_start_ns"],
        "last_end_ns": stats["last_end_ns"],
        "total_e2e_ns": stats["total_e2e_ns"],
        "avg_latency_ns": fmt_optional_float(stats["avg_latency_ns"], 3),
        "median_latency_ns": fmt_optional_float(stats["median_latency_ns"], 3),
        "p50_latency_ns": fmt_optional_float(stats["p50_latency_ns"], 3),
        "p90_latency_ns": fmt_optional_float(stats["p90_latency_ns"], 3),
        "p99_latency_ns": fmt_optional_float(stats["p99_latency_ns"], 3),
        "aggregate_throughput_mrps": fmt_optional_float(
            stats["aggregate_throughput_mrps"], 6
        ),
        "aggregate_throughput_kops": fmt_optional_float(
            stats["aggregate_throughput_kops"], 3
        ),
        "avg_server_poll_notify_ns": fmt_optional_float(
            stats.get("avg_server_poll_notify_ns"), 3
        ),
        "median_server_poll_notify_ns": fmt_optional_float(
            stats.get("median_server_poll_notify_ns"), 3
        ),
        "p50_server_poll_notify_ns": fmt_optional_float(
            stats.get("p50_server_poll_notify_ns"), 3
        ),
        "p90_server_poll_notify_ns": fmt_optional_float(
            stats.get("p90_server_poll_notify_ns"), 3
        ),
        "p99_server_poll_notify_ns": fmt_optional_float(
            stats.get("p99_server_poll_notify_ns"), 3
        ),
        "avg_server_execute_ns": fmt_optional_float(
            stats.get("avg_server_execute_ns"), 3
        ),
        "median_server_execute_ns": fmt_optional_float(
            stats.get("median_server_execute_ns"), 3
        ),
        "p50_server_execute_ns": fmt_optional_float(
            stats.get("p50_server_execute_ns"), 3
        ),
        "p90_server_execute_ns": fmt_optional_float(
            stats.get("p90_server_execute_ns"), 3
        ),
        "p99_server_execute_ns": fmt_optional_float(
            stats.get("p99_server_execute_ns"), 3
        ),
        "avg_server_response_ns": fmt_optional_float(
            stats.get("avg_server_response_ns"), 3
        ),
        "median_server_response_ns": fmt_optional_float(
            stats.get("median_server_response_ns"), 3
        ),
        "p50_server_response_ns": fmt_optional_float(
            stats.get("p50_server_response_ns"), 3
        ),
        "p90_server_response_ns": fmt_optional_float(
            stats.get("p90_server_response_ns"), 3
        ),
        "p99_server_response_ns": fmt_optional_float(
            stats.get("p99_server_response_ns"), 3
        ),
        "outdir": outdir,
        "log_path": log_path,
    }
    return row


def build_steady_csv_row(
    experiment: str,
    client_count: int,
    count_per_client: int,
    outdir: str,
    log_path: str,
    steady_stats: dict,
):
    row = {
        "experiment": experiment,
        "client_count": client_count,
        "count_per_client": count_per_client,
        "drop_first_requests_per_client": steady_stats["drop_first_requests_per_client"],
        "dropped_requests_total": steady_stats["dropped_requests_total"],
        "steady_requests": steady_stats["steady_requests"],
        "first_kept_start_ns": steady_stats["first_kept_start_ns"],
        "first_kept_end_ns": steady_stats["first_kept_end_ns"],
        "last_kept_end_ns": steady_stats["last_kept_end_ns"],
        "steady_total_e2e_ns": steady_stats["steady_total_e2e_ns"],
        "steady_avg_latency_ns": fmt_optional_float(
            steady_stats["steady_avg_latency_ns"], 3
        ),
        "steady_avg_reqresp_latency_ns": fmt_optional_float(
            steady_stats["steady_avg_reqresp_latency_ns"], 3
        ),
        "steady_median_latency_ns": fmt_optional_float(
            steady_stats["steady_median_latency_ns"], 3
        ),
        "steady_p50_latency_ns": fmt_optional_float(
            steady_stats["steady_p50_latency_ns"], 3
        ),
        "steady_p90_latency_ns": fmt_optional_float(
            steady_stats["steady_p90_latency_ns"], 3
        ),
        "steady_p99_latency_ns": fmt_optional_float(
            steady_stats["steady_p99_latency_ns"], 3
        ),
        "steady_avg_gap_ns": fmt_optional_float(
            steady_stats["steady_avg_gap_ns"], 3
        ),
        "steady_median_gap_ns": fmt_optional_float(
            steady_stats["steady_median_gap_ns"], 3
        ),
        "steady_p90_gap_ns": fmt_optional_float(
            steady_stats["steady_p90_gap_ns"], 3
        ),
        "steady_p99_gap_ns": fmt_optional_float(
            steady_stats["steady_p99_gap_ns"], 3
        ),
        "steady_avg_throughput_mrps": fmt_optional_float(
            steady_stats["steady_avg_throughput_mrps"], 6
        ),
        "steady_avg_throughput_kops": fmt_optional_float(
            steady_stats["steady_avg_throughput_kops"], 3
        ),
        "steady_avg_gap_throughput_mrps": fmt_optional_float(
            steady_stats["steady_avg_gap_throughput_mrps"], 6
        ),
        "steady_avg_gap_throughput_kops": fmt_optional_float(
            steady_stats["steady_avg_gap_throughput_kops"], 3
        ),
        "steady_median_throughput_mrps": fmt_optional_float(
            steady_stats["steady_median_throughput_mrps"], 6
        ),
        "steady_median_throughput_kops": fmt_optional_float(
            steady_stats["steady_median_throughput_kops"], 3
        ),
        "steady_avg_server_poll_notify_ns": fmt_optional_float(
            steady_stats.get("steady_avg_server_poll_notify_ns"), 3
        ),
        "steady_median_server_poll_notify_ns": fmt_optional_float(
            steady_stats.get("steady_median_server_poll_notify_ns"), 3
        ),
        "steady_p50_server_poll_notify_ns": fmt_optional_float(
            steady_stats.get("steady_p50_server_poll_notify_ns"), 3
        ),
        "steady_p90_server_poll_notify_ns": fmt_optional_float(
            steady_stats.get("steady_p90_server_poll_notify_ns"), 3
        ),
        "steady_p99_server_poll_notify_ns": fmt_optional_float(
            steady_stats.get("steady_p99_server_poll_notify_ns"), 3
        ),
        "steady_avg_server_execute_ns": fmt_optional_float(
            steady_stats.get("steady_avg_server_execute_ns"), 3
        ),
        "steady_median_server_execute_ns": fmt_optional_float(
            steady_stats.get("steady_median_server_execute_ns"), 3
        ),
        "steady_p50_server_execute_ns": fmt_optional_float(
            steady_stats.get("steady_p50_server_execute_ns"), 3
        ),
        "steady_p90_server_execute_ns": fmt_optional_float(
            steady_stats.get("steady_p90_server_execute_ns"), 3
        ),
        "steady_p99_server_execute_ns": fmt_optional_float(
            steady_stats.get("steady_p99_server_execute_ns"), 3
        ),
        "steady_avg_server_response_ns": fmt_optional_float(
            steady_stats.get("steady_avg_server_response_ns"), 3
        ),
        "steady_median_server_response_ns": fmt_optional_float(
            steady_stats.get("steady_median_server_response_ns"), 3
        ),
        "steady_p50_server_response_ns": fmt_optional_float(
            steady_stats.get("steady_p50_server_response_ns"), 3
        ),
        "steady_p90_server_response_ns": fmt_optional_float(
            steady_stats.get("steady_p90_server_response_ns"), 3
        ),
        "steady_p99_server_response_ns": fmt_optional_float(
            steady_stats.get("steady_p99_server_response_ns"), 3
        ),
        "outdir": outdir,
        "log_path": log_path,
    }
    return row


def fmt_optional_float(value, digits: int):
    if value is None:
        return ""
    if math.isinf(value):
        return "inf"
    return f"{value:.{digits}f}"


def parse_extra_fields(raw_items):
    extra = {}

    for raw in raw_items or []:
        if "=" not in raw:
            raise ValueError(f"invalid --extra-field {raw!r}, expected KEY=VALUE")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"invalid --extra-field {raw!r}, empty key")
        extra[key] = value

    return extra


def write_result_json(
    result_json_path: pathlib.Path,
    status: str,
    experiment: str,
    client_count: int,
    count_per_client: int,
    log_path: str,
    benchmark_rc,
    guest_command_rc,
    extra_fields=None,
    stats=None,
    steady_stats=None,
    steady_drop1_stats=None,
    steady_drop5_stats=None,
    error=None,
):
    payload = {
        "status": status,
        "experiment": experiment,
        "client_count": client_count,
        "count_per_client": count_per_client,
        "log_path": log_path,
        "benchmark_rc": benchmark_rc,
        "guest_command_rc": guest_command_rc,
    }
    if extra_fields:
        payload["extra_fields"] = dict(extra_fields)
    if stats is not None:
        payload["stats"] = stats
    if steady_stats is not None:
        payload["steady_stats"] = steady_stats
    if steady_drop1_stats is not None:
        payload["steady_drop1_stats"] = steady_drop1_stats
    if steady_drop5_stats is not None:
        payload["steady_drop5_stats"] = steady_drop5_stats
    if error is not None:
        payload["error"] = error

    result_json_path.parent.mkdir(parents=True, exist_ok=True)
    with result_json_path.open("w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def record_failure_csv(
    fail_csv_path: pathlib.Path,
    experiment: str,
    client_count: int,
    count_per_client: int,
    outdir: str,
    log_path: str,
    reason: str,
    extra_fields=None,
):
    fieldnames = [
        "experiment",
        "client_count",
        "count_per_client",
        "outdir",
        "log_path",
        "reason",
    ]
    if extra_fields:
        fieldnames.extend(extra_fields.keys())
    append_csv_row(
        fail_csv_path,
        fieldnames,
        {
            "experiment": experiment,
            "client_count": client_count,
            "count_per_client": count_per_client,
            "outdir": outdir,
            "log_path": log_path,
            "reason": reason,
            **(extra_fields or {}),
        },
    )


def print_summary(
    benchmark_rc,
    guest_command_rc,
    stats: dict,
    steady_stats: dict,
):
    print(f"benchmark_rc={benchmark_rc}")
    print(f"guest_command_rc={guest_command_rc}")

    print(f"total_requests={stats['total_requests']}")
    print(f"first_start_ns={stats['first_start_ns']}")
    print(f"last_end_ns={stats['last_end_ns']}")
    print(f"total_e2e_ns={stats['total_e2e_ns']}")
    print_float("avg_latency_ns", stats["avg_latency_ns"])
    print_float("median_latency_ns", stats["median_latency_ns"])
    print_float("p50_latency_ns", stats["p50_latency_ns"])
    print_float("p90_latency_ns", stats["p90_latency_ns"])
    print_float("p99_latency_ns", stats["p99_latency_ns"])
    print_float("aggregate_throughput_mrps", stats["aggregate_throughput_mrps"])
    print_float("aggregate_throughput_kops", stats["aggregate_throughput_kops"])

    for key in (
        "avg_server_poll_notify_ns",
        "median_server_poll_notify_ns",
        "p50_server_poll_notify_ns",
        "p90_server_poll_notify_ns",
        "p99_server_poll_notify_ns",
        "avg_server_execute_ns",
        "median_server_execute_ns",
        "p50_server_execute_ns",
        "p90_server_execute_ns",
        "p99_server_execute_ns",
        "avg_server_response_ns",
        "median_server_response_ns",
        "p50_server_response_ns",
        "p90_server_response_ns",
        "p99_server_response_ns",
    ):
        if key in stats:
            print_float(key, stats[key])

    print(
        f"drop_first_requests_per_client={steady_stats['drop_first_requests_per_client']}"
    )
    print(f"dropped_requests_total={steady_stats['dropped_requests_total']}")
    print(f"steady_requests={steady_stats['steady_requests']}")
    print(f"first_kept_start_ns={steady_stats['first_kept_start_ns']}")
    print(f"first_kept_end_ns={steady_stats['first_kept_end_ns']}")
    print(f"last_kept_end_ns={steady_stats['last_kept_end_ns']}")
    print(f"steady_total_e2e_ns={steady_stats['steady_total_e2e_ns']}")
    print_float("steady_avg_latency_ns", steady_stats["steady_avg_latency_ns"])
    print_float(
        "steady_avg_reqresp_latency_ns",
        steady_stats["steady_avg_reqresp_latency_ns"],
    )
    print_float(
        "steady_median_latency_ns",
        steady_stats["steady_median_latency_ns"],
    )
    print_float("steady_p50_latency_ns", steady_stats["steady_p50_latency_ns"])
    print_float("steady_p90_latency_ns", steady_stats["steady_p90_latency_ns"])
    print_float("steady_p99_latency_ns", steady_stats["steady_p99_latency_ns"])
    print_float("steady_avg_gap_ns", steady_stats["steady_avg_gap_ns"])
    print_float("steady_median_gap_ns", steady_stats["steady_median_gap_ns"])
    print_float("steady_p90_gap_ns", steady_stats["steady_p90_gap_ns"])
    print_float("steady_p99_gap_ns", steady_stats["steady_p99_gap_ns"])
    print_float(
        "steady_avg_throughput_mrps",
        steady_stats["steady_avg_throughput_mrps"],
    )
    print_float(
        "steady_avg_throughput_kops",
        steady_stats["steady_avg_throughput_kops"],
    )
    print_float(
        "steady_avg_gap_throughput_mrps",
        steady_stats["steady_avg_gap_throughput_mrps"],
    )
    print_float(
        "steady_avg_gap_throughput_kops",
        steady_stats["steady_avg_gap_throughput_kops"],
    )
    print_float(
        "steady_median_throughput_mrps",
        steady_stats["steady_median_throughput_mrps"],
    )
    print_float(
        "steady_median_throughput_kops",
        steady_stats["steady_median_throughput_kops"],
    )

    for key in (
        "steady_avg_server_poll_notify_ns",
        "steady_median_server_poll_notify_ns",
        "steady_p50_server_poll_notify_ns",
        "steady_p90_server_poll_notify_ns",
        "steady_p99_server_poll_notify_ns",
        "steady_avg_server_execute_ns",
        "steady_median_server_execute_ns",
        "steady_p50_server_execute_ns",
        "steady_p90_server_execute_ns",
        "steady_p99_server_execute_ns",
        "steady_avg_server_response_ns",
        "steady_median_server_response_ns",
        "steady_p50_server_response_ns",
        "steady_p90_server_response_ns",
        "steady_p99_server_response_ns",
    ):
        if key in steady_stats:
            print_float(key, steady_stats[key])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--client-count", type=int, required=True)
    parser.add_argument("--count-per-client", type=int, required=True)
    parser.add_argument("--expected-total-requests", type=int)
    parser.add_argument("--drop-first-per-client", type=int, default=2)
    parser.add_argument("--csv")
    parser.add_argument("--steady-csv")
    parser.add_argument("--fail-csv")
    parser.add_argument("--outdir", default="")
    parser.add_argument("--result-json")
    parser.add_argument("--extra-field", action="append", default=[])
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        extra_fields = parse_extra_fields(args.extra_field)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    log_path = pathlib.Path(args.log).resolve()
    parsed = parse_log(log_path)

    expected_total_requests = args.expected_total_requests
    if expected_total_requests is None:
        expected_total_requests = args.client_count * args.count_per_client

    benchmark_rc = parsed["benchmark_rc"]
    guest_command_rc = parsed["guest_command_rc"]

    if benchmark_rc is None or guest_command_rc is None:
        reason = (
            "missing command rc markers: "
            f"benchmark_rc={benchmark_rc} guest_command_rc={guest_command_rc}"
        )
        print(reason, file=sys.stderr)
        if args.fail_csv:
            record_failure_csv(
                pathlib.Path(args.fail_csv),
                args.experiment,
                args.client_count,
                args.count_per_client,
                args.outdir,
                str(log_path),
                reason,
                extra_fields=extra_fields,
            )
        if args.result_json:
            write_result_json(
                pathlib.Path(args.result_json),
                "failure",
                args.experiment,
                args.client_count,
                args.count_per_client,
                str(log_path),
                benchmark_rc,
                guest_command_rc,
                extra_fields=extra_fields,
                error=reason,
            )
        return 1

    if benchmark_rc != 0 or guest_command_rc != 0:
        reason = (
            "benchmark failed: "
            f"benchmark_rc={benchmark_rc} guest_command_rc={guest_command_rc}"
        )
        print(reason, file=sys.stderr)
        if args.fail_csv:
            record_failure_csv(
                pathlib.Path(args.fail_csv),
                args.experiment,
                args.client_count,
                args.count_per_client,
                args.outdir,
                str(log_path),
                reason,
                extra_fields=extra_fields,
            )
        if args.result_json:
            write_result_json(
                pathlib.Path(args.result_json),
                "failure",
                args.experiment,
                args.client_count,
                args.count_per_client,
                str(log_path),
                benchmark_rc,
                guest_command_rc,
                extra_fields=extra_fields,
                error=reason,
            )
        return 1

    rows, missing_start, missing_end, partial_start_only, partial_end_only = (
        build_request_rows(
            parsed["timing_by_request"],
            args.client_count,
            args.count_per_client,
        )
    )

    validation_error = validate_rows(
        rows,
        missing_start,
        missing_end,
        partial_start_only,
        partial_end_only,
        expected_total_requests,
    )
    if validation_error is not None:
        print(validation_error, file=sys.stderr)
        if args.fail_csv:
            record_failure_csv(
                pathlib.Path(args.fail_csv),
                args.experiment,
                args.client_count,
                args.count_per_client,
                args.outdir,
                str(log_path),
                validation_error,
                extra_fields=extra_fields,
            )
        if args.result_json:
            write_result_json(
                pathlib.Path(args.result_json),
                "failure",
                args.experiment,
                args.client_count,
                args.count_per_client,
                str(log_path),
                benchmark_rc,
                guest_command_rc,
                extra_fields=extra_fields,
                error=validation_error,
            )
        return 1

    server_phase_durations = compute_server_phase_durations(
        rows, parsed["server_loop_start_ts_ns"]
    )

    stats = compute_window_stats(rows)
    stats.update(summarize_server_phase_durations(rows, server_phase_durations))

    steady_drop1_stats = compute_steady_stats_with_server_phase(
        rows, server_phase_durations, 1
    )
    steady_drop5_stats = compute_steady_stats_with_server_phase(
        rows, server_phase_durations, 5
    )
    steady_stats_by_drop = {
        1: steady_drop1_stats,
        5: steady_drop5_stats,
    }
    if args.drop_first_per_client not in steady_stats_by_drop:
        steady_stats_by_drop[args.drop_first_per_client] = (
            compute_steady_stats_with_server_phase(
                rows,
                server_phase_durations,
                args.drop_first_per_client,
            )
        )
    steady_stats = steady_stats_by_drop[args.drop_first_per_client]

    print_summary(benchmark_rc, guest_command_rc, stats, steady_stats)

    if args.csv:
        summary_fieldnames = [
            "experiment",
            "client_count",
            "count_per_client",
            "total_requests",
            "first_start_ns",
            "last_end_ns",
            "total_e2e_ns",
            "avg_latency_ns",
            "median_latency_ns",
            "p50_latency_ns",
            "p90_latency_ns",
            "p99_latency_ns",
            "aggregate_throughput_mrps",
            "aggregate_throughput_kops",
            "avg_server_poll_notify_ns",
            "median_server_poll_notify_ns",
            "p50_server_poll_notify_ns",
            "p90_server_poll_notify_ns",
            "p99_server_poll_notify_ns",
            "avg_server_execute_ns",
            "median_server_execute_ns",
            "p50_server_execute_ns",
            "p90_server_execute_ns",
            "p99_server_execute_ns",
            "avg_server_response_ns",
            "median_server_response_ns",
            "p50_server_response_ns",
            "p90_server_response_ns",
            "p99_server_response_ns",
            "outdir",
            "log_path",
        ]
        summary_row = build_summary_csv_row(
            args.experiment,
            args.client_count,
            args.count_per_client,
            args.outdir,
            str(log_path),
            stats,
        )
        summary_row.update(extra_fields)
        summary_fieldnames.extend(extra_fields.keys())
        append_csv_row(
            pathlib.Path(args.csv),
            summary_fieldnames,
            summary_row,
        )

    if args.steady_csv:
        steady_fieldnames = [
            "experiment",
            "client_count",
            "count_per_client",
            "drop_first_requests_per_client",
            "dropped_requests_total",
            "steady_requests",
            "first_kept_start_ns",
            "first_kept_end_ns",
            "last_kept_end_ns",
            "steady_total_e2e_ns",
            "steady_avg_latency_ns",
            "steady_avg_reqresp_latency_ns",
            "steady_median_latency_ns",
            "steady_p50_latency_ns",
            "steady_p90_latency_ns",
            "steady_p99_latency_ns",
            "steady_avg_gap_ns",
            "steady_median_gap_ns",
            "steady_p90_gap_ns",
            "steady_p99_gap_ns",
            "steady_avg_throughput_mrps",
            "steady_avg_throughput_kops",
            "steady_avg_gap_throughput_mrps",
            "steady_avg_gap_throughput_kops",
            "steady_median_throughput_mrps",
            "steady_median_throughput_kops",
            "steady_avg_server_poll_notify_ns",
            "steady_median_server_poll_notify_ns",
            "steady_p50_server_poll_notify_ns",
            "steady_p90_server_poll_notify_ns",
            "steady_p99_server_poll_notify_ns",
            "steady_avg_server_execute_ns",
            "steady_median_server_execute_ns",
            "steady_p50_server_execute_ns",
            "steady_p90_server_execute_ns",
            "steady_p99_server_execute_ns",
            "steady_avg_server_response_ns",
            "steady_median_server_response_ns",
            "steady_p50_server_response_ns",
            "steady_p90_server_response_ns",
            "steady_p99_server_response_ns",
            "outdir",
            "log_path",
        ]
        steady_fieldnames.extend(extra_fields.keys())
        for drop in ordered_unique_drops(args.drop_first_per_client):
            steady_row = build_steady_csv_row(
                args.experiment,
                args.client_count,
                args.count_per_client,
                args.outdir,
                str(log_path),
                steady_stats_by_drop[drop],
            )
            steady_row.update(extra_fields)
            append_csv_row(
                pathlib.Path(args.steady_csv),
                steady_fieldnames,
                steady_row,
            )

    if args.result_json:
        write_result_json(
            pathlib.Path(args.result_json),
            "success",
            args.experiment,
            args.client_count,
            args.count_per_client,
            str(log_path),
            benchmark_rc,
            guest_command_rc,
            extra_fields=extra_fields,
            stats=stats,
            steady_stats=steady_stats,
            steady_drop1_stats=steady_drop1_stats,
            steady_drop5_stats=steady_drop5_stats,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
