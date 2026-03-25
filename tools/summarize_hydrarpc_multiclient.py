#!/usr/bin/env python3
import argparse
import csv
import json
import pathlib
import re
import statistics as st
import sys


TIMING_PATTERN = re.compile(r"^client_(\d+)_req_(\d+)_(start_ns|end_ns)=(\d+)$")
FAIL_PATTERN = re.compile(
    r"^client_(\d+)_req_(\d+)_correctness_fail expected=(\d+) actual=(\d+)$"
)
BREAKDOWN_PATTERN = re.compile(
    r"^client_(\d+)_req_(\d+)_"
    r"(request_publish_ns|server_observe_ns|response_publish_ns|response_observe_ns|"
    r"client_request_poll_count|client_response_poll_count|"
    r"server_request_poll_count|server_response_poll_count)=(\d+)$"
)
BENCHMARK_RC_PATTERN = re.compile(r"^benchmark_rc=(-?\d+)$")
GUEST_RC_PATTERN = re.compile(r"^guest_command_rc=(-?\d+)$")


def build_request_rows(starts: dict, ends: dict):
    rows = []
    for key in sorted(starts):
        start_ns = starts[key]
        end_ns = ends[key]
        rows.append(
            {
                "client_id": key[0],
                "req_id": key[1],
                "start_ns": start_ns,
                "end_ns": end_ns,
                "latency_ns": end_ns - start_ns,
            }
        )
    return rows


def compute_window_stats(request_rows):
    if not request_rows:
        return {
            "total_requests": 0,
            "first_start_ns": None,
            "last_end_ns": None,
            "total_e2e_ns": None,
            "avg_latency_ns": None,
            "median_latency_ns": None,
            "aggregate_throughput_mrps": None,
        }

    latencies = [row["latency_ns"] for row in request_rows]
    first_start_ns = min(row["start_ns"] for row in request_rows)
    last_end_ns = max(row["end_ns"] for row in request_rows)
    total_e2e_ns = last_end_ns - first_start_ns
    throughput_mrps = (
        (len(request_rows) * 1000.0 / total_e2e_ns)
        if total_e2e_ns > 0
        else float("inf")
    )

    return {
        "total_requests": len(request_rows),
        "first_start_ns": first_start_ns,
        "last_end_ns": last_end_ns,
        "total_e2e_ns": total_e2e_ns,
        "avg_latency_ns": sum(latencies) / len(latencies),
        "median_latency_ns": st.median(latencies),
        "aggregate_throughput_mrps": throughput_mrps,
    }


def select_steady_rows(request_rows, drop_first_per_client: int):
    by_client = {}
    kept_rows = []
    dropped_requests_total = 0

    for row in request_rows:
        by_client.setdefault(row["client_id"], []).append(row)

    for client_rows in by_client.values():
        ordered = sorted(
            client_rows,
            key=lambda row: (row["req_id"], row["start_ns"], row["end_ns"]),
        )
        dropped_requests_total += min(drop_first_per_client, len(ordered))
        kept_rows.extend(ordered[drop_first_per_client:])

    kept_rows.sort(key=lambda row: (row["end_ns"], row["client_id"], row["req_id"]))
    return kept_rows, dropped_requests_total


def compute_steady_stats(request_rows, drop_first_per_client: int):
    kept_rows, dropped_requests_total = select_steady_rows(
        request_rows, drop_first_per_client
    )
    kept_end_ns = [row["end_ns"] for row in kept_rows]
    kept_start_ns = [row["start_ns"] for row in kept_rows]
    kept_latencies = [row["latency_ns"] for row in kept_rows]

    if kept_end_ns:
        first_kept_start_ns = min(kept_start_ns)
        first_kept_end_ns = kept_end_ns[0]
        last_kept_end_ns = kept_end_ns[-1]
    else:
        first_kept_start_ns = None
        first_kept_end_ns = None
        last_kept_end_ns = None

    if kept_rows and first_kept_start_ns is not None and last_kept_end_ns is not None:
        total_e2e_ns = last_kept_end_ns - first_kept_start_ns
        avg_latency_ns = (
            (total_e2e_ns / len(kept_rows))
            if total_e2e_ns > 0
            else float("inf")
        )
        avg_throughput_mrps = (
            (len(kept_rows) * 1000.0 / total_e2e_ns)
            if total_e2e_ns > 0
            else float("inf")
        )
    else:
        total_e2e_ns = None
        avg_latency_ns = None
        avg_throughput_mrps = None

    if kept_latencies:
        avg_reqresp_latency_ns = sum(kept_latencies) / len(kept_latencies)
        median_reqresp_latency_ns = st.median(kept_latencies)
    else:
        avg_reqresp_latency_ns = None
        median_reqresp_latency_ns = None

    if len(kept_end_ns) >= 2:
        gaps = [
            curr_end_ns - prev_end_ns
            for prev_end_ns, curr_end_ns in zip(kept_end_ns, kept_end_ns[1:])
        ]
        avg_gap_ns = sum(gaps) / len(gaps)
        median_gap_ns = st.median(gaps)
        avg_gap_throughput_mrps = (
            (1000.0 / avg_gap_ns) if avg_gap_ns > 0 else float("inf")
        )
        median_throughput_mrps = (
            (1000.0 / median_gap_ns) if median_gap_ns > 0 else float("inf")
        )
    else:
        avg_gap_ns = None
        median_gap_ns = None
        avg_gap_throughput_mrps = None
        median_throughput_mrps = None

    return {
        "drop_first_requests_per_client": drop_first_per_client,
        "dropped_requests_total": dropped_requests_total,
        "steady_requests": len(kept_rows),
        "first_kept_start_ns": first_kept_start_ns,
        "steady_avg_latency_ns": avg_latency_ns,
        "steady_total_e2e_ns": total_e2e_ns,
        "steady_avg_reqresp_latency_ns": avg_reqresp_latency_ns,
        "steady_median_latency_ns": median_reqresp_latency_ns,
        "steady_avg_gap_ns": avg_gap_ns,
        "steady_median_gap_ns": median_gap_ns,
        "steady_avg_throughput_mrps": avg_throughput_mrps,
        "steady_avg_gap_throughput_mrps": avg_gap_throughput_mrps,
        "steady_median_throughput_mrps": median_throughput_mrps,
        "first_kept_end_ns": first_kept_end_ns,
        "last_kept_end_ns": last_kept_end_ns,
    }


def compute_breakdown_stats(request_rows, breakdown_by_request: dict):
    req_to_srv = []
    srv_to_resp = []
    resp_to_cli = []
    req_to_cli = []
    client_req_polls = []
    client_resp_polls = []
    server_req_polls = []
    server_resp_polls = []

    for row in request_rows:
        key = (row["client_id"], row["req_id"])
        breakdown = breakdown_by_request.get(key)

        if not breakdown:
            continue

        request_publish_ns = breakdown.get("request_publish_ns")
        server_observe_ns = breakdown.get("server_observe_ns")
        response_publish_ns = breakdown.get("response_publish_ns")
        response_observe_ns = breakdown.get("response_observe_ns")

        if (
            request_publish_ns is not None
            and server_observe_ns is not None
            and server_observe_ns >= request_publish_ns
        ):
            req_to_srv.append(server_observe_ns - request_publish_ns)

        if (
            server_observe_ns is not None
            and response_publish_ns is not None
            and response_publish_ns >= server_observe_ns
        ):
            srv_to_resp.append(response_publish_ns - server_observe_ns)

        if (
            response_publish_ns is not None
            and response_observe_ns is not None
            and response_observe_ns >= response_publish_ns
        ):
            resp_to_cli.append(response_observe_ns - response_publish_ns)

        if (
            request_publish_ns is not None
            and response_observe_ns is not None
            and response_observe_ns >= request_publish_ns
        ):
            req_to_cli.append(response_observe_ns - request_publish_ns)

        if breakdown.get("client_request_poll_count") is not None:
            client_req_polls.append(breakdown["client_request_poll_count"])
        if breakdown.get("client_response_poll_count") is not None:
            client_resp_polls.append(breakdown["client_response_poll_count"])
        if breakdown.get("server_request_poll_count") is not None:
            server_req_polls.append(breakdown["server_request_poll_count"])
        if breakdown.get("server_response_poll_count") is not None:
            server_resp_polls.append(breakdown["server_response_poll_count"])

    stats = {}

    if req_to_srv:
        stats["avg_req_publish_to_server_observe_ns"] = sum(req_to_srv) / len(req_to_srv)
    if srv_to_resp:
        stats["avg_server_observe_to_resp_publish_ns"] = sum(srv_to_resp) / len(srv_to_resp)
    if resp_to_cli:
        stats["avg_resp_publish_to_client_observe_ns"] = sum(resp_to_cli) / len(resp_to_cli)
    if req_to_cli:
        stats["avg_req_publish_to_client_observe_ns"] = sum(req_to_cli) / len(req_to_cli)
    if client_req_polls:
        stats["avg_client_request_poll_count"] = sum(client_req_polls) / len(client_req_polls)
    if client_resp_polls:
        stats["avg_client_response_poll_count"] = sum(client_resp_polls) / len(client_resp_polls)
    if server_req_polls:
        stats["avg_server_request_poll_count"] = sum(server_req_polls) / len(server_req_polls)
    if server_resp_polls:
        stats["avg_server_response_poll_count"] = sum(server_resp_polls) / len(server_resp_polls)

    return stats


def prefix_stats(prefix: str, stats: dict):
    return {f"{prefix}{key}": value for key, value in stats.items()}


def parse_log(log_path: pathlib.Path):
    starts = {}
    ends = {}
    breakdown_by_request = {}
    fail_rows = []
    benchmark_rc = None
    guest_command_rc = None

    with log_path.open("r", errors="ignore") as fh:
        for raw_line in fh:
            line = raw_line.strip().replace("\r", "")
            match = TIMING_PATTERN.match(line)
            if match:
                client_id = int(match.group(1))
                req_id = int(match.group(2))
                kind = match.group(3)
                value = int(match.group(4))
                key = (client_id, req_id)
                if kind == "start_ns":
                    starts[key] = value
                else:
                    ends[key] = value
                continue

            match = BREAKDOWN_PATTERN.match(line)
            if match:
                client_id = int(match.group(1))
                req_id = int(match.group(2))
                kind = match.group(3)
                value = int(match.group(4))
                breakdown_by_request.setdefault((client_id, req_id), {})[kind] = value
                continue

            match = FAIL_PATTERN.match(line)
            if match:
                fail_rows.append(
                    {
                        "client_id": int(match.group(1)),
                        "req_id": int(match.group(2)),
                        "expected": int(match.group(3)),
                        "actual": int(match.group(4)),
                    }
                )
                continue

            match = BENCHMARK_RC_PATTERN.match(line)
            if match:
                benchmark_rc = int(match.group(1))
                continue

            match = GUEST_RC_PATTERN.match(line)
            if match:
                guest_command_rc = int(match.group(1))
                continue

    missing_end = sorted(set(starts) - set(ends))
    missing_start = sorted(set(ends) - set(starts))
    if missing_end or missing_start:
        raise ValueError(
            "incomplete timing records: "
            f"missing_end={len(missing_end)} missing_start={len(missing_start)}"
        )

    request_rows = build_request_rows(starts, ends)
    stats = compute_window_stats(request_rows)
    stats.update(compute_breakdown_stats(request_rows, breakdown_by_request))
    stats.update(
        {
            "correctness_fail_count": len(fail_rows),
            "fail_rows": fail_rows,
            "benchmark_rc": benchmark_rc,
            "guest_command_rc": guest_command_rc,
        }
    )
    return stats, request_rows, breakdown_by_request


def print_text_summary(label: str, client_count: int, count_per_client: int, stats):
    print(f"experiment={label}")
    print(f"client_count={client_count}")
    print(f"count_per_client={count_per_client}")
    print(f"total_requests={stats['total_requests']}")
    print(f"first_start_ns={stats['first_start_ns']}")
    print(f"last_end_ns={stats['last_end_ns']}")
    print(f"total_e2e_ns={stats['total_e2e_ns']}")
    print(f"avg_latency_ns={stats['avg_latency_ns']:.3f}")
    print(f"median_latency_ns={stats['median_latency_ns']:.3f}")
    print(
        "aggregate_throughput_mrps="
        f"{stats['aggregate_throughput_mrps']:.6f}"
    )
    print(f"correctness_fail_count={stats['correctness_fail_count']}")
    if "avg_req_publish_to_server_observe_ns" in stats:
        print(
            "avg_req_publish_to_server_observe_ns="
            f"{stats['avg_req_publish_to_server_observe_ns']:.3f}"
        )
    if "avg_server_observe_to_resp_publish_ns" in stats:
        print(
            "avg_server_observe_to_resp_publish_ns="
            f"{stats['avg_server_observe_to_resp_publish_ns']:.3f}"
        )
    if "avg_resp_publish_to_client_observe_ns" in stats:
        print(
            "avg_resp_publish_to_client_observe_ns="
            f"{stats['avg_resp_publish_to_client_observe_ns']:.3f}"
        )
    if "avg_req_publish_to_client_observe_ns" in stats:
        print(
            "avg_req_publish_to_client_observe_ns="
            f"{stats['avg_req_publish_to_client_observe_ns']:.3f}"
        )
    if "avg_client_request_poll_count" in stats:
        print(
            "avg_client_request_poll_count="
            f"{stats['avg_client_request_poll_count']:.3f}"
        )
    if "avg_client_response_poll_count" in stats:
        print(
            "avg_client_response_poll_count="
            f"{stats['avg_client_response_poll_count']:.3f}"
        )
    if "avg_server_request_poll_count" in stats:
        print(
            "avg_server_request_poll_count="
            f"{stats['avg_server_request_poll_count']:.3f}"
        )
    if "avg_server_response_poll_count" in stats:
        print(
            "avg_server_response_poll_count="
            f"{stats['avg_server_response_poll_count']:.3f}"
        )
    if stats["benchmark_rc"] is not None:
        print(f"benchmark_rc={stats['benchmark_rc']}")
    if stats["guest_command_rc"] is not None:
        print(f"guest_command_rc={stats['guest_command_rc']}")


def print_steady_text_summary(steady_stats):
    print(
        "drop_first_requests_per_client="
        f"{steady_stats['drop_first_requests_per_client']}"
    )
    print(f"dropped_requests_total={steady_stats['dropped_requests_total']}")
    print(f"steady_requests={steady_stats['steady_requests']}")
    print(f"first_kept_start_ns={steady_stats['first_kept_start_ns']}")
    print(f"first_kept_end_ns={steady_stats['first_kept_end_ns']}")
    print(f"last_kept_end_ns={steady_stats['last_kept_end_ns']}")
    print(f"steady_total_e2e_ns={steady_stats['steady_total_e2e_ns']}")
    if steady_stats["steady_avg_latency_ns"] is not None:
        print(
            "steady_avg_latency_ns="
            f"{steady_stats['steady_avg_latency_ns']:.3f}"
        )
    else:
        print("steady_avg_latency_ns=None")
    if steady_stats["steady_median_latency_ns"] is not None:
        print(
            "steady_median_latency_ns="
            f"{steady_stats['steady_median_latency_ns']:.3f}"
        )
    else:
        print("steady_median_latency_ns=None")
    if steady_stats["steady_avg_reqresp_latency_ns"] is not None:
        print(
            "steady_avg_reqresp_latency_ns="
            f"{steady_stats['steady_avg_reqresp_latency_ns']:.3f}"
        )
    else:
        print("steady_avg_reqresp_latency_ns=None")
    if steady_stats["steady_avg_gap_ns"] is not None:
        print(f"steady_avg_gap_ns={steady_stats['steady_avg_gap_ns']:.3f}")
    else:
        print("steady_avg_gap_ns=None")
    if steady_stats["steady_median_gap_ns"] is not None:
        print(
            "steady_median_gap_ns="
            f"{steady_stats['steady_median_gap_ns']:.3f}"
        )
    else:
        print("steady_median_gap_ns=None")
    if steady_stats["steady_avg_throughput_mrps"] is not None:
        print(
            "steady_avg_throughput_mrps="
            f"{steady_stats['steady_avg_throughput_mrps']:.6f}"
        )
    else:
        print("steady_avg_throughput_mrps=None")
    if steady_stats["steady_avg_gap_throughput_mrps"] is not None:
        print(
            "steady_avg_gap_throughput_mrps="
            f"{steady_stats['steady_avg_gap_throughput_mrps']:.6f}"
        )
    else:
        print("steady_avg_gap_throughput_mrps=None")
    if steady_stats["steady_median_throughput_mrps"] is not None:
        print(
            "steady_median_throughput_mrps="
            f"{steady_stats['steady_median_throughput_mrps']:.6f}"
        )
    else:
        print("steady_median_throughput_mrps=None")
    if "steady_avg_req_publish_to_server_observe_ns" in steady_stats:
        print(
            "steady_avg_req_publish_to_server_observe_ns="
            f"{steady_stats['steady_avg_req_publish_to_server_observe_ns']:.3f}"
        )
    if "steady_avg_server_observe_to_resp_publish_ns" in steady_stats:
        print(
            "steady_avg_server_observe_to_resp_publish_ns="
            f"{steady_stats['steady_avg_server_observe_to_resp_publish_ns']:.3f}"
        )
    if "steady_avg_resp_publish_to_client_observe_ns" in steady_stats:
        print(
            "steady_avg_resp_publish_to_client_observe_ns="
            f"{steady_stats['steady_avg_resp_publish_to_client_observe_ns']:.3f}"
        )
    if "steady_avg_req_publish_to_client_observe_ns" in steady_stats:
        print(
            "steady_avg_req_publish_to_client_observe_ns="
            f"{steady_stats['steady_avg_req_publish_to_client_observe_ns']:.3f}"
        )
    if "steady_avg_client_request_poll_count" in steady_stats:
        print(
            "steady_avg_client_request_poll_count="
            f"{steady_stats['steady_avg_client_request_poll_count']:.3f}"
        )
    if "steady_avg_client_response_poll_count" in steady_stats:
        print(
            "steady_avg_client_response_poll_count="
            f"{steady_stats['steady_avg_client_response_poll_count']:.3f}"
        )
    if "steady_avg_server_request_poll_count" in steady_stats:
        print(
            "steady_avg_server_request_poll_count="
            f"{steady_stats['steady_avg_server_request_poll_count']:.3f}"
        )
    if "steady_avg_server_response_poll_count" in steady_stats:
        print(
            "steady_avg_server_response_poll_count="
            f"{steady_stats['steady_avg_server_response_poll_count']:.3f}"
        )


def validate_stats(stats, expected_total: int):
    if stats["benchmark_rc"] not in (None, 0):
        raise ValueError(f"benchmark exited non-zero: rc={stats['benchmark_rc']}")
    if stats["guest_command_rc"] not in (None, 0):
        raise ValueError(
            f"guest command exited non-zero: rc={stats['guest_command_rc']}"
        )
    if stats["total_requests"] != expected_total:
        raise ValueError(
            "incomplete timing records: "
            f"got_total={stats['total_requests']} expected_total={expected_total}"
        )
    if stats["correctness_fail_count"] != 0:
        raise ValueError(
            "correctness failures present: "
            f"count={stats['correctness_fail_count']}"
        )


def append_csv(csv_path: pathlib.Path, row: dict):
    fieldnames = [
        "experiment",
        "client_count",
        "count_per_client",
        "total_requests",
        "first_start_ns",
        "last_end_ns",
        "total_e2e_ns",
        "avg_latency_ns",
        "median_latency_ns",
        "aggregate_throughput_mrps",
        "correctness_fail_count",
        "outdir",
        "log_path",
    ]
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def append_steady_csv(csv_path: pathlib.Path, row: dict):
    fieldnames = [
        "experiment",
        "client_count",
        "count_per_client",
        "drop_first_requests_per_client",
        "dropped_requests_total",
        "steady_requests",
        "first_kept_start_ns",
        "steady_total_e2e_ns",
        "steady_avg_latency_ns",
        "steady_median_latency_ns",
        "steady_avg_reqresp_latency_ns",
        "steady_avg_gap_ns",
        "steady_median_gap_ns",
        "steady_avg_throughput_mrps",
        "steady_avg_gap_throughput_mrps",
        "steady_median_throughput_mrps",
        "first_kept_end_ns",
        "last_kept_end_ns",
        "outdir",
        "log_path",
    ]
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def write_fail_csv(csv_path: pathlib.Path, experiment: str, outdir: pathlib.Path, stats):
    fieldnames = ["experiment", "client_id", "req_id", "expected", "actual", "outdir"]
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for fail_row in stats["fail_rows"]:
            writer.writerow(
                {
                    "experiment": experiment,
                    "client_id": fail_row["client_id"],
                    "req_id": fail_row["req_id"],
                    "expected": fail_row["expected"],
                    "actual": fail_row["actual"],
                    "outdir": str(outdir),
                }
            )


def write_result_json(result_path: pathlib.Path, payload: dict):
    result_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True, help="Path to board.pc.com_1.device")
    parser.add_argument("--experiment", required=True, help="Experiment label")
    parser.add_argument("--client-count", required=True, type=int)
    parser.add_argument("--count-per-client", required=True, type=int)
    parser.add_argument(
        "--expected-total-requests",
        type=int,
        help="Override the expected number of complete request rows",
    )
    parser.add_argument("--csv", help="Append summary row to CSV")
    parser.add_argument(
        "--steady-csv",
        help="Append steady-state row to CSV using drop-first-per-client trimming",
    )
    parser.add_argument("--fail-csv", help="Append failure rows to CSV")
    parser.add_argument("--outdir", help="Outdir recorded in CSV")
    parser.add_argument("--result-json", help="Write per-run result JSON")
    parser.add_argument(
        "--drop-first-per-client",
        type=int,
        default=0,
        help="Drop the first N requests from each client for steady-state metrics",
    )
    args = parser.parse_args()

    log_path = pathlib.Path(args.log)
    outdir = pathlib.Path(args.outdir) if args.outdir else log_path.parent
    expected_total = (
        args.expected_total_requests
        if args.expected_total_requests is not None
        else args.client_count * args.count_per_client
    )

    stats = None
    steady_stats = None
    try:
        stats, request_rows, breakdown_by_request = parse_log(log_path)
        if args.fail_csv and stats["fail_rows"]:
            write_fail_csv(
                pathlib.Path(args.fail_csv), args.experiment, outdir, stats
            )

        validate_stats(stats, expected_total)
        print_text_summary(
            args.experiment, args.client_count, args.count_per_client, stats
        )
        steady_stats = compute_steady_stats(
            request_rows, args.drop_first_per_client
        )
        steady_rows, _ = select_steady_rows(request_rows, args.drop_first_per_client)
        steady_stats.update(
            prefix_stats(
                "steady_",
                compute_breakdown_stats(steady_rows, breakdown_by_request),
            )
        )
        print_steady_text_summary(steady_stats)

        if args.csv:
            append_csv(
                pathlib.Path(args.csv),
                {
                    "experiment": args.experiment,
                    "client_count": args.client_count,
                    "count_per_client": args.count_per_client,
                    "total_requests": stats["total_requests"],
                    "first_start_ns": stats["first_start_ns"],
                    "last_end_ns": stats["last_end_ns"],
                    "total_e2e_ns": stats["total_e2e_ns"],
                    "avg_latency_ns": f"{stats['avg_latency_ns']:.3f}",
                    "median_latency_ns": f"{stats['median_latency_ns']:.3f}",
                    "aggregate_throughput_mrps": (
                        f"{stats['aggregate_throughput_mrps']:.6f}"
                    ),
                    "correctness_fail_count": stats["correctness_fail_count"],
                    "outdir": str(outdir),
                    "log_path": str(log_path),
                },
            )

        if args.steady_csv:
            append_steady_csv(
                pathlib.Path(args.steady_csv),
                {
                    "experiment": args.experiment,
                    "client_count": args.client_count,
                    "count_per_client": args.count_per_client,
                    "drop_first_requests_per_client": (
                        steady_stats["drop_first_requests_per_client"]
                    ),
                    "dropped_requests_total": (
                        steady_stats["dropped_requests_total"]
                    ),
                    "steady_requests": steady_stats["steady_requests"],
                    "first_kept_start_ns": steady_stats["first_kept_start_ns"],
                    "steady_total_e2e_ns": steady_stats["steady_total_e2e_ns"],
                    "steady_avg_latency_ns": (
                        f"{steady_stats['steady_avg_latency_ns']:.3f}"
                        if steady_stats["steady_avg_latency_ns"] is not None
                        else ""
                    ),
                    "steady_median_latency_ns": (
                        f"{steady_stats['steady_median_latency_ns']:.3f}"
                        if steady_stats["steady_median_latency_ns"] is not None
                        else ""
                    ),
                    "steady_avg_reqresp_latency_ns": (
                        f"{steady_stats['steady_avg_reqresp_latency_ns']:.3f}"
                        if steady_stats["steady_avg_reqresp_latency_ns"] is not None
                        else ""
                    ),
                    "steady_avg_gap_ns": (
                        f"{steady_stats['steady_avg_gap_ns']:.3f}"
                        if steady_stats["steady_avg_gap_ns"] is not None
                        else ""
                    ),
                    "steady_median_gap_ns": (
                        f"{steady_stats['steady_median_gap_ns']:.3f}"
                        if steady_stats["steady_median_gap_ns"] is not None
                        else ""
                    ),
                    "steady_avg_throughput_mrps": (
                        f"{steady_stats['steady_avg_throughput_mrps']:.6f}"
                        if steady_stats["steady_avg_throughput_mrps"] is not None
                        else ""
                    ),
                    "steady_avg_gap_throughput_mrps": (
                        f"{steady_stats['steady_avg_gap_throughput_mrps']:.6f}"
                        if steady_stats["steady_avg_gap_throughput_mrps"] is not None
                        else ""
                    ),
                    "steady_median_throughput_mrps": (
                        f"{steady_stats['steady_median_throughput_mrps']:.6f}"
                        if steady_stats["steady_median_throughput_mrps"] is not None
                        else ""
                    ),
                    "first_kept_end_ns": steady_stats["first_kept_end_ns"],
                    "last_kept_end_ns": steady_stats["last_kept_end_ns"],
                    "outdir": str(outdir),
                    "log_path": str(log_path),
                },
            )

        if args.result_json:
            write_result_json(
                pathlib.Path(args.result_json),
                {
                    "status": "success",
                    "experiment": args.experiment,
                    "client_count": args.client_count,
                    "count_per_client": args.count_per_client,
                    "expected_total_requests": expected_total,
                    "outdir": str(outdir),
                    "log_path": str(log_path),
                    "stats": stats,
                    "steady_stats": steady_stats,
                },
            )
        return 0
    except Exception as exc:
        if args.result_json:
            payload = {
                "status": "error",
                "experiment": args.experiment,
                "client_count": args.client_count,
                "count_per_client": args.count_per_client,
                "expected_total_requests": expected_total,
                "outdir": str(outdir),
                "log_path": str(log_path),
                "error": str(exc),
            }
            if stats is not None:
                payload["stats"] = stats
            if steady_stats is not None:
                payload["steady_stats"] = steady_stats
            write_result_json(pathlib.Path(args.result_json), payload)
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
