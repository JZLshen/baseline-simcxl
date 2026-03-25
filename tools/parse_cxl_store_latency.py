#!/usr/bin/env python3
"""Parse CXL store latency from gem5 debug trace."""

import argparse
import math
import re
import statistics
from collections import defaultdict, deque
from pathlib import Path


REQ_RE = re.compile(
    r"^\s*(\d+):\s+([^:]+):\s+recvTimingReq:\s+(\S+)\s+addr\s+0x([0-9a-fA-F]+)"
)
RSP_RE = re.compile(
    r"^\s*(\d+):\s+([^:]+):\s+recvTimingResp:\s+(\S+)\s+addr\s+0x([0-9a-fA-F]+)"
)


def percentile_nearest_rank(sorted_values, percentile: float):
    if not sorted_values:
        raise ValueError("percentile_nearest_rank requires non-empty input")
    rank = max(1, math.ceil((percentile / 100.0) * len(sorted_values)))
    return sorted_values[rank - 1]


def ns_to_tick_floor(ns: int, simfreq: float) -> int:
    return int((ns * simfreq) / 1e9)


def ns_bucket_end_to_tick(ns: int, simfreq: float) -> int:
    return int(math.ceil(((ns + 1) * simfreq) / 1e9)) - 1


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract store req/resp latency from CXL trace"
    )
    parser.add_argument("--trace", required=True, help="Path to debug trace file")
    parser.add_argument("--stats", required=True, help="Path to stats.txt")
    parser.add_argument(
        "--component-substr",
        default="bridge",
        help="Only match lines whose component name contains this string",
    )
    parser.add_argument(
        "--addr-start",
        type=lambda x: int(x, 0),
        default=0x100000000,
        help="Only match addresses >= this value (default: CXL range start)",
    )
    parser.add_argument(
        "--addr-exact",
        type=lambda x: int(x, 0),
        default=None,
        help="Only match this exact address. Overrides --addr-start when set.",
    )
    parser.add_argument(
        "--cmd-filter",
        default="Write",
        help="Only match req/resp lines whose cmd contains this substring",
    )
    parser.add_argument(
        "--time-start-ns",
        type=int,
        default=None,
        help="Only match requests at or after this simulated time (ns).",
    )
    parser.add_argument(
        "--time-end-ns",
        type=int,
        default=None,
        help="Only match requests at or before this simulated time (ns).",
    )
    parser.add_argument(
        "--strict",
        dest="strict",
        action="store_true",
        help="Fail when no matched req/resp pairs are found (default: true)",
    )
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Do not fail when no pairs are found",
    )
    parser.set_defaults(strict=True)
    parser.add_argument(
        "--expect-pairs",
        type=int,
        default=None,
        help="Fail unless exactly this many req/resp pairs are matched.",
    )
    parser.add_argument(
        "--require-clean-window",
        action="store_true",
        help="Fail if any orphan response or unmatched request remains.",
    )
    parser.add_argument(
        "--output-kv",
        default=None,
        help="Optional output path for key=value metrics",
    )
    return parser.parse_args()


def read_simfreq(stats_path: Path) -> float:
    with stats_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if line.startswith("simFreq"):
                parts = line.split()
                if len(parts) >= 2:
                    return float(parts[1])
    raise RuntimeError(f"Could not find simFreq in {stats_path}")


def main():
    args = parse_args()

    if (args.time_start_ns is None) != (args.time_end_ns is None):
        raise RuntimeError(
            "--time-start-ns and --time-end-ns must be provided together"
        )

    trace_path = Path(args.trace)
    stats_path = Path(args.stats)
    comp_filter = args.component_substr.lower()
    cmd_filter = args.cmd_filter
    simfreq = None
    start_tick = None
    end_tick = None

    if args.time_start_ns is not None:
        simfreq = read_simfreq(stats_path)
        start_tick = ns_to_tick_floor(args.time_start_ns, simfreq)
        end_tick = ns_bucket_end_to_tick(args.time_end_ns, simfreq)
        if end_tick < start_tick:
            raise RuntimeError("time window end precedes start")

    req_queues = defaultdict(deque)
    lat_ticks = []
    orphan_resp_count = 0

    with trace_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            m = REQ_RE.match(line)
            if m:
                tick = int(m.group(1))
                comp = m.group(2).lower()
                cmd = m.group(3)
                addr = int(m.group(4), 16)
                if comp_filter not in comp:
                    continue
                if start_tick is not None and (
                    tick < start_tick or tick > end_tick
                ):
                    continue
                if args.addr_exact is not None:
                    if addr != args.addr_exact:
                        continue
                elif addr < args.addr_start:
                    continue
                if cmd_filter and cmd_filter not in cmd:
                    continue
                req_queues[addr].append(tick)
                continue

            m = RSP_RE.match(line)
            if not m:
                continue
            tick = int(m.group(1))
            comp = m.group(2).lower()
            cmd = m.group(3)
            addr = int(m.group(4), 16)
            if comp_filter not in comp:
                continue
            if start_tick is not None and tick < start_tick:
                continue
            if args.addr_exact is not None:
                if addr != args.addr_exact:
                    continue
            elif addr < args.addr_start:
                continue
            if cmd_filter and cmd_filter not in cmd:
                continue
            if not req_queues[addr]:
                orphan_resp_count += 1
                continue
            req_tick = req_queues[addr].popleft()
            lat_ticks.append(tick - req_tick)

    unmatched_req_count = sum(len(queue) for queue in req_queues.values())

    if not lat_ticks and args.strict:
        raise RuntimeError(
            "No store req/resp pair matched. "
            "Check debug flags, workload, cmd-filter, address filter, "
            "and component filter."
        )
    if args.expect_pairs is not None and len(lat_ticks) != args.expect_pairs:
        raise RuntimeError(
            f"Matched pair count {len(lat_ticks)} != expected {args.expect_pairs}"
        )
    if args.require_clean_window and (
        unmatched_req_count != 0 or orphan_resp_count != 0
    ):
        raise RuntimeError(
            "Trace window was not clean: "
            f"unmatched_req_count={unmatched_req_count}, "
            f"orphan_resp_count={orphan_resp_count}"
        )

    metrics = {
        "matched_pairs": len(lat_ticks),
        "unmatched_req_count": unmatched_req_count,
        "orphan_resp_count": orphan_resp_count,
    }
    if start_tick is not None:
        metrics["window_start_tick"] = start_tick
        metrics["window_end_tick"] = end_tick

    if not lat_ticks:
        print(f"matched_pairs={metrics['matched_pairs']}")
        print(f"unmatched_req_count={metrics['unmatched_req_count']}")
        print(f"orphan_resp_count={metrics['orphan_resp_count']}")
    else:
        if simfreq is None:
            simfreq = read_simfreq(stats_path)
        tick_to_ns = 1e9 / simfreq
        lats_ns = [tick * tick_to_ns for tick in lat_ticks]

        sorted_lats = sorted(lats_ns)
        p50 = statistics.median(sorted_lats)
        p95 = percentile_nearest_rank(sorted_lats, 95)

        metrics.update(
            {
                "first_store_latency_ticks": lat_ticks[0],
                "first_store_latency_ns": f"{lats_ns[0]:.3f}",
                "min_latency_ns": f"{min(lats_ns):.3f}",
                "p50_latency_ns": f"{p50:.3f}",
                "p95_latency_ns": f"{p95:.3f}",
                "max_latency_ns": f"{max(lats_ns):.3f}",
            }
        )

        print(f"matched_pairs={metrics['matched_pairs']}")
        print(f"unmatched_req_count={metrics['unmatched_req_count']}")
        print(f"orphan_resp_count={metrics['orphan_resp_count']}")
        if start_tick is not None:
            print(f"window_start_tick={start_tick}")
            print(f"window_end_tick={end_tick}")
        print(f"first_store_latency_ticks={metrics['first_store_latency_ticks']}")
        print(f"first_store_latency_ns={metrics['first_store_latency_ns']}")
        print(f"min_latency_ns={metrics['min_latency_ns']}")
        print(f"p50_latency_ns={metrics['p50_latency_ns']}")
        print(f"p95_latency_ns={metrics['p95_latency_ns']}")
        print(f"max_latency_ns={metrics['max_latency_ns']}")

    if args.output_kv:
        output_path = Path(args.output_kv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as fh:
            for key, value in metrics.items():
                fh.write(f"{key}={value}\n")


if __name__ == "__main__":
    main()
