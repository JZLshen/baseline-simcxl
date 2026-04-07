#!/usr/bin/env python3

import json
from pathlib import Path
from typing import Dict


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


def fmt_metric(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def blank_metrics() -> Dict[str, str]:
    return {field: "" for field in METRIC_FIELDS}


def metrics_from_drop1_stats(stats: dict) -> Dict[str, str]:
    return {
        "throughput_kops": fmt_metric(stats.get("steady_avg_throughput_kops")),
        "latency_median_ns": fmt_metric(stats.get("steady_median_latency_ns")),
        "latency_p90_ns": fmt_metric(stats.get("steady_p90_latency_ns")),
        "latency_p99_ns": fmt_metric(stats.get("steady_p99_latency_ns")),
        "server_poll_median_ns": fmt_metric(
            stats.get("steady_median_server_poll_notify_ns")
        ),
        "server_poll_p90_ns": fmt_metric(stats.get("steady_p90_server_poll_notify_ns")),
        "server_poll_p99_ns": fmt_metric(stats.get("steady_p99_server_poll_notify_ns")),
        "server_execute_median_ns": fmt_metric(
            stats.get("steady_median_server_execute_ns")
        ),
        "server_execute_p90_ns": fmt_metric(stats.get("steady_p90_server_execute_ns")),
        "server_execute_p99_ns": fmt_metric(stats.get("steady_p99_server_execute_ns")),
        "server_response_median_ns": fmt_metric(
            stats.get("steady_median_server_response_ns")
        ),
        "server_response_p90_ns": fmt_metric(stats.get("steady_p90_server_response_ns")),
        "server_response_p99_ns": fmt_metric(stats.get("steady_p99_server_response_ns")),
    }


def load_drop1_metrics(result_json_path: Path) -> Dict[str, str]:
    payload = json.loads(Path(result_json_path).read_text())
    stats = payload.get("steady_drop1_stats")
    if not stats:
        raise ValueError(f"missing steady_drop1_stats in {result_json_path}")
    return metrics_from_drop1_stats(stats)
