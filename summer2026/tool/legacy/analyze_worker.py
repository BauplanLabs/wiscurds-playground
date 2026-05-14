#!/usr/bin/env python3
"""Subprocess worker for scheduler latency analysis.

This module executes generated scheduler code in a short-lived process so the
main analyzer never imports untrusted scheduler modules in-process.
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path

SUMMER2026_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(SUMMER2026_DIR))

from simulation_utils import extract_metrics_from_stats, get_raw_stats_for_policy


def evaluate_scheduler(
    scheduler_file: str,
    trace_files: list[str],
    baseline_median: float,
    metric: str,
    base_params: dict,
) -> dict:
    src = Path(scheduler_file).read_text(encoding="utf-8")
    key_match = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", src)
    if not key_match:
        return {"functional": False, "failure_mode": "no_scheduler_key"}

    try:
        from typing import List, Tuple

        from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
        from eudoxia.scheduler.decorators import register_scheduler, register_scheduler_init
        from eudoxia.utils import Priority
        from eudoxia.workload import OperatorState, Pipeline
        from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

        worker_globals = {
            "__builtins__": __builtins__,
            "__name__": "__worker__",
            "List": List,
            "Tuple": Tuple,
            "Pipeline": Pipeline,
            "OperatorState": OperatorState,
            "ASSIGNABLE_STATES": ASSIGNABLE_STATES,
            "Assignment": Assignment,
            "ExecutionResult": ExecutionResult,
            "Suspend": Suspend,
            "register_scheduler_init": register_scheduler_init,
            "register_scheduler": register_scheduler,
            "Priority": Priority,
        }
        exec(src, worker_globals)
    except Exception as exc:
        return {
            "functional": False,
            "failure_mode": "exec_error",
            "error_message": str(exc),
        }

    try:
        params = dict(base_params)
        cluster_sizes = params.pop("_cluster_sizes", None)
        policy_key = key_match.group(1)

        if cluster_sizes is None:
            raw = get_raw_stats_for_policy(params, trace_files, policy_key)
            expected = len(trace_files)
        else:
            raw = []
            for n_pools in cluster_sizes:
                scale_params = params.copy()
                scale_params["num_pools"] = n_pools
                raw.extend(get_raw_stats_for_policy(scale_params, trace_files, policy_key))
            expected = len(cluster_sizes) * len(trace_files)

        if len(raw) != expected:
            return {
                "functional": False,
                "failure_mode": "simulation_error",
                "error_message": f"Got {len(raw)}/{expected} results",
            }

        values = [float(value) for value in extract_metrics_from_stats(raw, metric, base_params=params)]
        median = float(statistics.median(values))
        beats_baseline = bool(median < baseline_median) if metric == "latency" else bool(median > baseline_median)
        improvement_pct = float((median - baseline_median) / baseline_median * 100) if baseline_median else None

        simulator_stats = aggregate_simulator_stats(raw)
        pipeline_stats = {}
        for attr in ["pipelines_all", "pipelines_query", "pipelines_interactive", "pipelines_batch"]:
            pipeline_stats.update(aggregate_pipeline_stats(raw, attr))

        oom_count = simulator_stats["failure_error_counts"].get("OOM", 0)
        total_arrivals = pipeline_stats["pipelines_all_arrival_count"]
        total_completions = pipeline_stats["pipelines_all_completion_count"]
        total_assignments = simulator_stats["assignments"]
        total_suspensions = simulator_stats["suspensions"]

        return {
            "functional": True,
            "failure_mode": "success",
            f"median_{metric}": median,
            "metric_values": values,
            "beats_baseline": beats_baseline,
            "improvement_pct": improvement_pct,
            **simulator_stats,
            **pipeline_stats,
            "oom_count": oom_count,
            "suspension_rate": total_suspensions / total_assignments if total_assignments else None,
            "completion_rate": total_completions / total_arrivals if total_arrivals else None,
            "completion_rate_query": completion_rate(pipeline_stats, "pipelines_query"),
            "completion_rate_interactive": completion_rate(pipeline_stats, "pipelines_interactive"),
            "completion_rate_batch": completion_rate(pipeline_stats, "pipelines_batch"),
        }
    except Exception as exc:
        return {
            "functional": False,
            "failure_mode": "simulation_error",
            "error_message": str(exc),
        }


def aggregate_simulator_stats(raw: list) -> dict:
    failure_error_counts: dict = {}
    for stats in raw:
        for key, value in stats.failure_error_counts.items():
            failure_error_counts[key] = failure_error_counts.get(key, 0) + value

    return {
        "pipelines_created": sum(stats.pipelines_created for stats in raw),
        "containers_completed": sum(stats.containers_completed for stats in raw),
        "throughput": statistics.mean(stats.throughput for stats in raw),
        "p99_latency": statistics.mean(stats.p99_latency for stats in raw),
        "assignments": sum(stats.assignments for stats in raw),
        "suspensions": sum(stats.suspensions for stats in raw),
        "failures": sum(stats.failures for stats in raw),
        "failure_error_counts": failure_error_counts,
        "mean_memory_allocated_percent": statistics.mean(stats.mean_memory_allocated_percent for stats in raw),
        "mean_memory_consumed_percent": statistics.mean(stats.mean_memory_consumed_percent for stats in raw),
    }


def aggregate_pipeline_stats(raw: list, attr: str) -> dict:
    arrivals = sum(getattr(stats, attr).arrival_count for stats in raw)
    completions = sum(getattr(stats, attr).completion_count for stats in raw)
    timeouts = sum(getattr(stats, attr).timeout_count for stats in raw)
    weighted_latency = sum(
        getattr(stats, attr).mean_latency_seconds * getattr(stats, attr).completion_count
        for stats in raw
        if getattr(stats, attr).completion_count > 0
    )
    completed_count = sum(getattr(stats, attr).completion_count for stats in raw)
    p99_values = [
        getattr(stats, attr).p99_latency_seconds
        for stats in raw
        if getattr(stats, attr).completion_count > 0
    ]
    return {
        f"{attr}_arrival_count": arrivals,
        f"{attr}_completion_count": completions,
        f"{attr}_timeout_count": timeouts,
        f"{attr}_mean_latency_seconds": weighted_latency / completed_count if completed_count else None,
        f"{attr}_p99_latency_seconds": statistics.mean(p99_values) if p99_values else None,
    }


def completion_rate(pipeline_stats: dict, attr: str) -> float | None:
    arrivals = pipeline_stats[f"{attr}_arrival_count"]
    completions = pipeline_stats[f"{attr}_completion_count"]
    return completions / arrivals if arrivals else None


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print(json.dumps({"functional": False, "failure_mode": "invalid_worker_args"}))
        return 2

    scheduler_file, trace_files_raw, baseline_raw, metric, base_params_raw = argv
    try:
        trace_files = json.loads(trace_files_raw)
        baseline_median = float(baseline_raw)
        base_params = json.loads(base_params_raw)
    except Exception as exc:
        print(json.dumps({
            "functional": False,
            "failure_mode": "invalid_worker_args",
            "error_message": str(exc),
        }))
        return 2

    result = evaluate_scheduler(
        scheduler_file=scheduler_file,
        trace_files=trace_files,
        baseline_median=baseline_median,
        metric=metric,
        base_params=base_params,
    )
    print(f"__RESULT__{json.dumps(result)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
