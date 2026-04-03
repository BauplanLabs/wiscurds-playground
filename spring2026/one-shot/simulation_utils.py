"""Simulation utilities for one-shot scheduler evaluation."""
from __future__ import annotations

import signal
from typing import Any

_last_failure: dict | None = None


def get_last_simulation_failure() -> dict | None:
    return _last_failure


def deregister_scheduler(key: str) -> None:
    """Remove a scheduler key from the global registry so it can be re-exec'd."""
    from eudoxia.scheduler.decorators import INIT_ALGOS, SCHEDULING_ALGOS
    INIT_ALGOS.pop(key, None)
    SCHEDULING_ALGOS.pop(key, None)


def run_simulation_once(
    base_params: dict,
    trace_file: str,
    scheduler_key: str,
    timeout_seconds: int = 60,
) -> Any | None:
    """Run the simulator with one trace file. Returns SimulatorStats on success, None on failure."""
    from eudoxia.simulator import run_simulator
    from eudoxia.workload.csv_io import CSVWorkloadReader

    global _last_failure
    _last_failure = None

    params = base_params.copy()
    params["scheduler_algo"] = scheduler_key

    def _timeout_handler(signum, frame):
        raise TimeoutError(f"Simulation timed out after {timeout_seconds}s")

    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout_seconds)
    f = None
    try:
        f = open(trace_file)
        reader = CSVWorkloadReader(f)
        workload = reader.get_workload(params["ticks_per_second"])
        stats = run_simulator(params, workload=workload)
        signal.alarm(0)
        return stats
    except TimeoutError:
        _last_failure = {
            "reason": "timeout",
            "trace_file": trace_file,
            "timeout_seconds": timeout_seconds,
        }
        return None
    except Exception as exc:
        _last_failure = {
            "reason": "exception",
            "trace_file": trace_file,
            "detail": str(exc),
        }
        return None
    finally:
        signal.signal(signal.SIGALRM, old_handler)
        signal.alarm(0)
        if f is not None:
            f.close()


def get_raw_stats_for_policy(
    base_params: dict,
    trace_files: list[str],
    scheduler_key: str,
    timeout_seconds: int = 60,
) -> list[Any]:
    """Run the simulation across all trace files. Returns partial list on failure."""
    results = []
    for tf in trace_files:
        stats = run_simulation_once(base_params, tf, scheduler_key, timeout_seconds)
        if stats is None:
            return results
        results.append(stats)
    return results


def extract_metrics_from_stats(stats_list: list[Any], metric: str) -> list[float]:
    """Extract a scalar metric from a list of SimulatorStats objects.

    Supported metrics: "latency", "throughput", "oom_rate".
    """
    values = []
    for s in stats_list:
        if metric == "latency":
            values.append(s.adjusted_latency())
        elif metric == "throughput":
            values.append(s.throughput)
        elif metric == "oom_rate":
            total_ops = s.assignments + s.failures
            values.append(s.failures / total_ops if total_ops else 0.0)
        else:
            raise ValueError(f"Unknown metric: {metric!r}")
    return values
