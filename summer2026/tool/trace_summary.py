"""Summarize a Eudoxia workload CSV into a markdown block for prompt injection.

Importable: `from tool.trace_summary import summarize_trace`
CLI:        `uv run python tool/trace_summary.py <trace.csv>`

Fairness boundary: only a-priori-knowable workload structure is reported
(volume, arrival timing, priority mix, DAG size). Per-op resource curves
(memory_gb / storage_read_gb / baseline_cpu_seconds / cpu_scaling) are
deliberately omitted — the scheduler must infer them from runtime signals.

Schema produced (kept compact so it fits comfortably in a system prompt):

    ## Workload at hand: <trace_name>

    - Pipelines: <N> over <T>s
    - Priority mix: <counts>
    - Inter-arrival: mean <X>s, median <Y>s
    - Operators per pipeline: median <M>, p90 <P>, max <K>
"""
from __future__ import annotations

import argparse
import csv
import statistics
from collections import Counter
from pathlib import Path


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    return s[min(int(len(s) * p), len(s) - 1)]


def summarize_trace(trace_path: Path) -> str:
    rows = list(csv.DictReader(open(trace_path, encoding="utf-8")))
    if not rows:
        return f"## Workload at hand: `{trace_path.name}` (empty trace)"

    pipelines: dict[str, dict] = {}
    for r in rows:
        pid = r["pipeline_id"]
        if pid not in pipelines:
            pipelines[pid] = {
                "arrival": float(r["arrival_seconds"]) if r["arrival_seconds"] else 0.0,
                "priority": r["priority"] or "UNKNOWN",
                "n_ops": 0,
            }
        pipelines[pid]["n_ops"] += 1

    n_pipelines = len(pipelines)
    arrivals = sorted(p["arrival"] for p in pipelines.values())
    duration = arrivals[-1] - arrivals[0] if len(arrivals) > 1 else 0.0
    inter = [arrivals[i + 1] - arrivals[i] for i in range(len(arrivals) - 1)] or [0.0]
    prio_counter = Counter(p["priority"] for p in pipelines.values())
    ops_per = [p["n_ops"] for p in pipelines.values()]

    def _prio_line() -> str:
        order = ["QUERY", "INTERACTIVE", "BATCH_PIPELINE"]
        parts = []
        for k in order:
            if k in prio_counter:
                pct = 100.0 * prio_counter[k] / n_pipelines
                parts.append(f"{prio_counter[k]} {k} ({pct:.0f}%)")
        for k, v in prio_counter.items():
            if k not in order:
                parts.append(f"{v} {k}")
        return " / ".join(parts)

    lines = [
        f"## Workload at hand: `{trace_path.name}`",
        "",
        f"- **Pipelines:** {n_pipelines:,} over {duration:.0f}s window",
        f"- **Priority mix:** {_prio_line()}",
        f"- **Inter-arrival:** mean {statistics.mean(inter):.2f}s, median {statistics.median(inter):.2f}s",
        f"- **Operators per pipeline:** median {int(statistics.median(ops_per))}, "
        f"p90 {_pct(ops_per, 0.9)}, max {max(ops_per)}",
    ]

    # Fairness boundary: only a-priori-knowable workload structure is reported
    # (volume, arrival timing, priority mix, DAG size). Per-op resource curves
    # — memory_gb, storage_read_gb (== true peak RAM in this trace since
    # memory_gb is None), baseline_cpu_seconds, cpu_scaling — are deliberately
    # NOT surfaced. The Eudoxia context states the scheduler must infer the
    # resource curve from runtime signals (OOMs, observed runtimes); exposing
    # it here would be an oracle leak.

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    print(summarize_trace(args.trace))


if __name__ == "__main__":
    main()
