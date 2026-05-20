"""Query functions for Step 2: LLM-driven context selection.

The LLM declares which queries it wants (Phase 1); the runner fetches them here;
Phase 2 injects the results into the feedback prompt before scheduler generation.

Fairness boundary: queries may expose
  - simulation observables (eval_out.json — completion rates, latencies, OOM counts,
    memory utilisation %) — the scheduler could observe these from production telemetry
  - a-priori workload structure (trace arrival timing, priority mix, DAG topology)
Queries must NOT expose per-operator resource curves (baseline_cpu_seconds,
cpu_scaling, memory_gb, storage_read_gb) — those are oracle info.
Queries also must NOT expose prompt-designer reasoning hints, scheduler API
tutorials, or policy recommendations. Those belong in the shared prompt, not
in the data-query surface.

CLI: uv run python tool/query.py [query_name] [--trace t.csv] [--eval eval_out.json] [--args k=v ...]
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

_REGISTRY: dict[str, dict] = {}
_PRIORITY_KEYS = {
    "query": "query",
    "interactive": "interactive",
    "batch": "batch",
    "QUERY": "query",
    "INTERACTIVE": "interactive",
    "BATCH": "batch",
    "BATCH_PIPELINE": "batch",
}
_TRACE_PRIORITY_ORDER = ["QUERY", "INTERACTIVE", "BATCH_PIPELINE"]


def _query(name: str, description: str, args_schema: dict):
    def decorator(fn):
        _REGISTRY[name] = {"fn": fn, "description": description, "args_schema": args_schema}
        return fn
    return decorator


def available_queries_text() -> str:
    lines = []
    for name, meta in _REGISTRY.items():
        schema = meta["args_schema"]
        if schema:
            args = ", ".join(
                f'{k} ∈ {{{", ".join(str(v) for v in vals)}}}'
                for k, vals in schema.items()
            )
            sig = f"{name}({args})"
        else:
            sig = f"{name}()"
        lines.append(f"- **{sig}** — {meta['description']}")
    return "\n".join(lines)


def run_query(name: str, args: dict, trace_path: Path, eval_data: dict) -> str:
    if name not in _REGISTRY:
        return f"*Unknown query '{name}' — skipped.*"
    try:
        return _REGISTRY[name]["fn"](trace_path=trace_path, eval_data=eval_data, **args)
    except Exception as exc:
        return f"*Query '{name}' error: {exc}*"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pct(count: int, total: int) -> str:
    return f"{100 * count / total:.0f}%" if total > 0 else "N/A"


def _f(v) -> str:
    return f"{v:.2f}" if isinstance(v, (int, float)) and v is not None else "N/A"


def _scale_results(eval_data: dict) -> dict:
    return eval_data.get("scale_results", eval_data)


def _entry(eval_data: dict, scale: int) -> dict | None:
    sr = _scale_results(eval_data)
    return sr.get(str(scale)) or sr.get(scale)


def _psub(entry: dict, key: str) -> dict:
    return entry.get(key, {})


def _priority_key(priority: str) -> str:
    key = _PRIORITY_KEYS.get(priority)
    if key is None:
        raise ValueError(f"unknown priority {priority!r}; use query, interactive, or batch")
    return key


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    return s[min(int(len(s) * p), len(s) - 1)]


def _read_workload_pipelines(trace_path: Path) -> dict[str, dict]:
    pipelines: dict[str, dict] = {}
    with open(trace_path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            pid = r["pipeline_id"]
            if pid not in pipelines:
                pipelines[pid] = {
                    "arrival": float(r.get("arrival_seconds") or 0),
                    "priority": r.get("priority") or "UNKNOWN",
                    "ops": {},
                    "n_ops": 0,
                }
            elif r.get("priority"):
                pipelines[pid]["priority"] = r["priority"]
            pipelines[pid]["n_ops"] += 1
            op_id = r.get("operator_id", "").strip()
            raw = r.get("parents", "").strip()
            parents = [p.strip() for p in raw.split(",") if p.strip()] if raw else []
            if op_id:
                pipelines[pid]["ops"][op_id] = parents
    return pipelines


# ---------------------------------------------------------------------------
# Scale status and score observables
# Fair: simulation observable status/latency/error only
# ---------------------------------------------------------------------------

@_query(
    "scale_status",
    "per-scale ok/fail status, adjusted latency, and failure error if the scale aborted",
    {},
)
def _scale_status(*, trace_path: Path, eval_data: dict) -> str:
    sr = _scale_results(eval_data)
    scales = sorted(int(k) for k in sr)
    if not scales:
        return "*No scale results in eval data.*"
    lines = ["**scale_status()**"]
    for scale in scales:
        e = _entry(eval_data, scale) or {}
        if e.get("ok"):
            lines.append(f"- {scale}x: OK, adjusted latency {_f(e.get('latency'))}s")
        else:
            lines.append(f"- {scale}x: FAILED, error: {e.get('error', 'unknown')}")
    return "\n".join(lines)


@_query(
    "priority_completion",
    "arrived/completed/completion-rate for one priority class at one scale",
    {"scale": [1, 2, 4, 8, 16], "priority": ["query", "interactive", "batch"]},
)
def _priority_completion(*, trace_path: Path, eval_data: dict, scale: int, priority: str) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    key = _priority_key(priority)
    ps = _psub(e, key)
    if not ps:
        return f"**priority_completion({scale}x, {key})**: no data"
    arrived, completed = ps.get("arrived", 0), ps.get("completed", 0)
    return (
        f"**priority_completion({scale}x, {key})**\n"
        f"- Arrived: {arrived}\n"
        f"- Completed: {completed}\n"
        f"- Completion rate: {_pct(completed, arrived)}"
    )


@_query(
    "priority_latency",
    "mean and p99 latency for one priority class at one scale",
    {"scale": [1, 2, 4, 8, 16], "priority": ["query", "interactive", "batch"]},
)
def _priority_latency(*, trace_path: Path, eval_data: dict, scale: int, priority: str) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    key = _priority_key(priority)
    ps = _psub(e, key)
    if not ps:
        return f"**priority_latency({scale}x, {key})**: no data"
    return (
        f"**priority_latency({scale}x, {key})**\n"
        f"- Mean latency: {_f(ps.get('mean_s'))}s\n"
        f"- P99 latency: {_f(ps.get('p99_s'))}s"
    )


# ---------------------------------------------------------------------------
# Query 1: priority_breakdown — maps directly to the priority-weighted score
# Fair: simulation observable (arrived/completed counts, latency percentiles)
# ---------------------------------------------------------------------------

@_query(
    "priority_breakdown",
    "per-priority arrived/completed/mean_s/p99 at one scale — maps directly to score (QUERY x10, INTERACTIVE x5, BATCH x1)",
    {"scale": [1, 2, 4, 8, 16]},
)
def _priority_breakdown(*, trace_path: Path, eval_data: dict, scale: int) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    if not e.get("ok"):
        return (
            f"**priority_breakdown({scale}x)** — scale FAILED\n"
            f"- Error: {e.get('error', 'unknown')}"
        )
    lines = [f"**priority_breakdown({scale}x)** — score {e.get('latency', '?'):.3f}s"]
    for key, label, weight in [
        ("query", "QUERY", 10),
        ("interactive", "INTERACTIVE", 5),
        ("batch", "BATCH", 1),
    ]:
        ps = _psub(e, key)
        if not ps:
            lines.append(f"- {label} (x{weight}): no data")
            continue
        arrived, completed = ps.get("arrived", 0), ps.get("completed", 0)
        lines.append(
            f"- {label} (x{weight}): {completed}/{arrived} completed ({_pct(completed, arrived)}), "
            f"mean {_f(ps.get('mean_s'))}s, p99 {_f(ps.get('p99_s'))}s"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query 2: scale_trend — cross-scale overview; avoids querying N scales one-by-one
# Fair: simulation observables across all scales
# ---------------------------------------------------------------------------

@_query(
    "scale_trend",
    "all-scale table: latency, ok/fail, OOM count, per-priority completion rate",
    {},
)
def _scale_trend(*, trace_path: Path, eval_data: dict) -> str:
    sr = _scale_results(eval_data)
    scales = sorted(int(k) for k in sr)
    if not scales:
        return "*No scale results in eval data.*"
    lines = [
        "**scale_trend()** — all scales",
        f"{'scale':>6}  {'status':6}  {'latency':>10}  {'OOMs':>5}  {'Q%':>5}  {'I%':>5}  {'B%':>5}",
    ]
    for s in scales:
        e = sr[str(s)]
        if e.get("ok"):
            q = _psub(e, "query")
            i = _psub(e, "interactive")
            b = _psub(e, "batch")
            lines.append(
                f"  {s:2d}x    OK    {e.get('latency', 0):>10.2f}s"
                f"  {e.get('oom_count', 0):>5}"
                f"  {_pct(q.get('completed',0), q.get('arrived',1)):>5}"
                f"  {_pct(i.get('completed',0), i.get('arrived',1)):>5}"
                f"  {_pct(b.get('completed',0), b.get('arrived',1)):>5}"
            )
        else:
            lines.append(f"  {s:2d}x    FAIL  {'N/A':>10}  {'N/A':>5}  {'N/A':>5}  {'N/A':>5}  {'N/A':>5}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query 3: failure_modes — OOM vs timeout vs other (diagnose failure type)
# Fair: simulation observable error counts
# ---------------------------------------------------------------------------

@_query(
    "failure_modes",
    "operator failure breakdown (OOM / timeout / other), suspensions, and assignments at one scale",
    {"scale": [1, 2, 4, 8, 16]},
)
def _failure_modes(*, trace_path: Path, eval_data: dict, scale: int) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    fec = e.get("failure_error_counts", {})
    lines = [
        f"**failure_modes({scale}x)** — {'OK' if e.get('ok') else 'FAILED'}",
        f"- Operator failures:  {e.get('failures', 0)}",
        f"- Suspensions:        {e.get('suspensions', 0)}",
        f"- Assignments made:   {e.get('assignments', 0)}",
        f"- Containers completed: {e.get('containers_completed', 0)}",
    ]
    if fec:
        lines.append("- Failure types:")
        for err, count in sorted(fec.items(), key=lambda x: -x[1]):
            lines.append(f"    {err}: {count}")
    else:
        lines.append("- Failure types: (none recorded)")
    if not e.get("ok"):
        lines.append(f"- Run error: {e.get('error', 'unknown')}")
    return "\n".join(lines)


@_query(
    "failure_counts",
    "operator failure total and failure-type counts at one scale",
    {"scale": [1, 2, 4, 8, 16]},
)
def _failure_counts(*, trace_path: Path, eval_data: dict, scale: int) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    fec = e.get("failure_error_counts", {}) or {}
    lines = [
        f"**failure_counts({scale}x)**",
        f"- Scale status: {'OK' if e.get('ok') else 'FAILED'}",
        f"- Operator failures: {e.get('failures', 0)}",
    ]
    if fec:
        for err, count in sorted(fec.items(), key=lambda x: -x[1]):
            lines.append(f"- {err}: {count}")
    else:
        lines.append("- Failure types: none recorded")
    if not e.get("ok"):
        lines.append(f"- Run error: {e.get('error', 'unknown')}")
    return "\n".join(lines)


@_query(
    "throughput_counts",
    "assignments, completed containers, and suspensions at one scale",
    {"scale": [1, 2, 4, 8, 16]},
)
def _throughput_counts(*, trace_path: Path, eval_data: dict, scale: int) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    return (
        f"**throughput_counts({scale}x)**\n"
        f"- Assignments made: {e.get('assignments', 0)}\n"
        f"- Containers completed: {e.get('containers_completed', 0)}\n"
        f"- Suspensions: {e.get('suspensions', 0)}"
    )


# ---------------------------------------------------------------------------
# Query 4: memory_pressure — aggregate cluster memory utilisation
# Fair: aggregate % only — no per-operator resource curves
# ---------------------------------------------------------------------------

@_query(
    "memory_pressure",
    "cluster mean memory consumed%, allocated%, and allocated-minus-consumed gap at one scale",
    {"scale": [1, 2, 4, 8, 16]},
)
def _memory_pressure(*, trace_path: Path, eval_data: dict, scale: int) -> str:
    e = _entry(eval_data, scale)
    if e is None:
        return f"*Scale {scale}x not found in eval data.*"
    consumed = e.get("mean_memory_consumed_percent")
    allocated = e.get("mean_memory_allocated_percent")
    lines = [
        f"**memory_pressure({scale}x)** — {'OK' if e.get('ok') else 'FAILED'}",
        f"- Mean memory consumed (actual):   {_f(consumed)}%",
        f"- Mean memory allocated (reserved): {_f(allocated)}%",
    ]
    if consumed is not None and allocated is not None:
        gap = allocated - consumed
        lines.append(f"- Allocated-minus-consumed gap: {gap:.1f}%")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Workload structure queries
# Fair: a-priori trace structure only; no resource-curve columns are read.
# ---------------------------------------------------------------------------

@_query(
    "workload_volume",
    "number of pipelines and trace arrival window duration",
    {},
)
def _workload_volume(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)
    if not pipelines:
        return "*Empty trace.*"
    arrivals = sorted(p["arrival"] for p in pipelines.values())
    duration = arrivals[-1] - arrivals[0] if len(arrivals) > 1 else 0.0
    return (
        f"**workload_volume()**\n"
        f"- Pipelines: {len(pipelines)}\n"
        f"- Arrival window: {duration:.0f}s"
    )


@_query(
    "priority_mix",
    "pipeline count and percentage by priority class",
    {},
)
def _priority_mix(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)
    if not pipelines:
        return "*Empty trace.*"
    counts = Counter(p["priority"] for p in pipelines.values())
    total = sum(counts.values())
    lines = ["**priority_mix()**"]
    for prio in _TRACE_PRIORITY_ORDER:
        if counts.get(prio):
            lines.append(f"- {prio}: {counts[prio]} ({_pct(counts[prio], total)})")
    for prio, count in sorted(counts.items()):
        if prio not in _TRACE_PRIORITY_ORDER:
            lines.append(f"- {prio}: {count} ({_pct(count, total)})")
    return "\n".join(lines)


@_query(
    "interarrival_stats",
    "mean and median time between pipeline arrivals",
    {},
)
def _interarrival_stats(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)
    arrivals = sorted(p["arrival"] for p in pipelines.values())
    if len(arrivals) < 2:
        return "*Too few pipelines for inter-arrival statistics.*"
    inter = [arrivals[i + 1] - arrivals[i] for i in range(len(arrivals) - 1)]
    return (
        f"**interarrival_stats()**\n"
        f"- Mean inter-arrival: {statistics.mean(inter):.2f}s\n"
        f"- Median inter-arrival: {statistics.median(inter):.2f}s"
    )


@_query(
    "ops_per_pipeline_stats",
    "median, p90, and max operators per pipeline",
    {},
)
def _ops_per_pipeline_stats(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)
    ops_per = [p["n_ops"] for p in pipelines.values()]
    if not ops_per:
        return "*Empty trace.*"
    return (
        f"**ops_per_pipeline_stats()**\n"
        f"- Median operators/pipeline: {int(statistics.median(ops_per))}\n"
        f"- P90 operators/pipeline: {_percentile(ops_per, 0.9)}\n"
        f"- Max operators/pipeline: {max(ops_per)}"
    )


# ---------------------------------------------------------------------------
# Query 5: dag_topology — DAG shape from trace `parents` column
# Fair: a-priori structural info; does NOT read resource columns
# ---------------------------------------------------------------------------

@_query(
    "dag_topology",
    "DAG shape distribution (linear / branch_out / branch_in) per priority class from trace structure",
    {},
)
def _dag_topology(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)

    def _classify(ops: dict) -> str:
        child_count: Counter = Counter()
        for plist in ops.values():
            for p in plist:
                child_count[p] += 1
        has_fork = any(c >= 2 for c in child_count.values())
        has_join = any(len(plist) >= 2 for plist in ops.values())
        if has_fork and has_join:
            return "complex"
        if has_fork:
            return "branch_out"
        if has_join:
            return "branch_in"
        return "linear"

    by_priority: dict[str, Counter] = defaultdict(Counter)
    for p in pipelines.values():
        by_priority[p["priority"]][_classify(p["ops"])] += 1

    lines = ["**dag_topology()** — DAG shape per priority class"]
    for prio in ["QUERY", "INTERACTIVE", "BATCH_PIPELINE"]:
        counts = by_priority.get(prio)
        if not counts:
            continue
        total = sum(counts.values())
        parts = ", ".join(
            f"{shape} {counts[shape]} ({_pct(counts[shape], total)})"
            for shape in ["linear", "branch_out", "branch_in", "complex"]
            if counts.get(shape)
        )
        lines.append(f"- {prio} ({total}): {parts}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query 6: arrival_histogram — burst detection from trace timing
# Fair: a-priori arrival timing, no resource info
# ---------------------------------------------------------------------------

@_query(
    "arrival_histogram",
    "pipeline arrivals per 5-minute window; marks burst spikes (> 2x mean)",
    {},
)
def _arrival_histogram(*, trace_path: Path, eval_data: dict) -> str:
    pipelines = _read_workload_pipelines(trace_path)
    arrivals = [p["arrival"] for p in pipelines.values()]
    arrivals.sort()
    if len(arrivals) < 2:
        return "*Too few pipelines for histogram.*"

    t0 = arrivals[0]
    bin_size = 300
    n_bins = max(1, int((arrivals[-1] - t0) / bin_size) + 1)
    bins = [0] * n_bins
    for a in arrivals:
        bins[min(int((a - t0) / bin_size), n_bins - 1)] += 1

    mean_b = statistics.mean(bins)
    lines = ["**arrival_histogram() — pipelines per 5-min window**"]
    for i, count in enumerate(bins):
        bar = "#" * min(count // 2, 30)
        burst = " ← BURST" if mean_b > 0 and count > 2 * mean_b else ""
        lines.append(f"  t={i * bin_size:5.0f}s  {count:4d}  {bar}{burst}")
    lines.append(f"  mean/window: {mean_b:.1f}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("query_name", nargs="?", help="Query to run. Omit to list all.")
    parser.add_argument("--trace", type=Path, default=None)
    parser.add_argument("--eval", type=Path, default=None, dest="eval_json")
    parser.add_argument("--args", nargs="*", default=[],
                        help="key=value pairs passed to the query function.")
    args = parser.parse_args()

    if args.query_name is None:
        print("Available queries:\n")
        print(available_queries_text())
        return

    eval_data: dict = {}
    if args.eval_json:
        eval_data = json.loads(args.eval_json.read_text(encoding="utf-8"))

    kv: dict = {}
    for pair in (args.args or []):
        k, _, v = pair.partition("=")
        try:
            v = int(v)
        except ValueError:
            pass
        kv[k] = v

    trace_path = args.trace or Path(".")
    print(run_query(args.query_name, kv, trace_path, eval_data))


if __name__ == "__main__":
    main()
