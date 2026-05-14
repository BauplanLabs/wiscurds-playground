#!/usr/bin/env python3
"""Analyze retained summer2026 scheduler experiments.

Usage:
    python tool/legacy/analyze.py 01_reasoning [all|latency|probe]
    python tool/legacy/analyze.py 02_estimation [all|latency|probe]
    python tool/legacy/analyze.py all [--prototype]

Latency analysis writes analysis.jsonl files under results/.
Probe analysis writes probes.csv files under results/.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from pathlib import Path

SUMMER2026_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(SUMMER2026_DIR))

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("simulation_utils").setLevel(logging.ERROR)

from simulation_utils import extract_metrics_from_stats, get_raw_stats_for_policy
from tool.config import (
    ESTIMATOR_CONDITIONS,
    EXPERIMENTS,
    PROJECT_ROOT,
    RESULTS_DIR,
    SCHEDULERS_DIR,
    TRACES_DIR,
    get_canonical_base_params,
)

EFFORTS = ["none", "low", "medium", "high"]
PHASES = ["all", "latency", "probe"]
SCHEDULER_LIMIT_KEYS = {"tick_timeout_ms", "max_outstanding", "_cluster_sizes"}


def parse_header(filepath: Path) -> dict:
    """Read generated scheduler metadata from leading comment headers."""
    meta: dict = {}
    with filepath.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip().startswith("#"):
                break
            match = re.match(r"^#\s+(\w+):\s+(.+)$", line.strip())
            if not match:
                continue
            key, value = match.group(1), match.group(2)
            with suppress(ValueError, OverflowError):
                numeric = float(value)
                value = int(numeric) if numeric == int(numeric) else numeric
            meta[key] = value
    return meta


def static_analysis(filepath: Path) -> dict:
    """Cheap source-level features used by plots and sanity checks."""
    src = filepath.read_text(encoding="utf-8")
    code_lines = [
        line for line in src.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    return {
        "code_lines": len(code_lines),
        "uses_priority": "Priority" in src,
        "uses_suspension": "Suspend(" in src,
        "uses_multi_pool": src.count("pool_id") > 1,
    }


def load_existing_records(output_path: Path) -> dict[str, dict]:
    """Return existing JSONL records keyed by scheduler filename."""
    if not output_path.exists():
        return {}

    records: dict[str, dict] = {}
    with output_path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print(f"WARNING: skipping malformed JSON at {output_path}:{line_no}")
                continue
            if filename := record.get("filename"):
                records[filename] = record
    return records


def baseline_median(trace_files: list[str], metric: str, base_params: dict) -> float:
    """Evaluate the built-in naive scheduler under the same trace/scale setup."""
    cluster_sizes = base_params.get("_cluster_sizes")
    baseline_params = {
        key: value for key, value in base_params.items()
        if key not in SCHEDULER_LIMIT_KEYS
    }

    if cluster_sizes:
        raw = []
        for n_pools in cluster_sizes:
            params = baseline_params.copy()
            params["num_pools"] = n_pools
            raw.extend(get_raw_stats_for_policy(params, trace_files, "naive"))
        assert raw, "Baseline failed"
    else:
        raw = get_raw_stats_for_policy(baseline_params, trace_files, "naive")
        assert len(raw) == len(trace_files), "Baseline failed"

    values = extract_metrics_from_stats(raw, metric, base_params=baseline_params)
    return float(statistics.median(values))


def evaluate_in_subprocess(
    scheduler_file: Path,
    trace_files: list[str],
    baseline: float,
    metric: str,
    base_params: dict,
) -> dict:
    """Run untrusted scheduler code in a fresh Python process."""
    worker = SUMMER2026_DIR / "tool" / "legacy" / "analyze_worker.py"
    cmd = [
        sys.executable,
        str(worker),
        str(scheduler_file),
        json.dumps(trace_files),
        str(baseline),
        metric,
        json.dumps(base_params),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(SUMMER2026_DIR),
            timeout=base_params.get("subprocess_timeout"),
        )
    except subprocess.TimeoutExpired:
        return {
            "functional": False,
            "failure_mode": "total_timeout",
            "error_message": f"Subprocess exceeded {base_params.get('subprocess_timeout')}s",
        }
    except Exception as exc:
        return {
            "functional": False,
            "failure_mode": "eval_error",
            "error_message": str(exc),
        }

    for line in reversed(result.stdout.strip().splitlines()):
        if line.startswith("__RESULT__"):
            return json.loads(line[len("__RESULT__"):])

    stderr_snippet = result.stderr[-200:] if result.stderr else "no output"
    return {
        "functional": False,
        "failure_mode": "no_output",
        "error_message": stderr_snippet,
    }


def evaluate_one(
    scheduler_file: Path,
    trace_files: list[str],
    baseline: float,
    metric: str,
    base_params: dict,
) -> dict:
    """Build the complete persisted analysis record for one scheduler."""
    start = time.time()
    return {
        "filename": scheduler_file.name,
        "scheduler_dir": scheduler_file.parent.name,
        "baseline_median": baseline,
        "metric": metric,
        **parse_header(scheduler_file),
        **static_analysis(scheduler_file),
        **evaluate_in_subprocess(
            scheduler_file=scheduler_file,
            trace_files=trace_files,
            baseline=baseline,
            metric=metric,
            base_params=base_params,
        ),
        "simulation_seconds": round(time.time() - start, 2),
    }


def scheduler_files(scheduler_dirs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for directory in scheduler_dirs:
        files.extend(sorted(directory.glob("scheduler_*.py")))
    return files


def append_record(output_path: Path, record: dict) -> None:
    with output_path.open("a", encoding="utf-8") as out_f:
        out_f.write(json.dumps(record) + "\n")
        out_f.flush()
        with suppress(OSError):
            os.fsync(out_f.fileno())


def run_latency_analysis(
    scheduler_dirs: list[Path],
    trace_files: list[str],
    output_dir: Path,
    base_params: dict,
    *,
    metric: str = "latency",
    label: str = "",
    workers: int = 1,
) -> None:
    """Evaluate scheduler files and append missing records to analysis.jsonl."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "analysis.jsonl"
    existing = load_existing_records(output_path)

    print("Running baseline...")
    baseline = baseline_median(trace_files, metric, base_params)
    print(f"Baseline median {metric}: {baseline:.4f}")

    files = scheduler_files(scheduler_dirs)
    assert files, f"No scheduler_*.py found in {scheduler_dirs}"
    print(
        f"{len(trace_files)} traces | {len(files)} schedulers | metric={metric}"
        f"{' | ' + label if label else ''} | workers={workers}"
    )

    if existing:
        print(f"Resuming: {len(existing)} existing records")
    for path in files:
        if path.name in existing:
            print(f"[skip] {path.name}  already recorded")

    pending = [path for path in files if path.name not in existing]
    records = dict(existing)

    def save(path: Path, record: dict, idx: int) -> None:
        records[path.name] = record
        append_record(output_path, record)
        ok = "OK" if record["functional"] else "FAIL"
        detail = (
            f"{metric}={record.get(f'median_{metric}', 'N/A')}"
            if record["functional"]
            else record.get("failure_mode", "")
        )
        print(f"[{idx}/{len(pending)}] {path.name}  {ok} {detail}")

    if workers <= 1:
        for idx, path in enumerate(pending, 1):
            save(path, evaluate_one(path, trace_files, baseline, metric, base_params), idx)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(evaluate_one, path, trace_files, baseline, metric, base_params): path
                for path in pending
            }
            for idx, future in enumerate(as_completed(futures), 1):
                save(futures[future], future.result(), idx)

    for path in PROJECT_ROOT.glob("pool_*_utility.csv"):
        path.unlink(missing_ok=True)

    ordered_records = [records[path.name] for path in files if path.name in records]
    functional = sum(1 for record in ordered_records if record["functional"])
    beats = sum(1 for record in ordered_records if record.get("beats_baseline"))
    print("=" * 50)
    print(f"DONE: {functional}/{len(ordered_records)} functional, {beats}/{len(ordered_records)} beat baseline")
    print(f"Output: {output_path}")


def run_probes_for_dir(
    scheduler_dir: Path,
    output_dir: Path,
    base_params: dict,
    *,
    workers: int = 1,
) -> None:
    """Run property probes and write output_dir/probes.csv."""
    from tool.probe.run_probes import ensure_traces, run_all_probes, _worker, write_csv

    traces = ensure_traces()
    files = sorted(scheduler_dir.glob("scheduler_*.py"))
    if not files:
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    results_by_file: dict[Path, dict] = {}

    if workers <= 1:
        for idx, path in enumerate(files, 1):
            results = run_all_probes(path, base_params, traces)
            passed = sum(1 for result in results.values() if result.get("functional"))
            print(f"  [{idx}/{len(files)}] {path.name}: {passed}/{len(results)}")
            results_by_file[path] = results
    else:
        import multiprocessing as mp

        probe_timeout = base_params.get("subprocess_timeout", 120)
        pool = mp.Pool(processes=workers)
        async_results = [(path, pool.apply_async(_worker, ((path, base_params, traces),))) for path in files]
        pool.close()

        for idx, (path, async_result) in enumerate(async_results, 1):
            try:
                _, results = async_result.get(timeout=probe_timeout)
            except mp.TimeoutError:
                results = {"_probe": {"functional": False, "failure_mode": "timeout"}}
                print(f"  [{idx}/{len(files)}] {path.name}: TIMEOUT after {probe_timeout}s")
            except Exception as exc:
                results = {
                    "_probe": {
                        "functional": False,
                        "failure_mode": "probe_error",
                        "error_message": str(exc),
                    }
                }
            passed = sum(1 for result in results.values() if result.get("functional"))
            print(f"  [{idx}/{len(files)}] {path.name}: {passed}/{len(results)}")
            results_by_file[path] = results

        pool.terminate()
        pool.join()

    write_csv(files, [results_by_file[path] for path in files], output_dir / "probes.csv")


def canonical_trace_setup(prototype: bool) -> tuple[dict, list[str]]:
    base_params = get_canonical_base_params(prototype=prototype)
    canonical = TRACES_DIR / "bench_canonical_train.csv"
    assert canonical.exists(), f"Canonical trace not found: {canonical}"
    base_params["_cluster_sizes"] = [1, 2, 4, 8, 16]
    return base_params, [str(canonical)]


def analyze_01_reasoning(prototype: bool, workers: int, phase: str) -> None:
    base_params, trace_files = canonical_trace_setup(prototype)

    for effort in EFFORTS:
        scheduler_dir = SCHEDULERS_DIR / "reasoning" / effort
        if not scheduler_files([scheduler_dir]):
            print(f"  SKIP {effort}: no schedulers in {scheduler_dir}")
            continue

        output_dir = RESULTS_DIR / "01_reasoning" / effort
        if phase in ("all", "probe"):
            print(f"\n--- Probes: {effort} ---")
            probe_params = base_params.copy()
            probe_params["duration"] = 120
            run_probes_for_dir(scheduler_dir, output_dir, probe_params, workers=workers)

        if phase in ("all", "latency"):
            print(f"\n--- Latency: {effort} ---")
            run_latency_analysis(
                [scheduler_dir],
                trace_files,
                output_dir,
                base_params,
                label=f"reasoning={effort}",
                workers=workers,
            )


def analyze_02_estimation(prototype: bool, workers: int, phase: str) -> None:
    scheduler_dir = SCHEDULERS_DIR / "estimation" / "low"
    if not scheduler_files([scheduler_dir]):
        print(f"  SKIP: no schedulers in {scheduler_dir}")
        return

    base_params, trace_files = canonical_trace_setup(prototype)
    output_base = RESULTS_DIR / "02_estimation"
    output_effort = output_base / "low"

    if phase in ("all", "probe"):
        print("\n--- Probes: estimation ---")
        probe_params = base_params.copy()
        probe_params["duration"] = 120
        run_probes_for_dir(scheduler_dir, output_base, probe_params, workers=workers)

    if phase in ("all", "latency"):
        for sigma, overrides in ESTIMATOR_CONDITIONS.items():
            print(f"\n--- Latency: {sigma} ---")
            output_dir = output_effort / sigma

            if sigma == "no_estimation":
                source = RESULTS_DIR / "01_reasoning" / "low" / "analysis.jsonl"
                if source.exists():
                    output_dir.mkdir(parents=True, exist_ok=True)
                    target = output_dir / "analysis.jsonl"
                    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
                    print(f"  [Retrieved] Data copied from {source}")
                    continue
                print(f"  WARNING: {source} not found; evaluating no_estimation directly.")

            params = base_params.copy()
            params.update(overrides)
            run_latency_analysis(
                [scheduler_dir],
                trace_files,
                output_dir,
                params,
                label=f"estimation={sigma}",
                workers=workers,
            )


HANDLERS = {
    "01_reasoning": analyze_01_reasoning,
    "02_estimation": analyze_02_estimation,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=list(HANDLERS) + ["all"])
    parser.add_argument("phase", nargs="?", default="all", choices=PHASES)
    parser.add_argument("--prototype", action="store_true", help="Use fast simulator settings.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers for latency/probe runs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    experiments = list(HANDLERS) if args.experiment == "all" else [args.experiment]

    for experiment in experiments:
        print(f"\n{'=' * 60}")
        print(f"Experiment: {experiment} - {EXPERIMENTS.get(experiment, '')}")
        if args.prototype:
            print("  [PROTOTYPE MODE - results not meaningful]")
        if args.phase != "all":
            print(f"  [PHASE: {args.phase} only]")
        print("=" * 60)
        HANDLERS[experiment](prototype=args.prototype, workers=args.workers, phase=args.phase)


if __name__ == "__main__":
    main()
