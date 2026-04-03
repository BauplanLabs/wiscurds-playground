#!/usr/bin/env python3
"""Run all probes against one or more scheduler files.

Usage:
    python run_probes.py scheduler_low_001.py
    python run_probes.py schedulers-low/
    python run_probes.py schedulers-low/ schedulers-high/
"""
from __future__ import annotations

import sys
import argparse
from pathlib import Path

PROBE_DIR = Path(__file__).resolve().parent
ONE_SHOT_DIR = PROBE_DIR.parent
sys.path.insert(0, str(ONE_SHOT_DIR))
sys.path.insert(0, str(PROBE_DIR))

from config import get_canonical_base_params
from generate_probe_traces import PRESETS, generate_probe_trace
from probe_syntax import probe_syntax
from probe_valid_scheduler import probe_valid_scheduler
from probe_basic_run import probe_basic_run
from probe_grouping import probe_grouping
from probe_overcommit import probe_overcommit
from probe_priority_ordering import probe_priority_ordering
from probe_starvation import probe_starvation
from probe_no_deadlock import probe_no_deadlock

# probe_retry_run and probe_suspend_run have incorrect function names (copy-paste bugs);
# import by attribute so the runner still works until those are fixed.
import probe_retry_run as _retry_mod
import probe_suspend_run as _suspend_mod
probe_retry_run = getattr(_retry_mod, "probe_retry_run", None) or getattr(_retry_mod, "probe_basic_run")
probe_suspend_run = getattr(_suspend_mod, "probe_suspend_run", None) or getattr(_suspend_mod, "probe_retry_run")


TRACES_DIR = PROBE_DIR / "traces"

PROBE_TRACES = {
    "basic":              "probe_basic.csv",
    "oom_retry":          "probe_oom_retry.csv",
    "suspend":            "probe_suspend.csv",
    "priority_ordering":  "probe_priority_ordering.csv",
    "starvation":         "probe_starvation.csv",
    "no_deadlock":        "probe_no_deadlock.csv",
}


def ensure_traces() -> dict[str, str]:
    """Generate any missing probe trace files. Returns dict mapping preset name to path."""
    TRACES_DIR.mkdir(exist_ok=True)
    paths = {}
    for preset_name, filename in PROBE_TRACES.items():
        path = TRACES_DIR / filename
        if not path.exists():
            print(f"  Generating trace: {filename}")
            generate_probe_trace(PRESETS[preset_name], str(path))
        paths[preset_name] = str(path)
    return paths


def run_all_probes(scheduler_file: Path, base_params: dict, traces: dict[str, str]) -> dict[str, dict]:
    results: dict[str, dict] = {}

    results["syntax"] = probe_syntax(str(scheduler_file))
    if not results["syntax"]["functional"]:
        return results

    results["valid_scheduler"] = probe_valid_scheduler(str(scheduler_file))
    if not results["valid_scheduler"]["functional"]:
        return results

    results["basic_run"]         = probe_basic_run(str(scheduler_file),         [traces["basic"]],             base_params)
    results["retry_run"]         = probe_retry_run(str(scheduler_file),         [traces["oom_retry"]],         base_params)
    results["suspend_run"]       = probe_suspend_run(str(scheduler_file),       [traces["suspend"]],           base_params)
    results["grouping"]          = probe_grouping(str(scheduler_file),          [traces["basic"]],             base_params)
    results["overcommit"]        = probe_overcommit(str(scheduler_file),        [traces["basic"]],             base_params)
    results["priority_ordering"] = probe_priority_ordering(str(scheduler_file), [traces["priority_ordering"]], base_params)
    results["starvation"]        = probe_starvation(str(scheduler_file),        [traces["starvation"]],        base_params)
    results["no_deadlock"]       = probe_no_deadlock(str(scheduler_file),       [traces["no_deadlock"]],       base_params)

    return results


def print_results(scheduler_file: Path, results: dict[str, dict]) -> None:
    passed = sum(1 for r in results.values() if r.get("functional"))
    total = len(results)
    status = "PASS" if passed == total else "FAIL"
    print(f"\n[{status}] {scheduler_file.name}  ({passed}/{total} probes passed)")
    for probe_name, result in results.items():
        ok = "  OK  " if result.get("functional") else " FAIL "
        detail = ""
        if not result.get("functional"):
            detail = f"  [{result.get('failure_mode', '?')}] {result.get('error_message', '')}"
        print(f"  {ok}  {probe_name}{detail}")


def collect_scheduler_files(paths: list[Path]) -> list[Path]:
    files = []
    for p in paths:
        if p.is_dir():
            files.extend(sorted(p.glob("scheduler_*.py")))
        elif p.is_file():
            files.append(p)
        else:
            print(f"Warning: {p} does not exist, skipping.")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("targets", nargs="+", type=Path,
                        help="Scheduler .py file(s) or director(ies) containing scheduler_*.py files.")
    args = parser.parse_args()

    scheduler_files = collect_scheduler_files(args.targets)
    if not scheduler_files:
        print("No scheduler files found.")
        sys.exit(1)

    print(f"Ensuring probe traces exist...")
    traces = ensure_traces()

    base_params = get_canonical_base_params()

    total_pass = 0
    for sf in scheduler_files:
        results = run_all_probes(sf, base_params, traces)
        print_results(sf, results)
        if all(r.get("functional") for r in results.values()):
            total_pass += 1

    print(f"\n{'='*50}")
    print(f"DONE: {total_pass}/{len(scheduler_files)} schedulers passed all probes")


if __name__ == "__main__":
    main()
