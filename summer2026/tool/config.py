"""Shared config for summer2026 experiments."""

from __future__ import annotations

import math
from pathlib import Path

SUMMER2026_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = SUMMER2026_DIR
TRACES_DIR = SUMMER2026_DIR / "scenarios" / "traces" / "v1"
RESULTS_DIR = SUMMER2026_DIR / "results"   # tool/probe writes results/probes/ here

# Each experiment owns its own schedulers/ + results/ + plots/ subtree.
EXPERIMENTS_DIR = SUMMER2026_DIR / "experiments"
ONESHOT_DIR = EXPERIMENTS_DIR / "oneshot"          # was schedulers/reasoning + results/01_reasoning
ONESHOT_EST_DIR = EXPERIMENTS_DIR / "oneshot-est"  # was schedulers/estimation + results/02_estimation

# Legacy 03-09 pipeline roots (dormant: no data on disk; kept so tool/legacy
# imports resolve and those branches degrade gracefully to "no results").
SCHEDULERS_DIR = SUMMER2026_DIR / "schedulers"
PLOTS_DIR = SUMMER2026_DIR / "plots"

# ---------------------------------------------------------------------------
# Experiment registry
# ---------------------------------------------------------------------------

EXPERIMENTS = {
    "oneshot":      "Fig 1  -  one-shot, vary reasoning level (no estimation)",
    "oneshot-est":  "Fig 2  -  one-shot, vary estimation noise (medium reasoning)",
}

# ---------------------------------------------------------------------------
# Canonical simulation parameters
# ---------------------------------------------------------------------------

CANONICAL_SIM_PARAMS = {
    "duration": 3600,           # 60 minutes
    "ticks_per_second": 100,    # coarser than old 1000
    "num_pools": 1,
    "cpus_per_pool": 64,
    "ram_gb_per_pool": 500,
    "num_pipelines": 10,
    "num_operators": 10,
    "waiting_seconds_mean": 10.0,
    "multi_operator_containers": False,
    "interactive_prob": 0.3,
    "query_prob": 0.1,
    "batch_prob": 0.6,
    "random_seed": 42,
    "per_trace_timeout": None,
    "subprocess_timeout": 180,   # 3-min wall clock per single sim (per scale).
                                  # Historical max for functional schedulers in 01/02 was 119s/sim,
                                  # so 180s gives ~1.5x headroom while killing infinite loops fast.
    # Max job time = 6 minutes (eudoxia param name: max_job_seconds)
    "max_job_seconds": 360,
}

# Prototype mode: fast/cheap, results won't be meaningful but infra is validated
PROTOTYPE_OVERRIDES = {
    "duration": 60,             # 1 minute
    "ticks_per_second": 10,
    "subprocess_timeout": 120,
}

DEFAULT_MODEL = "gpt-5.2-2025-12-11"
SUPPORTED_EFFORTS = ["none", "low", "medium", "high"]

# Estimation conditions: maps label -> sim param overrides
ESTIMATOR_CONDITIONS = {
    "no_estimation": {},  # no estimator at all
    "sigma_0.0": {"estimator_algo": "noisy", "noisy_estimator_sigma": 0.0},     # oracle
    "sigma_0.75": {"estimator_algo": "noisy", "noisy_estimator_sigma": 0.75},   # medium noise
    "sigma_1.5": {"estimator_algo": "noisy", "noisy_estimator_sigma": 1.5},     # high noise
}

# Legacy 05 constants retained for archived scripts in tool/legacy.
TWO_SHOT_PERF_CONDITIONS = {
    "dur3600_ticks100": {"duration": 3600, "ticks_per_second": 100},  # full (baseline)
    "dur1800_ticks100": {"duration": 1800, "ticks_per_second": 100},
    "dur900_ticks100":  {"duration":  900, "ticks_per_second": 100},
    "dur3600_ticks50":  {"duration": 3600, "ticks_per_second":  50},
    "dur3600_ticks25":  {"duration": 3600, "ticks_per_second":  25},
}


def get_canonical_base_params(prototype: bool = False) -> dict:
    from eudoxia.simulator import get_param_defaults

    params = get_param_defaults()
    params.update(CANONICAL_SIM_PARAMS)
    if prototype:
        params.update(PROTOTYPE_OVERRIDES)
    return params


def wilson_interval(
    successes: int, n: int, z: float = 1.96
) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = successes / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - margin), min(1.0, center + margin)
