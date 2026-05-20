# Results Summary

**Metric:** penalized geomean (pgeom, seconds) — a failed scale contributes 720s; lower is better.

**Prompt variants:**
- `short` — base Eudoxia context only (~40 lines)
- `full`  — + OOM behavior, objective intuition, 5 hard constraints (~152 lines)

**Seed schedulers:**
- `s1` — `chain/chain-last/runs/1/iter_001_20260515_185912/scheduler_in.py`: 3/5 pass, pgeom 190s
- `s2` — `oneshot/schedulers/high/scheduler_high_045.py`: 5/5 pass, pgeom 486s

---

## 1. Prompt ablation — single-shot

**Dirs:** `prompt-ablation/ab-s1-*`, `prompt-ablation/ab-s2-*`
**Config:** `bench_canonical_train.csv` · gpt-5.2/high · n=5 draws per prompt · scales=[1,2,4,8,16] @ 180s(timeout)

n=5 draws per cell. avg pgeom = mean of per-draw penalized geomean (failed scale = 720s).

| prompt | s1: avg pass /5 | s1: avg pgeom (s) | s2: avg pass /5 | s2: avg pgeom (s) |
|--------|---:|---:|---:|---:|
| `short` | 4.0 | 143 | 2.4 | 407 |
| `full` | 3.4 | 263 | 3.4 | 325 |
| `full – apicontract` | 3.2 | 247 | — | — |
| `full – objective` | 3.4 | 199 | — | — |
| `full – perfbudget` | **5.0** | 223 | 0.4 | 624 (collapse) |
| `full – retrybound` | 1.8 | 511 | — | — |

---

## 2. Width × Prompt × Scenario

**Dir:** `cross-scen/`
**Config:** 4 v2 traces · s2 seed · gpt-5.2/high · {1,2,4,8,16} @ 180s

Each draw = one generated scheduler evaluated at 5 cluster scales. n=20 per prompt (5 draws × 4 scenarios).

Best-of-5: generate 5 candidates per scenario, keep the one with lowest pgeom.

### Pass rate

| prompt | n=1 pass rate | n=5 (best-of-5) pass rate |
|--------|-------------:|-------------------------:|
| `short` | 60% (12/20 draws) | 100% (4/4 scenarios) |
| `full`  | **90%** (18/20 draws) | 100% (4/4 scenarios) |

### Best-of-5 winner pgeom by scenario (lower is better)

| scenario | s2 seed | `short · n=1` | `short · n=5` | `full · n=5` |
|----------|---:|---:|---:|---:|
| canonical_steady_balanced | 464.8 | 37.1 | **20.7** | 28.6 |
| priority-heavy_steady_balanced | 143.0 | 720.0 (failed) | 14.2 | **13.2** |
| canonical_bursty_cpu-bound | 312.9 | 20.8 | 20.4 | **17.4** |
| batch-heavy_bursty_memory-bound | 468.0 | 158.7 (2/5) | **19.8** | 20.7 |
| **avg** | | 234.2 | **18.8** | 20.0 |

