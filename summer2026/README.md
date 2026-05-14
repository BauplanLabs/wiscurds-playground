# summer2026

LLM scheduler experiments on Eudoxia. Target: **NOVAS Workshop 2026-06-13**.

## Setup

```powershell
uv sync
Copy-Item .env.example .env   # then set OPENAI_API_KEY
```

Do **not** set `UV_CACHE_DIR` to a project-local path - with the `eudoxia` git
source, that slows `uv run` by ~200x.

## Step 1: prompt-quality research

**Goal.** Make the prompt rich enough that a senior systems engineer reading it
could articulate 3-5 specific scheduler-improvement directions. Until that bar
is met, we are testing "LLM with a blindfold," not "LLM as scheduler designer."

**Unit of work.** One `tool/iterate.py` invocation. Each call is independent -
no chain, no history. Compare runs by inspecting their artifacts side by side.

**Loop.**

```
1. Run iterate.py on any scheduler  (dry-run = free, real = LLM cost)
2. Open iterations/<run_id>/ - read prompt_system.txt + prompt_user.txt
3. Apply the "senior eng" test on those two files
4. If the prompt is thin, edit ONE lever:
     - prompts/context/eudoxia_bauplan.md  (domain knowledge)
     - prompts/system_iterate.md           (API ref / output rules)
     - prompts.py:get_iteration_feedback_prompt  (perf table / diagnostics)
5. Re-run. Compare new vs. old iterations/<run_id>/.
```

**Commands.**

```powershell
# Free dry-run - composes real prompts, skips LLM & simulator
uv run python tool/iterate.py `
    --scheduler schedulers/reasoning/low/scheduler_low_001.py `
    --trace traces/bench_canonical_train.csv --dry-run

# Real run - runs simulator across 5 scales + 1 LLM call
uv run python tool/iterate.py --scheduler <path.py> --trace <trace.csv>

# Swap prompt files without editing Python
uv run python tool/iterate.py --scheduler <path.py> --trace <trace.csv> `
    --system-prompt prompts/system_iterate.md `
    --context prompts/context/eudoxia_bauplan.md
```

**Each run writes to `iterations/<run_id>/`:**

| File | What it is |
|---|---|
| `prompt_system.txt` / `prompt_user.txt` | Final rendered prompts sent to LLM |
| `*.template.md` | Copies of the template files used (for reproducibility) |
| `scheduler_in.py` / `scheduler_out.py` | Input and improved scheduler |
| `response_raw.txt` | Raw LLM output before code extraction |
| `eval_in.json` | Per-scale simulator results: latency, OOM count, mem util, per-priority |
| `meta.json` | Model, effort, template SHAs, timestamps |

**Exit Step 1 when** the prompts pass the "senior eng" test on multiple runs
*and* `scheduler_out.py` changes look targeted at signals in the prompt.

## Layout

| Dir | Role |
|---|---|
| `prompts/` | Editable prompt templates (`system_iterate.md`) + `context/` domain markdown |
| `iterations/` | Canonical per-run artifacts from `tool/iterate.py` |
| `scenarios/` | `scenarios.csv` registry + `params/` simulator TOMLs |
| `schedulers/` | Retained 01/02 input schedulers: `reasoning/` and `estimation/`. |
| `traces/`, `traces-v2/` | Workload CSVs |
| `tool/` | `iterate.py`, `iterate_worker.py`, `config.py`, `_mock/`, `probe/`, `legacy/` |
| `results/`, `plots/` | 01/02 analysis outputs and paper figures |

Current canonical test trace: `traces/bench_canonical_train.csv`.

## Legacy 01/02 experiments

Preserved under `tool/legacy/`, not on the current path:

```powershell
uv run python tool/legacy/generate.py --exp reasoning --effort low --n 50
uv run python tool/legacy/analyze.py 01_reasoning
uv run python tool/legacy/plot.py 01_reasoning
uv run python tool/probe/run_probes.py schedulers/reasoning/low
```
