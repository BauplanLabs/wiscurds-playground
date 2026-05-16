# experiments/

One subtree per experiment. An experiment owns numbered runs; a run owns one or
more iterations:

```text
experiments/<experiment>/runs/<N>/iters/<M>/
```

## Names

- `<experiment>`: human-readable study name, for example `prompt-quality`,
  `cure-overalloc`, or a `tool/iterate_chain.py --chain` value.
- `runs/<N>`: the Nth execution of that experiment. Re-running the same
  experiment creates `runs/2`, `runs/3`, and so on.
- `iters/<M>`: iteration M within that run. Plain `tool/iterate.py` writes
  `iters/1`; `tool/iterate_chain.py` writes `iters/1..K`.

`tool/iterate.py` now uses `--experiment` for this name. `--exp` remains a
compatibility alias.

## Tracked vs Generated

- `oneshot/`: retained 01 collection: seed schedulers, results, probes, plots.
- `oneshot-est/`: retained 02 collection for estimation-noise studies.
- `RESEARCH_LOG.md`: tracked research notes.
- `<experiment>/runs/`: generated prompts, responses, schedulers, and eval JSON.
  These are gitignored by default, except already tracked reference runs.

## Run-Dir Contract

The public interface for later iterations is:

- `scheduler_out.py`: scheduler produced by the run dir.
- `eval_out.json`: per-scale simulation results for that scheduler.

The directory also keeps provenance: `scheduler_in*.py`, `eval_in*.json`,
`prompt_user.txt`, `prompt_system.txt`, `response_raw.txt`, and `meta.json`.

## Step API

```powershell
# Bootstrap a seed scheduler into the run-dir contract.
uv run python tool/iterate.py evaluate --output-dir experiments/E/r0 `
    --scheduler experiments/oneshot/schedulers/high/scheduler_high_006.py `
    --trace scenarios/traces/v1/bench_canonical_train.csv

# Build prompt, call LLM, then evaluate.
uv run python tool/iterate.py genprompt --output-dir experiments/E/r1 `
    --input-dirs experiments/E/r0 --trace scenarios/traces/v1/bench_canonical_train.csv
uv run python tool/iterate.py gensched --output-dir experiments/E/r1
uv run python tool/iterate.py evaluate --output-dir experiments/E/r1 `
    --trace scenarios/traces/v1/bench_canonical_train.csv

# Or run those three steps together.
uv run python tool/iterate.py nextiter --output-dir experiments/E/r1 `
    --input-dirs experiments/E/r0 --trace scenarios/traces/v1/bench_canonical_train.csv
```

`--input-dirs A,B,...` switches prompt generation to the multi-candidate path.

## Current Experiments

| Experiment | Purpose | Starting point |
|---|---|---|
| `oneshot/` | Retained one-shot reasoning-effort study. | No seed scheduler; each scheduler is generated directly from the one-shot prompt. |
| `oneshot-est/` | Retained one-shot estimation-noise study. | No iterative seed; uses generated `scheduler_est_*.py` schedulers and compares estimator conditions. |
| `prompt-quality/` | Prompt/feedback quality audit for the current iteration loop. | `experiments/oneshot/schedulers/high/scheduler_high_006.py` on `scenarios/traces/v1/bench_canonical_train.csv`. |
| `cure-overalloc/` | First chain test: can iteration fix over-allocation crashes? | `experiments/cure-overalloc/runs/1/iter_001_20260515_185912/scheduler_in.py` (`20260515_173934...` prompt-quality output). |
| `cure-overalloc-preserve/` | Same chain with preserve-feedback added. | Same `scheduler_in.py` as `cure-overalloc/`. |
| `cure-overalloc-best/` | Test `--select best`; exposed the old selector scoring bug. | Same `scheduler_in.py` as `cure-overalloc/`. |
| `cure-overalloc-best2/` | Re-run `--select best` after fixing selector scoring. | Same `scheduler_in.py` as `cure-overalloc/`. |
| `cure-overalloc-bestofn/` | Best-of-N sampling-width test using `--width K`. | Same `scheduler_in.py` as `cure-overalloc/`. |
