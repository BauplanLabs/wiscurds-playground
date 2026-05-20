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

- `oneshot/`, `oneshot-est/`: retained one-shot scheduler collections.
- `RESULTS.md`: concise experiment results (key configs and result tables).
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

## Experiment Groups

| Group | Description |
|---|---|
| `oneshot/` | One-shot schedulers by reasoning effort (`none`/`low`/`medium`/`high`). Seed pool for later experiments. |
| `oneshot-est/` | One-shot schedulers under different runtime-estimator noise conditions. |
| `prompt-quality/` | Prompt and feedback quality audit; two tracked reference runs. |
| `chain/` | Iterative chain experiments (`chain-last`, `chain-last-preserve`, `chain-best-v1`, `chain-best`, `chain-w5`, `chain-depth10`, `chain-depth10-bestk2`). |
| `prompt-ablation/` | System prompt ablation (`short` vs `full` and section drop-outs); 2 seeds × up to 6 arms × n=5 draws. |
| `cross-scen/` | Cross-scenario generalization: 4 v2 traces × 3 arms (`short·n=1`, `short·n=5`, `full·n=5`). |
| `width/` | Sampling width study: `short`/`full` prompt × `n=1`/`n=5` × 2 seeds. |
| `query/` | Assembled-query prompt exploration (smoke test and single-scenario runs). |
