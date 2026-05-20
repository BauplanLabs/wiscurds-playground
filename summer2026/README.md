# summer2026

Self-contained LLM scheduler experiments on Eudoxia for the NOVAS 2026 work.

## Setup

```powershell
uv sync
Copy-Item .env.example .env   # set OPENAI_API_KEY for real LLM runs
```

Run commands from this directory. Do not set `UV_CACHE_DIR` inside the project;
the `eudoxia` git dependency becomes much slower with a local uv cache.

## Structure

| Path | Purpose |
|---|---|
| `tool/iterate.py` | Main single-iteration entry point. Also exposes `evaluate`, `genprompt`, `gensched`, and `nextiter` subcommands. |
| `tool/iterate_chain.py` | Multi-round improvement loop over one experiment name. |
| `tool/legacy/` | Kept-runnable scripts for the old 01/02 experiments. |
| `tool/probe/` | Scheduler validity and behavior probes. |
| `prompts/` | Editable system prompt templates and domain context. |
| `scenarios/` | Scenario registry, params, and workload traces. |
| `experiments/` | Tracked curated results plus gitignored generated runs. |
| `llm.py`, `prompts.py`, `simulation_utils.py` | Shared LLM, prompt-building, and Eudoxia adapter code. |

Generated run artifacts live under:

```text
experiments/<experiment>/runs/<N>/iters/<M>/
```

`<experiment>` is a human-readable experiment name. The old `--exp` flag still
works, but new commands should use `--experiment`.

## Current Workflow

Dry-run a prompt and artifact write without calling the LLM or simulator:

```powershell
uv run python tool/iterate.py `
    --experiment prompt-quality `
    --scheduler experiments/oneshot/schedulers/low/scheduler_low_001.py `
    --trace scenarios/traces/v1/bench_canonical_train.csv `
    --dry-run
```

Run one real LLM improvement iteration:

```powershell
uv run python tool/iterate.py `
    --experiment prompt-quality `
    --scheduler <scheduler.py> `
    --trace <trace.csv>
```

Each iteration writes:

| File | Meaning |
|---|---|
| `prompt_system.txt`, `prompt_user.txt` | Rendered prompts sent to the model. |
| `*.template.md` | Prompt/context template snapshots. |
| `scheduler_in.py`, `scheduler_out.py` | Input scheduler and generated scheduler. |
| `response_raw.txt` | Raw LLM response before code extraction. |
| `eval_in.json`, `eval_out.json` | Simulator results before and after the generated scheduler. |
| `meta.json` | Model, effort, prompt hashes, timestamps, and run metadata. |

## Step API

For manual or chained workflows, the same iteration can be split into separate
steps that share one `--output-dir`:

```powershell
uv run python tool/iterate.py evaluate  --output-dir experiments/E/r0 `
    --scheduler <seed.py> --trace <trace.csv>
uv run python tool/iterate.py genprompt --output-dir experiments/E/r1 `
    --input-dirs experiments/E/r0 --trace <trace.csv>
uv run python tool/iterate.py gensched  --output-dir experiments/E/r1
uv run python tool/iterate.py evaluate  --output-dir experiments/E/r1 `
    --trace <trace.csv>
```

`nextiter` runs `genprompt`, `gensched`, and `evaluate` in one command.

## Legacy 01/02

The preserved one-shot collections are under `experiments/oneshot/` and
`experiments/oneshot-est/`. Legacy scripts still use their original option
names:

```powershell
uv run python tool/legacy/generate.py --exp reasoning --effort low --n 50
uv run python tool/legacy/analyze.py oneshot
uv run python tool/legacy/plot.py oneshot
uv run python tool/probe/run_probes.py experiments/oneshot/schedulers/low
```
