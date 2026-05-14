# summer2026

This directory is the **only** working tree for the LLM-scheduler research. Everything - code, scenarios, traces, generated schedulers, simulation results, plots - lives here. Do not reach into `../spring2026/`, `../spring2026-v2/`, or the old `AI-for-Distributed-System-Design/` repo: those are archives.

## Two layers of work

### 1. Dev environment (current focus)

The infrastructure has to be solid before the research is meaningful. The four standing goals:

1. **One dir in one repo.** `summer2026/` is self-contained. The `from summer2026.X` import prefix is gone; scripts use flat imports with `summer2026/` on `sys.path`.
2. **One source of scenarios.** Every experiment must draw from `scenarios/` - no experiment-local trace/param hardcoding. Adding a scenario means adding a file there; running an experiment means referencing a scenario by id.
3. **Detailed per-iter output.** When we run one improvement iteration (or a multi-iter loop), we save the *prompt sent to the LLM*, the *raw response*, the *extracted code*, the *per-scale simulation results*, and metadata - to disk, so we can later eyeball, diff, and tweak. Don't compress this away.
4. **README as an index.** `README.md` is the entry point: what's where, how to run each piece, which experiment maps to which paper figure.

If you're about to add a code path that violates any of the four, stop and ask first.

### 2. Research direction

**Question:** Under what conditions does an LLM write a *better* scheduler?

**Target venue:** NOVAS Workshop (Novel Optimizations for Visionary AI Systems), **2026-06-13**. Everything below is prioritized against that date - we want submittable results well before, not a scramble the week of.

**Working order (do (1) first; only escalate to (2) and (3) if results are weak):**

1. **Give the LLM enough context.** Test: would a strong systems person, given exactly what we hand the model, have plausible ideas for how to improve the scheduler? If no, the experiment is measuring "LLM with a blindfold," not "LLM as a scheduler designer." Audit and expand: the eudoxia domain context in `prompts/context/`, the feedback we surface from prior iterations (what ran, what failed, where queue depth blew up, which jobs starved), and the per-iter trace summaries. Until this bar is met, do not move on.
    - Why this is first: gut call is that this alone gets us good NOVAS results. Cheapest to try, easiest to attribute.
2. **Give the LLM some control over what context it gets.** Examples: it can request a specific subset of the trace output, or emit logs in a format we agree on so that output gets fed back into the *next* iteration's prompt. This is the model deciding what it needs to see, not us pre-deciding.
3. **Give the LLM some control over what simulations to run.** It can propose experiments to feed into Eudoxia - extra workloads, perturbed params, ablations - *in addition to* the experiments we fix at each iteration. We keep our evaluation harness; the model gets to also probe.

Stop at the earliest step that produces "good results" for NOVAS. (1) is likely enough; (2) and (3) are escalation paths.

**Variables already on the table (orthogonal to the (1)/(2)/(3) axis):** reasoning effort, context richness, iteration depth, evaluation fidelity (full sim vs. coarse), source-scheduler quality (best/worst/median seed), workload class, prompt structure.

Inspiration / further reading:
- PolicySmith - https://arxiv.org/pdf/2510.08803
- Barbarians (Gated Reasoning) - https://arxiv.org/pdf/2510.06189

The infrastructure work above directly enables this: a unified scenario registry + detailed per-iter logs are what let us *attribute* a quality difference to a specific variable instead of guessing.

## Repo layout (within `summer2026/`)

```
scenarios/        single source of truth for (train_trace, test_trace, params, objective):
                    scenarios.csv         - the registry
                    params/               - sim parameter TOMLs (v1..v4) referenced by the registry
traces/  traces-v2/   raw CSV traces (legacy + current)
                  Current canonical test trace: traces/bench_canonical_train.csv
prompts/          editable prompt templates + domain context for the LLM:
                    system_iterate.md     - system prompt skeleton ({context} placeholder)
                    context/              - eudoxia domain text filled into {context}
                    README.md
schedulers/       retained 01/02 input schedulers:
                    reasoning/            - one-shot reasoning-effort schedulers
                    estimation/           - one-shot estimator schedulers
results/          per-experiment evaluation outputs from 01/02 (analysis.jsonl, summary.csv, ...)
plots/            paper-figure PDFs/PNGs from 01/02
iterations/       per-run artifacts written by tool/iterate.py - one subdir per invocation;
                  canonical location for every iteration's scheduler_in/out + prompts + eval
tool/             current single-iteration loop:
                    iterate.py            - entry point
                    iterate_worker.py     - subprocess simulator runner
                    config.py             - canonical sim params + experiment constants
                    _mock/                - dry-run fixtures (mock schedulers used by --dry-run)
                    probe/                - validity / starvation / overcommit probes
                  legacy/                 - 01/02-era scripts (kept runnable, not active):
                    generate.py, analyze.py, analyze_worker.py, plot.py,
                    batch_iteration_tool.py, generate_scenarios.py, trace_generator.py
llm.py            litellm wrapper + cost tracking
prompts.py        dynamic prompt builders (feedback table). Static system prompt now lives in prompts/.
simulation_utils.py  eudoxia adapter
.env              OPENAI_API_KEY (gitignored)
```

## Common workflows

Install dependencies from this directory with `uv sync`, then run commands with
`uv run` from inside `summer2026/`:

```
# Current workflow - one LLM iteration on a single scheduler
uv run python tool/iterate.py --scheduler <path> --trace <path> --dry-run
uv run python tool/iterate.py --scheduler <path> --trace <path>
#   writes iterations/<run_id>/ with prompts (rendered + template copies),
#   raw LLM response, scheduler_in.py, scheduler_out.py, eval_in.json, meta.json

# Swap the system prompt without touching Python:
uv run python tool/iterate.py --scheduler <path> --trace <path> \
    --system-prompt prompts/system_iterate.md \
    --context prompts/context/eudoxia_bauplan.md

# Probes (validity / starvation / overcommit / ...) over a scheduler dir
uv run python tool/probe/run_probes.py <schedulers-dir>

# --- Legacy 01/02 workflow (kept runnable; not the current research direction) ---
uv run python tool/legacy/generate.py --exp reasoning --effort low --n 50
uv run python tool/legacy/analyze.py 01_reasoning
uv run python tool/legacy/plot.py 01_reasoning
uv run python tool/legacy/batch_iteration_tool.py --schedulers <dir> --trace <path> --output <dir>
```

Each script must be runnable from `summer2026/` with no extra env setup beyond
`uv sync` and `.env` for LLM calls.

## Code conventions (project-specific)

These are on top of the global rules in `~/.claude/CLAUDE.md` - don't restate them.

- **Read existing files before adding new ones.** This codebase is small enough that the answer often already exists in `tool/`. New files need a reason.
- **Per-iter outputs are sacred.** When in doubt, save more (prompt, response, raw stats, metadata) rather than less. We will lose nothing by writing a 50KB JSON; we will lose a week reproducing a result we can't introspect.
- **Scenarios are the contract.** A scenario id (e.g. `s001`) should fully determine `(train_trace, test_trace, params, sim_overrides, objective)`. If an experiment needs a knob the scenario doesn't expose, extend the scenario schema - don't pass side-channel CLI flags.
- **Subprocess for scheduler eval, no exceptions.** Generated scheduler code is untrusted Python - always run it in `tool/iterate_worker.py` style isolation with a timeout, never `exec` in-process.
- **Cost is observable.** Every LLM call must go through `llm.completion_with_retry` / `setup_cost_tracking` so `get_cost_statistics()` totals stay correct.
- **Don't silently dedupe results.** If a run is repeated, append to `results/iterations/<run_id>/...` with a new run_id; never overwrite. Resumption is opt-in via `--resume`, not the default.
- **Windows-aware I/O.** Always pass `encoding="utf-8"` to `read_text()` / `write_text()` - system locale on this machine is GBK and will silently corrupt markdown.

## When to ask vs. when to proceed

- Adding a new dependency: ask first.
- Adding a new top-level dir in `summer2026/`: ask first.
- Renaming a public function in `tool/`: ask first - analyze.py has many callers and ad-hoc scripts may exist.
- Changing the scenario schema or scenario id format: ask first.
- Touching `prompts/context/eudoxia_bauplan*.md`: ask first - this is the LLM context and small wording changes shift all downstream results.

## Status note

Phase 1 of the reorg is done (single repo, flat imports, `.env` local, dry-run validated). Phase 2 (scenario registry) is open. Phase 3 (detailed per-iter logger) landed via `tool/iterate.py` writing every artifact to `iterations/<run_id>/`. Next: enrich the diagnostics flowing into the feedback prompt (Step 1 context audit - see "Research direction" above).

## Deferred experiments / backlog

Tracked here so we don't lose ideas while focusing on Step 1.

- **Synthesis mode (multi-scheduler -> one improved).** `tool/iterate.py` currently takes a single `--scheduler`. Idea: accept `--schedulers X.py,Y.py,Z.py`, evaluate each, and feed all N + their per-scale results into one LLM call asking it to synthesize the strongest ideas. Single new function `prompts.py:get_synthesis_feedback_prompt(new_policy_key, candidates, base_params)`. ~70 lines. Decide *after* Step 1 single-scheduler context is solid.
- **Chained iteration (round-N sees rounds 1..N-1).** Not implemented despite vestigial "consider previous attempts" wording in legacy `prompts.py:get_user_request_v2*`. Would need a `--chain` arg, `iterations/<chain_id>/iter_NNN/` nesting, and history injection into the user prompt. Token cost grows linearly with chain depth. Build only when needed for the NOVAS "iterative improvement curve" experiment.
- **Dead code cleanup in `prompts.py`.** `ITERATION_SYSTEM_PROMPT` (superseded by `prompts/system_iterate.md`), `get_user_request_v2`, and `get_user_request_v2_est` are unimported. Delete when prompts.py is touched anyway.
