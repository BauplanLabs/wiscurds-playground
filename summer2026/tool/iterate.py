#!/usr/bin/env python3
"""Single LLM-iteration on a scheduler.

Evaluates --scheduler across --scales, builds an improvement prompt, calls the
LLM, writes an improved scheduler, and saves every artifact of the run to
``experiments/<experiment>/runs/<N>/iters/1/`` so each attempt is independently
inspectable and reproducible (rendered prompts, raw response, eval data,
template copies).

Usage:
    uv run python tool/iterate.py \\
        --experiment prompt-quality \\
        --scheduler experiments/oneshot/schedulers/low/scheduler_low_001.py \\
        --trace scenarios/traces/v1/bench_canonical_train.csv

    uv run python tool/iterate.py \\
        --scheduler <path> --trace <path> --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

TOOLS_DIR = Path(__file__).resolve().parent
SUMMER2026_DIR = TOOLS_DIR.parent
WORKER_SCRIPT = TOOLS_DIR / "iterate_worker.py"
MOCK_DATA_DIR = TOOLS_DIR / "_mock"
EXPERIMENTS_DIR = SUMMER2026_DIR / "experiments"
DEFAULT_SYSTEM_PROMPT = SUMMER2026_DIR / "prompts" / "system_iterate.md"
DEFAULT_CONTEXT = SUMMER2026_DIR / "prompts" / "context" / "eudoxia_bauplan.md"

sys.path.insert(0, str(SUMMER2026_DIR))

from dotenv import load_dotenv
from tool.config import get_canonical_base_params, PROJECT_ROOT
from tool.trace_summary import summarize_trace
from prompts import get_iteration_feedback_prompt, get_multi_candidate_feedback_prompt, get_minimal_prompt
from tool.query import available_queries_text, run_query

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Prompt loading
# ---------------------------------------------------------------------------

def load_system_prompt(template_path: Path, context_path: Path) -> str:
    """Read system prompt template and inline the {context} placeholder."""
    template = template_path.read_text(encoding="utf-8")
    context = context_path.read_text(encoding="utf-8")
    return template.replace("{context}", context)


def _sha8(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Subprocess scheduler evaluation
# ---------------------------------------------------------------------------

def _run_worker_subprocess(scheduler_file: str, trace_file: str, params: dict) -> dict:
    """Evaluate one (scheduler, trace, params) triple in a subprocess for isolation."""
    cmd = [
        sys.executable,
        str(WORKER_SCRIPT),
        scheduler_file,
        trace_file,
        json.dumps(params),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=str(PROJECT_ROOT),
            timeout=params.get("subprocess_timeout", 120),
        )
        for line in reversed(proc.stdout.strip().split("\n")):
            if line.startswith("__RESULT__"):
                return json.loads(line[len("__RESULT__"):])
        stderr_snip = proc.stderr[-300:] if proc.stderr else "no stderr"
        return {"ok": False, "error": f"no result marker in output: {stderr_snip!r}"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "subprocess timeout"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def evaluate_across_scales(
    scheduler_path: Path,
    trace_file: str,
    scales: list[int],
    base_params: dict,
) -> dict[int, dict]:
    """Run the scheduler at each scale. Returns {scale: worker payload}.

    Always runs every scale even if some fail; the caller (and the LLM via the
    feedback prompt's "all failed" branch) is responsible for handling partial
    or fully-failed runs.
    """
    results = {}
    for scale in scales:
        params = base_params.copy()
        params["cpus_per_pool"] = base_params["cpus_per_pool"] * scale
        params["ram_gb_per_pool"] = base_params["ram_gb_per_pool"] * scale

        cpus = params["cpus_per_pool"]
        ram = params["ram_gb_per_pool"]
        print(f"  scale={scale}x  ({cpus} CPUs, {ram} GB RAM)...", end=" ", flush=True)
        t0 = time.time()
        result = _run_worker_subprocess(str(scheduler_path), trace_file, params)
        elapsed = time.time() - t0

        if result.get("ok"):
            tag = f"oom={result.get('oom_count', 0)}" if result.get("oom_count") else ""
            print(f"{result['latency']:.4f}s  ({elapsed:.1f}s)  {tag}".rstrip())
        else:
            print(f"FAILED: {result.get('error', '?')}  ({elapsed:.1f}s)")
        results[scale] = result
    return results


def geometric_mean(values: list[float]) -> float:
    if not values:
        return float("nan")
    return math.exp(sum(math.log(max(v, 1e-12)) for v in values) / len(values))


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def build_improvement_prompt(
    scheduler_code: str,
    scale_results: dict[int, dict],
    base_params: dict,
    workload_summary: str | None = None,
) -> str:
    m = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", scheduler_code)
    policy_key = m.group(1) if m else "unknown"
    return get_iteration_feedback_prompt(
        policy_key, scheduler_code, scale_results, base_params,
        workload_summary=workload_summary,
    )


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def call_llm(prompt: str, system_prompt: str, model: str, effort: str, verbose: bool) -> str:
    import litellm

    kwargs: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 1.0,
    }
    if effort and effort != "none":
        kwargs["reasoning_effort"] = effort

    if verbose:
        print(f"[LLM] model={model}  effort={effort}  prompt_chars={len(prompt)}")

    t0 = time.time()
    response = litellm.completion(**kwargs)
    elapsed = time.time() - t0

    content = response.choices[0].message.content or ""
    if verbose:
        print(f"[LLM] response: {len(content)} chars in {elapsed:.1f}s")
    return content


def extract_code(text: str) -> str:
    """Strip markdown code fences if present; otherwise return as-is."""
    m = re.search(r"```(?:python)?\n(.*?)```", text, re.DOTALL)
    return m.group(1).strip() if m else text.strip()


# ---------------------------------------------------------------------------
# Shared core  -  the prompt/LLM/eval primitives. tool/iterate_chain.py calls
# build_feedback_prompt + llm_to_code instead of inlining the same three calls,
# so the chain and the subcommands below cannot drift apart.
# ---------------------------------------------------------------------------

def _policy_key(code: str) -> str:
    m = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", code)
    return m.group(1) if m else "scheduler"


def build_feedback_prompt(
    mode: str, payload, base_params: dict, workload_summary: str | None = None
) -> str:
    """mode='single': payload=(code, scale_results) -> single-scheduler feedback.
    mode='multi':  payload=list[{label, scheduler_code, scale_results}]."""
    if mode == "single":
        code, scale_results = payload
        return get_iteration_feedback_prompt(
            _policy_key(code), code, scale_results, base_params,
            workload_summary=workload_summary,
        )
    if mode == "multi":
        cands = payload
        return get_multi_candidate_feedback_prompt(
            _policy_key(cands[0]["scheduler_code"]), cands, base_params,
            workload_summary=workload_summary,
        )
    raise ValueError(f"unknown mode: {mode!r}")


def llm_to_code(
    user_prompt: str, system_prompt: str, model: str, effort: str,
    verbose: bool = False,
) -> tuple[str, str]:
    """Returns (raw_response, extracted_code)."""
    raw = call_llm(user_prompt, system_prompt, model, effort, verbose)
    return raw, extract_code(raw)


# ---------------------------------------------------------------------------
# Step 2 helpers (query-driven context)
# ---------------------------------------------------------------------------

_QUERY_REQUEST_SUFFIX = """

---

## What additional context would help you?

Before generating the improved scheduler, declare any additional workload data you want to see.

Available queries:

{available_queries}

Reply with ONLY a JSON object (no explanation, no code):
{{"queries": [{{"name": "query_name", "args": {{...}}}}, ...]}}

Request any number of queries. To skip: {{"queries": []}}
"""


def _parse_query_response(raw: str) -> list[dict]:
    """Extract query list from LLM Phase-1 JSON response. Returns [] on failure."""
    text = re.sub(r"```(?:json)?|```", "", raw).strip()
    start = text.find("{")
    if start == -1:
        return []
    depth = 0
    for i, c in enumerate(text[start:], start):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    data = json.loads(text[start : i + 1])
                    return [q for q in data.get("queries", []) if isinstance(q, dict)]
                except Exception:
                    return []
    return []


# ---------------------------------------------------------------------------
# Subcommand layer  -  each step of an iteration as a standalone command that
# shares one --output-dir as its unit of state. A run dir's public interface
# (what a later round consumes) is scheduler_out.py + eval_out.json.
# ---------------------------------------------------------------------------

def _read_scale_results(eval_json: Path) -> dict[int, dict]:
    data = json.loads(eval_json.read_text(encoding="utf-8"))
    sr = data["scale_results"] if "scale_results" in data else data
    return {int(k): v for k, v in sr.items()}


def cmd_evaluate(
    out_dir: Path, trace: Path, scales: list[int], base_params: dict,
    scheduler: Path | None = None,
) -> dict[int, dict]:
    """Evaluate out_dir/scheduler_out.py (or copy --scheduler in first to
    bootstrap a seed dir) -> out_dir/eval_out.json. Returns scale_results."""
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "scheduler_out.py"
    if scheduler is not None:
        target.write_text(scheduler.read_text(encoding="utf-8"), encoding="utf-8")
    assert target.exists(), f"No scheduler to evaluate: {target}"

    results = evaluate_across_scales(target.resolve(), str(trace.resolve()),
                                     scales, base_params)
    (out_dir / "eval_out.json").write_text(
        json.dumps(_eval_payload(trace, scales, base_params, results), indent=2),
        encoding="utf-8",
    )
    return results


def cmd_genprompt(
    out_dir: Path, input_dirs: list[Path], trace: Path, base_params: dict,
    system_prompt_path: Path, context_path: Path,
    no_workload_summary: bool = False,
) -> None:
    """Build the feedback prompt for out_dir from prior run dirs' public
    interface (scheduler_out.py + eval_out.json). One input -> single-scheduler
    feedback; several -> multi-candidate. Writes prompt_{user,system}.txt plus
    scheduler_in*/eval_in* provenance copies."""
    out_dir.mkdir(parents=True, exist_ok=True)
    assert input_dirs, "genprompt needs at least one --input-dirs entry"
    ws = None if no_workload_summary else summarize_trace(trace.resolve())
    system_prompt = load_system_prompt(system_prompt_path, context_path)

    if len(input_dirs) == 1:
        d = input_dirs[0]
        code = (d / "scheduler_out.py").read_text(encoding="utf-8")
        sr = _read_scale_results(d / "eval_out.json")
        user_prompt = build_feedback_prompt("single", (code, sr), base_params, ws)
        (out_dir / "scheduler_in.py").write_text(code + "\n", encoding="utf-8")
        (out_dir / "eval_in.json").write_text(
            json.dumps({str(k): v for k, v in sr.items()}, indent=2),
            encoding="utf-8")
    else:
        cands = []
        for i, d in enumerate(input_dirs, 1):
            code = (d / "scheduler_out.py").read_text(encoding="utf-8")
            sr = _read_scale_results(d / "eval_out.json")
            cands.append({"label": f"input {i} ({d.name})",
                          "scheduler_code": code, "scale_results": sr})
            (out_dir / f"scheduler_in_{i:02d}.py").write_text(
                code + "\n", encoding="utf-8")
            (out_dir / f"eval_in_{i:02d}.json").write_text(
                json.dumps({str(k): v for k, v in sr.items()}, indent=2),
                encoding="utf-8")
        user_prompt = build_feedback_prompt("multi", cands, base_params, ws)

    (out_dir / "prompt_user.txt").write_text(user_prompt, encoding="utf-8")
    (out_dir / "prompt_system.txt").write_text(system_prompt, encoding="utf-8")


def cmd_gensched(
    out_dir: Path, model: str, effort: str, verbose: bool = False
) -> str:
    """Read out_dir/prompt_{user,system}.txt -> LLM -> scheduler_out.py."""
    user_prompt = (out_dir / "prompt_user.txt").read_text(encoding="utf-8")
    system_prompt = (out_dir / "prompt_system.txt").read_text(encoding="utf-8")
    raw, code = llm_to_code(user_prompt, system_prompt, model, effort, verbose)
    (out_dir / "response_raw.txt").write_text(raw, encoding="utf-8")
    (out_dir / "scheduler_out.py").write_text(code + "\n", encoding="utf-8")
    return code


def cmd_nextiter(
    out_dir: Path, input_dirs: list[Path], trace: Path, scales: list[int],
    base_params: dict, system_prompt_path: Path, context_path: Path,
    model: str, effort: str, no_workload_summary: bool = False,
    verbose: bool = False,
) -> dict[int, dict]:
    """One full round: genprompt -> gensched -> evaluate, in dependency order
    (input dirs must already carry their own eval_out.json)."""
    cmd_genprompt(out_dir, input_dirs, trace, base_params,
                  system_prompt_path, context_path, no_workload_summary)
    cmd_gensched(out_dir, model, effort, verbose)
    return cmd_evaluate(out_dir, trace, scales, base_params)


def cmd_query(
    out_dir: Path, input_dirs: list[Path], trace: Path, base_params: dict,
    system_prompt_path: Path, context_path: Path,
    model: str, effort: str,
    no_workload_summary: bool = False,
    verbose: bool = False,
) -> None:
    """Step 2 two-phase prompt building.

    Phase 1: LLM sees current performance and declares which queries it wants.
    Queries are fetched from the trace / eval data.
    Phase 2: query results are appended to the normal feedback prompt.

    Writes the same outputs as genprompt (prompt_user.txt, prompt_system.txt,
    scheduler_in.py, eval_in.json) so gensched can follow unchanged, plus
    query artifacts (query_request_prompt.txt, query_request_response.txt,
    queries_resolved.json).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    assert len(input_dirs) == 1, "queryctx supports exactly one --input-dirs entry"

    d = input_dirs[0]
    code = (d / "scheduler_out.py").read_text(encoding="utf-8")
    sr = _read_scale_results(d / "eval_out.json")
    eval_data = json.loads((d / "eval_out.json").read_text(encoding="utf-8"))

    system_prompt = load_system_prompt(system_prompt_path, context_path)
    minimal_body = get_minimal_prompt(_policy_key(code), code, sr, base_params)

    # Phase 1: LLM sees only the score + available queries, declares what it needs
    p1_user = minimal_body + _QUERY_REQUEST_SUFFIX.format(
        available_queries=available_queries_text()
    )
    p1_system = (
        context_path.read_text(encoding="utf-8")
        + "\n\nYou are a systems expert reviewing scheduler performance. "
        "Reply ONLY with a JSON object — no explanation, no code."
    )
    if verbose:
        print("[query] Phase 1: requesting context from LLM ...")
    raw_p1 = call_llm(p1_user, p1_system, model, effort, verbose)
    (out_dir / "query_request_prompt.txt").write_text(p1_user, encoding="utf-8")
    (out_dir / "query_request_response.txt").write_text(raw_p1, encoding="utf-8")

    # Resolve queries
    requested = _parse_query_response(raw_p1)
    resolved = [
        {
            "name": req.get("name", ""),
            "args": req.get("args", {}),
            "result": run_query(
                req.get("name", ""), req.get("args", {}), trace.resolve(), eval_data
            ),
        }
        for req in requested
    ]
    (out_dir / "queries_resolved.json").write_text(
        json.dumps(resolved, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if verbose:
        print(f"[query] Resolved {len(resolved)} queries: {[r['name'] for r in resolved]}")

    # Phase 2: minimal body + query results + produce instruction
    policy_key = _policy_key(code)
    query_section = ""
    if resolved:
        parts = [r["result"] for r in resolved]
        query_section = "\n\n## Additional Context (requested)\n\n" + "\n\n".join(parts)

    produce = (
        f"\n\nProduce an improved version of this scheduler that reduces the geometric mean "
        f"of adjusted latency across all cluster sizes.\n\n"
        f'Use the EXACT key "{policy_key}" in both @register_scheduler_init and '
        f"@register_scheduler decorators.\nOutput ONLY the complete Python code."
    )

    (out_dir / "prompt_user.txt").write_text(minimal_body + query_section + produce, encoding="utf-8")
    (out_dir / "prompt_system.txt").write_text(system_prompt, encoding="utf-8")
    (out_dir / "scheduler_in.py").write_text(code + "\n", encoding="utf-8")
    (out_dir / "eval_in.json").write_text(
        json.dumps({str(k): v for k, v in sr.items()}, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Run artifact saving
# ---------------------------------------------------------------------------

def next_run_index(experiment_dir: Path) -> int:
    """Next 1-based run number under experiments/<experiment>/runs/. Re-running the
    same experiment name yields runs/2, runs/3, ... (e.g. variance studies)."""
    runs = experiment_dir / "runs"
    existing = [int(p.name) for p in runs.iterdir()
                if p.is_dir() and p.name.isdigit()] if runs.is_dir() else []
    return max(existing, default=0) + 1


def _eval_payload(trace_path: Path, scales: list[int], base_params: dict, scale_results: dict[int, dict]) -> dict:
    return {
        "trace": str(trace_path),
        "scales": scales,
        "base_params": {
            "cpus_per_pool": base_params.get("cpus_per_pool"),
            "ram_gb_per_pool": base_params.get("ram_gb_per_pool"),
            "duration": base_params.get("duration"),
            "ticks_per_second": base_params.get("ticks_per_second"),
            "max_job_seconds": base_params.get("max_job_seconds"),
        },
        "scale_results": {str(k): v for k, v in scale_results.items()},
    }


def save_run_artifacts(
    run_dir: Path,
    *,
    scheduler_in: Path,
    scheduler_out_code: str,
    trace_path: Path,
    scales: list[int],
    base_params: dict,
    scale_results: dict[int, dict],
    out_scale_results: dict[int, dict] | None,
    system_prompt_template: Path,
    context_path: Path,
    system_prompt_rendered: str,
    user_prompt_rendered: str,
    response_raw: str,
    model: str,
    effort: str,
    dry_run: bool,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "scheduler_in.py").write_text(
        scheduler_in.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (run_dir / "scheduler_out.py").write_text(scheduler_out_code + "\n", encoding="utf-8")
    (run_dir / "prompt_system.txt").write_text(system_prompt_rendered, encoding="utf-8")
    (run_dir / "prompt_user.txt").write_text(user_prompt_rendered, encoding="utf-8")
    (run_dir / "response_raw.txt").write_text(response_raw, encoding="utf-8")
    shutil.copy2(system_prompt_template, run_dir / "system_iterate.template.md")
    shutil.copy2(context_path, run_dir / "context.template.md")

    (run_dir / "eval_in.json").write_text(
        json.dumps(_eval_payload(trace_path, scales, base_params, scale_results), indent=2),
        encoding="utf-8",
    )
    if out_scale_results is not None:
        (run_dir / "eval_out.json").write_text(
            json.dumps(_eval_payload(trace_path, scales, base_params, out_scale_results), indent=2),
            encoding="utf-8",
        )

    meta = {
        "run_id": run_dir.name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "scheduler_in": str(scheduler_in),
        "trace": str(trace_path),
        "scales": scales,
        "model": model,
        "effort": effort,
        "dry_run": dry_run,
        "has_eval_out": out_scale_results is not None,
        "system_prompt_template": str(system_prompt_template),
        "system_prompt_template_sha8": _sha8(system_prompt_template),
        "context_path": str(context_path),
        "context_sha8": _sha8(context_path),
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _legacy_main() -> None:
    load_dotenv(SUMMER2026_DIR / ".env")

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scheduler", type=Path, required=True,
                        help="Scheduler .py file to improve.")
    parser.add_argument("--trace", type=Path, required=True,
                        help="Fixed trace file (.csv) to evaluate on.")
    parser.add_argument("--experiment", "--exp", dest="experiment", default="prompt-quality",
                        help="Experiment name this run belongs to (default: prompt-quality). "
                             "Writes experiments/<experiment>/runs/<N>/iters/1/. "
                             "--exp is kept as a compatibility alias.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output path for improved scheduler. "
                             "Default: experiments/<experiment>/runs/<N>/iters/1/scheduler_out.py")
    parser.add_argument("--scales", default="1,2,4,8,16",
                        help="Comma-separated cluster scale multipliers (default: 1,2,4,8,16).")
    parser.add_argument("--model", default="gpt-5.2-2025-12-11",
                        help="LLM model (default: gpt-5.2-2025-12-11).")
    parser.add_argument("--effort", default="high", choices=["none", "low", "medium", "high"],
                        help="Reasoning effort level (default: high).")
    parser.add_argument("--system-prompt", type=Path, default=DEFAULT_SYSTEM_PROMPT,
                        help=f"System prompt template (default: {DEFAULT_SYSTEM_PROMPT.relative_to(SUMMER2026_DIR)}).")
    parser.add_argument("--context", type=Path, default=DEFAULT_CONTEXT,
                        help=f"Context markdown filled into the {{context}} placeholder "
                             f"(default: {DEFAULT_CONTEXT.relative_to(SUMMER2026_DIR)}).")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-workload-summary", action="store_true",
                        help="Ablation: omit the auto-derived workload summary from the user prompt.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip simulator and LLM; use random latencies and copy a mock scheduler as output.")
    args = parser.parse_args()

    assert args.scheduler.exists(), f"Scheduler not found: {args.scheduler}"
    assert args.trace.exists(), f"Trace not found: {args.trace}"
    assert args.system_prompt.exists(), f"System prompt not found: {args.system_prompt}"
    assert args.context.exists(), f"Context not found: {args.context}"
    if not args.dry_run:
        assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"

    scales = [int(s.strip()) for s in args.scales.split(",")]
    assert scales, "Need at least one scale."

    experiment_dir = EXPERIMENTS_DIR / args.experiment
    run_n = next_run_index(experiment_dir)
    run_dir = experiment_dir / "runs" / str(run_n) / "iters" / "1"
    run_id = f"{args.experiment}/runs/{run_n}/iters/1"
    output_path = args.output or (run_dir / "scheduler_out.py")
    base_params = get_canonical_base_params()
    system_prompt = load_system_prompt(args.system_prompt, args.context)

    print(f"Run id    : {run_id}")
    print(f"Scheduler : {args.scheduler}")
    print(f"Trace     : {args.trace}")
    print(f"Scales    : {scales}  (base {base_params['cpus_per_pool']} CPUs / {base_params['ram_gb_per_pool']} GB)")
    print(f"Prompts   : {args.system_prompt.relative_to(SUMMER2026_DIR)} + {args.context.relative_to(SUMMER2026_DIR)}")
    print(f"Output    : {output_path}")
    print()

    # 1. Evaluate across scales
    print("=== Evaluating across cluster sizes ===")
    if args.dry_run:
        import random
        rng = random.Random(42)
        scale_results = {
            scale: {"ok": True, "latency": round(rng.uniform(100, 300), 4),
                    "failures": 0, "suspensions": 0, "oom_count": 0}
            for scale in scales
        }
        for scale, r in scale_results.items():
            cpus = base_params["cpus_per_pool"] * scale
            ram = base_params["ram_gb_per_pool"] * scale
            print(f"  scale={scale}x  ({cpus} CPUs, {ram} GB RAM)  {r['latency']:.4f}s  (dry-run)")
    else:
        scale_results = evaluate_across_scales(
            scheduler_path=args.scheduler.resolve(),
            trace_file=str(args.trace.resolve()),
            scales=scales,
            base_params=base_params,
        )

    valid_latencies = [r["latency"] for r in scale_results.values() if r.get("ok")]
    if valid_latencies:
        gm = geometric_mean(valid_latencies)
        print(f"\nGeometric mean ({len(valid_latencies)}/{len(scales)} runs): {gm:.4f}s")
    else:
        print("\nWARNING: all runs failed  -  LLM will still attempt improvement.")

    # 2. Build prompt and call LLM
    print("\n=== Calling LLM for improvement ===")
    scheduler_code = args.scheduler.read_text(encoding="utf-8")
    workload_summary = None if args.no_workload_summary else summarize_trace(args.trace.resolve())
    user_prompt = build_improvement_prompt(
        scheduler_code, scale_results, base_params, workload_summary=workload_summary,
    )

    if args.dry_run:
        mock_schedulers = sorted(MOCK_DATA_DIR.glob("scheduler_*.py"))
        assert mock_schedulers, f"No scheduler_*.py found in {MOCK_DATA_DIR}"
        mock_path = mock_schedulers[0]
        print(f"  (dry-run: skipping LLM call, using {mock_path.name} as improved scheduler)")
        response_raw = mock_path.read_text(encoding="utf-8")
        improved_code = response_raw
    else:
        response_raw = call_llm(user_prompt, system_prompt, args.model, args.effort, args.verbose)
        improved_code = extract_code(response_raw)

    # 3. Write scheduler_out
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(improved_code + "\n", encoding="utf-8")

    # 4. Evaluate scheduler_out on the same trace + scales (skipped on dry-run)
    out_scale_results = None
    if not args.dry_run:
        print("\n=== Evaluating improved scheduler ===")
        out_scale_results = evaluate_across_scales(
            scheduler_path=output_path.resolve(),
            trace_file=str(args.trace.resolve()),
            scales=scales,
            base_params=base_params,
        )

        # Headline in/out comparison
        in_valid = [r["latency"] for r in scale_results.values() if r.get("ok")]
        out_valid = [r["latency"] for r in out_scale_results.values() if r.get("ok")]
        in_gm = geometric_mean(in_valid) if in_valid else float("nan")
        out_gm = geometric_mean(out_valid) if out_valid else float("nan")
        print(f"\n=== In/Out summary ===")
        print(f"  scheduler_in  geomean: {in_gm:>10.2f}s  ({len(in_valid)}/{len(scales)} scales ok)")
        print(f"  scheduler_out geomean: {out_gm:>10.2f}s  ({len(out_valid)}/{len(scales)} scales ok)")
        if in_valid and out_valid:
            delta_pct = (out_gm - in_gm) / in_gm * 100
            verdict = "BETTER" if delta_pct < 0 else "WORSE"
            print(f"  delta:                {delta_pct:>+9.1f}%  ({verdict})")

    # 5. Save artifacts
    save_run_artifacts(
        run_dir,
        scheduler_in=args.scheduler,
        scheduler_out_code=improved_code,
        trace_path=args.trace,
        scales=scales,
        base_params=base_params,
        scale_results=scale_results,
        out_scale_results=out_scale_results,
        system_prompt_template=args.system_prompt,
        context_path=args.context,
        system_prompt_rendered=system_prompt,
        user_prompt_rendered=user_prompt,
        response_raw=response_raw,
        model=args.model,
        effort=args.effort,
        dry_run=args.dry_run,
    )

    print(f"\nImproved scheduler : {output_path}  ({len(improved_code.splitlines())} lines)")
    print(f"Run artifacts      : {run_dir}")


def _csv_ints(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _dirs(s: str) -> list[Path]:
    return [Path(x.strip()) for x in s.split(",") if x.strip()]


_SUBCOMMANDS = {"evaluate", "genprompt", "gensched", "nextiter", "query"}


def main() -> None:
    """Dispatch: a subcommand runs the decomposed step API; anything else
    (no subcommand / leading flags) falls back to the original single-shot
    flow so existing `iterate.py --scheduler ... --trace ...` keeps working."""
    argv = sys.argv[1:]
    if not (argv and argv[0] in _SUBCOMMANDS):
        _legacy_main()
        return

    load_dotenv(SUMMER2026_DIR / ".env")
    p = argparse.ArgumentParser(prog="iterate.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    def _common(sp, *, needs_inputs=False, needs_llm=False):
        sp.add_argument("--output-dir", type=Path, required=True)
        sp.add_argument("--trace", type=Path)
        sp.add_argument("--scales", type=_csv_ints, default=_csv_ints("1,2,4,8,16"))
        if needs_inputs:
            sp.add_argument("--input-dirs", type=_dirs, required=True,
                            help="Comma-separated prior run dirs (each with "
                                 "scheduler_out.py + eval_out.json).")
            sp.add_argument("--system-prompt", type=Path, default=DEFAULT_SYSTEM_PROMPT)
            sp.add_argument("--context", type=Path, default=DEFAULT_CONTEXT)
            sp.add_argument("--no-workload-summary", action="store_true")
        if needs_llm:
            sp.add_argument("--model", default="gpt-5.2-2025-12-11")
            sp.add_argument("--effort", default="high",
                            choices=["none", "low", "medium", "high"])
            sp.add_argument("--verbose", "-v", action="store_true")

    se = sub.add_parser("evaluate", help="scheduler_out.py -> eval_out.json")
    _common(se)
    se.add_argument("--scheduler", type=Path, default=None,
                    help="Optional seed to copy in as scheduler_out.py first.")

    sg = sub.add_parser("genprompt", help="input dirs -> prompt_*.txt")
    _common(sg, needs_inputs=True)

    sgs = sub.add_parser("gensched", help="prompt_*.txt -> scheduler_out.py")
    _common(sgs, needs_llm=True)

    sn = sub.add_parser("nextiter", help="genprompt + gensched + evaluate")
    _common(sn, needs_inputs=True, needs_llm=True)

    sq = sub.add_parser("query",
                        help="Step 2: Phase-1 LLM context request -> queries -> prompt")
    _common(sq, needs_inputs=True, needs_llm=True)

    a = p.parse_args(argv)
    base_params = get_canonical_base_params()

    if a.cmd == "evaluate":
        assert a.trace, "evaluate needs --trace"
        cmd_evaluate(a.output_dir, a.trace, a.scales, base_params, a.scheduler)
    elif a.cmd == "genprompt":
        assert a.trace, "genprompt needs --trace"
        cmd_genprompt(a.output_dir, a.input_dirs, a.trace, base_params,
                      a.system_prompt, a.context, a.no_workload_summary)
    elif a.cmd == "gensched":
        cmd_gensched(a.output_dir, a.model, a.effort, a.verbose)
    elif a.cmd == "nextiter":
        assert a.trace, "nextiter needs --trace"
        cmd_nextiter(a.output_dir, a.input_dirs, a.trace, a.scales, base_params,
                     a.system_prompt, a.context, a.model, a.effort,
                     a.no_workload_summary, a.verbose)
    elif a.cmd == "query":
        assert a.trace, "query needs --trace"
        cmd_query(a.output_dir, a.input_dirs, a.trace, base_params,
                  a.system_prompt, a.context, a.model, a.effort,
                  a.no_workload_summary, a.verbose)
    print(f"[{a.cmd}] done -> {a.output_dir}")


if __name__ == "__main__":
    main()
