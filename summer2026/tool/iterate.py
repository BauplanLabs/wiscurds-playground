#!/usr/bin/env python3
"""Single LLM-iteration on a scheduler.

Evaluates --scheduler across --scales, builds an improvement prompt, calls the
LLM, writes an improved scheduler, and saves every artifact of the run to
``iterations/<run_id>/`` so each attempt is independently inspectable and
reproducible (rendered prompts, raw response, eval data, template copies).

Usage:
    uv run python tool/iterate.py \\
        --scheduler schedulers/reasoning/low/scheduler_001.py \\
        --trace traces/bench_canonical_train.csv

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
ITERATIONS_DIR = SUMMER2026_DIR / "iterations"
DEFAULT_SYSTEM_PROMPT = SUMMER2026_DIR / "prompts" / "system_iterate.md"
DEFAULT_CONTEXT = SUMMER2026_DIR / "prompts" / "context" / "eudoxia_bauplan.md"

sys.path.insert(0, str(SUMMER2026_DIR))

from dotenv import load_dotenv
from tool.config import get_canonical_base_params, PROJECT_ROOT
from prompts import get_iteration_feedback_prompt

os.environ.setdefault("LITELLM_LOG", "ERROR")
logging.getLogger("eudoxia").setLevel(logging.CRITICAL)
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
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
    """Run the scheduler at each scale. Returns {scale: worker payload}."""
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
) -> str:
    m = re.search(r"""@register_scheduler\((?:key=)?['"]([^'"]+)['"]\)""", scheduler_code)
    policy_key = m.group(1) if m else "unknown"
    return get_iteration_feedback_prompt(policy_key, scheduler_code, scale_results, base_params)


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
# Run artifact saving
# ---------------------------------------------------------------------------

def make_run_id(scheduler_path: Path, trace_path: Path) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{scheduler_path.stem}_{trace_path.stem}"


def save_run_artifacts(
    run_dir: Path,
    *,
    scheduler_in: Path,
    scheduler_out_code: str,
    trace_path: Path,
    scales: list[int],
    base_params: dict,
    scale_results: dict[int, dict],
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

    eval_in = {
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
    (run_dir / "eval_in.json").write_text(json.dumps(eval_in, indent=2), encoding="utf-8")

    meta = {
        "run_id": run_dir.name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "scheduler_in": str(scheduler_in),
        "trace": str(trace_path),
        "scales": scales,
        "model": model,
        "effort": effort,
        "dry_run": dry_run,
        "system_prompt_template": str(system_prompt_template),
        "system_prompt_template_sha8": _sha8(system_prompt_template),
        "context_path": str(context_path),
        "context_sha8": _sha8(context_path),
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv(SUMMER2026_DIR / ".env")

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--scheduler", type=Path, required=True,
                        help="Scheduler .py file to improve.")
    parser.add_argument("--trace", type=Path, required=True,
                        help="Fixed trace file (.csv) to evaluate on.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output path for improved scheduler. "
                             "Default: iterations/<run_id>/scheduler_out.py")
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

    run_id = make_run_id(args.scheduler, args.trace)
    run_dir = ITERATIONS_DIR / run_id
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
    user_prompt = build_improvement_prompt(scheduler_code, scale_results, base_params)

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

    # 3. Write output + run artifacts
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(improved_code + "\n", encoding="utf-8")

    save_run_artifacts(
        run_dir,
        scheduler_in=args.scheduler,
        scheduler_out_code=improved_code,
        trace_path=args.trace,
        scales=scales,
        base_params=base_params,
        scale_results=scale_results,
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


if __name__ == "__main__":
    main()
