#!/usr/bin/env python3
"""Chained multi-round LLM scheduler improvement.

Round 1 starts from --scheduler. Each later round picks its input(s) from the
chain history via --select, builds a feedback prompt, calls the LLM, evaluates
the new scheduler, and records the round. Prior rounds are not re-evaluated;
their stored eval_out is reused, so the only sim cost per round is evaluating
that round's fresh scheduler_out.

Selection policies:
  last        round N input = round N-1 scheduler_out         (single-input)
  best        input = prior round with best valid geomean      (single-input)
  lastn-1     show ALL prior rounds 1..N-1                      (multi-candidate)
  best-k:K    show the best K prior rounds by geomean           (multi-candidate)

Width:
  --width K   sample K schedulers from the same round prompt, evaluate all K,
              and keep the candidate with the best objective-consistent score.

Usage:
  uv run python tool/iterate_chain.py --scheduler <init.py> --trace <t.csv> \\
      --chain mychain --rounds 5 --select last --width 5 --stop-on 5of5
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
SUMMER2026_DIR = TOOLS_DIR.parent
sys.path.insert(0, str(SUMMER2026_DIR))

from dotenv import load_dotenv
from tool.config import get_canonical_base_params
from tool.iterate import (
    EXPERIMENTS_DIR, next_run_index, DEFAULT_SYSTEM_PROMPT, DEFAULT_CONTEXT,
    load_system_prompt, evaluate_across_scales, geometric_mean,
    build_feedback_prompt, llm_to_code, _sha8,
)
from tool.trace_summary import summarize_trace


def _scales_ok(scale_results: dict) -> tuple[int, int]:
    ok = sum(1 for r in scale_results.values() if r.get("ok"))
    return ok, len(scale_results)


def _chain_score(scale_results: dict, penalty_s: float) -> float:
    """Objective-consistent score for ranking chain rounds.

    A failed scale is NOT dropped — dropping it would reward giving up on the
    hard scales (a 3/5 run averaging only its easy scales would outrank a
    correct 5/5 run, which is exactly the bug this replaces). Instead a failed
    scale contributes `penalty_s` (= 2 * max_job_seconds), mirroring how the
    real objective penalises incomplete pipelines. Geomean is over ALL
    requested scales, so a more-complete round always beats a fail-heavy one.
    """
    vals = [r["latency"] if r.get("ok") else penalty_s
            for r in scale_results.values()]
    return geometric_mean(vals) if vals else float("inf")


def main() -> None:
    load_dotenv(SUMMER2026_DIR / ".env")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scheduler", type=Path, required=True, help="Initial (round-1) scheduler.")
    p.add_argument("--trace", type=Path, required=True)
    p.add_argument("--chain", required=True,
                   help="Experiment name for this chain; writes experiments/<chain>/runs/<N>/.")
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--width", type=int, default=1,
                   help="Candidates sampled per round from the same prompt (default: 1).")
    p.add_argument("--select", default="last",
                   help="last | best | lastn-1 | best-k:K  (default last)")
    p.add_argument("--stop-on", default="none", choices=["none", "5of5"],
                   help="Early-stop when a round's output passes all scales.")
    p.add_argument("--scales", default="1,2,4,8,16")
    p.add_argument("--subprocess-timeout", type=int, default=None,
                   help="Override per-scale scheduler subprocess timeout in seconds.")
    p.add_argument("--model", default="gpt-5.2-2025-12-11")
    p.add_argument("--effort", default="high", choices=["none", "low", "medium", "high"])
    p.add_argument("--system-prompt", type=Path, default=DEFAULT_SYSTEM_PROMPT)
    p.add_argument("--context", type=Path, default=DEFAULT_CONTEXT)
    p.add_argument("--no-workload-summary", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args()

    assert args.scheduler.exists(), f"Not found: {args.scheduler}"
    assert args.trace.exists(), f"Not found: {args.trace}"
    import os
    assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not set"

    scales = [int(s) for s in args.scales.split(",")]
    assert args.width >= 1, "--width must be >= 1"
    base_params = get_canonical_base_params()
    if args.subprocess_timeout is not None:
        base_params["subprocess_timeout"] = args.subprocess_timeout
    system_prompt = load_system_prompt(args.system_prompt, args.context)
    workload_summary = None if args.no_workload_summary else summarize_trace(args.trace.resolve())
    trace_abs = str(args.trace.resolve())

    exp_dir = EXPERIMENTS_DIR / args.chain
    run_n = next_run_index(exp_dir)
    run_base = exp_dir / "runs" / str(run_n)
    run_base.mkdir(parents=True, exist_ok=True)
    chain_json = run_base / "chain.json"

    # history entries: {"n", "run_dir", "code", "scale_results", "geomean", "scales_ok"}
    history: list[dict] = []

    print(f"Chain     : {args.chain}")
    print(f"Initial   : {args.scheduler}")
    print(f"Trace     : {args.trace}")
    print(f"Select    : {args.select}   Rounds: {args.rounds}   Width: {args.width}   Stop: {args.stop_on}")
    print(f"Prompt    : {args.system_prompt.name} (sha {_sha8(args.system_prompt)})")
    print(f"Timeout   : {base_params.get('subprocess_timeout')}s per scale")
    print()

    def _select_inputs(n: int):
        """Return (mode, payload). mode='single' -> (code, scale_results);
        mode='multi' -> list of {label, scheduler_code, scale_results}."""
        if n == 1:
            code = args.scheduler.read_text(encoding="utf-8")
            print(f"  [round 1] evaluating initial scheduler ...")
            sr = evaluate_across_scales(args.scheduler.resolve(), trace_abs, scales, base_params)
            return "single", (code, sr)

        sel = args.select
        if sel == "last":
            h = history[-1]
            return "single", (h["code"], h["scale_results"])
        if sel == "best":
            h = min(history, key=lambda e: e["geomean"])
            return "single", (h["code"], h["scale_results"])
        if sel == "lastn-1":
            cands = [{"label": f"iter {e['n']}", "scheduler_code": e["code"],
                      "scale_results": e["scale_results"]} for e in history]
            return "multi", cands
        if sel.startswith("best-k:"):
            k = int(sel.split(":", 1)[1])
            top = sorted(history, key=lambda e: e["geomean"])[:k]
            cands = [{"label": f"iter {e['n']}", "scheduler_code": e["code"],
                      "scale_results": e["scale_results"]} for e in top]
            return "multi", cands
        raise SystemExit(f"Unknown --select: {sel}")

    for n in range(1, args.rounds + 1):
        run_dir = run_base / "iters" / str(n)
        run_dir.mkdir(parents=True, exist_ok=True)

        mode, payload = _select_inputs(n)
        if mode == "single":
            in_code, in_results = payload
            user_prompt = build_feedback_prompt(
                "single", (in_code, in_results), base_params, workload_summary)
            (run_dir / "scheduler_in.py").write_text(in_code + "\n", encoding="utf-8")
            (run_dir / "eval_in.json").write_text(
                json.dumps({str(k): v for k, v in in_results.items()}, indent=2), encoding="utf-8")
        else:
            cands = payload
            user_prompt = build_feedback_prompt(
                "multi", cands, base_params, workload_summary)
            for i, c in enumerate(cands, 1):
                (run_dir / f"scheduler_in_{i:02d}.py").write_text(
                    c["scheduler_code"] + "\n", encoding="utf-8")

        (run_dir / "prompt_system.txt").write_text(system_prompt, encoding="utf-8")
        (run_dir / "prompt_user.txt").write_text(user_prompt, encoding="utf-8")

        candidate_summaries = []
        candidates_dir = run_dir / "candidates"
        if args.width > 1:
            candidates_dir.mkdir(parents=True, exist_ok=True)

        for cidx in range(1, args.width + 1):
            cdir = candidates_dir / f"{cidx:03d}" if args.width > 1 else run_dir
            cdir.mkdir(parents=True, exist_ok=True)
            print(f"  [round {n}] candidate {cidx}/{args.width}: select={args.select} mode={mode}; calling LLM ...")
            raw, out_code = llm_to_code(
                user_prompt, system_prompt, args.model, args.effort, args.verbose)
            (cdir / "scheduler_out.py").write_text(out_code + "\n", encoding="utf-8")
            (cdir / "response_raw.txt").write_text(raw, encoding="utf-8")

            print(f"  [round {n}] candidate {cidx}/{args.width}: evaluating scheduler_out ...")
            out_results = evaluate_across_scales(
                (cdir / "scheduler_out.py").resolve(), trace_abs, scales, base_params)
            (cdir / "eval_out.json").write_text(
                json.dumps({str(k): v for k, v in out_results.items()}, indent=2), encoding="utf-8")

            gm = _chain_score(out_results, 2 * base_params["max_job_seconds"])
            ok, tot = _scales_ok(out_results)
            fails = {}
            for r in out_results.values():
                if not r.get("ok"):
                    fails[r.get("error", "?")[:50]] = fails.get(r.get("error", "?")[:50], 0) + 1
            print(f"  [round {n}] candidate {cidx}/{args.width}: score={gm:.2f}s  scales_ok={ok}/{tot}  fails={fails or 'none'}")
            candidate_summaries.append({
                "candidate": cidx,
                "dir": (f"candidates/{cidx:03d}" if args.width > 1 else "."),
                "code": out_code,
                "scale_results": out_results,
                "geomean": gm,
                "scales_ok": f"{ok}/{tot}",
                "fails": fails,
            })

        winner = min(candidate_summaries, key=lambda e: e["geomean"])
        out_code = winner["code"]
        out_results = winner["scale_results"]
        gm = winner["geomean"]
        ok, tot = _scales_ok(out_results)

        if args.width > 1:
            winner_dir = candidates_dir / f"{winner['candidate']:03d}"
            shutil.copy2(winner_dir / "scheduler_out.py", run_dir / "scheduler_out.py")
            shutil.copy2(winner_dir / "eval_out.json", run_dir / "eval_out.json")
            shutil.copy2(winner_dir / "response_raw.txt", run_dir / "response_raw.txt")
        (run_dir / "candidates.json").write_text(json.dumps([
            {
                "candidate": e["candidate"],
                "dir": e["dir"],
                "geomean": (None if e["geomean"] == float("inf") else round(e["geomean"], 4)),
                "scales_ok": e["scales_ok"],
                "fails": e["fails"],
            }
            for e in candidate_summaries
        ], indent=2), encoding="utf-8")

        print(f"  [round {n}] selected candidate {winner['candidate']}/{args.width}: score={gm:.2f}s  scales_ok={ok}/{tot}")

        history.append({"n": n, "run_dir": f"iters/{n}", "code": out_code,
                        "scale_results": out_results, "geomean": gm,
                        "scales_ok": f"{ok}/{tot}",
                        "selected_candidate": winner["candidate"]})

        chain_json.write_text(json.dumps({
            "chain": args.chain, "trace": trace_abs,
            "initial_scheduler": str(args.scheduler), "select": args.select,
            "rounds_planned": args.rounds, "width": args.width,
            "subprocess_timeout": base_params.get("subprocess_timeout"),
            "stop_on": args.stop_on,
            "system_prompt": str(args.system_prompt), "system_prompt_sha8": _sha8(args.system_prompt),
            "iters": [{"n": e["n"], "run_dir": e["run_dir"],
                       "geomean": (None if e["geomean"] == float("inf") else round(e["geomean"], 4)),
                       "scales_ok": e["scales_ok"],
                       "selected_candidate": e["selected_candidate"]} for e in history],
        }, indent=2), encoding="utf-8")

        if args.stop_on == "5of5" and ok == tot and tot > 0:
            print(f"\n  Early stop: round {n} passed all {tot} scales.")
            break

    print("\n=== Chain summary ===")
    for e in history:
        gm_s = "n/a" if e["geomean"] == float("inf") else f"{e['geomean']:.2f}s"
        print(f"  iter {e['n']:2d}  geomean={gm_s:>10s}  scales_ok={e['scales_ok']}  ({e['run_dir']})")
    print(f"\nChain run dir: {run_base}")


if __name__ == "__main__":
    main()
