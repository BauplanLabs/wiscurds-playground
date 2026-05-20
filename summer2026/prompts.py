# System prompt template for policy generation
POLICY_GENERATION_SYSTEM_PROMPT = """{context}

# Instructions

Based on the above context about the Eudoxia simulator and Bauplan scheduling, generate a new scheduling policy.

Your response should be ONLY valid Python code that:
1. Uses the @register_scheduler_init decorator with the EXACT key provided in the user request (do NOT generate your own key)
2. Uses the @register_scheduler decorator with the SAME EXACT key provided in the user request
3. Implements the init function to set up any necessary state on the scheduler object `s`
4. Implements the scheduler function with the correct signature: (s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]
5. Returns proper suspensions and assignments lists
6. Follow the access patterns from the examples, especially for accessing executor pools and creating Assignment objects

Available classes and their usage based on the examples:
- Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE - priority levels
- Assignment(ops=op_list, cpu=cpu_amount, ram=ram_amount, priority=priority, pool_id=pool_id, pipeline_id=pipeline.pipeline_id) - to create assignments
- Suspend(container_id, pool_id) - to create suspensions
- s.executor.num_pools - number of available pools
- s.executor.pools[i].avail_cpu_pool - available CPU in pool i
- s.executor.pools[i].avail_ram_pool - available RAM in pool i
- s.executor.pools[i].max_cpu_pool - max CPU in pool i
- s.executor.pools[i].max_ram_pool - max RAM in pool i
- Pipeline has .priority, .pipeline_id, .values (DAG of operators), and .runtime_status() method
- ExecutionResult has .priority, .ops, .container_id, .pool_id, .ram, .cpu, .error, and .failed() method
- pipeline.runtime_status().get_ops(states, require_parents_complete=True/False) - get operators matching criteria
- pipeline.runtime_status().is_pipeline_successful() - returns True if all operators completed
- OperatorState enum - PENDING, ASSIGNED, RUNNING, SUSPENDING, COMPLETED, FAILED (ASSIGNABLE_STATES lists states that can transition to ASSIGNED: PENDING and FAILED)

Do NOT include any explanations, markdown formatting, or import statements. Return ONLY the code for the policy functions, but rememember to add comments inside the Python code itself to clearly explain the logic of your policy, and use the function docstring
to give an overview of the main idea as if it were a standard functin comment. If you make use of any new helper functions, define them within the same code block and make sure to add the relevant
import statements inside the function itself, should they be needed.

Do NOT generate your own poliy key - you MUST use exactly what is provided in the user request.
"""

# Superseded by prompts/system_iterate.md — kept for reference
# ITERATION_SYSTEM_PROMPT = """{context} ...full system prompt... """


def get_minimal_prompt(
    policy_key: str,
    scheduler_code: str,
    scale_results: dict,
    base_params: dict,
) -> str:
    """Objective + per-scale latency only — no per-priority, memory%, or failure details.

    Phase 1 base for the Step 2 query flow: LLM sees only the score, then
    declares what additional context it needs via query functions.
    Returns the prompt body without a trailing produce instruction so the caller
    can append query results before adding it.
    """
    import math

    rows = []
    valid_latencies: list[float] = []
    for scale in sorted(scale_results):
        r = scale_results[scale]
        cpus = base_params["cpus_per_pool"] * scale
        ram = base_params["ram_gb_per_pool"] * scale
        if r.get("ok"):
            lat = r["latency"]
            valid_latencies.append(lat)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  adj={lat:.4f}s")
        else:
            rows.append(
                f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  "
                f"FAILED: {r.get('error', 'unknown error')}"
            )

    perf = "\n".join(rows)
    if valid_latencies:
        gm = math.exp(sum(math.log(max(v, 1e-12)) for v in valid_latencies) / len(valid_latencies))
        gm_line = f"Geometric mean: {gm:.4f}s ({len(valid_latencies)}/{len(scale_results)} scales ok)"
    else:
        gm_line = f"ALL {len(scale_results)} RUNS FAILED"

    return f"""\
## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 x max_job_seconds (e.g. 720s if max_job_seconds=360)

Weighted average:
  score = (sum query_latency x 10 + sum interactive_latency x 5 + sum batch_latency x 1)
        / (query_arrivals x 10 + interactive_arrivals x 5 + batch_arrivals x 1)

## Performance Results

{perf}

{gm_line}

## Current Scheduler Code

{scheduler_code}"""


def get_user_request(policy_key: str, metric: str) -> str:
    """Generate user request with the provided policy key.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
        metric: The metric to focus on improving in the policy
    Returns:
        The formatted user request string
    """
    return f"""
Starting from the naive policy provided as example, try to improve it by focusing on improving {metric}, for example leveraging the concept of priority.
Start with small improvements first, targeting obvious flaws, and make the policy complex gradually, only after you
have working code and a direction for improvement. Make sure to consider the results of previous attempts and the feedback provided
as you generate a new policy.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()


def get_user_request_v2_est(policy_key: str) -> str:
    """get_user_request_v2 with estimator context added.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
    Returns:
        The formatted user request string
    """
    return f"""
Design a scheduling policy that minimizes the following objective (lower is better):

## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 x max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency x 10  +  sum of interactive_latency x 5  +  sum of batch_latency x 1)
          / (query_arrivals x 10  +  interactive_arrivals x 5  +  batch_arrivals x 1)

Priority weights: query=10x, interactive=5x, batch=1x.
Incomplete and failed pipelines directly inflate the score through the penalty term  -  they are not ignored.
Aim for high completion rate alongside low latency.

## Estimator

Each operator has an `op.estimate.mem_peak_gb` field (float or None).
This is a pre-computed estimate of the operator's peak memory usage in GB.
It may be None if the estimator has not yet run for that operator  -  always check before using.

You can use this estimate to make smarter memory-aware scheduling decisions, for example:
  - Skip assigning an operator if `op.estimate.mem_peak_gb` exceeds available RAM in all pools
  - Prefer pools where the estimated memory fits without causing OOM
  - Prioritize operators with smaller memory footprint when resources are tight

Note: the estimate is noisy  -  treat it as a hint, not a guarantee.

## Guidelines

- Start with small improvements over the naive FIFO baseline.
- Protect query and interactive latency; they dominate the score.
- Avoid designs that drop or starve pipelines  -  each failure adds 720s to the weighted sum.
- Use `op.estimate.mem_peak_gb` to reduce OOM failures and improve memory utilization.
- Make the policy complex gradually, only after you have working code.
- Make sure to consider the results of previous attempts and the feedback provided.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()


def get_user_request_v2(policy_key: str) -> str:
    """Generate user request with explicit weighted-latency objective.

    Args:
        policy_key: The policy key that the LLM should use in @register_scheduler decorators
    Returns:
        The formatted user request string
    """
    return f"""
Design a scheduling policy that minimizes the following objective (lower is better):

## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 x max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency x 10  +  sum of interactive_latency x 5  +  sum of batch_latency x 1)
          / (query_arrivals x 10  +  interactive_arrivals x 5  +  batch_arrivals x 1)

Priority weights: query=10x, interactive=5x, batch=1x.
Incomplete and failed pipelines directly inflate the score through the penalty term  -  they are not ignored.
Aim for high completion rate alongside low latency.

## Guidelines

- Start with small improvements over the naive FIFO baseline.
- Protect query and interactive latency; they dominate the score.
- Avoid designs that drop or starve pipelines  -  each failure adds 720s to the weighted sum.
- Make the policy complex gradually, only after you have working code.
- Make sure to consider the results of previous attempts and the feedback provided.

IMPORTANT: Use the following EXACT key in both @register_scheduler_init and @register_scheduler decorators: "{policy_key}"
Do NOT generate your own key - you MUST use exactly: "{policy_key}"
""".strip()

def get_iteration_feedback_prompt(
    policy_key: str,
    scheduler_code: str,
    scale_results: dict,
    base_params: dict,
    workload_summary: str | None = None,
) -> str:
    """Build the user-turn feedback prompt for one iteration of LLM improvement.

    Combines the weighted-latency objective function with the performance table
    from the current scheduler run. Optionally prepends a workload summary
    (markdown produced by tool/trace_summary.py). Handles the all-failed case
    separately.
    """
    import math

    rows = []
    valid_latencies: list[float] = []
    errors: list[str] = []
    def _lat(v):
        return f"{v:.2f}s" if v is not None else "n/a"
    def _pct(v):
        return f"{v:.1f}%" if isinstance(v, (int, float)) else "n/a"

    for scale in sorted(scale_results):
        r = scale_results[scale]
        cpus = base_params["cpus_per_pool"] * scale
        ram = base_params["ram_gb_per_pool"] * scale
        if r.get("ok"):
            lat = r["latency"]
            valid_latencies.append(lat)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  adj={lat:.4f}s")

            # Failure type breakdown (OOM vs timeout vs other)  -  tells the LLM *why* things failed
            fec = r.get("failure_error_counts") or {}
            if fec:
                fec_str = "  ".join(f"{k}={v}" for k, v in sorted(fec.items()))
                rows.append(f"       failures: {fec_str}")
            else:
                rows.append(f"       failures: {r.get('failures', 0)} total (no type breakdown available)")

            # Memory utilization  -  distinguishes over-allocation (alloc high, consumed low) from real pressure
            mem_alloc = r.get("mean_memory_allocated_percent")
            mem_cons = r.get("mean_memory_consumed_percent")
            if mem_alloc is not None or mem_cons is not None:
                rows.append(f"       memory: allocated={_pct(mem_alloc)}  consumed={_pct(mem_cons)}")

            # Throughput: assignments/completions/suspensions in one line
            ass = r.get("assignments")
            comp_c = r.get("containers_completed")
            susp = r.get("suspensions", 0)
            if ass is not None or comp_c is not None:
                rows.append(f"       assignments={ass}  containers_completed={comp_c}  suspensions={susp}")

            # Per-priority outcomes
            for prio in ("query", "interactive", "batch"):
                ps = r.get(prio)
                if ps:
                    arr, comp = ps["arrived"], ps["completed"]
                    rows.append(f"       {prio:<13s} {comp:4d}/{arr:4d} completed"
                                f"  mean={_lat(ps['mean_s'])}  p99={_lat(ps['p99_s'])}")
        else:
            err = r.get("error", "unknown error")
            errors.append(err)
            rows.append(f"  {scale:2d}x  ({cpus:5d} CPUs, {ram:6d} GB RAM)  FAILED: {err}")

    perf_table = "\n".join(rows)
    n_valid = len(valid_latencies)
    n_total = len(scale_results)

    workload_block = f"{workload_summary}\n\n" if workload_summary else ""

    if valid_latencies:
        gm = math.exp(sum(math.log(max(v, 1e-12)) for v in valid_latencies) / len(valid_latencies))
        geomean_line = f"Geometric mean across {n_valid}/{n_total} successful runs: {gm:.4f}s"

        # Mixed case: some scales succeed, some fail. The observed failure mode
        # is the LLM discarding the working aggressive core and retreating to a
        # degenerate query-only policy that passes 5/5 but scores no better than
        # a do-nothing baseline. Anchor the model on what already worked.
        preserve_block = ""
        n_failed = n_total - n_valid
        if 0 < n_failed < n_total:
            ok_scale_ids = [s for s in sorted(scale_results) if scale_results[s].get("ok")]
            bad_scale_ids = [s for s in sorted(scale_results) if not scale_results[s].get("ok")]
            best_scale = min(ok_scale_ids, key=lambda s: scale_results[s]["latency"])
            br = scale_results[best_scale]
            strong = []
            for prio in ("query", "interactive", "batch"):
                ps = br.get(prio)
                if ps and ps["arrived"] > 0 and ps["completed"] / ps["arrived"] >= 0.9:
                    strong.append(f"{prio} {ps['completed']}/{ps['arrived']}")
            strong_s = "; ".join(strong) if strong else "low adjusted latency"
            ok_s = ", ".join(f"{s}x" for s in ok_scale_ids)
            bad_s = ", ".join(f"{s}x" for s in bad_scale_ids)
            preserve_block = f"""## What already works  -  PRESERVE THIS

Scales {ok_s} already run successfully; the best is {best_scale}x at \
{br['latency']:.2f}s with {strong_s}. This proves the core allocation and
scheduling logic in the code below is sound and competitive  -  it is not the
problem.

ONLY scales {bad_s} failed, and they failed with a *specific structural* error
(see the FAILED line above), not because the working scales' strategy is wrong.

Make the SMALLEST change that fixes only the failing scales. Do NOT rewrite
from scratch, and do NOT retreat to an ultra-conservative policy (e.g. running
only query and starving interactive/batch) just to make every scale "pass": a
scheduler that passes 5/5 but completes only query scores NO BETTER than a
do-nothing baseline (~485s here). Keep the behaviour that produced the good
{ok_s} numbers above; surgically fix what broke at {bad_s}.

"""
        return f"""\
{workload_block}## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 x max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are then combined into a weighted average across all arrived pipelines:
  score = (sum of query_latency x 10 + sum of interactive_latency x 5 + sum of batch_latency x 1)
        / (query_arrivals x 10 + interactive_arrivals x 5 + batch_arrivals x 1)

Incomplete and failed pipelines directly inflate the score through the penalty term  -  they are not ignored.
High completion rate matters as much as low latency.

## Performance Results (adjusted latency across cluster sizes)

{perf_table}

{geomean_line}

{preserve_block}## Current Scheduler Code

{scheduler_code}

Produce an improved version of this scheduler that reduces the geometric mean of adjusted latency across all cluster sizes. The best schedulers achieve high RAM utilization with zero or near-zero OOM failures and complete all pipeline classes.

Use the EXACT key "{policy_key}" in both @register_scheduler_init and @register_scheduler decorators.
Output ONLY the complete Python code."""

    # All runs failed  -  lead with errors and structural requirements
    unique_errors = list(dict.fromkeys(errors))
    error_list = "\n".join(f"  {e}" for e in unique_errors)
    return f"""\
{workload_block}!! ALL {n_total} RUNS FAILED  -  STRUCTURAL ERRORS MUST BE FIXED BEFORE OPTIMIZING !!

## Errors (deduplicated)

{error_list}

## Failed Run Details

{perf_table}

These are not performance problems  -  the scheduler could not execute at all. Fix the structural issues first.

## Scheduler Interface Reference

Required structure  -  both decorators must be present with the same key:

  @register_scheduler_init(key="{policy_key}")
  def init(s):
      s.waiting_queue = []  # attach any state to s here

  @register_scheduler(key="{policy_key}")
  def scheduler(s, results, pipelines):
      # results: List[ExecutionResult]  pipelines: List[Pipeline]
      return suspensions, assignments   # List[Suspend], List[Assignment]

Available globals (no imports needed): List, Tuple, Pipeline, OperatorState,
  ASSIGNABLE_STATES, Assignment, ExecutionResult, Suspend,
  register_scheduler_init, register_scheduler, Priority

Critical API facts:
  - Assignment requires a pipeline_id= keyword argument
  - s.executor.pools[i].avail_cpu_pool / avail_ram_pool do NOT update within
    a single scheduler call  -  track allocated resources manually
  - Priority levels: Priority.QUERY > Priority.INTERACTIVE > Priority.BATCH_PIPELINE

## Current (Broken) Scheduler

{scheduler_code}

Rewrite this scheduler so it runs without errors. Use the EXACT key "{policy_key}" in both @register_scheduler_init and @register_scheduler decorators. Output ONLY the complete Python code."""


def get_multi_candidate_feedback_prompt(
    policy_key: str,
    candidates: list[dict],
    base_params: dict,
    workload_summary: str | None = None,
) -> str:
    """Feedback prompt that shows several prior chain attempts at once.

    candidates: list of {"label": str, "scheduler_code": str, "scale_results": dict}.
    Used by the chained driver for the `lastn-1` and `best-k` selection policies.
    Each candidate is summarized compactly (one line per scale + full code) so
    K candidates stay within a reasonable prompt size.
    """
    import math

    def _gm(vals: list[float]) -> float:
        return math.exp(sum(math.log(max(v, 1e-12)) for v in vals) / len(vals)) if vals else float("nan")

    workload_block = f"{workload_summary}\n\n" if workload_summary else ""

    blocks = []
    for c in candidates:
        sr = c["scale_results"]
        valid = [r["latency"] for r in sr.values() if r.get("ok")]
        gm = _gm(valid)
        gm_s = f"{gm:.1f}s" if valid else "n/a"
        lines = []
        for scale in sorted(sr):
            r = sr[scale]
            if r.get("ok"):
                fec = r.get("failure_error_counts") or {}
                fec_s = " ".join(f"{k}={v}" for k, v in sorted(fec.items())) or "clean"
                lines.append(f"  {scale}x adj={r['latency']:.1f}s [{fec_s}]")
            else:
                lines.append(f"  {scale}x FAILED: {r.get('error', '?')}")
        blocks.append(
            f"### {c['label']} - geomean {gm_s} ({len(valid)}/{len(sr)} scales ok)\n"
            + "\n".join(lines)
            + f"\n\n```python\n{c['scheduler_code']}\n```"
        )

    candidates_text = "\n\n".join(blocks)
    return f"""\
{workload_block}## Objective Function

For each pipeline, assign a latency value:
  - Completed pipeline: actual end-to-end latency in seconds
  - Incomplete or failed pipeline: 2 x max_job_seconds (e.g. 720s if max_job_seconds=360)

These per-pipeline latencies are combined into a priority-weighted average:
  score = (sum query_latency x 10 + sum interactive_latency x 5 + sum batch_latency x 1)
        / (query_arrivals x 10 + interactive_arrivals x 5 + batch_arrivals x 1)

Incomplete and failed pipelines inflate the score through the penalty term.

## Prior attempts in this chain (study what worked and what failed)

{candidates_text}

## Task

Produce a single improved scheduler. Diagnose the specific failures shown
above (e.g. Overallocated, subprocess timeout, re-assigning non-assignable
operators) and fix them while keeping what worked. Lower the priority-weighted
geometric-mean latency and complete all 5 cluster scales.

Use the EXACT key "{policy_key}" in both @register_scheduler_init and
@register_scheduler decorators. Output ONLY the complete Python code."""