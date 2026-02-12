from typing import List, Tuple, Dict
from eudoxia.workload import Pipeline, OperatorState, Operator
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler
from eudoxia.utils import Priority


@register_scheduler_init(key="gurraj_scheduler_v2")
def gurraj_scheduler_init(s):
    """Initialize scheduler state for hybrid Priority + SJF scheduling + smart backfilling.

    Args:
        s: The scheduler instance. You can add custom attributes here.
    """
    # Track active pipelines across ticks (no priority queues)
    s.active_pipelines: List[Pipeline] = []

    # OOM retry tracking - maps operator_id -> [retry_count, [ram_history], [cpu_history]]
    s.oom_history: Dict[str, List] = {}

    # Get multi_operator_containers setting from params (default True)
    s.multi_operator_containers = s.params.get("multi_operator_containers", True)

    # Max retries for OOM failures
    s.max_oom_retries = 3

    # Round-robin pool assignment counter for load balancing
    s.next_pool_id = 0

    # Debug: print top operator scores once (first tick with candidates)
    s._debug_scores_printed = False


@register_scheduler(key="gurraj_scheduler_v2")
def gurraj_scheduler_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Hybrid Priority + SJF scheduler with smart backfilling.

    Core behavior:
    - Group operators by priority (QUERY, INTERACTIVE, BATCH)
    - Within each priority group, schedule shortest jobs first (SJF)
    - Overall order: all QUERY (shortest first), then INTERACTIVE, then BATCH
    - Backfill small batch operators into remaining resource gaps

    Kept features:
    - Smart resource estimation (6-20 CPUs, 20-40GB RAM based on workload)
    - Progressive OOM retry (1.5x, 2x, 3x)
    - Load-balanced pool selection with round-robin
    - require_parents_complete=True everywhere to prevent dependency violations

    Args:
        s: The scheduler instance
        results: List of execution results from previous tick
        pipelines: List of new pipelines from WorkloadGenerator

    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (always empty - no preemption)
            - List of new assignments to make
    """

    # Helper function: Estimate operator resource needs
    def estimate_operator_resources(op: Operator, pool_total_cpu: int, pool_total_ram: float):
        """Estimate CPU and RAM needs based on operator characteristics."""
        segments = op.get_segments()
        if not segments:
            return (max(6, int(pool_total_cpu * 0.11)), max(20, int(pool_total_ram * 0.12)))

        total_storage_read = sum(seg.storage_read_gb for seg in segments)
        total_cpu_seconds = sum(seg.baseline_cpu_seconds for seg in segments if seg.baseline_cpu_seconds)

        # Estimate if CPU-heavy or IO-heavy
        is_cpu_heavy = total_cpu_seconds > 10 if total_cpu_seconds else False
        is_io_heavy = total_storage_read > 20

        # Smart allocation based on workload type
        if is_cpu_heavy:
            cpu_pct = 0.18  # 18% for CPU-heavy
            ram_pct = 0.16
        elif is_io_heavy:
            cpu_pct = 0.11  # 11% for IO-heavy
            ram_pct = 0.16  # Need more memory for data
        else:
            cpu_pct = 0.13
            ram_pct = 0.14

        # Calculate with proven minimums (6 CPUs, 20 GB)
        est_cpu = max(6, int(pool_total_cpu * cpu_pct))
        est_ram = max(20, int(pool_total_ram * ram_pct))

        # For data-heavy operations, ensure RAM >= storage_read * 1.1
        if total_storage_read > 0:
            min_ram_for_data = int(total_storage_read * 1.1)
            est_ram = max(est_ram, min_ram_for_data)

        return (est_cpu, est_ram)

    # Helper function: Get least loaded pools (round-robin with load awareness)
    def get_sorted_pool_ids(s, pool_stats):
        """Return pool IDs sorted by available resources (most available first)."""
        pool_scores = []
        for pool_id in range(s.executor.num_pools):
            if pool_stats[pool_id]["avail_cpu"] <= 0 or pool_stats[pool_id]["avail_ram"] <= 0:
                continue
            # Score by available resources (higher is better)
            cpu_avail_pct = pool_stats[pool_id]["avail_cpu"] / pool_stats[pool_id]["total_cpu"]
            ram_avail_pct = pool_stats[pool_id]["avail_ram"] / pool_stats[pool_id]["total_ram"]
            score = (cpu_avail_pct + ram_avail_pct) / 2
            pool_scores.append((score, pool_id))

        # Sort by score (highest first) with round-robin tie-breaking
        pool_scores.sort(key=lambda x: (-x[0], (x[1] - s.next_pool_id) % s.executor.num_pools))
        return [pool_id for _, pool_id in pool_scores]

    def estimate_operator_runtime(op: Operator) -> float:
        """Estimate runtime using baseline_cpu_seconds from segments."""
        segments = op.get_segments()
        if not segments:
            return 0.0
        return float(sum(seg.baseline_cpu_seconds or 0.0 for seg in segments))

    # Add new pipelines to active tracking
    for p in pipelines:
        if p not in s.active_pipelines:
            s.active_pipelines.append(p)

    # Process results: Update OOM history and ensure pipelines with state changes are tracked
    pipelines_to_track = set()
    for r in results:
        for op in r.ops:
            pipelines_to_track.add(op.pipeline)

        if r.failed() and r.error == "OOM":
            # Track OOM failures with history for progressive retry
            for op in r.ops:
                if op.state() != OperatorState.COMPLETED:
                    op_id = str(op.id)
                    if op_id in s.oom_history:
                        retry_count, ram_history, cpu_history = s.oom_history[op_id]
                        retry_count += 1
                        ram_history.append(r.ram)
                        cpu_history.append(r.cpu)
                    else:
                        retry_count = 1
                        ram_history = [r.ram]
                        cpu_history = [r.cpu]

                    if retry_count <= s.max_oom_retries:
                        s.oom_history[op_id] = [retry_count, ram_history, cpu_history]
        else:
            # Clean up OOM history for successfully completed operators
            for op in r.ops:
                op_id = str(op.id)
                if op_id in s.oom_history:
                    del s.oom_history[op_id]

    for pipeline in pipelines_to_track:
        if pipeline not in s.active_pipelines:
            s.active_pipelines.append(pipeline)

    # Remove completed pipelines from active tracking
    s.active_pipelines = [
        p for p in s.active_pipelines if not p.runtime_status().is_pipeline_successful()
    ]

    # Early exit if nothing to do
    if not s.active_pipelines:
        return [], []

    # Track available resources per pool
    pool_stats = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_stats[pool_id] = {
            "avail_cpu": pool.avail_cpu_pool,
            "avail_ram": pool.avail_ram_pool,
            "total_cpu": pool.max_cpu_pool,
            "total_ram": pool.max_ram_pool,
        }

    # NO PREEMPTION: The system isn't designed for frequent preemption
    suspensions = []
    assignments = []

    priority_rank_map = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 2,
    }
    backfill_threshold = 0.25

    # Precompute ready ops per pipeline
    ready_ops_by_pipeline: Dict[Pipeline, List[Operator]] = {}

    for pipeline in s.active_pipelines:
        status = pipeline.runtime_status()
        ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ready_ops:
            ready_ops_by_pipeline[pipeline] = ready_ops

    # Build candidate operator list
    candidates = []
    for pipeline, ready_ops in ready_ops_by_pipeline.items():
        for op in ready_ops:
            estimated_runtime = estimate_operator_runtime(op)
            candidates.append({
                "op": op,
                "pipeline": pipeline,
                "priority_rank": priority_rank_map[pipeline.priority],
                "estimated_runtime": estimated_runtime,
            })

    # Sort by priority group first, then shortest runtime within group
    candidates.sort(
        key=lambda c: (
            c["priority_rank"],
            c["estimated_runtime"],
            c["pipeline"].pipeline_id,
            str(c["op"].id),
        )
    )

    if not s._debug_scores_printed and candidates:
        print("[gurraj_scheduler_v2] Top 5 operator candidates (Hybrid Priority+SJF, first tick):")
        for entry in candidates[:5]:
            print(
                f"  rank={entry['priority_rank']} "
                f"runtime={entry['estimated_runtime']:.1f} "
                f"priority={entry['pipeline'].priority.name} "
                f"pipeline={entry['pipeline'].pipeline_id} "
                f"op={entry['op'].id}"
            )
        s._debug_scores_printed = True

    assigned_ops = set()

    # Primary scheduling: highest score first
    for cand in candidates:
        op = cand["op"]
        pipeline = cand["pipeline"]
        if op in assigned_ops:
            continue

        sorted_pool_ids = get_sorted_pool_ids(s, pool_stats)
        if not sorted_pool_ids:
            break

        op_id = str(op.id)

        for pool_id in sorted_pool_ids:
            avail_cpu = pool_stats[pool_id]["avail_cpu"]
            avail_ram = pool_stats[pool_id]["avail_ram"]
            total_cpu = pool_stats[pool_id]["total_cpu"]
            total_ram = pool_stats[pool_id]["total_ram"]

            op_list = [op]

            if op_id in s.oom_history:
                retry_count, ram_history, cpu_history = s.oom_history[op_id]
                last_ram = ram_history[-1]
                last_cpu = cpu_history[-1]
                if retry_count == 1:
                    factor = 1.5
                elif retry_count == 2:
                    factor = 2.0
                else:
                    factor = 3.0

                job_cpu = int(last_cpu * factor)
                job_ram = int(last_ram * factor)

                # Skip if retry would use >60% of pool
                cpu_ratio = job_cpu / total_cpu
                ram_ratio = job_ram / total_ram
                if cpu_ratio >= 0.6 or ram_ratio >= 0.6:
                    continue
            else:
                # Smart initial allocation based on operator characteristics
                job_cpu, job_ram = estimate_operator_resources(op, total_cpu, total_ram)

                # Optionally pack additional ready ops from the same pipeline
                if s.multi_operator_containers:
                    extras = [
                        o for o in ready_ops_by_pipeline.get(pipeline, [])
                        if o not in assigned_ops and o is not op and str(o.id) not in s.oom_history
                    ]
                    if extras:
                        extras.sort(key=lambda o: estimate_operator_runtime(o))
                        total_cpu_est = job_cpu
                        total_ram_est = job_ram
                        packed = [op]
                        for extra in extras:
                            est_cpu, est_ram = estimate_operator_resources(extra, total_cpu, total_ram)
                            next_cpu = total_cpu_est + est_cpu
                            next_ram = total_ram_est + est_ram
                            cpu_pct = next_cpu / total_cpu
                            ram_pct = next_ram / total_ram
                            if cpu_pct > 0.30 or ram_pct > 0.30:
                                continue
                            total_cpu_est = next_cpu
                            total_ram_est = next_ram
                            packed.append(extra)

                        if len(packed) > 1:
                            op_list = packed
                            job_cpu = total_cpu_est
                            job_ram = total_ram_est

            # Clamp to available resources
            if job_cpu > avail_cpu or job_ram > avail_ram:
                if len(op_list) > 1:
                    op_list = [op]
                    job_cpu, job_ram = estimate_operator_resources(op, total_cpu, total_ram)
                job_cpu = min(job_cpu, avail_cpu)
                job_ram = min(job_ram, avail_ram)

            if job_cpu < 1 or job_ram < 1:
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=job_cpu,
                ram=job_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            pool_stats[pool_id]["avail_cpu"] -= job_cpu
            pool_stats[pool_id]["avail_ram"] -= job_ram
            s.next_pool_id = (pool_id + 1) % s.executor.num_pools

            for o in op_list:
                assigned_ops.add(o)
            break

    # Smart backfilling: pack small batch ops into leftover gaps
    backfill_candidates = []
    for pipeline, ready_ops in ready_ops_by_pipeline.items():
        if pipeline.priority != Priority.BATCH_PIPELINE:
            continue
        for op in ready_ops:
            if op not in assigned_ops:
                backfill_candidates.append((pipeline, op))

    if backfill_candidates:
        for pool_id in get_sorted_pool_ids(s, pool_stats):
            while True:
                avail_cpu = pool_stats[pool_id]["avail_cpu"]
                avail_ram = pool_stats[pool_id]["avail_ram"]
                total_cpu = pool_stats[pool_id]["total_cpu"]
                total_ram = pool_stats[pool_id]["total_ram"]

                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                best = None
                best_size = None

                for pipeline, op in backfill_candidates:
                    if op in assigned_ops:
                        continue

                    op_id = str(op.id)
                    if op_id in s.oom_history:
                        retry_count, ram_history, cpu_history = s.oom_history[op_id]
                        last_ram = ram_history[-1]
                        last_cpu = cpu_history[-1]
                        if retry_count == 1:
                            factor = 1.5
                        elif retry_count == 2:
                            factor = 2.0
                        else:
                            factor = 3.0

                        job_cpu = int(last_cpu * factor)
                        job_ram = int(last_ram * factor)

                        cpu_ratio = job_cpu / total_cpu
                        ram_ratio = job_ram / total_ram
                        if cpu_ratio >= 0.6 or ram_ratio >= 0.6:
                            continue
                    else:
                        job_cpu, job_ram = estimate_operator_resources(op, total_cpu, total_ram)
                        cpu_ratio = job_cpu / total_cpu
                        ram_ratio = job_ram / total_ram

                    if job_cpu > avail_cpu or job_ram > avail_ram:
                        continue

                    # Only backfill small jobs
                    if cpu_ratio > backfill_threshold or ram_ratio > backfill_threshold:
                        continue

                    size = cpu_ratio + ram_ratio
                    if best is None or size < best_size:
                        best = (pipeline, op, job_cpu, job_ram)
                        best_size = size

                if not best:
                    break

                pipeline, op, job_cpu, job_ram = best
                assignment = Assignment(
                    ops=[op],
                    cpu=job_cpu,
                    ram=job_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                pool_stats[pool_id]["avail_cpu"] -= job_cpu
                pool_stats[pool_id]["avail_ram"] -= job_ram
                s.next_pool_id = (pool_id + 1) % s.executor.num_pools
                assigned_ops.add(op)

    return suspensions, assignments
