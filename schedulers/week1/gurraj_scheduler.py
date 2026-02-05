from typing import List, Tuple, Dict
from eudoxia.workload import Pipeline, OperatorState, Operator
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler
from eudoxia.utils import Priority


@register_scheduler_init(key="gurraj_scheduler_v2")
def gurraj_scheduler_init(s):
    """Initialize scheduler state with aggressive optimization features.

    Args:
        s: The scheduler instance. You can add custom attributes here.
    """
    # Priority queues
    s.query_queue: List[Pipeline] = []
    s.interactive_queue: List[Pipeline] = []
    s.batch_queue: List[Pipeline] = []
    
    # OOM retry tracking - maps operator_id -> [retry_count, [ram_history], [cpu_history]]
    s.oom_history: Dict[str, List] = {}
    
    # Get multi_operator_containers setting from params (default True)
    s.multi_operator_containers = s.params.get("multi_operator_containers", True)
    
    # Max retries for OOM failures
    s.max_oom_retries = 3
    
    # Round-robin pool assignment counter for load balancing
    s.next_pool_id = 0


@register_scheduler(key="gurraj_scheduler_v2")
def gurraj_scheduler_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Optimized scheduler with smart resource allocation and progressive OOM retry.
    
    QUEUE MANAGEMENT PATTERN (Critical for correctness):
    =====================================================
    1. NEW ARRIVALS: Add new pipelines to priority queues
    2. STATE CHANGES: Requeue ALL pipelines from results (operators completed/failed)
    3. ASSIGNMENT: Process each pipeline ONCE per tick, then REMOVE from queue
    4. NATURAL RETURN: Pipelines automatically return next tick via step 2
    
    This prevents duplicate assignments in the same tick while ensuring pipelines
    with remaining operators are reassigned on subsequent ticks.
    
    Key optimizations:
    - Smart initial allocation based on operator characteristics (6-12 CPUs, 20-40GB)
    - Progressive OOM retry (1.5x, 2x, 3x)
    - Priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE)
    - Load-balanced pool selection with round-robin
    - require_parents_complete=True prevents dependency violations

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
        
        # Check first segment for characteristics
        seg = segments[0]
        total_storage_read = sum(s.storage_read_gb for s in segments)
        total_cpu_seconds = sum(s.baseline_cpu_seconds for s in segments if s.baseline_cpu_seconds)
        
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
    
    # Route new pipelines to priority queues
    for p in pipelines:
        if p.priority == Priority.QUERY:
            s.query_queue.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.interactive_queue.append(p)
        else:  # BATCH_PIPELINE
            s.batch_queue.append(p)

    # Process results: Update OOM history and requeue pipelines with state changes
    pipelines_to_requeue = set()
    for r in results:
        # CRITICAL: Requeue ALL pipelines with completed/failed operators
        # They may have more operators ready to run after state changes
        for op in r.ops:
            pipelines_to_requeue.add(op.pipeline)
        
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
                    # else: max retries exceeded, drop this operator
        else:
            # Clean up OOM history for successfully completed operators
            for op in r.ops:
                op_id = str(op.id)
                if op_id in s.oom_history:
                    del s.oom_history[op_id]

    # Requeue all pipelines with state changes to their priority queues
    for pipeline in pipelines_to_requeue:
        target_queue = None
        if pipeline.priority == Priority.QUERY:
            target_queue = s.query_queue
        elif pipeline.priority == Priority.INTERACTIVE:
            target_queue = s.interactive_queue
        else:
            target_queue = s.batch_queue
        
        if pipeline not in target_queue:
            target_queue.append(pipeline)

    # Early exit if no state changes
    if not pipelines and not results:
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
    # Suspended containers aren't properly requeued, causing work to be lost
    suspensions = []

    assignments = []

    # Process queues in priority order (high to low)
    queues = [
        (s.query_queue, Priority.QUERY),
        (s.interactive_queue, Priority.INTERACTIVE),
        (s.batch_queue, Priority.BATCH_PIPELINE)
    ]

    for queue, priority in queues:
        # Keep trying to assign from this queue until no more assignments possible
        while True:
            assigned_this_round = False
            pipelines_to_remove = []
            
            for pipeline in queue:
                status = pipeline.runtime_status()
                
                # Remove completed pipelines only
                if status.is_pipeline_successful():
                    pipelines_to_remove.append(pipeline)
                    continue
                
                # Get operators based on multi_operator_containers setting
                # CRITICAL: require_parents_complete=True prevents "Dependencies not satisfied" errors
                # Operators must have all parent dependencies completed before assignment
                if s.multi_operator_containers:
                    # Pack multiple operators from same pipeline (dependencies already satisfied)
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                else:
                    # Single operator at a time
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                
                if not op_list:
                    # No assignable operators right now, keep pipeline in queue
                    # (will be reassigned when ops complete or dependencies resolve)
                    continue
                
                # Get sorted pool IDs (load-balanced)
                sorted_pool_ids = get_sorted_pool_ids(s, pool_stats)
                if not sorted_pool_ids:
                    break  # No pools with resources
                
                # Try to find a pool with resources for this pipeline
                assigned = False
                for pool_id in sorted_pool_ids:
                    # Calculate resource allocation with smart estimation and progressive OOM retry
                    
                    # Check if ANY operators in this assignment have OOM history
                    has_oom_history = False
                    max_retry_ram = 0
                    max_retry_cpu = 0
                    max_retry_count = 0
                    
                    for op in op_list:
                        op_id = str(op.id)
                        if op_id in s.oom_history:
                            retry_count, ram_history, cpu_history = s.oom_history[op_id]
                            has_oom_history = True
                            max_retry_count = max(max_retry_count, retry_count)
                            
                            # Progressive retry: 1.5x, 2x, 3x
                            last_ram = ram_history[-1]
                            last_cpu = cpu_history[-1]
                            if retry_count == 1:
                                retry_ram = last_ram * 1.5
                                retry_cpu = last_cpu * 1.5
                            elif retry_count == 2:
                                retry_ram = last_ram * 2.0
                                retry_cpu = last_cpu * 2.0
                            else:  # retry_count == 3
                                retry_ram = last_ram * 3.0
                                retry_cpu = last_cpu * 3.0
                            
                            max_retry_ram = max(max_retry_ram, retry_ram)
                            max_retry_cpu = max(max_retry_cpu, retry_cpu)
                    
                    if has_oom_history:
                        # Use progressive retry allocation
                        job_cpu = min(int(max_retry_cpu), pool_stats[pool_id]["avail_cpu"])
                        job_ram = min(int(max_retry_ram), pool_stats[pool_id]["avail_ram"])
                        
                        # Skip if retry would use >60% of pool (relaxed from 50%)
                        cpu_ratio = job_cpu / pool_stats[pool_id]["total_cpu"]
                        ram_ratio = job_ram / pool_stats[pool_id]["total_ram"]
                        if cpu_ratio >= 0.6 or ram_ratio >= 0.6:
                            continue  # Try another pool or wait
                    else:
                        # Smart initial allocation based on operator characteristics
                        if len(op_list) == 1:
                            # Single operator - estimate based on characteristics
                            job_cpu, job_ram = estimate_operator_resources(
                                op_list[0], 
                                pool_stats[pool_id]["total_cpu"],
                                pool_stats[pool_id]["total_ram"]
                            )
                        else:
                            # Multi-operator packing - estimate combined needs
                            total_cpu = 0
                            total_ram = 0
                            for op in op_list:
                                est_cpu, est_ram = estimate_operator_resources(
                                    op,
                                    pool_stats[pool_id]["total_cpu"],
                                    pool_stats[pool_id]["total_ram"]
                                )
                                total_cpu += est_cpu
                                total_ram += est_ram
                            
                            # Don't pack if combined > 30% of pool
                            cpu_pct = total_cpu / pool_stats[pool_id]["total_cpu"]
                            ram_pct = total_ram / pool_stats[pool_id]["total_ram"]
                            if cpu_pct > 0.30 or ram_pct > 0.30:
                                # Try with just first operator instead
                                job_cpu, job_ram = estimate_operator_resources(
                                    op_list[0],
                                    pool_stats[pool_id]["total_cpu"],
                                    pool_stats[pool_id]["total_ram"]
                                )
                                op_list = [op_list[0]]
                            else:
                                job_cpu = total_cpu
                                job_ram = total_ram
                        
                        # Clamp to available resources
                        job_cpu = min(job_cpu, pool_stats[pool_id]["avail_cpu"])
                        job_ram = min(job_ram, pool_stats[pool_id]["avail_ram"])
                    
                    # Ensure minimum resources
                    if job_cpu < 1 or job_ram < 1:
                        continue
                    
                    # Create assignment
                    assignment = Assignment(
                        ops=op_list,
                        cpu=job_cpu,
                        ram=job_ram,
                        priority=priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id
                    )
                    assignments.append(assignment)
                    
                    # Update available resources
                    pool_stats[pool_id]["avail_cpu"] -= job_cpu
                    pool_stats[pool_id]["avail_ram"] -= job_ram
                    
                    # Update round-robin counter
                    s.next_pool_id = (pool_id + 1) % s.executor.num_pools
                    
                    # CRITICAL: Remove pipeline from queue after assignment to prevent duplicate
                    # assignments in the same tick. The pipeline will naturally return to the
                    # queue on the next tick (via results processing) when it has more operators ready.
                    # This is the correct pattern: assign once per tick, remove, re-queue next tick.
                    pipelines_to_remove.append(pipeline)
                    assigned = True
                    assigned_this_round = True
                    break  # Assigned to a pool, move to next pipeline
            
            # Remove COMPLETED and ASSIGNED pipelines from queue
            for p in pipelines_to_remove:
                if p in queue:
                    queue.remove(p)
            
            # If nothing was assigned this round, move to next priority queue
            if not assigned_this_round:
                break

    return suspensions, assignments
