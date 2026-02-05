from typing import List, Tuple, Dict
import logging
from eudoxia.workload import Pipeline, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler
from eudoxia.utils import Priority
# We import the helper class to track retries and resources better
from eudoxia.scheduler.waiting_queue import WaitingQueueJob, RetryStats

logger = logging.getLogger(__name__)

@register_scheduler_init(key="dax_scheduler")
def myscheduler_init(s):
    """
    Initialize scheduler with 3 separate priority queues.
    """
    s.multi_operator_containers = s.params.get("multi_operator_containers", False)
    
    # 1. Split waiting queue into buckets so Query jobs don't wait for Batch jobs
    s.qry_jobs: List[WaitingQueueJob] = [] 
    s.interactive_jobs: List[WaitingQueueJob] = [] 
    s.batch_ppln_jobs: List[WaitingQueueJob] = [] 
    
    s.queues_by_prio = {
        Priority.QUERY: s.qry_jobs,
        Priority.INTERACTIVE: s.interactive_jobs,
        Priority.BATCH_PIPELINE: s.batch_ppln_jobs,
    }
    
    # Track suspended jobs
    s.suspending = {}

@register_scheduler(key="dax_scheduler")
def myscheduler_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Latency Optimized Scheduler: Priority + Shortest Job First (SJF) + Backfilling.
    """

    # --- PHASE 1: PROCESS RESULTS & RETRIES ---
    # Convert failed results into retry stats so we don't crash again (OOM)
    retry_info = {}
    for r in results:
        if r.failed():
            for op in r.ops:
                if op.state() != OperatorState.COMPLETED:
                    retry_info[op.id] = RetryStats(
                        old_ram=r.ram, old_cpu=r.cpu, error=r.error,
                        container_id=r.container_id, pool_id=r.pool_id
                    )

    # --- PHASE 2: INGEST NEW PIPELINES ---
    # Map new pipelines to their objects
    pipelines_to_process = {p.pipeline_id: p for p in pipelines}
    # Also grab pipelines from failed ops to requeue them
    for r in results:
        for op in r.ops:
            pipelines_to_process[op.pipeline.pipeline_id] = op.pipeline

    # Identify what is already queued to avoid duplicates
    already_queued = set()
    for queue in s.queues_by_prio.values():
        for job in queue:
            for op in job.ops:
                already_queued.add(op.id)

    # Create Job wrappers and put them in the right bucket
    jobs = []
    for pipeline in pipelines_to_process.values():
        # Get assignable operators
        if s.multi_operator_containers:
            op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
        else:
            op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        
        # Filter out ops that are already waiting
        op_list = [op for op in op_list if op.id not in already_queued]
        if not op_list:
            continue

        # Create the job wrapper
        if s.multi_operator_containers:
            rs = retry_info.get(op_list[0].id)
            jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=op_list, retry_stats=rs))
        else:
            for op in op_list:
                rs = retry_info.get(op.id)
                jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=[op], retry_stats=rs))

    # Add to queues
    for job in jobs:
        s.queues_by_prio[job.pipeline.priority].append(job)

    # --- PHASE 3: THE LATENCY OPTIMIZATION (SJF SORTING) ---
    # We define a helper to guess duration. 
    # If explicit duration is unknown (0), we assume small op count = fast.
    def get_duration_score(job):
        runtime = getattr(job.pipeline, "expected_runtime", 0) or 0
        if runtime == 0:
            return len(job.ops) 
        return runtime

    # SORTING: This puts small jobs at the front of every queue!
    s.qry_jobs.sort(key=get_duration_score)
    s.interactive_jobs.sort(key=get_duration_score)
    s.batch_ppln_jobs.sort(key=get_duration_score)

   
   # --- PHASE 4: ALLOCATION LOOP ---
    # Snapshot current pool capacity
    pool_stats = {}
    for i in range(s.executor.num_pools):
        pool_stats[i] = {
            "avail_cpu": s.executor.pools[i].avail_cpu_pool,
            "avail_ram": s.executor.pools[i].avail_ram_pool,
            "total_cpu": s.executor.pools[i].max_cpu_pool,
            "total_ram": s.executor.pools[i].max_ram_pool,
        }

    # Process queues in strict priority order: QUERY -> INTERACTIVE -> BATCH
    queues = [s.qry_jobs, s.interactive_jobs, s.batch_ppln_jobs]
    assignments = []

    for queue in queues:
        to_remove = []
        
        # Iterate through the SORTED queue
        for job in queue:
            # Find the best pool (most RAM available)
            best_pool_id = -1
            max_ram = -1
            for pid, stats in pool_stats.items():
                if stats["avail_cpu"] > 0 and stats["avail_ram"] > max_ram:
                    max_ram = stats["avail_ram"]
                    best_pool_id = pid
            
            if best_pool_id == -1:
                break # No pools have CPU left at all

            # Determine Resource Size
            req_cpu = 0
            req_ram = 0
            
            if job.retry_stats:
                # If it failed before or was preempted, use the previous known safe values
                # (If it was OOM, double it)
                multiplier = 2 if job.retry_stats.error else 1
                req_cpu = job.retry_stats.old_cpu * multiplier
                req_ram = job.retry_stats.old_ram * multiplier
            else:
                # === THE FIX: GREEDY ALLOCATION ===
                # Instead of taking 10% (blocking concurrency), we take 
                # ALL available CPU to finish this job as fast as possible.
                # Because we sorted by Shortest Job First, this is optimal!
                req_cpu = pool_stats[best_pool_id]["avail_cpu"]
                
                # For RAM, we stick to a safe default (e.g. 25% of pool) so we don't 
                # cause artificial OOMs, but give enough to run smoothly.
                req_ram = int(pool_stats[best_pool_id]["total_ram"] / 4)

            # Cap request to what is actually available
            if req_cpu > pool_stats[best_pool_id]["avail_cpu"]:
                req_cpu = pool_stats[best_pool_id]["avail_cpu"]
            if req_ram > pool_stats[best_pool_id]["avail_ram"]:
                req_ram = pool_stats[best_pool_id]["avail_ram"]

            # BACKFILLING CHECK: Does it fit? (Must have at least 1 CPU and some RAM)
            if req_cpu >= 1 and req_ram >= 1:
                
                assign = Assignment(
                    ops=job.ops, cpu=req_cpu, ram=req_ram,
                    priority=job.priority, pool_id=best_pool_id,
                    pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown"
                )
                assignments.append(assign)
                to_remove.append(job)
                
                # Update our local stats
                pool_stats[best_pool_id]["avail_cpu"] -= req_cpu
                pool_stats[best_pool_id]["avail_ram"] -= req_ram
            else:
                continue

        # Clean up assigned jobs from the queue
        for job in to_remove:
            queue.remove(job)

    return [], assignments