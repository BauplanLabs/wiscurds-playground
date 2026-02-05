"""
Smart Scheduler - Optimized for minimizing SimulatorStats.adjusted_latency

Strategy:
1. Generate container_id at assignment time for preemption tracking
2. Aggressive preemption with "pending free" check to prevent thrashing
3. Unified 'Big Slot' allocation: 64GB RAM / 16 vCPU for ALL tasks to eliminate OOMs
4. Zero Tolerance for OOM (start at 64GB)
5. 100% completion rate to avoid division penalty in scoring
"""

from typing import List, Tuple, Dict, Optional
from collections import deque, defaultdict
import uuid
import logging

from eudoxia.workload import Pipeline, Operator, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler
from .waiting_queue import WaitingQueueJob, RetryStats

logger = logging.getLogger(__name__)


# Track memory requirements per operator ID (for exponential backoff on OOM)
# Key: operator ID, Value: last failed RAM allocation
_operator_memory_history: Dict[str, int] = defaultdict(lambda: 0)


class RunningContainerInfo:
    """Tracks info about a running container for preemption decisions."""
    __slots__ = ('container_id', 'priority', 'ram', 'cpu', 'pool_id', 'can_suspend')
    
    def __init__(self, container_id: str, priority: Priority, ram: int, cpu: int, pool_id: int):
        self.container_id = container_id
        self.priority = priority
        self.ram = ram
        self.cpu = cpu
        self.pool_id = pool_id
        self.can_suspend = False  # Updated when we know it can be suspended


@register_scheduler_init(key="smart")
def init_smart_scheduler(s):
    """Initialize the smart scheduler state."""
    s.multi_operator_containers = s.params.get("multi_operator_containers", True)
    
    # Priority queues (using deque for efficient popleft)
    s.query_queue: deque = deque()       # Priority.QUERY (weight 10)
    s.interactive_queue: deque = deque() # Priority.INTERACTIVE (weight 5)
    s.batch_queue: deque = deque()       # Priority.BATCH_PIPELINE (weight 1)
    
    s.queues_by_prio = {
        Priority.QUERY: s.query_queue,
        Priority.INTERACTIVE: s.interactive_queue,
        Priority.BATCH_PIPELINE: s.batch_queue,
    }
    
    # Track running containers by pool_id for preemption
    # Key: pool_id, Value: dict of container_id -> RunningContainerInfo
    s.running_containers: Dict[int, Dict[str, RunningContainerInfo]] = defaultdict(dict)
    
    # Track containers that are being suspended (waiting for them to complete suspension)
    s.suspending: Dict[str, WaitingQueueJob] = {}
    
    # Memory history for exponential backoff (persistent across calls)
    s.memory_history: Dict[str, int] = defaultdict(lambda: 0)


def get_memory_for_operator(s, op: Operator) -> int:
    """
    Get the RAM allocation for an operator, considering OOM history.
    Uses exponential backoff: starts at 4GB, doubles on each OOM.
    """
    op_id = str(op.id)
    if s.memory_history[op_id] > 0:
        # Double the previous failed allocation
        return min(s.memory_history[op_id] * 2, 128)  # Cap at 128GB
    return 16  # Initial allocation: 16 GB


def get_cpu_for_priority(priority: Priority) -> int:
    """
    Get CPU allocation based on priority.
    High Stability Strategy (4 slots per pool):
    All tasks get 64GB -> 16 vCPU
    This maximizes speed and eliminates OOMs.
    """
    return 16


def find_pool_with_resources(s, pool_stats: Dict, required_cpu: int, required_ram: int) -> int:
    """
    Find a pool with sufficient available resources.
    Uses pool_stats snapshot to avoid overallocation in same tick.
    Returns pool_id or -1 if none available.
    """
    best_pool = -1
    best_ram = -1
    
    for i in range(s.executor.num_pools):
        avail_cpu = pool_stats[i]["avail_cpu"]
        avail_ram = pool_stats[i]["avail_ram"]
        if avail_cpu >= required_cpu and avail_ram >= required_ram:
            if avail_ram > best_ram:
                best_ram = avail_ram
                best_pool = i
    
    return best_pool


def find_preemption_victim(s, required_ram: int, required_cpu: int, exclude_priority: Priority) -> Optional[Tuple[str, int]]:
    """
    Find a container that can be preempted (suspended) to free resources.
    Only preempts lower priority containers.
    Returns (container_id, pool_id) or None.
    """
    # Look for the best victim across all pools
    # Prefer: lower priority, larger RAM (to free more resources)
    best_victim = None
    best_score = -1
    
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        
        for container in pool.active_containers:
            # Skip if same or higher priority
            if container.priority.value <= exclude_priority.value:
                continue
            
            # Skip if can't suspend right now
            if not container.can_suspend_container():
                continue
            
            # Score: prioritize lower priority and larger RAM
            priority_score = container.priority.value  # Higher value = lower priority = better victim
            ram_score = container.assignment.ram
            score = priority_score * 100 + ram_score
            
            if score > best_score:
                best_score = score
                best_victim = (container.container_id, pool_id)
    
    return best_victim


@register_scheduler(key="smart")
def smart_scheduler(s, results: List[ExecutionResult],
                    pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Smart scheduler optimized for minimizing adjusted_latency.
    
    Key strategies:
    1. Prioritize Query tasks (weight 10) with maximum CPU
    2. Aggressively preempt Batch tasks for Query tasks
    3. Use exponential backoff for OOM failures
    4. Never abandon tasks (100% completion rate)
    """
    
    # === PHASE 1: Process execution results ===
    pipelines_to_process = {p.pipeline_id: p for p in pipelines}
    
    for r in results:
        # Update tracked running containers
        for pool_id, containers in s.running_containers.items():
            if r.container_id in containers:
                del containers[r.container_id]
                break
        
        if r.failed():
            # OOM failure: update memory history for exponential backoff
            if r.error == "OOM":
                for op in r.ops:
                    op_id = str(op.id)
                    old_ram = s.memory_history[op_id] if s.memory_history[op_id] > 0 else r.ram
                    s.memory_history[op_id] = old_ram  # Will be doubled when re-queued
                    logger.info(f"OOM for op {op_id}, will retry with {old_ram * 2}GB")
            
            # Mark ops for re-processing
            for op in r.ops:
                if op.state() != OperatorState.COMPLETED:
                    pipelines_to_process[op.pipeline.pipeline_id] = op.pipeline
        else:
            # Success: may have more ops to schedule
            for op in r.ops:
                pipelines_to_process[op.pipeline.pipeline_id] = op.pipeline
    
    # === PHASE 2: Handle suspended containers being added back to queue ===
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        
        # Track containers starting to suspend
        for c in pool.suspending_containers:
            if c.container_id not in s.suspending:
                ops = [op for op in c.operators if op.state() != OperatorState.COMPLETED]
                if ops:
                    pipeline = ops[0].pipeline
                    retry_stats = RetryStats(
                        old_ram=c.assignment.ram,
                        old_cpu=c.assignment.cpu,
                        error=c.error,
                        container_id=c.container_id,
                        pool_id=pool_id,
                    )
                    job = WaitingQueueJob(priority=c.priority, p=pipeline, ops=ops, retry_stats=retry_stats)
                    s.suspending[c.container_id] = job
        
        # Move fully suspended containers back to queue
        for container in pool.suspended_containers:
            if container.container_id in s.suspending:
                job = s.suspending.pop(container.container_id)
                s.queues_by_prio[job.priority].append(job)
    
    # === PHASE 3: Queue new work ===
    if pipelines_to_process:
        # Track already queued ops to avoid double-queueing
        already_queued = set()
        for queue in s.queues_by_prio.values():
            for job in queue:
                for op in job.ops:
                    already_queued.add(op.id)
        
        for pipeline in pipelines_to_process.values():
            if s.multi_operator_containers:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            else:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            
            op_list = [op for op in op_list if op.id not in already_queued]
            if not op_list:
                continue
            
            # Create job for each operator (better granularity for resource allocation)
            if s.multi_operator_containers:
                job = WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=op_list, retry_stats=None)
                s.queues_by_prio[pipeline.priority].append(job)
            else:
                for op in op_list:
                    job = WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=[op], retry_stats=None)
                    s.queues_by_prio[pipeline.priority].append(job)
    
    # === PHASE 4: Build resource snapshot ===
    pool_stats = {}
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        pool_stats[i] = {
            "avail_cpu": pool.avail_cpu_pool,
            "avail_ram": pool.avail_ram_pool,
            "total_cpu": pool.max_cpu_pool,
            "total_ram": pool.max_ram_pool,
        }
    
    # === PHASE 5: Assign work (priority order: Query > Interactive > Batch) ===
    new_assignments = []
    suspensions = []
    
    # Process queues in priority order
    queues = [
        (Priority.QUERY, s.query_queue),
        (Priority.INTERACTIVE, s.interactive_queue),
        (Priority.BATCH_PIPELINE, s.batch_queue),
    ]
    
    for priority, queue in queues:
        jobs_to_remove = []
        
        for job in queue:
            op_list = job.ops
            
            # Determine resource needs
            job_cpu = get_cpu_for_priority(priority)
            
            # Calculate RAM based on max of all ops (with OOM history)
            # Strategy: Give everyone 64GB to prevent OOM chrun
            base_ram = 64
                
            job_ram = base_ram  # Default
            for op in op_list:
                op_ram = get_memory_for_operator(s, op)
                job_ram = max(job_ram, op_ram)
            
            # If this is a retry with known resources, use those
            if job.retry_stats is not None:
                if job.retry_stats.error == "OOM":
                    job_ram = job.retry_stats.old_ram * 2  # Double on OOM
                else:
                    job_ram = job.retry_stats.old_ram
                    job_cpu = job.retry_stats.old_cpu
            
            # Cap resources to reasonable limits
            job_ram = min(job_ram, 128)
            job_cpu = min(job_cpu, 32)
            
            # Find a pool with available resources
            target_pool_id = find_pool_with_resources(s, pool_stats, job_cpu, job_ram)
            
            if target_pool_id == -1:
                # No resources available - try preemption for high priority tasks
                if priority == Priority.QUERY or priority == Priority.INTERACTIVE:
                    # Check if we are already freeing enough resources in some pool
                    # This prevents mass eviction when we only need a bit more space
                    best_victim = None
                    
                    for i in range(s.executor.num_pools):
                        pool = s.executor.pools[i]
                        
                        # Resources currently available (according to our local snapshot)
                        curr_cpu = pool_stats[i]["avail_cpu"]
                        curr_ram = pool_stats[i]["avail_ram"]
                        
                        # Resources being freed (suspending in pool)
                        # Note: we need to trust the executor's state for this
                        suspending_cpu = sum(c.assignment.cpu for c in pool.suspending_containers)
                        suspending_ram = sum(c.assignment.ram for c in pool.suspending_containers)
                        
                        # Also count resources from victims we JUST selected in this scheduling tick
                        # (We haven't sent them to executor yet, but we know they will suspend)
                        # We stored them in 'suspensions' list, but it's a list of Suspend objects (id, pool_id)
                        # We need to look up the container info.
                        newly_suspending_cpu = 0
                        newly_suspending_ram = 0
                        
                        # This iteration is a bit expensive but necessary to avoid thrashing
                        # Optimization: we could track this in a separate struct
                        for sus in suspensions:
                            if sus.pool_id == i:
                                # We need to find the container to know its resources
                                # It's in s.running_containers or active_containers
                                # But we removed it from s.running_containers in Phase 1?
                                # No, we add to s.running_containers on creation.
                                # Where do we get the object?
                                # We can look it up in s.executor.pools[i].active_containers
                                c = pool.get_container_by_id(sus.container_id)
                                if c:
                                    newly_suspending_cpu += c.assignment.cpu
                                    newly_suspending_ram += c.assignment.ram
                        
                        potential_cpu = curr_cpu + suspending_cpu + newly_suspending_cpu
                        potential_ram = curr_ram + suspending_ram + newly_suspending_ram
                        
                        # If we will have enough resources soon, DON'T PREEMPT MORE in this pool
                        # Just wait for them to free up
                        if potential_cpu >= job_cpu and potential_ram >= job_ram:
                            # We found a pool that will eventually fit this job
                            # So we don't need to preempt anything else for THIS job
                            # Break the preemption search for this job
                            best_victim = "WAITING" 
                            break

                    if best_victim == "WAITING":
                        continue

                    # If we are here, no pool has enough "future" resources
                    # So we MUST preempt something
                    victim = find_preemption_victim(s, job_ram, job_cpu, priority)
                    if victim:
                        victim_id, victim_pool_id = victim
                        suspension = Suspend(victim_id, victim_pool_id)
                        suspensions.append(suspension)
                        logger.info(f"Preempting container {victim_id} for {priority.name} task")
                        # Job stays in queue, will be scheduled next tick after resources free
                        continue
                # Can't schedule now, leave in queue
                continue
            
            # Mark job for removal and create assignment
            jobs_to_remove.append(job)
            
            # Generate unique container ID for tracking
            container_id = uuid.uuid4().hex
            
            # Update pool stats (local copy)
            pool_stats[target_pool_id]["avail_cpu"] -= job_cpu
            pool_stats[target_pool_id]["avail_ram"] -= job_ram
            
            # Create assignment
            assignment = Assignment(
                ops=op_list,
                cpu=job_cpu,
                ram=job_ram,
                priority=priority,
                pool_id=target_pool_id,
                pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown",
                container_id=container_id,
            )
            new_assignments.append(assignment)
            
            # Track running container for potential preemption
            info = RunningContainerInfo(container_id, priority, job_ram, job_cpu, target_pool_id)
            s.running_containers[target_pool_id][container_id] = info
            
            logger.info(f"Assigned {priority.name} job: cpu={job_cpu}, ram={job_ram}, pool={target_pool_id}")
        
        # Remove scheduled jobs from queue
        for job in jobs_to_remove:
            queue.remove(job)
    
    return suspensions, new_assignments
