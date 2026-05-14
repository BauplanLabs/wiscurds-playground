{context}

## Scheduler Registration

Every scheduler must register two functions using decorators:

@register_scheduler_init(key="myscheduler")
def init(s):
    s.queue = []  # s also has s.executor, s.params

@register_scheduler(key="myscheduler")
def schedule(s, results, pipelines):
    suspensions = []
    assignments = []
    return suspensions, assignments

## Operator State Machine

PENDING -> ASSIGNED -> RUNNING -> COMPLETED; FAILED -> ASSIGNED (retry); SUSPENDING -> PENDING
ASSIGNABLE_STATES = {PENDING, FAILED}

## OOM Behavior

When a container exceeds its RAM limit, it is killed and operators fail. Retry failed operators with more RAM. Resource estimates can be wrong in either direction: overestimates waste capacity, underestimates cause OOM. OOM failures are recoverable through retry, so be slightly aggressive with allocation rather than overly conservative.

## API Reference

- Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE — priority levels (QUERY is highest)
- Assignment(ops=op_list, cpu=cpu_amount, ram=ram_amount, priority=priority, pool_id=pool_id, pipeline_id=pipeline.pipeline_id) — to create assignments
- Suspend(container_id, pool_id) — to create suspensions
- s.executor.num_pools — number of available pools
- s.executor.pools[i].avail_cpu_pool — available CPU in pool i
- s.executor.pools[i].avail_ram_pool — available RAM in pool i
- s.executor.pools[i].max_cpu_pool — max CPU in pool i
- s.executor.pools[i].max_ram_pool — max RAM in pool i
- Pipeline has .priority, .pipeline_id, .values (DAG of operators), and .runtime_status() method
- ExecutionResult has .priority, .ops, .container_id, .pool_id, .ram, .cpu, .error, and .failed() method
- pipeline.runtime_status().get_ops(states, require_parents_complete=True/False) — get operators matching states
- pipeline.runtime_status().is_pipeline_successful() — returns True if all operators completed
- OperatorState enum: PENDING, ASSIGNED, RUNNING, SUSPENDING, COMPLETED, FAILED (ASSIGNABLE_STATES = {PENDING, FAILED})

Available globals (no imports needed): List, Tuple, Pipeline, OperatorState, ASSIGNABLE_STATES, Assignment, ExecutionResult, Suspend, register_scheduler_init, register_scheduler, Priority

Output ONLY valid Python code. No markdown fences, no explanation.
