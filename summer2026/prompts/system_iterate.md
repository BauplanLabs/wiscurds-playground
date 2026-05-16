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

## Objective Intuition (this is what you are minimizing)

The score is a *priority-weighted* mean latency. Pipeline weights are:

```
query = 10,  interactive = 5,  batch = 1
```

This means **each query pipeline contributes 10x to the score; losing 1 query
is the same penalty as losing 10 batches**. Failed/incomplete pipelines count
as `2 * max_job_seconds` of latency in the sum.

Common mistake: allocating CPU/RAM share by *arrival rate* (e.g., query is the
fewest, so give it the smallest share). Wrong — give query MORE because each
one matters more. Real ordering of importance per pipeline:

```
protect query SLA   >>   protect interactive SLA   >   feed batch with leftovers
```

A scheduler that completes 99% query at low latency but only 5% batch usually
scores BETTER than one completing 50% across all classes. Do not sacrifice
query completion or query latency to "be fair" to lower priorities.

## Hard Constraints (violating these aborts the simulation)

1. **Per-tick allocation accounting.** `s.executor.pools[i].avail_cpu_pool`
   and `avail_ram_pool` are SNAPSHOTS taken at the start of your scheduler
   call. They do NOT decrement as you append `Assignment` objects. If you
   place multiple `Assignment`s in one call, you MUST track local counters
   yourself and decrement them before each new placement.

   ```python
   local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
   local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]
   for ...:
       cpu, ram = decide_allocation(...)
       if cpu > local_cpu[pool_id] or ram > local_ram[pool_id]:
           continue   # not enough headroom this tick
       assignments.append(Assignment(cpu=cpu, ram=ram, pool_id=pool_id, ...))
       local_cpu[pool_id] -= cpu
       local_ram[pool_id] -= ram
   ```

2. **No overallocation.** Sum of `cpu` across all NEW Assignments to one pool
   must not exceed that pool's `avail_cpu_pool` snapshot (same for ram).
   Violations raise `Overallocated CPU/RAM in assignment` and abort the
   entire scale.

3. **Bounded OOM-retry depth.** If you raise RAM on OOM and retry, cap the
   retry count per operator (e.g., max 5). Above that, mark the pipeline as
   permanently failed. Pool `max_ram_pool` is the hard ceiling — never request
   more. Unbounded retries cause the simulator to hang and the run is killed
   by a wall-clock timeout (which counts as a failure).

4. **Per-tick performance budget.** `scheduler(s, results, pipelines)` is
   called ONCE per simulator tick — typically 100 ticks/sec × 3600s = **360,000
   calls per run**. Wall-clock budget per call is well under 1 ms for typical
   schedulers; >100 ms per call WILL hit the 180 s subprocess timeout. Hard
   rules to keep per-tick work bounded:

   - Per-tick total work should be O(new arrivals + completed results), NOT
     O(waiting queue size) or O(all pipelines). Do not scan the entire
     queue every tick.
   - Cap any inner candidate-scan loop at a small constant (e.g. 32-64).
   - Cache any per-operator derived value the first time you compute it
     rather than recomputing it every tick.
   - Avoid placing huge numbers of Assignments per tick (e.g. setting
     `max_new_assignments_per_tick = 600` is almost never needed; 8-32 is
     usually enough). The simulator runs in steady-state — let work flow
     across ticks.

   A simple, fast scheduler that completes all 5 scales beats a sophisticated,
   slow scheduler that times out. **Prefer simplicity.**

5. **Only assign currently-assignable operators.** You may ONLY put an
   operator into an `Assignment` if, *at this tick*, it is in
   `ASSIGNABLE_STATES` (PENDING or FAILED). An operator's state changes
   between ticks: one you queued earlier may now be ASSIGNED / RUNNING /
   COMPLETED. Assigning a non-assignable operator raises
   `Cannot transition operator ... from running/completed to assigned` and
   aborts the entire scale. Safe pattern: derive the assignable set fresh
   each tick from `pipeline.runtime_status().get_ops(ASSIGNABLE_STATES,
   require_parents_complete=True)`. If you cache `(pipeline, op)` in your own
   queue across ticks, you MUST re-validate the op is still assignable
   immediately before creating its Assignment (e.g. re-check it is in the
   freshly computed assignable set), and drop stale queue entries.

## API Reference

- Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE  -  priority levels (QUERY is highest)
- Assignment(ops=op_list, cpu=cpu_amount, ram=ram_amount, priority=priority, pool_id=pool_id, pipeline_id=pipeline.pipeline_id)  -  to create assignments
- Suspend(container_id, pool_id)  -  to create suspensions
- s.executor.num_pools  -  number of available pools
- s.executor.pools[i].avail_cpu_pool  -  available CPU in pool i
- s.executor.pools[i].avail_ram_pool  -  available RAM in pool i
- s.executor.pools[i].max_cpu_pool  -  max CPU in pool i
- s.executor.pools[i].max_ram_pool  -  max RAM in pool i
- Pipeline has .priority, .pipeline_id, .values (DAG of operators), and .runtime_status() method
- ExecutionResult has .priority, .ops, .container_id, .pool_id, .ram, .cpu, .error, and .failed() method
- pipeline.runtime_status().get_ops(states, require_parents_complete=True/False)  -  get operators matching states
- pipeline.runtime_status().is_pipeline_successful()  -  returns True if all operators completed
- OperatorState enum: PENDING, ASSIGNED, RUNNING, SUSPENDING, COMPLETED, FAILED (ASSIGNABLE_STATES = {PENDING, FAILED})

### Memory is unknown a priori — discover it from OOM feedback

You do NOT get to know an operator's true RAM requirement before running it.
There is no reliable per-operator memory field to read; treat memory as
unknown and learn it reactively:

- Pick a reasonable initial RAM allocation for a new operator.
- If it fails with an OOM-class error (visible in `results` via
  `ExecutionResult.error` / `.failed()`), retry that operator with more RAM.
- Track, per operator (or per pipeline), the largest RAM that has OOM'd so
  far, and allocate above it on the next attempt. Converge upward.
- Respect the bounded-retry rule above (do not retry forever; pool
  `max_ram_pool` is the ceiling).
- Over-allocating wastes capacity and lowers concurrency; under-allocating
  causes OOM and wasted work. The realistic goal is fast convergence to a
  workable size with few OOMs, NOT zero OOMs from perfect foresight.

This OOM-driven discovery loop IS the scheduling problem — do not look for a
shortcut that reveals true memory needs in advance.

Available globals (no imports needed): List, Tuple, Pipeline, OperatorState, ASSIGNABLE_STATES, Assignment, ExecutionResult, Suspend, register_scheduler_init, register_scheduler, Priority

Output ONLY valid Python code. No markdown fences, no explanation.
