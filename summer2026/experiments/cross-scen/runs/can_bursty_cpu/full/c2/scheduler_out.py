@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Active pipeline registry
    s.pipeline_by_id = {}   # pipeline_id -> Pipeline
    s.active_pipelines = set()
    s.dead_pipelines = set()

    # Runnable queues by priority (event-driven)
    s.runnable = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.runnable_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.runnable_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Deficit-based sharing between priorities (query still effectively wins when present)
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 2,  # slightly higher than weight(1) to avoid batch incompletion penalties
    }
    s.deficit_cap = {
        Priority.QUERY: 60,
        Priority.INTERACTIVE: 60,
        Priority.BATCH_PIPELINE: 40,
    }

    # Inflight resource tracking (approx; admission / batch gating)
    s.inflight_cpu = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.inflight_ram = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }

    # OOM learning (per operator object identity)
    # key: (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}
    s.op_oom_count = {}     # key -> int
    s.op_fail_count = {}    # key -> int (non-OOM)

    # Map op_key -> pipeline_id for attributing results (op_key is id(op))
    s.op_to_pipeline = {}

    # Remember last allocation for an op (helps when result.ram is missing)
    s.op_last_alloc = {}    # key -> (cpu, ram, priority)

    # Global pool capacities (for retry caps / heuristics)
    s.total_cpu_capacity = 0.0
    s.max_ram_any_pool = 0.0
    for i in range(s.executor.num_pools):
        p = s.executor.pools[i]
        s.total_cpu_capacity += float(p.max_cpu_pool)
        if float(p.max_ram_pool) > s.max_ram_any_pool:
            s.max_ram_any_pool = float(p.max_ram_pool)

    # Knobs (kept simple + fast)
    s.max_new_assignments_per_tick = 64
    s.pick_attempts_per_assignment = 6  # bound per-assignment priority/op selection work
    s.max_oom_retries = 5
    s.max_non_oom_retries = 2

    # Batch gating: keep headroom for interactive/query without preemption
    s.hp_reserve_frac = 0.25  # reserve ~25% CPU for high priority when HP exists


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_runnable(pid, pri):
        if pid in s.dead_pipelines or pid not in s.active_pipelines:
            return
        st = s.runnable_set[pri]
        if pid in st:
            return
        s.runnable[pri].append(pid)
        st.add(pid)

    def _dequeue_runnable(pri):
        q = s.runnable[pri]
        head = s.runnable_head[pri]
        st = s.runnable_set[pri]

        # Consume until we find a currently-queued pid (set membership) and return it
        # (Set membership handles rare duplicates / stale ids cheaply.)
        while head < len(q):
            pid = q[head]
            head += 1
            if pid in st:
                st.discard(pid)
                s.runnable_head[pri] = head

                # Periodic compaction to keep lists small
                if head > 64 and head > (len(q) // 2):
                    s.runnable[pri] = q[head:]
                    s.runnable_head[pri] = 0
                return pid

        # Queue exhausted
        s.runnable_head[pri] = head
        if head > 64:
            s.runnable[pri] = []
            s.runnable_head[pri] = 0
        return None

    def _maybe_mark_done_or_runnable(p):
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid not in s.active_pipelines:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            s.active_pipelines.discard(pid)
            return
        # If it has runnable ops now, enqueue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            _enqueue_runnable(pid, p.priority)

    def _base_cpu(pri):
        # Critical change vs previous version: DO NOT scale per-op CPU with cluster size.
        # Keep moderate sizes to maximize throughput under sublinear scaling.
        if pri == Priority.QUERY:
            return 8.0
        if pri == Priority.INTERACTIVE:
            return 6.0
        return 4.0

    def _max_cpu(pri):
        if pri == Priority.QUERY:
            return 16.0
        if pri == Priority.INTERACTIVE:
            return 12.0
        return 8.0

    def _base_ram(pri):
        # Start small; learn upward on OOM.
        if pri == Priority.QUERY:
            return 12.0
        if pri == Priority.INTERACTIVE:
            return 10.0
        return 8.0

    def _size_request(pri, pid, op):
        opk = _op_key(op)
        key = (pid, opk)

        cpu = _base_cpu(pri)
        cpu_cap = _max_cpu(pri)
        if cpu > cpu_cap:
            cpu = cpu_cap
        if cpu < 1.0:
            cpu = 1.0

        # RAM hinting for OOM retries
        hint = s.op_ram_hint.get(key)
        if hint is None:
            ram = _base_ram(pri)
        else:
            # add small safety margin to avoid repeated near-threshold OOMs
            ram = float(hint) * 1.10 + 1.0
            if ram < _base_ram(pri):
                ram = _base_ram(pri)

        if ram < 1.0:
            ram = 1.0
        if ram > s.max_ram_any_pool:
            ram = s.max_ram_any_pool

        return cpu, ram

    def _select_pool(pri, cpu_req, ram_req, local_cpu, local_ram):
        # For HP, prefer the pool with most CPU headroom; for batch, tighter packing.
        best_pool = None
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            best_cpu = -1.0
            for i in range(s.executor.num_pools):
                if local_cpu[i] >= cpu_req and local_ram[i] >= ram_req:
                    if local_cpu[i] > best_cpu:
                        best_cpu = local_cpu[i]
                        best_pool = i
            return best_pool

        # Batch: best-fit on CPU then RAM
        best_left_cpu = None
        best_left_ram = None
        for i in range(s.executor.num_pools):
            if local_cpu[i] >= cpu_req and local_ram[i] >= ram_req:
                left_cpu = local_cpu[i] - cpu_req
                left_ram = local_ram[i] - ram_req
                if best_pool is None:
                    best_pool = i
                    best_left_cpu = left_cpu
                    best_left_ram = left_ram
                else:
                    if left_cpu < best_left_cpu or (left_cpu == best_left_cpu and left_ram < best_left_ram):
                        best_pool = i
                        best_left_cpu = left_cpu
                        best_left_ram = left_ram
        return best_pool

    # --- Ingest new pipelines (assumed: arrivals for this tick) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid in s.active_pipelines:
            continue
        s.pipeline_by_id[pid] = p
        s.active_pipelines.add(pid)
        _maybe_mark_done_or_runnable(p)

    # --- Process results (update inflight + OOM learning + enqueue next runnable ops) ---
    for r in results:
        pri = getattr(r, "priority", None)
        rcpu = getattr(r, "cpu", None)
        rram = getattr(r, "ram", None)

        if pri in s.inflight_cpu:
            if rcpu is not None:
                s.inflight_cpu[pri] -= float(rcpu)
                if s.inflight_cpu[pri] < 0.0:
                    s.inflight_cpu[pri] = 0.0
            if rram is not None:
                s.inflight_ram[pri] -= float(rram)
                if s.inflight_ram[pri] < 0.0:
                    s.inflight_ram[pri] = 0.0

        failed = False
        if hasattr(r, "failed") and callable(getattr(r, "failed")):
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

        # Update per-op hints
        ops_list = getattr(r, "ops", None) or []
        if failed:
            is_oom = _is_oom_error(getattr(r, "error", None))
            for op in ops_list:
                opk = _op_key(op)
                pid = s.op_to_pipeline.get(opk)
                if pid is None or pid in s.dead_pipelines or pid not in s.active_pipelines:
                    continue
                key = (pid, opk)

                if is_oom:
                    prev = s.op_ram_hint.get(key)
                    oomc = s.op_oom_count.get(key, 0) + 1
                    s.op_oom_count[key] = oomc

                    # Use result.ram if present, else last allocation, else a small base.
                    alloc_ram = getattr(r, "ram", None)
                    if alloc_ram is None or float(alloc_ram) <= 0.0:
                        last = s.op_last_alloc.get(key)
                        alloc_ram = last[1] if last is not None else _base_ram(s.pipeline_by_id[pid].priority)

                    base = float(alloc_ram)
                    if prev is not None and float(prev) > base:
                        base = float(prev)

                    if oomc == s.max_oom_retries:
                        new_hint = s.max_ram_any_pool
                    elif oomc > s.max_oom_retries:
                        # Give up on this pipeline to avoid infinite retry loops.
                        s.dead_pipelines.add(pid)
                        s.active_pipelines.discard(pid)
                        continue
                    else:
                        # Aggressive convergence upward; still bounded by pool max.
                        new_hint = base * 2.0 + 2.0

                    if new_hint > s.max_ram_any_pool:
                        new_hint = s.max_ram_any_pool
                    if new_hint < 1.0:
                        new_hint = 1.0
                    s.op_ram_hint[key] = new_hint

                    # Make sure the pipeline is reconsidered for retry ASAP
                    p = s.pipeline_by_id.get(pid)
                    if p is not None:
                        _maybe_mark_done_or_runnable(p)
                else:
                    # Non-OOM failure: allow a couple retries, then give up.
                    fc = s.op_fail_count.get(key, 0) + 1
                    s.op_fail_count[key] = fc
                    if fc > s.max_non_oom_retries:
                        s.dead_pipelines.add(pid)
                        s.active_pipelines.discard(pid)
                        continue
                    p = s.pipeline_by_id.get(pid)
                    if p is not None:
                        _maybe_mark_done_or_runnable(p)
        else:
            # Success completion may unblock successors: enqueue pipeline if it has more runnable ops.
            # Attribute to a pipeline via any op in the result.
            pid = None
            for op in ops_list:
                opk = _op_key(op)
                pid = s.op_to_pipeline.get(opk)
                if pid is not None:
                    break
            if pid is not None:
                p = s.pipeline_by_id.get(pid)
                if p is not None:
                    _maybe_mark_done_or_runnable(p)

    # Refresh deficits each tick
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        d = s.deficit[pri] + s.quantum[pri]
        cap = s.deficit_cap[pri]
        s.deficit[pri] = d if d < cap else cap

    # Compute batch CPU cap (keep headroom for HP without preemption)
    hp_present = False
    if s.runnable_set[Priority.QUERY] or s.runnable_set[Priority.INTERACTIVE]:
        hp_present = True
    elif (s.inflight_cpu[Priority.QUERY] + s.inflight_cpu[Priority.INTERACTIVE]) > 0.0:
        hp_present = True

    if hp_present:
        batch_cpu_cap = s.total_cpu_capacity * (1.0 - s.hp_reserve_frac)
    else:
        batch_cpu_cap = s.total_cpu_capacity * 0.95

    suspensions = []
    assignments = []

    # Tick-local resource snapshots
    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    # One-op-per-pipeline-per-tick: defer re-enqueue until end of tick
    scheduled_this_tick = set()
    deferred = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    def _any_pool_has_capacity():
        for i in range(s.executor.num_pools):
            if local_cpu[i] >= 1.0 and local_ram[i] >= 1.0:
                return True
        return False

    def _choose_priority():
        # Strict preference when query has runnable work and deficit
        if s.deficit[Priority.QUERY] > 0 and s.runnable_set[Priority.QUERY]:
            return Priority.QUERY

        # Otherwise choose between interactive and batch via deficit, with batch gating
        inter_ok = (s.deficit[Priority.INTERACTIVE] > 0 and s.runnable_set[Priority.INTERACTIVE])
        batch_ok = (s.deficit[Priority.BATCH_PIPELINE] > 0 and s.runnable_set[Priority.BATCH_PIPELINE] and
                    (s.inflight_cpu[Priority.BATCH_PIPELINE] < batch_cpu_cap))

        if inter_ok and batch_ok:
            # Pick the one with larger deficit; tie-break to interactive
            if s.deficit[Priority.BATCH_PIPELINE] > s.deficit[Priority.INTERACTIVE]:
                return Priority.BATCH_PIPELINE
            return Priority.INTERACTIVE
        if inter_ok:
            return Priority.INTERACTIVE
        if batch_ok:
            return Priority.BATCH_PIPELINE

        # Fallback: allow query even if deficit is 0 (rare) to avoid stalling
        if s.runnable_set[Priority.QUERY]:
            return Priority.QUERY
        if s.runnable_set[Priority.INTERACTIVE]:
            return Priority.INTERACTIVE
        if s.runnable_set[Priority.BATCH_PIPELINE] and (s.inflight_cpu[Priority.BATCH_PIPELINE] < batch_cpu_cap):
            return Priority.BATCH_PIPELINE
        return None

    # --- Main scheduling loop ---
    new_assigned = 0
    while new_assigned < s.max_new_assignments_per_tick and _any_pool_has_capacity():
        pri = _choose_priority()
        if pri is None:
            break

        placed = False

        # Try a few times to find a schedulable pipeline/op for this priority
        attempts = 0
        while attempts < s.pick_attempts_per_assignment:
            attempts += 1
            pid = _dequeue_runnable(pri)
            if pid is None:
                break

            if pid in s.dead_pipelines or pid not in s.active_pipelines:
                continue
            if pid in scheduled_this_tick:
                deferred[pri].append(pid)
                continue

            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.active_pipelines.discard(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            cpu_req, ram_req = _size_request(pri, pid, op)

            # If batch is currently gated, don't schedule it.
            if pri == Priority.BATCH_PIPELINE and s.inflight_cpu[Priority.BATCH_PIPELINE] >= batch_cpu_cap:
                deferred[pri].append(pid)
                break

            # Place: shrink CPU if needed (avoid idle due to fragmentation)
            cpu_try = cpu_req
            pool_id = _select_pool(pri, cpu_try, ram_req, local_cpu, local_ram)
            while pool_id is None and cpu_try > 1.0:
                cpu_try = max(1.0, float(int(cpu_try // 2)) if cpu_try >= 2.0 else 1.0)
                pool_id = _select_pool(pri, cpu_try, ram_req, local_cpu, local_ram)

            if pool_id is None:
                # Can't place now; try later
                deferred[pri].append(pid)
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_try,
                    ram=ram_req,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Bookkeeping
            local_cpu[pool_id] -= cpu_try
            local_ram[pool_id] -= ram_req
            if local_cpu[pool_id] < 0.0:
                local_cpu[pool_id] = 0.0
            if local_ram[pool_id] < 0.0:
                local_ram[pool_id] = 0.0

            scheduled_this_tick.add(pid)
            deferred[pri].append(pid)  # reconsider next tick if more ops become runnable

            opk = _op_key(op)
            s.op_to_pipeline[opk] = pid
            s.op_last_alloc[(pid, opk)] = (cpu_try, ram_req, pri)

            s.inflight_cpu[pri] += cpu_try
            s.inflight_ram[pri] += ram_req

            if s.deficit[pri] > 0:
                s.deficit[pri] -= 1

            new_assigned += 1
            placed = True
            break

        if not placed:
            # If we couldn't place work for this chosen priority, reduce its deficit a bit to avoid spinning.
            if pri is not None and s.deficit.get(pri, 0) > 0:
                s.deficit[pri] -= 1

            # If nothing runnable/placable, stop.
            # (Next tick, results may free resources or unblock ops.)
            if not (s.runnable_set[Priority.QUERY] or s.runnable_set[Priority.INTERACTIVE] or s.runnable_set[Priority.BATCH_PIPELINE]):
                break

    # Re-enqueue deferred pipelines if they are still runnable (lazy re-check)
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        dlist = deferred[pri]
        for pid in dlist:
            if pid in s.dead_pipelines or pid not in s.active_pipelines:
                continue
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.active_pipelines.discard(pid)
                continue
            # Only enqueue if it currently has runnable ops; otherwise results will re-enqueue.
            if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                _enqueue_runnable(pid, pri)

    return suspensions, assignments
