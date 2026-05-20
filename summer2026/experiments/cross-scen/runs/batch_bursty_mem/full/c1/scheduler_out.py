@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Ready queues by priority and "remaining ops" bucket (1..MAX_BUCKET).
    s.MAX_BUCKET = 8
    s.ready_q = {
        Priority.QUERY: [[] for _ in range(s.MAX_BUCKET + 1)],
        Priority.INTERACTIVE: [[] for _ in range(s.MAX_BUCKET + 1)],
        Priority.BATCH_PIPELINE: [[] for _ in range(s.MAX_BUCKET + 1)],
    }
    s.ready_head = {
        Priority.QUERY: [0] * (s.MAX_BUCKET + 1),
        Priority.INTERACTIVE: [0] * (s.MAX_BUCKET + 1),
        Priority.BATCH_PIPELINE: [0] * (s.MAX_BUCKET + 1),
    }

    # Pipeline bookkeeping
    s.pipelines_by_id = {}   # pid -> Pipeline
    s.seen_pipelines = set()
    s.dead_pipelines = set()

    # Progress estimates for SRPT-ish bucketing
    s.total_ops = {}         # pid -> int
    s.done_ops = {}          # pid -> int

    # Inflight per pipeline (avoid letting one pipeline explode into many concurrent ops)
    s.inflight = {}          # pid -> int

    # Generation to invalidate stale ready-queue entries (pid, gen)
    s.gen = {}               # pid -> int

    # Op attribution and OOM learning
    # op_key -> (pid, priority, assigned_ram, assigned_cpu)
    s.op_meta = {}

    # (pid, op_key) -> largest RAM that has OOM'd
    s.op_oom_floor = {}

    # (pid, op_key) -> next RAM to try
    s.op_next_ram = {}

    # (pid, op_key) -> retry count
    s.op_retries = {}

    # Tuning knobs
    s.max_new_assignments_per_tick = 48
    s.scan_limit_per_pick = 48

    s.max_oom_retries = 6
    s.max_other_retries = 3

    # If batch is big-memory, defer unless cluster is quiet
    s.batch_big_mem_frac = 0.55

    # Ensure we keep some headroom so queries can start quickly even under batch load
    s.reserve_query_instances = 1
    s.reserve_interactive_instances_when_present = 1


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda out of memory" in msg)

    def _safe_len(x, default):
        try:
            return len(x)
        except Exception:
            return default

    def _q_compact(pri, bucket):
        q = s.ready_q[pri][bucket]
        h = s.ready_head[pri][bucket]
        if h > 64 and h * 2 >= len(q):
            s.ready_q[pri][bucket] = q[h:]
            s.ready_head[pri][bucket] = 0

    def _q_push(pri, bucket, item):
        if bucket < 1:
            bucket = 1
        elif bucket > s.MAX_BUCKET:
            bucket = s.MAX_BUCKET
        s.ready_q[pri][bucket].append(item)

    def _q_pop(pri, bucket):
        h = s.ready_head[pri][bucket]
        q = s.ready_q[pri][bucket]
        if h >= len(q):
            return None
        item = q[h]
        s.ready_head[pri][bucket] = h + 1
        _q_compact(pri, bucket)
        return item

    def _has_ready(pri):
        # Fast check: any queue has items beyond its head (may include stale, but good enough for reserves).
        for b in range(1, s.MAX_BUCKET + 1):
            if s.ready_head[pri][b] < len(s.ready_q[pri][b]):
                return True
        return False

    def _cleanup_pipeline(pid):
        s.seen_pipelines.discard(pid)
        s.pipelines_by_id.pop(pid, None)
        s.total_ops.pop(pid, None)
        s.done_ops.pop(pid, None)
        s.inflight.pop(pid, None)
        s.gen.pop(pid, None)
        s.dead_pipelines.discard(pid)

    def _remaining_bucket(pid):
        tot = s.total_ops.get(pid, 4)
        done = s.done_ops.get(pid, 0)
        rem = tot - done
        if rem < 1:
            rem = 1
        if rem > s.MAX_BUCKET:
            rem = s.MAX_BUCKET
        return rem

    def _enqueue_if_runnable(pid):
        if pid in s.dead_pipelines:
            return
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _cleanup_pipeline(pid)
            return
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return
        g = s.gen.get(pid, 0) + 1
        s.gen[pid] = g
        bucket = _remaining_bucket(pid)
        _q_push(p.priority, bucket, (pid, g))

    def _base_ram(pool, pri):
        # Memory-bound workload: allocate "just enough" and learn upward on OOM.
        # Use pool.max_ram_pool / divisor but cap to avoid absurd allocations on large clusters.
        if pri == Priority.QUERY:
            div = 14.0
            rmin, rmax = 8.0, 128.0
        elif pri == Priority.INTERACTIVE:
            div = 16.0
            rmin, rmax = 8.0, 96.0
        else:
            div = 18.0
            rmin, rmax = 6.0, 80.0

        try:
            base = pool.max_ram_pool / div
        except Exception:
            base = 16.0
        if base < rmin:
            base = rmin
        if base > rmax:
            base = rmax
        return base

    def _base_cpu(pool, pri):
        # Favor concurrency (queueing dominates) while keeping queries snappy.
        if pri == Priority.QUERY:
            div = 16.0
            cmin, cmax = 2.0, 16.0
        elif pri == Priority.INTERACTIVE:
            div = 20.0
            cmin, cmax = 2.0, 12.0
        else:
            div = 24.0
            cmin, cmax = 1.0, 8.0

        try:
            base = pool.max_cpu_pool / div
        except Exception:
            base = 2.0
        # Make it an integer-ish value (sim typically treats as numeric, but integers avoid odd fractions)
        try:
            base = int(base + 0.5)
        except Exception:
            pass
        if base < cmin:
            base = cmin
        if base > cmax:
            base = cmax
        return base

    def _inflight_limit(pri):
        # Allow a bit of pipeline parallelism for high-priority, keep batch conservative.
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _size_for_pool(pool, pid, pri, op):
        opk = _op_key(op)

        # RAM sizing: base -> bump above last OOM -> explicit next hint
        base_ram = _base_ram(pool, pri)
        floor_oom = s.op_oom_floor.get((pid, opk), 0.0)
        ram = base_ram
        if floor_oom and floor_oom > ram:
            ram = floor_oom * 1.6
        nxt = s.op_next_ram.get((pid, opk))
        if nxt is not None and nxt > ram:
            ram = nxt

        # Clamp to pool
        if ram < 1.0:
            ram = 1.0
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        cpu = _base_cpu(pool, pri)
        if cpu < 1.0:
            cpu = 1.0
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool

        return cpu, ram

    # --- Ingest pipelines (treat unseen as arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid in s.seen_pipelines or pid in s.dead_pipelines:
            continue

        s.seen_pipelines.add(pid)

        # Estimate total ops (used for SRPT-ish bucketing)
        tot = _safe_len(getattr(p, "values", None) or [], 4)
        if tot < 1:
            tot = 1
        if tot > s.MAX_BUCKET:
            tot = s.MAX_BUCKET
        s.total_ops[pid] = tot
        s.done_ops[pid] = 0
        s.inflight[pid] = 0
        s.gen[pid] = 0

        _enqueue_if_runnable(pid)

    # --- Process results: update OOM hints, retries, and wake pipelines that may have new runnable ops ---
    dirty_pids = set()

    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            if hasattr(r, "failed"):
                failed = r.failed()
        except Exception:
            failed = False

        for op in ops:
            opk = _op_key(op)
            meta = s.op_meta.pop(opk, None)
            if meta is None:
                continue
            pid, pri, assigned_ram, assigned_cpu = meta

            # Decrement inflight
            infl = s.inflight.get(pid, 0) - 1
            if infl < 0:
                infl = 0
            s.inflight[pid] = infl

            dirty_pids.add(pid)

            if not failed:
                s.done_ops[pid] = s.done_ops.get(pid, 0) + 1
                # On success, clear "next ram" hint (if any) for that op to avoid keeping inflated values around.
                s.op_next_ram.pop((pid, opk), None)
                s.op_retries.pop((pid, opk), None)
                continue

            # Failure path
            err = getattr(r, "error", None)
            is_oom = _is_oom_error(err)

            retries = s.op_retries.get((pid, opk), 0) + 1
            s.op_retries[(pid, opk)] = retries

            # Prefer r.ram if present; else use what we assigned
            try:
                alloc = getattr(r, "ram", None)
                if alloc is None or alloc <= 0:
                    alloc = assigned_ram
            except Exception:
                alloc = assigned_ram

            if is_oom:
                prev_floor = s.op_oom_floor.get((pid, opk), 0.0)
                if alloc is not None and alloc > prev_floor:
                    s.op_oom_floor[(pid, opk)] = alloc

                # Grow aggressively to converge fast, but bounded by pool max at assignment time.
                # Use a mix of last alloc and floor.
                floor_val = s.op_oom_floor.get((pid, opk), 0.0)
                nxt = max(floor_val * 2.0, (alloc or 1.0) * 2.0, 1.0)
                s.op_next_ram[(pid, opk)] = nxt

                if retries > s.max_oom_retries:
                    s.dead_pipelines.add(pid)
            else:
                if retries > s.max_other_retries:
                    s.dead_pipelines.add(pid)

    # Wake pipelines touched by results
    for pid in dirty_pids:
        if pid in s.dead_pipelines:
            continue
        _enqueue_if_runnable(pid)

    # --- Build local resource snapshots (must self-account for multiple assignments) ---
    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    def _reserve_for_pool(pool_id, interactive_present):
        pool = s.executor.pools[pool_id]
        q_cpu = _base_cpu(pool, Priority.QUERY)
        q_ram = _base_ram(pool, Priority.QUERY)

        reserve_cpu = q_cpu * s.reserve_query_instances
        reserve_ram = q_ram * s.reserve_query_instances

        if interactive_present:
            i_cpu = _base_cpu(pool, Priority.INTERACTIVE)
            i_ram = _base_ram(pool, Priority.INTERACTIVE)
            reserve_cpu += i_cpu * s.reserve_interactive_instances_when_present
            reserve_ram += i_ram * s.reserve_interactive_instances_when_present

        # Clamp reserves so they don't exceed pool capacity
        if reserve_cpu > pool.max_cpu_pool:
            reserve_cpu = pool.max_cpu_pool
        if reserve_ram > pool.max_ram_pool:
            reserve_ram = pool.max_ram_pool
        return reserve_cpu, reserve_ram

    # Batch reserves depend on whether there's any high-priority runnable work right now
    interactive_present = _has_ready(Priority.INTERACTIVE)
    query_present = _has_ready(Priority.QUERY)
    high_present = query_present or interactive_present

    def _pick_entry(pri):
        # Pop the smallest remaining bucket; skip stale entries. Bounded work.
        scans = 0
        while scans < s.scan_limit_per_pick:
            picked_any_bucket = False
            for b in range(1, s.MAX_BUCKET + 1):
                item = _q_pop(pri, b)
                if item is None:
                    continue
                picked_any_bucket = True
                pid, g = item

                # Stale?
                if s.gen.get(pid, -1) != g:
                    scans += 1
                    continue
                if pid in s.dead_pipelines:
                    scans += 1
                    continue
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    scans += 1
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _cleanup_pipeline(pid)
                    scans += 1
                    continue

                # Inflight limit
                if s.inflight.get(pid, 0) >= _inflight_limit(pri):
                    # Requeue same generation; don't churn the gen counter.
                    _q_push(pri, b, (pid, g))
                    scans += 1
                    continue

                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    scans += 1
                    continue

                return pid, g, b, p, ops
            if not picked_any_bucket:
                return None
            scans += 1
        return None

    def _choose_pool_for(pid, pri, op, ops_len):
        # Return (pool_id, cpu, ram) or (None, None, None)
        best = None
        best_score = None

        for pool_id in range(num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = local_cpu[pool_id]
            avail_ram = local_ram[pool_id]

            if avail_cpu < 1 or avail_ram < 1:
                continue

            cpu, ram = _size_for_pool(pool, pid, pri, op)

            # Adjust CPU down to availability (CPU under-alloc doesn't fail)
            if cpu > avail_cpu:
                cpu = avail_cpu
            if cpu < 1:
                continue

            # RAM must fit or we skip (under-alloc risks OOM; we learn upward via OOM).
            if ram > avail_ram:
                continue

            # Batch: avoid scheduling huge-memory batch ops unless cluster is quiet
            if pri == Priority.BATCH_PIPELINE:
                if ram >= pool.max_ram_pool * s.batch_big_mem_frac and high_present:
                    continue

                reserve_cpu, reserve_ram = _reserve_for_pool(pool_id, interactive_present)
                # Keep headroom for high priority work
                if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                    continue

            # Choose pool: high priority -> emptiest; batch -> best fit
            cpu_left = avail_cpu - cpu
            ram_left = avail_ram - ram

            if pri == Priority.BATCH_PIPELINE:
                score = ram_left * 1000.0 + cpu_left  # minimize waste
                if best is None or score < best_score:
                    best = (pool_id, cpu, ram)
                    best_score = score
            else:
                score = cpu_left * 1000.0 + ram_left  # prefer more headroom
                if best is None or score > best_score:
                    best = (pool_id, cpu, ram)
                    best_score = score

        return best if best is not None else (None, None, None)

    assignments = []
    suspensions = []
    scheduled_this_tick = set()

    # Main scheduling loop: always try QUERY -> INTERACTIVE -> BATCH, but backfill batch opportunistically.
    priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    while len(assignments) < s.max_new_assignments_per_tick:
        made_progress = False

        for pri in priorities:
            picked = _pick_entry(pri)
            if picked is None:
                continue
            pid, g, bucket, p, ops = picked

            if pid in scheduled_this_tick:
                # Requeue without bumping gen
                _q_push(pri, bucket, (pid, g))
                continue

            op = ops[0]
            pool_id, cpu, ram = _choose_pool_for(pid, pri, op, len(ops))
            if pool_id is None:
                # Couldn't place now; requeue same generation
                _q_push(pri, bucket, (pid, g))
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Tick-local accounting
            local_cpu[pool_id] -= cpu
            local_ram[pool_id] -= ram

            # Bookkeeping for result attribution and inflight limits
            opk = _op_key(op)
            s.op_meta[opk] = (pid, pri, ram, cpu)
            s.inflight[pid] = s.inflight.get(pid, 0) + 1
            scheduled_this_tick.add(pid)

            # If there are multiple runnable ops right now, re-enqueue so we can exploit DAG parallelism.
            # Also, if this is batch and we didn't fill the pool, let it re-enter to keep throughput up.
            if len(ops) > 1:
                _q_push(pri, bucket, (pid, g))

            made_progress = True
            break  # restart from highest priority after any successful placement

        if not made_progress:
            break

    return suspensions, assignments
