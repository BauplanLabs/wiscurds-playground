@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority queues implemented as list + head index (amortized O(1) pop/push without imports).
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.qh = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Active pipelines we still intend to schedule: pipeline_id -> Pipeline
    s.active = {}

    # Active counts per priority (exact; maintained on enqueue/drop).
    s.backlog = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # pipeline_id -> meta
    # meta: { "enq": tick, "next_check": tick }
    s.pmeta = {}

    # op_key -> pipeline_id (for attributing results)
    s.op_to_pid = {}

    # (pipeline_id, op_key) -> op meta
    # meta: { "oom_floor": float, "next_ram": float, "oom_tries": int, "fail_tries": int }
    s.opmeta = {}

    # Pipelines we consider hopeless (stop scheduling).
    s.dead = set()

    # Per-priority RAM default multipliers, adapted slowly based on OOM feedback.
    s.pri_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.pri_oom_ct = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.pri_ok_ct = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Weighted round-robin order (interactive gets the most share; protect query via urgency + reserve).
    s.rr_order = [
        Priority.QUERY, Priority.QUERY,
        Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE, Priority.BATCH_PIPELINE,
        Priority.INTERACTIVE, Priority.INTERACTIVE,
    ]
    s.rr_i = 0

    # Knobs (keep per-tick work bounded)
    s.scan_cap = 32
    s.max_new_assignments_per_tick = 48

    # Backoff (in ticks) before re-checking pipelines with no runnable ops
    s.blocked_backoff = 8

    # Retry limits
    s.max_oom_tries = 5
    s.max_non_oom_tries = 2

    # Urgency thresholds (ticks). NOTE: simulator tick is typically ~0.01s, so 50 ticks ~0.5s.
    s.query_urgent_wait = 60
    s.interactive_urgent_wait = 200

    # Periodic queue compaction thresholds
    s.compact_every = 500
    s.compact_head_threshold = 256


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return (
            ("oom" in msg) or
            ("out of memory" in msg) or
            ("memoryerror" in msg) or
            ("killed" in msg and "memory" in msg)
        )

    def _q_len(pri):
        q = s.q[pri]
        h = s.qh[pri]
        n = len(q) - h
        return n if n > 0 else 0

    def _q_push(pri, pid):
        s.q[pri].append(pid)

    def _q_pop(pri):
        h = s.qh[pri]
        q = s.q[pri]
        if h >= len(q):
            return None
        pid = q[h]
        s.qh[pri] = h + 1
        return pid

    def _maybe_compact(pri):
        # Compact occasionally to keep memory bounded; avoid doing this too often.
        h = s.qh[pri]
        q = s.q[pri]
        if h >= s.compact_head_threshold and h >= (len(q) // 2):
            s.q[pri] = q[h:]
            s.qh[pri] = 0

    def _drop_pipeline(pid):
        p = s.active.pop(pid, None)
        if p is not None:
            pri = p.priority
            s.backlog[pri] = max(0, s.backlog[pri] - 1)
        s.pmeta.pop(pid, None)

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.dead:
            return
        if pid in s.active:
            return
        s.active[pid] = p
        s.backlog[p.priority] += 1
        s.pmeta[pid] = {"enq": s.ticks, "next_check": 0}
        _q_push(p.priority, pid)

    def _oldest_wait_ticks(pri):
        # Fast-ish estimate: scan up to scan_cap queue entries to find oldest active.
        # (Avoid scanning whole backlog per tick.)
        qn = _q_len(pri)
        if qn <= 0:
            return 0
        oldest = None
        # Look at a small prefix of the queue (pop+push to rotate and clean stales).
        k = s.scan_cap if qn > s.scan_cap else qn
        for _ in range(k):
            pid = _q_pop(pri)
            if pid is None:
                break
            p = s.active.get(pid)
            if p is None or pid in s.dead:
                continue
            meta = s.pmeta.get(pid)
            if meta is not None:
                w = s.ticks - meta.get("enq", s.ticks)
                if oldest is None or w > oldest:
                    oldest = w
            _q_push(pri, pid)
        _maybe_compact(pri)
        return oldest or 0

    def _base_cpu(pool, pri):
        mcpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            cpu = int(mcpu * 0.06)
            if cpu < 3:
                cpu = 3
            if cpu > 8:
                cpu = 8
            return cpu
        if pri == Priority.INTERACTIVE:
            cpu = int(mcpu * 0.04)
            if cpu < 2:
                cpu = 2
            if cpu > 6:
                cpu = 6
            return cpu
        cpu = int(mcpu * 0.02)
        if cpu < 1:
            cpu = 1
        if cpu > 4:
            cpu = 4
        return cpu

    def _base_ram(pool, pri):
        mram = pool.max_ram_pool
        mult = s.pri_ram_mult.get(pri, 1.0)
        if pri == Priority.QUERY:
            # ~4% of pool, capped
            ram = mram * 0.04
            if ram < 16:
                ram = 16
            if ram > 64:
                ram = 64
            return ram * mult
        if pri == Priority.INTERACTIVE:
            ram = mram * 0.03
            if ram < 12:
                ram = 12
            if ram > 56:
                ram = 56
            return ram * mult
        ram = mram * 0.02
        if ram < 8:
            ram = 8
        if ram > 48:
            ram = 48
        return ram * mult

    def _reserve_for_query(pool):
        # Keep a small headroom for query so it can start quickly without preemption.
        rcpu = int(pool.max_cpu_pool * 0.10)
        if rcpu < 4:
            rcpu = 4
        if rcpu > 16:
            rcpu = 16

        rram = pool.max_ram_pool * 0.05
        if rram < 24:
            rram = 24
        if rram > 128:
            rram = 128
        return rcpu, rram

    # --- Ingest new pipelines (assume arrivals since last tick) ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process results (update OOM hints, unblock pipelines sooner, adapt defaults) ---
    for r in results:
        pri = getattr(r, "priority", None)
        ops = getattr(r, "ops", None) or []
        failed = (hasattr(r, "failed") and r.failed())

        if pri in s.pri_ram_mult:
            if failed and _is_oom_error(getattr(r, "error", None)):
                s.pri_oom_ct[pri] += 1
            elif not failed:
                s.pri_ok_ct[pri] += 1

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pid.pop(opk, None)
            if pid is None:
                continue

            # Make the pipeline eligible for checking soon after any op finishes/fails.
            meta = s.pmeta.get(pid)
            if meta is not None:
                meta["next_check"] = 0

            if pid in s.dead:
                continue

            key = (pid, opk)
            om = s.opmeta.get(key)
            if om is None:
                om = {"oom_floor": 0.0, "next_ram": 0.0, "oom_tries": 0, "fail_tries": 0}
                s.opmeta[key] = om

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    om["oom_tries"] += 1
                    alloc = getattr(r, "ram", None)
                    if alloc is None or alloc <= 0:
                        alloc = om["oom_floor"] if om["oom_floor"] > 0 else 1.0
                    if alloc > om["oom_floor"]:
                        om["oom_floor"] = float(alloc)

                    # Cap by pool max RAM where it ran.
                    pool_id = getattr(r, "pool_id", 0)
                    if pool_id is None:
                        pool_id = 0
                    if pool_id < 0 or pool_id >= s.executor.num_pools:
                        pool_id = 0
                    max_ram = s.executor.pools[pool_id].max_ram_pool

                    # Aggressive but not insane growth (fast convergence).
                    floor = om["oom_floor"]
                    grow = max(floor * 1.7, floor + 12.0, float(alloc) * 1.5)
                    if grow > max_ram:
                        grow = max_ram
                    if grow < 1:
                        grow = 1.0
                    om["next_ram"] = float(grow)

                    # If we hit too many OOMs, we still allow one final max-RAM attempt;
                    # if that also fails, we mark pipeline dead below on the next failure.
                else:
                    om["fail_tries"] += 1
                    if om["fail_tries"] > s.max_non_oom_tries:
                        s.dead.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success: clear per-op meta to keep state bounded.
                s.opmeta.pop(key, None)

    # --- Adapt per-priority RAM multipliers periodically (bounded, slow) ---
    if s.ticks % 250 == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            oom = s.pri_oom_ct.get(pri, 0)
            ok = s.pri_ok_ct.get(pri, 0)
            tot = oom + ok
            if tot > 0:
                rate = oom / float(tot)
                m = s.pri_ram_mult.get(pri, 1.0)
                if rate > 0.05:
                    m *= 1.15
                elif rate < 0.01 and m > 0.9:
                    m *= 0.98
                if m < 0.6:
                    m = 0.6
                if m > 3.0:
                    m = 3.0
                s.pri_ram_mult[pri] = m
            s.pri_oom_ct[pri] = 0
            s.pri_ok_ct[pri] = 0

    # --- Periodic queue compaction ---
    if s.ticks % s.compact_every == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            _maybe_compact(pri)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    # Tick-local tracking to prevent duplicate assignment of the same op (pipeline state doesn't update until next tick)
    scheduled_ops = set()
    per_pid_ct = {}

    # Urgency signals
    query_oldest = _oldest_wait_ticks(Priority.QUERY) if s.backlog[Priority.QUERY] > 0 else 0
    interactive_oldest = _oldest_wait_ticks(Priority.INTERACTIVE) if s.backlog[Priority.INTERACTIVE] > 0 else 0

    def _pick_priority():
        # Urgent query always wins; urgent interactive gets a bump; otherwise weighted RR.
        nonlocal query_oldest, interactive_oldest
        if s.backlog[Priority.QUERY] > 0 and query_oldest >= s.query_urgent_wait:
            return Priority.QUERY
        if s.backlog[Priority.INTERACTIVE] > 0 and interactive_oldest >= s.interactive_urgent_wait:
            # Interleave interactive more heavily when it builds up tail.
            return Priority.INTERACTIVE
        pri = s.rr_order[s.rr_i]
        s.rr_i += 1
        if s.rr_i >= len(s.rr_order):
            s.rr_i = 0
        return pri

    def _pipeline_tick_limit(pri):
        if pri == Priority.BATCH_PIPELINE:
            return 1
        return 2  # allow a little more for query/interactive, supports parallel DAGs

    def _get_runnable_op(p, pid):
        # Respect backoff to avoid repeatedly scanning blocked pipelines.
        meta = s.pmeta.get(pid)
        if meta is not None:
            nxt = meta.get("next_check", 0)
            if nxt and s.ticks < nxt:
                return None

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _drop_pipeline(pid)
            return None

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            if meta is not None:
                meta["next_check"] = s.ticks + s.blocked_backoff
            return None

        # Choose the first assignable op not already scheduled this tick.
        # Limit scan to a few to keep per-pick work bounded.
        lim = 4 if len(ops) > 4 else len(ops)
        for i in range(lim):
            op = ops[i]
            opk = _op_key(op)
            if opk in scheduled_ops:
                continue
            return op
        return None

    def _size_op(pool, pid, p, op):
        pri = p.priority
        cpu = _base_cpu(pool, pri)
        ram = _base_ram(pool, pri)

        opk = _op_key(op)
        om = s.opmeta.get((pid, opk))
        if om is not None:
            # If we have a next_ram from previous OOM, respect it.
            nr = om.get("next_ram", 0.0) or 0.0
            floor = om.get("oom_floor", 0.0) or 0.0
            if nr > ram:
                ram = nr
            if floor > 0 and ram < floor * 1.05:
                ram = floor * 1.05

            # If we've already OOM'd too many times, make the next attempt a "final" max-RAM shot.
            if (om.get("oom_tries", 0) or 0) >= s.max_oom_tries:
                ram = pool.max_ram_pool

        if cpu < 1:
            cpu = 1
        if ram < 1:
            ram = 1

        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        return cpu, ram

    # Local availability snapshots (must decrement ourselves)
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # Schedule across pools; keep per-tick assignment count bounded.
    max_new = s.max_new_assignments_per_tick
    made = 0

    for pool_id in range(s.executor.num_pools):
        if made >= max_new:
            break
        pool = s.executor.pools[pool_id]

        # Query reserve (only if query backlog exists)
        q_res_cpu, q_res_ram = _reserve_for_query(pool) if s.backlog[Priority.QUERY] > 0 else (0, 0)

        # Fill this pool with small containers to increase parallelism.
        # Bound the inner loop to protect per-tick runtime.
        inner_cap = 24
        inner = 0
        while inner < inner_cap and made < max_new:
            inner += 1
            if local_cpu[pool_id] < 1 or local_ram[pool_id] < 1:
                break

            pri_try = _pick_priority()

            # Try up to 3 priorities to find a fit without scanning too much.
            tried = 0
            assigned = False
            pri = pri_try
            while tried < 3 and not assigned:
                tried += 1

                if s.backlog.get(pri, 0) <= 0:
                    # Fall back to next RR choice.
                    pri = _pick_priority()
                    continue

                # Scan a bounded number of pipelines from this priority queue
                qn = _q_len(pri)
                if qn <= 0:
                    pri = _pick_priority()
                    continue

                k = s.scan_cap if qn > s.scan_cap else qn
                picked_pid = None
                picked_op = None
                picked_p = None
                picked_cpu = None
                picked_ram = None

                for _ in range(k):
                    pid = _q_pop(pri)
                    if pid is None:
                        break
                    p = s.active.get(pid)
                    if p is None or pid in s.dead:
                        continue

                    # Per-pipeline per-tick cap
                    lim = _pipeline_tick_limit(p.priority)
                    if per_pid_ct.get(pid, 0) >= lim:
                        _q_push(pri, pid)
                        continue

                    op = _get_runnable_op(p, pid)
                    if op is None:
                        _q_push(pri, pid)
                        continue

                    cpu_req, ram_req = _size_op(pool, pid, p, op)

                    # Fit check with availability
                    if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                        # If it doesn't fit now, keep it in rotation.
                        _q_push(pri, pid)
                        continue

                    # Protect query headroom: don't let lower priorities consume the last reserved chunk.
                    if p.priority != Priority.QUERY and s.backlog[Priority.QUERY] > 0:
                        if (local_cpu[pool_id] - cpu_req) < q_res_cpu and local_cpu[pool_id] >= q_res_cpu:
                            _q_push(pri, pid)
                            continue
                        if (local_ram[pool_id] - ram_req) < q_res_ram and local_ram[pool_id] >= q_res_ram:
                            _q_push(pri, pid)
                            continue

                    picked_pid = pid
                    picked_p = p
                    picked_op = op
                    picked_cpu = cpu_req
                    picked_ram = ram_req
                    _q_push(pri, pid)
                    break

                _maybe_compact(pri)

                if picked_p is None:
                    pri = _pick_priority()
                    continue

                opk = _op_key(picked_op)
                if opk in scheduled_ops:
                    pri = _pick_priority()
                    continue

                # Create assignment
                assignments.append(
                    Assignment(
                        ops=[picked_op],
                        cpu=picked_cpu,
                        ram=picked_ram,
                        priority=picked_p.priority,
                        pool_id=pool_id,
                        pipeline_id=picked_pid,
                    )
                )

                # Bookkeeping
                scheduled_ops.add(opk)
                per_pid_ct[picked_pid] = per_pid_ct.get(picked_pid, 0) + 1
                s.op_to_pid[opk] = picked_pid

                meta = s.pmeta.get(picked_pid)
                if meta is not None:
                    meta["next_check"] = s.ticks + 1  # avoid immediate re-scan within next tick

                local_cpu[pool_id] -= picked_cpu
                local_ram[pool_id] -= picked_ram

                made += 1
                assigned = True

            if not assigned:
                # Couldn't find any fitting work quickly; stop trying to fill this pool this tick.
                break

    return suspensions, assignments
