@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter (scheduler invocations)
    s.ticks = 0

    # Per-priority round-robin queues of pipeline_ids
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Active pipeline bookkeeping
    s.active = set()          # pipeline_ids that are still eligible
    s.pipeline_map = {}       # pipeline_id -> Pipeline (latest reference)
    s.pipeline_meta = {}      # pipeline_id -> {"enqueued_tick": int}

    # Operator hinting / retry tracking
    # keys use (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_oom_count = {}
    s.op_timeout_count = {}
    s.op_fail_count = {}

    # Map op_key -> pipeline_id for attributing ExecutionResult back to the right pipeline
    s.op_to_pipeline = {}

    # Pipelines we stop scheduling (after repeated non-resource failures)
    s.dead = set()
    s.pipeline_hard_fail_count = {}

    # Fairness knobs
    s.force_batch_after = 10   # after this many non-batch assignments, force a batch if available
    s.nonbatch_streak = 0

    # Queue hygiene
    s.scan_limit = 512
    s.prune_every = 25

    # Per-pipeline inflight caps (prevents a single pipeline from monopolizing the pool)
    s.inflight_cap = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }

    # Fixed default sizing (intended units: vCPU, GB)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.base_ram = {
        Priority.QUERY: 24,
        Priority.INTERACTIVE: 20,
        Priority.BATCH_PIPELINE: 16,
    }

    # Absolute per-op caps (avoid giving one op the whole machine)
    s.max_cpu_per_op = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 16,
    }

    # Always keep a small amount of headroom so QUERY/INTERACTIVE can start without preemption.
    # (Fixed amounts scale well across cluster sizes; negligible on big clusters.)
    s.query_reserve_cpu = 8
    s.query_reserve_ram = 24
    s.interactive_reserve_cpu = 6
    s.interactive_reserve_ram = 20

    # Aging / urgency
    s.age_cpu_boost_tick_1 = 150
    s.age_cpu_boost_tick_2 = 300


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _ceil(x):
        i = int(x)
        return i if x <= i else i + 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _err_str(err):
        if err is None:
            return ""
        try:
            return str(err).lower()
        except Exception:
            return ""

    def _is_oom(err):
        m = _err_str(err)
        return ("oom" in m) or ("out of memory" in m) or ("memoryerror" in m)

    def _is_timeout(err):
        m = _err_str(err)
        return ("timeout" in m) or ("timed out" in m) or ("deadline" in m)

    def _active_pipeline(pid):
        return (pid in s.active) and (pid not in s.dead)

    def _drop_pipeline(pid):
        s.active.discard(pid)
        s.pipeline_map.pop(pid, None)
        s.pipeline_meta.pop(pid, None)

    def _wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _prune_queues():
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            old = s.q[pri]
            if not old:
                continue
            # Keep only active pipeline_ids; preserve relative order
            s.q[pri] = [pid for pid in old if _active_pipeline(pid)]

    # ---- Ingest / refresh pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead:
            continue
        s.pipeline_map[pid] = p
        if pid not in s.active:
            s.active.add(pid)
            s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
            s.q[p.priority].append(pid)

    if s.prune_every > 0 and (s.ticks % s.prune_every == 0):
        _prune_queues()

    # ---- Process results: adapt RAM/CPU hints on failures ----
    for r in results:
        ops = getattr(r, "ops", []) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = False

        if not ops:
            continue

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None or pid in s.dead:
                continue

            if not failed:
                # On success, we can optionally decay aggressive CPU hints slightly.
                # Keep it simple: no decay; stability beats oscillation.
                continue

            err = getattr(r, "error", None)
            pri = None
            p_obj = s.pipeline_map.get(pid, None)
            if p_obj is not None:
                pri = p_obj.priority

            alloc_ram = getattr(r, "ram", None)
            alloc_cpu = getattr(r, "cpu", None)
            if alloc_ram is None or alloc_ram <= 0:
                alloc_ram = 0
            if alloc_cpu is None or alloc_cpu <= 0:
                alloc_cpu = 0

            key = (pid, opk)

            if _is_oom(err):
                s.op_oom_count[key] = s.op_oom_count.get(key, 0) + 1
                prev = s.op_ram_hint.get(key, None)
                base = prev if (prev is not None and prev > 0) else (alloc_ram if alloc_ram > 0 else s.base_ram.get(pri, 16))
                # Aggressive exponential backoff to converge quickly
                new_hint = max(base * 2, (alloc_ram * 2 if alloc_ram > 0 else 0))
                s.op_ram_hint[key] = new_hint
            elif _is_timeout(err):
                s.op_timeout_count[key] = s.op_timeout_count.get(key, 0) + 1
                prev = s.op_cpu_hint.get(key, None)
                base = prev if (prev is not None and prev > 0) else (alloc_cpu if alloc_cpu > 0 else s.base_cpu.get(pri, 4))
                # Increase CPU to reduce runtime; moderate ramp
                new_hint = max(base + 2, (alloc_cpu * 1.5 if alloc_cpu > 0 else 0))
                s.op_cpu_hint[key] = new_hint
                # Small RAM bump too (some timeouts are GC/memory-pressure correlated)
                prev_r = s.op_ram_hint.get(key, None)
                base_r = prev_r if (prev_r is not None and prev_r > 0) else (alloc_ram if alloc_ram > 0 else s.base_ram.get(pri, 16))
                s.op_ram_hint[key] = max(base_r, base_r * 1.25)
            else:
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
                s.pipeline_hard_fail_count[pid] = s.pipeline_hard_fail_count.get(pid, 0) + 1
                # Only give up after repeated hard failures (not OOM/timeout)
                if s.pipeline_hard_fail_count[pid] >= 4:
                    s.dead.add(pid)
                    _drop_pipeline(pid)

    # ---- Helper: compute inflight count for a pipeline ----
    def _inflight_ops(status):
        try:
            inflight = status.get_ops(
                [OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING],
                require_parents_complete=False,
            )
            return len(inflight) if inflight is not None else 0
        except Exception:
            # Fallback: be conservative
            return 0

    # ---- Sizing for an op on a specific pool ----
    def _size(pool, pri, pid, op):
        opk = _op_key(op)
        key = (pid, opk)

        # Base sizes
        cpu = s.op_cpu_hint.get(key, None)
        if cpu is None or cpu <= 0:
            cpu = s.base_cpu.get(pri, 4)

        ram = s.op_ram_hint.get(key, None)
        if ram is None or ram <= 0:
            ram = s.base_ram.get(pri, 16)

        # Aging boosts (reduce long waits / timeouts)
        wt = _wait_ticks(pid)
        if pri != Priority.QUERY:
            if wt >= s.age_cpu_boost_tick_2:
                cpu += 4
            elif wt >= s.age_cpu_boost_tick_1:
                cpu += 2

        # Priority slight CPU preference
        if pri == Priority.QUERY:
            cpu += 2
        elif pri == Priority.INTERACTIVE:
            cpu += 1

        # Cap per-op
        max_cpu_cap = s.max_cpu_per_op.get(pri, 16)
        if cpu > max_cpu_cap:
            cpu = max_cpu_cap

        # Clip to pool max
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        # Avoid extreme RAM grabs (still allow large ops via hints up to 60% pool)
        ram_cap = pool.max_ram_pool * 0.60
        if ram > ram_cap:
            ram = ram_cap

        # Minimums
        cpu = _ceil(cpu)
        ram = _ceil(ram)
        if cpu < 1:
            cpu = 1
        if ram < 1:
            ram = 1

        return cpu, ram

    # ---- Pick a runnable op from a priority queue ----
    def _pick_from_queue(pri, pool, avail_cpu, avail_ram, step_assigned_opkeys, high_backlog, query_backlog):
        q = s.q[pri]
        if not q:
            return None

        scan = len(q)
        if s.scan_limit and s.scan_limit > 0 and scan > s.scan_limit:
            scan = s.scan_limit

        for _ in range(scan):
            pid = q.pop(0)

            if not _active_pipeline(pid):
                continue

            p = s.pipeline_map.get(pid, None)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            # Per-pipeline inflight cap
            cap = s.inflight_cap.get(pri, 1)
            if _inflight_ops(status) >= cap:
                q.append(pid)
                continue

            # Runnable ops
            try:
                ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                ready = []

            if not ready:
                q.append(pid)
                continue

            # Choose first not already assigned in this scheduler step
            op = None
            for cand in ready:
                ok = _op_key(cand)
                if ok in step_assigned_opkeys:
                    continue
                op = cand
                break

            if op is None:
                q.append(pid)
                continue

            cpu_req, ram_req = _size(pool, pri, pid, op)

            # Admission / headroom rules (no preemption available)
            # Always preserve minimal headroom for QUERY; preserve for INTERACTIVE when high backlog exists.
            reserve_cpu = 0
            reserve_ram = 0

            # QUERY reserve is always on (small fixed)
            reserve_cpu += min(s.query_reserve_cpu, int(pool.max_cpu_pool))
            reserve_ram += min(s.query_reserve_ram, int(pool.max_ram_pool))

            # INTERACTIVE reserve only matters when any high-priority backlog exists
            if high_backlog:
                reserve_cpu += min(s.interactive_reserve_cpu, int(pool.max_cpu_pool))
                reserve_ram += min(s.interactive_reserve_ram, int(pool.max_ram_pool))

            if pri == Priority.INTERACTIVE:
                # Don't consume the query reserve unless there is no query backlog
                if query_backlog:
                    if (avail_cpu - cpu_req) < min(s.query_reserve_cpu, int(pool.max_cpu_pool)) or (avail_ram - ram_req) < min(s.query_reserve_ram, int(pool.max_ram_pool)):
                        q.append(pid)
                        continue

            if pri == Priority.BATCH_PIPELINE:
                # Keep headroom for higher priorities when they exist
                if high_backlog:
                    if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                        q.append(pid)
                        continue

            # Must fit now
            if cpu_req > avail_cpu or ram_req > avail_ram:
                q.append(pid)
                continue

            # Re-append for round-robin fairness
            q.append(pid)
            return (p, pid, op, cpu_req, ram_req)

        return None

    # ---- Main scheduling loop ----
    suspensions = []
    assignments = []

    # Determine pool roles (if multiple pools exist)
    num_pools = s.executor.num_pools
    high_pools = 1
    if num_pools >= 4:
        high_pools = max(1, num_pools // 4)

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # On "high pools", avoid batch unless there's no high backlog.
        is_high_pool = pool_id < high_pools

        step_assigned_opkeys = set()

        # Simple backlog signals (queue non-empty is a good-enough proxy)
        query_backlog = len(s.q[Priority.QUERY]) > 0
        interactive_backlog = len(s.q[Priority.INTERACTIVE]) > 0
        batch_backlog = len(s.q[Priority.BATCH_PIPELINE]) > 0
        high_backlog = query_backlog or interactive_backlog

        # Fill the pool
        guard = 0
        while avail_cpu >= 1 and avail_ram >= 1:
            guard += 1
            if guard > 100000:
                break

            # Refresh backlog booleans cheaply
            query_backlog = len(s.q[Priority.QUERY]) > 0
            interactive_backlog = len(s.q[Priority.INTERACTIVE]) > 0
            batch_backlog = len(s.q[Priority.BATCH_PIPELINE]) > 0
            high_backlog = query_backlog or interactive_backlog

            if not (query_backlog or interactive_backlog or batch_backlog):
                break

            # Batch fairness
            force_batch = batch_backlog and (s.nonbatch_streak >= s.force_batch_after)

            # Priority attempt order
            pri_order = []
            if force_batch:
                pri_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
            else:
                pri_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            # High pool policy: de-prioritize batch unless no high backlog
            if is_high_pool and high_backlog:
                pri_order = [Priority.QUERY, Priority.INTERACTIVE]

            picked = None
            for pri in pri_order:
                picked = _pick_from_queue(
                    pri=pri,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    step_assigned_opkeys=step_assigned_opkeys,
                    high_backlog=high_backlog,
                    query_backlog=query_backlog,
                )
                if picked is not None:
                    break

            # If high-pool is blocked and no high could be placed, allow batch as backfill
            if picked is None and is_high_pool and (not high_backlog) and batch_backlog:
                picked = _pick_from_queue(
                    pri=Priority.BATCH_PIPELINE,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    step_assigned_opkeys=step_assigned_opkeys,
                    high_backlog=False,
                    query_backlog=query_backlog,
                )

            if picked is None:
                break

            p_obj, pid, op, cpu_req, ram_req = picked

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p_obj.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            ok = _op_key(op)
            s.op_to_pipeline[ok] = pid
            step_assigned_opkeys.add(ok)

            avail_cpu -= cpu_req
            avail_ram -= ram_req

            if p_obj.priority == Priority.BATCH_PIPELINE:
                s.nonbatch_streak = 0
            else:
                s.nonbatch_streak += 1

    return suspensions, assignments
