@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter (discrete simulator time)
    s.ticks = 0

    # Per-priority FIFO queues (round-robin by rotation)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Avoid duplicate enqueues
    s.enqueued = set()

    # Pipeline metadata (aging / fairness)
    # {pipeline_id: {"enqueued_tick": int}}
    s.pipeline_meta = {}

    # Per-op resource hints (learned from failures/successes)
    # key: (pipeline_id, op_key) -> amount
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Per-op retry tracking
    s.op_retry_oom = {}
    s.op_retry_other = {}

    # Map op_key -> last assigned metadata so results can be attributed
    # op_key -> {"pipeline_id":..., "priority":..., "pool_id":..., "cpu":..., "ram":...}
    s.op_meta = {}

    # Pipelines deemed impossible / too-failure-prone (stop spending resources)
    s.dead_pipelines = set()

    # Global adaptive multipliers per priority (stabilize OOM / timeout behavior)
    s.global_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.global_cpu_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Recent failure counters (lightweight control loop)
    s.recent_oom = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.recent_timeout = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.recent_fail = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # After queries, allocate capacity between interactive and batch using a pick cycle.
    # (query is handled with strict priority / reservation)
    s.cycle = [
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
        Priority.BATCH_PIPELINE,
    ]
    s.cycle_ptr = 0

    # Per-pipeline per-tick cap (lets DAG-parallel query/interactive run a bit faster without hogging)
    s.max_ops_per_pipeline_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Base sizing (absolute, so scaling cluster size increases parallelism rather than container sizes)
    # These are intentionally modest to increase concurrency and reduce queueing timeouts.
    s.base_cpu = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.base_ram = {
        Priority.QUERY: 32.0,
        Priority.INTERACTIVE: 28.0,
        Priority.BATCH_PIPELINE: 24.0,
    }
    s.min_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.min_ram = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 6.0,
    }

    # Simple protection: keep enough headroom for one query op when a runnable query exists
    s.query_reserve_cpu = 4.0
    s.query_reserve_ram = 32.0

    # Retry limits: stop burning the cluster on hopeless ops
    s.max_oom_retries = 6
    s.max_other_retries = 3

    # Aging knobs: gradually add CPU to long-waiting work to reduce timeout penalties
    s.age_cpu_boost_after = 120     # ticks
    s.age_cpu_boost_max = 1.75      # cap multiplier


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _call_failed(r):
        fn = getattr(r, "failed", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:
                return False
        return False

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _pipeline_age(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _clip(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _age_cpu_factor(pid):
        age = _pipeline_age(pid)
        if age <= s.age_cpu_boost_after:
            return 1.0
        # Linear ramp to max factor over another ~2*after ticks
        excess = age - s.age_cpu_boost_after
        ramp = min(1.0, excess / max(1.0, 2.0 * s.age_cpu_boost_after))
        return 1.0 + ramp * (s.age_cpu_boost_max - 1.0)

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)

        # Base requests (absolute), scaled by adaptive multipliers and mild aging
        cpu_req = s.op_cpu_hint.get((pid, opk), None)
        if cpu_req is None:
            cpu_req = s.base_cpu[pri] * s.global_cpu_mult[pri] * _age_cpu_factor(pid)

        ram_req = s.op_ram_hint.get((pid, opk), None)
        base_ram = s.base_ram[pri] * s.global_ram_mult[pri]
        if ram_req is None:
            ram_req = base_ram
        else:
            # Never go below global base once we've learned a higher value
            ram_req = max(ram_req, base_ram)

        # Enforce minimums
        cpu_req = max(cpu_req, s.min_cpu[pri])
        ram_req = max(ram_req, s.min_ram[pri])

        # Clip to pool maxima
        cpu_req = _clip(cpu_req, 1.0, float(pool.max_cpu_pool))
        ram_req = _clip(ram_req, 1.0, float(pool.max_ram_pool))

        return float(cpu_req), float(ram_req)

    def _mark_dead(pid):
        s.dead_pipelines.add(pid)
        _drop_pipeline(pid)

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid in s.enqueued:
            continue
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            pass
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        _queue_for_priority(p.priority).append(p)

    # --- Process results: learn hints and adapt global multipliers ---
    # Reset per-tick recent counters with a gentle decay (keeps controller responsive)
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.recent_oom[pri] = int(s.recent_oom[pri] * 0.8)
        s.recent_timeout[pri] = int(s.recent_timeout[pri] * 0.8)
        s.recent_fail[pri] = int(s.recent_fail[pri] * 0.8)

    for r in results:
        failed = _call_failed(r)
        err = getattr(r, "error", None)
        r_pri = getattr(r, "priority", None)

        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            meta = s.op_meta.get(opk, None)
            pid = meta["pipeline_id"] if meta is not None else None
            pri = meta["priority"] if meta is not None else r_pri

            if pid is None or pri is None:
                continue

            if pid in s.dead_pipelines:
                continue

            pool_id = getattr(r, "pool_id", None)
            pool = None
            if pool_id is not None:
                try:
                    pool = s.executor.pools[int(pool_id)]
                except Exception:
                    pool = None

            if not failed:
                # Success: if we ever had a higher RAM hint, allow tightening to the proven-success value.
                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is not None and alloc_ram > 0:
                    prev = s.op_ram_hint.get((pid, opk), None)
                    if prev is None or alloc_ram < prev:
                        s.op_ram_hint[(pid, opk)] = float(alloc_ram)
                alloc_cpu = getattr(r, "cpu", None)
                if alloc_cpu is not None and alloc_cpu > 0:
                    prevc = s.op_cpu_hint.get((pid, opk), None)
                    if prevc is None:
                        s.op_cpu_hint[(pid, opk)] = float(alloc_cpu)
                continue

            # Failure handling
            s.recent_fail[pri] += 1

            if _is_oom_error(err):
                s.recent_oom[pri] += 1
                key = (pid, opk)
                prev_hint = s.op_ram_hint.get(key, None)
                alloc = getattr(r, "ram", None)
                base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev_hint) if prev_hint is not None else s.base_ram[pri])

                # Aggressive but controlled increase
                new_hint = base * 1.8
                if prev_hint is not None:
                    new_hint = max(new_hint, float(prev_hint) * 1.5)

                # Clip to pool max if known
                if pool is not None:
                    new_hint = min(float(pool.max_ram_pool), float(new_hint))

                s.op_ram_hint[key] = float(max(new_hint, s.min_ram[pri]))

                # Retry accounting
                s.op_retry_oom[key] = s.op_retry_oom.get(key, 0) + 1

                # If we are already at (near) pool max and still OOMing, give up on this pipeline
                if pool is not None:
                    if s.op_ram_hint[key] >= 0.98 * float(pool.max_ram_pool) and s.op_retry_oom[key] >= 2:
                        _mark_dead(pid)
                        continue

                if s.op_retry_oom[key] > s.max_oom_retries:
                    _mark_dead(pid)
                    continue

            else:
                # Non-OOM failures: often timeouts -> try more CPU
                key = (pid, opk)
                s.op_retry_other[key] = s.op_retry_other.get(key, 0) + 1

                if _is_timeout_error(err):
                    s.recent_timeout[pri] += 1
                    prev_cpu = s.op_cpu_hint.get(key, None)
                    alloc_cpu = getattr(r, "cpu", None)
                    base_cpu = float(alloc_cpu) if (alloc_cpu is not None and alloc_cpu > 0) else (float(prev_cpu) if prev_cpu is not None else s.base_cpu[pri])
                    new_cpu = base_cpu * 1.5
                    if pool is not None:
                        new_cpu = min(float(pool.max_cpu_pool), new_cpu)
                    s.op_cpu_hint[key] = float(max(new_cpu, s.min_cpu[pri]))
                else:
                    # Generic bump (small)
                    prev_cpu = s.op_cpu_hint.get(key, None)
                    if prev_cpu is not None:
                        s.op_cpu_hint[key] = float(prev_cpu * 1.15)

                if s.op_retry_other[key] > s.max_other_retries:
                    _mark_dead(pid)
                    continue

    # Adjust global multipliers (per-priority) based on recent failures.
    # Aim: near-zero OOM and fewer timeouts, but keep concurrency high.
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if s.recent_fail[pri] <= 0:
            continue

        oom_rate = float(s.recent_oom[pri]) / float(max(1, s.recent_fail[pri]))
        to_rate = float(s.recent_timeout[pri]) / float(max(1, s.recent_fail[pri]))

        # If OOM dominates, increase RAM multiplier
        if oom_rate >= 0.35:
            s.global_ram_mult[pri] = min(4.0, s.global_ram_mult[pri] * 1.25)

        # If timeouts dominate, increase CPU multiplier a bit
        if to_rate >= 0.35:
            s.global_cpu_mult[pri] = min(3.0, s.global_cpu_mult[pri] * 1.15)

        # If few OOMs recently, slowly relax RAM multiplier (keeps utilization high over time)
        if oom_rate <= 0.05 and s.global_ram_mult[pri] > 1.0:
            s.global_ram_mult[pri] = max(1.0, s.global_ram_mult[pri] * 0.985)

        # If few timeouts, relax CPU multiplier
        if to_rate <= 0.05 and s.global_cpu_mult[pri] > 1.0:
            s.global_cpu_mult[pri] = max(1.0, s.global_cpu_mult[pri] * 0.99)

    suspensions = []
    assignments = []

    # Track per-pipeline assignments this tick
    scheduled_counts = {}

    def _can_schedule_more(p):
        pid = p.pipeline_id
        limit = s.max_ops_per_pipeline_tick.get(p.priority, 1)
        return scheduled_counts.get(pid, 0) < limit

    def _inc_scheduled(p):
        pid = p.pipeline_id
        scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

    def _has_runnable_in_queue(q, max_scan=30):
        # Cheap scan to decide if "runnable query exists" for reservation
        n = len(q)
        if n <= 0:
            return False
        scan = min(n, max_scan)
        for i in range(scan):
            p = q[i]
            if p.pipeline_id in s.dead_pipelines:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
            except Exception:
                continue
        return False

    def _pick_from_queue(q, pool, avail_cpu, avail_ram, require_fit=True):
        """Round-robin over q; returns (pipeline, op, cpu_req, ram_req) or (None, None, None, None)."""
        if not q:
            return None, None, None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            if pid in s.dead_pipelines:
                continue

            # Drop completed pipelines promptly
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                st = None

            # Per-tick cap
            if not _can_schedule_more(p):
                q.append(p)
                continue

            # Find an assignable op
            try:
                if st is None:
                    st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = []

            if not ops:
                q.append(p)
                continue

            op = ops[0]
            cpu_req, ram_req = _size_request(pool, p.priority, pid, op)

            # If it can't ever fit here due to max constraints, keep it queued (it may fit other pools)
            if cpu_req > float(pool.max_cpu_pool) or ram_req > float(pool.max_ram_pool):
                q.append(p)
                continue

            # Check availability (and enforce minimums against current availability)
            if require_fit:
                if avail_cpu < s.min_cpu[p.priority] or avail_ram < s.min_ram[p.priority]:
                    q.append(p)
                    continue

                # Prefer not to schedule below priority minimums
                if cpu_req > avail_cpu:
                    # Allow clipping but not below min_cpu
                    cpu_req = float(avail_cpu)
                if ram_req > avail_ram:
                    ram_req = float(avail_ram)

                if cpu_req < s.min_cpu[p.priority] or ram_req < s.min_ram[p.priority]:
                    q.append(p)
                    continue

            q.append(p)
            return p, op, float(cpu_req), float(ram_req)

        return None, None, None, None

    # Decide if we should reserve for queries (only when a runnable query exists)
    runnable_query_exists = _has_runnable_in_queue(s.wait_q[Priority.QUERY], max_scan=40)

    # --- Main scheduling loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        made_progress = True
        while made_progress and avail_cpu >= 1.0 and avail_ram >= 1.0:
            made_progress = False

            # Helper: try schedule one op from a given priority, with optional query reservation checks.
            def _try_schedule_priority(pri):
                nonlocal avail_cpu, avail_ram, made_progress

                q = _queue_for_priority(pri)
                p, op, cpu_req, ram_req = _pick_from_queue(q, pool, avail_cpu, avail_ram, require_fit=True)
                if p is None:
                    return False

                # Protect query headroom: don't let lower-priority consume the last "query slot"
                if runnable_query_exists and pri != Priority.QUERY:
                    # Only enforce reservation if we are currently above reservation; otherwise schedule anyway.
                    if (avail_cpu - cpu_req) < s.query_reserve_cpu or (avail_ram - ram_req) < s.query_reserve_ram:
                        # If we can still schedule queries right now, keep headroom.
                        qp, qop, qcpu, qram = _pick_from_queue(
                            s.wait_q[Priority.QUERY], pool, avail_cpu, avail_ram, require_fit=True
                        )
                        if qp is not None:
                            return False

                # Final clip to what's available (while preserving priority minimums)
                cpu_req = min(cpu_req, avail_cpu)
                ram_req = min(ram_req, avail_ram)
                if cpu_req < s.min_cpu[p.priority] or ram_req < s.min_ram[p.priority]:
                    return False

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                opk = _op_key(op)
                s.op_meta[opk] = {
                    "pipeline_id": p.pipeline_id,
                    "priority": p.priority,
                    "pool_id": pool_id,
                    "cpu": cpu_req,
                    "ram": ram_req,
                }

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                _inc_scheduled(p)
                made_progress = True
                return True

            # 1) Strict priority for QUERY
            if _try_schedule_priority(Priority.QUERY):
                continue

            # 2) Weighted share between INTERACTIVE and BATCH via cycle, but fallback to the other if empty
            pri0 = s.cycle[s.cycle_ptr]
            s.cycle_ptr = (s.cycle_ptr + 1) % len(s.cycle)
            pri1 = Priority.BATCH_PIPELINE if pri0 == Priority.INTERACTIVE else Priority.INTERACTIVE

            if _try_schedule_priority(pri0):
                continue
            if _try_schedule_priority(pri1):
                continue

            # 3) As a final fallback, try anything (including QUERY again in case availability changed)
            if _try_schedule_priority(Priority.QUERY):
                continue
            if _try_schedule_priority(Priority.INTERACTIVE):
                continue
            if _try_schedule_priority(Priority.BATCH_PIPELINE):
                continue

    return suspensions, assignments
