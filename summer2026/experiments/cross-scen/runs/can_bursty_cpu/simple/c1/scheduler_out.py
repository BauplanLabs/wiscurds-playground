@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on:
      - Much higher container concurrency (CPU/RAM quanta instead of pool-fraction sizing)
      - Dedicated high-priority pools when multiple pools exist (QUERY/INTERACTIVE isolation)
      - Soft batch admission control / headroom in mixed pools to protect bursty interactive work
      - OOM-aware RAM hinting with quick convergence, without aggressive blacklisting
      - Limited per-pipeline parallelism (allow some DAG parallel ops for QUERY/INTERACTIVE)
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()

    # pipeline_id -> dict(enqueued_tick, last_scheduled_tick)
    s.pipeline_meta = {}

    # (pipeline_id, op_key) -> ram hint
    s.op_ram_hint = {}

    # op_key -> pipeline_id (best-effort attribution)
    s.op_to_pipeline = {}

    # failure tracking to avoid infinite retries on deterministic non-OOM failures
    s.op_fail_count = {}       # (pipeline_id, op_key) -> int
    s.pipeline_fail_count = {} # pipeline_id -> int
    s.dead_pipelines = set()

    # recent high-priority activity (to keep headroom in mixed/hi pools)
    s.last_hi_nonempty_tick = 0

    # knobs (kept as attributes for easy sweep/tuning)
    s.hi_pool_frac = 0.25            # fraction of pools reserved primarily for QUERY/INTERACTIVE (when num_pools>1)
    s.hi_reserve_window_ticks = 10   # if hi work was recently present, keep some headroom from batch
    s.hi_reserve_cpu_frac = 0.20     # keep this fraction of CPU free from batch in protected pools (when reserve active)
    s.hi_reserve_ram_frac = 0.10     # keep this fraction of RAM free from batch in protected pools (when reserve active)

    s.non_oom_op_retry_limit = 3
    s.non_oom_pipeline_retry_limit = 6

    # OOM RAM backoff behavior
    s.oom_growth = 1.8
    s.oom_min_bump = 4  # minimum additive bump (in same units as pool RAM)
    s.min_ram_alloc = 2
    s.min_cpu_alloc = 1

    # per-pipeline parallelism caps (per scheduling tick)
    s.max_ops_per_pipeline_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # batch usage in batch pools when interactive is urgent
    s.batch_pool_interactive_spill_cap = 2  # max interactive assignments per batch pool per tick when batch queue non-empty


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        # Keep op_ram_hint/op_fail_count around; harmless and can help if IDs reused (unlikely).

    def _meta(pipeline_id):
        m = s.pipeline_meta.get(pipeline_id)
        if m is None:
            m = {"enqueued_tick": s.ticks, "last_scheduled_tick": s.ticks}
            s.pipeline_meta[pipeline_id] = m
        return m

    def _staleness_ticks(pipeline_id):
        m = s.pipeline_meta.get(pipeline_id)
        if not m:
            return 0
        return max(0, s.ticks - m.get("last_scheduled_tick", m.get("enqueued_tick", s.ticks)))

    def _hi_backlog_len():
        return len(s.wait_q[Priority.QUERY]) + len(s.wait_q[Priority.INTERACTIVE])

    def _compute_hi_pool_count(num_pools):
        if num_pools <= 1:
            return 1
        # ensure at least 1 hi pool; round down but keep meaningful isolation
        cnt = int(num_pools * s.hi_pool_frac)
        if cnt < 1:
            cnt = 1
        if cnt >= num_pools:
            cnt = num_pools - 1
        return cnt

    def _cleanup_queues_if_needed():
        # Periodically compact queues by removing dead/completed pipelines.
        if s.ticks % 10 != 0:
            return
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            newq = []
            for p in s.wait_q[pri]:
                pid = p.pipeline_id
                if pid in s.dead_pipelines:
                    _drop_pipeline(pid)
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
                newq.append(p)
            s.wait_q[pri] = newq

    def _base_cpu_quantum(pool, pri, qlens):
        max_cpu = pool.max_cpu_pool
        # Default quanta (favor concurrency); then adapt down under heavy backlog.
        if pri == Priority.QUERY:
            base = max_cpu // 4  # e.g. 64 -> 16
            if base < 4:
                base = 4
            if base > 16:
                base = 16
        elif pri == Priority.INTERACTIVE:
            base = max_cpu // 8  # e.g. 64 -> 8
            if base < 2:
                base = 2
            if base > 10:
                base = 10
            if qlens[Priority.INTERACTIVE] > 60:
                base = max(2, base // 2)
        else:
            base = max_cpu // 16  # e.g. 64 -> 4
            if base < 1:
                base = 1
            if base > 6:
                base = 6
            if qlens[Priority.BATCH_PIPELINE] > 120:
                base = max(1, base // 2)

        if base < s.min_cpu_alloc:
            base = s.min_cpu_alloc
        return base

    def _base_ram_quantum(pool, pri, qlens):
        max_ram = pool.max_ram_pool
        # Default RAM quanta: small by default, scaled a bit by pool size, capped.
        if pri == Priority.QUERY:
            base = max_ram / 48.0
            if base < 8:
                base = 8
            if base > 48:
                base = 48
        elif pri == Priority.INTERACTIVE:
            base = max_ram / 64.0
            if base < 6:
                base = 6
            if base > 32:
                base = 32
            if qlens[Priority.INTERACTIVE] > 60:
                base = max(6, base * 0.8)
        else:
            base = max_ram / 80.0
            if base < 4:
                base = 4
            if base > 24:
                base = 24
            if qlens[Priority.BATCH_PIPELINE] > 120:
                base = max(4, base * 0.8)

        if base < s.min_ram_alloc:
            base = s.min_ram_alloc
        return base

    def _size_request(pool, pri, pid, op, qlens):
        # CPU
        cpu_req = float(_base_cpu_quantum(pool, pri, qlens))

        # Age-based CPU boost (helps long-waiting pipelines finish before timeout)
        stale = _staleness_ticks(pid)
        if pri != Priority.BATCH_PIPELINE:
            if stale >= 25:
                cpu_req *= 1.5
            elif stale >= 10:
                cpu_req *= 1.25
        else:
            if stale >= 40:
                cpu_req *= 1.25

        if cpu_req < s.min_cpu_alloc:
            cpu_req = float(s.min_cpu_alloc)
        if cpu_req > pool.max_cpu_pool:
            cpu_req = float(pool.max_cpu_pool)

        # RAM (baseline + OOM hints)
        ram_req = float(_base_ram_quantum(pool, pri, qlens))
        opk = _op_key(op)
        hint = s.op_ram_hint.get((pid, opk))
        if hint is not None:
            if hint > ram_req:
                ram_req = float(hint)

        if ram_req < s.min_ram_alloc:
            ram_req = float(s.min_ram_alloc)
        if ram_req > pool.max_ram_pool:
            ram_req = float(pool.max_ram_pool)

        return cpu_req, ram_req

    def _pick_runnable_op(status):
        # Prefer retrying FAILED ops first to converge on correct sizing quickly.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not ops:
            return None
        failed = None
        for op in ops:
            if getattr(op, "state", None) == OperatorState.FAILED:
                failed = op
                break
        return failed if failed is not None else ops[0]

    def _queue_oldest_staleness(pri):
        q = s.wait_q[pri]
        if not q:
            return 0
        m = 0
        for p in q:
            pid = p.pipeline_id
            st = _staleness_ticks(pid)
            if st > m:
                m = st
        return m

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        _meta(pid)  # initialize meta
        _queue_for_priority(p.priority).append(p)

    # --- Process results (OOM-aware RAM bumps; bounded retries for non-OOM failures) ---
    for r in results:
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue

            if hasattr(r, "failed") and r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    prev = s.op_ram_hint.get((pid, opk))
                    alloc = getattr(r, "ram", None)
                    base = None
                    if alloc is not None and alloc > 0:
                        base = float(alloc)
                    elif prev is not None and prev > 0:
                        base = float(prev)
                    else:
                        base = float(s.min_ram_alloc)

                    bumped = base * float(s.oom_growth)
                    bumped = max(bumped, base + float(s.oom_min_bump))
                    if prev is not None:
                        bumped = max(bumped, float(prev) * float(s.oom_growth))

                    s.op_ram_hint[(pid, opk)] = bumped
                    s.op_fail_count[(pid, opk)] = s.op_fail_count.get((pid, opk), 0) + 1
                    s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1
                else:
                    s.op_fail_count[(pid, opk)] = s.op_fail_count.get((pid, opk), 0) + 1
                    s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1
                    if (
                        s.op_fail_count[(pid, opk)] >= s.non_oom_op_retry_limit
                        or s.pipeline_fail_count[pid] >= s.non_oom_pipeline_retry_limit
                    ):
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

    _cleanup_queues_if_needed()

    # Track recent high-priority presence to protect headroom from batch in mixed/hi pools.
    if _hi_backlog_len() > 0:
        s.last_hi_nonempty_tick = s.ticks

    # Fast-path
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Tick-local accounting
    scheduled_ops = set()  # op_key
    scheduled_count_by_pid = {}  # pipeline_id -> count

    qlens = {
        Priority.QUERY: len(s.wait_q[Priority.QUERY]),
        Priority.INTERACTIVE: len(s.wait_q[Priority.INTERACTIVE]),
        Priority.BATCH_PIPELINE: len(s.wait_q[Priority.BATCH_PIPELINE]),
    }

    num_pools = s.executor.num_pools
    hi_pool_count = _compute_hi_pool_count(num_pools)
    hi_pools = set(range(hi_pool_count))

    interactive_oldest = _queue_oldest_staleness(Priority.INTERACTIVE)
    interactive_urgent = interactive_oldest >= 18

    def _max_ops_for_pipeline(pri):
        return s.max_ops_per_pipeline_tick.get(pri, 1)

    def _pool_allowed_priorities(pool_id):
        # Always allow QUERY everywhere (avoid waiting behind batch-only pools).
        if num_pools <= 1:
            # Single pool: prioritize QUERY/INTERACTIVE strongly; allow batch with headroom gating.
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        if pool_id in hi_pools:
            # Hi pools: prefer QUERY/INTERACTIVE; batch only when hi is empty.
            if qlens[Priority.QUERY] == 0 and qlens[Priority.INTERACTIVE] == 0:
                return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Batch pools: default to batch-heavy; spill interactive if urgent or batch empty.
        if qlens[Priority.BATCH_PIPELINE] == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if interactive_urgent:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

    def _can_schedule_batch_with_reserve(pool, avail_cpu, avail_ram, cpu_req, ram_req):
        # If recent hi activity, keep some headroom in protected pools.
        if (s.ticks - s.last_hi_nonempty_tick) > s.hi_reserve_window_ticks:
            return True

        reserve_cpu = float(pool.max_cpu_pool) * float(s.hi_reserve_cpu_frac)
        reserve_ram = float(pool.max_ram_pool) * float(s.hi_reserve_ram_frac)

        # Only reserve when we might need it: if any hi backlog exists or interactive is urgent.
        if _hi_backlog_len() == 0 and not interactive_urgent:
            return True

        # Don't schedule batch if it would reduce free resources below reserve.
        return (avail_cpu - cpu_req) >= reserve_cpu and (avail_ram - ram_req) >= reserve_ram

    def _pick_next_from_queue(q, pri, pool, pool_id, avail_cpu, avail_ram, spill_state):
        if not q:
            return None, None, None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Dead/completed cleanup
            if pid in s.dead_pipelines:
                _drop_pipeline(pid)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            # Per-pipeline per-tick cap
            cap = _max_ops_for_pipeline(p.priority)
            cnt = scheduled_count_by_pid.get(pid, 0)
            if cnt >= cap:
                q.append(p)
                continue

            op = _pick_runnable_op(status)
            if op is None:
                q.append(p)
                continue

            opk = _op_key(op)
            if opk in scheduled_ops:
                q.append(p)
                continue

            cpu_req, ram_req = _size_request(pool, pri, pid, op, qlens)

            # Clamp to current availability
            if cpu_req > avail_cpu:
                cpu_req = float(avail_cpu)
            if ram_req > avail_ram:
                ram_req = float(avail_ram)

            if cpu_req < s.min_cpu_alloc or ram_req < s.min_ram_alloc:
                q.append(p)
                continue

            # If we have an OOM hint larger than avail RAM, don't try here.
            hint = s.op_ram_hint.get((pid, opk))
            if hint is not None and float(hint) > float(avail_ram):
                q.append(p)
                continue

            # In batch pools, cap interactive spill assignments per pool per tick when batch exists.
            if (
                num_pools > 1
                and (pool_id not in hi_pools)
                and pri == Priority.INTERACTIVE
                and qlens[Priority.BATCH_PIPELINE] > 0
                and spill_state.get("interactive_spill_used", 0) >= s.batch_pool_interactive_spill_cap
            ):
                q.append(p)
                continue

            # Batch reserve gating in protected contexts:
            if pri == Priority.BATCH_PIPELINE:
                protected = (num_pools <= 1) or (pool_id in hi_pools)
                if protected and not _can_schedule_batch_with_reserve(pool, avail_cpu, avail_ram, cpu_req, ram_req):
                    q.append(p)
                    continue

            # Success: re-append pipeline for round-robin fairness.
            q.append(p)
            return p, op, cpu_req, ram_req

        return None, None, None, None

    # --- Main scheduling loop ---
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        allowed = _pool_allowed_priorities(pool_id)
        spill_state = {"interactive_spill_used": 0}

        # Fill the pool
        while avail_cpu >= float(s.min_cpu_alloc) and avail_ram >= float(s.min_ram_alloc):
            made = False
            for pri in allowed:
                q = _queue_for_priority(pri)
                p, op, cpu_req, ram_req = _pick_next_from_queue(q, pri, pool, pool_id, avail_cpu, avail_ram, spill_state)
                if p is None:
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                opk = _op_key(op)
                scheduled_ops.add(opk)
                s.op_to_pipeline[opk] = p.pipeline_id

                scheduled_count_by_pid[p.pipeline_id] = scheduled_count_by_pid.get(p.pipeline_id, 0) + 1
                m = _meta(p.pipeline_id)
                m["last_scheduled_tick"] = s.ticks

                if num_pools > 1 and (pool_id not in hi_pools) and pri == Priority.INTERACTIVE and qlens[Priority.BATCH_PIPELINE] > 0:
                    spill_state["interactive_spill_used"] = spill_state.get("interactive_spill_used", 0) + 1

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                made = True
                break

            if not made:
                break

    return suspensions, assignments
