@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter (scheduler steps)
    s.ticks = 0

    # Per-priority pipeline queues (pipelines rotate for fairness)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline bookkeeping
    s.enqueued = set()          # pipeline_ids currently tracked in queues
    s.ignored = set()           # completed or permanently failed pipelines (don't re-enqueue)
    s.pipeline_meta = {}        # pipeline_id -> dict(enqueued_tick, boost_until, last_fail_tick, last_sched_tick)

    # Per-operator retry + sizing hints (only meaningful for retries of the same op)
    # Keys: (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_retry = {}

    # Map op_key -> pipeline_id so ExecutionResult can be attributed
    s.op_to_pipeline = {}

    # Conservative failure handling:
    # - OOM: bump RAM and retry
    # - timeout: bump CPU and retry
    # - other: allow a few retries, then give up
    s.max_retries_oom = 6
    s.max_retries_timeout = 6
    s.max_retries_other = 2

    # Batch starvation prevention (in ticks)
    s.batch_starve_ticks = 140

    # Per-pipeline per-tick cap (prevents a single pipeline from flooding the cluster)
    s.max_ops_per_pipeline_per_tick = 2


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

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
        msg = _err_str(err)
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout(err):
        msg = _err_str(err)
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _meta(pid):
        m = s.pipeline_meta.get(pid)
        if m is None:
            m = {"enqueued_tick": s.ticks, "boost_until": 0, "last_fail_tick": -1, "last_sched_tick": -1}
            s.pipeline_meta[pid] = m
        return m

    def _drop_pipeline(pid, permanent=False):
        s.enqueued.discard(pid)
        if permanent:
            s.ignored.add(pid)
        s.pipeline_meta.pop(pid, None)

    def _wait_ticks(pid):
        m = s.pipeline_meta.get(pid)
        if not m:
            return 0
        return max(0, s.ticks - m.get("enqueued_tick", s.ticks))

    def _is_boosted(pid):
        m = s.pipeline_meta.get(pid)
        if not m:
            return False
        return m.get("boost_until", 0) >= s.ticks

    def _remaining_ops_estimate(p):
        # Favor shorter remaining pipelines to reduce tail (SRPT-ish).
        # Best-effort only; fall back to 1 if unknown.
        try:
            status = p.runtime_status()
        except Exception:
            return 1

        total = None
        try:
            v = getattr(p, "values", None)
            if v is not None and hasattr(v, "__len__"):
                total = len(v)
        except Exception:
            total = None

        if not total or total <= 0:
            return 1

        try:
            completed = status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False)
            comp_n = len(completed) if completed is not None else 0
        except Exception:
            comp_n = 0

        rem = total - comp_n
        if rem <= 0:
            return 1
        return rem

    # ---- Ingest new pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.ignored:
            continue
        if pid in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "boost_until": 0, "last_fail_tick": -1, "last_sched_tick": -1}

    # ---- Process results (update hints & retries; avoid abandoning on timeout) ----
    for r in results:
        failed = False
        try:
            failed = r.failed() if hasattr(r, "failed") else (getattr(r, "error", None) is not None)
        except Exception:
            failed = getattr(r, "error", None) is not None

        rops = getattr(r, "ops", []) or []
        for op in rops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None or pid in s.ignored:
                continue

            if not failed:
                # Success: no action needed (hints are only for retries of this op)
                continue

            err = getattr(r, "error", None)
            is_oom = _is_oom(err)
            is_to = _is_timeout(err)

            key = (pid, opk)
            prev_retry = s.op_retry.get(key, 0) + 1
            s.op_retry[key] = prev_retry

            # Boost this pipeline temporarily so retries are not drowned out by bursty arrivals.
            m = _meta(pid)
            m["boost_until"] = max(m.get("boost_until", 0), s.ticks + 25)
            m["last_fail_tick"] = s.ticks

            if is_oom:
                # Retry with larger RAM. Use the last allocation as base if available.
                alloc = getattr(r, "ram", None)
                prev = s.op_ram_hint.get(key, None)
                base = None
                if alloc is not None and alloc > 0:
                    base = alloc
                elif prev is not None and prev > 0:
                    base = prev
                else:
                    base = 1

                # Aggressive but bounded growth (fast convergence, few repeated OOMs).
                new_hint = base * 1.8 + 2
                if prev is not None:
                    new_hint = max(new_hint, prev * 1.6 + 1)
                s.op_ram_hint[key] = new_hint

                if prev_retry > s.max_retries_oom:
                    _drop_pipeline(pid, permanent=True)

            elif is_to:
                # Retry with more CPU (timeouts are often from under-sizing / queue delay).
                alloc = getattr(r, "cpu", None)
                prev = s.op_cpu_hint.get(key, None)
                base = None
                if alloc is not None and alloc > 0:
                    base = alloc
                elif prev is not None and prev > 0:
                    base = prev
                else:
                    base = 1

                new_hint = base * 1.7 + 1
                if prev is not None:
                    new_hint = max(new_hint, prev * 1.5 + 1)
                s.op_cpu_hint[key] = new_hint

                if prev_retry > s.max_retries_timeout:
                    _drop_pipeline(pid, permanent=True)

            else:
                # Unknown failures: allow a small number of retries, but do not instantly abandon.
                # Also nudge both resources slightly (sometimes misclassified failures are resource-related).
                prev_ram = s.op_ram_hint.get(key, None)
                prev_cpu = s.op_cpu_hint.get(key, None)
                if prev_ram is None:
                    prev_ram = getattr(r, "ram", None) or 1
                if prev_cpu is None:
                    prev_cpu = getattr(r, "cpu", None) or 1
                s.op_ram_hint[key] = max(prev_ram, 1) * 1.25 + 1
                s.op_cpu_hint[key] = max(prev_cpu, 1) * 1.25 + 1

                if prev_retry > s.max_retries_other:
                    _drop_pipeline(pid, permanent=True)

    # ---- Helper: is there any runnable op in a queue? (for reservation / gating) ----
    def _any_runnable_in_queue(q):
        # Scan a limited prefix for speed; enough for reservation decisions.
        limit = 60
        n = len(q)
        if n <= 0:
            return False
        scan = n if n < limit else limit
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id
            if pid in s.ignored:
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

    query_runnable = _any_runnable_in_queue(s.wait_q[Priority.QUERY])
    interactive_runnable = _any_runnable_in_queue(s.wait_q[Priority.INTERACTIVE])

    # Batch urgency (aging-based): if any batch pipeline has waited long and is runnable, let batch contend.
    def _any_starved_batch():
        q = s.wait_q[Priority.BATCH_PIPELINE]
        limit = 120
        n = len(q)
        if n <= 0:
            return False
        scan = n if n < limit else limit
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id
            if pid in s.ignored:
                continue
            if _wait_ticks(pid) < s.batch_starve_ticks:
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

    batch_urgent = _any_starved_batch()

    # ---- Sizing policy (CPU capped to avoid "one op eats the pool" and reduce queueing) ----
    def _cpu_bounds(pri, pool):
        max_cpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            return 4, min(32, max_cpu)
        if pri == Priority.INTERACTIVE:
            return 4, min(24, max_cpu)
        return 2, min(16, max_cpu)

    def _ram_min(pri):
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 14
        return 10

    def _nominal_cpu_frac(pri):
        # Fraction of pool CPU to devote to a single op when the queue is small.
        if pri == Priority.QUERY:
            return 0.35
        if pri == Priority.INTERACTIVE:
            return 0.25
        return 0.18

    def _compute_request(pool, pri, pid, op, runnable_estimate):
        opk = _op_key(op)
        key = (pid, opk)

        cpu_min, cpu_cap = _cpu_bounds(pri, pool)

        # Backlog-aware CPU: when many pipelines are runnable, shrink per-op CPU to increase parallelism.
        # Use runnable_estimate (count of runnable pipelines in this priority) if available.
        if runnable_estimate is None or runnable_estimate <= 0:
            runnable_estimate = 1

        base = int(pool.max_cpu_pool * _nominal_cpu_frac(pri) / max(1, min(runnable_estimate, 10)))
        if base < cpu_min:
            base = cpu_min
        if base > cpu_cap:
            base = cpu_cap

        cpu_hint = s.op_cpu_hint.get(key, None)
        if cpu_hint is not None and cpu_hint > base:
            base = int(cpu_hint)
            if base < cpu_min:
                base = cpu_min
            if base > cpu_cap:
                base = cpu_cap

        # RAM: modest baseline + mild scaling with CPU; OOM hints override.
        ram_req = max(_ram_min(pri), int(base * 2))
        ram_hint = s.op_ram_hint.get(key, None)
        if ram_hint is not None and ram_hint > ram_req:
            ram_req = int(ram_hint)

        if ram_req < 1:
            ram_req = 1
        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool

        return base, ram_req, cpu_min

    # ---- Picking policy: boosted first, then SRPT-ish (fewer remaining ops), then older first ----
    def _pick_best_from_queue(q, pool, pri, avail_cpu, avail_ram, reserve_cpu, reserve_ram, per_pid_count, runnable_estimate):
        best_idx = None
        best = None  # (score, pipeline, op, cpu_req, ram_req, cpu_min)
        n = len(q)
        for i in range(n):
            p = q[i]
            pid = p.pipeline_id

            if pid in s.ignored:
                continue

            # Per-tick per-pipeline cap
            if per_pid_count.get(pid, 0) >= s.max_ops_per_pipeline_per_tick:
                continue

            try:
                st = p.runtime_status()
            except Exception:
                continue

            try:
                if st.is_pipeline_successful():
                    # Drop completed pipelines aggressively to avoid re-enqueue churn.
                    _drop_pipeline(pid, permanent=True)
                    continue
            except Exception:
                pass

            try:
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = None

            if not ops:
                continue

            op = ops[0]
            cpu_req, ram_req, cpu_min = _compute_request(pool, pri, pid, op, runnable_estimate)

            # Respect reservation for queries (and optionally for interactive) since we don't preempt.
            eff_cpu = avail_cpu - reserve_cpu
            eff_ram = avail_ram - reserve_ram
            if eff_cpu < cpu_min or eff_ram < 1:
                return None  # can't schedule anything lower-priority right now

            # Fit checks: RAM must fit; CPU can be shrunk down to available if still >= cpu_min.
            if ram_req > eff_ram:
                continue
            if cpu_req > eff_cpu:
                cpu_req = eff_cpu
                if cpu_req < cpu_min:
                    continue

            # Score: boosted first; then fewest remaining ops; then oldest wait.
            boosted = _is_boosted(pid)
            rem = _remaining_ops_estimate(p)
            wait = _wait_ticks(pid)

            # Lower is better.
            score = 0
            if not boosted:
                score += 1_000_000
            score += rem * 1000
            score -= min(wait, 999)

            if best is None or score < best[0]:
                best = (score, p, op, cpu_req, ram_req, cpu_min)
                best_idx = i

        if best is None:
            return None

        # Move chosen pipeline to end for fairness (keeps it enqueued for later ops).
        chosen_p = best[1]
        try:
            q.pop(best_idx)
            q.append(chosen_p)
        except Exception:
            pass

        return best[1], best[2], best[3], best[4]

    # Runnable estimate per priority (queue length proxy, but capped to avoid extreme shrink)
    runnable_est = {
        Priority.QUERY: max(1, min(len(s.wait_q[Priority.QUERY]), 50)),
        Priority.INTERACTIVE: max(1, min(len(s.wait_q[Priority.INTERACTIVE]), 80)),
        Priority.BATCH_PIPELINE: max(1, min(len(s.wait_q[Priority.BATCH_PIPELINE]), 120)),
    }

    suspensions = []
    assignments = []

    # Per-tick per-pipeline scheduled op counters
    per_pid_count = {}

    # ---- Main scheduling loop ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reservations to protect query tail latency (no preemption available here).
        # Keep them small to avoid hurting throughput.
        reserve_cpu_for_query = 0
        reserve_ram_for_query = 0
        if query_runnable:
            reserve_cpu_for_query = 4 if pool.max_cpu_pool >= 8 else 1
            reserve_ram_for_query = 10 if pool.max_ram_pool >= 20 else 1

        # Interactive protection: only reserve if interactive is runnable and we are considering batch.
        reserve_cpu_for_interactive = 0
        reserve_ram_for_interactive = 0
        if interactive_runnable:
            reserve_cpu_for_interactive = 2 if pool.max_cpu_pool >= 8 else 0
            reserve_ram_for_interactive = 8 if pool.max_ram_pool >= 16 else 0

        while avail_cpu >= 1 and avail_ram >= 1:
            picked = False

            # Always try QUERY first.
            if avail_cpu >= 1 and avail_ram >= 1:
                res = _pick_best_from_queue(
                    s.wait_q[Priority.QUERY],
                    pool,
                    Priority.QUERY,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=0,
                    reserve_ram=0,
                    per_pid_count=per_pid_count,
                    runnable_estimate=runnable_est[Priority.QUERY],
                )
                if res is not None:
                    p, op, cpu_req, ram_req = res
                    pid = p.pipeline_id
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )
                    s.op_to_pipeline[_op_key(op)] = pid
                    m = _meta(pid)
                    m["last_sched_tick"] = s.ticks
                    per_pid_count[pid] = per_pid_count.get(pid, 0) + 1
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    picked = True
                    continue  # keep filling

            # Then INTERACTIVE (keep some headroom for queries if any are runnable).
            if avail_cpu >= 1 and avail_ram >= 1:
                res = _pick_best_from_queue(
                    s.wait_q[Priority.INTERACTIVE],
                    pool,
                    Priority.INTERACTIVE,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_cpu_for_query,
                    reserve_ram=reserve_ram_for_query,
                    per_pid_count=per_pid_count,
                    runnable_estimate=runnable_est[Priority.INTERACTIVE],
                )
                if res is not None:
                    p, op, cpu_req, ram_req = res
                    pid = p.pipeline_id
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )
                    s.op_to_pipeline[_op_key(op)] = pid
                    m = _meta(pid)
                    m["last_sched_tick"] = s.ticks
                    per_pid_count[pid] = per_pid_count.get(pid, 0) + 1
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    picked = True
                    continue

            # Finally BATCH: only if interactive isn't runnable, or batch is urgent (starved).
            allow_batch = (not interactive_runnable) or batch_urgent
            if allow_batch and avail_cpu >= 1 and avail_ram >= 1:
                # Reserve headroom for query and interactive (since we can't preempt).
                res = _pick_best_from_queue(
                    s.wait_q[Priority.BATCH_PIPELINE],
                    pool,
                    Priority.BATCH_PIPELINE,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_cpu_for_query + reserve_cpu_for_interactive,
                    reserve_ram=reserve_ram_for_query + reserve_ram_for_interactive,
                    per_pid_count=per_pid_count,
                    runnable_estimate=runnable_est[Priority.BATCH_PIPELINE],
                )
                if res is not None:
                    p, op, cpu_req, ram_req = res
                    pid = p.pipeline_id
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )
                    s.op_to_pipeline[_op_key(op)] = pid
                    m = _meta(pid)
                    m["last_sched_tick"] = s.ticks
                    per_pid_count[pid] = per_pid_count.get(pid, 0) + 1
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    picked = True
                    continue

            if not picked:
                break

    return suspensions, assignments
