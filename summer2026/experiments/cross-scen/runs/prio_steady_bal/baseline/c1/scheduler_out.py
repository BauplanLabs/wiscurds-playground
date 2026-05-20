@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler tuned for priority-heavy steady workloads.

    Key changes vs previous version:
      - Weighted round-robin across priorities (with INTERACTIVE favored to avoid mass timeouts).
      - Much smaller, capped per-op CPU/RAM requests to increase parallelism across all cluster sizes.
      - OOM-aware RAM learning using per-signature lower/upper bounds, plus cautious downshift on successes.
      - Mild "finish-what-you-start" bias via remaining-op estimation (helps completion rate).
      - No blanket blacklisting (completion rate is more valuable than avoiding a few retries).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.enqueued = set()
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int, "last_scheduled_tick": int}

    # op_key -> pipeline_id attribution for results
    s.op_to_pipeline = {}

    # Per (pipeline_id, op_key) hints / bounds
    s.op_ram_lb = {}   # minimum known required (from OOM)
    s.op_ram_ub = {}   # maximum known safe (from success)
    s.op_cpu_hint = {}  # from repeated timeouts / slowdowns (best-effort)

    # Per operator "signature" bounds for faster convergence across similar ops
    s.sig_ram_lb = {}
    s.sig_ram_ub = {}
    s.sig_cpu_hint = {}

    # Retry counts (avoid infinite churn on pathological non-resource errors)
    s.op_fail_count = {}  # (pipeline_id, op_key) -> int

    # Completed pipeline tracking (for metrics & cleanup)
    s.completed_pipelines = set()
    s.completed_counts = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.arrived_counts = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Weighted RR cycle (INTERACTIVE favored to prevent the massive timeout tail seen in this workload)
    # Share ≈ 58% INTERACTIVE, 33% QUERY, 8% BATCH.
    s.wrr_cycle = (
        [Priority.INTERACTIVE] * 7
        + [Priority.QUERY] * 4
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.wrr_idx = []  # per-pool index into cycle

    # Tuning knobs
    s.scan_limit = {
        Priority.QUERY: 64,
        Priority.INTERACTIVE: 96,
        Priority.BATCH_PIPELINE: 48,
    }
    s.urgent_wait_ticks = {
        Priority.QUERY: 120,
        Priority.INTERACTIVE: 60,
        Priority.BATCH_PIPELINE: 240,
    }

    # Per-tick max parallel ops per pipeline (across all pools)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 2,
    }

    # Soft failure cutoff (still retry resource failures well past this via OOM/timeout bumps)
    s.max_nonresource_failures = 6


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    ALL_NONTERM_STATES = {
        OperatorState.PENDING,
        OperatorState.FAILED,
        OperatorState.ASSIGNED,
        OperatorState.RUNNING,
        OperatorState.SUSPENDING,
    }

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _op_sig(op):
        # Best-effort: keep signatures reasonably stable and not too large.
        for attr in ("op_name", "name", "kind", "fn_name", "sql_name"):
            v = getattr(op, attr, None)
            if isinstance(v, str) and v:
                return v
        # Fallback: class name
        return type(op).__name__

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

    def _get_op_ram_min(op):
        # Generator likely provides a RAM minimum; try common attribute names.
        for attr in (
            "ram_min",
            "min_ram",
            "min_ram_gb",
            "min_memory",
            "memory_min",
            "mem_min",
            "min_mem",
            "required_ram",
            "required_memory",
        ):
            v = getattr(op, attr, None)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
        return None

    def _ensure_pool_state():
        n = s.executor.num_pools
        if len(s.wrr_idx) != n:
            s.wrr_idx = [0 for _ in range(n)]

    def _queue_for_priority(pri):
        return s.wait_q[pri]

    def _drop_pipeline(p):
        pid = p.pipeline_id
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _age_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _mark_completed(p):
        pid = p.pipeline_id
        if pid in s.completed_pipelines:
            return
        s.completed_pipelines.add(pid)
        s.completed_counts[p.priority] = s.completed_counts.get(p.priority, 0) + 1
        _drop_pipeline(p)

    def _default_cpu(pool, pri):
        # Capped, modest per-op CPU to avoid "one op monopolizes the pool" across big clusters.
        max_cpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            return min(16, max(4, int(max_cpu * 0.08)))
        if pri == Priority.INTERACTIVE:
            return min(32, max(8, int(max_cpu * 0.12)))
        return min(16, max(4, int(max_cpu * 0.06)))

    def _default_ram(pool, pri, cpu_req):
        # RAM per CPU + minimum floor, capped to prevent massive overallocation on large clusters.
        if pri == Priority.QUERY:
            floor_ = 24
            per_cpu = 6
            cap_ = 128
        elif pri == Priority.INTERACTIVE:
            floor_ = 48
            per_cpu = 8
            cap_ = 256
        else:
            floor_ = 24
            per_cpu = 6
            cap_ = 192

        ram = max(floor_, int(per_cpu * max(1, cpu_req)))
        ram = min(ram, cap_, pool.max_ram_pool)
        if ram < 1:
            ram = 1
        return ram

    def _combine_bounds(lb1, ub1, lb2, ub2):
        lb = None
        ub = None
        if lb1 is not None and lb2 is not None:
            lb = max(lb1, lb2)
        else:
            lb = lb1 if lb2 is None else lb2
        if ub1 is not None and ub2 is not None:
            ub = min(ub1, ub2)
        else:
            ub = ub1 if ub2 is None else ub2
        return lb, ub

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        opk = _op_key(op)
        sig = _op_sig(op)

        # CPU
        cpu_req = s.op_cpu_hint.get((pid, opk))
        if cpu_req is None:
            cpu_req = s.sig_cpu_hint.get(sig)
        if cpu_req is None:
            cpu_req = _default_cpu(pool, pri)

        # Clamp CPU to what we can actually allocate now.
        if cpu_req > pool.max_cpu_pool:
            cpu_req = pool.max_cpu_pool
        if cpu_req > avail_cpu:
            cpu_req = avail_cpu
        if cpu_req < 1:
            return None, None

        cpu_req = int(cpu_req)

        # RAM bounds
        op_min = _get_op_ram_min(op)
        op_lb = s.op_ram_lb.get((pid, opk))
        op_ub = s.op_ram_ub.get((pid, opk))
        sig_lb = s.sig_ram_lb.get(sig)
        sig_ub = s.sig_ram_ub.get(sig)

        lb, ub = _combine_bounds(op_lb, op_ub, sig_lb, sig_ub)
        if op_min is not None:
            # Treat min as a strong lower bound with small headroom.
            min_with_headroom = op_min * 1.10
            lb = min_with_headroom if lb is None else max(lb, min_with_headroom)

        # Baseline RAM
        ram_req = _default_ram(pool, pri, cpu_req)

        # If we have an upper bound but no lower bound, cautiously downshift from the known-safe value
        # to reduce over-allocation while keeping OOM risk low (especially for INTERACTIVE).
        if ub is not None and lb is None:
            decay = 0.95 if pri == Priority.INTERACTIVE else 0.90
            ram_req = min(ram_req, int(max(1, ub * decay)))
        elif lb is not None and ub is not None and lb < ub:
            # Pick a mid-point between bounds (geometric mean is robust across scales).
            mid = (lb * ub) ** 0.5
            # Also don't exceed baseline too much; but ensure we respect the lower bound.
            ram_req = int(max(lb, min(mid, ram_req)))
        elif lb is not None:
            ram_req = int(max(lb, ram_req))
        elif ub is not None:
            ram_req = int(min(ub, ram_req))

        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool
        if ram_req < 1:
            ram_req = 1

        # If it doesn't fit now, don't "clip" it down (would risk guaranteed OOM); instead declare unschedulable.
        if ram_req > avail_ram:
            return None, None

        return int(cpu_req), int(ram_req)

    def _remaining_ops_count(p):
        st = p.runtime_status()
        try:
            return len(st.get_ops(ALL_NONTERM_STATES, require_parents_complete=False))
        except Exception:
            # Fallback: unknown, treat as moderate.
            return 4

    def _pick_next(pri, pool, avail_cpu, avail_ram, scheduled_ops_by_pid):
        q = _queue_for_priority(pri)
        if not q:
            return None

        cursor = s.q_cursor.get(pri, 0)
        scans = min(len(q), s.scan_limit.get(pri, 64))
        best = None  # (score_tuple, pipeline, op, cpu, ram)

        for _ in range(scans):
            if not q:
                break

            n = len(q)
            idx = cursor % n
            p = q[idx]
            cursor = idx + 1

            pid = p.pipeline_id
            st = p.runtime_status()

            if st.is_pipeline_successful():
                _mark_completed(p)
                # Remove from queue to keep scans efficient.
                q.pop(idx)
                cursor -= 1
                continue

            # Per-tick per-pipeline cap (prevents one DAG from hogging a tick)
            max_ops = s.max_ops_per_pipeline_per_tick.get(pri, 1)
            if scheduled_ops_by_pid.get(pid, 0) >= max_ops:
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            cpu_req, ram_req = _size_request(pool, pri, pid, op, avail_cpu, avail_ram)
            if cpu_req is None:
                continue

            age = _age_ticks(pid)
            remaining = _remaining_ops_count(p)

            # Priority-specific tie-break:
            # - INTERACTIVE: oldest-first to avoid timeouts.
            # - QUERY/BATCH: "shorter remaining" first to increase completion rate.
            if pri == Priority.INTERACTIVE:
                score = (-age, remaining)
            else:
                score = (remaining, -age)

            if best is None or score < best[0]:
                best = (score, p, op, cpu_req, ram_req)

        s.q_cursor[pri] = cursor
        if best is None:
            return None
        return best[1], best[2], best[3], best[4]

    def _any_urgent(pri):
        q = _queue_for_priority(pri)
        if not q:
            return False
        limit = min(len(q), 16)
        thr = s.urgent_wait_ticks.get(pri, 120)
        # Peek a small sample (deterministic: from cursor forward) to detect urgency.
        cursor = s.q_cursor.get(pri, 0)
        for i in range(limit):
            p = q[(cursor + i) % len(q)]
            if _age_ticks(p.pipeline_id) >= thr:
                return True
        return False

    # --- Ensure per-pool WRR indices exist ---
    _ensure_pool_state()

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.completed_pipelines:
            continue
        if pid in s.enqueued:
            continue
        # Avoid enqueuing already-completed pipelines
        if p.runtime_status().is_pipeline_successful():
            _mark_completed(p)
            continue
        _queue_for_priority(p.priority).append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_scheduled_tick": 0}
        s.arrived_counts[p.priority] = s.arrived_counts.get(p.priority, 0) + 1

    # --- Process results: update bounds/hints ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = (hasattr(r, "failed") and r.failed())
        err = getattr(r, "error", None)
        is_oom = failed and _is_oom_error(err)
        is_to = failed and _is_timeout_error(err)

        # Prefer pool-specific cap when present
        pool_max_ram = None
        try:
            pool_max_ram = s.executor.pools[getattr(r, "pool_id", 0)].max_ram_pool
        except Exception:
            pool_max_ram = None

        alloc_ram = getattr(r, "ram", None)
        alloc_cpu = getattr(r, "cpu", None)

        for op in ops:
            opk = _op_key(op)
            sig = _op_sig(op)
            pid = s.op_to_pipeline.get(opk, None)

            if pid is None:
                continue

            # cleanup mapping to avoid growth
            s.op_to_pipeline.pop(opk, None)

            key = (pid, opk)
            if failed:
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                if is_oom:
                    if isinstance(alloc_ram, (int, float)) and alloc_ram > 0:
                        new_lb = float(alloc_ram) * 2.0  # aggressive to converge in <=2 tries
                        if pool_max_ram is not None:
                            new_lb = min(new_lb, float(pool_max_ram))
                        prev_op_lb = s.op_ram_lb.get(key)
                        prev_sig_lb = s.sig_ram_lb.get(sig)
                        s.op_ram_lb[key] = new_lb if prev_op_lb is None else max(prev_op_lb, new_lb)
                        s.sig_ram_lb[sig] = new_lb if prev_sig_lb is None else max(prev_sig_lb, new_lb)

                    # On OOM, any "upper bound" at/below this size is invalid; clear if conflicting.
                    op_ub = s.op_ram_ub.get(key)
                    if op_ub is not None and isinstance(alloc_ram, (int, float)) and op_ub <= float(alloc_ram):
                        s.op_ram_ub.pop(key, None)
                    sig_ub = s.sig_ram_ub.get(sig)
                    if sig_ub is not None and isinstance(alloc_ram, (int, float)) and sig_ub <= float(alloc_ram):
                        s.sig_ram_ub.pop(sig, None)

                elif is_to:
                    # Best-effort CPU bump on timeout-like failures.
                    if isinstance(alloc_cpu, (int, float)) and alloc_cpu > 0:
                        new_cpu = float(alloc_cpu) * 1.35
                        prev_op = s.op_cpu_hint.get(key)
                        prev_sig = s.sig_cpu_hint.get(sig)
                        s.op_cpu_hint[key] = new_cpu if prev_op is None else max(prev_op, new_cpu)
                        s.sig_cpu_hint[sig] = new_cpu if prev_sig is None else max(prev_sig, new_cpu)

                else:
                    # Non-resource failure: keep retrying, but avoid runaway resource inflation.
                    # If it keeps failing, modestly increase CPU to reduce stragglers, but do not mark dead.
                    if s.op_fail_count.get(key, 0) <= s.max_nonresource_failures:
                        if isinstance(alloc_cpu, (int, float)) and alloc_cpu > 0:
                            bump = float(alloc_cpu) * 1.15
                            prev = s.op_cpu_hint.get(key)
                            s.op_cpu_hint[key] = bump if prev is None else max(prev, bump)

            else:
                # Success: tighten upper bounds (safe RAM at this allocation)
                if isinstance(alloc_ram, (int, float)) and alloc_ram > 0:
                    prev_op_ub = s.op_ram_ub.get(key)
                    prev_sig_ub = s.sig_ram_ub.get(sig)
                    s.op_ram_ub[key] = float(alloc_ram) if prev_op_ub is None else min(prev_op_ub, float(alloc_ram))
                    s.sig_ram_ub[sig] = float(alloc_ram) if prev_sig_ub is None else min(prev_sig_ub, float(alloc_ram))

    # --- Scheduling ---
    suspensions = []
    assignments = []

    scheduled_ops_by_pid = {}

    # Order pools by available CPU (then RAM), descending, to reduce fragmentation.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    # Global urgency flags (cheap)
    urgent_interactive = _any_urgent(Priority.INTERACTIVE)
    urgent_query = _any_urgent(Priority.QUERY)
    urgent_batch = _any_urgent(Priority.BATCH_PIPELINE)

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu < 1 or avail_ram < 1:
            continue

        # Warm-start: ensure at least some INTERACTIVE progresses when present
        warm_interactive = 2 if len(s.wait_q[Priority.INTERACTIVE]) > 0 else 0
        warm_query = 1 if len(s.wait_q[Priority.QUERY]) > 0 else 0
        warm_batch = 1 if (len(s.wait_q[Priority.BATCH_PIPELINE]) > 0 and (s.ticks % 8 == 0)) else 0

        def _try_schedule_one(pri):
            nonlocal avail_cpu, avail_ram
            picked = _pick_next(pri, pool, avail_cpu, avail_ram, scheduled_ops_by_pid)
            if picked is None:
                return False
            p, op, cpu_req, ram_req = picked
            pid = p.pipeline_id

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            s.op_to_pipeline[_op_key(op)] = pid
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_scheduled_tick"] = s.ticks

            scheduled_ops_by_pid[pid] = scheduled_ops_by_pid.get(pid, 0) + 1

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            return True

        # Warm-up phase
        while warm_interactive > 0 and avail_cpu >= 1 and avail_ram >= 1:
            if not _try_schedule_one(Priority.INTERACTIVE):
                break
            warm_interactive -= 1

        while warm_query > 0 and avail_cpu >= 1 and avail_ram >= 1:
            if not _try_schedule_one(Priority.QUERY):
                break
            warm_query -= 1

        while warm_batch > 0 and avail_cpu >= 1 and avail_ram >= 1:
            if not _try_schedule_one(Priority.BATCH_PIPELINE):
                break
            warm_batch -= 1

        # Main WRR loop with urgency overrides
        idx = s.wrr_idx[pool_id]
        cycle = s.wrr_cycle
        cycle_len = len(cycle)

        while avail_cpu >= 1 and avail_ram >= 1:
            # Urgency override: if something is aging badly, bias selection for a few picks.
            if urgent_interactive and len(s.wait_q[Priority.INTERACTIVE]) > 0:
                if _try_schedule_one(Priority.INTERACTIVE):
                    continue
            if urgent_query and len(s.wait_q[Priority.QUERY]) > 0:
                # Keep queries from timing out as well (rare but costly).
                if _try_schedule_one(Priority.QUERY):
                    continue
            if urgent_batch and len(s.wait_q[Priority.BATCH_PIPELINE]) > 0 and (s.ticks % 4 == 0):
                if _try_schedule_one(Priority.BATCH_PIPELINE):
                    continue

            scheduled = False
            # Try up to one full cycle to find something schedulable.
            for _ in range(cycle_len):
                pri = cycle[idx]
                idx = (idx + 1) % cycle_len
                if len(s.wait_q[pri]) == 0:
                    continue
                if _try_schedule_one(pri):
                    scheduled = True
                    break

            if not scheduled:
                # As a last attempt, try any priority (in a deterministic order)
                if len(s.wait_q[Priority.INTERACTIVE]) > 0 and _try_schedule_one(Priority.INTERACTIVE):
                    continue
                if len(s.wait_q[Priority.QUERY]) > 0 and _try_schedule_one(Priority.QUERY):
                    continue
                if len(s.wait_q[Priority.BATCH_PIPELINE]) > 0 and _try_schedule_one(Priority.BATCH_PIPELINE):
                    continue
                break

        s.wrr_idx[pool_id] = idx

    # Opportunistic cleanup: mark completed pipelines encountered in the queues (cheap partial sweep)
    # Keeps queue scan costs bounded over long runs.
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.wait_q[pri]
        if not q:
            continue
        # Scan a small window from cursor forward.
        cursor = s.q_cursor.get(pri, 0)
        limit = min(len(q), 32)
        to_remove = []
        for i in range(limit):
            p = q[(cursor + i) % len(q)]
            if p.pipeline_id in s.completed_pipelines:
                to_remove.append(p.pipeline_id)
            else:
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _mark_completed(p)
                        to_remove.append(p.pipeline_id)
                except Exception:
                    pass
        if to_remove:
            # Remove by rebuilding (small, deterministic)
            remove_set = set(to_remove)
            s.wait_q[pri] = [p for p in q if p.pipeline_id not in remove_set]
            s.q_cursor[pri] = min(s.q_cursor.get(pri, 0), max(0, len(s.wait_q[pri]) - 1))

    return suspensions, assignments
