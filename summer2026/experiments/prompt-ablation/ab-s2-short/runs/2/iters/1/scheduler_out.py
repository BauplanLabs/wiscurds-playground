@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_idx = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.enqueued = set()
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int, "arrival_time": float|None}

    # Per-operator adaptive hints
    # key: (pipeline_id, op_key) -> hint
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_fail_count = {}

    # op_key -> pipeline_id for attributing results
    s.op_to_pipeline = {}

    # Recent-arrival tracking for headroom
    s.last_query_tick = -10**9
    s.last_interactive_tick = -10**9

    # Periodic queue compaction
    s._compact_every = 50

    # Hard stop for pathological repeated failures (avoid endless thrash)
    s.max_failures_per_op = 8


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _safe_float(x, default=0.0):
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    def _safe_int(x, default=0):
        try:
            if x is None:
                return default
            xi = int(x)
            return xi
        except Exception:
            return default

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda oom" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg) or ("took too long" in msg)

    def _get_op_min_ram(op):
        # Try common attribute names used in simulators / operator specs.
        candidates = [
            "min_ram",
            "min_ram_gb",
            "ram_min",
            "required_ram",
            "required_ram_gb",
            "memory_min",
            "min_memory",
            "min_memory_gb",
            "peak_ram_min",
        ]
        for name in candidates:
            v = getattr(op, name, None)
            fv = _safe_float(v, default=0.0)
            if fv and fv > 0:
                return fv
        return 0.0

    def _get_op_min_cpu(op):
        candidates = ["min_cpu", "cpu_min", "required_cpu", "required_vcpu", "min_vcpu"]
        for name in candidates:
            v = getattr(op, name, None)
            fv = _safe_float(v, default=0.0)
            if fv and fv > 0:
                return fv
        return 1.0

    def _default_cpu_for_priority(pool, pri):
        # Fixed-ish per-op CPU to scale with cluster size via concurrency (not per-op bloat).
        # Keep QUERY snappy, INTERACTIVE responsive, BATCH throughput oriented.
        maxc = _safe_float(getattr(pool, "max_cpu_pool", 1), 1.0)
        if pri == Priority.QUERY:
            return float(min(10, max(4, int(maxc // 12) or 4)))
        if pri == Priority.INTERACTIVE:
            return float(min(6, max(2, int(maxc // 20) or 2)))
        return float(min(4, max(1, int(maxc // 28) or 2)))

    def _ram_per_cpu_for_priority(pri):
        # Bias towards CPU-limited packing; use spare RAM to reduce OOM risk without hurting throughput.
        # With typical 500GB/64CPU ~= 7.8GB/CPU, using ~6GB/CPU keeps CPU as the limiter.
        if pri == Priority.QUERY:
            return 6.0
        if pri == Priority.INTERACTIVE:
            return 6.0
        return 6.0

    def _min_ram_floor(pri):
        if pri == Priority.QUERY:
            return 12.0
        if pri == Priority.INTERACTIVE:
            return 10.0
        return 8.0

    def _max_ops_per_pipeline_per_call(pri):
        if pri == Priority.QUERY:
            return 2
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)

        min_cpu = max(1.0, _get_op_min_cpu(op))
        min_ram = max(0.0, _get_op_min_ram(op))

        # CPU hinting (timeout-driven)
        base_cpu = _default_cpu_for_priority(pool, pri)
        cpu_hint = s.op_cpu_hint.get((pid, opk), None)
        cpu_req = float(cpu_hint) if (cpu_hint is not None and cpu_hint > 0) else base_cpu
        cpu_req = max(cpu_req, min_cpu)

        # RAM sizing: max(min_ram * safety, cpu_req * ram_per_cpu, floor), then apply hint if larger.
        safety = 1.35 if pri in (Priority.QUERY, Priority.INTERACTIVE) else 1.40
        ram_req = max(min_ram * safety, cpu_req * _ram_per_cpu_for_priority(pri), _min_ram_floor(pri))

        ram_hint = s.op_ram_hint.get((pid, opk), None)
        if ram_hint is not None and ram_hint > ram_req:
            ram_req = float(ram_hint)

        # Cap to pool maxima
        max_cpu = _safe_float(getattr(pool, "max_cpu_pool", 1), 1.0)
        max_ram = _safe_float(getattr(pool, "max_ram_pool", 1), 1.0)
        if cpu_req > max_cpu:
            cpu_req = max_cpu
        if ram_req > max_ram:
            ram_req = max_ram

        # Ensure positive
        if cpu_req < 1.0:
            cpu_req = 1.0
        if ram_req < 1.0:
            ram_req = 1.0

        return cpu_req, ram_req

    def _maybe_compact_queues():
        if s.ticks % s._compact_every != 0:
            return
        alive = s.enqueued
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                s.rr_idx[pri] = 0
                continue
            newq = []
            for p in q:
                if getattr(p, "pipeline_id", None) in alive:
                    newq.append(p)
            s.wait_q[pri] = newq
            if not newq:
                s.rr_idx[pri] = 0
            else:
                s.rr_idx[pri] = s.rr_idx[pri] % len(newq)

    # --------------------
    # Ingest new pipelines
    # --------------------
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            continue
        if pid in s.enqueued:
            continue
        pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
        s.wait_q[pri].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {
            "enqueued_tick": s.ticks,
            "arrival_time": getattr(p, "arrival_time", None),
        }
        if pri == Priority.QUERY:
            s.last_query_tick = s.ticks
        elif pri == Priority.INTERACTIVE:
            s.last_interactive_tick = s.ticks

    # --------------------
    # Process results (adaptive hints)
    # --------------------
    for r in results:
        ops = getattr(r, "ops", []) or []
        pri = getattr(r, "priority", None)
        pool_id = getattr(r, "pool_id", None)
        pool_obj = None
        if pool_id is not None:
            try:
                pool_obj = s.executor.pools[pool_id]
            except Exception:
                pool_obj = None

        for op in ops:
            opk = _op_key(op)
            pid = getattr(r, "pipeline_id", None) or s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            # Increment/clear failure counters
            if hasattr(r, "failed") and r.failed():
                key = (pid, opk)
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                # If this op is consistently failing, stop escalating beyond pool max (if known).
                err = getattr(r, "error", None)

                if _is_oom_error(err):
                    prev = s.op_ram_hint.get(key, None)
                    alloc = _safe_float(getattr(r, "ram", None), default=0.0)
                    min_ram = _get_op_min_ram(op)
                    base = 0.0
                    if alloc > 0:
                        base = alloc
                    elif prev is not None:
                        base = float(prev)
                    else:
                        base = max(min_ram * 1.4, _min_ram_floor(pri if pri is not None else Priority.BATCH_PIPELINE))

                    # Aggressive enough to converge quickly, but not explode.
                    new_hint = max(base * 1.8, (float(prev) * 1.6 if prev is not None else 0.0), min_ram * 1.6)
                    if pool_obj is not None:
                        max_ram = _safe_float(getattr(pool_obj, "max_ram_pool", None), default=new_hint)
                        if max_ram > 0:
                            new_hint = min(new_hint, max_ram)
                    s.op_ram_hint[key] = new_hint

                elif _is_timeout_error(err):
                    prev = s.op_cpu_hint.get((pid, opk), None)
                    alloc = _safe_float(getattr(r, "cpu", None), default=0.0)
                    min_cpu = _get_op_min_cpu(op)
                    base = alloc if alloc > 0 else (float(prev) if (prev is not None and prev > 0) else min_cpu)
                    new_cpu = max(base * 1.5, min_cpu + 1.0)
                    if pool_obj is not None:
                        max_cpu = _safe_float(getattr(pool_obj, "max_cpu_pool", None), default=new_cpu)
                        if max_cpu > 0:
                            new_cpu = min(new_cpu, max_cpu)
                    s.op_cpu_hint[(pid, opk)] = new_cpu

                else:
                    # Unknown error: nudge both slightly (often resource-related in practice).
                    # Avoid endless wasting if it keeps failing.
                    key_fail = (pid, opk)
                    if s.op_fail_count.get(key_fail, 0) <= 3:
                        # Mild RAM bump
                        prev_ram = s.op_ram_hint.get(key_fail, None)
                        alloc_ram = _safe_float(getattr(r, "ram", None), default=0.0)
                        base_ram = alloc_ram if alloc_ram > 0 else (float(prev_ram) if prev_ram is not None else 0.0)
                        if base_ram > 0:
                            bump = base_ram * 1.25
                            if pool_obj is not None:
                                max_ram = _safe_float(getattr(pool_obj, "max_ram_pool", None), default=bump)
                                if max_ram > 0:
                                    bump = min(bump, max_ram)
                            s.op_ram_hint[key_fail] = max(bump, float(prev_ram) if prev_ram is not None else 0.0)

                        # Mild CPU bump
                        prev_cpu = s.op_cpu_hint.get(key_fail, None)
                        alloc_cpu = _safe_float(getattr(r, "cpu", None), default=0.0)
                        base_cpu = alloc_cpu if alloc_cpu > 0 else (float(prev_cpu) if prev_cpu is not None else 0.0)
                        if base_cpu > 0:
                            bumpc = base_cpu * 1.20
                            if pool_obj is not None:
                                max_cpu = _safe_float(getattr(pool_obj, "max_cpu_pool", None), default=bumpc)
                                if max_cpu > 0:
                                    bumpc = min(bumpc, max_cpu)
                            s.op_cpu_hint[key_fail] = max(bumpc, float(prev_cpu) if prev_cpu is not None else 0.0)

            else:
                # Success: clear fail count for this op
                s.op_fail_count.pop((pid, opk), None)

            # Clean mapping; safe because we only schedule single-op containers here.
            s.op_to_pipeline.pop(opk, None)

    _maybe_compact_queues()

    if not pipelines and not results and not s.enqueued:
        return [], []

    suspensions = []
    assignments = []

    # --------------------
    # Scheduling helpers
    # --------------------
    def _pick_ready(pri, pool, avail_cpu, avail_ram, scheduled_counts, search_limit, enforce_headroom_cpu, enforce_headroom_ram):
        q = s.wait_q[pri]
        if not q:
            return None

        n = len(q)
        if n == 0:
            return None

        # Search a small window (starting at rr_idx) and pick the "best" candidate by age.
        start = s.rr_idx.get(pri, 0) % n
        best = None  # (score, idx, pipeline, op, cpu_req, ram_req)

        tries = min(n, max(1, search_limit))
        for t in range(tries):
            idx = (start + t) % n
            p = q[idx]
            pid = getattr(p, "pipeline_id", None)
            if pid is None or pid not in s.enqueued:
                continue

            # Hard stop for pathological failing ops: avoid burning capacity forever.
            # (We apply this per op at selection time; failures counted in results processing.)
            # If a pipeline has no schedulable ops except ones that exceed fail limit, it will naturally stall.
            status = p.runtime_status()

            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            if scheduled_counts.get(pid, 0) >= _max_ops_per_pipeline_per_call(pri):
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                continue

            op = op_list[0]
            opk = _op_key(op)
            if s.op_fail_count.get((pid, opk), 0) >= s.max_failures_per_op:
                continue

            cpu_req, ram_req = _size_request(pool, pri, pid, op)

            # Fit checks (do not clip RAM down; that risks OOM).
            if ram_req > avail_ram:
                continue

            # CPU can be flexed down a bit to fit, but keep >= op min.
            min_cpu = max(1.0, _get_op_min_cpu(op))
            if cpu_req > avail_cpu:
                cpu_req = avail_cpu
            if cpu_req < min_cpu:
                continue

            # Enforce headroom for future QUERY (mainly for BATCH placements).
            if enforce_headroom_cpu is not None and (avail_cpu - cpu_req) < enforce_headroom_cpu:
                continue
            if enforce_headroom_ram is not None and (avail_ram - ram_req) < enforce_headroom_ram:
                continue

            age = _pipeline_wait_ticks(pid)
            # Mild "finish sooner" bonus: if only one op is runnable/assignable, prioritize a bit.
            # (Cheap proxy, avoids expensive full remaining-op counts.)
            runnable_count = len(op_list)
            finisher_bonus = 5 if runnable_count <= 1 else 0
            score = age + finisher_bonus

            if best is None or score > best[0]:
                best = (score, idx, p, op, cpu_req, ram_req)

        if best is None:
            # Move rr_idx a bit to avoid getting stuck on a blocked prefix.
            s.rr_idx[pri] = (start + tries) % n
            return None

        _, idx, p, op, cpu_req, ram_req = best

        # Advance rr_idx beyond the chosen item to keep fairness.
        s.rr_idx[pri] = (idx + 1) % n

        return p, op, cpu_req, ram_req

    # --------------------
    # Main scheduling loop
    # --------------------
    # Latency pools: reserve a subset primarily for QUERY/INTERACTIVE, keep batch out unless slack.
    num_pools = s.executor.num_pools
    num_latency_pools = 1
    if num_pools > 1:
        num_latency_pools = max(1, int(round(num_pools * 0.25)))

    # Headroom policy: if no pending QUERY right now but a QUERY arrived recently, keep enough for one query op.
    query_recent_window = 4  # ticks
    query_queue_has_backlog = len(s.wait_q[Priority.QUERY]) > 0
    preserve_for_future_query = (not query_queue_has_backlog) and ((s.ticks - s.last_query_tick) <= query_recent_window)

    # Precompute a conservative "one query op" headroom per pool on-demand (pool-specific CPU default).
    def _query_headroom(pool):
        cpu_q = _default_cpu_for_priority(pool, Priority.QUERY)
        ram_q = max(cpu_q * _ram_per_cpu_for_priority(Priority.QUERY), _min_ram_floor(Priority.QUERY))
        return cpu_q, ram_q

    # Per-call pipeline scheduling limits
    scheduled_counts = {}

    # Two-phase per pool:
    #   1) prioritize QUERY then INTERACTIVE up to a cap that leaves some batch capacity (prevents batch starvation)
    #   2) fill remaining with BATCH (respecting future-query headroom when enabled)
    batch_min_cpu_share = 0.22  # minimum share of currently available CPU to attempt to dedicate to batch (when backlog exists)

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = _safe_float(getattr(pool, "avail_cpu_pool", 0.0), 0.0)
        avail_ram = _safe_float(getattr(pool, "avail_ram_pool", 0.0), 0.0)

        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Determine whether to keep QUERY headroom from BATCH in this pool.
        # In latency pools, preserve headroom more aggressively.
        enforce_future_query_headroom = preserve_for_future_query and (pool_id < num_latency_pools)
        headroom_cpu = None
        headroom_ram = None
        if enforce_future_query_headroom:
            hc, hr = _query_headroom(pool)
            headroom_cpu = hc
            headroom_ram = hr

        batch_backlog = len(s.wait_q[Priority.BATCH_PIPELINE]) > 0
        interactive_backlog = len(s.wait_q[Priority.INTERACTIVE]) > 0
        query_backlog = len(s.wait_q[Priority.QUERY]) > 0

        # In throughput pools, bias more towards batch; in latency pools, bias more to Q/I.
        if pool_id < num_latency_pools:
            local_batch_share = batch_min_cpu_share
        else:
            local_batch_share = min(0.40, max(0.28, batch_min_cpu_share + 0.10))

        cpu_start = avail_cpu
        cpu_target_for_batch = (cpu_start * local_batch_share) if batch_backlog else 0.0
        cpu_cap_for_hi = max(0.0, cpu_start - cpu_target_for_batch)

        cpu_used_hi = 0.0
        did_progress = True

        # Phase 1: schedule QUERY then INTERACTIVE until hi cap reached (or no more schedulable hi work)
        while did_progress and avail_cpu >= 1.0 and avail_ram >= 1.0 and cpu_used_hi < cpu_cap_for_hi:
            did_progress = False

            # Prefer QUERY strongly in latency pools; in throughput pools still allow QUERY if present.
            hi_priorities = [Priority.QUERY, Priority.INTERACTIVE]

            for pri in hi_priorities:
                if pri == Priority.INTERACTIVE and (pool_id >= num_latency_pools) and batch_backlog and not query_backlog:
                    # In throughput pools, if no queries are waiting, allow interactive but don't let it crowd out batch.
                    # We'll still schedule it here; batch cap is handled by cpu_cap_for_hi.
                    pass

                picked = _pick_ready(
                    pri=pri,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    scheduled_counts=scheduled_counts,
                    search_limit=10 if pri == Priority.QUERY else 12,
                    enforce_headroom_cpu=None,  # headroom applies only to batch
                    enforce_headroom_ram=None,
                )
                if picked is None:
                    continue

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

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                cpu_used_hi += cpu_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

                did_progress = True
                break

        # Phase 2: fill with BATCH (and any remaining INTERACTIVE/QUERY if batch empty), respecting future-query headroom if enabled
        did_progress = True
        while did_progress and avail_cpu >= 1.0 and avail_ram >= 1.0:
            did_progress = False

            # In latency pools, keep trying any remaining HI work before batch (low tail latency).
            if pool_id < num_latency_pools:
                for pri in (Priority.QUERY, Priority.INTERACTIVE):
                    picked = _pick_ready(
                        pri=pri,
                        pool=pool,
                        avail_cpu=avail_cpu,
                        avail_ram=avail_ram,
                        scheduled_counts=scheduled_counts,
                        search_limit=8 if pri == Priority.QUERY else 10,
                        enforce_headroom_cpu=None,
                        enforce_headroom_ram=None,
                    )
                    if picked is None:
                        continue

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

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

                    did_progress = True
                    break

                if did_progress:
                    continue

            # Then BATCH (or, if no batch backlog, try interactive)
            if batch_backlog:
                picked = _pick_ready(
                    pri=Priority.BATCH_PIPELINE,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    scheduled_counts=scheduled_counts,
                    search_limit=18,
                    enforce_headroom_cpu=headroom_cpu,
                    enforce_headroom_ram=headroom_ram,
                )
                if picked is not None:
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

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

                    did_progress = True
                    continue

            # If batch not schedulable (or absent), try INTERACTIVE then QUERY (for throughput pools)
            for pri in (Priority.INTERACTIVE, Priority.QUERY):
                picked = _pick_ready(
                    pri=pri,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    scheduled_counts=scheduled_counts,
                    search_limit=10,
                    enforce_headroom_cpu=None,
                    enforce_headroom_ram=None,
                )
                if picked is None:
                    continue

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

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

                did_progress = True
                break

    # Opportunistic cleanup: drop pipelines that have completed but still in enqueued set.
    # (We don't have a global list of all active pipelines here, so we only clean those we touch.)
    # This keeps bookkeeping bounded without expensive scans.

    return suspensions, assignments
