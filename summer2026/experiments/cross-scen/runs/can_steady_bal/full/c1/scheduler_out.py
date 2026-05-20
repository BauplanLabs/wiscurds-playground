@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Active pipelines: pipeline_id -> Pipeline
    s.active = {}
    s.dead = set()

    # Per-pipeline inflight operator count
    s.pipeline_inflight = {}

    # Ready operator queues per priority: store (pipeline_id, op_obj)
    s.ready_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.ready_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # De-dup / inflight tracking for ops
    # key is (pipeline_id, op_uid) where op_uid = id(op_obj)
    s.op_enqueued = set()
    s.op_inflight = set()

    # Map op_uid -> pipeline_id (for attributing results)
    s.op_to_pipeline = {}

    # Track allocation per inflight op (for debugging / possible future use)
    s.inflight_alloc = {}  # (pid, op_uid) -> (cpu, ram, pri, pool_id)

    # OOM-driven RAM learning
    s.op_ram_hint = {}  # (pid, op_uid) -> ram
    s.pipeline_ram_floor = {}  # pid -> ram floor to apply to ops without hints

    # Retry limits
    s.op_oom_fails = {}     # (pid, op_uid) -> count
    s.op_other_fails = {}   # (pid, op_uid) -> count
    s.max_oom_retries = 6
    s.max_other_retries = 2

    # Per-pipeline inflight cap (allows some parallelism but prevents a single pipeline from hogging)
    s.inflight_cap = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Scheduler performance knobs
    s.max_new_assignments_per_tick = 64
    s.max_candidate_scans = 24  # per attempted placement

    # Small always-on headroom to reduce high-priority tail latency without killing throughput
    s.always_reserve_query_cpu = 2
    s.always_reserve_query_ram = 2


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_uid(op):
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return (
            ("oom" in msg)
            or ("out of memory" in msg)
            or ("memoryerror" in msg)
            or ("cuda out of memory" in msg)
            or ("killed" in msg and "memory" in msg)
        )

    def _q_len(pri):
        h = s.ready_head[pri]
        q = s.ready_q[pri]
        n = len(q) - h
        return n if n > 0 else 0

    def _compact_queue(pri):
        q = s.ready_q[pri]
        h = s.ready_head[pri]
        if h > 256 and h > (len(q) // 2):
            s.ready_q[pri] = q[h:]
            s.ready_head[pri] = 0

    def _pipeline_completed(p):
        try:
            return p.runtime_status().is_pipeline_successful()
        except Exception:
            return False

    def _cleanup_pipeline(pid):
        s.active.pop(pid, None)
        s.pipeline_inflight.pop(pid, None)
        s.pipeline_ram_floor.pop(pid, None)

    def _base_cpu(pool, pri):
        mc = float(pool.max_cpu_pool)
        if pri == Priority.QUERY:
            v = mc * 0.12
            if v < 4:
                v = 4
            if v > 16:
                v = 16
        elif pri == Priority.INTERACTIVE:
            v = mc * 0.09
            if v < 2:
                v = 2
            if v > 12:
                v = 12
        else:
            v = mc * 0.06
            if v < 1:
                v = 1
            if v > 8:
                v = 8
        iv = int(v)
        return iv if iv >= 1 else 1

    def _base_ram(pool, pri):
        mr = float(pool.max_ram_pool)
        if pri == Priority.QUERY:
            v = mr * 0.010
            if v < 8:
                v = 8
            if v > 48:
                v = 48
        elif pri == Priority.INTERACTIVE:
            v = mr * 0.008
            if v < 6:
                v = 6
            if v > 32:
                v = 32
        else:
            v = mr * 0.006
            if v < 4:
                v = 4
            if v > 24:
                v = 24
        return v if v >= 1 else 1.0

    def _size_request(pool, pri, pid, op):
        opid = _op_uid(op)
        key = (pid, opid)

        cpu = float(_base_cpu(pool, pri))
        ram = float(_base_ram(pool, pri))

        # Apply learned floor for this pipeline (helps reduce repeated OOMs across similar ops)
        pf = s.pipeline_ram_floor.get(pid, 0.0)
        if pf and pf > ram:
            ram = float(pf)

        # Apply learned per-op hint (dominates base/floor)
        hint = s.op_ram_hint.get(key, None)
        if hint is not None and hint > ram:
            ram = float(hint)

        # Clip to pool maximums
        if cpu > float(pool.max_cpu_pool):
            cpu = float(pool.max_cpu_pool)
        if ram > float(pool.max_ram_pool):
            ram = float(pool.max_ram_pool)

        if cpu < 1:
            cpu = 1.0
        if ram < 1:
            ram = 1.0

        return cpu, ram

    def _enqueue_ready_ops(p):
        pid = p.pipeline_id
        if pid in s.dead:
            return
        if pid not in s.active:
            return
        if _pipeline_completed(p):
            _cleanup_pipeline(pid)
            return

        try:
            ops = p.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            ops = []

        # Small pipelines (<=9 ops), safe to enqueue all ready ops; de-dup via s.op_enqueued.
        for op in ops:
            opid = _op_uid(op)
            key = (pid, opid)
            if key in s.op_inflight or key in s.op_enqueued:
                continue
            s.ready_q[p.priority].append((pid, op))
            s.op_enqueued.add(key)

    # --- Ingest new pipelines (assumed to be arrivals this tick) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead:
            continue
        # Update/insert active reference
        if pid not in s.active:
            s.active[pid] = p
            s.pipeline_inflight[pid] = 0
        else:
            s.active[pid] = p
        _enqueue_ready_ops(p)

    # --- Process results (update inflight, learn RAM on OOM, re-enqueue newly-ready ops) ---
    affected_pids = set()
    for r in results:
        ops = getattr(r, "ops", None) or []
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        failed = False
        if hasattr(r, "failed"):
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

        is_oom = False
        if failed:
            is_oom = _is_oom_error(getattr(r, "error", None))

        alloc_ram = getattr(r, "ram", None)
        if alloc_ram is None:
            alloc_ram = 0.0
        else:
            try:
                alloc_ram = float(alloc_ram)
            except Exception:
                alloc_ram = 0.0

        for op in ops:
            opid = _op_uid(op)
            pid = s.op_to_pipeline.get(opid, None)
            if pid is None:
                continue

            affected_pids.add(pid)

            key = (pid, opid)

            if key in s.op_inflight:
                s.op_inflight.discard(key)
                s.inflight_alloc.pop(key, None)
                cur = s.pipeline_inflight.get(pid, 0)
                s.pipeline_inflight[pid] = cur - 1 if cur > 0 else 0

            # Update retry state / RAM hints on failure
            if failed and pid not in s.dead:
                if is_oom:
                    prev = s.op_ram_hint.get(key, None)
                    prev_val = float(prev) if prev is not None else 0.0

                    # Exponential backoff with slightly faster early growth
                    oom_ct = s.op_oom_fails.get(key, 0) + 1
                    s.op_oom_fails[key] = oom_ct
                    factor = 2.0 if oom_ct <= 2 else 1.6

                    base_for_pri = None
                    p_obj = s.active.get(pid, None)
                    if p_obj is not None and pool is not None:
                        base_for_pri = _base_ram(pool, p_obj.priority)
                    else:
                        base_for_pri = 1.0

                    base = prev_val
                    if alloc_ram > base:
                        base = alloc_ram
                    if base_for_pri > base:
                        base = float(base_for_pri)

                    new_hint = base * factor
                    if pool is not None and new_hint > float(pool.max_ram_pool):
                        new_hint = float(pool.max_ram_pool)
                    if new_hint < 1:
                        new_hint = 1.0

                    s.op_ram_hint[key] = new_hint

                    # Raise pipeline-wide floor moderately to reduce repeated OOMs on similar ops
                    pf = s.pipeline_ram_floor.get(pid, 0.0)
                    floor_candidate = new_hint * 0.6
                    if pool is not None:
                        cap = float(pool.max_ram_pool) * 0.75
                        if floor_candidate > cap:
                            floor_candidate = cap
                    if floor_candidate > pf:
                        s.pipeline_ram_floor[pid] = floor_candidate

                    # Give up if repeated OOMs
                    if s.op_oom_fails.get(key, 0) > s.max_oom_retries:
                        s.dead.add(pid)
                else:
                    other_ct = s.op_other_fails.get(key, 0) + 1
                    s.op_other_fails[key] = other_ct
                    if other_ct > s.max_other_retries:
                        s.dead.add(pid)

    # Re-enqueue ops for pipelines impacted by results
    for pid in affected_pids:
        if pid in s.dead:
            _cleanup_pipeline(pid)
            continue
        p = s.active.get(pid, None)
        if p is None:
            continue
        if _pipeline_completed(p):
            _cleanup_pipeline(pid)
            continue
        _enqueue_ready_ops(p)

    # Compact queues occasionally
    _compact_queue(Priority.QUERY)
    _compact_queue(Priority.INTERACTIVE)
    _compact_queue(Priority.BATCH_PIPELINE)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]

    # Prefer to spread assignment budget across pools (if multiple) to avoid starving later pools
    total_budget = s.max_new_assignments_per_tick
    per_pool_budget = total_budget // num_pools
    if per_pool_budget < 1:
        per_pool_budget = 1
    extra = total_budget - (per_pool_budget * num_pools)

    # Schedule pools with more headroom first
    pool_order = list(range(num_pools))
    pool_order.sort(key=lambda i: local_cpu[i], reverse=True)

    # Cache assignable-op lists per pipeline for this tick to ensure "only assign assignable ops"
    assignable_cache = {}

    def _assignable_ops_now(p):
        pid = p.pipeline_id
        v = assignable_cache.get(pid, None)
        if v is not None:
            return v
        try:
            v = p.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            v = []
        assignable_cache[pid] = v
        return v

    def _take_from_queue(pri):
        q = s.ready_q[pri]
        h = s.ready_head[pri]
        if h >= len(q):
            return None
        item = q[h]
        s.ready_head[pri] = h + 1
        return item

    def _requeue(pri, item):
        # Keep op_enqueued marker; we are just moving it to the tail by appending a fresh entry.
        s.ready_q[pri].append(item)

    def _drop_enqueued(pid, op):
        s.op_enqueued.discard((pid, _op_uid(op)))

    def _try_pick_op(pri, pool, pool_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        scanned = 0
        while scanned < s.max_candidate_scans and _q_len(pri) > 0:
            scanned += 1
            item = _take_from_queue(pri)
            if item is None:
                return None
            pid, op = item
            opid = _op_uid(op)
            key = (pid, opid)

            # Drop dead or inactive pipelines
            if pid in s.dead:
                _drop_enqueued(pid, op)
                continue
            p = s.active.get(pid, None)
            if p is None:
                _drop_enqueued(pid, op)
                continue
            if _pipeline_completed(p):
                _drop_enqueued(pid, op)
                _cleanup_pipeline(pid)
                continue

            # If already inflight, drop stale queue entry
            if key in s.op_inflight:
                _drop_enqueued(pid, op)
                continue

            # Enforce per-pipeline inflight cap
            cap = s.inflight_cap.get(p.priority, 1)
            if s.pipeline_inflight.get(pid, 0) >= cap:
                _requeue(pri, item)
                continue

            # Validate op is assignable *now*
            assignable = _assignable_ops_now(p)
            ok = False
            for aop in assignable:
                if aop is op:
                    ok = True
                    break
            if not ok:
                # Stale; drop and let future enqueue refresh if needed
                _drop_enqueued(pid, op)
                continue

            cpu_req, ram_req = _size_request(pool, pri, pid, op)

            # Enforce headroom reservation for higher priorities
            if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                # Can't fit while keeping reserved headroom: move to tail and try other work
                _requeue(pri, item)
                continue

            # Check fit
            if cpu_req > avail_cpu or ram_req > avail_ram:
                _requeue(pri, item)
                continue

            return p, op, cpu_req, ram_req

        return None

    # Precompute queue lengths (lightweight) for ratio decisions
    q_len_query = _q_len(Priority.QUERY)
    q_len_inter = _q_len(Priority.INTERACTIVE)
    q_len_batch = _q_len(Priority.BATCH_PIPELINE)

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = local_cpu[pool_id]
        avail_ram = local_ram[pool_id]

        budget = per_pool_budget + (1 if extra > 0 else 0)
        if extra > 0:
            extra -= 1

        if budget <= 0 or avail_cpu < 1 or avail_ram < 1:
            continue

        # Compute base sizes for headroom decisions
        base_q_cpu = _base_cpu(pool, Priority.QUERY)
        base_q_ram = _base_ram(pool, Priority.QUERY)
        base_i_cpu = _base_cpu(pool, Priority.INTERACTIVE)
        base_i_ram = _base_ram(pool, Priority.INTERACTIVE)

        # Effective always-on query headroom (tiny)
        always_q_cpu = float(s.always_reserve_query_cpu)
        always_q_ram = float(s.always_reserve_query_ram)

        # If we currently have query backlog, keep enough headroom to start at least one query quickly
        if q_len_query > 0:
            reserve_q_cpu = float(max(always_q_cpu, int(base_q_cpu * 0.8)))
            reserve_q_ram = float(max(always_q_ram, base_q_ram))
        else:
            reserve_q_cpu = float(always_q_cpu)
            reserve_q_ram = float(always_q_ram)

        # If we have interactive backlog, keep headroom while placing batch
        if q_len_inter > 0:
            reserve_i_cpu = float(max(1, int(base_i_cpu * 0.8)))
            reserve_i_ram = float(max(1.0, base_i_ram))
        else:
            reserve_i_cpu = 0.0
            reserve_i_ram = 0.0

        # Limit how much CPU we spend on queries in one tick when interactive exists (avoid monopolizing)
        start_cpu = avail_cpu
        query_cpu_cap = start_cpu
        if q_len_inter > 0 and start_cpu > 0:
            query_cpu_cap = start_cpu * 0.65
        query_cpu_used = 0.0

        # Decide interactive vs batch mix based on ready backlog
        i_len = q_len_inter
        b_len = q_len_batch
        if b_len > i_len * 2:
            interactive_per_batch = 1
        elif b_len > i_len:
            interactive_per_batch = 2
        else:
            interactive_per_batch = 3
        ib_count = 0

        # Local scheduling loop
        stall_loops = 0
        while budget > 0 and avail_cpu >= 1 and avail_ram >= 1:
            stall_loops += 1
            if stall_loops > 200:
                break

            # Refresh coarse queue lengths occasionally (cheap)
            if stall_loops % 16 == 0:
                q_len_query = _q_len(Priority.QUERY)
                q_len_inter = _q_len(Priority.INTERACTIVE)
                q_len_batch = _q_len(Priority.BATCH_PIPELINE)

            picked = None

            # 1) Queries: try to place if any are ready and we haven't exceeded the per-tick query cap
            if q_len_query > 0 and (query_cpu_used < query_cpu_cap or q_len_inter == 0):
                picked = _try_pick_op(
                    Priority.QUERY,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=0.0,
                    reserve_ram=0.0,
                )
                if picked is not None:
                    p, op, cpu_req, ram_req = picked
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
                    opid = _op_uid(op)
                    key = (p.pipeline_id, opid)
                    s.op_enqueued.discard(key)
                    s.op_inflight.add(key)
                    s.op_to_pipeline[opid] = p.pipeline_id
                    s.inflight_alloc[key] = (cpu_req, ram_req, p.priority, pool_id)
                    s.pipeline_inflight[p.pipeline_id] = s.pipeline_inflight.get(p.pipeline_id, 0) + 1

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    local_cpu[pool_id] = avail_cpu
                    local_ram[pool_id] = avail_ram
                    query_cpu_used += cpu_req
                    budget -= 1
                    continue

            # 2) Interactive vs Batch mix (while protecting query headroom; and protecting interactive headroom from batch)
            prefer_batch = False
            if q_len_batch > 0 and (q_len_inter == 0 or ib_count >= interactive_per_batch):
                prefer_batch = True

            if prefer_batch and q_len_batch > 0:
                picked = _try_pick_op(
                    Priority.BATCH_PIPELINE,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_q_cpu + reserve_i_cpu,
                    reserve_ram=reserve_q_ram + reserve_i_ram,
                )
                if picked is None and q_len_inter > 0:
                    # Fall back to interactive if batch can't fit under headroom constraints
                    picked = _try_pick_op(
                        Priority.INTERACTIVE,
                        pool,
                        pool_id,
                        avail_cpu,
                        avail_ram,
                        reserve_cpu=reserve_q_cpu,
                        reserve_ram=reserve_q_ram,
                    )
                if picked is not None:
                    p, op, cpu_req, ram_req = picked
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
                    opid = _op_uid(op)
                    key = (p.pipeline_id, opid)
                    s.op_enqueued.discard(key)
                    s.op_inflight.add(key)
                    s.op_to_pipeline[opid] = p.pipeline_id
                    s.inflight_alloc[key] = (cpu_req, ram_req, p.priority, pool_id)
                    s.pipeline_inflight[p.pipeline_id] = s.pipeline_inflight.get(p.pipeline_id, 0) + 1

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    local_cpu[pool_id] = avail_cpu
                    local_ram[pool_id] = avail_ram
                    budget -= 1

                    if p.priority == Priority.BATCH_PIPELINE:
                        ib_count = 0
                    else:
                        ib_count += 1
                    continue

            # Prefer interactive if available
            if q_len_inter > 0:
                picked = _try_pick_op(
                    Priority.INTERACTIVE,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_q_cpu,
                    reserve_ram=reserve_q_ram,
                )
                if picked is not None:
                    p, op, cpu_req, ram_req = picked
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
                    opid = _op_uid(op)
                    key = (p.pipeline_id, opid)
                    s.op_enqueued.discard(key)
                    s.op_inflight.add(key)
                    s.op_to_pipeline[opid] = p.pipeline_id
                    s.inflight_alloc[key] = (cpu_req, ram_req, p.priority, pool_id)
                    s.pipeline_inflight[p.pipeline_id] = s.pipeline_inflight.get(p.pipeline_id, 0) + 1

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    local_cpu[pool_id] = avail_cpu
                    local_ram[pool_id] = avail_ram
                    budget -= 1
                    ib_count += 1
                    continue

            # Then batch if any and it fits under headroom constraints
            if q_len_batch > 0:
                picked = _try_pick_op(
                    Priority.BATCH_PIPELINE,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_q_cpu + reserve_i_cpu,
                    reserve_ram=reserve_q_ram + reserve_i_ram,
                )
                if picked is not None:
                    p, op, cpu_req, ram_req = picked
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
                    opid = _op_uid(op)
                    key = (p.pipeline_id, opid)
                    s.op_enqueued.discard(key)
                    s.op_inflight.add(key)
                    s.op_to_pipeline[opid] = p.pipeline_id
                    s.inflight_alloc[key] = (cpu_req, ram_req, p.priority, pool_id)
                    s.pipeline_inflight[p.pipeline_id] = s.pipeline_inflight.get(p.pipeline_id, 0) + 1

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    local_cpu[pool_id] = avail_cpu
                    local_ram[pool_id] = avail_ram
                    budget -= 1
                    ib_count = 0
                    continue

            # Nothing placeable right now for this pool
            break

    return suspensions, assignments
