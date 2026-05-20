@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_ptr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.holes = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.enqueued = set()
    s.dead_pipelines = set()

    # pipeline_id -> {"enq_tick": int, "last_sched_tick": int}
    s.pipeline_meta = {}

    # (pipeline_id, op_key) -> {"ram_floor": float, "ram_hint": float, "oom": int, "fails": int}
    s.op_rec = {}

    # op_key -> {"pid": int, "pool_id": int, "priority": Priority, "cpu": float, "ram": float}
    s.inflight_by_op = {}

    # pipeline_id -> number of in-flight containers (we schedule 1 op per container)
    s.inflight_per_pipeline = {}

    # Initialize pool accounting (may be resized if pools change)
    num_pools = getattr(s.executor, "num_pools", 1) or 1
    s.inflight_cpu_pool = [
        {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        for _ in range(num_pools)
    ]
    s.inflight_ram_pool = [
        {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        for _ in range(num_pools)
    ]

    # Knobs
    s.max_scan_per_pick = 160

    # Retry caps (bounded)
    s.max_oom_retries = 5
    s.max_non_oom_retries = 2
    s.max_total_failures = 6

    # Baseline resources (tuned to increase concurrency; OOM feedback bumps RAM)
    s.base_cpu = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.max_cpu_per_op = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 12.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    s.base_ram = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # Pipeline parallelism caps
    s.max_inflight_per_pipeline = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Aging / anti-timeout boosting
    s.starve_ticks_interactive = 180
    s.starve_ticks_batch = 320
    s.starve_ticks_query = 60


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _ensure_pool_accounting():
        num_pools = getattr(s.executor, "num_pools", 1) or 1
        if len(s.inflight_cpu_pool) != num_pools:
            s.inflight_cpu_pool = [
                {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
                for _ in range(num_pools)
            ]
            s.inflight_ram_pool = [
                {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
                for _ in range(num_pools)
            ]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.dead_pipelines.discard(pid)
        s.inflight_per_pipeline.pop(pid, None)
        # op_rec intentionally kept (can help future retries), but bounded by workload size.

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        last = meta.get("last_sched_tick", None)
        if last is not None:
            return max(0, s.ticks - last)
        return max(0, s.ticks - meta.get("enq_tick", s.ticks))

    def _maybe_compact_queue(pri):
        q = s.queues[pri]
        if not q:
            s.rr_ptr[pri] = 0
            s.holes[pri] = 0
            return
        holes = s.holes[pri]
        if holes <= 0:
            return
        if holes < 64 and holes < (len(q) // 5):
            return
        new_q = []
        for p in q:
            if p is not None:
                new_q.append(p)
        s.queues[pri] = new_q
        s.rr_ptr[pri] = 0 if not new_q else (s.rr_ptr[pri] % len(new_q))
        s.holes[pri] = 0

    def _get_op_rec(pid, opk):
        key = (pid, opk)
        rec = s.op_rec.get(key)
        if rec is None:
            rec = {"ram_floor": 0.0, "ram_hint": 0.0, "oom": 0, "fails": 0}
            s.op_rec[key] = rec
        return rec

    def _cpu_request(pool, pri, q_len_hint, avail_cpu, starved=False):
        cpu = s.base_cpu.get(pri, 2.0)

        # If starved, slightly increase CPU to reduce tail latency for old work.
        if starved:
            cpu = max(cpu, cpu + 1.0)

        # If queue is very small and we have a lot of free CPU, spend more CPU to reduce latency.
        if q_len_hint <= 10 and avail_cpu >= (pool.max_cpu_pool * 0.35):
            cpu = max(cpu, cpu * 2.0)

        # Cap by priority and pool
        cpu = min(cpu, s.max_cpu_per_op.get(pri, cpu), pool.max_cpu_pool)

        # Ensure at least 1 CPU
        if cpu < 1.0:
            cpu = 1.0

        # Finally clip to local availability (caller will also check)
        if cpu > avail_cpu:
            cpu = avail_cpu
        return cpu

    def _ram_request(pool, pri, pid, opk):
        base = s.base_ram.get(pri, 8.0)
        rec = _get_op_rec(pid, opk)

        # If we have an OOM-driven hint, use it.
        if rec["ram_hint"] and rec["ram_hint"] > 0.0:
            ram = rec["ram_hint"]
        else:
            ram = base

        # Ensure above the known OOM floor with a safety margin.
        floor = rec["ram_floor"] or 0.0
        if floor > 0.0:
            ram = max(ram, floor * 1.20)

        # Clip to pool
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        if ram < 1.0:
            ram = 1.0
        return ram

    def _shares(has_q, has_i, has_b, q_pressure=False, b_pressure=False):
        # Shares are for *running CPU* targets, per pool.
        if has_q and has_i and has_b:
            if q_pressure:
                return {Priority.QUERY: 0.45, Priority.INTERACTIVE: 0.40, Priority.BATCH_PIPELINE: 0.15}
            if b_pressure:
                return {Priority.QUERY: 0.28, Priority.INTERACTIVE: 0.47, Priority.BATCH_PIPELINE: 0.25}
            return {Priority.QUERY: 0.30, Priority.INTERACTIVE: 0.50, Priority.BATCH_PIPELINE: 0.20}
        if has_q and has_i:
            if q_pressure:
                return {Priority.QUERY: 0.48, Priority.INTERACTIVE: 0.52, Priority.BATCH_PIPELINE: 0.0}
            return {Priority.QUERY: 0.40, Priority.INTERACTIVE: 0.60, Priority.BATCH_PIPELINE: 0.0}
        if has_q and has_b:
            if b_pressure:
                return {Priority.QUERY: 0.45, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.55}
            return {Priority.QUERY: 0.55, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.45}
        if has_i and has_b:
            if b_pressure:
                return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.60, Priority.BATCH_PIPELINE: 0.40}
            return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.65, Priority.BATCH_PIPELINE: 0.35}
        if has_q:
            return {Priority.QUERY: 1.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        if has_i:
            return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 1.0, Priority.BATCH_PIPELINE: 0.0}
        if has_b:
            return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 1.0}
        return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}

    def _pick_next(pri, pool, pool_id, avail_cpu, avail_ram):
        q = s.queues[pri]
        n = len(q)
        if n == 0 or avail_cpu < 1.0 or avail_ram < 1.0:
            return None, None, None, None

        start = s.rr_ptr.get(pri, 0)
        if start >= n:
            start = 0
        scan_n = n if n < s.max_scan_per_pick else s.max_scan_per_pick

        # Two-pass: try to find a starved pipeline first.
        def _scan(require_starved):
            nonlocal start
            for j in range(scan_n):
                idx = start + j
                if idx >= n:
                    idx -= n
                p = q[idx]
                if p is None:
                    continue

                pid = p.pipeline_id
                if pid in s.dead_pipelines:
                    q[idx] = None
                    s.holes[pri] += 1
                    continue

                status = p.runtime_status()
                if status.is_pipeline_successful():
                    q[idx] = None
                    s.holes[pri] += 1
                    _drop_pipeline(pid)
                    continue

                wait = _pipeline_wait_ticks(pid)
                if require_starved:
                    if pri == Priority.QUERY:
                        if wait < s.starve_ticks_query:
                            continue
                    elif pri == Priority.INTERACTIVE:
                        if wait < s.starve_ticks_interactive:
                            continue
                    else:
                        if wait < s.starve_ticks_batch:
                            continue

                inflight = s.inflight_per_pipeline.get(pid, 0)
                cap = s.max_inflight_per_pipeline.get(pri, 1)
                if inflight >= cap:
                    continue

                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue
                op = ops[0]
                opk = _op_key(op)

                ram_req = _ram_request(pool, pri, pid, opk)
                if ram_req > avail_ram:
                    continue

                cpu_req = _cpu_request(
                    pool,
                    pri,
                    q_len_hint=n,
                    avail_cpu=avail_cpu,
                    starved=require_starved,
                )
                if cpu_req < 1.0 or cpu_req > avail_cpu:
                    continue

                # Move rr pointer past this pipeline
                s.rr_ptr[pri] = idx + 1
                if s.rr_ptr[pri] >= n:
                    s.rr_ptr[pri] -= n
                return p, op, cpu_req, ram_req
            return None, None, None, None

        p, op, cpu_req, ram_req = _scan(require_starved=True)
        if p is not None:
            return p, op, cpu_req, ram_req

        p, op, cpu_req, ram_req = _scan(require_starved=False)
        return p, op, cpu_req, ram_req

    _ensure_pool_accounting()

    # --- Ingest pipelines (new or first-seen) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enq_tick": s.ticks, "last_sched_tick": None}
        s.queues[p.priority].append(p)

    # --- Process results: release inflight, update RAM hints, handle failure caps ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        # We schedule 1 op per container; use the first op as the inflight key.
        op0 = ops[0]
        opk0 = _op_key(op0)

        infl = s.inflight_by_op.pop(opk0, None)
        if infl is not None:
            pool_id = infl["pool_id"]
            pri = infl["priority"]
            pid = infl["pid"]
            cpu_rel = infl["cpu"]
            ram_rel = infl["ram"]

            s.inflight_cpu_pool[pool_id][pri] = max(0.0, s.inflight_cpu_pool[pool_id][pri] - cpu_rel)
            s.inflight_ram_pool[pool_id][pri] = max(0.0, s.inflight_ram_pool[pool_id][pri] - ram_rel)

            cur = s.inflight_per_pipeline.get(pid, 0)
            if cur <= 1:
                s.inflight_per_pipeline.pop(pid, None)
            else:
                s.inflight_per_pipeline[pid] = cur - 1
        else:
            # If we can't match inflight state, still proceed with RAM hinting via pipeline mapping if possible.
            pri = getattr(r, "priority", None)
            pid = None

        failed = False
        if hasattr(r, "failed"):
            failed = bool(r.failed())

        if failed:
            err = getattr(r, "error", None)
            is_oom = _is_oom_error(err)

            # Attribute to pid if we can
            if infl is not None:
                pid = infl["pid"]
                pri_eff = infl["priority"]
            else:
                pri_eff = getattr(r, "priority", Priority.BATCH_PIPELINE)

            if pid is None:
                # Can't safely learn without a pipeline id.
                continue

            # Update per-op RAM record for each op in the container (usually 1).
            for op in ops:
                opk = _op_key(op)
                rec = _get_op_rec(pid, opk)
                rec["fails"] += 1
                if is_oom:
                    rec["oom"] += 1

                    alloc_ram = getattr(r, "ram", None)
                    if alloc_ram is None or alloc_ram <= 0:
                        alloc_ram = rec["ram_hint"] or s.base_ram.get(pri_eff, 8.0)

                    # Raise floor to at least what just OOM'd.
                    rec["ram_floor"] = max(rec["ram_floor"] or 0.0, float(alloc_ram))

                    # Aggressive but bounded convergence: jump by ~1.8x; after multiple OOMs, jump by 2x.
                    mult = 2.0 if rec["oom"] >= 2 else 1.8
                    next_hint = float(alloc_ram) * mult
                    # Ensure above floor margin
                    next_hint = max(next_hint, (rec["ram_floor"] or 0.0) * 1.25)

                    # Clip to pool max if we can infer pool; otherwise keep as-is and clip at scheduling.
                    pool_id_eff = getattr(r, "pool_id", None)
                    if pool_id_eff is not None and 0 <= pool_id_eff < s.executor.num_pools:
                        pool = s.executor.pools[pool_id_eff]
                        if next_hint > pool.max_ram_pool:
                            next_hint = pool.max_ram_pool

                    rec["ram_hint"] = max(rec["ram_hint"] or 0.0, next_hint)
                else:
                    # Non-OOM failure: allow a couple retries, then kill the pipeline to avoid infinite churn.
                    pass

                # Enforce bounded retry depth
                if rec["oom"] > s.max_oom_retries or rec["fails"] > s.max_total_failures:
                    s.dead_pipelines.add(pid)

            # Pipeline-level kill on too many non-OOM failures across ops
            if (not is_oom) and pid is not None:
                # If any op crosses non-OOM retry cap, kill pipeline.
                for op in ops:
                    opk = _op_key(op)
                    rec = _get_op_rec(pid, opk)
                    non_oom = rec["fails"] - rec["oom"]
                    if non_oom > s.max_non_oom_retries:
                        s.dead_pipelines.add(pid)
                        break

    # Periodic queue compaction
    if s.ticks % 37 == 0:
        _maybe_compact_queue(Priority.QUERY)
        _maybe_compact_queue(Priority.INTERACTIVE)
        _maybe_compact_queue(Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    # --- Per-pool scheduling with CPU-share targets + aging ---
    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Fast backlog hints (non-empty queues); we'll refine by pick failures below.
        has_q = len(s.queues[Priority.QUERY]) > 0
        has_i = len(s.queues[Priority.INTERACTIVE]) > 0
        has_b = len(s.queues[Priority.BATCH_PIPELINE]) > 0

        # Pressure signals via quick sampling: if we see very old pipelines, boost that class share.
        def _sample_pressure(pri, sample=24, starve=200):
            q = s.queues[pri]
            n = len(q)
            if n == 0:
                return False
            idx = s.rr_ptr.get(pri, 0)
            seen = 0
            checked = 0
            while checked < sample and checked < n:
                if idx >= n:
                    idx = 0
                p = q[idx]
                idx += 1
                checked += 1
                if p is None:
                    continue
                pid = p.pipeline_id
                if pid in s.dead_pipelines:
                    continue
                if _pipeline_wait_ticks(pid) >= starve:
                    return True
                seen += 1
                if seen >= 8:
                    break
            return False

        q_pressure = _sample_pressure(Priority.QUERY, starve=s.starve_ticks_query)
        b_pressure = _sample_pressure(Priority.BATCH_PIPELINE, starve=s.starve_ticks_batch)

        shares = _shares(has_q, has_i, has_b, q_pressure=q_pressure, b_pressure=b_pressure)

        # Track local running CPU (inflight) for target calculations; include currently running.
        inflight_cpu = {
            Priority.QUERY: float(s.inflight_cpu_pool[pool_id][Priority.QUERY]),
            Priority.INTERACTIVE: float(s.inflight_cpu_pool[pool_id][Priority.INTERACTIVE]),
            Priority.BATCH_PIPELINE: float(s.inflight_cpu_pool[pool_id][Priority.BATCH_PIPELINE]),
        }

        # Local scheduling loop
        # Avoid infinite loops if queues are blocked
        stall_count = 0
        max_iters = int(pool.max_cpu_pool) + 64
        iters = 0

        while local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0 and iters < max_iters:
            iters += 1

            # Choose which priority "needs" CPU most to reach its share target.
            best_pri = None
            best_need = -1e18

            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if shares.get(pri, 0.0) <= 0.0:
                    continue
                # Skip if its queue is empty-ish; we'll handle via pick returning None.
                target = shares[pri] * float(pool.max_cpu_pool)
                need = target - inflight_cpu.get(pri, 0.0)

                # Small bias toward higher priorities when needs are similar
                if pri == Priority.QUERY:
                    need += 0.15
                elif pri == Priority.INTERACTIVE:
                    need += 0.05

                if need > best_need:
                    best_need = need
                    best_pri = pri

            # If all shares are zero or no best, fall back to strict priority.
            if best_pri is None:
                for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    if len(s.queues[pri]) > 0:
                        best_pri = pri
                        break

            if best_pri is None:
                break

            p, op, cpu_req, ram_req = _pick_next(best_pri, pool, pool_id, local_cpu[pool_id], local_ram[pool_id])

            # If nothing found in the chosen class, try other classes this tick before stalling.
            if p is None:
                tried = 1
                for alt in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    if alt == best_pri or shares.get(alt, 0.0) <= 0.0:
                        continue
                    p, op, cpu_req, ram_req = _pick_next(alt, pool, pool_id, local_cpu[pool_id], local_ram[pool_id])
                    tried += 1
                    if p is not None:
                        best_pri = alt
                        break

                if p is None:
                    stall_count += 1
                    if stall_count >= 3:
                        break
                    continue

            stall_count = 0

            # Final fit check against local snapshots
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                # Shouldn't happen due to clipping, but be safe.
                continue

            pid = p.pipeline_id
            pri = p.priority
            opk = _op_key(op)

            # If this pipeline got killed, skip
            if pid in s.dead_pipelines:
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Update local & global accounting
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            s.inflight_cpu_pool[pool_id][pri] += cpu_req
            s.inflight_ram_pool[pool_id][pri] += ram_req
            inflight_cpu[pri] = inflight_cpu.get(pri, 0.0) + cpu_req

            s.inflight_by_op[opk] = {
                "pid": pid,
                "pool_id": pool_id,
                "priority": pri,
                "cpu": cpu_req,
                "ram": ram_req,
            }
            s.inflight_per_pipeline[pid] = s.inflight_per_pipeline.get(pid, 0) + 1

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_sched_tick"] = s.ticks

        _maybe_compact_queue(Priority.QUERY)
        _maybe_compact_queue(Priority.INTERACTIVE)
        _maybe_compact_queue(Priority.BATCH_PIPELINE)

    return suspensions, assignments
