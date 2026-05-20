@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Known pipelines by id (we only add on arrival).
    s.pipelines_by_id = {}

    # Ready queues are ring-buffers implemented via (list, head_index).
    s.ready_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.ready_h = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.ready_set = set()  # pipeline_ids currently present in some ready queue

    # Pipelines with an in-flight operator (we schedule at most 1 op per pipeline at a time).
    s.inflight = set()

    # Dead pipelines we stop scheduling (bounded retry / hopeless fits).
    s.dead = set()

    # Delayed re-check bucket: tick -> [pipeline_id,...]
    s.delay_bucket = {}

    # Map operator key -> pipeline_id for attributing results.
    s.op_to_pid = {}

    # Per-operator adaptive hints: (pipeline_id, op_key) -> meta
    # meta fields:
    #   ram_lb: largest RAM that OOM'd (lower bound)
    #   ram_hint: next RAM target
    #   cpu_hint: next CPU target (for timeout-like failures)
    #   oom_retries, timeout_retries, other_retries, total_retries
    s.op_meta = {}

    # Pipeline-level floors derived from failures (helps next ops in same pipeline).
    s.pipeline_ram_floor = {}  # pipeline_id -> ram_floor
    s.pipeline_cpu_floor = {}  # pipeline_id -> cpu_floor

    # Retry caps
    s.max_oom_retries = 6
    s.max_timeout_retries = 3
    s.max_other_retries = 2
    s.max_total_retries = 8

    # Scheduling limits (performance + stability)
    s.scan_limit = 16
    s.max_new_assignments_per_tick = 32

    # When high-priority work arrives, keep a short "burst reserve" window that limits new batch fills.
    s.reserve_until_tick = 0
    s.reserve_horizon_ticks = 300  # ~3s if tick rate is ~100Hz

    # Batch throttling while high-priority backlog exists (prevents interactive starvation during bursts)
    s.batch_limit_when_high = 8

    # Soft reservation (applied only to new BATCH placements during reserve window/backlog)
    s.reserve_frac = 0.08
    s.reserve_cpu_cap = 16
    s.reserve_ram_cap = 256

    # Cache global maxima (for clamping hints)
    max_ram = 0
    max_cpu = 0
    for i in range(s.executor.num_pools):
        p = s.executor.pools[i]
        if p.max_ram_pool > max_ram:
            max_ram = p.max_ram_pool
        if p.max_cpu_pool > max_cpu:
            max_cpu = p.max_cpu_pool
    s.global_max_ram = max_ram
    s.global_max_cpu = max_cpu


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1
    now = s.ticks

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _classify_error(err):
        if err is None:
            return None
        msg = str(err).lower()
        if ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg):
            return "oom"
        if ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg):
            return "timeout"
        return "other"

    def _q_len(pri):
        return len(s.ready_q[pri]) - s.ready_h[pri]

    def _q_compact(pri):
        h = s.ready_h[pri]
        q = s.ready_q[pri]
        if h > 64 and h * 2 > len(q):
            s.ready_q[pri] = q[h:]
            s.ready_h[pri] = 0

    def _q_append(pri, pid):
        s.ready_q[pri].append(pid)

    def _q_pop_left(pri):
        h = s.ready_h[pri]
        q = s.ready_q[pri]
        if h >= len(q):
            return None
        pid = q[h]
        s.ready_h[pri] = h + 1
        _q_compact(pri)
        return pid

    def _enqueue(pid):
        if pid in s.dead or pid in s.inflight or pid in s.ready_set:
            return
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return
        s.ready_set.add(pid)
        _q_append(p.priority, pid)

    def _cleanup_pipeline(pid):
        s.ready_set.discard(pid)
        s.inflight.discard(pid)
        s.dead.discard(pid)
        s.pipeline_ram_floor.pop(pid, None)
        s.pipeline_cpu_floor.pop(pid, None)
        # Note: we intentionally keep s.pipelines_by_id entries; they are few (<= workload size).

    # --- Process due delayed pipelines (no full scans; O(1) bucket lookup) ---
    due = s.delay_bucket.pop(now, None)
    if due:
        for pid in due:
            _enqueue(pid)

    # --- Ingest new arrivals ---
    # Treat `pipelines` as newly arrived in this tick.
    if pipelines:
        for p in pipelines:
            pid = p.pipeline_id
            if pid not in s.pipelines_by_id:
                s.pipelines_by_id[pid] = p
            # Burst reserve if high priority arrives
            if p.priority != Priority.BATCH_PIPELINE:
                if now + s.reserve_horizon_ticks > s.reserve_until_tick:
                    s.reserve_until_tick = now + s.reserve_horizon_ticks
            _enqueue(pid)

    # --- Process results: update hints, unblock pipelines (remove inflight), enqueue next op ---
    if results:
        for r in results:
            failed = (hasattr(r, "failed") and r.failed())
            err_cls = _classify_error(getattr(r, "error", None)) if failed else None

            ops = getattr(r, "ops", None) or []
            # Most assignments are single-op, but handle multi-op defensively.
            seen_pids = set()

            for op in ops:
                opk = _op_key(op)
                pid = s.op_to_pid.pop(opk, None)
                if pid is None:
                    continue

                if pid not in seen_pids:
                    s.inflight.discard(pid)
                    seen_pids.add(pid)

                p = s.pipelines_by_id.get(pid)
                if p is None or pid in s.dead:
                    continue

                key = (pid, opk)
                meta = s.op_meta.get(key)
                if meta is None:
                    meta = {
                        "ram_lb": 0,
                        "ram_hint": 0,
                        "cpu_hint": 0,
                        "oom_retries": 0,
                        "timeout_retries": 0,
                        "other_retries": 0,
                        "total_retries": 0,
                    }
                    s.op_meta[key] = meta

                if failed:
                    meta["total_retries"] += 1

                    alloc_ram = getattr(r, "ram", None)
                    alloc_cpu = getattr(r, "cpu", None)
                    alloc_ram = alloc_ram if (alloc_ram is not None and alloc_ram > 0) else 0
                    alloc_cpu = alloc_cpu if (alloc_cpu is not None and alloc_cpu > 0) else 0

                    if err_cls == "oom":
                        meta["oom_retries"] += 1
                        if alloc_ram > meta["ram_lb"]:
                            meta["ram_lb"] = alloc_ram

                        # Next RAM target: step up quickly but avoid over-shooting too hard.
                        prev = meta["ram_hint"] if meta["ram_hint"] > 0 else max(alloc_ram, 1)
                        lb = meta["ram_lb"]
                        next_ram = prev * 1.7
                        if lb > 0:
                            alt = lb * 1.35
                            if alt > next_ram:
                                next_ram = alt
                        next_ram = int(next_ram + 1)

                        if next_ram > s.global_max_ram:
                            next_ram = s.global_max_ram
                        meta["ram_hint"] = next_ram

                        # Help subsequent ops in this pipeline start closer to the needed RAM.
                        floor_prev = s.pipeline_ram_floor.get(pid, 0)
                        floor_new = int(next_ram * 0.9)
                        if floor_new > floor_prev:
                            s.pipeline_ram_floor[pid] = floor_new

                    elif err_cls == "timeout":
                        meta["timeout_retries"] += 1

                        # Increase CPU moderately (and a small RAM bump to avoid paging-like behavior).
                        prev_cpu = meta["cpu_hint"] if meta["cpu_hint"] > 0 else (alloc_cpu if alloc_cpu > 0 else 1)
                        next_cpu = int(prev_cpu * 1.5 + 1)
                        if next_cpu > s.global_max_cpu:
                            next_cpu = s.global_max_cpu
                        meta["cpu_hint"] = next_cpu

                        # Gentle RAM bump (kept small to avoid collapsing concurrency).
                        prev_ram = meta["ram_hint"] if meta["ram_hint"] > 0 else (alloc_ram if alloc_ram > 0 else 0)
                        if prev_ram > 0:
                            bump = int(prev_ram * 1.15 + 1)
                            if bump > s.global_max_ram:
                                bump = s.global_max_ram
                            if bump > meta["ram_hint"]:
                                meta["ram_hint"] = bump

                        # Pipeline CPU floor (helps later ops not under-provision CPU)
                        pf = s.pipeline_cpu_floor.get(pid, 0)
                        if next_cpu > pf:
                            s.pipeline_cpu_floor[pid] = next_cpu

                    else:
                        meta["other_retries"] += 1

                    # Bounded retries: kill hopeless operators to avoid hangs.
                    if (
                        meta["total_retries"] >= s.max_total_retries
                        or meta["oom_retries"] > s.max_oom_retries
                        or meta["timeout_retries"] > s.max_timeout_retries
                        or meta["other_retries"] > s.max_other_retries
                    ):
                        s.dead.add(pid)
                        _cleanup_pipeline(pid)
                        continue

                # Enqueue pipeline for next runnable op (or retry) unless completed/dead.
                if pid in s.dead:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _cleanup_pipeline(pid)
                else:
                    _enqueue(pid)

    # --- Prepare local availability snapshots (MUST be decremented locally) ---
    num_pools = s.executor.num_pools
    local_cpu = [0] * num_pools
    local_ram = [0] * num_pools
    pools = s.executor.pools
    for i in range(num_pools):
        local_cpu[i] = pools[i].avail_cpu_pool
        local_ram[i] = pools[i].avail_ram_pool

    # Reservation mode: active while high backlog exists or shortly after high arrivals (bursts).
    high_backlog = (_q_len(Priority.QUERY) + _q_len(Priority.INTERACTIVE)) > 0
    reserve_active = high_backlog or (now < s.reserve_until_tick)

    def _reserve_for_pool(pool):
        rc = pool.max_cpu_pool * s.reserve_frac
        rr = pool.max_ram_pool * s.reserve_frac
        if rc > s.reserve_cpu_cap:
            rc = s.reserve_cpu_cap
        if rr > s.reserve_ram_cap:
            rr = s.reserve_ram_cap
        # Keep small but non-zero; also avoid reserving more than pool capacity.
        if rc < 0:
            rc = 0
        if rr < 0:
            rr = 0
        if rc > pool.max_cpu_pool:
            rc = pool.max_cpu_pool
        if rr > pool.max_ram_pool:
            rr = pool.max_ram_pool
        return rc, rr

    def _base_cpu_for(pool, pri):
        # Small, stable CPU allocations improve concurrency for memory-bound workloads.
        if pri == Priority.QUERY:
            fixed, cmin, frac = 8, 4, 0.12
            cap = 16
        elif pri == Priority.INTERACTIVE:
            fixed, cmin, frac = 6, 2, 0.09
            cap = 12
        else:
            fixed, cmin, frac = 4, 1, 0.07
            cap = 8
        base = pool.max_cpu_pool * frac
        if base < cmin:
            base = cmin
        if base > fixed:
            base = fixed
        if base < 1:
            base = 1
        if base > cap:
            base = cap
        if base > pool.max_cpu_pool:
            base = pool.max_cpu_pool
        return int(base)

    def _base_ram_for(pool, pri):
        # Moderate base RAM: avoid the "60% allocated, 2-18% consumed" waste from pool-fraction sizing.
        if pri == Priority.QUERY:
            fixed, rmin, frac = 64, 32, 0.10
        elif pri == Priority.INTERACTIVE:
            fixed, rmin, frac = 64, 32, 0.10
        else:
            fixed, rmin, frac = 48, 24, 0.08

        base = pool.max_ram_pool * frac
        if base < rmin:
            base = rmin
        if base > fixed:
            base = fixed
        if base < 1:
            base = 1
        if base > pool.max_ram_pool:
            base = pool.max_ram_pool
        return int(base)

    def _place(p, op, pri):
        pid = p.pipeline_id
        opk = _op_key(op)
        meta = s.op_meta.get((pid, opk), None)
        ram_lb = meta["ram_lb"] if meta is not None else 0
        ram_hint = meta["ram_hint"] if meta is not None else 0
        cpu_hint = meta["cpu_hint"] if meta is not None else 0

        pipe_ram_floor = s.pipeline_ram_floor.get(pid, 0)
        pipe_cpu_floor = s.pipeline_cpu_floor.get(pid, 0)

        best = None  # (score, pool_id, cpu_req, ram_req)

        for i in range(num_pools):
            pool = pools[i]
            avail_c = local_cpu[i]
            avail_r = local_ram[i]

            # Apply soft reservation only when placing BATCH during reserve windows.
            if pri == Priority.BATCH_PIPELINE and reserve_active:
                rc, rr = _reserve_for_pool(pool)
                avail_c_eff = avail_c - rc
                avail_r_eff = avail_r - rr
                if avail_c_eff < 0:
                    avail_c_eff = 0
                if avail_r_eff < 0:
                    avail_r_eff = 0
            else:
                avail_c_eff = avail_c
                avail_r_eff = avail_r

            if avail_c_eff < 1 or avail_r_eff < 1:
                continue

            # CPU request (cap to keep concurrency high)
            cpu_req = _base_cpu_for(pool, pri)
            if pipe_cpu_floor > cpu_req:
                cpu_req = pipe_cpu_floor
            if cpu_hint > cpu_req:
                cpu_req = cpu_hint
            if cpu_req > pool.max_cpu_pool:
                cpu_req = int(pool.max_cpu_pool)
            if cpu_req < 1:
                cpu_req = 1

            # RAM request (must exceed lower bound that already OOM'd)
            ram_req = _base_ram_for(pool, pri)
            if pipe_ram_floor > ram_req:
                ram_req = pipe_ram_floor
            if ram_hint > ram_req:
                ram_req = ram_hint
            if ram_lb > 0:
                lb_target = int(ram_lb * 1.25 + 1)
                if lb_target > ram_req:
                    ram_req = lb_target

            # If request exceeds pool max, allow clamping *only if* it still exceeds ram_lb.
            if ram_req > pool.max_ram_pool:
                ram_req = int(pool.max_ram_pool)

            if ram_req < 1:
                ram_req = 1

            # Don't retry with <= last OOM'd RAM in the same pool.
            if ram_lb > 0 and ram_req <= ram_lb:
                # If already at pool max and it's still <= lb, this pool cannot satisfy this op.
                continue

            # Fit check (use effective availability for batch)
            if cpu_req <= avail_c_eff and ram_req <= avail_r_eff:
                # Scoring: pack batch (best-fit), give headroom preference to high priority.
                if pri == Priority.BATCH_PIPELINE:
                    score = (avail_r_eff - ram_req) * 1000 + (avail_c_eff - cpu_req)
                else:
                    score = -(avail_r_eff * 1000 + avail_c_eff)

                if best is None or score < best[0]:
                    best = (score, i, cpu_req, ram_req)

            else:
                # If only CPU is tight, allow CPU downshift (RAM is the risky dimension).
                if ram_req <= avail_r_eff and avail_c_eff >= 1:
                    min_cpu = 1
                    if pri == Priority.QUERY:
                        min_cpu = 2
                    elif pri == Priority.INTERACTIVE:
                        min_cpu = 1
                    cpu_req2 = cpu_req
                    if cpu_req2 > avail_c_eff:
                        cpu_req2 = int(avail_c_eff)
                    if cpu_req2 < min_cpu:
                        cpu_req2 = min_cpu

                    if cpu_req2 <= avail_c_eff and ram_req <= avail_r_eff and cpu_req2 >= 1:
                        if pri == Priority.BATCH_PIPELINE:
                            score = (avail_r_eff - ram_req) * 1000 + (avail_c_eff - cpu_req2) + 5
                        else:
                            score = -(avail_r_eff * 1000 + avail_c_eff) + 5
                        if best is None or score < best[0]:
                            best = (score, i, cpu_req2, ram_req)

        return best

    def _pick_and_schedule(pri, scan_limit, batch_budget_ok):
        if _q_len(pri) <= 0:
            return False

        scanned = 0
        while scanned < scan_limit:
            pid = _q_pop_left(pri)
            if pid is None:
                return False
            if pid not in s.ready_set:
                # Stale; continue.
                scanned += 1
                continue
            s.ready_set.discard(pid)

            if pid in s.dead or pid in s.inflight:
                scanned += 1
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                scanned += 1
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _cleanup_pipeline(pid)
                scanned += 1
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Avoid hot-spinning on pipelines that aren't runnable.
                release = now + 10
                bucket = s.delay_bucket.get(release)
                if bucket is None:
                    s.delay_bucket[release] = [pid]
                else:
                    bucket.append(pid)
                scanned += 1
                continue

            if pri == Priority.BATCH_PIPELINE and not batch_budget_ok:
                # Put it back quickly; we'll try later in the tick after high-priority work.
                s.ready_set.add(pid)
                _q_append(pri, pid)
                return False

            op = ops[0]
            placed = _place(p, op, pri)
            if placed is None:
                # Can't fit now: requeue to try later (do not block the whole queue).
                s.ready_set.add(pid)
                _q_append(pri, pid)
                scanned += 1
                continue

            _, pool_id, cpu_req, ram_req = placed
            # Final fit check against true local snapshot (defensive)
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                s.ready_set.add(pid)
                _q_append(pri, pid)
                scanned += 1
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            # Track for result attribution + in-flight gating
            s.inflight.add(pid)
            s.op_to_pid[_op_key(op)] = pid

            return True

        return False

    suspensions = []
    assignments = []

    max_new = s.max_new_assignments_per_tick
    if max_new < 1:
        max_new = 1

    # Batch limit while high backlog exists (prevents interactive starvation during bursts).
    batch_limit = max_new
    if high_backlog:
        batch_limit = s.batch_limit_when_high
        if batch_limit < 1:
            batch_limit = 1
        if batch_limit > max_new:
            batch_limit = max_new

    batch_scheduled = 0

    # Main loop: prioritize QUERY > INTERACTIVE, but still make steady BATCH progress.
    for _ in range(max_new):
        made = False

        # Always try to drain queries first.
        if _q_len(Priority.QUERY) > 0:
            if _pick_and_schedule(Priority.QUERY, s.scan_limit, batch_budget_ok=True):
                made = True

        if not made and _q_len(Priority.INTERACTIVE) > 0:
            if _pick_and_schedule(Priority.INTERACTIVE, s.scan_limit, batch_budget_ok=True):
                made = True

        # Batch, but throttled during high backlog
        if not made and _q_len(Priority.BATCH_PIPELINE) > 0:
            ok = (batch_scheduled < batch_limit)
            if _pick_and_schedule(Priority.BATCH_PIPELINE, s.scan_limit, batch_budget_ok=ok):
                made = True
                batch_scheduled += 1

        # If nothing placed, try relaxing the batch throttle (but reservations still apply in _place).
        if not made and _q_len(Priority.BATCH_PIPELINE) > 0 and batch_scheduled < max_new:
            if _pick_and_schedule(Priority.BATCH_PIPELINE, 4, batch_budget_ok=True):
                made = True
                batch_scheduled += 1

        if not made:
            break

    return suspensions, assignments
