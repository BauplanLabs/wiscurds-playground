@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Ready queues per priority, implemented as list + head index (O(1) amortized pops).
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.in_q = set()  # pipeline_ids currently enqueued (prevents duplicates)

    # Pipeline registry / metadata
    s.pipes = {}         # pipeline_id -> Pipeline
    s.pid_pri = {}       # pipeline_id -> Priority
    s.pid_arrival_tick = {}
    s.pid_cooldown_until = {}  # pipeline_id -> tick until we should stop retrying it (resource/backoff)

    # OOM-driven memory discovery per operator attempt
    # key: (pipeline_id, stable_op_id) -> dict(lb, ub, est, oom, fail)
    s.op_meta = {}
    s.pipeline_ram_floor = {}  # pipeline_id -> largest RAM that OOM'd in this pipeline (helps seed new ops)

    # Map op instance / stable id back to pipeline for result attribution
    s.op_inst_to_pid = {}    # id(op) -> pipeline_id
    s.op_stable_to_pid = {}  # stable_op_id -> pipeline_id (best-effort fallback)

    # Running resource accounting to enforce non-preemptive headroom (caps)
    # running_cpu[pool_id][priority] = cpu currently "in flight" by our assignments
    s.running_cpu = []
    s.running_ram = []

    # Give up after bounded OOM retries per operator
    s.max_oom_retries = 5
    s.max_other_retries = 3
    s.dead_pipelines = set()

    # Batch sprinkling (avoid total starvation while still prioritizing INTERACTIVE)
    s.non_query_picks = 0
    s.batch_every = 7  # once every N non-query placements, try a batch if feasible


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _stable_op_id(op):
        return (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "id", None)
            or id(op)
        )

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _q_trim(pri):
        head = s.q_head[pri]
        q = s.q[pri]
        if head > 256 and head * 2 >= len(q):
            s.q[pri] = q[head:]
            s.q_head[pri] = 0

    def _q_push(pri, pid):
        if pid in s.in_q:
            return
        s.q[pri].append(pid)
        s.in_q.add(pid)

    def _q_pop(pri):
        head = s.q_head[pri]
        q = s.q[pri]
        if head >= len(q):
            return None
        pid = q[head]
        s.q_head[pri] = head + 1
        _q_trim(pri)
        s.in_q.discard(pid)
        return pid

    def _requeue(pid, pri, cooldown_ticks):
        if pid in s.dead_pipelines:
            return
        if cooldown_ticks and cooldown_ticks > 0:
            until = s.ticks + cooldown_ticks
            prev = s.pid_cooldown_until.get(pid, 0)
            if until > prev:
                s.pid_cooldown_until[pid] = until
        _q_push(pri, pid)

    def _cleanup_pipeline(pid):
        s.dead_pipelines.add(pid)
        s.in_q.discard(pid)
        s.pipes.pop(pid, None)
        s.pid_pri.pop(pid, None)
        s.pid_arrival_tick.pop(pid, None)
        s.pid_cooldown_until.pop(pid, None)
        s.pipeline_ram_floor.pop(pid, None)
        # leave op_meta / mappings (small) to avoid O(n) cleanup cost

    def _base_ram(pri):
        # Conservative-but-not-huge starting point; OOM bumps converge fast.
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 8
        return 6

    def _cpu_target(max_cpu, pri):
        # Prefer concurrency over fat containers; scale up gently with pool size.
        if pri == Priority.QUERY:
            v = max(4, int(max_cpu // 16))
            return 16 if v > 16 else v
        if pri == Priority.INTERACTIVE:
            v = max(2, int(max_cpu // 32))
            return 12 if v > 12 else v
        v = max(2, int(max_cpu // 64))
        return 8 if v > 8 else v

    def _cpu_min(pri):
        if pri == Priority.QUERY:
            return 2
        if pri == Priority.INTERACTIVE:
            return 1
        return 1

    def _cooldown_on_block(pri):
        # Avoid thrashing on pipelines whose next op isn't ready yet.
        if pri == Priority.QUERY:
            return 2
        if pri == Priority.INTERACTIVE:
            return 3
        return 5

    def _cooldown_on_no_fit(pri):
        # Re-try soon, but not every tick.
        if pri == Priority.QUERY:
            return 1
        if pri == Priority.INTERACTIVE:
            return 2
        return 3

    def _oom_bump(lb, oom_count):
        # Aggressive convergence to near-zero repeat OOMs.
        if lb < 1:
            lb = 1
        if oom_count <= 1:
            return int(lb * 2.0) + 1
        if oom_count == 2:
            return int(lb * 2.2) + 1
        return int(lb * 2.4) + 1

    def _ram_estimate(pid, pri, op, pool_max_ram):
        opid = _stable_op_id(op)
        key = (pid, opid)
        meta = s.op_meta.get(key)
        base = _base_ram(pri)

        floor = s.pipeline_ram_floor.get(pid, 0)
        if meta is None:
            # If we've already seen an OOM in this pipeline, seed unknown ops higher (but not too high).
            if floor and floor > 0:
                seeded = int(floor * 0.6) + 1
                if seeded > base:
                    base = seeded
            est = base
            if est > pool_max_ram:
                est = pool_max_ram
            return est

        lb = meta.get("lb", 0) or 0
        ub = meta.get("ub", None)
        est = meta.get("est", None)

        if est is None:
            est = base

        # Must be above last known-OOM lower bound.
        if lb and lb > 0:
            min_ok = int(lb * 1.12) + 1
            if est < min_ok:
                est = min_ok

        # Respect known success upper bound if we have it.
        if ub is not None and ub > 0 and est > ub:
            est = ub

        if est < base:
            est = base
        if est > pool_max_ram:
            est = pool_max_ram

        meta["est"] = est
        s.op_meta[key] = meta
        return est

    def _init_running_arrays_if_needed():
        if s.running_cpu and s.running_ram and len(s.running_cpu) == s.executor.num_pools:
            return
        s.running_cpu = []
        s.running_ram = []
        for _ in range(s.executor.num_pools):
            s.running_cpu.append({
                Priority.QUERY: 0,
                Priority.INTERACTIVE: 0,
                Priority.BATCH_PIPELINE: 0,
            })
            s.running_ram.append({
                Priority.QUERY: 0,
                Priority.INTERACTIVE: 0,
                Priority.BATCH_PIPELINE: 0,
            })

    def _caps_cpu(pool_id, pri, pool):
        # Non-preemptive headroom to keep QUERY tail latency low.
        # If we have multiple pools, treat pool 0 as latency-sensitive.
        max_cpu = pool.max_cpu_pool
        if s.executor.num_pools >= 2:
            if pool_id == 0:
                if pri == Priority.INTERACTIVE:
                    return max(_cpu_min(pri), int(max_cpu * 0.75))
                if pri == Priority.BATCH_PIPELINE:
                    return max(_cpu_min(pri), int(max_cpu * 0.10))
            # Throughput pools
            if pri == Priority.INTERACTIVE:
                return max(_cpu_min(pri), int(max_cpu * 0.90))
            if pri == Priority.BATCH_PIPELINE:
                return max(_cpu_min(pri), int(max_cpu * 0.90))
        else:
            # Single pool
            if pri == Priority.INTERACTIVE:
                return max(_cpu_min(pri), int(max_cpu * 0.70))
            if pri == Priority.BATCH_PIPELINE:
                return max(_cpu_min(pri), int(max_cpu * 0.20))
        # QUERY is uncapped
        return max_cpu

    def _pool_order_for_priority(pri):
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if pri == Priority.BATCH_PIPELINE:
            return list(range(1, n)) + [0]
        return [0] + list(range(1, n))

    def _pick_pid(pri, scan_limit):
        # Returns a pipeline_id or None. Bounded scan to avoid O(queue) per tick.
        for _ in range(scan_limit):
            pid = _q_pop(pri)
            if pid is None:
                return None
            if pid in s.dead_pipelines:
                continue
            until = s.pid_cooldown_until.get(pid, 0)
            if until and s.ticks < until:
                # Too soon; put back.
                _requeue(pid, pri, 0)
                continue
            p = s.pipes.get(pid)
            if p is None:
                continue
            # If completed, stop tracking.
            try:
                if p.runtime_status().is_pipeline_successful():
                    _cleanup_pipeline(pid)
                    continue
            except Exception:
                # If runtime_status breaks for any reason, don't wedge the scheduler.
                _requeue(pid, pri, _cooldown_on_block(pri))
                continue
            return pid
        return None

    # --- Init arrays for running accounting ---
    _init_running_arrays_if_needed()

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid not in s.pipes:
            s.pipes[pid] = p
            s.pid_pri[pid] = p.priority
            s.pid_arrival_tick[pid] = s.ticks
            _q_push(p.priority, pid)

    # --- Process results: update running totals and OOM estimates; enqueue pipeline for next op ---
    for r in results:
        # Decrement running totals based on what actually ran
        pool_id = getattr(r, "pool_id", None)
        r_pri = getattr(r, "priority", None)
        r_cpu = getattr(r, "cpu", 0) or 0
        r_ram = getattr(r, "ram", 0) or 0

        if pool_id is not None and r_pri is not None:
            try:
                prevc = s.running_cpu[pool_id].get(r_pri, 0)
                prevr = s.running_ram[pool_id].get(r_pri, 0)
                s.running_cpu[pool_id][r_pri] = prevc - r_cpu if prevc > r_cpu else 0
                s.running_ram[pool_id][r_pri] = prevr - r_ram if prevr > r_ram else 0
            except Exception:
                pass

        # Attribute to pipeline via op mapping
        pid = None
        ops_list = getattr(r, "ops", None) or []
        for op in ops_list:
            pid = s.op_inst_to_pid.get(id(op))
            if pid is not None:
                break
            pid = s.op_stable_to_pid.get(_stable_op_id(op))
            if pid is not None:
                break
        if pid is None or pid in s.dead_pipelines:
            continue

        pri = s.pid_pri.get(pid, None)
        if pri is None:
            # Best-effort: if we lost bookkeeping, try to use the result priority.
            pri = r_pri if r_pri is not None else Priority.BATCH_PIPELINE

        # Update OOM / failure metadata per operator (single-op containers are typical, but handle list)
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = False

        if ops_list:
            for op in ops_list:
                opid = _stable_op_id(op)
                key = (pid, opid)
                meta = s.op_meta.get(key)
                if meta is None:
                    meta = {"lb": 0, "ub": None, "est": None, "oom": 0, "fail": 0}

                if failed:
                    err = getattr(r, "error", None)
                    if _is_oom_error(err):
                        meta["oom"] = (meta.get("oom", 0) or 0) + 1
                        alloc = r_ram if r_ram and r_ram > 0 else (meta.get("est", 0) or 0) or _base_ram(pri)
                        lb = meta.get("lb", 0) or 0
                        if alloc > lb:
                            lb = alloc
                        meta["lb"] = lb
                        # pipeline-level floor helps seed subsequent ops
                        pf = s.pipeline_ram_floor.get(pid, 0) or 0
                        if lb > pf:
                            s.pipeline_ram_floor[pid] = lb

                        bumped = _oom_bump(lb, meta["oom"])
                        meta["est"] = bumped
                    else:
                        meta["fail"] = (meta.get("fail", 0) or 0) + 1
                else:
                    # Success: record upper bound
                    if r_ram and r_ram > 0:
                        ub = meta.get("ub", None)
                        if ub is None or r_ram < ub:
                            meta["ub"] = r_ram
                        # keep est from drifting above known ub
                        est = meta.get("est", None)
                        if est is not None and meta.get("ub", None) is not None and est > meta["ub"]:
                            meta["est"] = meta["ub"]

                s.op_meta[key] = meta

        # Give up if an operator is repeatedly failing (OOM or otherwise) at/near max RAM.
        # We can't see true required RAM; rely on bounded retries.
        if ops_list:
            op0 = ops_list[0]
            opid0 = _stable_op_id(op0)
            m0 = s.op_meta.get((pid, opid0), {})
            if (m0.get("oom", 0) or 0) > s.max_oom_retries:
                # We'll still requeue once or twice via cooldown; but if it's clearly hopeless, mark dead.
                # Use global-ish signal: pipeline floor too large for pool 0 max (or any pool max).
                max_pool_ram = 0
                for i in range(s.executor.num_pools):
                    mr = s.executor.pools[i].max_ram_pool
                    if mr > max_pool_ram:
                        max_pool_ram = mr
                pf = s.pipeline_ram_floor.get(pid, 0) or 0
                if max_pool_ram and pf >= int(max_pool_ram * 0.98):
                    _cleanup_pipeline(pid)
                    continue

            if (m0.get("fail", 0) or 0) > s.max_other_retries:
                _cleanup_pipeline(pid)
                continue

        # Re-enqueue pipeline for its next runnable op (or retry of FAILED op)
        _requeue(pid, pri, 0)

    # --- Fast exit if no ready pipelines ---
    if not s.in_q:
        return [], []

    # --- Snapshot pool availability and schedule new assignments ---
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    total_max_cpu = 0
    for i in range(s.executor.num_pools):
        total_max_cpu += s.executor.pools[i].max_cpu_pool

    max_new = int(total_max_cpu / 8) if total_max_cpu else 16
    if max_new < 16:
        max_new = 16
    if max_new > 64:
        max_new = 64

    assignments = []
    suspensions = []

    # Limit work even if queues are huge
    scan_limit = 12

    def _place_one(pid, pri, op):
        # Try pools in preferred order; pick first that fits both availability and caps.
        pool_order = _pool_order_for_priority(pri)
        stable_id = _stable_op_id(op)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            if local_cpu[pool_id] < 1 or local_ram[pool_id] < 1:
                continue

            cpu_tgt = _cpu_target(pool.max_cpu_pool, pri)
            cpu_min = _cpu_min(pri)
            avail_cpu = local_cpu[pool_id]

            cpu_req = cpu_tgt
            if cpu_req > avail_cpu:
                cpu_req = int(avail_cpu)
            if cpu_req < cpu_min:
                continue

            # Enforce class caps to preserve query headroom (non-preemptive)
            if pri != Priority.QUERY:
                cap = _caps_cpu(pool_id, pri, pool)
                running = s.running_cpu[pool_id].get(pri, 0)
                if running + cpu_req > cap:
                    # Try shrinking CPU to fit cap.
                    cpu_req = cap - running
                    if cpu_req > avail_cpu:
                        cpu_req = int(avail_cpu)
                    if cpu_req < cpu_min:
                        continue

            ram_req = _ram_estimate(pid, pri, op, pool.max_ram_pool)
            if ram_req < 1:
                ram_req = 1
            if ram_req > pool.max_ram_pool:
                ram_req = pool.max_ram_pool
            if ram_req > local_ram[pool_id]:
                continue

            # Placement accepted
            return pool_id, cpu_req, ram_req, stable_id

        return None

    placed_any = True
    while placed_any and len(assignments) < max_new:
        placed_any = False

        # Always try to drain a bit of QUERY first if possible.
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if len(assignments) >= max_new:
                break

            # Batch sprinkling: if we're placing non-query work, occasionally try a batch even if interactive exists.
            if pri == Priority.BATCH_PIPELINE:
                if s.non_query_picks % s.batch_every != 0:
                    continue

            pid = _pick_pid(pri, scan_limit)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue

            p = s.pipes.get(pid)
            if p is None:
                continue

            # Find a runnable operator
            try:
                status = p.runtime_status()
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                _requeue(pid, pri, _cooldown_on_block(pri))
                continue

            if not ops:
                _requeue(pid, pri, _cooldown_on_block(pri))
                continue

            op = ops[0]

            placed = _place_one(pid, pri, op)
            if placed is None:
                # Couldn't fit now; requeue with short cooldown.
                _requeue(pid, pri, _cooldown_on_no_fit(pri))
                continue

            pool_id, cpu_req, ram_req, stable_id = placed

            # Safety: only assign if still assignable right now (avoid stale operator transitions).
            try:
                fresh_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if op not in fresh_ops:
                    _requeue(pid, pri, _cooldown_on_block(pri))
                    continue
            except Exception:
                _requeue(pid, pri, _cooldown_on_block(pri))
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

            # Update local availability (per-tick accounting)
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            # Update running totals (for caps)
            try:
                s.running_cpu[pool_id][pri] = s.running_cpu[pool_id].get(pri, 0) + cpu_req
                s.running_ram[pool_id][pri] = s.running_ram[pool_id].get(pri, 0) + ram_req
            except Exception:
                pass

            # Map op -> pipeline for later result attribution
            s.op_inst_to_pid[id(op)] = pid
            s.op_stable_to_pid[stable_id] = pid

            # Don't immediately requeue this pipeline; wait for result to re-enqueue.
            placed_any = True

            if pri != Priority.QUERY:
                s.non_query_picks += 1

            # Continue outer loop (re-evaluate priorities after each placement)
            break

    return suspensions, assignments
