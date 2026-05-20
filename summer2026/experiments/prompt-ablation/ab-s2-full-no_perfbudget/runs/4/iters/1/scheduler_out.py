@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.wait_pos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.enqueued = set()
    s.dead_pipelines = set()

    # pipeline_id -> Pipeline (latest reference seen)
    s.pipeline_by_id = {}

    # pipeline_id -> metadata
    s.pipeline_meta = {}  # {"enqueued_tick": int, "priority": Priority}

    # op_key -> pipeline_id for attributing results
    s.op_to_pipeline = {}

    # (pipeline_id, op_key) -> profile
    # {
    #   "attempts": int,
    #   "oom_fail": int,
    #   "other_fail": int,
    #   "oom_lb": float,          # largest RAM allocation that OOM'd
    #   "next_ram": float|None,   # suggested next RAM after OOM
    # }
    s.op_profile = {}

    # Scheduling fairness (per pool) via deficit round robin
    # pool_id -> {Priority -> deficit}
    s.deficit = {}
    s.quantum = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 3.0,
    }
    s.deficit_cap = 80.0

    # Retry bounds
    s.max_oom_retries = 5
    s.max_other_retries = 2
    s.max_total_attempts = 7

    # Per-scheduler-call cap to avoid one pipeline hogging the whole tick
    s.per_call_pipeline_cap = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Mild query headroom (no preemption in this policy)
    s.last_query_seen_tick = -10**9
    s.query_headroom_window_ticks = 60

    # Queue maintenance
    s.compact_every = 200
    s.max_scan_per_pick = 200


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Use object identity to avoid cross-pipeline collisions.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_by_id.pop(pipeline_id, None)

    def _compact_queues():
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                s.wait_pos[pri] = 0
                continue
            new_q = []
            for pid in q:
                if pid is None:
                    continue
                if pid in s.dead_pipelines:
                    continue
                if pid not in s.enqueued:
                    continue
                new_q.append(pid)
            s.wait_q[pri] = new_q
            s.wait_pos[pri] = 0

    def _next_pid_slot(pri):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None, None
        # Skip None slots with bounded probing (at most n).
        for _ in range(n):
            pos = s.wait_pos[pri] % n
            s.wait_pos[pri] = pos + 1
            pid = q[pos]
            if pid is None:
                continue
            return pid, pos
        return None, None

    def _pool_query_headroom(pool):
        # Keep a small amount of CPU/RAM free to admit new QUERY work quickly.
        cpu = int(pool.max_cpu_pool * 0.01)
        if cpu < 1:
            cpu = 1
        if cpu > 8:
            cpu = 8

        ram = int(pool.max_ram_pool * 0.005)
        if ram < 4:
            ram = 4
        if ram > 32:
            ram = 32

        return float(cpu), float(ram)

    def _base_cpu(pool, pri):
        if pri == Priority.QUERY:
            cpu = int(pool.max_cpu_pool * 0.03)
            if cpu < 4:
                cpu = 4
            if cpu > 24:
                cpu = 24
            return float(cpu)
        if pri == Priority.INTERACTIVE:
            cpu = int(pool.max_cpu_pool * 0.02)
            if cpu < 3:
                cpu = 3
            if cpu > 16:
                cpu = 16
            return float(cpu)
        cpu = int(pool.max_cpu_pool * 0.015)
        if cpu < 2:
            cpu = 2
        if cpu > 8:
            cpu = 8
        return float(cpu)

    def _base_ram(pool, pri):
        # Small initial RAM to maximize concurrency; OOM feedback corrects upward quickly.
        if pri == Priority.QUERY:
            ram = int(pool.max_ram_pool * 0.01)
            if ram < 6:
                ram = 6
            if ram > 24:
                ram = 24
            return float(ram)
        if pri == Priority.INTERACTIVE:
            ram = int(pool.max_ram_pool * 0.008)
            if ram < 5:
                ram = 5
            if ram > 18:
                ram = 18
            return float(ram)
        ram = int(pool.max_ram_pool * 0.006)
        if ram < 4:
            ram = 4
        if ram > 12:
            ram = 12
        return float(ram)

    def _size_request(pool, pri, pid, op, local_cpu, local_ram):
        cpu = _base_cpu(pool, pri)
        ram = _base_ram(pool, pri)

        meta = s.pipeline_meta.get(pid)
        if meta is not None:
            age = s.ticks - meta.get("enqueued_tick", s.ticks)
            # If a pipeline has been waiting a while, slightly bias toward finishing it.
            if age >= 50:
                cpu = cpu * 1.25
            if age >= 120:
                cpu = cpu * 1.35
                ram = ram * 1.10

        opk = _op_key(op)
        prof = s.op_profile.get((pid, opk))
        if prof is not None:
            oom_lb = float(prof.get("oom_lb", 0.0) or 0.0)
            next_ram = prof.get("next_ram", None)
            if oom_lb > 0:
                ram = max(ram, oom_lb * 1.25)
            if next_ram is not None:
                ram = max(ram, float(next_ram))

        # Clamp to pool maxima
        if cpu > pool.max_cpu_pool:
            cpu = float(pool.max_cpu_pool)
        if ram > pool.max_ram_pool:
            ram = float(pool.max_ram_pool)

        # Clamp to currently available (but keep >= 1 if any available)
        if local_cpu < 1 or local_ram < 1:
            return None, None

        if cpu > local_cpu:
            cpu = float(local_cpu)
        if ram > local_ram:
            ram = float(local_ram)

        # Ensure at least 1 unit if possible
        if cpu < 1:
            cpu = 1.0
        if ram < 1:
            ram = 1.0

        # Convert to integers (executor expects numeric; int is safest)
        cpu_i = int(cpu)
        ram_i = int(ram)
        if cpu_i < 1:
            cpu_i = 1
        if ram_i < 1:
            ram_i = 1

        # Final clamp
        if cpu_i > int(local_cpu):
            cpu_i = int(local_cpu)
        if ram_i > int(local_ram):
            ram_i = int(local_ram)

        if cpu_i < 1 or ram_i < 1:
            return None, None
        return float(cpu_i), float(ram_i)

    # --- Ensure deficit state exists per pool ---
    for pool_id in range(s.executor.num_pools):
        if pool_id not in s.deficit:
            s.deficit[pool_id] = {
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            }

    # --- Ingest pipelines (latest refs) ---
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id in s.dead_pipelines:
            continue
        if p.pipeline_id not in s.enqueued:
            # Skip immediate enqueue if already completed (defensive)
            try:
                if p.runtime_status().is_pipeline_successful():
                    continue
            except Exception:
                pass

            s.enqueued.add(p.pipeline_id)
            s.pipeline_meta[p.pipeline_id] = {"enqueued_tick": s.ticks, "priority": p.priority}
            s.wait_q[p.priority].append(p.pipeline_id)
            if p.priority == Priority.QUERY:
                s.last_query_seen_tick = s.ticks

    # --- Process results (learn OOM, cap retries, avoid premature pipeline death) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            key = (pid, opk)
            prof = s.op_profile.get(key)
            if prof is None:
                prof = {
                    "attempts": 0,
                    "oom_fail": 0,
                    "other_fail": 0,
                    "oom_lb": 0.0,
                    "next_ram": None,
                }
                s.op_profile[key] = prof

            failed = False
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

            if failed:
                prof["attempts"] = int(prof.get("attempts", 0) or 0) + 1
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    prof["oom_fail"] = int(prof.get("oom_fail", 0) or 0) + 1
                    alloc_ram = getattr(r, "ram", None)
                    try:
                        alloc_ram = float(alloc_ram) if alloc_ram is not None else 0.0
                    except Exception:
                        alloc_ram = 0.0
                    if alloc_ram > float(prof.get("oom_lb", 0.0) or 0.0):
                        prof["oom_lb"] = alloc_ram

                    # Aggressive but bounded convergence
                    base = alloc_ram if alloc_ram > 0 else float(prof.get("oom_lb", 0.0) or 1.0)
                    next_ram = max(base * 2.0, float(prof.get("oom_lb", 0.0) or 0.0) * 1.8, 1.0)
                    # Small cushion to reduce repeated OOM due to granularity
                    next_ram = next_ram * 1.05 + 1.0
                    prof["next_ram"] = next_ram
                else:
                    prof["other_fail"] = int(prof.get("other_fail", 0) or 0) + 1
            else:
                # Success: clear "next_ram" so we don't keep over-allocating on the same op
                prof["next_ram"] = None

            # If this op looks hopeless, stop spending resources on this pipeline
            attempts = int(prof.get("attempts", 0) or 0)
            oom_fail = int(prof.get("oom_fail", 0) or 0)
            other_fail = int(prof.get("other_fail", 0) or 0)
            if (oom_fail >= s.max_oom_retries) or (other_fail >= s.max_other_retries) or (attempts >= s.max_total_attempts):
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)

    # Periodic cleanup to keep scans fast
    if s.ticks % s.compact_every == 0:
        _compact_queues()

    # --- Add DRR quantum per call (cap to avoid bursts) ---
    for pool_id in range(s.executor.num_pools):
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            d = float(s.deficit[pool_id].get(pri, 0.0) or 0.0) + float(s.quantum[pri])
            if d > s.deficit_cap:
                d = s.deficit_cap
            s.deficit[pool_id][pri] = d

    # --- Main scheduling ---
    suspensions = []
    assignments = []

    per_call_counts = {}  # pid -> ops scheduled in this scheduler call

    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    protect_queries = (s.ticks - s.last_query_seen_tick) <= int(s.query_headroom_window_ticks)

    def _pick_from_priority(pri, pool_id):
        pool = s.executor.pools[pool_id]
        scans = 0
        q = s.wait_q[pri]
        if not q:
            return None

        while scans < s.max_scan_per_pick:
            scans += 1
            pid, pos = _next_pid_slot(pri)
            if pid is None:
                return None

            if pid in s.dead_pipelines or pid not in s.enqueued:
                # Lazily clear slot
                try:
                    s.wait_q[pri][pos] = None
                except Exception:
                    pass
                continue

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                continue

            # If pipeline finished, drop it
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    try:
                        s.wait_q[pri][pos] = None
                    except Exception:
                        pass
                    continue
            except Exception:
                status = None

            cap = int(s.per_call_pipeline_cap.get(pri, 1))
            if int(per_call_counts.get(pid, 0)) >= cap:
                continue

            if status is None:
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            # Prefer an op that seems easiest to fit (smaller observed OOM lower bound).
            best_op = ops[0]
            best_lb = None
            for cand in ops[:3]:
                candk = _op_key(cand)
                prof = s.op_profile.get((pid, candk))
                lb = float(prof.get("oom_lb", 0.0) or 0.0) if prof is not None else 0.0
                if best_lb is None or lb < best_lb:
                    best_lb = lb
                    best_op = cand

            cpu_req, ram_req = _size_request(
                pool=pool,
                pri=pri,
                pid=pid,
                op=best_op,
                local_cpu=local_cpu[pool_id],
                local_ram=local_ram[pool_id],
            )
            if cpu_req is None or ram_req is None:
                continue

            # Enforce mild QUERY headroom against BATCH saturation (no preemption available)
            if pri == Priority.BATCH_PIPELINE and protect_queries:
                hcpu, hram = _pool_query_headroom(pool)
                if (local_cpu[pool_id] - cpu_req) < hcpu or (local_ram[pool_id] - ram_req) < hram:
                    continue

            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                continue

            # Respect any known OOM lower bound strictly
            opk = _op_key(best_op)
            prof = s.op_profile.get((pid, opk))
            if prof is not None:
                oom_lb = float(prof.get("oom_lb", 0.0) or 0.0)
                if oom_lb > 0.0 and ram_req < oom_lb:
                    continue

            return pid, best_op, cpu_req, ram_req

        return None

    def _priority_rank(pri):
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        blocked = set()

        while local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0:
            # Order candidates by deficit desc, tie-break by priority rank
            cands = []
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if pri in blocked:
                    continue
                cands.append(pri)

            if not cands:
                break

            # Sort without imports
            for i in range(len(cands)):
                for j in range(i + 1, len(cands)):
                    pi = cands[i]
                    pj = cands[j]
                    di = float(s.deficit[pool_id].get(pi, 0.0) or 0.0)
                    dj = float(s.deficit[pool_id].get(pj, 0.0) or 0.0)
                    if (dj > di) or (dj == di and _priority_rank(pj) > _priority_rank(pi)):
                        cands[i], cands[j] = cands[j], cands[i]

            placed = False
            for pri in cands:
                picked = _pick_from_priority(pri, pool_id)
                if picked is None:
                    blocked.add(pri)
                    continue

                pid, op, cpu_req, ram_req = picked

                # Final safety against over-allocation
                if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                    blocked.add(pri)
                    continue

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

                # Attribute future results
                s.op_to_pipeline[_op_key(op)] = pid

                # Update local pool availability
                local_cpu[pool_id] -= cpu_req
                local_ram[pool_id] -= ram_req

                # Update DRR deficit (charge CPU as the "cost")
                s.deficit[pool_id][pri] = float(s.deficit[pool_id].get(pri, 0.0) or 0.0) - float(cpu_req)

                # Per-call pipeline cap accounting
                per_call_counts[pid] = int(per_call_counts.get(pid, 0)) + 1

                # Unblock after successful placement
                blocked.clear()
                placed = True
                break

            if not placed:
                break

    return suspensions, assignments
