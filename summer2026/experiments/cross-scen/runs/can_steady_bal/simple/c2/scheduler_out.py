@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on completion rate + low latency.

    Key fixes/improvements vs previous version:
      - Correct operator identity: use id(op) to avoid op_id collisions across pipelines.
      - No "blacklist on first non-OOM": retry with resource bumps (RAM on OOM, CPU on timeout).
      - Much smaller, slot-like CPU sizing to increase parallelism (avoid serializing the pool).
      - RAM sizing tied to pool RAM/CPU ratio with safety factors + per-op learning.
      - Two-pass scheduling: strict reserves for higher priority, then relaxed backfill.
      - SRPT-ish within a priority: prefer pipelines with fewer remaining ops (lookahead window).
    """
    s.ticks = 0

    # Queues store pipeline_ids (not pipeline objects) to avoid stale references.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()
    s.pipeline_by_id = {}  # pipeline_id -> Pipeline
    s.pipeline_meta = {}   # pipeline_id -> {"enqueued_tick": int, "priority": Priority}

    # Per-operator learned hints keyed by operator object identity.
    s.op_ram_hint = {}      # op_uid -> ram
    s.op_cpu_hint = {}      # op_uid -> cpu
    s.op_fail_count = {}    # op_uid -> int
    s.op_last_fail = {}     # op_uid -> "oom"|"timeout"|"other"|None
    s.op_to_pipeline = {}   # op_uid -> pipeline_id

    # Per-pipeline failure tracking (to avoid infinite thrash on hopeless pipelines).
    s.pipeline_fail_count = {}  # pipeline_id -> int

    # Knobs
    s.lookahead = 30  # scan window per priority queue pick (SRPT-ish)
    s.max_sched_per_step = 10_000  # safety valve

    # Limit how many ops from the same pipeline we schedule in one scheduler call.
    s.max_ops_per_pipeline_step = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 2,
    }

    # Failure caps (after which we stop spending resources on a pipeline).
    s.max_pipeline_failures = {
        Priority.QUERY: 12,
        Priority.INTERACTIVE: 10,
        Priority.BATCH_PIPELINE: 7,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg or "deadline" in msg

    def _total_ops(p):
        vals = getattr(p, "values", None)
        if vals is None:
            return 1
        try:
            return max(1, len(vals))
        except Exception:
            return 1

    def _completed_ops_count(status):
        try:
            done = status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False)
            return len(done) if done is not None else 0
        except Exception:
            return 0

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_by_id.pop(pipeline_id, None)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _pipeline_priority(pid):
        meta = s.pipeline_meta.get(pid)
        if meta is None:
            return None
        return meta.get("priority", None)

    def _pipeline_age_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - int(meta.get("enqueued_tick", s.ticks)))

    def _has_any_enqueued(pri):
        q = _queue_for_priority(pri)
        return len(q) > 0

    def _base_cpu(pool, pri):
        # "Slot-like" sizing: small enough to enable parallelism, capped to avoid hogging.
        max_cpu = float(getattr(pool, "max_cpu_pool", 1) or 1)
        mc = int(max(1, max_cpu))

        if pri == Priority.QUERY:
            # 64->8, 128->16, 256->32, 512->32(cap)
            return max(8, min(32, mc // 8 if mc >= 8 else mc))
        if pri == Priority.INTERACTIVE:
            # 64->6, 128->12, 256->24(cap)
            return max(6, min(24, mc // 10 if mc >= 10 else max(3, mc // 2)))
        # BATCH
        # 64->4, 128->8, 256->16(cap)
        return max(4, min(16, mc // 16 if mc >= 16 else max(2, mc // 2)))

    def _base_ram(pool, pri, cpu_amt):
        # Tie RAM to pool's RAM/CPU ratio; add safety factor; clamp.
        max_ram = float(getattr(pool, "max_ram_pool", 1) or 1)
        max_cpu = float(getattr(pool, "max_cpu_pool", 1) or 1)
        mem_per_cpu = max_ram / max(1.0, max_cpu)

        if pri == Priority.QUERY:
            safety = 1.55
            min_ram = 64.0
            cap_ram = 224.0
        elif pri == Priority.INTERACTIVE:
            safety = 1.75
            min_ram = 72.0
            cap_ram = 224.0
        else:
            safety = 2.15
            min_ram = 80.0
            cap_ram = 224.0

        est = float(cpu_amt) * mem_per_cpu * safety
        ram = max(min_ram, est)
        ram = min(ram, cap_ram, max_ram)
        return ram

    def _reserve_for_pri(pool, pri):
        cpu_r = _base_cpu(pool, pri)
        ram_r = _base_ram(pool, pri, cpu_r)
        return cpu_r, ram_r

    def _resource_request(pool, pri, op_uid):
        # Minimums from defaults, then apply learned hints (which act as floors).
        base_cpu = _base_cpu(pool, pri)
        base_ram = _base_ram(pool, pri, base_cpu)

        cpu_hint = s.op_cpu_hint.get(op_uid, None)
        ram_hint = s.op_ram_hint.get(op_uid, None)

        cpu_req = float(base_cpu if cpu_hint is None else max(float(base_cpu), float(cpu_hint)))
        ram_req = float(base_ram if ram_hint is None else max(float(base_ram), float(ram_hint)))

        # Never exceed pool max.
        cpu_req = min(cpu_req, float(getattr(pool, "max_cpu_pool", cpu_req) or cpu_req))
        ram_req = min(ram_req, float(getattr(pool, "max_ram_pool", ram_req) or ram_req))

        # Enforce minimal positive allocations.
        if cpu_req < 1.0:
            cpu_req = 1.0
        if ram_req < 1.0:
            ram_req = 1.0

        return cpu_req, ram_req

    # --- Ingest/refresh pipelines into queues ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p

        try:
            if p.runtime_status().is_pipeline_successful():
                _drop_pipeline(pid)
                continue
        except Exception:
            pass

        if pid in s.enqueued:
            continue

        pri = p.priority
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "priority": pri}
        _queue_for_priority(pri).append(pid)

    # --- Process results: learn RAM/CPU needs and track failures ---
    for r in results:
        r_ops = getattr(r, "ops", None) or []
        for op in r_ops:
            op_uid = id(op)
            pid = s.op_to_pipeline.get(op_uid, None)
            if pid is None:
                continue

            pri = getattr(r, "priority", None) or _pipeline_priority(pid) or Priority.BATCH_PIPELINE

            if hasattr(r, "failed") and r.failed():
                s.op_fail_count[op_uid] = int(s.op_fail_count.get(op_uid, 0)) + 1
                s.pipeline_fail_count[pid] = int(s.pipeline_fail_count.get(pid, 0)) + 1

                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    s.op_last_fail[op_uid] = "oom"
                    prev = s.op_ram_hint.get(op_uid, None)
                    alloc = getattr(r, "ram", None)
                    base = None
                    try:
                        if alloc is not None and float(alloc) > 0:
                            base = float(alloc)
                    except Exception:
                        base = None
                    if base is None:
                        base = float(prev) if prev is not None else 1.0
                    # Aggressive RAM bump to converge quickly.
                    new_hint = max(base * 1.8, (float(prev) * 1.5 if prev is not None else 0.0), 1.0)
                    s.op_ram_hint[op_uid] = new_hint

                elif _is_timeout_error(err):
                    s.op_last_fail[op_uid] = "timeout"
                    prev = s.op_cpu_hint.get(op_uid, None)
                    alloc = getattr(r, "cpu", None)
                    base = None
                    try:
                        if alloc is not None and float(alloc) > 0:
                            base = float(alloc)
                    except Exception:
                        base = None
                    if base is None:
                        base = float(prev) if prev is not None else 1.0
                    # Bump CPU to reduce runtime and avoid repeated operator-level timeouts.
                    new_hint = max(base * 1.7, (float(prev) * 1.4 if prev is not None else 0.0), 1.0)
                    s.op_cpu_hint[op_uid] = new_hint

                else:
                    s.op_last_fail[op_uid] = "other"
                    # Mild bumps to escape transient under-provisioning.
                    prev_ram = s.op_ram_hint.get(op_uid, None)
                    prev_cpu = s.op_cpu_hint.get(op_uid, None)
                    if prev_ram is not None:
                        s.op_ram_hint[op_uid] = max(1.0, float(prev_ram) * 1.15)
                    if prev_cpu is not None:
                        s.op_cpu_hint[op_uid] = max(1.0, float(prev_cpu) * 1.15)

    suspensions = []
    assignments = []

    # Local view of availability we can update as we create assignments.
    pool_cpu = []
    pool_ram = []
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        pool_cpu.append(float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0))
        pool_ram.append(float(getattr(pool, "avail_ram_pool", 0.0) or 0.0))

    scheduled_count_by_pid = {}

    def _pipeline_failed_too_much(pid):
        pri = _pipeline_priority(pid)
        if pri is None:
            return False
        lim = int(s.max_pipeline_failures.get(pri, 7))
        return int(s.pipeline_fail_count.get(pid, 0)) >= lim

    def _best_pool_for_request(pri, op_uid, strict_reserves):
        # Returns (pool_id, cpu_req, ram_req) or (None, None, None)
        best = None  # (waste_score, pool_id, cpu_req, ram_req)
        has_query = _has_any_enqueued(Priority.QUERY)
        has_interactive = _has_any_enqueued(Priority.INTERACTIVE)

        for pool_id in range(s.executor.num_pools):
            if pool_cpu[pool_id] < 1.0 or pool_ram[pool_id] < 1.0:
                continue

            pool = s.executor.pools[pool_id]
            cpu_req, ram_req = _resource_request(pool, pri, op_uid)

            # Fit check (RAM must fit; CPU can be slightly downscaled for backfilling when safe).
            avail_c = pool_cpu[pool_id]
            avail_r = pool_ram[pool_id]

            # Honor hints as hard floors (don't downscale below learned minima).
            cpu_floor = float(s.op_cpu_hint.get(op_uid, 0.0) or 0.0)
            last_fail = s.op_last_fail.get(op_uid, None)

            # For non-timeout ops, allow CPU downscale to fill fragments (batch more permissive).
            min_cpu_by_pri = 4.0 if pri == Priority.QUERY else (3.0 if pri == Priority.INTERACTIVE else 2.0)
            if avail_c < min_cpu_by_pri:
                continue

            cpu_use = cpu_req
            if cpu_use > avail_c:
                # Only downscale if we are not violating a learned CPU floor and not recovering from timeout.
                if cpu_floor > 0.0 or last_fail == "timeout":
                    continue
                cpu_use = avail_c  # backfill
                if cpu_use < min_cpu_by_pri:
                    continue

            if ram_req > avail_r:
                continue

            if strict_reserves and pri != Priority.QUERY:
                # Soft reserves for higher priority: only enforce if enough headroom exists to matter.
                if has_query:
                    rq_c, rq_r = _reserve_for_pri(pool, Priority.QUERY)
                    if avail_c >= float(rq_c) and avail_r >= float(rq_r):
                        if (avail_c - cpu_use) < float(rq_c) or (avail_r - ram_req) < float(rq_r):
                            continue

                if pri == Priority.BATCH_PIPELINE and has_interactive:
                    ri_c, ri_r = _reserve_for_pri(pool, Priority.INTERACTIVE)
                    if avail_c >= float(ri_c) and avail_r >= float(ri_r):
                        if (avail_c - cpu_use) < float(ri_c) or (avail_r - ram_req) < float(ri_r):
                            continue

            # Best-fit primarily on RAM to reduce fragmentation; then CPU.
            waste = (avail_r - ram_req) * 10.0 + (avail_c - cpu_use)
            if best is None or waste < best[0]:
                best = (waste, pool_id, cpu_use, ram_req)

        if best is None:
            return None, None, None
        return best[1], best[2], best[3]

    def _pick_candidate_from_queue(pri, strict_reserves):
        q = _queue_for_priority(pri)
        if not q:
            return None

        best = None  # (score, pid, op, pool_id, cpu, ram)
        n = min(len(q), int(s.lookahead))
        for _ in range(n):
            pid = q.pop(0)
            q.append(pid)

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                continue

            if _pipeline_failed_too_much(pid):
                _drop_pipeline(pid)
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            try:
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass

            # Per-step per-pipeline cap
            cap = int(s.max_ops_per_pipeline_step.get(pri, 1))
            if int(scheduled_count_by_pid.get(pid, 0)) >= cap:
                continue

            try:
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = None
            if not ops:
                continue

            op = ops[0]
            op_uid = id(op)

            # Find a pool that can run it.
            pool_id, cpu_req, ram_req = _best_pool_for_request(pri, op_uid, strict_reserves)
            if pool_id is None:
                continue

            # SRPT-ish score within priority: fewer remaining ops first; older breaks ties.
            tot = _total_ops(p)
            done = _completed_ops_count(status)
            remaining = max(1, int(tot) - int(done))
            age = _pipeline_age_ticks(pid)
            score = remaining * 100000 - age

            if best is None or score < best[0]:
                best = (score, pid, op, pool_id, cpu_req, ram_req)

        return best

    def _make_assignment(pid, p, op, pool_id, cpu_req, ram_req):
        a = Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=pid,
        )
        assignments.append(a)
        op_uid = id(op)
        s.op_to_pipeline[op_uid] = pid
        scheduled_count_by_pid[pid] = int(scheduled_count_by_pid.get(pid, 0)) + 1
        pool_cpu[pool_id] -= float(cpu_req)
        pool_ram[pool_id] -= float(ram_req)

    # Two-pass fill: strict reserves first (protect QUERY/INTERACTIVE), then relax to backfill.
    def _schedule_pass(strict_reserves):
        made = 0
        guard = 0
        while guard < s.max_sched_per_step:
            guard += 1
            progressed = False

            # Always prioritize QUERY > INTERACTIVE > BATCH
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                cand = _pick_candidate_from_queue(pri, strict_reserves)
                if cand is None:
                    continue

                _, pid, op, pool_id, cpu_req, ram_req = cand
                p = s.pipeline_by_id.get(pid, None)
                if p is None:
                    continue

                # Double-check pipeline still not done.
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(pid)
                        continue
                except Exception:
                    pass

                if pool_cpu[pool_id] < 1.0 or pool_ram[pool_id] < 1.0:
                    continue
                if float(ram_req) > pool_ram[pool_id] or float(cpu_req) > pool_cpu[pool_id] + 1e-9:
                    # (CPU can go slightly negative due to float noise if we don't guard)
                    continue

                _make_assignment(pid, p, op, pool_id, cpu_req, ram_req)
                made += 1
                progressed = True
                break

            if not progressed:
                break
        return made

    _schedule_pass(strict_reserves=True)
    _schedule_pass(strict_reserves=False)

    return suspensions, assignments
