@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Throughput-first, completion-first priority scheduler.

    Key changes vs prior version:
      - Stop massively over-allocating CPU/RAM per op (which caused low concurrency and interactive timeouts).
      - Weighted fair sharing (QUERY and INTERACTIVE both get regular service; BATCH gets background service).
      - Small-pipeline-first within each priority (bucketed by operator count) to maximize completion rate.
      - OOM-/timeout-aware retries with per-op RAM/CPU hints.
      - Light anti-starvation: cap query burst when interactive backlog exists.
    """
    s.ticks = 0

    # Buckets per priority by pipeline length (1..8, 9==9+ / unknown)
    # wait_q[priority][bucket] -> List[Pipeline]
    s.wait_q = {
        Priority.QUERY: [[] for _ in range(10)],
        Priority.INTERACTIVE: [[] for _ in range(10)],
        Priority.BATCH_PIPELINE: [[] for _ in range(10)],
    }

    # Pipeline bookkeeping
    s.enqueued = set()          # pipeline_ids currently tracked in queues
    s.done_pipelines = set()    # pipeline_ids completed successfully (avoid re-enqueue)
    s.abandoned = set()         # pipeline_ids abandoned after too many failures
    s.pipeline_meta = {}        # pipeline_id -> {"enqueued_tick": int}
    s.pipeline_len = {}         # pipeline_id -> int (estimated operator count)

    # Per-op adaptive hints (keyed by (pipeline_id, op_key))
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_fail_count = {}

    # Map op_key -> pipeline_id (for attributing results)
    s.op_to_pipeline = {}

    # Weighted fairness across priorities
    # (Interactive is boosted to avoid catastrophic timeout penalties.)
    s.pri_deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.pri_weight = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 9.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.deficit_cap = 200.0

    # Scheduling knobs
    s.scan_limit = 40                 # how many pipelines to scan per bucket before giving up
    s.max_query_burst = 3             # if interactive backlog exists, don't schedule more than this many queries in a row per pool
    s.max_failures_per_op = 12        # abandon pipeline after too many retries for the same op (prevents infinite churn)

    # Aging knobs (help old pipelines finish)
    s.age_cpu_bump_ticks_1 = 25
    s.age_cpu_bump_ticks_2 = 60


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

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("time limit" in msg) or ("timelimit" in msg) or ("deadline" in msg)

    def _pipeline_len_est(p):
        vals = getattr(p, "values", None)
        if vals is None:
            return 1
        try:
            n = len(vals)
        except Exception:
            n = 1
        if n is None or n <= 0:
            return 1
        return int(n)

    def _bucket_for_len(n):
        if n <= 0:
            return 1
        if n <= 8:
            return n
        return 9

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_len.pop(pid, None)

    def _wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _has_backlog(pri):
        qs = s.wait_q.get(pri)
        if not qs:
            return False
        for b in qs[1:]:
            if b:
                return True
        return False

    def _backlog_size(pri):
        qs = s.wait_q.get(pri)
        if not qs:
            return 0
        total = 0
        for b in qs[1:]:
            total += len(b)
        return total

    def _base_cpu(pool, pri):
        # Throughput-first: keep per-op CPU modest to allow concurrency.
        # Use slightly lower CPU when backlog is high to improve completion rate.
        backlog = _backlog_size(pri)
        if pri == Priority.BATCH_PIPELINE:
            frac = 0.05 if backlog > 30 else 0.06
            floor = 2.0
            cap = 6.0
        else:
            frac = 0.06 if backlog > 60 else 0.08
            floor = 3.0
            cap = 8.0

        cpu = pool.max_cpu_pool * frac
        if cpu < floor:
            cpu = floor
        if cpu > cap:
            cpu = cap
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        if cpu < 1.0:
            cpu = 1.0
        return cpu

    def _base_ram(pool, pri):
        # Moderate baseline RAM to avoid OOM churn; still far below "fraction of pool" allocations.
        # Cap to avoid huge allocations on large-memory pools.
        if pri == Priority.BATCH_PIPELINE:
            frac = 0.02
            floor = 10.0
            cap = 80.0
        else:
            frac = 0.025
            floor = 12.0
            cap = 96.0

        ram = pool.max_ram_pool * frac
        if ram < floor:
            ram = floor
        if ram > cap:
            ram = cap
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        if ram < 1.0:
            ram = 1.0
        return ram

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)
        key = (pid, opk)

        cpu = s.op_cpu_hint.get(key, None)
        if cpu is None:
            cpu = _base_cpu(pool, pri)
        else:
            # Keep hints within pool bounds
            if cpu > pool.max_cpu_pool:
                cpu = pool.max_cpu_pool
            if cpu < 1.0:
                cpu = 1.0

        ram = s.op_ram_hint.get(key, None)
        if ram is None:
            ram = _base_ram(pool, pri)
        else:
            if ram > pool.max_ram_pool:
                ram = pool.max_ram_pool
            if ram < 1.0:
                ram = 1.0

        # Age-based CPU bump (help old pipelines finish before global timeout penalties)
        wt = _wait_ticks(pid)
        if wt >= s.age_cpu_bump_ticks_2:
            cpu = min(pool.max_cpu_pool, max(cpu, _base_cpu(pool, pri) * 2.5))
        elif wt >= s.age_cpu_bump_ticks_1:
            cpu = min(pool.max_cpu_pool, max(cpu, _base_cpu(pool, pri) * 1.7))

        # Enforce minimums
        if cpu < 1.0:
            cpu = 1.0
        if ram < 1.0:
            ram = 1.0

        # Clip to pool max (availability clipping is handled at placement time)
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        return cpu, ram, opk, key

    def _queue_push(p):
        pid = p.pipeline_id
        if pid in s.done_pipelines or pid in s.abandoned:
            return
        if pid in s.enqueued:
            return

        # Avoid enqueuing already-successful pipelines
        try:
            if p.runtime_status().is_pipeline_successful():
                s.done_pipelines.add(pid)
                _drop_pipeline(pid)
                return
        except Exception:
            pass

        n = _pipeline_len_est(p)
        b = _bucket_for_len(n)
        s.pipeline_len[pid] = n
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.wait_q[p.priority][b].append(p)
        s.enqueued.add(pid)

    def _pick_from_priority(pri, pool, avail_cpu, avail_ram, scheduled_pipeline_ids):
        # Small pipelines first (bucket order), with round-robin inside each bucket.
        for bucket in range(1, 10):
            q = s.wait_q[pri][bucket]
            if not q:
                continue

            scans = min(len(q), s.scan_limit)
            for _ in range(scans):
                p = q.pop(0)
                pid = p.pipeline_id

                if pid in s.done_pipelines or pid in s.abandoned:
                    continue

                if pid in scheduled_pipeline_ids:
                    q.append(p)
                    continue

                status = p.runtime_status()

                if status.is_pipeline_successful():
                    s.done_pipelines.add(pid)
                    _drop_pipeline(pid)
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    q.append(p)
                    continue

                op = op_list[0]
                cpu_req, ram_req, opk, key = _size_request(pool, pri, pid, op)

                # If we have explicit RAM hint and it doesn't fit now, skip (avoid guaranteed OOM).
                hint_ram = s.op_ram_hint.get(key, None)
                if hint_ram is not None and hint_ram > avail_ram:
                    q.append(p)
                    continue

                # Placeable sizing: allow shrinking to current availability (but keep >= 1)
                cpu_use = cpu_req if cpu_req <= avail_cpu else avail_cpu
                ram_use = ram_req if ram_req <= avail_ram else avail_ram

                if cpu_use < 1.0 or ram_use < 1.0:
                    q.append(p)
                    continue

                # Also keep a small RAM floor to reduce OOM risk under pressure
                min_ram_floor = 4.0
                if ram_use < min_ram_floor:
                    q.append(p)
                    continue

                # Keep pipeline in queue for subsequent ops
                q.append(p)
                return p, op, cpu_use, ram_use, opk

        return None, None, None, None, None

    # --- Ingest new pipelines ---
    for p in pipelines:
        _queue_push(p)

    # --- Update fairness deficits ---
    for pri, w in s.pri_weight.items():
        s.pri_deficit[pri] = min(s.deficit_cap, s.pri_deficit.get(pri, 0.0) + w)

    # --- Process results: update hints on failure/success ---
    for r in results:
        pool = None
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            key = (pid, opk)

            if hasattr(r, "failed") and r.failed():
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                # Prevent infinite churn on pathological ops
                if s.op_fail_count[key] >= s.max_failures_per_op:
                    s.abandoned.add(pid)
                    _drop_pipeline(pid)
                    continue

                err = getattr(r, "error", None)
                alloc_ram = getattr(r, "ram", None)
                alloc_cpu = getattr(r, "cpu", None)

                if _is_oom_error(err):
                    prev = s.op_ram_hint.get(key, None)
                    base = alloc_ram if (alloc_ram is not None and alloc_ram > 0) else (prev if prev is not None else 1.0)
                    new_hint = base * 2.0
                    if prev is not None:
                        new_hint = max(new_hint, prev * 2.0)
                    if pool is not None:
                        if new_hint > pool.max_ram_pool:
                            new_hint = pool.max_ram_pool
                    if new_hint < 1.0:
                        new_hint = 1.0
                    s.op_ram_hint[key] = new_hint

                elif _is_timeout_error(err):
                    prev = s.op_cpu_hint.get(key, None)
                    base = alloc_cpu if (alloc_cpu is not None and alloc_cpu > 0) else (prev if prev is not None else 1.0)
                    new_hint = base * 1.6
                    if prev is not None:
                        new_hint = max(new_hint, prev * 1.3)
                    if pool is not None:
                        if new_hint > pool.max_cpu_pool:
                            new_hint = pool.max_cpu_pool
                    if new_hint < 1.0:
                        new_hint = 1.0
                    s.op_cpu_hint[key] = new_hint

                else:
                    # Generic failure: gentle bump to improve chance of completion without exploding allocations
                    if pool is not None:
                        prev_r = s.op_ram_hint.get(key, None)
                        prev_c = s.op_cpu_hint.get(key, None)

                        base_r = alloc_ram if (alloc_ram is not None and alloc_ram > 0) else (prev_r if prev_r is not None else _base_ram(pool, Priority.QUERY))
                        base_c = alloc_cpu if (alloc_cpu is not None and alloc_cpu > 0) else (prev_c if prev_c is not None else _base_cpu(pool, Priority.QUERY))

                        new_r = min(pool.max_ram_pool, max(base_r * 1.25, (prev_r * 1.15 if prev_r is not None else 0.0), base_r + 2.0))
                        new_c = min(pool.max_cpu_pool, max(base_c * 1.25, (prev_c * 1.15 if prev_c is not None else 0.0), base_c + 1.0))

                        if new_r < 1.0:
                            new_r = 1.0
                        if new_c < 1.0:
                            new_c = 1.0

                        s.op_ram_hint[key] = new_r
                        s.op_cpu_hint[key] = new_c
            else:
                # Success: if we had hints, gently decay them toward the successful allocation to reduce waste.
                alloc_ram = getattr(r, "ram", None)
                alloc_cpu = getattr(r, "cpu", None)

                if alloc_ram is not None and alloc_ram > 0 and key in s.op_ram_hint:
                    prev = s.op_ram_hint.get(key, alloc_ram)
                    target = alloc_ram * 1.10
                    if target < alloc_ram + 0.5:
                        target = alloc_ram + 0.5
                    s.op_ram_hint[key] = min(prev, target)

                if alloc_cpu is not None and alloc_cpu > 0 and key in s.op_cpu_hint:
                    prev = s.op_cpu_hint.get(key, alloc_cpu)
                    target = alloc_cpu * 1.10
                    if target < alloc_cpu + 0.2:
                        target = alloc_cpu + 0.2
                    s.op_cpu_hint[key] = min(prev, target)

    suspensions = []
    assignments = []

    scheduled_pipeline_ids = set()

    # --- Main placement loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        consecutive_queries = 0

        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            has_q = _has_backlog(Priority.QUERY)
            has_i = _has_backlog(Priority.INTERACTIVE)
            has_b = _has_backlog(Priority.BATCH_PIPELINE)

            if not (has_q or has_i or has_b):
                break

            # Anti-starvation: if interactive backlog exists, cap query streaks.
            pri_try = []
            if has_i and consecutive_queries >= s.max_query_burst:
                pri_try = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]
            else:
                # Deficit-based ordering among priorities with backlog
                pri_try = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
                pri_try.sort(key=lambda pr: s.pri_deficit.get(pr, 0.0), reverse=True)

            picked_any = False
            for pri in pri_try:
                if pri == Priority.QUERY and not has_q:
                    continue
                if pri == Priority.INTERACTIVE and not has_i:
                    continue
                if pri == Priority.BATCH_PIPELINE and not has_b:
                    continue

                p, op, cpu_use, ram_use, opk = _pick_from_priority(pri, pool, avail_cpu, avail_ram, scheduled_pipeline_ids)
                if p is None:
                    continue

                a = Assignment(
                    ops=[op],
                    cpu=cpu_use,
                    ram=ram_use,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(a)

                s.op_to_pipeline[opk] = p.pipeline_id

                avail_cpu -= cpu_use
                avail_ram -= ram_use
                scheduled_pipeline_ids.add(p.pipeline_id)

                s.pri_deficit[pri] = s.pri_deficit.get(pri, 0.0) - 1.0
                if s.pri_deficit[pri] < -s.deficit_cap:
                    s.pri_deficit[pri] = -s.deficit_cap

                if pri == Priority.QUERY:
                    consecutive_queries += 1
                else:
                    consecutive_queries = 0

                picked_any = True
                break

            if not picked_any:
                break

    return suspensions, assignments
