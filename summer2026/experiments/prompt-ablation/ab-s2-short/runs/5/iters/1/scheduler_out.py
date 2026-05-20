@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Queues store pipeline_ids (not pipeline objects) to avoid stale references.
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

    # All known pipelines by id (latest object reference).
    s.all_pipelines = {}

    # Enqueued/active pipeline ids.
    s.enqueued = set()

    # Pipeline metadata for aging/fairness.
    # { pipeline_id: {"enqueued_tick": int} }
    s.pipeline_meta = {}

    # Per-operator resource learning (keys are (pipeline_id, op_key)).
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_ram_success = {}
    s.op_cpu_success = {}
    s.op_fail_count = {}

    # Map op_key -> pipeline_id for attributing results.
    s.op_to_pipeline = {}

    # Pipelines deemed impossible to run on any pool (e.g., needs > pool max RAM).
    s.impossible_pipelines = set()

    # Scheduling knobs
    s.compact_every = 200
    s.batch_starve_ticks = 400

    # Per-pool cursors and bookkeeping
    s.pool_cursor = {}
    s.last_query_scheduled_tick = {}


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "key", None)
            or getattr(op, "name", None)
            or id(op)
        )

    def _norm_ram(val, pool):
        if val is None:
            return None
        try:
            v = float(val)
        except Exception:
            return None
        if v <= 0:
            return None
        # Heuristic unit normalization: if wildly larger than pool RAM, treat as MB -> GB.
        if pool is not None and hasattr(pool, "max_ram_pool"):
            mx = float(pool.max_ram_pool)
            if mx > 0 and v > mx * 4.0:
                v = v / 1024.0
        return v

    def _norm_cpu(val, pool):
        if val is None:
            return None
        try:
            v = float(val)
        except Exception:
            return None
        if v <= 0:
            return None
        return v

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg

    def _declared_min_ram(op, pool):
        # Try common attribute names that might exist in the simulator operator objects.
        candidates = [
            "min_ram",
            "ram_min",
            "min_memory",
            "memory_min",
            "min_mem",
            "mem_min",
            "required_ram",
            "required_memory",
            "ram_gb",
            "memory_gb",
            "ram",
            "memory",
        ]
        for a in candidates:
            if hasattr(op, a):
                v = _norm_ram(getattr(op, a), pool)
                if v is not None:
                    return v
        return None

    def _base_cpu(pri, pool):
        mx = float(pool.max_cpu_pool)
        if pri == Priority.QUERY:
            return float(max(2, int(round(mx * 0.12))))  # ~8 on 64 vCPU
        if pri == Priority.INTERACTIVE:
            return float(max(1, int(round(mx * 0.06))))  # ~4 on 64 vCPU
        return float(max(1, int(round(mx * 0.03))))      # ~2 on 64 vCPU

    def _base_ram(pri, pool):
        mx = float(pool.max_ram_pool)
        if pri == Priority.QUERY:
            return float(max(6.0, mx * 0.020))   # ~10GB on 500GB
        if pri == Priority.INTERACTIVE:
            return float(max(5.0, mx * 0.018))   # ~9GB on 500GB
        return float(max(4.0, mx * 0.015))       # ~7.5GB on 500GB

    def _size_request(pool, pri, pipeline_id, op):
        opk = _op_key(op)

        # RAM: prefer the smallest known safe allocation (success), else declared minimum, else base;
        # if we OOM'd before, honor the hint.
        declared = _declared_min_ram(op, pool)
        base_r = _base_ram(pri, pool)
        hint_r = s.op_ram_hint.get((pipeline_id, opk))
        succ_r = s.op_ram_success.get((pipeline_id, opk))

        ram_req = base_r
        if declared is not None:
            ram_req = max(ram_req, declared * 1.05)
        if succ_r is not None:
            ram_req = max(ram_req, float(succ_r))
        if hint_r is not None:
            ram_req = max(ram_req, float(hint_r))

        # CPU: small by default to maximize concurrency; bump on repeated timeouts.
        base_c = _base_cpu(pri, pool)
        hint_c = s.op_cpu_hint.get((pipeline_id, opk))
        succ_c = s.op_cpu_success.get((pipeline_id, opk))

        cpu_req = base_c
        if succ_c is not None:
            cpu_req = max(cpu_req, float(succ_c))
        if hint_c is not None:
            cpu_req = max(cpu_req, float(hint_c))

        # Clip to pool capacity and enforce minimums.
        cpu_req = max(1.0, min(cpu_req, float(pool.max_cpu_pool)))
        ram_req = max(1.0, min(ram_req, float(pool.max_ram_pool)))

        return cpu_req, ram_req

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.all_pipelines.pop(pipeline_id, None)

    def _pipeline_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - int(meta.get("enqueued_tick", s.ticks)))

    def _choose_pattern():
        # Dynamic weighted round-robin pattern to avoid QUERY monopolization while ensuring progress.
        qn = len(s.wait_q[Priority.QUERY])
        in_ = len(s.wait_q[Priority.INTERACTIVE])
        bn = len(s.wait_q[Priority.BATCH_PIPELINE])

        # Base: Q2 / I5 / B5
        base = [
            Priority.QUERY,
            Priority.INTERACTIVE,
            Priority.BATCH_PIPELINE,
            Priority.INTERACTIVE,
            Priority.BATCH_PIPELINE,
            Priority.INTERACTIVE,
            Priority.BATCH_PIPELINE,
            Priority.INTERACTIVE,
            Priority.BATCH_PIPELINE,
            Priority.INTERACTIVE,
            Priority.QUERY,
            Priority.BATCH_PIPELINE,
        ]

        if qn == 0 and in_ == 0 and bn == 0:
            return [Priority.BATCH_PIPELINE]

        # If batch backlog dwarfs interactive, skew slightly more toward batch.
        if bn > max(10, in_ * 2):
            return [
                Priority.QUERY,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.QUERY,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
            ]  # Q2 / I4 / B6

        # If interactive backlog dwarfs batch, skew toward interactive.
        if in_ > max(10, bn * 2):
            return [
                Priority.QUERY,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.INTERACTIVE,
                Priority.BATCH_PIPELINE,
                Priority.INTERACTIVE,
                Priority.QUERY,
                Priority.INTERACTIVE,
                Priority.INTERACTIVE,
            ]  # Q2 / I8 / B2

        return base

    # --- Ingest pipelines (store latest object reference; enqueue only once) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.all_pipelines[pid] = p

        if pid in s.impossible_pipelines:
            continue

        # Don't enqueue pipelines that already finished.
        try:
            if p.runtime_status().is_pipeline_successful():
                _drop_pipeline(pid)
                continue
        except Exception:
            pass

        if pid in s.enqueued:
            continue

        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        if p.priority == Priority.QUERY:
            s.wait_q[Priority.QUERY].append(pid)
        elif p.priority == Priority.INTERACTIVE:
            s.wait_q[Priority.INTERACTIVE].append(pid)
        else:
            s.wait_q[Priority.BATCH_PIPELINE].append(pid)

    # --- Process results (learn RAM/CPU; detect impossible RAM requirements) ---
    for r in results:
        pid_from_result = getattr(r, "pipeline_id", None)
        pool = None
        try:
            pool = s.executor.pools[int(getattr(r, "pool_id", 0))]
        except Exception:
            pool = None

        ops_in_result = getattr(r, "ops", []) or []
        if not ops_in_result and pid_from_result is not None:
            ops_in_result = [None]

        for op in ops_in_result:
            opk = _op_key(op) if op is not None else None

            pid = pid_from_result
            if pid is None and opk is not None:
                pid = s.op_to_pipeline.get(opk)

            if pid is None:
                continue
            if pid in s.impossible_pipelines:
                continue

            failed = False
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

            alloc_ram = _norm_ram(getattr(r, "ram", None), pool)
            alloc_cpu = _norm_cpu(getattr(r, "cpu", None), pool)
            err = getattr(r, "error", None)

            if not failed:
                if opk is not None:
                    if alloc_ram is not None:
                        prev = s.op_ram_success.get((pid, opk))
                        s.op_ram_success[(pid, opk)] = alloc_ram if prev is None else min(float(prev), alloc_ram)
                    if alloc_cpu is not None:
                        prev = s.op_cpu_success.get((pid, opk))
                        s.op_cpu_success[(pid, opk)] = alloc_cpu if prev is None else min(float(prev), alloc_cpu)
                    s.op_fail_count[(pid, opk)] = 0
                continue

            # Failed: learn and retry, unless it's impossible.
            if opk is None:
                continue

            s.op_fail_count[(pid, opk)] = int(s.op_fail_count.get((pid, opk), 0)) + 1

            if _is_oom_error(err):
                # Aggressively bump RAM to converge quickly.
                prev = s.op_ram_hint.get((pid, opk))
                base = alloc_ram if alloc_ram is not None else (float(prev) if prev is not None else 1.0)
                new_hint = max(base * 1.8, (float(prev) * 2.0 if prev is not None else 0.0), 1.0)

                if pool is not None:
                    mx = float(pool.max_ram_pool)
                    new_hint = min(new_hint, mx)

                    # If we were already at (near) max and still OOM, mark pipeline impossible.
                    if alloc_ram is not None and alloc_ram >= mx * 0.98 and new_hint >= mx * 0.98:
                        s.impossible_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

                s.op_ram_hint[(pid, opk)] = new_hint

            elif _is_timeout_error(err):
                # Bump CPU a bit on timeout-like errors.
                prev = s.op_cpu_hint.get((pid, opk))
                base = alloc_cpu if alloc_cpu is not None else (float(prev) if prev is not None else 1.0)
                new_hint = max(base * 1.5, (float(prev) * 1.6 if prev is not None else 0.0), 1.0)
                if pool is not None:
                    new_hint = min(new_hint, float(pool.max_cpu_pool))
                s.op_cpu_hint[(pid, opk)] = new_hint

            else:
                # Unknown failure: don't instantly kill; but if it repeats many times, give up.
                if s.op_fail_count[(pid, opk)] >= 12:
                    s.impossible_pipelines.add(pid)
                    _drop_pipeline(pid)

    # --- Periodic compaction to keep queue scans cheap ---
    if s.ticks % int(s.compact_every) == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            new_q = []
            for pid in s.wait_q[pri]:
                if pid not in s.enqueued:
                    continue
                if pid in s.impossible_pipelines:
                    continue
                p = s.all_pipelines.get(pid)
                if p is None:
                    s.enqueued.discard(pid)
                    continue
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(pid)
                        continue
                except Exception:
                    pass
                new_q.append(pid)
            s.wait_q[pri] = new_q
            s.q_cursor[pri] = 0

    suspensions = []
    assignments = []

    # To avoid double-assigning the same pipeline before runtime state updates.
    scheduled_this_step = set()

    pattern = _choose_pattern()

    def _pick_next(pri, pool, avail_cpu, avail_ram):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None, None, None, None

        start = int(s.q_cursor.get(pri, 0)) % n
        for offset in range(n):
            idx = (start + offset) % n
            pid = q[idx]

            if pid in scheduled_this_step:
                continue
            if pid not in s.enqueued or pid in s.impossible_pipelines:
                continue

            p = s.all_pipelines.get(pid)
            if p is None:
                s.enqueued.discard(pid)
                continue

            status = None
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                status = None

            # Simple aging: if batch is very old, allow it to be picked even in interactive slots.
            if pri == Priority.INTERACTIVE:
                if s.wait_q[Priority.INTERACTIVE]:
                    pass
                # no-op; aging handled in main loop by explicit batch check below

            # Get one runnable op (require parents complete).
            try:
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) if status is not None else []
            except Exception:
                op_list = []
            if not op_list:
                continue

            op = op_list[0]
            cpu_req, ram_req = _size_request(pool, p.priority, pid, op)

            # Must fit current pool availability.
            if cpu_req > float(avail_cpu) or ram_req > float(avail_ram):
                continue

            # Advance cursor past this inspected element for fairness next time.
            s.q_cursor[pri] = idx + 1
            return p, op, cpu_req, ram_req

        # If we scanned all, still advance cursor a bit.
        s.q_cursor[pri] = start + 1
        return None, None, None, None

    def _oldest_batch_wait():
        oldest = 0
        for pid in s.wait_q[Priority.BATCH_PIPELINE]:
            if pid not in s.enqueued or pid in s.impossible_pipelines:
                continue
            wt = _pipeline_wait_ticks(pid)
            if wt > oldest:
                oldest = wt
        return oldest

    # --- Main scheduling: pack tightly with small per-op sizing; avoid QUERY monopolization ---
    for pool_id in range(int(s.executor.num_pools)):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if pool_id not in s.pool_cursor:
            s.pool_cursor[pool_id] = 0
        if pool_id not in s.last_query_scheduled_tick:
            s.last_query_scheduled_tick[pool_id] = -10**9

        # If a batch has been waiting very long, temporarily increase batch attempts.
        batch_starved = _oldest_batch_wait() >= int(s.batch_starve_ticks)

        # Fill the pool with many small containers for high throughput.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            tried = []
            chosen_pri = None

            # Force a QUERY attempt occasionally if any QUERY is waiting.
            if s.wait_q[Priority.QUERY] and (s.ticks - int(s.last_query_scheduled_tick.get(pool_id, 0)) >= 1):
                chosen_pri = Priority.QUERY
                tried = [Priority.QUERY]
            else:
                # Weighted RR by pattern.
                cur = int(s.pool_cursor[pool_id]) % len(pattern)
                chosen_pri = pattern[cur]
                s.pool_cursor[pool_id] = cur + 1
                tried = [chosen_pri]

            # Build fallback priority order (unique, preserves earlier choices).
            fallbacks = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            if batch_starved:
                # If batch is starved, bias it ahead of interactive (but never ahead of query).
                fallbacks = [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

            for pri in fallbacks:
                if pri not in tried:
                    tried.append(pri)

            picked = False
            for pri in tried:
                # In interactive slots, if batch is starved, try batch first.
                if pri == Priority.INTERACTIVE and batch_starved:
                    p2, op2, cpu2, ram2 = _pick_next(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                    if p2 is not None:
                        assignment = Assignment(
                            ops=[op2],
                            cpu=cpu2,
                            ram=ram2,
                            priority=p2.priority,
                            pool_id=pool_id,
                            pipeline_id=p2.pipeline_id,
                        )
                        assignments.append(assignment)
                        s.op_to_pipeline[_op_key(op2)] = p2.pipeline_id
                        scheduled_this_step.add(p2.pipeline_id)
                        avail_cpu -= float(cpu2)
                        avail_ram -= float(ram2)
                        picked = True
                        break

                p, op, cpu_req, ram_req = _pick_next(pri, pool, avail_cpu, avail_ram)
                if p is None:
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                s.op_to_pipeline[_op_key(op)] = p.pipeline_id
                scheduled_this_step.add(p.pipeline_id)

                if p.priority == Priority.QUERY:
                    s.last_query_scheduled_tick[pool_id] = s.ticks

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                picked = True
                break

            if not picked:
                break

    return suspensions, assignments
