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
    s.pipeline_by_id = {}
    s.pipeline_meta = {}  # pid -> {"enqueued_tick": int}
    s.pipeline_started = set()
    s.dead_pipelines = set()

    # Per-op tracking (keyed by (pipeline_id, op_key))
    s.op_to_pipeline = {}       # op_key -> pipeline_id
    s.op_last_alloc_ram = {}    # (pid, opk) -> ram
    s.op_last_alloc_cpu = {}    # (pid, opk) -> cpu
    s.op_mem_oom = {}           # (pid, opk) -> largest ram that OOM'd
    s.op_mem_ok = {}            # (pid, opk) -> last ram that succeeded (allocation)
    s.op_retry_oom = {}         # (pid, opk) -> count
    s.op_retry_other = {}       # (pid, opk) -> count

    # Scheduling knobs
    s.scan_limit = 24
    s.max_total_assignments_per_tick = 256
    s.max_assignments_per_pool_per_tick = 128

    # Base RAM (GB) by priority: tuned to avoid massive over-allocation while reducing OOMs.
    s.base_ram = {
        Priority.QUERY: 28.0,
        Priority.INTERACTIVE: 24.0,
        Priority.BATCH_PIPELINE: 20.0,
    }
    s.oom_growth = 1.7
    s.oom_add_gb = 8.0
    s.max_oom_retries = 6
    s.max_other_retries = 2

    # CPU sizing: derive from RAM request and cluster RAM/CPU ratio, with per-priority clamps.
    s.min_cpu = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }
    s.max_cpu = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 8,
    }

    # Backfill mix between INTERACTIVE and BATCH (queries are always attempted first).
    s.backfill_cycle_default = [Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.backfill_cycle_equal = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.backfill_cycle_batchy = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.backfill_i = 0

    # Queue hygiene / compaction
    s.holes = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # "Need fill" lets us keep filling large pools across ticks even if no new events
    s.need_fill = [True for _ in range(getattr(s.executor, "num_pools", 1))]


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Prefer truly unique identity; fall back to object id.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # Common patterns: "oom", "out of memory", "MemoryError", "killed", exit code 137
        if "oom" in msg or "out of memory" in msg or "memoryerror" in msg:
            return True
        if "killed" in msg or "exit code 137" in msg or "signal 9" in msg:
            return True
        return False

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_by_id.pop(pid, None)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_started.discard(pid)

    def _compact_queue(pri):
        q = s.wait_q[pri]
        if not q:
            s.wait_pos[pri] = 0
            s.holes[pri] = 0
            return
        new_q = []
        for pid in q:
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue
            if pid not in s.enqueued:
                continue
            if pid not in s.pipeline_by_id:
                continue
            new_q.append(pid)
        s.wait_q[pri] = new_q
        s.wait_pos[pri] = 0 if not new_q else (s.wait_pos[pri] % len(new_q))
        s.holes[pri] = 0

    def _queue_len(pri):
        # Approximate backlog; queue may contain some holes.
        q = s.wait_q[pri]
        return 0 if not q else len(q)

    # --- Ingest new pipelines (assumed: new arrivals since last tick) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        s.pipeline_by_id[pid] = p
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.wait_q[p.priority].append(pid)

    # --- Process results: learn memory, bound retries, blacklist hopeless pipelines ---
    for r in results:
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue
            key = (pid, opk)

            failed = (hasattr(r, "failed") and r.failed())
            alloc_ram = getattr(r, "ram", None)
            if alloc_ram is None or alloc_ram <= 0:
                alloc_ram = s.op_last_alloc_ram.get(key, None)

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    s.op_retry_oom[key] = s.op_retry_oom.get(key, 0) + 1
                    if alloc_ram is not None and alloc_ram > 0:
                        prev_oom = s.op_mem_oom.get(key, 0.0)
                        if alloc_ram > prev_oom:
                            s.op_mem_oom[key] = float(alloc_ram)

                    # If we previously "succeeded" at <= this RAM, invalidate (memory can vary).
                    ok = s.op_mem_ok.get(key, None)
                    if ok is not None and alloc_ram is not None and alloc_ram >= ok:
                        s.op_mem_ok.pop(key, None)

                    # If too many OOMs, give up on the pipeline (prevents infinite thrash).
                    if s.op_retry_oom[key] > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    s.op_retry_other[key] = s.op_retry_other.get(key, 0) + 1
                    if s.op_retry_other[key] > s.max_other_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success: record a "known good" allocation for this op.
                if alloc_ram is not None and alloc_ram > 0:
                    prev_ok = s.op_mem_ok.get(key, None)
                    if prev_ok is None or alloc_ram < prev_ok:
                        s.op_mem_ok[key] = float(alloc_ram)

                # Success resets non-OOM retry pressure for this op.
                if key in s.op_retry_other:
                    s.op_retry_other.pop(key, None)

    # Fast return if nothing new and we don't need to keep filling any pool
    if not results and not pipelines and (not any(s.need_fill) if hasattr(s, "need_fill") else True):
        return [], []

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    scheduled_pipeline_ids = set()

    def _current_backfill_cycle():
        # Use queue size pressure to ensure batch makes progress (avoid batch timeouts).
        ilen = _queue_len(Priority.INTERACTIVE)
        blen = _queue_len(Priority.BATCH_PIPELINE)
        if blen > 6 * (ilen + 1):
            return s.backfill_cycle_batchy
        if blen > 3 * (ilen + 1):
            return s.backfill_cycle_equal
        return s.backfill_cycle_default

    def _reserve_for_hp(pool):
        # Without preemption, avoid consuming the "last slot" needed to start at least one HP op.
        # Reserve for QUERY if any query is waiting; otherwise reserve for INTERACTIVE if any interactive is waiting.
        if _queue_len(Priority.QUERY) > 0:
            pri = Priority.QUERY
        elif _queue_len(Priority.INTERACTIVE) > 0:
            pri = Priority.INTERACTIVE
        else:
            return 0.0, 0.0

        ratio = pool.max_ram_pool / max(1.0, pool.max_cpu_pool)
        base_ram = min(pool.max_ram_pool, max(1.0, s.base_ram[pri]))
        base_cpu = int(base_ram / max(0.0001, ratio))
        base_cpu = max(s.min_cpu[pri], min(s.max_cpu[pri], base_cpu))
        return float(base_cpu), float(base_ram)

    def _ram_request(pri, pool, pid, opk):
        key = (pid, opk)

        ok = s.op_mem_ok.get(key, None)
        if ok is not None:
            return min(float(ok), float(pool.max_ram_pool))

        oom = s.op_mem_oom.get(key, 0.0)
        base = min(float(pool.max_ram_pool), max(1.0, float(s.base_ram[pri])))

        if oom and oom > 0:
            grown = max(oom * s.oom_growth, oom + s.oom_add_gb)
            req = max(base, grown)
        else:
            req = base

        # Avoid a single op taking the entire pool unless it must (leave at least 10% headroom).
        cap = float(pool.max_ram_pool) * 0.90
        if cap < 1.0:
            cap = float(pool.max_ram_pool)
        return min(float(pool.max_ram_pool), max(1.0, min(req, cap)))

    def _cpu_request(pri, pool, ram_req):
        ratio = pool.max_ram_pool / max(1.0, pool.max_cpu_pool)  # GB per vCPU
        cpu = int(float(ram_req) / max(0.0001, ratio))
        cpu = max(s.min_cpu[pri], cpu)
        cpu = min(s.max_cpu[pri], cpu)
        return float(cpu)

    def _pick_op_from_queue(pri, pool_id, pool, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        q = s.wait_q[pri]
        if not q:
            return None, None, None, None

        # Compaction trigger if too many holes accumulated.
        if s.holes[pri] > 2048 and s.holes[pri] > (len(q) // 2):
            _compact_queue(pri)
            q = s.wait_q[pri]
            if not q:
                return None, None, None, None

        n = len(q)
        pos0 = s.wait_pos[pri] % n
        scan = s.scan_limit if s.scan_limit < n else n

        # Two-pass: prefer pipelines that have started (reduces half-done timeout risk), then any.
        for pass_started_only in (True, False):
            for i in range(scan):
                idx = (pos0 + i) % n
                pid = q[idx]
                if pid is None:
                    continue
                if pid in s.dead_pipelines or pid not in s.enqueued:
                    q[idx] = None
                    s.holes[pri] += 1
                    continue
                if pid in scheduled_pipeline_ids:
                    continue
                if pass_started_only and (pid not in s.pipeline_started):
                    continue

                p = s.pipeline_by_id.get(pid, None)
                if p is None:
                    q[idx] = None
                    s.holes[pri] += 1
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    q[idx] = None
                    s.holes[pri] += 1
                    _drop_pipeline(pid)
                    continue

                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue

                op = ops[0]
                opk = _op_key(op)

                ram_req = _ram_request(pri, pool, pid, opk)
                if ram_req > avail_ram:
                    continue

                cpu_req = _cpu_request(pri, pool, ram_req)
                if cpu_req < 1.0:
                    cpu_req = 1.0
                if cpu_req > avail_cpu:
                    cpu_req = avail_cpu
                if cpu_req < 1.0:
                    continue

                # Reservation guard: don't let lower priorities consume the last slot needed for HP.
                if pri != Priority.QUERY and reserve_cpu > 0 and reserve_ram > 0:
                    if (avail_cpu - cpu_req) < reserve_cpu and (avail_ram - ram_req) < reserve_ram:
                        # Too tight: keep headroom for a HP op to start.
                        continue

                # Advance cursor past this element for fairness.
                s.wait_pos[pri] = (idx + 1) % n
                return pid, op, cpu_req, ram_req

        # Nothing found; advance cursor to avoid re-scanning same region next attempt.
        s.wait_pos[pri] = (pos0 + scan) % n
        return None, None, None, None

    total_cap = s.max_total_assignments_per_tick

    # Reset fill flags; we'll set them true if we hit caps with capacity left.
    if not hasattr(s, "need_fill") or len(s.need_fill) != num_pools:
        s.need_fill = [True for _ in range(num_pools)]
    else:
        for i in range(num_pools):
            s.need_fill[i] = False

    backfill_cycle = _current_backfill_cycle()

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        pool_cap = s.max_assignments_per_pool_per_tick

        # Reserve headroom for HP admissions (no preemption).
        reserve_cpu, reserve_ram = _reserve_for_hp(pool)

        while pool_cap > 0 and total_cap > 0:
            avail_cpu = local_cpu[pool_id]
            avail_ram = local_ram[pool_id]
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            # 1) Always try QUERY first (low scan, repeated per placement to keep latency tight)
            pid, op, cpu_req, ram_req = _pick_op_from_queue(
                Priority.QUERY, pool_id, pool, avail_cpu, avail_ram, 0.0, 0.0
            )

            # 2) Backfill INTERACTIVE/BATCH with a controlled mix
            if pid is None:
                pri0 = backfill_cycle[s.backfill_i % len(backfill_cycle)]
                s.backfill_i = (s.backfill_i + 1) % len(backfill_cycle)
                pri1 = Priority.BATCH_PIPELINE if pri0 == Priority.INTERACTIVE else Priority.INTERACTIVE

                pid, op, cpu_req, ram_req = _pick_op_from_queue(
                    pri0, pool_id, pool, avail_cpu, avail_ram, reserve_cpu, reserve_ram
                )
                if pid is None:
                    pid, op, cpu_req, ram_req = _pick_op_from_queue(
                        pri1, pool_id, pool, avail_cpu, avail_ram, reserve_cpu, reserve_ram
                    )

            if pid is None:
                break

            # Validate pipeline still exists and isn't dead
            p = s.pipeline_by_id.get(pid, None)
            if p is None or pid in s.dead_pipelines or pid not in s.enqueued:
                continue

            pri = p.priority

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

            opk = _op_key(op)
            s.op_to_pipeline[opk] = pid
            s.op_last_alloc_ram[(pid, opk)] = ram_req
            s.op_last_alloc_cpu[(pid, opk)] = cpu_req

            s.pipeline_started.add(pid)
            scheduled_pipeline_ids.add(pid)

            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            pool_cap -= 1
            total_cap -= 1

        # If we hit the pool cap but still have meaningful capacity left, keep filling next tick.
        if pool_cap <= 0 and local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= min(
            s.base_ram[Priority.BATCH_PIPELINE], pool.max_ram_pool
        ):
            s.need_fill[pool_id] = True

    return suspensions, assignments
