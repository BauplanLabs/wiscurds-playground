@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on:
      - Stable per-op sizing across cluster sizes (absolute caps, not fractions of cluster)
      - Much higher concurrency for CPU-bound bursty workloads (avoid giant per-op CPU/RAM)
      - Always-on headroom for high-priority arrivals (no preemption available)
      - OOM-aware RAM retries with fast convergence, plus gentle downsize after success
      - Aging/urgency within each priority to reduce timeout penalties
    """
    s.ticks = 0

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

    s.enqueued = set()
    s.pipeline_by_id = {}  # pipeline_id -> Pipeline
    s.pipeline_meta = {}   # pipeline_id -> {"enqueued_tick": int, "last_scheduled_tick": int}

    # Per operator hints
    s.op_ram_hint = {}      # (pipeline_id, op_key) -> ram
    s.op_fail_count = {}    # (pipeline_id, op_key) -> int
    s.pipeline_fail_count = {}  # pipeline_id -> int

    # Attribution + inflight accounting
    s.op_to_pipeline = {}   # op_key -> pipeline_id
    s.op_inflight = {}      # op_key -> {"cpu": float, "ram": float, "pool_id": int, "priority": Priority, "pipeline_id": str/int}

    s.inflight_cpu = {}     # pool_id -> {Priority -> float}
    s.inflight_ram = {}     # pool_id -> {Priority -> float}

    # Arrival tracking for elastic headroom
    s.last_query_arrival_tick = -10**9
    s.last_interactive_arrival_tick = -10**9

    # Recent OOM tracking to slightly bias RAM upward if we see a spike
    s.recent_oom_ticks = []   # list[int]
    s.recent_oom_window = 200

    # Housekeeping
    s.compact_every_ticks = 200
    s.pick_scan = 60  # scan window per pick within a priority queue (limits per-step overhead)

    # Safety caps (non-OOM failures)
    s.max_nonoom_failures_per_op = 10
    s.max_nonoom_failures_per_pipeline = 25


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
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _get_max_job_seconds():
        params = getattr(s, "params", None)
        if params is None:
            return None
        if isinstance(params, dict):
            return params.get("max_job_seconds", None)
        return getattr(params, "max_job_seconds", None)

    def _ensure_pool_counters(pool_id):
        if pool_id not in s.inflight_cpu:
            s.inflight_cpu[pool_id] = {
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            }
            s.inflight_ram[pool_id] = {
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            }

    def _queue_compact_if_needed():
        if s.ticks % s.compact_every_ticks != 0:
            return
        active = set(s.pipeline_by_id.keys())
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                s.q_cursor[pri] = 0
                continue
            new_q = [pid for pid in q if pid in active]
            s.wait_q[pri] = new_q
            s.q_cursor[pri] = 0

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_by_id.pop(pid, None)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_fail_count.pop(pid, None)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _oom_pressure_multiplier():
        # Slight upward bias if OOMs recently spiked; kept small to avoid blowing RAM.
        # (Most gain here comes from concurrency + fast per-op OOM convergence.)
        w = s.recent_oom_window
        while s.recent_oom_ticks and s.recent_oom_ticks[0] <= s.ticks - w:
            s.recent_oom_ticks.pop(0)
        n = len(s.recent_oom_ticks)
        if n >= 10:
            return 1.25
        if n >= 5:
            return 1.15
        return 1.0

    def _base_cpu(pri):
        if pri == Priority.QUERY:
            return 8.0
        if pri == Priority.INTERACTIVE:
            return 6.0
        return 4.0

    def _cap_cpu(pri):
        if pri == Priority.QUERY:
            return 12.0
        if pri == Priority.INTERACTIVE:
            return 10.0
        return 8.0

    def _base_ram(pri):
        # GB-scale defaults; significantly smaller than old "fraction of cluster" sizing
        if pri == Priority.QUERY:
            return 12.0
        if pri == Priority.INTERACTIVE:
            return 10.0
        return 8.0

    def _cap_ram(pri):
        if pri == Priority.QUERY:
            return 96.0
        if pri == Priority.INTERACTIVE:
            return 96.0
        return 128.0

    def _reserve_cpu(pool, pool_id):
        # Always leave some headroom (no preemption).
        # Make interactive reserve elastic based on backlog/recency.
        max_cpu = float(pool.max_cpu_pool)
        qbuf = max(2.0, 0.08 * max_cpu)

        interactive_backlog = len(s.wait_q[Priority.INTERACTIVE]) > 0
        recent_interactive = (s.ticks - s.last_interactive_arrival_tick) <= 30
        if interactive_backlog or recent_interactive:
            ibuf = max(4.0, 0.25 * max_cpu)
        else:
            ibuf = max(2.0, 0.08 * max_cpu)

        # If query has been arriving recently, slightly increase its buffer.
        recent_query = (s.ticks - s.last_query_arrival_tick) <= 30
        if recent_query:
            qbuf = max(qbuf, 0.10 * max_cpu)

        return qbuf, ibuf

    def _reserve_ram(pool):
        max_ram = float(pool.max_ram_pool)
        qbuf = max(2.0, 0.02 * max_ram)
        ibuf = max(4.0, 0.04 * max_ram)
        return qbuf, ibuf

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        opk = _op_key(op)
        wait = _pipeline_wait_ticks(pid)

        # CPU: base + mild aging boost, capped (absolute caps => scales well with cluster size).
        cpu = _base_cpu(pri)
        if pri == Priority.INTERACTIVE:
            if wait >= 200:
                cpu += 3.0
            elif wait >= 100:
                cpu += 2.0
        elif pri == Priority.BATCH_PIPELINE:
            if wait >= 400:
                cpu += 2.0
            elif wait >= 250:
                cpu += 1.0

        cpu = min(cpu, _cap_cpu(pri))

        # If there's a lot of free CPU (quiet period), opportunistically scale up within caps.
        # This improves makespan without harming burst latency (buffers still enforced).
        if avail_cpu >= 0.60 * float(pool.max_cpu_pool):
            cpu = min(_cap_cpu(pri), max(cpu, 0.75 * _cap_cpu(pri)))

        # RAM: per-op hint if known; else modest base with small OOM-pressure multiplier.
        hint = s.op_ram_hint.get((pid, opk), None)
        if hint is None:
            ram = _base_ram(pri) * _oom_pressure_multiplier()
        else:
            ram = float(hint)

        ram = min(ram, _cap_ram(pri))

        # Clip to pool capacity; also enforce minimums.
        cpu = max(1.0, min(cpu, float(pool.max_cpu_pool)))
        ram = max(1.0, min(ram, float(pool.max_ram_pool)))

        # Finally, clip to current availability (fit checks handled by caller with reserves).
        cpu = min(cpu, float(avail_cpu))
        ram = min(ram, float(avail_ram))
        return cpu, ram

    def _pick_runnable_from_priority(pri, pool, pool_id, avail_cpu, avail_ram, min_free_cpu_after, min_free_ram_after, scheduled_this_tick):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None

        cursor = s.q_cursor[pri] % n if n > 0 else 0
        scan = min(n, s.pick_scan)

        best = None
        best_age = -1

        for i in range(scan):
            idx = (cursor + i) % n
            pid = q[idx]
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            if pid in scheduled_this_tick:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                continue
            op = op_list[0]

            cpu_req, ram_req = _size_request(pool, pri, pid, op, avail_cpu, avail_ram)
            if cpu_req < 1.0 or ram_req < 1.0:
                continue

            # If we have an explicit hint that cannot fit (even before reserves), skip quickly.
            hint = s.op_ram_hint.get((pid, _op_key(op)), None)
            if hint is not None and float(hint) > float(avail_ram):
                continue

            # Respect headroom constraints
            if (float(avail_cpu) - float(cpu_req)) < float(min_free_cpu_after):
                continue
            if (float(avail_ram) - float(ram_req)) < float(min_free_ram_after):
                continue

            age = _pipeline_wait_ticks(pid)
            if age > best_age:
                best_age = age
                best = (pid, p, op, cpu_req, ram_req, idx)

        if best is None:
            return None

        # Advance cursor to promote RR fairness while still aging-aware.
        _, _, _, _, _, chosen_idx = best
        s.q_cursor[pri] = (chosen_idx + 1) % max(1, len(s.wait_q[pri]))
        return best

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        # Always keep the latest object reference
        s.pipeline_by_id[pid] = p

        if pid not in s.enqueued:
            s.enqueued.add(pid)
            s.wait_q[p.priority].append(pid)
            s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_scheduled_tick": -1}

            if p.priority == Priority.QUERY:
                s.last_query_arrival_tick = s.ticks
            elif p.priority == Priority.INTERACTIVE:
                s.last_interactive_arrival_tick = s.ticks

    _queue_compact_if_needed()

    # --- Process results: update inflight counters + OOM hints + failure counters ---
    for r in results:
        pri = getattr(r, "priority", None)
        failed = (hasattr(r, "failed") and r.failed())

        ops = getattr(r, "ops", []) or []
        # Decrement inflight accounting for each op in this result (we schedule one-op assignments,
        # but handle lists defensively).
        for op in ops:
            opk = _op_key(op)
            rec = s.op_inflight.pop(opk, None)
            if rec is not None:
                pool_id = rec.get("pool_id", getattr(r, "pool_id", None))
                _ensure_pool_counters(pool_id)
                ppri = rec.get("priority", pri)
                cpu = float(rec.get("cpu", 0.0))
                ram = float(rec.get("ram", 0.0))
                s.inflight_cpu[pool_id][ppri] = max(0.0, s.inflight_cpu[pool_id][ppri] - cpu)
                s.inflight_ram[pool_id][ppri] = max(0.0, s.inflight_ram[pool_id][ppri] - ram)

            pid = None
            if rec is not None:
                pid = rec.get("pipeline_id", None)
            if pid is None:
                pid = s.op_to_pipeline.get(opk, None)

            if pid is None:
                continue

            s.op_to_pipeline[opk] = pid  # keep attribution fresh

            if failed:
                err = getattr(r, "error", None)
                op_fail_key = (pid, opk)
                s.op_fail_count[op_fail_key] = s.op_fail_count.get(op_fail_key, 0) + 1
                s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

                if _is_oom_error(err):
                    s.recent_oom_ticks.append(s.ticks)

                    prev = s.op_ram_hint.get(op_fail_key, None)
                    alloc = getattr(r, "ram", None)
                    base = None
                    if alloc is not None and float(alloc) > 0:
                        base = float(alloc)
                    elif rec is not None and rec.get("ram", None) is not None:
                        base = float(rec.get("ram"))
                    elif prev is not None:
                        base = float(prev)
                    else:
                        base = 4.0

                    # Fast convergence: multiply, plus a minimum jump to avoid many retries.
                    # Still capped later by pool.max_ram_pool when we size.
                    new_hint = max(base * 2.0, (float(prev) * 1.7 if prev is not None else 0.0), base + 8.0)
                    s.op_ram_hint[op_fail_key] = new_hint
                else:
                    # For non-OOM failures, keep retrying, but stop burning resources forever.
                    # (Dropping still yields an incomplete pipeline penalty, but avoids runaway.)
                    if s.op_fail_count[op_fail_key] >= s.max_nonoom_failures_per_op:
                        # Mark as "huge RAM hint" so it won't fit and won't get scheduled.
                        s.op_ram_hint[op_fail_key] = 10**18
                    if s.pipeline_fail_count.get(pid, 0) >= s.max_nonoom_failures_per_pipeline:
                        _drop_pipeline(pid)
            else:
                # Success: gently reduce RAM hint if we had over-bumped it.
                alloc = getattr(r, "ram", None)
                if alloc is not None and float(alloc) > 0:
                    op_ok_key = (pid, opk)
                    prev = s.op_ram_hint.get(op_ok_key, None)
                    if prev is not None:
                        # If we can, shrink toward a small safety margin above last allocation.
                        target = max(1.0, float(alloc) * 1.10)
                        # Only shrink, never grow on success.
                        s.op_ram_hint[op_ok_key] = min(float(prev), target)
                    else:
                        # Record a conservative "known good" baseline.
                        s.op_ram_hint[op_ok_key] = max(1.0, float(alloc) * 1.10)

                # Clear failure counters on success
                s.op_fail_count.pop((pid, opk), None)

    # --- Main scheduling ---
    suspensions = []
    assignments = []

    # If no active pipelines, nothing to do.
    if not s.pipeline_by_id:
        return suspensions, assignments

    max_job_seconds = _get_max_job_seconds()

    # Global per-tick fairness: at most 1 op per pipeline per tick.
    scheduled_this_tick = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        _ensure_pool_counters(pool_id)

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        q_res_cpu, i_res_cpu = _reserve_cpu(pool, pool_id)
        q_res_ram, i_res_ram = _reserve_ram(pool)

        # In very urgent situations, relax buffers slightly to avoid near-certain penalties.
        # (Only if we can estimate a deadline and pipelines have been waiting long.)
        relax = False
        if max_job_seconds is not None:
            # If many interactive pipelines have waited close to max_job_seconds, relax buffers.
            # We use tick-as-seconds approximation.
            oldest_interactive_wait = 0
            for pid in s.wait_q[Priority.INTERACTIVE][:min(len(s.wait_q[Priority.INTERACTIVE]), 25)]:
                oldest_interactive_wait = max(oldest_interactive_wait, _pipeline_wait_ticks(pid))
            if oldest_interactive_wait >= 0.85 * float(max_job_seconds):
                relax = True

        if relax:
            q_res_cpu = max(1.0, 0.70 * q_res_cpu)
            i_res_cpu = max(2.0, 0.75 * i_res_cpu)
            q_res_ram = max(1.0, 0.70 * q_res_ram)
            i_res_ram = max(2.0, 0.75 * i_res_ram)

        # Fill loop
        attempts = 0
        max_attempts = 500  # avoids pathological loops if queues are blocked
        while avail_cpu >= 1.0 and avail_ram >= 1.0 and attempts < max_attempts:
            attempts += 1
            picked = None

            # Priority order: QUERY -> INTERACTIVE -> BATCH
            # Enforce headroom by priority (no preemption).
            # QUERY: no headroom constraint.
            picked = _pick_runnable_from_priority(
                Priority.QUERY,
                pool,
                pool_id,
                avail_cpu,
                avail_ram,
                min_free_cpu_after=0.0,
                min_free_ram_after=0.0,
                scheduled_this_tick=scheduled_this_tick,
            )

            if picked is None:
                # INTERACTIVE: preserve query headroom.
                picked = _pick_runnable_from_priority(
                    Priority.INTERACTIVE,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    min_free_cpu_after=q_res_cpu,
                    min_free_ram_after=q_res_ram,
                    scheduled_this_tick=scheduled_this_tick,
                )

            if picked is None:
                # BATCH: preserve query + interactive headroom.
                picked = _pick_runnable_from_priority(
                    Priority.BATCH_PIPELINE,
                    pool,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    min_free_cpu_after=(q_res_cpu + i_res_cpu),
                    min_free_ram_after=(q_res_ram + i_res_ram),
                    scheduled_this_tick=scheduled_this_tick,
                )

            if picked is None:
                break

            pid, p, op, cpu_req, ram_req, _ = picked
            if cpu_req < 1.0 or ram_req < 1.0:
                scheduled_this_tick.add(pid)
                continue

            # Final fit guard
            if cpu_req > avail_cpu or ram_req > avail_ram:
                scheduled_this_tick.add(pid)
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            opk = _op_key(op)
            s.op_to_pipeline[opk] = pid
            s.op_inflight[opk] = {
                "cpu": float(cpu_req),
                "ram": float(ram_req),
                "pool_id": pool_id,
                "priority": p.priority,
                "pipeline_id": pid,
            }
            s.inflight_cpu[pool_id][p.priority] += float(cpu_req)
            s.inflight_ram[pool_id][p.priority] += float(ram_req)

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_scheduled_tick"] = s.ticks

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            scheduled_this_tick.add(pid)

    return suspensions, assignments
