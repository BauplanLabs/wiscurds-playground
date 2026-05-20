@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on:
      - Fixed (cluster-size independent) per-op CPU/RAM sizing to avoid pathological scale-up.
      - Much higher concurrency for CPU-bound workloads (reduces queueing -> fewer timeouts).
      - Weighted sharing between INTERACTIVE and BATCH (protect interactive latency without starving batch).
      - OOM/timeout-aware retries via per-operator resource hints (RAM-first for OOM, CPU-first for timeout).
      - No early-exit: always try to schedule from existing queues when capacity is available.
    """
    s.ticks = 0

    # Queue holds pipeline_ids (not Pipeline objects) to avoid stale references.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipelines seen/managed.
    s.seen = set()          # pipeline_ids ever ingested (prevents re-enqueue loops)
    s.enqueued = set()      # pipeline_ids currently tracked (queued/alive)
    s.dead_pipelines = set()

    # Latest Pipeline objects by id (refreshed each schedule() call).
    s.pipeline_by_id = {}
    s.pipeline_priority = {}  # pipeline_id -> Priority

    # Per-pipeline metadata for aging/urgency.
    # { pipeline_id: {"enqueued_tick": int, "last_progress_tick": int} }
    s.pipeline_meta = {}

    # Operator hints keyed by op object identity (stable within the simulator).
    # opk -> ram_hint / cpu_hint
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Operator failure counts
    s.op_fail_count = {}  # opk -> int

    # Map opk -> pipeline_id for attributing results.
    s.op_to_pipeline = {}

    # Non-query fairness state (interactive vs batch).
    s.i_since_b = 0

    # Knobs (kept conservative; tuned for CPU-bound bursty workloads).
    s.max_non_oom_failures_per_op = 3

    # CPU sizing (vCPU)
    s.cpu_min = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_base = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 6.0,
    }
    s.cpu_cap = {
        Priority.QUERY: 32.0,
        Priority.INTERACTIVE: 24.0,
        Priority.BATCH_PIPELINE: 16.0,
    }

    # RAM sizing (GB)
    s.ram_min = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 10.0,
    }
    s.ram_base = {
        Priority.QUERY: 14.0,
        Priority.INTERACTIVE: 18.0,
        Priority.BATCH_PIPELINE: 26.0,
    }
    s.ram_cap = {
        Priority.QUERY: 128.0,
        Priority.INTERACTIVE: 192.0,
        Priority.BATCH_PIPELINE: 256.0,
    }

    # Aging thresholds (ticks, not seconds)
    s.urgent_query_ticks = 3
    s.urgent_interactive_ticks = 8
    s.very_urgent_interactive_ticks = 14

    # Weighted sharing between INTERACTIVE and BATCH when both backlogged.
    s.interactive_before_batch_default = 5
    s.interactive_before_batch_under_pressure = 10


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Prefer object identity to avoid cross-pipeline collisions; simulator should preserve op objects.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time limit" in msg) or ("deadline" in msg)

    def _queue(pri):
        return s.wait_q[pri]

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_priority.pop(pipeline_id, None)
        # We intentionally keep pipeline_by_id and seen for dedupe and possible introspection.

    def _pipeline_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _approx_oldest_wait_ticks(pri, sample=6):
        q = _queue(pri)
        if not q:
            return 0
        m = 0
        n = 0
        for pid in q:
            if pid in s.enqueued and pid not in s.dead_pipelines:
                w = _pipeline_wait_ticks(pid)
                if w > m:
                    m = w
                n += 1
                if n >= sample:
                    break
        return m

    def _backlog_len(pri):
        return len(_queue(pri))

    def _cpu_target_for(pri, pid, opk, pool):
        # Start from hint if exists, else base; reduce CPU under heavy backlog (more parallelism).
        hint = s.op_cpu_hint.get(opk)
        if hint is not None and hint > 0:
            target = float(hint)
        else:
            target = float(s.cpu_base[pri])
            qlen = _backlog_len(pri)
            if qlen >= 120:
                target = max(s.cpu_min[pri], target * 0.60)
            elif qlen >= 60:
                target = max(s.cpu_min[pri], target * 0.75)
            elif qlen >= 25:
                target = max(s.cpu_min[pri], target * 0.90)

        # Age boost to reduce deadline misses (helps interactive completion rate).
        age = _pipeline_wait_ticks(pid)
        if pri == Priority.QUERY and age >= s.urgent_query_ticks:
            target *= 1.5
        elif pri == Priority.INTERACTIVE:
            if age >= s.very_urgent_interactive_ticks:
                target *= 2.0
            elif age >= s.urgent_interactive_ticks:
                target *= 1.5

        # Clamp by policy caps and pool capacity.
        target = min(target, float(s.cpu_cap[pri]), float(pool.max_cpu_pool))
        target = max(target, float(s.cpu_min[pri]))
        return target

    def _ram_target_for(pri, opk, pool):
        hint = s.op_ram_hint.get(opk)
        if hint is not None and hint > 0:
            target = float(hint)
        else:
            target = float(s.ram_base[pri])

        # Clamp by caps and pool capacity.
        target = min(target, float(s.ram_cap[pri]), float(pool.max_ram_pool))
        target = max(target, float(s.ram_min[pri]))
        return target

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)

        # If we have a RAM hint that doesn't fit, don't try this pool right now.
        ram_target = _ram_target_for(pri, opk, pool)
        if ram_target > float(pool.avail_ram_pool):
            return None, None

        cpu_target = _cpu_target_for(pri, pid, opk, pool)

        # Enforce minima against current availability (don't create 1-vCPU slivers).
        if float(pool.avail_cpu_pool) < float(s.cpu_min[pri]) or float(pool.avail_ram_pool) < float(s.ram_min[pri]):
            return None, None

        # Choose request not exceeding availability. Avoid going below min.
        cpu_req = min(cpu_target, float(pool.avail_cpu_pool))
        if cpu_req < float(s.cpu_min[pri]):
            return None, None

        ram_req = min(ram_target, float(pool.avail_ram_pool))
        if ram_req < float(s.ram_min[pri]):
            return None, None

        # Final clamp (defensive).
        cpu_req = min(cpu_req, float(pool.max_cpu_pool))
        ram_req = min(ram_req, float(pool.max_ram_pool))
        return cpu_req, ram_req

    # --- Refresh pipeline references and ingest new arrivals ---
    for p in pipelines or []:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p

        if pid in s.dead_pipelines:
            continue

        if pid in s.seen:
            continue

        s.seen.add(pid)
        s.enqueued.add(pid)
        s.pipeline_priority[pid] = p.priority
        _queue(p.priority).append(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_progress_tick": s.ticks}

    # --- Process results for OOM/timeout hints and failure handling ---
    for r in results or []:
        ops = getattr(r, "ops", None) or []
        failed = (hasattr(r, "failed") and r.failed())

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_progress_tick"] = s.ticks

            if not failed:
                # On success, we can optionally be slightly less aggressive on future CPU hints.
                # Keep it simple and stable: no decay (prevents oscillation).
                continue

            err = getattr(r, "error", None)
            is_oom = _is_oom_error(err)
            is_to = _is_timeout_error(err)

            s.op_fail_count[opk] = s.op_fail_count.get(opk, 0) + 1

            if is_oom:
                prev = s.op_ram_hint.get(opk)
                alloc = getattr(r, "ram", None)
                base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev) if prev is not None else float(s.ram_base.get(s.pipeline_priority.get(pid, Priority.BATCH_PIPELINE), 16.0)))
                new_hint = max(base * 2.0, (float(prev) * 2.0 if prev is not None else 0.0))
                s.op_ram_hint[opk] = new_hint
                # Slight CPU bump on OOM can help by finishing faster once it fits.
                prev_cpu = s.op_cpu_hint.get(opk)
                if prev_cpu is not None:
                    s.op_cpu_hint[opk] = max(float(prev_cpu), float(prev_cpu) * 1.10)
            elif is_to:
                prev = s.op_cpu_hint.get(opk)
                alloc = getattr(r, "cpu", None)
                base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev) if prev is not None else 0.0)
                new_hint = max(base * 1.6, (float(prev) * 1.6 if prev is not None else 0.0), 1.0)
                s.op_cpu_hint[opk] = new_hint
            else:
                # Unknown failure: allow a few retries with mild resource bumps; then give up on that pipeline.
                prev_cpu = s.op_cpu_hint.get(opk)
                if prev_cpu is None:
                    s.op_cpu_hint[opk] = 1.25 * float(getattr(r, "cpu", 1.0) or 1.0)
                else:
                    s.op_cpu_hint[opk] = float(prev_cpu) * 1.25

                prev_ram = s.op_ram_hint.get(opk)
                if prev_ram is None:
                    s.op_ram_hint[opk] = 1.15 * float(getattr(r, "ram", 1.0) or 1.0)
                else:
                    s.op_ram_hint[opk] = float(prev_ram) * 1.15

                if s.op_fail_count.get(opk, 0) >= int(s.max_non_oom_failures_per_op):
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)

    suspensions = []
    assignments = []

    scheduled_pipeline_ids = set()

    def _pick_next_op_from_queue(pri, pool):
        q = _queue(pri)
        if not q:
            return None, None, None, None

        n = len(q)
        for _ in range(n):
            pid = q.pop(0)

            # Skip if no longer tracked
            if pid not in s.enqueued or pid in s.dead_pipelines:
                continue

            if pid in scheduled_pipeline_ids:
                q.append(pid)
                continue

            p = s.pipeline_by_id.get(pid)
            if p is None:
                # Can't schedule without object; keep it for later.
                q.append(pid)
                continue

            status = p.runtime_status()

            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            # Get one runnable op (safe; avoids duplicate-assign in same tick if status updates are delayed).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(pid)
                continue

            op = op_list[0]

            cpu_req, ram_req = _size_request(pool, pri, pid, op)
            if cpu_req is None or ram_req is None:
                q.append(pid)
                continue

            # Keep pipeline in queue for future ops; mark as scheduled for this tick.
            q.append(pid)
            return pid, op, cpu_req, ram_req

        return None, None, None, None

    def _interactive_before_batch():
        i_backlog = _backlog_len(Priority.INTERACTIVE)
        i_oldest = _approx_oldest_wait_ticks(Priority.INTERACTIVE)
        if i_backlog >= 80 or i_oldest >= s.urgent_interactive_ticks:
            return int(s.interactive_before_batch_under_pressure)
        return int(s.interactive_before_batch_default)

    # --- Main scheduling loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Snapshot avail; we update locally as we add assignments.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Hard stop if essentially empty.
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Fill the pool.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            made = False

            # Always try QUERY first (protect tail latency).
            pid, op, cpu_req, ram_req = _pick_next_op_from_queue(Priority.QUERY, pool)
            if pid is not None:
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=Priority.QUERY,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)
                s.op_to_pipeline[_op_key(op)] = pid
                scheduled_pipeline_ids.add(pid)
                meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["last_progress_tick"] = s.ticks

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                made = True
                continue  # keep draining

            # Choose between INTERACTIVE and BATCH with weighted sharing.
            ib = _interactive_before_batch()

            # Prefer INTERACTIVE most of the time when both present.
            prefer_batch = False
            if _backlog_len(Priority.INTERACTIVE) <= 0:
                prefer_batch = True
            elif _backlog_len(Priority.BATCH_PIPELINE) > 0 and s.i_since_b >= ib:
                prefer_batch = True

            primary = Priority.BATCH_PIPELINE if prefer_batch else Priority.INTERACTIVE
            secondary = Priority.INTERACTIVE if prefer_batch else Priority.BATCH_PIPELINE

            pid, op, cpu_req, ram_req = _pick_next_op_from_queue(primary, pool)
            chosen_pri = primary

            if pid is None:
                pid, op, cpu_req, ram_req = _pick_next_op_from_queue(secondary, pool)
                chosen_pri = secondary

            if pid is None:
                # No schedulable work fits right now.
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pri,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            s.op_to_pipeline[_op_key(op)] = pid
            scheduled_pipeline_ids.add(pid)
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_progress_tick"] = s.ticks

            if chosen_pri == Priority.BATCH_PIPELINE:
                s.i_since_b = 0
            elif chosen_pri == Priority.INTERACTIVE:
                s.i_since_b += 1

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            made = True

            if not made:
                break

    return suspensions, assignments
