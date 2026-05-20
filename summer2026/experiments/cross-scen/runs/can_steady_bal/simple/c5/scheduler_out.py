@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on (1) completion rate and (2) avoiding
    pathological per-op sizing that scales with cluster size and destroys concurrency.

    Key changes vs previous version:
      - Fixed / ratio-based sizing: CPU and RAM requests are NOT fractions of pool max.
        They are small, stable targets (with RAM derived from pool RAM-per-CPU), so
        concurrency scales up with bigger clusters.
      - Robust retries: no permanent blacklisting on first non-OOM failure; instead,
        adapt CPU/RAM hints based on error type (OOM vs timeout vs unknown).
      - Balanced service between INTERACTIVE and BATCH to prevent mass batch timeouts
        (penalties dominate the score if batches don't finish).
      - Optional multi-pool behavior: if multiple pools exist, pool 0 is "latency-leaning"
        but never idles (falls back to batch if no higher-priority work can be scheduled).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()

    # { pipeline_id: {"enqueued_tick": int, "last_progress_tick": int, "last_scheduled_tick": int} }
    s.pipeline_meta = {}

    # Resource hints learned from failures (absolute units).
    # (pipeline_id, op_key) -> hint
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Failure counters
    s.op_failures = {}        # (pipeline_id, op_key) -> int
    s.pipeline_failures = {}  # pipeline_id -> int

    # Map op_key -> pipeline_id (best-effort attribution for results)
    s.op_to_pipeline = {}

    # Scheduling knobs (stable across cluster sizes)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.min_cpu = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # RAM request = ceil(mem_per_cpu * cpu_req * ram_factor), then max with per-op hints.
    # Choose modest factors to avoid OOM while preserving concurrency.
    s.ram_factor = {
        Priority.QUERY: 1.30,
        Priority.INTERACTIVE: 1.20,
        Priority.BATCH_PIPELINE: 1.10,
    }
    s.min_ram = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 16,
        Priority.BATCH_PIPELINE: 12,
    }

    # Retry escalation
    s.cpu_bump_timeout = 1.60     # multiply CPU on timeout-like failures
    s.ram_bump_oom = 2.00         # multiply RAM on OOM-like failures
    s.bump_unknown = 1.25         # multiply both CPU/RAM on unknown failures
    s.hard_escalate_after = 4     # after N failures for an op, escalate more aggressively

    # Avoid over-favoring interactive at the cost of batch completion.
    # We'll dynamically set batch share vs interactive based on queue sizes.
    s.ib_cursor = 0

    # Batch aging: after this many scheduler ticks, allow batch to be picked earlier.
    s.batch_starve_ticks = 40

    # Per-tick per-pipeline cap (prevents one pipeline from consuming a whole scheduling step)
    s.max_ops_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda oom" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg) or ("time limit" in msg)

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_failures.pop(pipeline_id, None)
        # Keep op hints: they are harmless and may help if IDs are reused in traces.

    def _pipeline_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _note_progress(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if meta is not None:
            meta["last_progress_tick"] = s.ticks

    # --- Ingest pipelines (best-effort: pipelines may be "new arrivals" or "all active") ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        try:
            st = p.runtime_status()
            if st is not None and st.is_pipeline_successful():
                continue
        except Exception:
            pass

        s.wait_q[p.priority].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {
            "enqueued_tick": s.ticks,
            "last_progress_tick": s.ticks,
            "last_scheduled_tick": -1,
        }

    # --- Process results (learn resource hints; never permanently blacklist on first failure) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        if hasattr(r, "failed"):
            try:
                failed = r.failed()
            except Exception:
                failed = False

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            if not failed:
                _note_progress(pid)
                continue

            err = getattr(r, "error", None)
            key = (pid, opk)

            s.op_failures[key] = s.op_failures.get(key, 0) + 1
            s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

            # Prior allocations (if present)
            prev_cpu = s.op_cpu_hint.get(key, None)
            prev_ram = s.op_ram_hint.get(key, None)
            alloc_cpu = getattr(r, "cpu", None)
            alloc_ram = getattr(r, "ram", None)

            # Normalize bases
            base_cpu = alloc_cpu if (alloc_cpu is not None and alloc_cpu > 0) else (prev_cpu if prev_cpu is not None else 1)
            base_ram = alloc_ram if (alloc_ram is not None and alloc_ram > 0) else (prev_ram if prev_ram is not None else 1)

            # Escalate more after repeated failures (helps converge quickly)
            hard = s.op_failures[key] >= s.hard_escalate_after
            hard_mul = 2.0 if hard else 1.0

            if _is_oom_error(err):
                new_ram = base_ram * (s.ram_bump_oom * hard_mul)
                # modest CPU bump too (sometimes OOM correlates with longer runtimes/GC pressure)
                new_cpu = base_cpu * (1.15 if not hard else 1.35)
                s.op_ram_hint[key] = max(prev_ram or 0, new_ram)
                s.op_cpu_hint[key] = max(prev_cpu or 0, new_cpu)
            elif _is_timeout_error(err):
                new_cpu = base_cpu * (s.cpu_bump_timeout * hard_mul)
                # tiny RAM bump for timeouts (can help avoid spill/overhead in some models)
                new_ram = base_ram * (1.10 if not hard else 1.25)
                s.op_cpu_hint[key] = max(prev_cpu or 0, new_cpu)
                s.op_ram_hint[key] = max(prev_ram or 0, new_ram)
            else:
                new_cpu = base_cpu * (s.bump_unknown * hard_mul)
                new_ram = base_ram * (s.bump_unknown * hard_mul)
                s.op_cpu_hint[key] = max(prev_cpu or 0, new_cpu)
                s.op_ram_hint[key] = max(prev_ram or 0, new_ram)

    suspensions = []
    assignments = []

    # Dynamic interactive vs batch share: ensure batch gets enough service to complete.
    q_len = len(s.wait_q[Priority.QUERY])
    i_len = len(s.wait_q[Priority.INTERACTIVE])
    b_len = len(s.wait_q[Priority.BATCH_PIPELINE])

    if i_len <= 0:
        batch_per_interactive = 4
    else:
        # Aim roughly proportional to backlog, but cap to keep interactive moving.
        # With this workload (batch ~2x interactive), this will usually be 2.
        ratio = int((b_len / max(1, i_len)) + 0.5)
        batch_per_interactive = max(1, min(3, ratio))

    # Pattern: 1 interactive then N batch
    ib_pattern = [Priority.INTERACTIVE] + ([Priority.BATCH_PIPELINE] * batch_per_interactive)
    if not ib_pattern:
        ib_pattern = [Priority.BATCH_PIPELINE]

    # Per-tick pipeline scheduling counts
    scheduled_count = {}

    def _can_schedule_more(p):
        pid = p.pipeline_id
        cap = s.max_ops_per_tick.get(p.priority, 1)
        return scheduled_count.get(pid, 0) < cap

    def _mem_per_cpu(pool):
        max_cpu = getattr(pool, "max_cpu_pool", 0) or 0
        max_ram = getattr(pool, "max_ram_pool", 0) or 0
        if max_cpu <= 0:
            return float(max_ram)
        return float(max_ram) / float(max_cpu)

    def _ceil(x):
        try:
            xi = int(x)
        except Exception:
            return 1
        return xi if xi >= x else xi + 1

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        opk = _op_key(op)
        key = (pid, opk)

        # CPU
        cpu_hint = s.op_cpu_hint.get(key, None)
        cpu_req = float(cpu_hint) if (cpu_hint is not None and cpu_hint > 0) else float(s.base_cpu.get(pri, 2))

        # Aging bump for long-waiting batch (small, and only if it doesn't prevent fitting)
        wait = _pipeline_wait_ticks(pid)
        if pri == Priority.BATCH_PIPELINE and wait >= s.batch_starve_ticks:
            cpu_req += 1.0

        # Clip to pool max and availability (but enforce minimum)
        max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
        if max_cpu > 0:
            cpu_req = min(cpu_req, max_cpu)
        cpu_req = min(cpu_req, float(avail_cpu))

        min_cpu = float(s.min_cpu.get(pri, 1))
        if cpu_req < min_cpu:
            return None

        # RAM
        mpc = _mem_per_cpu(pool)
        base_ram = mpc * cpu_req * float(s.ram_factor.get(pri, 1.0))
        base_ram = max(base_ram, float(s.min_ram.get(pri, 8)))

        ram_hint = s.op_ram_hint.get(key, None)
        ram_req = max(base_ram, float(ram_hint)) if (ram_hint is not None and ram_hint > 0) else base_ram

        max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)
        if max_ram > 0:
            ram_req = min(ram_req, max_ram)

        # If we have a hint that doesn't fit, don't downsize (would likely repeat failure).
        if ram_hint is not None and ram_hint > float(avail_ram):
            return None

        # Otherwise, allow slight downsizing to use fragments, but not below 80% of computed base.
        if ram_req > float(avail_ram):
            if float(avail_ram) < 0.80 * float(base_ram):
                return None
            ram_req = float(avail_ram)

        min_ram = float(s.min_ram.get(pri, 8))
        if ram_req < min_ram:
            return None

        # Use integer-like resource amounts for determinism / packing
        cpu_out = max(1, int(cpu_req))
        ram_out = max(1, _ceil(ram_req))

        # Re-check against availability after rounding
        if cpu_out > avail_cpu or ram_out > avail_ram:
            return None
        if cpu_out < int(min_cpu) or ram_out < int(min_ram):
            return None

        return cpu_out, ram_out

    def _pick_from_queue(q, pool, avail_cpu, avail_ram, pri_filter=None):
        if not q:
            return None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Skip if we dropped bookkeeping (completed long ago)
            if pid not in s.enqueued:
                continue

            # Enforce per-tick pipeline cap
            if not _can_schedule_more(p):
                q.append(p)
                continue

            # Completion check
            try:
                st = p.runtime_status()
                if st is not None and st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass

            # Optional priority filter (used for pool-role behavior)
            if pri_filter is not None and p.priority != pri_filter:
                q.append(p)
                continue

            # Find runnable op
            try:
                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) if st is not None else []
            except Exception:
                op_list = []

            if not op_list:
                q.append(p)
                continue

            op = op_list[0]

            sized = _size_request(pool, p.priority, pid, op, avail_cpu, avail_ram)
            if sized is None:
                q.append(p)
                continue

            cpu_req, ram_req = sized
            q.append(p)
            return p, op, cpu_req, ram_req

        return None

    def _try_pick(pri, pool, avail_cpu, avail_ram):
        return _pick_from_queue(s.wait_q[pri], pool, avail_cpu, avail_ram)

    # --- Main scheduling loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If we have multiple pools, make pool 0 "latency leaning" but never idle.
        latency_pool = (s.executor.num_pools > 1 and pool_id == 0)

        while avail_cpu >= 1 and avail_ram >= 1:
            picked = None

            # 1) Always try QUERY first
            picked = _try_pick(Priority.QUERY, pool, avail_cpu, avail_ram)

            # 2) Then INTERACTIVE/BATCH according to pattern
            if picked is None:
                # In latency pool, first attempt to drain interactive before batch;
                # but if nothing schedulable, fall back to batch to avoid idling.
                if latency_pool:
                    picked = _try_pick(Priority.INTERACTIVE, pool, avail_cpu, avail_ram)
                    if picked is None:
                        picked = _try_pick(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                else:
                    # Throughput pools: follow dynamic pattern, but with an aging escape hatch.
                    pri = ib_pattern[s.ib_cursor % len(ib_pattern)]
                    s.ib_cursor += 1

                    # If batches are starving badly, occasionally try batch first.
                    if s.ticks % 9 == 0:
                        pri = Priority.BATCH_PIPELINE

                    picked = _try_pick(pri, pool, avail_cpu, avail_ram)
                    if picked is None:
                        other = Priority.BATCH_PIPELINE if pri == Priority.INTERACTIVE else Priority.INTERACTIVE
                        picked = _try_pick(other, pool, avail_cpu, avail_ram)

                    # Aging-based escape hatch: if still nothing picked, and batch is old, try batch.
                    if picked is None and b_len > 0:
                        # Very cheap probe: look at a few batch items for long wait.
                        probe = 0
                        for bp in s.wait_q[Priority.BATCH_PIPELINE]:
                            if bp.pipeline_id in s.enqueued and _pipeline_wait_ticks(bp.pipeline_id) >= s.batch_starve_ticks:
                                picked = _try_pick(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                                break
                            probe += 1
                            if probe >= 8:
                                break

            if picked is None:
                break

            p, op, cpu_req, ram_req = picked

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Track for result attribution
            s.op_to_pipeline[_op_key(op)] = p.pipeline_id

            # Update meta + local accounting
            meta = s.pipeline_meta.get(p.pipeline_id)
            if meta is not None:
                meta["last_scheduled_tick"] = s.ticks

            scheduled_count[p.pipeline_id] = scheduled_count.get(p.pipeline_id, 0) + 1

            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
