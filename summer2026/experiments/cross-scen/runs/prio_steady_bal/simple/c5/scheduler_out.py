@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on:
      - Preventing INTERACTIVE starvation under heavy QUERY load (work-conserving weighted fairness).
      - Increasing throughput via smaller default CPU/RAM requests (higher concurrency).
      - Adaptive per-operator RAM/CPU hints on OOM/timeout failures (fast convergence, few retries).
      - Aging-based boosts to protect tail latency and completion rate for INTERACTIVE and BATCH.
    """
    s.ticks = 0

    # Per-priority pipeline queues (arrays with round-robin cursor; entries may be set to None).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.qpos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.none_count = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track which pipeline_ids are currently enqueued.
    s.enqueued = set()

    # Pipeline metadata for aging/fairness:
    # { pipeline_id: {"enqueued_tick": int, "last_service_tick": int} }
    s.pipeline_meta = {}

    # Per-operator adaptive hints and retry counters:
    # key: (pipeline_id, op_key) -> {"ram": float|None, "cpu": float|None, "fails": int}
    s.op_hint = {}

    # Map op_key -> (pipeline_id, op_key) for attributing ExecutionResult back to pipeline/op.
    s.op_to_key = {}

    # Pipelines we stop trying after repeated non-recoverable failures.
    s.dead_pipelines = set()

    # Per-pool round-robin pointer for weighted priority selection (initialized lazily).
    s.pool_rr_idx = []

    # Weighted fairness baseline (QUERY dominates, but INTERACTIVE is guaranteed service).
    # We build the effective RR list dynamically each tick (aging/backlog adjustments).
    s.base_weights = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }

    # Default sizing via "target concurrency" denominators (smaller denom => bigger per-op).
    # Tuned to avoid CPU monopolization and improve completion rate under load.
    s.cpu_denom = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 7.0,
        Priority.BATCH_PIPELINE: 10.0,
    }
    s.ram_denom = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 9.0,
    }

    # Caps for per-op sizing (avoid giving a single op the whole pool).
    s.cpu_cap = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 20.0,
        Priority.BATCH_PIPELINE: 16.0,
    }

    # Minimums
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Retry controls
    s.max_fails_per_op = 4          # total failures per op before we give up on the pipeline
    s.max_nonoom_fails_per_op = 2   # stricter for unknown errors

    # Aging / urgency thresholds (ticks since last service)
    s.interactive_urgent_ticks = 10
    s.batch_urgent_ticks = 25

    # Service caps per pipeline per tick (allows limited parallelism for wider DAGs)
    s.per_pipeline_tick_cap = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 1,
    }

    # Queue scan limits (avoid O(n^2) behavior under large queues)
    s.scan_limit = {
        Priority.QUERY: 250,
        Priority.INTERACTIVE: 250,
        Priority.BATCH_PIPELINE: 150,
    }

    # Compaction thresholds
    s.compact_min_len = 200
    s.compact_none_frac = 0.30


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return (
            ("oom" in msg)
            or ("out of memory" in msg)
            or ("memoryerror" in msg)
            or ("cannot allocate memory" in msg)
            or ("killed process" in msg)
        )

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _clip(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _mark_dead(pid):
        s.dead_pipelines.add(pid)
        _drop_pipeline(pid)

    def _ensure_pool_state():
        np = s.executor.num_pools
        if len(s.pool_rr_idx) != np:
            s.pool_rr_idx = [0 for _ in range(np)]

    def _compact_queue(pri):
        q = s.wait_q[pri]
        if len(q) < s.compact_min_len:
            return
        none_ct = s.none_count.get(pri, 0)
        if none_ct <= 0:
            return
        if (none_ct / float(len(q))) < s.compact_none_frac:
            return
        new_q = [p for p in q if p is not None]
        s.wait_q[pri] = new_q
        s.qpos[pri] = s.qpos[pri] % (len(new_q) if new_q else 1)
        s.none_count[pri] = 0

    def _queue_len(pri):
        # Approximate length ignoring None without scanning whole queue.
        q = s.wait_q[pri]
        if not q:
            return 0
        none_ct = s.none_count.get(pri, 0)
        return max(0, len(q) - none_ct)

    def _waiting_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        last = meta.get("last_service_tick", meta.get("enqueued_tick", s.ticks))
        return max(0, s.ticks - last)

    def _estimate_oldest_wait(pri, sample=40):
        q = s.wait_q[pri]
        if not q:
            return 0
        # Scan early portion of the list (captures old arrivals reasonably well).
        m = min(len(q), sample)
        oldest = 0
        for i in range(m):
            p = q[i]
            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.dead_pipelines:
                continue
            wt = _waiting_ticks(pid)
            if wt > oldest:
                oldest = wt
        return oldest

    def _build_rr_list():
        wq = s.base_weights[Priority.QUERY]
        wi = s.base_weights[Priority.INTERACTIVE]
        wb = s.base_weights[Priority.BATCH_PIPELINE]

        iq = _queue_len(Priority.INTERACTIVE)
        bq = _queue_len(Priority.BATCH_PIPELINE)

        if iq > 0:
            # Always bias a bit towards INTERACTIVE to avoid starvation.
            wi += 1
            # If INTERACTIVE is getting old, strongly boost it.
            if _estimate_oldest_wait(Priority.INTERACTIVE) >= s.interactive_urgent_ticks:
                wi += 4

        if bq > 0 and _estimate_oldest_wait(Priority.BATCH_PIPELINE) >= s.batch_urgent_ticks:
            wb += 1

        # Keep list bounded and deterministic.
        rr = (
            [Priority.QUERY] * int(max(1, wq))
            + [Priority.INTERACTIVE] * int(max(1, wi))
            + [Priority.BATCH_PIPELINE] * int(max(1, wb))
        )
        return rr

    def _default_size(pool, pri):
        cpu = pool.max_cpu_pool / float(s.cpu_denom[pri])
        ram = pool.max_ram_pool / float(s.ram_denom[pri])

        cpu = _clip(cpu, s.min_cpu, min(pool.max_cpu_pool, s.cpu_cap[pri]))
        ram = _clip(ram, s.min_ram, pool.max_ram_pool)
        return cpu, ram

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)
        base_cpu, base_ram = _default_size(pool, pri)

        hint = s.op_hint.get((pid, opk))
        if hint is None:
            hint_cpu = 0.0
            hint_ram = 0.0
        else:
            hint_cpu = float(hint.get("cpu") or 0.0)
            hint_ram = float(hint.get("ram") or 0.0)

        cpu = max(base_cpu, hint_cpu)
        ram = max(base_ram, hint_ram)

        # Aging boost: if a pipeline hasn't been serviced in a while, give it more CPU (helps completion).
        wt = _waiting_ticks(pid)
        if pri == Priority.INTERACTIVE and wt >= s.interactive_urgent_ticks:
            cpu = min(pool.max_cpu_pool, cpu * 1.6)
        elif pri == Priority.BATCH_PIPELINE and wt >= s.batch_urgent_ticks:
            cpu = min(pool.max_cpu_pool, cpu * 1.3)

        cpu = _clip(cpu, s.min_cpu, min(pool.max_cpu_pool, s.cpu_cap[pri]))
        ram = _clip(ram, s.min_ram, pool.max_ram_pool)

        # Integer-ish allocations
        cpu_i = int(cpu) if cpu >= 1 else 1
        ram_i = int(ram) if ram >= 1 else 1
        return cpu_i, ram_i

    # --- Ingest pipelines (treat `pipelines` as the current visible set; avoid duplicates) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid in s.enqueued:
            continue

        # If it already finished by the time we see it, ignore.
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            pass

        s.wait_q[p.priority].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_service_tick": s.ticks}

    # --- Process results: adaptive hints & failure handling ---
    for r in results:
        ops = getattr(r, "ops", []) or []
        failed = (hasattr(r, "failed") and r.failed())
        err = getattr(r, "error", None)

        for op in ops:
            opk = _op_key(op)
            key = s.op_to_key.get(opk)
            if key is None:
                # Best-effort attribution fallback: (pipeline_id, op_key) may not be known.
                continue
            pid, _ = key

            hint = s.op_hint.get((pid, opk))
            if hint is None:
                hint = {"ram": None, "cpu": None, "fails": 0}

            if failed:
                hint["fails"] = int(hint.get("fails") or 0) + 1

                if _is_oom_error(err):
                    prev = float(hint.get("ram") or 0.0)
                    alloc = float(getattr(r, "ram", 0.0) or 0.0)
                    base = alloc if alloc > 0 else (prev if prev > 0 else 1.0)
                    # Fast convergence: double and add a small floor.
                    new_ram = max(prev, base * 2.0)
                    hint["ram"] = new_ram
                elif _is_timeout_error(err):
                    prev = float(hint.get("cpu") or 0.0)
                    alloc = float(getattr(r, "cpu", 0.0) or 0.0)
                    base = alloc if alloc > 0 else (prev if prev > 0 else 1.0)
                    new_cpu = max(prev, base * 1.7)
                    hint["cpu"] = new_cpu
                else:
                    # Unknown failure: small CPU bump, but stricter retry cap.
                    prev = float(hint.get("cpu") or 0.0)
                    alloc = float(getattr(r, "cpu", 0.0) or 0.0)
                    base = alloc if alloc > 0 else (prev if prev > 0 else 1.0)
                    hint["cpu"] = max(prev, base * 1.25)

                s.op_hint[(pid, opk)] = hint

                # Give up conditions (avoid infinite retries on bad ops).
                fails = int(hint.get("fails") or 0)
                if _is_oom_error(err) or _is_timeout_error(err):
                    if fails >= s.max_fails_per_op:
                        _mark_dead(pid)
                else:
                    if fails >= s.max_nonoom_fails_per_op:
                        _mark_dead(pid)
            else:
                # Success: reset failure counter.
                hint["fails"] = 0
                s.op_hint[(pid, opk)] = hint

    _ensure_pool_state()

    # Build dynamic weighted RR list once per tick.
    rr_list = _build_rr_list()
    rr_len = len(rr_list) if rr_list else 1

    suspensions = []
    assignments = []

    # Per-tick per-pipeline scheduling caps.
    scheduled_this_tick = {}

    # Helper to pick a runnable op from a priority queue for a given pool.
    def _pick_op_from_priority(pri, pool, avail_cpu, avail_ram):
        q = s.wait_q[pri]
        if not q:
            return None

        n = len(q)
        if n == 0:
            return None

        limit = min(n, int(s.scan_limit[pri]))
        for _ in range(limit):
            idx = s.qpos[pri] % n
            s.qpos[pri] += 1
            p = q[idx]
            if p is None:
                continue

            pid = p.pipeline_id

            # Drop dead pipelines eagerly.
            if pid in s.dead_pipelines:
                q[idx] = None
                s.none_count[pri] += 1
                continue

            # If completed, drop it.
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    q[idx] = None
                    s.none_count[pri] += 1
                    _drop_pipeline(pid)
                    continue
            except Exception:
                # If runtime status is inaccessible, keep it for later.
                continue

            # Per-pipeline cap this tick (helps fairness and avoids one DAG dominating).
            cap = int(s.per_pipeline_tick_cap.get(pri, 1))
            if scheduled_this_tick.get(pid, 0) >= cap:
                continue

            # Find runnable ops.
            try:
                ops_ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops_ready = []
            if not ops_ready:
                continue

            op = ops_ready[0]

            cpu_req, ram_req = _size_request(pool, pri, pid, op)

            # RAM must fit (avoid deliberate under-allocation that causes OOM churn).
            if ram_req > avail_ram:
                continue

            # CPU can be shrunk to use leftovers (won't OOM; might run slower).
            if cpu_req > avail_cpu:
                cpu_req = int(avail_cpu)

            if cpu_req < 1 or ram_req < 1:
                continue

            # If we have a known RAM hint larger than availability, don't place here now.
            opk = _op_key(op)
            hint = s.op_hint.get((pid, opk))
            if hint is not None:
                hr = hint.get("ram")
                if hr is not None and float(hr) > float(avail_ram):
                    continue

            return p, op, cpu_req, ram_req

        _compact_queue(pri)
        return None

    # --- Main scheduling: work-conserving weighted fairness + urgency boosts ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu < 1 or avail_ram < 1:
            continue

        # Urgency: try to schedule at least one INTERACTIVE first if backlog exists.
        if _queue_len(Priority.INTERACTIVE) > 0 and avail_cpu >= 1 and avail_ram >= 1:
            picked = _pick_op_from_priority(Priority.INTERACTIVE, pool, avail_cpu, avail_ram)
            if picked is not None:
                p, op, cpu_req, ram_req = picked
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                opk = _op_key(op)
                s.op_to_key[opk] = (p.pipeline_id, opk)
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_this_tick[p.pipeline_id] = scheduled_this_tick.get(p.pipeline_id, 0) + 1
                meta = s.pipeline_meta.get(p.pipeline_id)
                if meta is not None:
                    meta["last_service_tick"] = s.ticks

        # Fill remaining capacity using weighted RR; if a chosen priority can't place, try others.
        idle_spins = 0
        max_idle_spins = rr_len + 8

        while avail_cpu >= 1 and avail_ram >= 1 and idle_spins < max_idle_spins:
            rr_i = s.pool_rr_idx[pool_id] % rr_len
            s.pool_rr_idx[pool_id] += 1

            # If INTERACTIVE is urgent, override the RR choice frequently.
            if _queue_len(Priority.INTERACTIVE) > 0 and _estimate_oldest_wait(Priority.INTERACTIVE) >= s.interactive_urgent_ticks:
                primary = Priority.INTERACTIVE
            else:
                primary = rr_list[rr_i]

            # Try primary, then fall back in descending importance.
            tried = [primary]
            if primary != Priority.INTERACTIVE:
                tried.append(Priority.INTERACTIVE)
            if primary != Priority.QUERY:
                tried.append(Priority.QUERY)
            tried.append(Priority.BATCH_PIPELINE)

            scheduled = False
            for pri in tried:
                if _queue_len(pri) <= 0:
                    continue
                picked = _pick_op_from_priority(pri, pool, avail_cpu, avail_ram)
                if picked is None:
                    continue

                p, op, cpu_req, ram_req = picked
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                opk = _op_key(op)
                s.op_to_key[opk] = (p.pipeline_id, opk)

                avail_cpu -= cpu_req
                avail_ram -= ram_req

                scheduled_this_tick[p.pipeline_id] = scheduled_this_tick.get(p.pipeline_id, 0) + 1
                meta = s.pipeline_meta.get(p.pipeline_id)
                if meta is not None:
                    meta["last_service_tick"] = s.ticks

                scheduled = True
                idle_spins = 0
                break

            if not scheduled:
                idle_spins += 1

    # Compact occasionally.
    _compact_queue(Priority.QUERY)
    _compact_queue(Priority.INTERACTIVE)
    _compact_queue(Priority.BATCH_PIPELINE)

    return suspensions, assignments
