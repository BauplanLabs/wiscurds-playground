@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()
    s.completed_pipelines = set()

    # {pipeline_id: {"enqueued_tick": int, "last_scheduled_tick": int}}
    s.pipeline_meta = {}

    # Resource hints learned from failures/successes.
    # Keys use (pipeline_id, op_key) where op_key is id(op) for stability within a run.
    s.op_ram_hint = {}   # (pid, opk) -> ram
    s.op_cpu_hint = {}   # (pid, opk) -> cpu
    s.op_fail_cnt = {}   # (pid, opk) -> int

    # Map op_key -> pipeline_id for attributing results.
    s.op_to_pipeline = {}

    # Round-robin cursors per priority queue.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Periodic cleanup to drop completed pipelines from queues.
    s.cleanup_every = 29

    # Sizing knobs (chosen to maximize packing / throughput on memory-bound workloads).
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.max_cpu = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 8,
    }
    s.ram_mult = {
        Priority.QUERY: 1.10,
        Priority.INTERACTIVE: 1.05,
        Priority.BATCH_PIPELINE: 1.00,
    }
    s.ram_safety = 1.15  # multiply known min-ram by this safety factor

    # Per-pool CPU budget weights (service fairness). Query still gets strict priority within its budget.
    # Bias towards BATCH to avoid mass timeouts on batch-heavy traces.
    s.service_w = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 10,
    }

    # Search limits per pick (avoid O(N^2) under huge pools).
    s.scan_limit = {
        Priority.QUERY: 64,
        Priority.INTERACTIVE: 96,
        Priority.BATCH_PIPELINE: 192,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.completed_pipelines.add(pid)

    def _pipeline_age(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _maybe_cleanup():
        if s.ticks % s.cleanup_every != 0:
            return
        for pri in _prio_order():
            q = s.wait_q[pri]
            if not q:
                continue
            new_q = []
            for p in q:
                pid = p.pipeline_id
                if pid in s.completed_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
                new_q.append(p)
            s.wait_q[pri] = new_q
            n = len(new_q)
            if n > 0:
                s.rr_cursor[pri] = s.rr_cursor[pri] % n
            else:
                s.rr_cursor[pri] = 0

    def _get_op_min_ram(pool, op):
        # Try common attribute names; normalize if value looks like bytes or MB.
        candidates = [
            "ram_min", "min_ram", "min_ram_gb",
            "memory_min", "min_memory", "min_memory_gb",
            "mem_min", "min_mem", "min_rss_gb",
            "required_ram", "ram_required",
            "peak_ram", "peak_ram_gb",
        ]
        val = None
        for name in candidates:
            if hasattr(op, name):
                v = getattr(op, name)
                try:
                    if v is not None and float(v) > 0:
                        val = float(v)
                        break
                except Exception:
                    pass
        if val is None:
            return None

        # Heuristic unit normalization (only if it is wildly larger than pool capacity).
        try:
            max_ram = float(pool.max_ram_pool)
            if max_ram > 0 and val > max_ram * 8 and val > 1024:
                # bytes?
                if val > (1024.0 ** 3):
                    val = val / (1024.0 ** 3)
                else:
                    # MB -> GB
                    val = val / 1024.0
        except Exception:
            pass

        if val <= 0:
            return None
        return val

    def _get_op_min_cpu(op):
        candidates = ["cpu_min", "min_cpu", "required_cpu", "cpu_required"]
        for name in candidates:
            if hasattr(op, name):
                v = getattr(op, name)
                try:
                    if v is not None and float(v) > 0:
                        return float(v)
                except Exception:
                    pass
        return None

    def _remaining_ops_count(p):
        st = p.runtime_status()
        try:
            states = {
                OperatorState.PENDING,
                OperatorState.ASSIGNED,
                OperatorState.RUNNING,
                OperatorState.SUSPENDING,
                OperatorState.FAILED,
            }
            ops = st.get_ops(states, require_parents_complete=False) or []
            return len(ops)
        except Exception:
            return 0

    def _fit_request(pool, pri, pid, op, avail_cpu, avail_ram):
        # Desired CPU (bounded, and can be reduced to fit).
        opk = _op_key(op)
        cpu_hint = s.op_cpu_hint.get((pid, opk))
        desired_cpu = cpu_hint if (cpu_hint is not None and cpu_hint > 0) else s.base_cpu[pri]
        min_cpu_attr = _get_op_min_cpu(op)
        if min_cpu_attr is not None and min_cpu_attr > 0:
            desired_cpu = max(desired_cpu, min_cpu_attr)

        desired_cpu = min(desired_cpu, s.max_cpu[pri], pool.max_cpu_pool)
        if desired_cpu < 1:
            desired_cpu = 1

        # RAM target: max(min_ram*safety, hint, cpu * ram_per_cpu * mult)
        ram_hint = s.op_ram_hint.get((pid, opk))
        min_ram = _get_op_min_ram(pool, op)
        if min_ram is not None:
            min_ram = min_ram * s.ram_safety

        ram_per_cpu = 0.0
        try:
            if pool.max_cpu_pool > 0:
                ram_per_cpu = float(pool.max_ram_pool) / float(pool.max_cpu_pool)
        except Exception:
            ram_per_cpu = 0.0

        mult = s.ram_mult[pri]
        # Try to fit by reducing CPU (which reduces the cpu-proportional component of RAM).
        cpu_cap = int(min(desired_cpu, avail_cpu, pool.max_cpu_pool))
        if cpu_cap < 1:
            return None

        cpu_try = cpu_cap
        while cpu_try >= 1:
            ram_req = cpu_try * ram_per_cpu * mult if ram_per_cpu > 0 else 1.0
            if min_ram is not None:
                ram_req = max(ram_req, min_ram)
            if ram_hint is not None:
                ram_req = max(ram_req, float(ram_hint))

            if ram_req < 1:
                ram_req = 1.0
            if ram_req > pool.max_ram_pool:
                ram_req = float(pool.max_ram_pool)

            # CPU can be clipped to availability; RAM must fit.
            if ram_req <= avail_ram and cpu_try <= avail_cpu:
                return int(max(1, cpu_try)), float(ram_req)

            # If RAM doesn't fit and min_ram/hint dominate, lowering CPU won't help much; still try a few steps.
            if cpu_try > 8:
                cpu_try -= 2
            else:
                cpu_try -= 1

        return None

    def _pick_from_priority(pri, pool, avail_cpu, avail_ram, scheduled_pids):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None

        start = s.rr_cursor[pri] % n if n > 0 else 0
        limit = s.scan_limit.get(pri, 128)
        scanned = 0

        best = None  # (score, idx, pipeline, op, cpu, ram)
        while scanned < n and scanned < limit:
            idx = (start + scanned) % n
            p = q[idx]
            pid = p.pipeline_id

            if pid in s.completed_pipelines:
                scanned += 1
                continue
            if pid in scheduled_pids:
                scanned += 1
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Leave for periodic cleanup; avoid churn here.
                scanned += 1
                continue

            ops_ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not ops_ready:
                scanned += 1
                continue

            op = ops_ready[0]
            fit = _fit_request(pool, pri, pid, op, avail_cpu, avail_ram)
            if fit is None:
                scanned += 1
                continue

            cpu_req, ram_req = fit
            age = _pipeline_age(pid)
            rem = _remaining_ops_count(p)

            # Scoring:
            # - prefer older pipelines to avoid timeouts
            # - prefer smaller remaining op-count to increase completion count
            # - for batch, slightly prefer larger RAM ops to avoid "big op" starvation
            rem_bonus = 1.0 / float(1 + max(0, rem))
            age_term = float(age)

            if pri == Priority.BATCH_PIPELINE:
                ram_term = float(ram_req) / float(pool.max_ram_pool) if pool.max_ram_pool > 0 else 0.0
                score = (age_term * 6.0) + (rem_bonus * 4.0) + (ram_term * 2.0)
            else:
                score = (age_term * 7.0) + (rem_bonus * 6.0)

            # Small bonus if it recently OOM'd (retry promptly with higher RAM).
            opk = _op_key(op)
            fails = s.op_fail_cnt.get((pid, opk), 0)
            if fails > 0:
                score += min(3.0, 0.5 * float(fails))

            if best is None or score > best[0]:
                best = (score, idx, p, op, cpu_req, ram_req)

            scanned += 1

        if best is None:
            return None

        _, idx, p, op, cpu_req, ram_req = best
        # Advance cursor past the chosen entry for RR fairness.
        if len(q) > 0:
            s.rr_cursor[pri] = (idx + 1) % len(q)
        return p, op, cpu_req, ram_req

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.completed_pipelines:
            continue
        if pid in s.enqueued:
            continue
        pri = p.priority
        if pri not in s.wait_q:
            pri = Priority.BATCH_PIPELINE
        s.wait_q[pri].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_scheduled_tick": -1}

    # --- Process results: learn RAM/CPU hints from failures ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue

            # Remove mapping once we see a terminal result for this container/op.
            # (If the simulator reports intermediate states, this is still safe.)
            s.op_to_pipeline.pop(opk, None)

            if failed:
                s.op_fail_cnt[(pid, opk)] = s.op_fail_cnt.get((pid, opk), 0) + 1
                err = getattr(r, "error", None)

                if _is_oom_error(err):
                    prev = s.op_ram_hint.get((pid, opk))
                    alloc = getattr(r, "ram", None)
                    base = None
                    try:
                        if alloc is not None and float(alloc) > 0:
                            base = float(alloc)
                    except Exception:
                        base = None
                    if base is None:
                        base = float(prev) if (prev is not None and prev > 0) else 1.0

                    # Aggressive but not insane: 1.7x growth + a small additive bump.
                    new_hint = (base * 1.7) + 4.0
                    if prev is not None and prev > 0:
                        new_hint = max(new_hint, float(prev) * 1.4 + 2.0)
                    s.op_ram_hint[(pid, opk)] = new_hint
                else:
                    # Non-OOM failures: gently increase CPU hint to reduce wall time.
                    prev_cpu = s.op_cpu_hint.get((pid, opk))
                    alloc_cpu = getattr(r, "cpu", None)
                    base_cpu = None
                    try:
                        if alloc_cpu is not None and float(alloc_cpu) > 0:
                            base_cpu = float(alloc_cpu)
                    except Exception:
                        base_cpu = None
                    if base_cpu is None:
                        base_cpu = float(prev_cpu) if (prev_cpu is not None and prev_cpu > 0) else 1.0
                    s.op_cpu_hint[(pid, opk)] = min(base_cpu + 1.0, 32.0)

    _maybe_cleanup()

    suspensions = []
    assignments = []

    # Fast exit.
    if not s.wait_q[Priority.QUERY] and not s.wait_q[Priority.INTERACTIVE] and not s.wait_q[Priority.BATCH_PIPELINE]:
        return suspensions, assignments

    # Avoid multiple ops from the same pipeline in a single scheduler step.
    scheduled_pids = set()

    # Pool ordering: try to schedule on the most free pools first (helps large ops fit).
    pool_ids = list(range(s.executor.num_pools))
    try:
        pool_ids.sort(
            key=lambda i: (s.executor.pools[i].avail_ram_pool + s.executor.pools[i].avail_cpu_pool),
            reverse=True,
        )
    except Exception:
        pass

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1 or avail_ram < 1:
            continue

        # Pool role: if multiple pools, bias pool 0 to serve QUERY/INTERACTIVE first (helps latency),
        # but don't fully forbid batch (keeps throughput high).
        role_bias = None
        if s.executor.num_pools > 1 and pool_id == 0:
            role_bias = "high"

        init_cpu = avail_cpu
        nonempty = [pri for pri in _prio_order() if len(s.wait_q[pri]) > 0]
        if not nonempty:
            continue

        # Compute per-priority CPU budgets for this scheduling step.
        w_sum = 0.0
        eff_w = {}
        for pri in nonempty:
            w = float(s.service_w.get(pri, 1))
            # If pool 0 is "high" role, slightly bias away from batch while keeping it non-zero.
            if role_bias == "high" and pri == Priority.BATCH_PIPELINE:
                w *= 0.60
            eff_w[pri] = max(0.1, w)
            w_sum += eff_w[pri]

        cpu_budget = {pri: (init_cpu * eff_w.get(pri, 0.0) / w_sum) for pri in _prio_order()}
        cpu_used = {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        blocked = set()

        while avail_cpu >= 1 and avail_ram >= 1:
            # Choose next priority: first try those under budget, then any available, in strict order.
            chosen_pri = None
            for pri in _prio_order():
                if pri in blocked:
                    continue
                if len(s.wait_q[pri]) == 0:
                    continue
                if cpu_used.get(pri, 0.0) + 0.5 <= cpu_budget.get(pri, 0.0):
                    chosen_pri = pri
                    break

            if chosen_pri is None:
                for pri in _prio_order():
                    if pri in blocked:
                        continue
                    if len(s.wait_q[pri]) == 0:
                        continue
                    chosen_pri = pri
                    break

            if chosen_pri is None:
                break

            picked = _pick_from_priority(chosen_pri, pool, avail_cpu, avail_ram, scheduled_pids)
            if picked is None:
                blocked.add(chosen_pri)
                if len(blocked) >= 3:
                    break
                continue

            p, op, cpu_req, ram_req = picked

            # Defensive clamps.
            if cpu_req < 1:
                cpu_req = 1
            if ram_req < 1:
                ram_req = 1.0
            if cpu_req > avail_cpu or ram_req > avail_ram:
                blocked.add(chosen_pri)
                if len(blocked) >= 3:
                    break
                continue

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

            # Track mapping for attribution on completion/failure.
            s.op_to_pipeline[_op_key(op)] = p.pipeline_id

            # Update accounting.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            cpu_used[chosen_pri] = cpu_used.get(chosen_pri, 0.0) + float(cpu_req)
            scheduled_pids.add(p.pipeline_id)

            meta = s.pipeline_meta.get(p.pipeline_id)
            if meta is not None:
                meta["last_scheduled_tick"] = s.ticks

    return suspensions, assignments
