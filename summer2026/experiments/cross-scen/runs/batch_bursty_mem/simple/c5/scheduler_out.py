@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on completion rate and throughput under bursty,
    memory-bound workloads, while still protecting QUERY/INTERACTIVE latency.

    Key changes vs prior version:
      - Avoid allocating CPU/RAM as large fractions of the whole pool (which killed concurrency).
      - Use small, per-op CPU allocations with RAM sized from (hint -> op min -> conservative default).
      - Fast OOM convergence: retry with higher RAM for the specific (pipeline, op).
      - Headroom (single-pool) or soft pool-partitioning (multi-pool) so batch can't block interactive.
      - Lightweight queue cleanup to remove completed pipelines.
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}

    # Resource hints learned from OOMs (per operator instance)
    # key: (pipeline_id, op_key) -> ram_amount
    s.op_ram_hint = {}

    # Map op_key -> pipeline_id for attributing results
    s.op_to_pipeline = {}

    # Failure counters (avoid pathological endless non-OOM retries if they exist)
    s.op_fail_counts = {}        # (pipeline_id, op_key) -> int
    s.pipeline_fail_counts = {}  # pipeline_id -> int

    # Tick-local fairness
    s.clean_every = 37

    # Mix pattern for non-query work when both queues have demand
    # (favor batch enough to complete the heavy batch mix, but always give interactive slots)
    s.mix_pattern = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE, Priority.BATCH_PIPELINE]
    s.mix_idx = 0

    # Sizing knobs (in "pool RAM units", typically GB)
    s.ram_margin = 1.20
    s.oom_ram_mult = 1.70
    s.max_non_oom_op_retries = 2

    # Conservative defaults when no min RAM is discoverable on the op object
    s.default_ram = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 16.0,
        Priority.BATCH_PIPELINE: 24.0,
    }

    # Hard floors (keep >0 allocations)
    s.ram_floor = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 6.0,
    }

    # Default CPU sizing (small, concurrency-friendly)
    s.base_cpu = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_cap = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 6.0,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _as_pos_float(x):
        try:
            if x is None:
                return None
            if isinstance(x, bool):
                return None
            v = float(x)
            if v > 0:
                return v
        except Exception:
            return None
        return None

    def _ceil(x):
        # no imports; tolerate ints/floats
        try:
            xi = int(x)
            if float(xi) == float(x):
                return xi
            return xi + 1
        except Exception:
            return 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda out of memory" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_fail_counts.pop(pid, None)
        # Keep op hints (helpful if op objects reused), but could be large; prune a bit
        # by removing keys for this pid.
        to_del = []
        for (ppid, opk) in s.op_ram_hint.keys():
            if ppid == pid:
                to_del.append((ppid, opk))
        for k in to_del:
            s.op_ram_hint.pop(k, None)
            s.op_fail_counts.pop(k, None)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _extract_min_ram(op):
        # Try common attribute names (workload generator often stores minima on operator objects).
        cand_names = [
            "min_ram",
            "ram_min",
            "min_ram_gb",
            "ram_min_gb",
            "min_memory",
            "min_memory_gb",
            "memory_min",
            "memory_min_gb",
            "required_ram",
            "required_ram_gb",
            "required_memory",
            "required_memory_gb",
            "peak_ram",
            "peak_ram_gb",
            "ram_required",
            "memory_required",
        ]
        for name in cand_names:
            if hasattr(op, name):
                v = _as_pos_float(getattr(op, name, None))
                if v is not None:
                    return v
        # Some generators store a dict-like "resources"
        if hasattr(op, "resources"):
            res = getattr(op, "resources", None)
            try:
                if isinstance(res, dict):
                    for k in ["min_ram", "ram", "memory", "min_memory", "ram_gb", "memory_gb"]:
                        v = _as_pos_float(res.get(k, None))
                        if v is not None:
                            return v
            except Exception:
                pass
        return None

    def _estimate_cpu(pool, pri, est_ram):
        cpu = _as_pos_float(s.base_cpu.get(pri, 2.0)) or 2.0

        # If RAM need is large, nudge CPU up slightly (still keep concurrency high).
        if est_ram is not None:
            if est_ram >= 64:
                cpu = max(cpu, 3.0)
            if est_ram >= 128:
                cpu = max(cpu, 4.0)
            if est_ram >= 256:
                cpu = max(cpu, 6.0)

        cap = _as_pos_float(s.cpu_cap.get(pri, 6.0)) or 6.0
        cpu = min(cpu, cap)
        cpu = min(cpu, _as_pos_float(pool.max_cpu_pool) or cpu)
        cpu = max(cpu, 1.0)
        return float(_ceil(cpu))

    def _estimate_ram(pool, pri, pid, op, cpu_req):
        opk = _op_key(op)
        hint = s.op_ram_hint.get((pid, opk), None)
        if hint is not None:
            return float(hint)

        min_ram = _extract_min_ram(op)
        if min_ram is not None:
            ram = min_ram * s.ram_margin
        else:
            ram = float(s.default_ram.get(pri, 16.0))

        # Lightly tie RAM to CPU allocation so requests stay coherent across pool sizes.
        max_cpu = _as_pos_float(pool.max_cpu_pool) or 1.0
        max_ram = _as_pos_float(pool.max_ram_pool) or ram
        ram_per_cpu = max_ram / max(1.0, max_cpu)
        # Memory-bound training tends to be RAM-heavier than raw ram_per_cpu; apply mild factor by pri.
        pri_factor = 1.2
        if pri == Priority.QUERY:
            pri_factor = 0.8
        elif pri == Priority.INTERACTIVE:
            pri_factor = 1.1
        else:
            pri_factor = 1.25

        ram = max(ram, ram_per_cpu * float(cpu_req) * pri_factor)

        floor = float(s.ram_floor.get(pri, 2.0))
        ram = max(ram, floor)
        ram = min(ram, max_ram)
        return float(_ceil(ram))

    def _estimate_request(pool, pri, pid, op):
        # Compute cpu first using a preliminary RAM estimate (min/default) then finalize RAM with cpu.
        min_ram = _extract_min_ram(op)
        prelim_ram = None
        if min_ram is not None:
            prelim_ram = min_ram * s.ram_margin
        else:
            prelim_ram = float(s.default_ram.get(pri, 16.0))

        cpu_req = _estimate_cpu(pool, pri, prelim_ram)
        ram_req = _estimate_ram(pool, pri, pid, op, cpu_req)
        # Re-check CPU with final RAM (only ever increases a bit)
        cpu_req2 = _estimate_cpu(pool, pri, ram_req)
        if cpu_req2 != cpu_req:
            cpu_req = cpu_req2
            ram_req = _estimate_ram(pool, pri, pid, op, cpu_req)

        # Clip to pool bounds
        cpu_req = min(cpu_req, float(_as_pos_float(pool.max_cpu_pool) or cpu_req))
        ram_req = min(ram_req, float(_as_pos_float(pool.max_ram_pool) or ram_req))
        cpu_req = max(cpu_req, 1.0)
        ram_req = max(ram_req, 1.0)
        return float(_ceil(cpu_req)), float(_ceil(ram_req))

    def _peek_one_runnable_estimate(q, pool, max_scan):
        n = len(q)
        if n == 0:
            return None
        lim = min(n, max_scan)
        for i in range(lim):
            p = q[i]
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue
                op = ops[0]
                cpu_req, ram_req = _estimate_request(pool, p.priority, p.pipeline_id, op)
                return cpu_req, ram_req
            except Exception:
                continue
        return None

    def _reserve_headroom_for_lower_pri(pool, pool_id, lower_pri):
        # With multiple pools, treat pool 0 as latency pool:
        # keep headroom only there; let other pools run batch work-conserving for throughput.
        if s.executor.num_pools > 1 and pool_id != 0:
            return 0.0, 0.0

        q_query = s.wait_q[Priority.QUERY]
        q_inter = s.wait_q[Priority.INTERACTIVE]

        keep_cpu = 0.0
        keep_ram = 0.0

        if lower_pri == Priority.BATCH_PIPELINE:
            # Keep room for at least 1 QUERY and 1 INTERACTIVE if they exist
            if q_query:
                est = _peek_one_runnable_estimate(q_query, pool, 8)
                if est is None:
                    est = (s.base_cpu[Priority.QUERY], s.default_ram[Priority.QUERY])
                keep_cpu += float(est[0])
                keep_ram += float(est[1])

            if q_inter:
                est = _peek_one_runnable_estimate(q_inter, pool, 10)
                if est is None:
                    est = (s.base_cpu[Priority.INTERACTIVE], s.default_ram[Priority.INTERACTIVE])
                keep_cpu += float(est[0])
                keep_ram += float(est[1])

        elif lower_pri == Priority.INTERACTIVE:
            # Keep room for at least 1 QUERY if any exist
            if q_query:
                est = _peek_one_runnable_estimate(q_query, pool, 8)
                if est is None:
                    est = (s.base_cpu[Priority.QUERY], s.default_ram[Priority.QUERY])
                keep_cpu += float(est[0])
                keep_ram += float(est[1])

        # Don't reserve more than pool max (avoid negative effective capacity)
        keep_cpu = min(keep_cpu, float(_as_pos_float(pool.max_cpu_pool) or keep_cpu))
        keep_ram = min(keep_ram, float(_as_pos_float(pool.max_ram_pool) or keep_ram))
        return keep_cpu, keep_ram

    def _queue_cleanup():
        # Remove successful pipelines lingering in queues, occasionally.
        for pri in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
            q = s.wait_q[pri]
            if not q:
                continue
            newq = []
            for p in q:
                try:
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        _drop_pipeline(p.pipeline_id)
                        continue
                except Exception:
                    pass
                newq.append(p)
            s.wait_q[pri] = newq

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}

    # --- Process results (OOM-aware RAM bumps; bounded non-OOM retry tracking) ---
    for r in results:
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            failed = False
            try:
                if hasattr(r, "failed") and r.failed():
                    failed = True
            except Exception:
                failed = False

            if not failed:
                # Success: reset per-op non-OOM failure count
                s.op_fail_counts.pop((pid, opk), None)
                continue

            # Any failure increments pipeline failure count (for diagnostics / mild damping)
            s.pipeline_fail_counts[pid] = s.pipeline_fail_counts.get(pid, 0) + 1

            err = getattr(r, "error", None)
            if _is_oom_error(err):
                prev = s.op_ram_hint.get((pid, opk), None)
                alloc = _as_pos_float(getattr(r, "ram", None))
                base = alloc if alloc is not None else (prev if prev is not None else None)

                # If we can extract a min RAM from the op, use it as a lower bound too.
                min_ram = _extract_min_ram(op)
                min_based = (min_ram * s.ram_margin) if min_ram is not None else None

                candidates = []
                if prev is not None:
                    candidates.append(prev * s.oom_ram_mult)
                if base is not None:
                    candidates.append(base * s.oom_ram_mult)
                if min_based is not None:
                    candidates.append(min_based * 2.0)

                new_hint = max(candidates) if candidates else float(s.default_ram.get(Priority.BATCH_PIPELINE, 24.0)) * 2.0

                # Cap at pool max if known from result (pool_id), else leave uncapped for later clipping.
                pool_id = getattr(r, "pool_id", None)
                if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                    pool = s.executor.pools[int(pool_id)]
                    max_ram = _as_pos_float(pool.max_ram_pool)
                    if max_ram is not None:
                        new_hint = min(new_hint, max_ram)

                s.op_ram_hint[(pid, opk)] = float(_ceil(new_hint))
                # Reset non-OOM retry counter on OOM (we're actively adjusting)
                s.op_fail_counts.pop((pid, opk), None)
            else:
                # Track non-OOM retry count but do NOT drop pipelines aggressively (completion matters).
                k = (pid, opk)
                s.op_fail_counts[k] = s.op_fail_counts.get(k, 0) + 1

    if s.ticks % s.clean_every == 0:
        _queue_cleanup()

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # One-op-per-pipeline-per-tick (safe vs duplicate assignment when status doesn't update mid-tick)
    scheduled_pipeline_ids = set()

    def _pick_next_op_from_queue(q, pool, pool_id, pri, avail_cpu, avail_ram):
        if not q:
            return None, None, None, None

        keep_cpu, keep_ram = _reserve_headroom_for_lower_pri(pool, pool_id, pri)
        eff_cpu = max(0.0, float(avail_cpu) - keep_cpu) if pri != Priority.QUERY else float(avail_cpu)
        eff_ram = max(0.0, float(avail_ram) - keep_ram) if pri != Priority.QUERY else float(avail_ram)

        if eff_cpu < 1.0 or eff_ram < 1.0:
            return None, None, None, None

        n = len(q)
        best_idx = None
        best = None  # (age, p, op, cpu_req, ram_req) pick older if multiple fit

        # Scan a limited window for efficiency; queues can be large (especially batch).
        scan_lim = n
        if pri == Priority.BATCH_PIPELINE:
            scan_lim = min(n, 60)
        elif pri == Priority.INTERACTIVE:
            scan_lim = min(n, 40)
        else:
            scan_lim = min(n, 25)

        for i in range(scan_lim):
            p = q[i]
            pid = p.pipeline_id

            if pid in scheduled_pipeline_ids:
                continue

            try:
                st = p.runtime_status()
            except Exception:
                continue

            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            cpu_req, ram_req = _estimate_request(pool, pri, pid, op)

            # Respect effective availability (keeping headroom for higher priority).
            if cpu_req > eff_cpu or ram_req > eff_ram:
                continue

            # If this op has accumulated non-OOM failures, give it a slightly larger CPU share.
            opk = _op_key(op)
            non_oom_fails = s.op_fail_counts.get((pid, opk), 0)
            if non_oom_fails > 0:
                cpu_req = min(float(_ceil(cpu_req + non_oom_fails)), float(_as_pos_float(pool.max_cpu_pool) or cpu_req))
                if cpu_req > eff_cpu:
                    continue

            age = _pipeline_wait_ticks(pid)
            cand = (age, p, op, cpu_req, ram_req)
            if best is None or cand[0] > best[0]:
                best = cand
                best_idx = i

        if best is None:
            # Rotate one element to avoid being stuck on unschedulable head forever.
            q.append(q.pop(0))
            return None, None, None, None

        # Rotate: move the chosen pipeline to the back for round-robin fairness.
        try:
            chosen = q.pop(best_idx)
            q.append(chosen)
        except Exception:
            pass

        _, p, op, cpu_req, ram_req = best
        return p, op, float(_ceil(cpu_req)), float(_ceil(ram_req))

    # --- Main scheduling loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Pool role:
        #  - If multiple pools: pool 0 prioritizes latency (QUERY/INTERACTIVE); others prioritize throughput (BATCH).
        #  - If single pool: use headroom reservations to prevent batch blocking interactive/query.
        is_latency_pool = (pool_id == 0) if s.executor.num_pools > 1 else True

        # First, always try to place QUERY work as soon as it can fit (especially in latency pool).
        # In non-latency pools we still allow queries if latency pool is saturated.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            q_query = s.wait_q[Priority.QUERY]
            if not q_query:
                break
            p, op, cpu_req, ram_req = _pick_next_op_from_queue(q_query, pool, pool_id, Priority.QUERY, avail_cpu, avail_ram)
            if p is None:
                break
            assignment = Assignment(
                ops=[op],
                cpu=min(cpu_req, avail_cpu),
                ram=min(ram_req, avail_ram),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            s.op_to_pipeline[_op_key(op)] = p.pipeline_id
            avail_cpu -= float(assignment.cpu)
            avail_ram -= float(assignment.ram)
            scheduled_pipeline_ids.add(p.pipeline_id)

        # Then schedule INTERACTIVE and BATCH.
        # Latency pool favors interactive; throughput pools favor batch but will help interactive if it fits.
        idle_guard = 0
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            idle_guard += 1
            if idle_guard > 5000:
                break

            if is_latency_pool:
                pri_choices = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                # Mostly batch, but still provide interactive opportunities if backlog exists.
                pri_choices = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

            picked_any = False

            # Decide which non-query priority to try first using a simple global pattern when both have demand.
            q_int = s.wait_q[Priority.INTERACTIVE]
            q_bat = s.wait_q[Priority.BATCH_PIPELINE]
            if q_int and q_bat:
                pri_first = s.mix_pattern[s.mix_idx % len(s.mix_pattern)]
                s.mix_idx += 1
                pri_order = [pri_first, Priority.BATCH_PIPELINE if pri_first == Priority.INTERACTIVE else Priority.INTERACTIVE]
            else:
                pri_order = pri_choices

            for pri in pri_order:
                q = _queue_for_priority(pri)
                if not q:
                    continue

                p, op, cpu_req, ram_req = _pick_next_op_from_queue(q, pool, pool_id, pri, avail_cpu, avail_ram)
                if p is None:
                    continue

                # Final clip to availability
                cpu_req = min(cpu_req, avail_cpu)
                ram_req = min(ram_req, avail_ram)
                if cpu_req < 1.0 or ram_req < 1.0:
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=float(_ceil(cpu_req)),
                    ram=float(_ceil(ram_req)),
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)
                s.op_to_pipeline[_op_key(op)] = p.pipeline_id
                avail_cpu -= float(assignment.cpu)
                avail_ram -= float(assignment.ram)
                scheduled_pipeline_ids.add(p.pipeline_id)

                picked_any = True
                break

            if not picked_any:
                break

    return suspensions, assignments
