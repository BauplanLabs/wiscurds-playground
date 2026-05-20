@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority circular queues (store Pipeline objects; entries may be set to None and compacted later).
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

    # Pipeline bookkeeping
    s.enqueued = set()  # pipeline_id present in queues
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}
    s.completed_pipelines = set()
    s.dead_pipelines = set()

    # Operator-level learning / retry control (keyed by (pipeline_id, stable_op_key))
    s.op_ram_hint = {}        # desired RAM allocation next try
    s.op_oom_floor = {}       # largest RAM that has OOM'd (lower bound)
    s.op_oom_retries = {}     # count
    s.op_other_retries = {}   # count

    # Pipeline-level RAM signal (if any op OOMs big, start future ops a bit higher)
    s.pipeline_ram_floor = {}  # pipeline_id -> oom_floor (ram)

    # Map runtime op identity (based on object id) to (pipeline_id, stable_op_key)
    s.op_runtime_map = {}
    s.op_last_pool = {}  # (pipeline_id, stable_op_key) -> pool_id of last assignment

    # Limits (bounded retry depth)
    s.max_oom_retries = 6
    s.max_other_retries = 3

    # Queue maintenance
    s.compact_every = 25  # ticks
    s.max_scan_per_pick = 128  # cap queue scans per pick to avoid worst-case O(n^2)


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # Include common kill signals that typically indicate OOM in container contexts.
        return (
            ("oom" in msg)
            or ("out of memory" in msg)
            or ("memoryerror" in msg)
            or ("oomkilled" in msg)
            or ("killed" in msg and "oom" in msg)
        )

    def _op_runtime_key(op):
        # Must match between assignment-time and result-time; object identity is safest.
        return ("pyid", id(op))

    def _op_stable_key(op):
        # Prefer stable identifiers if present; else fall back to object identity.
        for attr in ("op_id", "operator_id", "node_id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return (attr, v)
        return ("pyid", id(op))

    def _mark_pipeline_completed(pid):
        s.completed_pipelines.add(pid)
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _mark_pipeline_dead(pid):
        s.dead_pipelines.add(pid)
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _active_queue_len(pri):
        q = s.wait_q[pri]
        n = 0
        for p in q:
            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.completed_pipelines or pid in s.dead_pipelines:
                continue
            n += 1
        return n

    def _maybe_compact_queues(force=False):
        if not force and (s.ticks % s.compact_every != 0):
            return
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                s.q_cursor[pri] = 0
                continue
            # Compact if many None or dead/completed entries.
            live = []
            for p in q:
                if p is None:
                    continue
                pid = p.pipeline_id
                if pid in s.completed_pipelines or pid in s.dead_pipelines:
                    continue
                live.append(p)
            s.wait_q[pri] = live
            s.q_cursor[pri] = 0

    def _compute_cpu_shares():
        # Backlog-aware shares: objective weights * sqrt(backlog) to keep interactive flowing.
        base_w = {
            Priority.QUERY: 10.0,
            Priority.INTERACTIVE: 5.0,
            Priority.BATCH_PIPELINE: 1.0,
        }
        bq = _active_queue_len(Priority.QUERY)
        bi = _active_queue_len(Priority.INTERACTIVE)
        bb = _active_queue_len(Priority.BATCH_PIPELINE)

        eff = {
            Priority.QUERY: base_w[Priority.QUERY] * ((bq + 1.0) ** 0.5),
            Priority.INTERACTIVE: base_w[Priority.INTERACTIVE] * ((bi + 1.0) ** 0.5),
            Priority.BATCH_PIPELINE: base_w[Priority.BATCH_PIPELINE] * ((bb + 1.0) ** 0.5),
        }
        total = eff[Priority.QUERY] + eff[Priority.INTERACTIVE] + eff[Priority.BATCH_PIPELINE]
        if total <= 0:
            return {Priority.QUERY: 0.6, Priority.INTERACTIVE: 0.3, Priority.BATCH_PIPELINE: 0.1}

        share = {
            Priority.QUERY: eff[Priority.QUERY] / total,
            Priority.INTERACTIVE: eff[Priority.INTERACTIVE] / total,
            Priority.BATCH_PIPELINE: eff[Priority.BATCH_PIPELINE] / total,
        }

        # Floors to avoid starvation (only if there is backlog).
        floors = {
            Priority.QUERY: 0.20 if bq > 0 else 0.0,
            Priority.INTERACTIVE: 0.20 if bi > 0 else 0.0,
            Priority.BATCH_PIPELINE: 0.08 if bb > 0 else 0.0,
        }

        # Apply floors, then renormalize the remainder.
        used = 0.0
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if share[pri] < floors[pri]:
                share[pri] = floors[pri]
            used += share[pri]

        if used <= 0:
            return {Priority.QUERY: 0.6, Priority.INTERACTIVE: 0.3, Priority.BATCH_PIPELINE: 0.1}

        # If floors pushed sum above 1, renormalize down.
        if used > 1.0:
            for pri in share:
                share[pri] = share[pri] / used
            return share

        # Otherwise, distribute remaining slack proportionally to original share mass above floors.
        slack = 1.0 - used
        rem_mass = 0.0
        orig = {
            Priority.QUERY: eff[Priority.QUERY],
            Priority.INTERACTIVE: eff[Priority.INTERACTIVE],
            Priority.BATCH_PIPELINE: eff[Priority.BATCH_PIPELINE],
        }
        for pri in orig:
            if floors[pri] <= 0 and _active_queue_len(pri) <= 0:
                orig[pri] = 0.0
            rem_mass += orig[pri]
        if rem_mass <= 0:
            return share
        for pri in share:
            share[pri] += slack * (orig[pri] / rem_mass)
        return share

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        # CPU: small, capped, to maximize concurrency and completion.
        # RAM: modest baseline + OOM-driven convergence. Also incorporate pipeline-level floor.
        if pri == Priority.QUERY:
            cpu_base, cpu_cap = 4.0, 8.0
            ram_base = max(8.0, min(16.0, pool.max_ram_pool * 0.015))
        elif pri == Priority.INTERACTIVE:
            cpu_base, cpu_cap = 2.0, 4.0
            ram_base = max(6.0, min(14.0, pool.max_ram_pool * 0.012))
        else:
            cpu_base, cpu_cap = 1.0, 3.0
            ram_base = max(5.0, min(12.0, pool.max_ram_pool * 0.010))

        # If there is plenty of idle CPU and low contention, slightly boost to reduce tail.
        # (Avoid large boosts; throughput and completion matter more here.)
        if avail_cpu >= 16.0 and pri == Priority.QUERY:
            cpu_base = min(cpu_cap, cpu_base + 1.0)
        if avail_cpu >= 24.0 and pri == Priority.INTERACTIVE:
            cpu_base = min(cpu_cap, cpu_base + 1.0)
        if avail_cpu >= 32.0 and pri == Priority.BATCH_PIPELINE:
            cpu_base = min(cpu_cap, cpu_base + 1.0)

        cpu_req = cpu_base
        if cpu_req < 1.0:
            cpu_req = 1.0
        if cpu_req > cpu_cap:
            cpu_req = cpu_cap
        if cpu_req > pool.max_cpu_pool:
            cpu_req = pool.max_cpu_pool

        stable = _op_stable_key(op)

        # RAM hinting
        hint = s.op_ram_hint.get((pid, stable), None)
        oom_floor = s.op_oom_floor.get((pid, stable), 0.0)
        pipe_floor = s.pipeline_ram_floor.get(pid, 0.0)

        if hint is None:
            ram_req = ram_base
            # If pipeline has shown it's memory-heavy, nudge baseline up.
            if pipe_floor > 0.0:
                ram_req = max(ram_req, min(pool.max_ram_pool, pipe_floor * 0.9))
        else:
            ram_req = hint

        if oom_floor > 0.0:
            ram_req = max(ram_req, oom_floor * 1.25 + 1.0)

        if ram_req < 1.0:
            ram_req = 1.0
        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool

        # Fit checks (do not silently shrink below learned hints/floors).
        if cpu_req > avail_cpu:
            # CPU can be shrunk without correctness risk (only runtime).
            cpu_req = max(1.0, avail_cpu)
        if ram_req > avail_ram:
            return None

        return cpu_req, ram_req

    # Queue maintenance occasionally (keeps scans bounded).
    _maybe_compact_queues(force=False)

    # Ingest pipelines (may include previously seen pipelines; avoid duplicates).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.completed_pipelines or pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        s.wait_q[p.priority].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}

    # Process results: update OOM floors/hints and retry accounting.
    # Also avoid blacklisting on first non-OOM error; retry a few times.
    global_max_ram = 0.0
    for i in range(s.executor.num_pools):
        mr = s.executor.pools[i].max_ram_pool
        if mr > global_max_ram:
            global_max_ram = mr

    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            rk = _op_runtime_key(op)
            mapped = s.op_runtime_map.get(rk, None)
            if mapped is None:
                continue
            pid, stable = mapped

            # Keep runtime_map entries until we see a result; then delete to bound growth.
            # (If simulator reuses op objects across results, this is still fine because we re-add on assignment.)
            s.op_runtime_map.pop(rk, None)

            if pid in s.completed_pipelines or pid in s.dead_pipelines:
                continue

            failed = False
            if hasattr(r, "failed"):
                try:
                    failed = r.failed()
                except Exception:
                    failed = False

            if not failed:
                # Success: clear non-OOM retry counters for this op.
                s.op_other_retries.pop((pid, stable), None)
                continue

            err = getattr(r, "error", None)
            alloc_ram = getattr(r, "ram", None)
            try:
                alloc_ram_val = float(alloc_ram) if alloc_ram is not None else 0.0
            except Exception:
                alloc_ram_val = 0.0

            # Determine pool cap if we know where it last ran.
            last_pool = s.op_last_pool.get((pid, stable), None)
            pool_cap = global_max_ram
            if last_pool is not None and 0 <= last_pool < s.executor.num_pools:
                pool_cap = s.executor.pools[last_pool].max_ram_pool

            if _is_oom_error(err):
                # OOM: raise floor and hint, bounded retries.
                prev_floor = s.op_oom_floor.get((pid, stable), 0.0)
                new_floor = max(prev_floor, alloc_ram_val)
                s.op_oom_floor[(pid, stable)] = new_floor
                s.pipeline_ram_floor[pid] = max(s.pipeline_ram_floor.get(pid, 0.0), new_floor)

                prev_hint = s.op_ram_hint.get((pid, stable), 0.0) or 0.0
                base = max(prev_hint, alloc_ram_val, new_floor)
                # Aggressive but not explosive: ~1.7x step.
                new_hint = base * 1.7 + 1.0
                if new_hint > pool_cap:
                    new_hint = pool_cap
                s.op_ram_hint[(pid, stable)] = new_hint

                s.op_oom_retries[(pid, stable)] = s.op_oom_retries.get((pid, stable), 0) + 1
                if s.op_oom_retries[(pid, stable)] > s.max_oom_retries:
                    _mark_pipeline_dead(pid)
            else:
                # Non-OOM: retry a small number of times (do not immediately kill the pipeline).
                s.op_other_retries[(pid, stable)] = s.op_other_retries.get((pid, stable), 0) + 1
                if s.op_other_retries[(pid, stable)] > s.max_other_retries:
                    _mark_pipeline_dead(pid)

    suspensions = []
    assignments = []

    # Tick-local accounting (required by simulator).
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # Limit per-pipeline scheduling per tick (avoid a single pipeline hogging).
    scheduled_counts = {}  # pid -> count
    max_ops_per_pipeline_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    shares = _compute_cpu_shares()

    def _pick_next(pri, pool_id, cpu_budget):
        q = s.wait_q[pri]
        n = len(q)
        if n <= 0:
            return None

        cursor = s.q_cursor[pri] % n if n > 0 else 0
        scanned = 0
        max_scan = min(n, s.max_scan_per_pick)

        while scanned < max_scan and n > 0:
            idx = cursor
            cursor = (cursor + 1) % n
            scanned += 1

            p = q[idx]
            if p is None:
                continue

            pid = p.pipeline_id
            if pid in s.completed_pipelines:
                q[idx] = None
                s.enqueued.discard(pid)
                continue
            if pid in s.dead_pipelines:
                q[idx] = None
                s.enqueued.discard(pid)
                continue

            # Per-pipeline per-tick cap
            cap = max_ops_per_pipeline_tick.get(pri, 1)
            if scheduled_counts.get(pid, 0) >= cap:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                q[idx] = None
                _mark_pipeline_completed(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]

            # Size and fit checks
            pool = s.executor.pools[pool_id]
            sized = _size_request(pool, pri, pid, op, local_cpu[pool_id], local_ram[pool_id])
            if sized is None:
                continue
            cpu_req, ram_req = sized

            # Respect CPU budget in the "quota" phase; allow slight overshoot by <=1 CPU if it helps packing.
            if cpu_budget is not None:
                if cpu_req > cpu_budget and cpu_req > (cpu_budget + 1.0):
                    continue

            # Also avoid scheduling if learned hint cannot fit in this pool (multi-pool case).
            stable = _op_stable_key(op)
            hint = s.op_ram_hint.get((pid, stable), None)
            if hint is not None and hint > s.executor.pools[pool_id].max_ram_pool:
                continue

            s.q_cursor[pri] = cursor
            return idx, p, op, cpu_req, ram_req

        s.q_cursor[pri] = cursor
        return None

    # Schedule per pool: quota pass then work-conserving pass.
    for pool_id in range(s.executor.num_pools):
        if local_cpu[pool_id] < 1.0 or local_ram[pool_id] < 1.0:
            continue

        total_cpu = float(local_cpu[pool_id])

        # CPU budgets by priority for quota pass
        cpu_budget = {
            Priority.QUERY: total_cpu * shares.get(Priority.QUERY, 0.0),
            Priority.INTERACTIVE: total_cpu * shares.get(Priority.INTERACTIVE, 0.0),
            Priority.BATCH_PIPELINE: total_cpu * shares.get(Priority.BATCH_PIPELINE, 0.0),
        }

        # Ensure tiny minimum budget if backlog exists and we have capacity.
        if _active_queue_len(Priority.QUERY) > 0:
            cpu_budget[Priority.QUERY] = max(cpu_budget[Priority.QUERY], 1.0)
        if _active_queue_len(Priority.INTERACTIVE) > 0:
            cpu_budget[Priority.INTERACTIVE] = max(cpu_budget[Priority.INTERACTIVE], 1.0)
        if _active_queue_len(Priority.BATCH_PIPELINE) > 0:
            cpu_budget[Priority.BATCH_PIPELINE] = max(cpu_budget[Priority.BATCH_PIPELINE], 1.0)

        # Quota pass (prevents starvation, especially for interactive).
        progressed = True
        while progressed and local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0:
            progressed = False
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if local_cpu[pool_id] < 1.0 or local_ram[pool_id] < 1.0:
                    break
                if cpu_budget.get(pri, 0.0) < 1.0:
                    continue

                picked = _pick_next(pri, pool_id, cpu_budget[pri])
                if picked is None:
                    continue

                _, p, op, cpu_req, ram_req = picked

                if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                    continue

                pid = p.pipeline_id
                stable = _op_stable_key(op)
                rk = _op_runtime_key(op)
                s.op_runtime_map[rk] = (pid, stable)
                s.op_last_pool[(pid, stable)] = pool_id

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                local_cpu[pool_id] -= cpu_req
                local_ram[pool_id] -= ram_req
                cpu_budget[pri] -= cpu_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                progressed = True

        # Work-conserving pass: fill remaining capacity, preferring higher priority.
        while local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0:
            did = False
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                picked = _pick_next(pri, pool_id, None)
                if picked is None:
                    continue

                _, p, op, cpu_req, ram_req = picked
                if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                    continue

                pid = p.pipeline_id
                stable = _op_stable_key(op)
                rk = _op_runtime_key(op)
                s.op_runtime_map[rk] = (pid, stable)
                s.op_last_pool[(pid, stable)] = pool_id

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                local_cpu[pool_id] -= cpu_req
                local_ram[pool_id] -= ram_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                did = True
                break

            if not did:
                break

    # Opportunistic compaction if queues have accumulated many dead slots.
    _maybe_compact_queues(force=False)

    return suspensions, assignments
