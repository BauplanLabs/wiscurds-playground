@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Queues hold pipeline_ids (not pipeline objects) to avoid stale references.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Latest pipeline object by id (updated on ingest).
    s.pipeline_ref = {}

    # Track pipeline ids we've ever enqueued.
    s.enqueued = set()

    # Pipeline bookkeeping.
    s.pipeline_meta = {}  # pid -> {"enqueued_tick": int}
    s.completed_pipelines = set()
    s.dead_pipelines = set()

    # Per-op state keyed by (pipeline_id, stable_op_id)
    # state: {"oom": int, "fail": int, "ram_floor": float, "ram_hi": float|None}
    s.op_state = {}

    # Next RAM hint for retry keyed by (pid, stable_op_id) -> float
    s.op_ram_hint = {}

    # Map op object identity -> (pid, stable_op_id) for attributing ExecutionResult back to state.
    s.op_instance_map = {}

    # Policy knobs
    s.max_oom_retries = 5
    s.max_other_fail_retries = 2

    # CPU/RAM sizing (units are whatever the simulator uses; commonly vCPU and GB).
    # Keep CPU small and mostly constant so bigger clusters gain concurrency instead of inflating per-op size.
    s.base_cpu = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.min_cpu = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Start RAM modest to allow high concurrency; converge upward on OOM quickly.
    s.base_ram = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    s.min_ram = 1.0

    # Target CPU shares per priority (approximate importance mass across arrivals):
    # interactive matters a lot here (many arrivals * weight 5).
    s.base_cpu_share = {
        Priority.QUERY: 0.33,
        Priority.INTERACTIVE: 0.47,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Aging / urgency
    s.urgent_wait_ticks_query = 80
    s.urgent_wait_ticks_interactive = 120
    s.starve_wait_ticks_batch = 200

    # How many queue entries to scan per attempt to find a runnable+fit op.
    s.scan_limit = 120

    # Periodic compaction
    s.compact_every = 200


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _stable_op_id(op):
        for attr in ("op_id", "operator_id", "node_id", "task_id"):
            v = getattr(op, attr, None)
            if v is None:
                continue
            try:
                hash(v)
                hv = v
            except Exception:
                hv = str(v)
            return (attr, hv)
        # Fallback: object identity (stable for this run; good for retry if the op object persists)
        return ("obj", id(op))

    def _drop_pipeline(pid, mark_completed=False, mark_dead=False):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        if mark_completed:
            s.completed_pipelines.add(pid)
        if mark_dead:
            s.dead_pipelines.add(pid)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _queue_has_items(pri):
        q = s.wait_q.get(pri, [])
        # Fast check (we compact lazily anyway)
        return len(q) > 0

    def _oldest_wait_sample(pri, sample=60):
        q = s.wait_q.get(pri, [])
        if not q:
            return 0
        mx = 0
        n = len(q)
        lim = sample if n > sample else n
        # Sample front portion of queue (roughly oldest in RR rotation)
        for i in range(lim):
            pid = q[i]
            if pid in s.dead_pipelines or pid in s.completed_pipelines:
                continue
            wt = _pipeline_wait_ticks(pid)
            if wt > mx:
                mx = wt
        return mx

    def _effective_cpu_shares():
        shares = dict(s.base_cpu_share)

        # Urgency boosts: if high-priority work has been waiting a while, temporarily increase its share.
        q_wait = _oldest_wait_sample(Priority.QUERY)
        i_wait = _oldest_wait_sample(Priority.INTERACTIVE)
        b_wait = _oldest_wait_sample(Priority.BATCH_PIPELINE)

        if q_wait >= s.urgent_wait_ticks_query:
            shares[Priority.QUERY] += 0.12
            shares[Priority.BATCH_PIPELINE] -= 0.07
            shares[Priority.INTERACTIVE] -= 0.05

        if i_wait >= s.urgent_wait_ticks_interactive:
            shares[Priority.INTERACTIVE] += 0.10
            shares[Priority.BATCH_PIPELINE] -= 0.07
            shares[Priority.QUERY] -= 0.03

        if b_wait >= s.starve_wait_ticks_batch:
            shares[Priority.BATCH_PIPELINE] += 0.08
            shares[Priority.INTERACTIVE] -= 0.05
            shares[Priority.QUERY] -= 0.03

        # Clamp and renormalize among priorities that actually have queued work.
        active = [pri for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE) if _queue_has_items(pri)]
        if not active:
            return shares

        for pri in shares:
            if shares[pri] < 0.05:
                shares[pri] = 0.05
            if shares[pri] > 0.85:
                shares[pri] = 0.85

        total = sum(shares[pri] for pri in active)
        if total <= 0:
            # Fallback equal shares
            eq = 1.0 / float(len(active))
            for pri in shares:
                shares[pri] = eq if pri in active else 0.0
            return shares

        for pri in shares:
            shares[pri] = (shares[pri] / total) if (pri in active) else 0.0
        return shares

    def _global_max_ram_any_pool():
        mx = 0.0
        for pool in s.executor.pools:
            if pool.max_ram_pool > mx:
                mx = pool.max_ram_pool
        return mx

    def _choose_ram(pool, pri, pid, sid):
        # Start from hint if we have one; else base.
        hint = s.op_ram_hint.get((pid, sid), None)
        if hint is None:
            ram = float(s.base_ram[pri])
        else:
            ram = float(hint)

        # If we have a known OOM floor, keep above it with margin.
        st = s.op_state.get((pid, sid))
        if st:
            floor = float(st.get("ram_floor", 0.0) or 0.0)
            if floor > 0.0:
                ram = max(ram, floor * 1.25 + 1.0)

            hi = st.get("ram_hi", None)
            if hi is not None:
                try:
                    hi_f = float(hi)
                    # If we ever succeeded, don't grow unboundedly.
                    ram = min(ram, hi_f * 1.10)
                except Exception:
                    pass

        # Clamp to pool.
        if ram < s.min_ram:
            ram = s.min_ram
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        return ram

    def _choose_cpu(pri, avail_cpu_in_pool):
        base = float(s.base_cpu[pri])
        cpu = base
        if cpu > avail_cpu_in_pool:
            cpu = float(avail_cpu_in_pool)
        if cpu < float(s.min_cpu[pri]):
            return None
        return cpu

    def _pick_and_place_from_priority(pri, local_cpu, local_ram, scheduled_pipeline_ids):
        q = s.wait_q.get(pri, [])
        if not q:
            return None

        n = len(q)
        if n == 0:
            return None

        scan = s.scan_limit
        if scan > n:
            scan = n

        # Round-robin scan.
        for _ in range(scan):
            pid = q.pop(0)
            q.append(pid)

            if pid in s.dead_pipelines or pid in s.completed_pipelines:
                continue
            if pid in scheduled_pipeline_ids:
                continue

            p = s.pipeline_ref.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid, mark_completed=True)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]
            sid = _stable_op_id(op)

            # Find a pool that can fit it; best-fit to reduce fragmentation.
            best = None  # (waste, pool_id, cpu, ram)
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]

                cpu_req = _choose_cpu(pri, local_cpu[pool_id])
                if cpu_req is None:
                    continue

                ram_req = _choose_ram(pool, pri, pid, sid)
                if ram_req > local_ram[pool_id]:
                    continue
                if cpu_req > local_cpu[pool_id]:
                    continue

                # Best-fit metric: minimize leftover RAM, then leftover CPU.
                waste = (local_ram[pool_id] - ram_req) * 10.0 + (local_cpu[pool_id] - cpu_req)
                if best is None or waste < best[0]:
                    best = (waste, pool_id, cpu_req, ram_req)

            if best is None:
                continue

            _, pool_id, cpu_req, ram_req = best
            return pid, p, op, sid, pool_id, cpu_req, ram_req

        return None

    # --- Ingest pipelines (new arrivals and/or refresh references) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_ref[pid] = p

        if pid in s.dead_pipelines or pid in s.completed_pipelines:
            continue

        if pid not in s.enqueued:
            s.enqueued.add(pid)
            s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
            s.wait_q[p.priority].append(pid)

    # --- Process results: update OOM hints and failure counters ---
    max_ram_any = _global_max_ram_any_pool()
    for r in results:
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in ops:
            mapping = s.op_instance_map.get(id(op))
            if mapping is None:
                continue
            pid, sid = mapping
            if pid in s.dead_pipelines or pid in s.completed_pipelines:
                continue

            key = (pid, sid)
            st = s.op_state.get(key)
            if st is None:
                st = {"oom": 0, "fail": 0, "ram_floor": 0.0, "ram_hi": None}
                s.op_state[key] = st

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    st["oom"] = int(st.get("oom", 0) or 0) + 1

                    alloc_ram = getattr(r, "ram", None)
                    try:
                        alloc_ram_f = float(alloc_ram) if alloc_ram is not None else 0.0
                    except Exception:
                        alloc_ram_f = 0.0

                    if alloc_ram_f > 0.0:
                        floor = float(st.get("ram_floor", 0.0) or 0.0)
                        st["ram_floor"] = alloc_ram_f if alloc_ram_f > floor else floor

                    # Next hint: fast convergence upward, but bounded.
                    prev_hint = s.op_ram_hint.get(key, None)
                    base = float(prev_hint) if prev_hint is not None else float(s.base_ram.get(getattr(r, "priority", Priority.BATCH_PIPELINE), 8.0))

                    floor = float(st.get("ram_floor", 0.0) or 0.0)
                    next_hint = max(base * 1.6 + 2.0, floor * 1.35 + 4.0)

                    if next_hint > max_ram_any:
                        next_hint = max_ram_any

                    s.op_ram_hint[key] = next_hint

                    if st["oom"] >= int(s.max_oom_retries):
                        # Stop infinite OOM loops; mark dead.
                        _drop_pipeline(pid, mark_dead=True)
                else:
                    st["fail"] = int(st.get("fail", 0) or 0) + 1
                    if st["fail"] >= int(s.max_other_fail_retries):
                        _drop_pipeline(pid, mark_dead=True)
            else:
                # Success: record a "high" (known-good) RAM bound for this op.
                alloc_ram = getattr(r, "ram", None)
                try:
                    alloc_ram_f = float(alloc_ram) if alloc_ram is not None else None
                except Exception:
                    alloc_ram_f = None

                if alloc_ram_f is not None and alloc_ram_f > 0.0:
                    hi = st.get("ram_hi", None)
                    if hi is None:
                        st["ram_hi"] = alloc_ram_f
                    else:
                        try:
                            hi_f = float(hi)
                            st["ram_hi"] = alloc_ram_f if alloc_ram_f < hi_f else hi_f
                        except Exception:
                            st["ram_hi"] = alloc_ram_f

                # Clear aggressive hint after success (kept bounded by ram_hi anyway).
                if key in s.op_ram_hint:
                    # Keep a modest safety margin above floor, otherwise fall back to base.
                    floor = float(st.get("ram_floor", 0.0) or 0.0)
                    if floor > 0.0:
                        s.op_ram_hint[key] = floor * 1.25 + 1.0
                    else:
                        s.op_ram_hint.pop(key, None)

    # Periodic compaction to reduce overhead from completed/dead pipeline ids.
    if s.compact_every and (s.ticks % int(s.compact_every) == 0):
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            old = s.wait_q.get(pri, [])
            if not old:
                continue
            s.wait_q[pri] = [pid for pid in old if (pid not in s.dead_pipelines and pid not in s.completed_pipelines)]

    # --- Scheduling ---
    suspensions = []
    assignments = []

    # Snapshot availabilities; MUST decrement locally as we append assignments.
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # Fair sharing by CPU across priorities; fill leftover opportunistically.
    shares = _effective_cpu_shares()
    cpu_alloc = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }

    scheduled_pipeline_ids = set()

    def _any_pool_has_capacity():
        for i in range(s.executor.num_pools):
            if local_cpu[i] >= 1.0 and local_ram[i] >= s.min_ram:
                return True
        return False

    def _priority_order_for_ties():
        # Prefer QUERY, then INTERACTIVE, then BATCH on ties.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pick_next_priority(blocked):
        candidates = []
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if blocked.get(pri, False):
                continue
            if shares.get(pri, 0.0) <= 0.0:
                continue
            if not s.wait_q.get(pri):
                continue
            # Under-service metric: allocated / share (lower => more underserved).
            metric = cpu_alloc[pri] / (shares[pri] + 1e-9)
            candidates.append((metric, pri))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        # Tie-break: higher priority first.
        best_metric = candidates[0][0]
        tied = [pri for (m, pri) in candidates if abs(m - best_metric) <= 1e-9]
        if len(tied) == 1:
            return tied[0]
        for pri in _priority_order_for_ties():
            if pri in tied:
                return pri
        return candidates[0][1]

    blocked_pri = {Priority.QUERY: False, Priority.INTERACTIVE: False, Priority.BATCH_PIPELINE: False}

    # Main packing loop: keep placing single-op containers until no more placements possible this tick.
    guard = 0
    while _any_pool_has_capacity():
        guard += 1
        if guard > 5000:
            break  # safety

        pri = _pick_next_priority(blocked_pri)
        if pri is None:
            break

        picked = _pick_and_place_from_priority(pri, local_cpu, local_ram, scheduled_pipeline_ids)
        if picked is None:
            blocked_pri[pri] = True
            # If all blocked, stop.
            if blocked_pri[Priority.QUERY] and blocked_pri[Priority.INTERACTIVE] and blocked_pri[Priority.BATCH_PIPELINE]:
                break
            continue

        pid, p, op, sid, pool_id, cpu_req, ram_req = picked

        # Final sanity check (no overallocation).
        if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
            blocked_pri[pri] = True
            continue

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

        # Attribute future results to pipeline/op state.
        s.op_instance_map[id(op)] = (pid, sid)

        # Decrement local availability.
        local_cpu[pool_id] -= cpu_req
        local_ram[pool_id] -= ram_req

        cpu_alloc[pri] += cpu_req
        scheduled_pipeline_ids.add(pid)

        # Unblock all priorities after a successful placement (resource situation changed).
        blocked_pri[Priority.QUERY] = False
        blocked_pri[Priority.INTERACTIVE] = False
        blocked_pri[Priority.BATCH_PIPELINE] = False

    return suspensions, assignments
