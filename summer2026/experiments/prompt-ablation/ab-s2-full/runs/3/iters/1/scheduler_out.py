@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Queues: keep two rings per priority: pipelines we have started vs brand new.
    # Stored as pipeline_id lists with a rotating cursor; we skip stale ids at pick time.
    s.q_new = {pri: [] for pri in s.priorities}
    s.q_started = {pri: [] for pri in s.priorities}
    s.qpos_new = {pri: 0 for pri in s.priorities}
    s.qpos_started = {pri: 0 for pri in s.priorities}

    s.active = set()  # pipeline_ids currently tracked
    s.active_by_pri = {pri: set() for pri in s.priorities}
    s.started = set()  # pipeline_ids that have been scheduled at least once
    s.dead = set()  # pipeline_ids blacklisted (permanent failure)

    s.pipe_by_id = {}  # pipeline_id -> Pipeline
    # pipeline_id -> {"pri": Priority, "enq": tick, "next": tick}
    s.meta = {}

    # Map op instance to (pipeline_id, op_key) for attributing results back.
    s.op_inst_map = {}  # id(op) -> (pipeline_id, op_key)

    # OOM-driven RAM discovery + bounded retries.
    s.ram_hint = {}       # (pipeline_id, op_key) -> next_ram
    s.retry_oom = {}      # (pipeline_id, op_key) -> count
    s.retry_other = {}    # (pipeline_id, op_key) -> count

    # Per-pool priority credits (CPU-budget) for fair sharing.
    s.credits = {}  # pool_id -> {pri: float}

    # Tuning knobs (fast per-tick, throughput-friendly).
    s.idle_cooldown_ticks = 8     # if no runnable ops, don't re-check immediately
    s.fit_cooldown_ticks = 3      # if doesn't fit now, re-check soon but not every loop
    s.post_schedule_cooldown = 1  # after scheduling one op, avoid rechecking same tick
    s.scan_limit_started = 10
    s.scan_limit_new = 8
    s.max_oom_retries = 5
    s.max_other_retries = 2
    s.cleanup_every_ticks = 250

    # CPU share targets; QUERY protected but do not starve others.
    s.share = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Priority tie-break bonuses (scaled by pool size at runtime).
    s.bonus_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.10,
        Priority.BATCH_PIPELINE: 0.00,
    }

    # CPU sizing (scaled by pool size, but capped to avoid giant single-container allocations).
    s.cpu_frac = {
        Priority.QUERY: 0.07,
        Priority.INTERACTIVE: 0.05,
        Priority.BATCH_PIPELINE: 0.035,
    }
    s.cpu_min = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.cpu_cap = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 4.0,
    }

    # RAM sizing: aim for high utilization without giant overallocations; OOM feedback corrects.
    s.ram_per_cpu = {
        Priority.QUERY: 7.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 6.0,
    }
    s.ram_min = {
        Priority.QUERY: 14.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    s.ram_cap = {
        Priority.QUERY: 72.0,
        Priority.INTERACTIVE: 60.0,
        Priority.BATCH_PIPELINE: 48.0,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed" in msg and "memory" in msg)

    def _drop_pipeline(pid):
        if pid in s.active:
            s.active.discard(pid)
            meta = s.meta.get(pid)
            if meta is not None:
                s.active_by_pri.get(meta.get("pri"), set()).discard(pid)
            s.started.discard(pid)
        s.pipe_by_id.pop(pid, None)
        s.meta.pop(pid, None)

    def _ensure_pool_credits():
        for pool_id in range(s.executor.num_pools):
            if pool_id not in s.credits:
                s.credits[pool_id] = {pri: 0.0 for pri in s.priorities}

    def _refill_credits(pool_id, pool):
        # Refill in max_cpu units to target long-run fairness even when scheduler isn't called often.
        max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
        if max_cpu <= 0.0:
            return
        cap = 3.0 * max_cpu
        c = s.credits[pool_id]
        for pri in s.priorities:
            c[pri] = min(cap, c.get(pri, 0.0) + s.share[pri] * max_cpu)

    def _cpu_target(pool, pri):
        max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
        if max_cpu <= 0.0:
            return 0.0
        raw = max_cpu * float(s.cpu_frac[pri])
        cpu = float(int(raw)) if raw >= 1.0 else raw
        cpu = max(cpu, float(s.cpu_min[pri]))
        cpu = min(cpu, float(s.cpu_cap[pri]))
        cpu = min(cpu, max_cpu)
        if cpu < 1.0 and max_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _ram_target(pool, pri, cpu_req, pid, opk):
        max_ram = float(getattr(pool, "max_ram_pool", 0.0) or 0.0)
        if max_ram <= 0.0:
            return 0.0

        base = max(float(s.ram_min[pri]), float(s.ram_per_cpu[pri]) * float(cpu_req))
        base = min(base, float(s.ram_cap[pri]), max_ram)

        hint = s.ram_hint.get((pid, opk), None)
        if hint is not None:
            if float(hint) > max_ram:
                return max_ram + 1.0  # sentinel: impossible to satisfy in this pool
            base = max(base, float(hint))

        # Final clip
        if base > max_ram:
            base = max_ram
        if base < 1.0 and max_ram >= 1.0:
            base = 1.0
        return base

    def _compact_queues_if_needed():
        if s.ticks % s.cleanup_every_ticks != 0:
            return
        for pri in s.priorities:
            act = s.active_by_pri[pri]
            # Started
            qs = s.q_started[pri]
            if len(qs) > (len(act) * 4 + 256):
                s.q_started[pri] = [pid for pid in qs if pid in act and pid in s.started and pid not in s.dead]
                s.qpos_started[pri] = 0
            # New
            qn = s.q_new[pri]
            if len(qn) > (len(act) * 4 + 256):
                s.q_new[pri] = [pid for pid in qn if pid in act and pid not in s.started and pid not in s.dead]
                s.qpos_new[pri] = 0

    def _pick_from_ring(pri, ring_name, pool, pool_id, local_cpu, local_ram, scheduled_pids):
        if ring_name == "started":
            q = s.q_started[pri]
            pos = s.qpos_started[pri]
            limit = s.scan_limit_started
        else:
            q = s.q_new[pri]
            pos = s.qpos_new[pri]
            limit = s.scan_limit_new

        n = len(q)
        if n == 0:
            return None

        checks = min(limit, n)
        for _ in range(checks):
            pid = q[pos % n]
            pos += 1

            if pid in s.dead or pid not in s.active:
                continue
            if pid in scheduled_pids:
                continue

            meta = s.meta.get(pid)
            if meta is None or meta.get("pri") != pri:
                continue
            if meta.get("next", 0) > s.ticks:
                continue

            p = s.pipe_by_id.get(pid)
            if p is None:
                _drop_pipeline(pid)
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                meta["next"] = s.ticks + s.idle_cooldown_ticks
                continue

            op = ops[0]
            opk = _op_key(op)

            cpu_des = _cpu_target(pool, pri)
            if cpu_des <= 0.0:
                return None
            cpu_req = min(cpu_des, float(local_cpu))
            if cpu_req < 1.0:
                return None

            ram_req = _ram_target(pool, pri, cpu_req, pid, opk)
            if ram_req <= 0.0:
                return None

            # If an OOM hint can't fit now (or is impossible), back off.
            if ram_req > float(local_ram):
                meta["next"] = s.ticks + s.fit_cooldown_ticks
                continue

            # Success
            if ring_name == "started":
                s.qpos_started[pri] = pos % max(1, n)
            else:
                s.qpos_new[pri] = pos % max(1, n)
            return (p, pid, op, opk, cpu_req, ram_req)

        if ring_name == "started":
            s.qpos_started[pri] = pos % max(1, n)
        else:
            s.qpos_new[pri] = pos % max(1, n)
        return None

    def _pick_op(pri, pool, pool_id, local_cpu, local_ram, scheduled_pids):
        # Prefer finishing pipelines we've already started to maximize completion rate.
        picked = _pick_from_ring(pri, "started", pool, pool_id, local_cpu, local_ram, scheduled_pids)
        if picked is not None:
            return picked
        return _pick_from_ring(pri, "new", pool, pool_id, local_cpu, local_ram, scheduled_pids)

    def _priority_order_for_pool(pool, pool_id):
        # Order by (credit + priority bonus) to keep QUERY snappy while guaranteeing shares.
        max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
        c = s.credits[pool_id]
        scored = []
        for pri in s.priorities:
            if not s.active_by_pri[pri]:
                continue
            score = float(c.get(pri, 0.0)) + float(s.bonus_frac.get(pri, 0.0)) * max_cpu
            scored.append((score, pri))
        scored.sort(reverse=True, key=lambda x: x[0])
        return [pri for _, pri in scored]

    # --- Ingest pipelines (treat as arrivals; also update reference if repeated) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead:
            continue
        if pid not in s.active:
            s.active.add(pid)
            s.pipe_by_id[pid] = p
            s.meta[pid] = {"pri": p.priority, "enq": s.ticks, "next": s.ticks}
            s.active_by_pri[p.priority].add(pid)
            s.q_new[p.priority].append(pid)
        else:
            # Refresh object reference (safe if simulator provides updated object)
            s.pipe_by_id[pid] = p

    _ensure_pool_credits()

    # --- Process results (OOM hints + bounded retries; wake pipelines on completion/failure) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        pid_seen = set()
        for op in ops:
            key = s.op_inst_map.get(id(op), None)
            if key is None:
                continue
            pid, opk = key
            pid_seen.add(pid)

            if pid in s.dead:
                continue

            if hasattr(r, "failed") and r.failed():
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    k = (pid, opk)
                    cnt = s.retry_oom.get(k, 0) + 1
                    s.retry_oom[k] = cnt
                    if cnt > s.max_oom_retries:
                        s.dead.add(pid)
                        _drop_pipeline(pid)
                        continue

                    alloc = float(getattr(r, "ram", 0.0) or 0.0)
                    prev = float(s.ram_hint.get(k, 0.0) or 0.0)
                    base = max(prev, alloc, 1.0)
                    new_hint = float(int(base * 2.0) + 1)

                    # Cap to the pool's hard limit if known.
                    try:
                        pool_id = int(getattr(r, "pool_id", 0))
                        max_ram = float(s.executor.pools[pool_id].max_ram_pool)
                        if new_hint > max_ram:
                            new_hint = max_ram
                    except Exception:
                        pass

                    s.ram_hint[k] = new_hint
                else:
                    k = (pid, opk)
                    cnt = s.retry_other.get(k, 0) + 1
                    s.retry_other[k] = cnt
                    if cnt > s.max_other_retries:
                        s.dead.add(pid)
                        _drop_pipeline(pid)
                        continue
                    # Mildly bump RAM to hedge against misclassified memory issues.
                    prev = float(s.ram_hint.get(k, 0.0) or 0.0)
                    if prev > 0.0:
                        s.ram_hint[k] = float(int(prev * 1.25) + 1)

        # Wake pipelines whose ops just completed/failed so next stage can be scheduled quickly.
        for pid in pid_seen:
            meta = s.meta.get(pid)
            if meta is not None:
                meta["next"] = min(meta.get("next", s.ticks), s.ticks)

    _compact_queues_if_needed()

    # Fast exit: nothing to do or no capacity
    if not s.active:
        return [], []

    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]
    if max(local_cpu) < 1.0 or max(local_ram) < 1.0:
        return [], []

    # Prefer pools with more free RAM first (helps large-memory retries).
    pool_order = list(range(s.executor.num_pools))
    if len(pool_order) > 1:
        pool_order.sort(key=lambda i: local_ram[i], reverse=True)

    suspensions = []
    assignments = []
    scheduled_pids = set()

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        _refill_credits(pool_id, pool)

        if local_cpu[pool_id] < 1.0 or local_ram[pool_id] < 1.0:
            continue

        # Bound per-tick work; with 100 ticks/sec this still fills the pool quickly.
        # Scale lightly with free CPU to keep utilization high when the pool is empty.
        cap = 24 + int(min(48.0, max(0.0, local_cpu[pool_id])) // 3.0)
        max_new = min(72, max(24, cap))

        placed = 0
        while placed < max_new and local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0:
            order = _priority_order_for_pool(pool, pool_id)
            if not order:
                break

            picked_any = False
            for pri in order:
                picked = _pick_op(pri, pool, pool_id, local_cpu[pool_id], local_ram[pool_id], scheduled_pids)
                if picked is None:
                    continue

                p, pid, op, opk, cpu_req, ram_req = picked

                # Re-validate assignable (defensive: avoid stale picks)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
                assignable = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not assignable or op not in assignable:
                    continue

                # Mark started (once) to prioritize completion.
                if pid not in s.started:
                    s.started.add(pid)
                    s.q_started[pri].append(pid)

                # Record op instance mapping for result attribution.
                s.op_inst_map[id(op)] = (pid, opk)

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

                # Update local resources and accounting.
                local_cpu[pool_id] -= float(cpu_req)
                local_ram[pool_id] -= float(ram_req)
                s.credits[pool_id][pri] = float(s.credits[pool_id].get(pri, 0.0)) - float(cpu_req)

                scheduled_pids.add(pid)
                meta = s.meta.get(pid)
                if meta is not None:
                    meta["next"] = s.ticks + s.post_schedule_cooldown

                placed += 1
                picked_any = True
                break

            if not picked_any:
                break

    return suspensions, assignments
