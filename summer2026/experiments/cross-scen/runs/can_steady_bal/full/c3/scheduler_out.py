@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Stable pipeline storage
    s.pipeline_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_slot = {}           # pipeline_id -> (priority, index_in_wait_list)

    # Tombstone-friendly waiting lists (stable indices; we never compact to preserve pipeline_slot)
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
    s.live_counts = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Event-driven "ready" queues of pipeline_ids (best-effort; backed by periodic refresh)
    s.ready_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.ready_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.ready_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Pipeline meta (cooldowns reduce repeated status checks on blocked pipelines)
    # {pipeline_id: {"enqueued_tick": int, "next_check_tick": int}}
    s.pipeline_meta = {}

    # OOM-driven memory discovery (per-operator) + mild pipeline-level hinting
    # op_stats key: (pipeline_id, op_key) -> dict
    s.op_stats = {}
    s.pipeline_ram_hint = {}  # pipeline_id -> ram_hint (helps later ops in same pipeline)

    # Map op_key -> pipeline_id for result attribution
    s.op_to_pipeline = {}

    # Permanent give-up set (bounded retries)
    s.dead_pipelines = set()

    # Knobs
    s.max_assignments_per_tick = 24
    s.ready_scan_limit = 16
    s.refresh_every = 8  # ticks
    s.refresh_checks = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 10,
    }

    s.max_tries_per_op = 7
    s.max_non_oom_failures_per_op = 3

    # Cooldowns (ticks)
    s.cooldown_no_runnable = 3
    s.cooldown_after_assign = 2


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        if ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg):
            return True
        if ("oomkilled" in msg) or ("oom-killed" in msg):
            return True
        # Common "killed" patterns that often correspond to OOM in container contexts
        if ("killed process" in msg) and ("memory" in msg):
            return True
        if ("exit code 137" in msg) or ("code 137" in msg):
            return True
        return False

    def _ready_count(pri):
        h = s.ready_head[pri]
        q = s.ready_q[pri]
        return len(q) - h

    def _ready_push(pri, pipeline_id):
        if pipeline_id in s.dead_pipelines:
            return
        if pipeline_id not in s.pipeline_by_id:
            return
        if pipeline_id in s.ready_set[pri]:
            return
        s.ready_q[pri].append(pipeline_id)
        s.ready_set[pri].add(pipeline_id)

    def _ready_pop_scan(pri):
        """Pop up to ready_scan_limit pipeline_ids (in order) and return list of ids (deduped)."""
        q = s.ready_q[pri]
        head = s.ready_head[pri]
        out = []
        scanned = 0
        n = len(q)
        while scanned < s.ready_scan_limit and head < n:
            pid = q[head]
            head += 1
            scanned += 1
            if pid in s.ready_set[pri]:
                s.ready_set[pri].discard(pid)
            out.append(pid)
        s.ready_head[pri] = head

        # Compact occasionally to keep head from growing without bound (amortized)
        if head > 256 and head * 2 > len(q):
            s.ready_q[pri] = q[head:]
            s.ready_head[pri] = 0
            # ready_set already reflects only not-yet-popped items; no rebuild required
        return out

    def _drop_pipeline(pipeline_id):
        if pipeline_id in s.dead_pipelines:
            s.dead_pipelines.discard(pipeline_id)
        p = s.pipeline_by_id.pop(pipeline_id, None)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_ram_hint.pop(pipeline_id, None)

        slot = s.pipeline_slot.pop(pipeline_id, None)
        if slot is not None:
            pri, idx = slot
            q = s.wait_q.get(pri)
            if q is not None and 0 <= idx < len(q) and q[idx] is not None:
                q[idx] = None
                if s.live_counts.get(pri, 0) > 0:
                    s.live_counts[pri] -= 1

        # best-effort cleanup
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            s.ready_set[pri].discard(pipeline_id)

        # NOTE: we do not clear s.op_stats / s.op_to_pipeline aggressively (can be large);
        # they are keyed by op identities and won't be revisited once pipeline disappears.

        return p

    def _base_cpu(pool, pri):
        m = float(pool.max_cpu_pool)
        if pri == Priority.QUERY:
            x = max(8.0, m * 0.15)
            x = min(x, 32.0)
        elif pri == Priority.INTERACTIVE:
            x = max(6.0, m * 0.12)
            x = min(x, 24.0)
        else:
            x = max(4.0, m * 0.10)
            x = min(x, 16.0)
        if x > m:
            x = m
        if x < 1.0:
            x = 1.0
        return x

    def _base_ram(pool, pri):
        m = float(pool.max_ram_pool)
        if pri == Priority.QUERY:
            x = max(20.0, m * 0.04)
            x = min(x, 96.0)
        elif pri == Priority.INTERACTIVE:
            x = max(18.0, m * 0.035)
            x = min(x, 80.0)
        else:
            x = max(14.0, m * 0.03)
            x = min(x, 64.0)
        if x > m:
            x = m
        if x < 1.0:
            x = 1.0
        return x

    def _reserve_amounts(pool, pri):
        """Soft reservation logic to protect higher-priority latency without killing throughput."""
        base_c = _base_cpu(pool, pri)
        base_r = _base_ram(pool, pri)
        if _ready_count(pri) > 0:
            return base_c, base_r
        if s.live_counts.get(pri, 0) > 0:
            return base_c * 0.5, base_r * 0.5
        return 0.0, 0.0

    def _size_request(pool, pri, pipeline_id, op):
        opk = _op_key(op)
        st = s.op_stats.get((pipeline_id, opk))

        ram = _base_ram(pool, pri)
        ph = s.pipeline_ram_hint.get(pipeline_id, 0.0)
        if ph and ph > ram:
            ram = ph

        if st is not None:
            rf = float(st.get("ram_floor", 0.0) or 0.0)
            if rf > 0.0:
                ram = max(ram, rf * 1.25 + 2.0)
            rh = st.get("ram_hint", None)
            if rh is not None:
                ram = max(ram, float(rh))

        if ram > float(pool.max_ram_pool):
            ram = float(pool.max_ram_pool)
        if ram < 1.0:
            ram = 1.0

        cpu = _base_cpu(pool, pri)
        if cpu > float(pool.max_cpu_pool):
            cpu = float(pool.max_cpu_pool)
        if cpu < 1.0:
            cpu = 1.0

        return cpu, ram

    def _bump_on_oom(st, pool, alloc_ram):
        prev_hint = st.get("ram_hint", None)
        prev_hint = float(prev_hint) if prev_hint is not None else 0.0
        alloc_ram = float(alloc_ram or 0.0)

        rf = float(st.get("ram_floor", 0.0) or 0.0)
        if alloc_ram > rf:
            rf = alloc_ram
        st["ram_floor"] = rf

        base = max(prev_hint, alloc_ram, rf, 1.0)
        # Aggressive multiplicative bump to converge quickly
        new_hint = base * 1.7 + 2.0
        max_ram = float(pool.max_ram_pool)
        if new_hint > max_ram:
            new_hint = max_ram
        st["ram_hint"] = new_hint

        return new_hint

    def _refresh_ready(pri, max_checks):
        q = s.wait_q[pri]
        n = len(q)
        if n <= 0:
            return
        pos = s.wait_pos[pri] % n
        checks = 0
        while checks < max_checks and n > 0:
            p = q[pos]
            pos += 1
            if pos >= n:
                pos = 0
            checks += 1

            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.dead_pipelines:
                continue
            if pid not in s.pipeline_by_id:
                continue
            if pid in s.ready_set[pri]:
                continue

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                nct = meta.get("next_check_tick", 0)
                if nct and nct > s.ticks:
                    continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                _ready_push(pri, pid)
            else:
                if meta is None:
                    meta = {}
                    s.pipeline_meta[pid] = meta
                meta["next_check_tick"] = s.ticks + s.cooldown_no_runnable

        s.wait_pos[pri] = pos

    # --- Ingest new pipelines (arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.pipeline_by_id:
            continue

        s.pipeline_by_id[pid] = p
        pri = p.priority
        idx = len(s.wait_q[pri])
        s.wait_q[pri].append(p)
        s.pipeline_slot[pid] = (pri, idx)
        s.live_counts[pri] += 1
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "next_check_tick": 0}

        _ready_push(pri, pid)

    # --- Process results (learn RAM, re-awaken pipelines) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        pool_id = getattr(r, "pool_id", None)
        if pool_id is None:
            continue
        if pool_id < 0 or pool_id >= s.executor.num_pools:
            continue
        pool = s.executor.pools[pool_id]
        failed = (hasattr(r, "failed") and r.failed())
        err = getattr(r, "error", None)
        alloc_ram = getattr(r, "ram", None)

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            pri = p.priority

            st = s.op_stats.get((pid, opk))
            if st is None:
                st = {"tries": 0, "ooms": 0, "non_oom": 0, "ram_floor": 0.0, "ram_hint": None}
                s.op_stats[(pid, opk)] = st

            if failed:
                if _is_oom_error(err):
                    st["ooms"] = int(st.get("ooms", 0) or 0) + 1
                    new_hint = _bump_on_oom(st, pool, alloc_ram)
                    # Pipeline-level hint (tempered) to reduce repeated OOMs on later ops
                    ph = float(s.pipeline_ram_hint.get(pid, 0.0) or 0.0)
                    uplift = new_hint * 0.75
                    if uplift > ph:
                        s.pipeline_ram_hint[pid] = min(uplift, float(pool.max_ram_pool))
                else:
                    st["non_oom"] = int(st.get("non_oom", 0) or 0) + 1
                    # Mild RAM bump after repeated unknown failures (covers misclassified OOM)
                    if st["non_oom"] >= 2:
                        prev = st.get("ram_hint", None)
                        prev = float(prev) if prev is not None else (float(alloc_ram) if alloc_ram else 0.0)
                        if prev > 0.0:
                            bumped = prev * 1.20 + 1.0
                            if bumped > float(pool.max_ram_pool):
                                bumped = float(pool.max_ram_pool)
                            st["ram_hint"] = max(float(st.get("ram_hint", 0.0) or 0.0), bumped)

            # Whatever happened, the pipeline may have new runnable ops now.
            _ready_push(pri, pid)

    # Periodic refresh to avoid stalls if result attribution is imperfect
    if s.ticks % s.refresh_every == 0:
        _refresh_ready(Priority.QUERY, s.refresh_checks[Priority.QUERY])
        _refresh_ready(Priority.INTERACTIVE, s.refresh_checks[Priority.INTERACTIVE])
        _refresh_ready(Priority.BATCH_PIPELINE, s.refresh_checks[Priority.BATCH_PIPELINE])

    # Quick exit if nothing to do
    if (s.live_counts[Priority.QUERY] + s.live_counts[Priority.INTERACTIVE] + s.live_counts[Priority.BATCH_PIPELINE]) <= 0:
        return [], []

    suspensions = []
    assignments = []

    # Local resource accounting (per-tick snapshots)
    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    scheduled_pids = set()
    total_new = 0

    def _attempt_pick_and_assign(pool_id, pri, reserve_cpu, reserve_ram):
        nonlocal total_new
        if total_new >= s.max_assignments_per_tick:
            return False
        if local_cpu[pool_id] < 1.0 or local_ram[pool_id] < 1.0:
            return False

        pool = s.executor.pools[pool_id]

        # Scan a small batch from ready_q for this priority
        pids = _ready_pop_scan(pri)
        if not pids:
            return False

        for pid in pids:
            if pid in s.dead_pipelines:
                continue
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            if pid in scheduled_pids:
                # Try again later
                _ready_push(pri, pid)
                continue

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                nct = meta.get("next_check_tick", 0)
                if nct and nct > s.ticks:
                    _ready_push(pri, pid)
                    continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                if meta is None:
                    meta = {}
                    s.pipeline_meta[pid] = meta
                meta["next_check_tick"] = s.ticks + s.cooldown_no_runnable
                continue

            op = ops[0]
            opk = _op_key(op)

            ost = s.op_stats.get((pid, opk))
            if ost is None:
                ost = {"tries": 0, "ooms": 0, "non_oom": 0, "ram_floor": 0.0, "ram_hint": None}
                s.op_stats[(pid, opk)] = ost

            # Bounded retries: stop thrashing when clearly hopeless
            tries = int(ost.get("tries", 0) or 0)
            non_oom = int(ost.get("non_oom", 0) or 0)
            if non_oom >= s.max_non_oom_failures_per_op:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)
                continue
            if tries >= s.max_tries_per_op:
                # If we've already escalated RAM near the pool max and still keep trying, give up.
                rh = ost.get("ram_hint", None)
                rh = float(rh) if rh is not None else 0.0
                if rh >= float(pool.max_ram_pool) * 0.98 or int(ost.get("ooms", 0) or 0) >= s.max_tries_per_op:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)
                    continue
                # Otherwise allow a few more tries (rare); fall through.

            cpu_req, ram_req = _size_request(pool, pri, pid, op)

            # Headroom protection for higher priorities
            # (Ensure we don't consume the last chunk that an imminent higher-priority op would need.)
            if pri != Priority.QUERY:
                # Ensure at least reserve remains after placing this op
                if (local_cpu[pool_id] - cpu_req) < reserve_cpu or (local_ram[pool_id] - ram_req) < reserve_ram:
                    # Can't fit while keeping reserve; requeue
                    _ready_push(pri, pid)
                    continue

            # Fit checks (RAM is hard; CPU can be downscaled a bit if needed)
            if ram_req > local_ram[pool_id]:
                _ready_push(pri, pid)
                continue

            # CPU downscale policy: allow partial CPU if it helps avoid idling,
            # but keep a small floor per priority to avoid extreme slowdowns.
            min_cpu = 1.0
            base_cpu = _base_cpu(pool, pri)
            if pri == Priority.QUERY:
                min_cpu = max(4.0, base_cpu * 0.60)
            elif pri == Priority.INTERACTIVE:
                min_cpu = max(3.0, base_cpu * 0.55)
            else:
                min_cpu = max(2.0, base_cpu * 0.50)

            if cpu_req > local_cpu[pool_id]:
                if local_cpu[pool_id] >= min_cpu:
                    cpu_req = local_cpu[pool_id]
                else:
                    _ready_push(pri, pid)
                    continue

            if cpu_req < 1.0:
                _ready_push(pri, pid)
                continue

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

            # Bookkeeping
            s.op_to_pipeline[opk] = pid
            ost["tries"] = int(ost.get("tries", 0) or 0) + 1
            ost["last_cpu"] = cpu_req
            ost["last_ram"] = ram_req

            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req
            if local_cpu[pool_id] < 0.0:
                local_cpu[pool_id] = 0.0
            if local_ram[pool_id] < 0.0:
                local_ram[pool_id] = 0.0

            scheduled_pids.add(pid)
            total_new += 1

            if meta is None:
                meta = {}
                s.pipeline_meta[pid] = meta
            meta["next_check_tick"] = s.ticks + s.cooldown_after_assign

            return True

        return False

    # Main placement loop (small, bounded number of new assignments per tick)
    progress = True
    while total_new < s.max_assignments_per_tick and progress:
        progress = False
        for pool_id in range(s.executor.num_pools):
            if total_new >= s.max_assignments_per_tick:
                break
            if local_cpu[pool_id] < 1.0 or local_ram[pool_id] < 1.0:
                continue

            pool = s.executor.pools[pool_id]

            # Compute dynamic reserves per pool
            rq_cpu, rq_ram = _reserve_amounts(pool, Priority.QUERY)
            ri_cpu, ri_ram = _reserve_amounts(pool, Priority.INTERACTIVE)

            # Try priorities in order, with headroom protection:
            # - Query: no reserve
            # - Interactive: reserve for Query
            # - Batch: reserve for Query + Interactive
            made = _attempt_pick_and_assign(pool_id, Priority.QUERY, 0.0, 0.0)
            if made:
                progress = True
                continue

            made = _attempt_pick_and_assign(pool_id, Priority.INTERACTIVE, rq_cpu, rq_ram)
            if made:
                progress = True
                continue

            made = _attempt_pick_and_assign(pool_id, Priority.BATCH_PIPELINE, rq_cpu + ri_cpu, rq_ram + ri_ram)
            if made:
                progress = True
                continue

    return suspensions, assignments
