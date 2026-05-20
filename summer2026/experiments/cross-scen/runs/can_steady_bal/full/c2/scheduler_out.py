@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Active pipeline registry
    s.pipelines = {}          # pid -> Pipeline (latest reference)
    s.pri_of = {}             # pid -> Priority
    s.active = set()          # pids considered by scheduler
    s.dead = set()            # pids we will not schedule anymore

    # Round-robin lists per priority (store pids; may include stale entries; skipped lazily)
    s.rr = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_pos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Wake queues per priority: pids to reconsider soon (after op completion/failure or arrival)
    s.wake = {
        Priority.QUERY: {"data": [], "head": 0},
        Priority.INTERACTIVE: {"data": [], "head": 0},
        Priority.BATCH_PIPELINE: {"data": [], "head": 0},
    }
    s.wake_in = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Operator learning: key=(pid, opk) -> stats
    # Fields: oom_floor, ok_ram, oom_retries, fail_retries, last_ram, last_cpu
    s.op_stats = {}
    s.pid_to_opks = {}   # pid -> list[opk] for cleanup
    s.op_to_pid = {}     # opk -> pid (for attributing results)

    # Scheduling knobs
    s.query_cpu = 8
    s.interactive_cpu = 6
    s.batch_cpu = 4
    s.batch_cpu_when_hi_recent = 2

    s.query_ram = 12
    s.interactive_ram = 16
    s.batch_ram = 12

    s.max_assignments_cap = 128
    s.max_scan_wake = 12
    s.max_scan_rr = 24

    s.max_oom_retries = 6
    s.max_fail_retries = 3

    # "Recent high-priority activity" window; used to keep batch off latency pool / keep headroom
    # Simulator typically runs ~100 ticks/sec.
    s.hi_cooldown_ticks = 800  # ~8 seconds
    s.last_hi_tick = 0

    # Weighted cycle favors INTERACTIVE throughput (currently the main deficit) while keeping QUERY tight.
    s.cycle = [Priority.QUERY, Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.cycle_idx = 0

    # Periodic compaction to keep RR lists from accumulating too many stale pids
    s.cleanup_every = 2000  # ticks
    s._cleanup_ctr = 0

    def _op_key(op):
        v = getattr(op, "op_id", None)
        if v is not None:
            return v
        v = getattr(op, "operator_id", None)
        if v is not None:
            return v
        v = getattr(op, "task_id", None)
        if v is not None:
            return v
        v = getattr(op, "node_id", None)
        if v is not None:
            return v
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    s._op_key = _op_key
    s._is_oom_error = _is_oom_error


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1
    s._cleanup_ctr += 1

    def _wake_push(pri, pid):
        if pid in s.wake_in[pri]:
            return
        s.wake_in[pri].add(pid)
        s.wake[pri]["data"].append(pid)

    def _wake_pop(pri):
        q = s.wake[pri]
        data = q["data"]
        head = q["head"]
        n = len(data)
        while head < n:
            pid = data[head]
            head += 1
            if pid in s.wake_in[pri]:
                s.wake_in[pri].discard(pid)
            q["head"] = head
            # Compact occasionally
            if q["head"] >= 1024:
                q["data"] = q["data"][q["head"] :]
                q["head"] = 0
            return pid
        return None

    def _drop_pipeline(pid):
        if pid in s.active:
            s.active.discard(pid)
        s.pri_of.pop(pid, None)
        s.pipelines.pop(pid, None)
        # Cleanup operator stats for this pid
        opks = s.pid_to_opks.pop(pid, None)
        if opks:
            for opk in opks:
                s.op_stats.pop((pid, opk), None)

    def _ensure_op_stats(pid, opk):
        key = (pid, opk)
        st = s.op_stats.get(key)
        if st is None:
            st = {
                "oom_floor": 0.0,
                "ok_ram": 0.0,
                "oom_retries": 0,
                "fail_retries": 0,
                "last_ram": 0.0,
                "last_cpu": 0.0,
            }
            s.op_stats[key] = st
            if pid not in s.pid_to_opks:
                s.pid_to_opks[pid] = []
            s.pid_to_opks[pid].append(opk)
        return st

    def _int_ge1(x):
        try:
            xi = int(x)
        except Exception:
            xi = 1
        if xi < 1:
            xi = 1
        return xi

    def _base_cpu(pri, pool_id):
        if pri == Priority.QUERY:
            return s.query_cpu
        if pri == Priority.INTERACTIVE:
            return s.interactive_cpu
        # Batch: shrink on shared/latency pool when high-priority has been active recently.
        if (s.executor.num_pools == 1 or pool_id == 0) and (s.ticks - s.last_hi_tick <= s.hi_cooldown_ticks):
            return s.batch_cpu_when_hi_recent
        return s.batch_cpu

    def _base_ram(pri):
        if pri == Priority.QUERY:
            return s.query_ram
        if pri == Priority.INTERACTIVE:
            return s.interactive_ram
        return s.batch_ram

    def _desired_resources(pid, op, pri, pool_id):
        opk = s._op_key(op)
        st = _ensure_op_stats(pid, opk)

        cpu = _base_cpu(pri, pool_id)
        cpu = _int_ge1(cpu)

        base = float(_base_ram(pri))
        oom_floor = float(st.get("oom_floor", 0.0) or 0.0)
        ok_ram = float(st.get("ok_ram", 0.0) or 0.0)

        # Prefer the smallest known-good RAM; otherwise jump above the largest known OOM.
        if ok_ram > 0.0:
            ram = ok_ram
            if oom_floor > 0.0 and ram < oom_floor * 1.25:
                ram = oom_floor * 1.35
        elif oom_floor > 0.0:
            ram = oom_floor * 2.0
        else:
            ram = base

        if ram < base:
            ram = base

        # Small cushion to reduce borderline OOM churn
        ram = ram * 1.05

        pool = s.executor.pools[pool_id]
        if ram > pool.max_ram_pool:
            ram = float(pool.max_ram_pool)
        if ram < 1.0:
            ram = 1.0

        return cpu, ram

    def _batch_reserve_ok(pool_id, cpu_req, ram_req, local_cpu, local_ram):
        # On a shared/latency pool, keep headroom so QUERY/INTERACTIVE can start quickly without preemption.
        if s.executor.num_pools > 1 and pool_id != 0:
            return True
        if s.ticks - s.last_hi_tick > s.hi_cooldown_ticks:
            return True

        pool = s.executor.pools[pool_id]
        reserve_cpu = int(pool.max_cpu_pool * 0.05)
        if reserve_cpu < s.query_cpu:
            reserve_cpu = s.query_cpu
        if reserve_cpu > 24:
            reserve_cpu = 24

        reserve_ram = int(pool.max_ram_pool * 0.01)
        if reserve_ram < 24:
            reserve_ram = 24
        if reserve_ram > 128:
            reserve_ram = 128

        if local_cpu[pool_id] - cpu_req < reserve_cpu:
            return False
        if local_ram[pool_id] - ram_req < reserve_ram:
            return False
        return True

    def _pool_order(pri):
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if pri == Priority.BATCH_PIPELINE:
            # Keep batch off pool0 while high-priority active recently.
            if s.ticks - s.last_hi_tick <= s.hi_cooldown_ticks:
                return list(range(1, n))
            return list(range(1, n)) + [0]
        # QUERY / INTERACTIVE prefer pool0
        return [0] + list(range(1, n))

    def _try_place(pid, op, pri, local_cpu, local_ram):
        for pool_id in _pool_order(pri):
            cpu_req, ram_req = _desired_resources(pid, op, pri, pool_id)

            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                continue

            if pri == Priority.BATCH_PIPELINE:
                if not _batch_reserve_ok(pool_id, cpu_req, ram_req, local_cpu, local_ram):
                    continue

            # Final clamp: keep requests within pool max (defensive)
            pool = s.executor.pools[pool_id]
            if cpu_req > pool.max_cpu_pool:
                cpu_req = int(pool.max_cpu_pool)
            if ram_req > pool.max_ram_pool:
                ram_req = float(pool.max_ram_pool)

            cpu_req = _int_ge1(cpu_req)
            if ram_req < 1.0:
                ram_req = 1.0

            if cpu_req <= local_cpu[pool_id] and ram_req <= local_ram[pool_id]:
                return pool_id, cpu_req, ram_req
        return None

    def _pick_one(pri, scheduled_pids, tried_pids, local_cpu, local_ram):
        # Wake scan (recently unblocked)
        for _ in range(s.max_scan_wake):
            pid = _wake_pop(pri)
            if pid is None:
                break
            if pid in tried_pids or pid in scheduled_pids:
                continue
            if pid in s.dead or pid not in s.active:
                continue
            p = s.pipelines.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            placement = _try_place(pid, op, pri, local_cpu, local_ram)
            if placement is None:
                tried_pids.add(pid)
                continue
            pool_id, cpu_req, ram_req = placement
            return pid, p, op, pool_id, cpu_req, ram_req

        # RR scan
        ids = s.rr[pri]
        m = len(ids)
        if m == 0:
            return None
        pos = s.rr_pos[pri]
        scan = s.max_scan_rr
        if scan > m:
            scan = m

        for _ in range(scan):
            if pos >= m:
                pos = 0
            pid = ids[pos]
            pos += 1

            if pid in tried_pids or pid in scheduled_pids:
                continue
            if pid in s.dead or pid not in s.active:
                continue
            p = s.pipelines.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            placement = _try_place(pid, op, pri, local_cpu, local_ram)
            if placement is None:
                tried_pids.add(pid)
                continue
            pool_id, cpu_req, ram_req = placement
            s.rr_pos[pri] = pos
            return pid, p, op, pool_id, cpu_req, ram_req

        s.rr_pos[pri] = pos
        return None

    # --- Ingest pipelines (assumed: new arrivals; safe if repeated) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines[pid] = p

        if pid in s.dead:
            continue

        pri = p.priority
        s.pri_of[pid] = pri

        if pri != Priority.BATCH_PIPELINE:
            s.last_hi_tick = s.ticks

        if pid not in s.active:
            s.active.add(pid)
            s.rr[pri].append(pid)
            _wake_push(pri, pid)

    # --- Process results (learn RAM via OOM feedback; wake pipelines) ---
    newly_dead = []
    for r in results:
        pid = getattr(r, "pipeline_id", None)

        ops = getattr(r, "ops", None) or []
        if pid is None and ops:
            pid = s.op_to_pid.get(s._op_key(ops[0]), None)
        if pid is None and ops:
            for op in ops:
                pid = s.op_to_pid.get(s._op_key(op), None)
                if pid is not None:
                    break

        if pid is None:
            continue
        if pid in s.dead or pid not in s.active:
            continue

        pri = s.pri_of.get(pid, getattr(r, "priority", Priority.BATCH_PIPELINE))
        if pri != Priority.BATCH_PIPELINE:
            s.last_hi_tick = s.ticks

        if ops:
            op = ops[0]
            opk = s._op_key(op)
            st = _ensure_op_stats(pid, opk)

            failed = False
            if hasattr(r, "failed"):
                try:
                    failed = r.failed()
                except Exception:
                    failed = False

            alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if alloc_ram <= 0.0:
                alloc_ram = float(st.get("last_ram", 0.0) or 0.0)

            if failed:
                err = getattr(r, "error", None)
                if s._is_oom_error(err):
                    st["oom_retries"] = int(st.get("oom_retries", 0) or 0) + 1
                    if alloc_ram > float(st.get("oom_floor", 0.0) or 0.0):
                        st["oom_floor"] = alloc_ram

                    pool_id = getattr(r, "pool_id", None)
                    if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                        max_ram = float(s.executor.pools[pool_id].max_ram_pool)
                        if float(st.get("oom_floor", 0.0) or 0.0) >= max_ram * 0.98:
                            newly_dead.append(pid)

                    if int(st.get("oom_retries", 0) or 0) > s.max_oom_retries:
                        newly_dead.append(pid)
                else:
                    st["fail_retries"] = int(st.get("fail_retries", 0) or 0) + 1
                    if int(st.get("fail_retries", 0) or 0) > s.max_fail_retries:
                        newly_dead.append(pid)
            else:
                if alloc_ram > 0.0:
                    ok = float(st.get("ok_ram", 0.0) or 0.0)
                    if ok <= 0.0 or alloc_ram < ok:
                        st["ok_ram"] = alloc_ram
                st["fail_retries"] = 0

        # Wake pipeline to schedule next op / retry promptly
        if pid not in s.dead:
            _wake_push(pri, pid)

    # Apply newly dead pipelines
    for pid in newly_dead:
        if pid not in s.dead:
            s.dead.add(pid)
            _drop_pipeline(pid)

    # Periodic compaction of RR lists (bounded, infrequent)
    if s._cleanup_ctr >= s.cleanup_every:
        s._cleanup_ctr = 0
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            ids = s.rr[pri]
            if not ids:
                s.rr_pos[pri] = 0
                continue
            new_ids = []
            for pid in ids:
                if pid in s.active and pid not in s.dead:
                    new_ids.append(pid)
            s.rr[pri] = new_ids
            s.rr_pos[pri] = 0

    # --- Build assignments ---
    if not s.active:
        return [], []

    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    total_avail_cpu = 0.0
    total_avail_ram = 0.0
    for i in range(s.executor.num_pools):
        total_avail_cpu += local_cpu[i]
        total_avail_ram += local_ram[i]

    if total_avail_cpu < 1.0 or total_avail_ram < 1.0:
        return [], []

    # Cap assignments per tick to keep scheduler fast and let the system flow across ticks.
    cap = int(total_avail_cpu / 4.0)
    if cap < 16:
        cap = 16
    if cap > s.max_assignments_cap:
        cap = s.max_assignments_cap
    # One op per pipeline per tick
    if cap > len(s.active):
        cap = len(s.active)

    scheduled_pids = set()
    tried_pids = set()

    assignments = []
    suspensions = []

    cycle = s.cycle
    clen = len(cycle)
    for _ in range(cap):
        placed = False
        for j in range(clen):
            pri = cycle[(s.cycle_idx + j) % clen]
            picked = _pick_one(pri, scheduled_pids, tried_pids, local_cpu, local_ram)
            if picked is None:
                continue

            pid, p, op, pool_id, cpu_req, ram_req = picked

            # Final safety checks (availability must be tracked locally per tick)
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                tried_pids.add(pid)
                continue

            # Record last request for better OOM attribution if result lacks ram
            opk = s._op_key(op)
            st = _ensure_op_stats(pid, opk)
            st["last_ram"] = float(ram_req)
            st["last_cpu"] = float(cpu_req)
            s.op_to_pid[opk] = pid

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

            local_cpu[pool_id] -= float(cpu_req)
            local_ram[pool_id] -= float(ram_req)
            scheduled_pids.add(pid)

            if p.priority != Priority.BATCH_PIPELINE:
                s.last_hi_tick = s.ticks

            s.cycle_idx = (s.cycle_idx + j + 1) % clen
            placed = True
            break

        if not placed:
            break

    return suspensions, assignments
