@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Priority ring-queues of pipelines that are eligible to start a new op (i.e., not "inflight").
    # Ring queue representation: {"items": [Pipeline, ...], "head": int}
    s.q = {
        Priority.QUERY: {"items": [], "head": 0},
        Priority.INTERACTIVE: {"items": [], "head": 0},
        Priority.BATCH_PIPELINE: {"items": [], "head": 0},
    }

    # "Hot" lists: pipelines woken up by a completion/failure result; try these first.
    s.hot = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline tracking
    s.known = set()          # ever-seen pipeline_ids (avoid re-enqueue dupes if pipelines list repeats)
    s.active = set()         # pipelines we still intend to run
    s.inflight = set()       # pipelines that currently have an op running/assigned (we run 1 op per pipeline at a time)
    s.queued = set()         # pipelines currently present in q/hot (eligible to be picked)

    # pipeline_id -> Pipeline object (best-effort)
    s.pid_to_pipeline = {}

    # Operator learning / retry state
    # (pipeline_id, op_key) -> dict
    #   tries: total assignment attempts
    #   ooms: number of OOM failures
    #   fails: number of non-OOM failures
    #   lb_ram: known-bad RAM lower bound (OOM at/above this)
    #   ram_hint: next RAM to try
    #   last_ram: last RAM requested
    s.op_state = {}

    # Map op_key -> pipeline_id so we can attribute results back to pipelines.
    s.op_to_pid = {}

    # Non-preemptive, so preserve some headroom from batch when high-priority is waiting.
    s.reserve_hi_cpu_frac = 0.12
    s.reserve_hi_ram_frac = 0.12

    # Retry caps (bounded to avoid simulator hang)
    s.max_oom_retries = 5
    s.max_non_oom_retries = 2
    s.max_total_tries = 7  # overall safety cap

    # Scheduling bounds (keep per-tick work small)
    s.max_new_assignments_per_tick = 32
    s.max_assignments_per_pool_per_tick = 3
    s.scan_limit_per_pick = 8  # max pipelines inspected per priority per pick

    # INTERACTIVE vs BATCH sharing once QUERY is empty (3:1 favors interactive)
    s.ib_cycle = [Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.ib_ptr = 0

    # Pool loop cursor (spread load across pools)
    s.pool_cursor = 0


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda oom" in msg)

    def _q_len(qobj):
        items = qobj["items"]
        head = qobj["head"]
        n = len(items) - head
        return n if n > 0 else 0

    def _q_compact_if_needed(qobj):
        head = qobj["head"]
        items = qobj["items"]
        # Compact occasionally to bound memory/time; head only moves forward.
        if head > 1024 and head > (len(items) // 2):
            qobj["items"] = items[head:]
            qobj["head"] = 0

    def _enqueue(pid, p, pri, hot=False):
        if pid not in s.active:
            return
        if pid in s.inflight:
            return
        if pid in s.queued:
            return
        s.queued.add(pid)
        s.pid_to_pipeline[pid] = p
        if hot:
            s.hot[pri].append(p)
        else:
            qobj = s.q[pri]
            qobj["items"].append(p)
            _q_compact_if_needed(qobj)

    def _dequeue_from_hot(pri):
        lst = s.hot[pri]
        while lst:
            p = lst.pop()
            pid = p.pipeline_id
            if pid in s.queued:
                s.queued.discard(pid)
            if pid not in s.active or pid in s.inflight:
                continue
            return p
        return None

    def _dequeue_from_q(pri):
        qobj = s.q[pri]
        items = qobj["items"]
        head = qobj["head"]
        while head < len(items):
            p = items[head]
            head += 1
            pid = p.pipeline_id
            if pid in s.queued:
                s.queued.discard(pid)
            if pid not in s.active or pid in s.inflight:
                continue
            qobj["head"] = head
            _q_compact_if_needed(qobj)
            return p
        qobj["head"] = head
        _q_compact_if_needed(qobj)
        return None

    def _base_ram(pool, pri):
        # Memory-bound workload: start with moderate per-op RAM to avoid excessive OOMs,
        # but far below "take 20% of the pool" to allow concurrency.
        max_ram = pool.max_ram_pool
        if pri == Priority.QUERY:
            frac, cap, floor = 0.10, 96, 12
        elif pri == Priority.INTERACTIVE:
            frac, cap, floor = 0.08, 80, 10
        else:
            frac, cap, floor = 0.06, 64, 8
        ram = int(max(floor, min(cap, max_ram * frac)))
        if ram < 1:
            ram = 1
        if ram > max_ram:
            ram = int(max_ram)
        return ram

    def _base_cpu(pool, pri):
        max_cpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            base, cap, floor = 10, 16, 4
        elif pri == Priority.INTERACTIVE:
            base, cap, floor = 6, 12, 2
        else:
            base, cap, floor = 3, 8, 1
        cpu = int(max(floor, min(cap, base)))
        if cpu > max_cpu:
            cpu = int(max_cpu)
        return cpu

    def _next_non_query_priority():
        pri = s.ib_cycle[s.ib_ptr]
        s.ib_ptr += 1
        if s.ib_ptr >= len(s.ib_cycle):
            s.ib_ptr = 0
        return pri

    def _get_op_state(pid, opk):
        st = s.op_state.get((pid, opk))
        if st is None:
            st = {
                "tries": 0,
                "ooms": 0,
                "fails": 0,
                "lb_ram": 0,
                "ram_hint": None,
                "last_ram": None,
            }
            s.op_state[(pid, opk)] = st
        return st

    def _ram_request(pool, pri, pid, op):
        opk = _op_key(op)
        st = _get_op_state(pid, opk)

        # If we've learned a hint, use it; otherwise start from a moderate base.
        ram = st["ram_hint"]
        if ram is None:
            ram = _base_ram(pool, pri)

        # Ensure we are above the known-bad lower bound (if any).
        lb = st["lb_ram"] or 0
        if lb > 0 and ram <= lb:
            # Small jump above lower bound; avoid too many retries.
            ram = int(max(ram, lb + max(2, int(0.15 * lb))))

        # Clip to pool
        if ram < 1:
            ram = 1
        if ram > pool.max_ram_pool:
            ram = int(pool.max_ram_pool)
        return ram

    def _cpu_request(pool, pri, avail_cpu):
        cpu = _base_cpu(pool, pri)
        if cpu > avail_cpu:
            cpu = int(avail_cpu)
        if pri == Priority.QUERY and cpu < 4:
            return 0
        if pri == Priority.INTERACTIVE and cpu < 2:
            return 0
        if pri == Priority.BATCH_PIPELINE and cpu < 1:
            return 0
        return cpu

    def _mark_pipeline_dead(pid):
        s.active.discard(pid)
        s.inflight.discard(pid)
        s.queued.discard(pid)
        s.pid_to_pipeline.pop(pid, None)

    def _maybe_retire_completed_pipeline(p):
        try:
            if p.runtime_status().is_pipeline_successful():
                pid = p.pipeline_id
                s.active.discard(pid)
                s.inflight.discard(pid)
                s.queued.discard(pid)
                s.pid_to_pipeline.pop(pid, None)
                return True
        except Exception:
            # If status lookup fails, keep it.
            return False
        return False

    # --- Ingest new pipelines (assumed new arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.known:
            s.known.add(pid)
            s.active.add(pid)
            s.pid_to_pipeline[pid] = p
            _enqueue(pid, p, p.priority, hot=False)

    # --- Process results: learn RAM hints, clear inflight, and wake pipeline for next op ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        pid = getattr(r, "pipeline_id", None)
        if pid is None and ops:
            pid = s.op_to_pid.get(_op_key(ops[0]), None)

        if pid is not None:
            s.inflight.discard(pid)

        failed = False
        if hasattr(r, "failed"):
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

        if ops:
            for op in ops:
                opk = _op_key(op)
                if pid is None:
                    pid = s.op_to_pid.get(opk, None)
                if pid is None or pid not in s.active:
                    continue

                st = _get_op_state(pid, opk)
                if failed:
                    if _is_oom_error(getattr(r, "error", None)):
                        st["ooms"] += 1
                        alloc = getattr(r, "ram", None)
                        if alloc is None or alloc <= 0:
                            alloc = st["last_ram"] if st["last_ram"] is not None else 1

                        if alloc > st["lb_ram"]:
                            st["lb_ram"] = int(alloc)

                        # Next try: multiplicative increase (fast convergence).
                        # Start aggressive enough to avoid repeated OOMs.
                        nxt = int(max(st["lb_ram"] * 2, st["lb_ram"] + 4))
                        # Clip to pool max later when scheduling; keep raw hint here.
                        st["ram_hint"] = nxt
                    else:
                        st["fails"] += 1
                else:
                    # On success, keep current hint (don't try to shrink; avoid extra OOM churn).
                    pass

        # Wake up pipeline quickly after any completion/failure, unless it's dead/completed.
        if pid is not None and pid in s.active:
            p = s.pid_to_pipeline.get(pid, None)
            if p is not None:
                if not _maybe_retire_completed_pipeline(p):
                    _enqueue(pid, p, p.priority, hot=True)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    # Determine if high priority is waiting (to reserve headroom from batch).
    hi_waiting = (
        len(s.hot[Priority.QUERY]) > 0
        or len(s.hot[Priority.INTERACTIVE]) > 0
        or _q_len(s.q[Priority.QUERY]) > 0
        or _q_len(s.q[Priority.INTERACTIVE]) > 0
    )

    scheduled_pids_this_tick = set()
    scheduled_opkeys_this_tick = set()

    max_new = s.max_new_assignments_per_tick
    pool_start = s.pool_cursor % num_pools
    s.pool_cursor = (s.pool_cursor + 1) % num_pools

    # Helper: pick next pipeline for a priority (hot first), bounded scans.
    def _pick_pipeline(pri):
        # Try hot first
        p = _dequeue_from_hot(pri)
        if p is not None:
            return p

        # Then try queue
        return _dequeue_from_q(pri)

    def _requeue_pipeline(p, hot=False):
        pid = p.pipeline_id
        if pid not in s.active or pid in s.inflight:
            return
        _enqueue(pid, p, p.priority, hot=hot)

    # Main loop: iterate pools round-robin, assign a few per pool.
    pool_id = pool_start
    pools_visited = 0
    while pools_visited < num_pools and len(assignments) < max_new:
        pool = s.executor.pools[pool_id]
        per_pool = 0

        while per_pool < s.max_assignments_per_pool_per_tick and len(assignments) < max_new:
            avail_cpu = local_cpu[pool_id]
            avail_ram = local_ram[pool_id]
            if avail_cpu < 1 or avail_ram < 1:
                break

            # Choose which priority to schedule.
            # Always attempt QUERY first; then cycle between INTERACTIVE and BATCH.
            chosen_pri = None

            # Attempt QUERY
            if len(s.hot[Priority.QUERY]) > 0 or _q_len(s.q[Priority.QUERY]) > 0:
                chosen_pri = Priority.QUERY
            else:
                chosen_pri = _next_non_query_priority()

            # We'll try up to 3 priorities in a small fallback order.
            if chosen_pri == Priority.QUERY:
                pri_try = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            elif chosen_pri == Priority.INTERACTIVE:
                pri_try = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE, Priority.QUERY]
            else:
                pri_try = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

            picked = None
            picked_pri = None

            # Bounded scanning: attempt a few pipelines per priority.
            for pri in pri_try:
                scans = 0
                while scans < s.scan_limit_per_pick:
                    scans += 1
                    p = _pick_pipeline(pri)
                    if p is None:
                        break
                    pid = p.pipeline_id

                    if pid not in s.active or pid in s.inflight:
                        continue
                    if pid in scheduled_pids_this_tick:
                        # Try again later; keep it eligible.
                        _requeue_pipeline(p, hot=False)
                        continue

                    # Drop completed pipelines promptly.
                    if _maybe_retire_completed_pipeline(p):
                        continue

                    # Find runnable op.
                    status = p.runtime_status()
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not op_list:
                        # No runnable op right now (unusual with 1-op-at-a-time); keep it eligible.
                        _requeue_pipeline(p, hot=False)
                        continue

                    op = op_list[0]
                    opk = _op_key(op)
                    if opk in scheduled_opkeys_this_tick:
                        _requeue_pipeline(p, hot=False)
                        continue

                    st = _get_op_state(pid, opk)

                    # Enforce bounded retries.
                    if st["tries"] >= s.max_total_tries or st["ooms"] >= s.max_oom_retries or st["fails"] >= s.max_non_oom_retries:
                        # Give up: better to stop thrashing and let others complete.
                        _mark_pipeline_dead(pid)
                        break

                    # Batch reservation: keep headroom when high priority is waiting.
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    if pri == Priority.BATCH_PIPELINE and hi_waiting:
                        reserve_cpu = int(max(0, pool.max_cpu_pool * s.reserve_hi_cpu_frac))
                        reserve_ram = int(max(0, pool.max_ram_pool * s.reserve_hi_ram_frac))
                        eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                        eff_avail_ram = max(0, avail_ram - reserve_ram)

                    if eff_avail_cpu < 1 or eff_avail_ram < 1:
                        # Can't schedule this class on this pool right now; requeue and try another pri/pool.
                        _requeue_pipeline(p, hot=False)
                        break

                    ram_req = _ram_request(pool, pri, pid, op)
                    if ram_req > eff_avail_ram:
                        # Don't schedule below hint; would likely OOM and waste time.
                        _requeue_pipeline(p, hot=False)
                        continue

                    cpu_req = _cpu_request(pool, pri, eff_avail_cpu)
                    if cpu_req <= 0:
                        _requeue_pipeline(p, hot=False)
                        continue

                    picked = (p, op, cpu_req, ram_req)
                    picked_pri = pri
                    break

                if picked is not None:
                    break

            if picked is None:
                break

            p, op, cpu_req, ram_req = picked
            pid = p.pipeline_id
            opk = _op_key(op)

            # Final availability check (strict, using full avail not eff)
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                # Put back and stop trying this pool (state changed due to earlier placements).
                _requeue_pipeline(p, hot=False)
                break

            # Create assignment
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

            # Bookkeeping for result attribution and retries
            s.op_to_pid[opk] = pid
            st = _get_op_state(pid, opk)
            st["tries"] += 1
            st["last_ram"] = ram_req

            # Pipeline now inflight until we see a result
            s.inflight.add(pid)
            scheduled_pids_this_tick.add(pid)
            scheduled_opkeys_this_tick.add(opk)

            # Update local resources
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req
            per_pool += 1

        pool_id = (pool_id + 1) % num_pools
        pools_visited += 1

    return suspensions, assignments
