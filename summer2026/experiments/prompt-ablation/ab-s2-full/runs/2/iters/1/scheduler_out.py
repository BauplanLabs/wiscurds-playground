@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Runnable pipeline queues per priority (store pipeline_id), with O(1) head via index.
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.qh = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-pipeline state:
    # pid -> {
    #   "p": Pipeline,
    #   "pri": Priority,
    #   "enqueued": bool,
    #   "busy": bool,          # we only allow 1 in-flight op per pipeline
    #   "dead": bool,
    #   "not_before": int,     # tick before which we won't retry scheduling (fit/backoff)
    # }
    s.pstate = {}

    # OOM-driven RAM learning and bounded retry.
    # (pid, opk) -> ram_hint
    s.op_ram_hint = {}
    # (pid, opk) -> {"oom": int, "fail": int}
    s.op_retry = {}

    # Map op_key -> pid for attributing results.
    s.op_to_pid = {}

    # Weighted RR that reflects total score mass in this workload (count * weight):
    # Query ~33%, Interactive ~49%, Batch ~18%  => 4 : 6 : 2
    s.prio_cycle = (
        [Priority.QUERY] * 4
        + [Priority.INTERACTIVE] * 6
        + [Priority.BATCH_PIPELINE] * 2
    )
    s.prio_idx = 0

    # Per-tick bounds / knobs (keep scheduler fast).
    s.max_new_assignments_per_tick = 64
    s.scan_limit_per_assignment = 10

    # Retry / backoff knobs.
    s.max_oom_retries = 5
    s.max_nonoom_retries = 2
    s.backoff_no_fit = 5
    s.backoff_no_op = 3

    # Light reservations to keep headroom for higher priority work (in "resource units").
    # Chosen to be small enough to avoid wasted capacity but effective to prevent full blocking.
    s.reserve_query_cpu = 4
    s.reserve_query_ram = 16
    s.reserve_interactive_cpu = 2
    s.reserve_interactive_ram = 8

    # "Service at least a couple queries quickly" fast-path.
    s.query_fastpath_per_tick = 2


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _ceil(x):
        xi = int(x)
        return xi if xi >= x else xi + 1

    def _op_key(op):
        return (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "name", None)
            or id(op)
        )

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _q_len(pri):
        q = s.q[pri]
        h = s.qh[pri]
        n = len(q) - h
        return n if n > 0 else 0

    def _q_push(pri, pid):
        st = s.pstate.get(pid)
        if st is None or st.get("dead"):
            return
        if st.get("enqueued"):
            return
        st["enqueued"] = True
        s.q[pri].append(pid)

    def _q_pop(pri):
        q = s.q[pri]
        h = s.qh[pri]
        if h >= len(q):
            return None
        pid = q[h]
        s.qh[pri] = h + 1

        # Periodic compaction to avoid unbounded list growth.
        if s.qh[pri] > 2048 and s.qh[pri] > (len(q) // 2):
            s.q[pri] = q[s.qh[pri] :]
            s.qh[pri] = 0

        st = s.pstate.get(pid)
        if st is not None:
            st["enqueued"] = False
        return pid

    def _base_ram(pri):
        # Conservative-but-not-huge starting points (GB-ish in this benchmark).
        if pri == Priority.QUERY:
            return 32
        if pri == Priority.INTERACTIVE:
            return 24
        return 16

    def _cpu_request(pool, pri):
        # Moderate, capped scaling (avoid "bigger pool => same concurrency" trap).
        mc = pool.max_cpu_pool
        if pri == Priority.QUERY:
            # 8..16
            return min(16, max(8, _ceil(mc * 0.02)))
        if pri == Priority.INTERACTIVE:
            # 4..8
            return min(8, max(4, _ceil(mc * 0.01)))
        # 2..4
        return min(4, max(2, _ceil(mc * 0.005)))

    def _ram_request(pool, pri, pid, op):
        opk = _op_key(op)
        hint = s.op_ram_hint.get((pid, opk))
        r = hint if hint is not None else _base_ram(pri)
        if r < 1:
            r = 1
        if r > pool.max_ram_pool:
            r = pool.max_ram_pool
        return r

    def _pick_op_for_pipeline(p, pri, pid):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return None
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Prefer smallest estimated RAM among a small prefix to help packing.
        best = ops[0]
        best_r = None
        limit = 6 if len(ops) > 6 else len(ops)
        for i in range(limit):
            op = ops[i]
            opk = _op_key(op)
            hint = s.op_ram_hint.get((pid, opk))
            est = hint if hint is not None else _base_ram(pri)
            if best_r is None or est < best_r:
                best_r = est
                best = op
        return best

    def _pool_fits_with_reserve(pool_id, cpu, ram, pri, need_query, need_interactive):
        # Check raw fit and then enforce small headroom reservations for higher pri.
        if cpu > local_cpu[pool_id] or ram > local_ram[pool_id]:
            return False
        rem_cpu = local_cpu[pool_id] - cpu
        rem_ram = local_ram[pool_id] - ram

        if pri == Priority.INTERACTIVE:
            if need_query and (rem_cpu < s.reserve_query_cpu or rem_ram < s.reserve_query_ram):
                return False
        elif pri == Priority.BATCH_PIPELINE:
            if need_query and (rem_cpu < s.reserve_query_cpu or rem_ram < s.reserve_query_ram):
                return False
            if need_interactive and (
                rem_cpu < s.reserve_interactive_cpu or rem_ram < s.reserve_interactive_ram
            ):
                return False
        return True

    def _choose_pool(cpu, ram, pri, need_query, need_interactive):
        # Best-fit by RAM leftover (keeps RAM utilization high); tie-break by CPU leftover.
        best_pool = None
        best_key = None
        for pid in range(s.executor.num_pools):
            if not _pool_fits_with_reserve(pid, cpu, ram, pri, need_query, need_interactive):
                continue
            key = (local_ram[pid] - ram, local_cpu[pid] - cpu)
            if best_key is None or key < best_key:
                best_key = key
                best_pool = pid
        return best_pool

    def _drop_pipeline(pid):
        st = s.pstate.get(pid)
        if st is None:
            return
        st["dead"] = True
        st["enqueued"] = False
        st["busy"] = False

    def _enqueue_if_runnable(pid):
        st = s.pstate.get(pid)
        if st is None or st.get("dead"):
            return
        p = st.get("p")
        if p is None:
            return
        if p.runtime_status().is_pipeline_successful():
            _drop_pipeline(pid)
            return
        if st.get("busy"):
            return
        _q_push(st["pri"], pid)

    # --- Ingest / refresh pipelines (assumed to be new arrivals; refresh if repeated) ---
    for p in pipelines:
        pid = p.pipeline_id
        st = s.pstate.get(pid)
        if st is None:
            s.pstate[pid] = {
                "p": p,
                "pri": p.priority,
                "enqueued": False,
                "busy": False,
                "dead": False,
                "not_before": 0,
            }
            _q_push(p.priority, pid)
        else:
            # Refresh pointer (in case simulator passes updated objects).
            st["p"] = p
            st["pri"] = p.priority
            if not st.get("dead"):
                # If it was not busy and not enqueued (e.g., created earlier), ensure it can run.
                if (not st.get("busy")) and (not st.get("enqueued")):
                    _enqueue_if_runnable(pid)

    # --- Process results: clear busy, learn OOM RAM, bounded retries ---
    # Some sims include pipeline_id in result; otherwise infer from op_to_pid.
    touched_pids = set()
    for r in results:
        rid_pid = getattr(r, "pipeline_id", None)
        r_failed = (hasattr(r, "failed") and r.failed())

        ops = getattr(r, "ops", None) or []
        inferred_pids = set()

        if rid_pid is not None:
            inferred_pids.add(rid_pid)

        # Attribute by ops mapping.
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pid.get(opk)
            if pid is not None:
                inferred_pids.add(pid)

        # Update per-op hints/retries.
        for op in ops:
            opk = _op_key(op)
            pid = rid_pid if rid_pid is not None else s.op_to_pid.get(opk)
            if pid is None:
                continue

            key = (pid, opk)
            rt = s.op_retry.get(key)
            if rt is None:
                rt = {"oom": 0, "fail": 0}
                s.op_retry[key] = rt

            if r_failed:
                if _is_oom_error(getattr(r, "error", None)):
                    rt["oom"] += 1
                    alloc = getattr(r, "ram", None)
                    prev = s.op_ram_hint.get(key)
                    base = alloc if (alloc is not None and alloc > 0) else (prev if prev is not None else _base_ram(s.pstate.get(pid, {}).get("pri", Priority.BATCH_PIPELINE)))
                    # Fast convergence upward, but cap at pool max when we later size.
                    new_hint = int(max(base * 2.0, (prev * 1.6 if prev is not None else 0), _base_ram(s.pstate.get(pid, {}).get("pri", Priority.BATCH_PIPELINE))))
                    if new_hint < 1:
                        new_hint = 1
                    s.op_ram_hint[key] = new_hint
                else:
                    rt["fail"] += 1

            # Cleanup op mapping to avoid growth.
            if opk in s.op_to_pid:
                del s.op_to_pid[opk]

        # Clear busy / decide on deadness / re-enqueue pipelines.
        for pid in inferred_pids:
            st = s.pstate.get(pid)
            if st is None or st.get("dead"):
                continue

            st["busy"] = False
            touched_pids.add(pid)

            # If any op exceeded retry caps, kill pipeline (prevents hangs).
            # We only know about ops we've seen; this is conservative.
            if r_failed and ops:
                too_many = False
                fatal = False
                for op in ops:
                    opk = _op_key(op)
                    rt = s.op_retry.get((pid, opk))
                    if rt is None:
                        continue
                    if rt["oom"] > s.max_oom_retries:
                        too_many = True
                    if rt["fail"] > s.max_nonoom_retries:
                        fatal = True
                if too_many or fatal:
                    _drop_pipeline(pid)
                    continue

            _enqueue_if_runnable(pid)

    # --- Build local resource snapshots (MUST track locally) ---
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # If nothing is free, early return (fast).
    any_free = False
    for i in range(s.executor.num_pools):
        if local_cpu[i] >= 1 and local_ram[i] >= 1:
            any_free = True
            break
    if not any_free:
        return [], []

    assignments = []
    suspensions = []

    def _schedule_one(pri):
        need_query = _q_len(Priority.QUERY) > 0
        need_interactive = _q_len(Priority.INTERACTIVE) > 0

        for _ in range(s.scan_limit_per_assignment):
            pid = _q_pop(pri)
            if pid is None:
                return False

            st = s.pstate.get(pid)
            if st is None or st.get("dead"):
                continue

            # Respect backoff.
            if st.get("not_before", 0) > s.ticks:
                _q_push(pri, pid)
                continue

            # If somehow enqueued while busy, skip and don't requeue.
            if st.get("busy"):
                continue

            p = st.get("p")
            if p is None:
                _drop_pipeline(pid)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            op = _pick_op_for_pipeline(p, pri, pid)
            if op is None:
                # Nothing runnable right now (parents running, etc.). Back off a bit.
                st["not_before"] = s.ticks + s.backoff_no_op
                _q_push(pri, pid)
                continue

            # Size request; clip to pool max later by pool selection logic.
            # Use a representative pool for cpu sizing (we'll clip by chosen pool's max anyway).
            # If pools differ, _choose_pool will filter by fit.
            # Choose cpu based on the largest pool max CPU among those with some capacity, to avoid under-sizing.
            # (Still capped to small numbers.)
            any_pool = None
            for i in range(s.executor.num_pools):
                if s.executor.pools[i].max_cpu_pool >= 1 and s.executor.pools[i].max_ram_pool >= 1:
                    any_pool = s.executor.pools[i]
                    break
            if any_pool is None:
                return False

            cpu = _cpu_request(any_pool, pri)
            ram = _ram_request(any_pool, pri, pid, op)

            # Ensure minimums.
            if cpu < 1:
                cpu = 1
            if ram < 1:
                ram = 1

            # If request can't fit in any pool maximum, kill it (prevents infinite waiting).
            max_cpu_any = 0
            max_ram_any = 0
            for i in range(s.executor.num_pools):
                pool = s.executor.pools[i]
                if pool.max_cpu_pool > max_cpu_any:
                    max_cpu_any = pool.max_cpu_pool
                if pool.max_ram_pool > max_ram_any:
                    max_ram_any = pool.max_ram_pool
            if cpu > max_cpu_any or ram > max_ram_any:
                _drop_pipeline(pid)
                continue

            pool_id = _choose_pool(cpu, ram, pri, need_query, need_interactive)
            if pool_id is None:
                # Can't fit now; back off to avoid thrashing.
                st["not_before"] = s.ticks + s.backoff_no_fit
                _q_push(pri, pid)
                continue

            # Clip to chosen pool max (safety).
            pool = s.executor.pools[pool_id]
            if cpu > pool.max_cpu_pool:
                cpu = pool.max_cpu_pool
            if ram > pool.max_ram_pool:
                ram = pool.max_ram_pool

            # Final availability check (should pass).
            if cpu > local_cpu[pool_id] or ram > local_ram[pool_id]:
                st["not_before"] = s.ticks + s.backoff_no_fit
                _q_push(pri, pid)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            local_cpu[pool_id] -= cpu
            local_ram[pool_id] -= ram

            st["busy"] = True
            # Record for result attribution.
            s.op_to_pid[_op_key(op)] = pid

            return True

        return False

    # Fast-path: ensure queries start promptly (keeps p99 tight) but don't monopolize.
    for _ in range(s.query_fastpath_per_tick):
        if len(assignments) >= s.max_new_assignments_per_tick:
            break
        if _q_len(Priority.QUERY) <= 0:
            break
        if not _schedule_one(Priority.QUERY):
            break

    # Main loop: weighted RR over priorities.
    spins = 0
    max_spins = s.max_new_assignments_per_tick * 3  # prevent pathological loops
    while len(assignments) < s.max_new_assignments_per_tick and spins < max_spins:
        spins += 1

        # If no capacity anywhere, stop.
        any_cap = False
        for i in range(s.executor.num_pools):
            if local_cpu[i] >= 1 and local_ram[i] >= 1:
                any_cap = True
                break
        if not any_cap:
            break

        pri = s.prio_cycle[s.prio_idx]
        s.prio_idx = (s.prio_idx + 1) % len(s.prio_cycle)

        # Skip empty queues quickly.
        if _q_len(pri) <= 0:
            continue

        scheduled = _schedule_one(pri)
        if not scheduled:
            # If we couldn't schedule from this priority, try others on subsequent iterations.
            continue

    return suspensions, assignments
