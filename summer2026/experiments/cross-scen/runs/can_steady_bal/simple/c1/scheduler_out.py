@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Priority-aware, completion-focused scheduler tuned for mixed workloads.

    Key changes vs previous version:
      - Remove aggressive blacklisting on non-OOM failures (timeouts are retried with more CPU).
      - Much smaller default CPU slices (higher concurrency, less head-of-line blocking).
      - RAM sized to support target concurrency (higher utilization) + fast OOM backoff.
      - Weighted fairness between INTERACTIVE and BATCH so both make progress.
      - Mild reservations to prevent BATCH from consuming the last headroom when high-priority is runnable.
      - Within a priority, prefer pipelines closer to completion (fewer remaining ops) to maximize completion rate.
    """
    s.ticks = 0
    s.turn = 0  # used for weighted fairness between interactive and batch

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    s.enqueued = set()
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}

    # Resource hints keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Failure accounting (avoid infinite thrash; but prefer retries over dropping)
    s.op_fail_counts = {}         # (pipeline_id, op_key) -> int
    s.pipeline_fail_counts = {}   # pipeline_id -> int

    # Map op_key -> pipeline_id to attribute results
    s.op_to_pipeline = {}

    # Knobs
    s.scan_limit = 24  # max items scanned per queue when picking next runnable op

    # Retry limits (high enough to converge on correct sizing, low enough to prevent runaway)
    s.max_op_failures = 8
    s.max_pipeline_failures = 24

    # Per-priority concurrency intent (CPU slices)
    s.base_cpu_slice = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.cpu_cap = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 16,
    }

    # Allow some intra-pipeline parallelism (esp. if DAG has multiple ready leaves)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _now_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_fail_counts.pop(pipeline_id, None)

        # Best-effort cleanup: remove per-op hints for this pipeline (bounded cost)
        # (keeps memory stable for long simulations)
        to_del = []
        for (pid, opk) in s.op_ram_hint.keys():
            if pid == pipeline_id:
                to_del.append((pid, opk))
        for k in to_del:
            s.op_ram_hint.pop(k, None)
            s.op_cpu_hint.pop(k, None)
            s.op_fail_counts.pop(k, None)

    def _int_cpu(x):
        try:
            v = int(x)
        except Exception:
            v = 1
        return 1 if v < 1 else v

    def _base_cpu(pool, pri, wait_ticks):
        base = s.base_cpu_slice.get(pri, 4)

        # Slight aging boost: long-waiting pipelines get a bit more CPU to help completion.
        if pri != Priority.QUERY:
            if wait_ticks >= 200:
                base = int(base * 1.25) + 1
            if wait_ticks >= 500:
                base = int(base * 1.35) + 1

        cap = s.cpu_cap.get(pri, base)
        if base > cap:
            base = cap

        # Never exceed pool
        if base > pool.max_cpu_pool:
            base = pool.max_cpu_pool
        return _int_cpu(base)

    def _target_parallelism(pool, pri, wait_ticks):
        cpu = _base_cpu(pool, pri, wait_ticks)
        if cpu < 1:
            cpu = 1
        par = int(pool.max_cpu_pool // cpu)
        return 1 if par < 1 else par

    def _base_ram(pool, pri, wait_ticks):
        # Aim to fill a large fraction of RAM at the intended parallelism to drive utilization.
        # (RAM beyond minimum doesn't speed up ops, but too much per-op reduces concurrency.)
        par = _target_parallelism(pool, pri, wait_ticks)

        if pri == Priority.QUERY:
            fill = 0.70
        elif pri == Priority.INTERACTIVE:
            fill = 0.78
        else:
            fill = 0.85

        ram = (pool.max_ram_pool * fill) / float(par)

        # Ensure non-trivial minimum, but keep it modest to encourage packing.
        # (OOM backoff will correct underestimates.)
        min_floor = max(1.0, pool.max_ram_pool * 0.01)  # 1% floor
        if ram < min_floor:
            ram = min_floor

        if pri == Priority.QUERY and ram < (pool.max_ram_pool * 0.02):
            ram = pool.max_ram_pool * 0.02

        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        return ram

    def _size_request(pool, pri, pipeline_id, op, wait_ticks):
        opk = _op_key(op)

        # CPU
        base_cpu = _base_cpu(pool, pri, wait_ticks)
        cpu_hint = s.op_cpu_hint.get((pipeline_id, opk))
        cpu_req = base_cpu if cpu_hint is None else max(base_cpu, cpu_hint)
        if cpu_req > pool.max_cpu_pool:
            cpu_req = pool.max_cpu_pool
        cpu_req = _int_cpu(cpu_req)

        # RAM
        base_ram = _base_ram(pool, pri, wait_ticks)
        ram_hint = s.op_ram_hint.get((pipeline_id, opk))
        ram_req = base_ram if ram_hint is None else max(base_ram, ram_hint)
        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool
        if ram_req < 1:
            ram_req = 1.0

        return cpu_req, ram_req

    def _pipeline_remaining_ops(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return 0
        # Approximate remaining work by counting ops not yet completed.
        # (Prefer completing short pipelines to maximize completion rate.)
        states = [
            OperatorState.PENDING,
            OperatorState.ASSIGNED,
            OperatorState.RUNNING,
            OperatorState.SUSPENDING,
            OperatorState.FAILED,
        ]
        try:
            return len(st.get_ops(states, require_parents_complete=False))
        except Exception:
            return 999999

    def _queue_has_runnable(q):
        # Bounded check: is there any runnable op in the first few entries?
        n = len(q)
        if n == 0:
            return False
        lim = 10 if n > 10 else n
        for i in range(lim):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _batch_reservation_ok(pool, avail_cpu, avail_ram, batch_cpu_req, batch_ram_req):
        # If high-priority work is runnable, keep a little headroom so it can start promptly.
        hp_runnable = _queue_has_runnable(s.wait_q[Priority.QUERY]) or _queue_has_runnable(s.wait_q[Priority.INTERACTIVE])
        if not hp_runnable:
            return True

        reserve_cpu = max(4.0, pool.max_cpu_pool * 0.15)
        reserve_ram = max(1.0, pool.max_ram_pool * 0.10)

        return (avail_cpu - batch_cpu_req) >= reserve_cpu and (avail_ram - batch_ram_req) >= reserve_ram

    def _choose_best_runnable_op(p, pool, avail_cpu, avail_ram, wait_ticks):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None, None, None

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None, None, None

        # Consider a few runnable ops; pick the one that best fits (lowest RAM, then CPU),
        # since RAM is the most common packing constraint.
        best = None
        best_key = None
        limit = 3 if len(ops) > 3 else len(ops)
        for i in range(limit):
            op = ops[i]
            cpu_req, ram_req = _size_request(pool, p.priority, p.pipeline_id, op, wait_ticks)
            if cpu_req <= avail_cpu and ram_req <= avail_ram:
                key = (ram_req, cpu_req)
                if best is None or key < best_key:
                    best = (op, cpu_req, ram_req)
                    best_key = key
        if best is not None:
            return best[0], best[1], best[2]

        # If nothing fits exactly, allow CPU to be clipped down (never below 1) to fit.
        # RAM is not clipped here (below-min RAM risks OOM); rely on later availability/pools.
        for i in range(limit):
            op = ops[i]
            cpu_req, ram_req = _size_request(pool, p.priority, p.pipeline_id, op, wait_ticks)
            if ram_req <= avail_ram:
                cpu_req = cpu_req if cpu_req <= avail_cpu else avail_cpu
                cpu_req = _int_cpu(cpu_req)
                if cpu_req >= 1:
                    key = (ram_req, cpu_req)
                    if best is None or key < best_key:
                        best = (op, cpu_req, ram_req)
                        best_key = key
        if best is None:
            return None, None, None
        return best[0], best[1], best[2]

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.pipeline_fail_counts.setdefault(pid, 0)

    # --- Process results: update hints and retry accounting ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue

            key = (pid, opk)

            if hasattr(r, "failed") and r.failed():
                s.op_fail_counts[key] = s.op_fail_counts.get(key, 0) + 1
                s.pipeline_fail_counts[pid] = s.pipeline_fail_counts.get(pid, 0) + 1

                err = getattr(r, "error", None)

                # Use reported allocation if available; otherwise fall back to stored hint/base.
                alloc_ram = getattr(r, "ram", None)
                alloc_cpu = getattr(r, "cpu", None)

                if _is_oom_error(err):
                    prev = s.op_ram_hint.get(key)
                    base = alloc_ram if (alloc_ram is not None and alloc_ram > 0) else (prev if prev is not None else 1.0)
                    new_hint = base * 2.0
                    if prev is not None and new_hint < (prev * 1.5):
                        new_hint = prev * 1.5
                    s.op_ram_hint[key] = new_hint

                elif _is_timeout_error(err):
                    prev = s.op_cpu_hint.get(key)
                    base = alloc_cpu if (alloc_cpu is not None and alloc_cpu > 0) else (prev if prev is not None else 1)
                    new_hint = int(base * 1.6) + 1
                    if prev is not None and new_hint < (prev + 1):
                        new_hint = prev + 1
                    s.op_cpu_hint[key] = new_hint

                else:
                    # Unknown error: small, conservative bump (prefer eventual completion over dropping).
                    prev_cpu = s.op_cpu_hint.get(key)
                    prev_ram = s.op_ram_hint.get(key)
                    if alloc_cpu is not None and alloc_cpu > 0:
                        bump_cpu = int(alloc_cpu * 1.25) + 1
                    else:
                        bump_cpu = (prev_cpu + 1) if prev_cpu is not None else 2
                    if alloc_ram is not None and alloc_ram > 0:
                        bump_ram = alloc_ram * 1.15
                    else:
                        bump_ram = (prev_ram * 1.15) if prev_ram is not None else 2.0
                    s.op_cpu_hint[key] = bump_cpu if prev_cpu is None else max(prev_cpu, bump_cpu)
                    s.op_ram_hint[key] = bump_ram if prev_ram is None else max(prev_ram, bump_ram)

            else:
                # Success: reset per-op failure count (helps avoid over-prioritizing old failures).
                if key in s.op_fail_counts:
                    s.op_fail_counts[key] = 0

    suspensions = []
    assignments = []

    # Per-tick per-pipeline assignment caps
    scheduled_counts = {}  # pipeline_id -> int

    def _can_schedule_more_from_pipeline(p):
        pid = p.pipeline_id
        lim = s.max_ops_per_pipeline_per_tick.get(p.priority, 1)
        return scheduled_counts.get(pid, 0) < lim

    def _mark_scheduled(p):
        pid = p.pipeline_id
        scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

    def _pipeline_should_give_up(p):
        pid = p.pipeline_id
        # Only give up if it's clearly thrashing; otherwise keep trying to maximize completion rate.
        if s.pipeline_fail_counts.get(pid, 0) >= s.max_pipeline_failures:
            return True
        return False

    def _pick_next_from_queue(q, pool, avail_cpu, avail_ram, pri):
        if not q:
            return None, None, None, None

        best_p = None
        best_op = None
        best_cpu = None
        best_ram = None
        best_score = None

        scanned = []
        lim = s.scan_limit
        if lim > len(q):
            lim = len(q)

        for _ in range(lim):
            p = q.pop(0)
            pid = p.pipeline_id

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            if _pipeline_should_give_up(p):
                # Leave it enqueued (so it counts as incomplete), but stop spending resources on it.
                # (We don't drop it to avoid any simulator-side assumptions.)
                scanned.append(p)
                continue

            if not _can_schedule_more_from_pipeline(p):
                scanned.append(p)
                continue

            wait_ticks = _now_wait_ticks(pid)

            op, cpu_req, ram_req = _choose_best_runnable_op(p, pool, avail_cpu, avail_ram, wait_ticks)
            if op is None:
                scanned.append(p)
                continue

            if pri == Priority.BATCH_PIPELINE:
                if not _batch_reservation_ok(pool, avail_cpu, avail_ram, cpu_req, ram_req):
                    scanned.append(p)
                    continue

            # Prefer pipelines closer to completion, then older, then ones with prior failures.
            rem = _pipeline_remaining_ops(p)
            fail_bonus = 0
            opk = _op_key(op)
            fail_bonus = s.op_fail_counts.get((pid, opk), 0)
            score = (rem, -wait_ticks, -fail_bonus)

            if best_p is None or score < best_score:
                best_p, best_op, best_cpu, best_ram, best_score = p, op, cpu_req, ram_req, score

            scanned.append(p)

        # Restore scanned items to queue (preserve relative order after scanning window)
        for p in scanned:
            q.append(p)

        if best_p is None:
            return None, None, None, None

        # Move the chosen pipeline to the back for fairness
        try:
            idx = q.index(best_p)
            chosen = q.pop(idx)
            q.append(chosen)
        except Exception:
            pass

        return best_p, best_op, best_cpu, best_ram

    def _pick_priority_order():
        # Always prioritize queries. Between interactive and batch, use weighted fairness.
        # Default ratio ~5:1 in favor of interactive, with aging boost for old batch.
        batch_q = s.wait_q[Priority.BATCH_PIPELINE]
        batch_oldest = 0
        if batch_q:
            # bounded scan for oldest wait
            lim = 12 if len(batch_q) > 12 else len(batch_q)
            for i in range(lim):
                pid = batch_q[i].pipeline_id
                w = _now_wait_ticks(pid)
                if w > batch_oldest:
                    batch_oldest = w

        # If batch has been waiting a long time, increase its share.
        if batch_oldest >= 800:
            denom = 3  # ~2:1 interactive:batch
        elif batch_oldest >= 400:
            denom = 4  # ~3:1
        else:
            denom = 6  # ~5:1

        s.turn += 1
        if (s.turn % denom) == 0:
            return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # --- Main scheduling loop: fill each pool with many small, well-packed assignments ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Schedule until no further progress can be made in this pool.
        while avail_cpu >= 1 and avail_ram >= 1:
            made = False
            for pri in _pick_priority_order():
                q = _queue_for_priority(pri)
                p, op, cpu_req, ram_req = _pick_next_from_queue(q, pool, avail_cpu, avail_ram, pri)
                if p is None:
                    continue

                # Clip CPU to availability at the last moment; RAM must fit exactly.
                if cpu_req > avail_cpu:
                    cpu_req = _int_cpu(avail_cpu)
                if ram_req > avail_ram:
                    continue
                if cpu_req < 1:
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                s.op_to_pipeline[_op_key(op)] = p.pipeline_id
                _mark_scheduled(p)

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                made = True
                break

            if not made:
                break

    return suspensions, assignments
