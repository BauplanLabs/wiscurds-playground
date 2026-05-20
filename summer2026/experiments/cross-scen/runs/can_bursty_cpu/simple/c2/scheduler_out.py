@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler tuned for bursty CPU-bound workloads.

    Key changes vs prior version:
      - Right-size CPU (avoid giving huge fractions of the whole pool to single ops).
      - Right-size RAM (avoid massive over-allocation that reduces concurrency).
      - Strict priority for QUERY/INTERACTIVE with controlled batch admission (no batch>interactive boosting).
      - Adaptive retries:
          * On OOM: increase RAM hint (multiplicative).
          * On timeout-like failure: increase CPU hint (multiplicative).
      - Short-pipeline bias within each priority (approx SRPT) + aging.
      - Works well across cluster sizes by capping per-op CPU/RAM.
    """
    s.ticks = 0

    # Queues store pipeline_ids for stability (pipeline objects may be re-sent/updated each step).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    s.enqueued = set()
    s.pipeline_meta = {}  # pid -> {"enqueued_tick": int}
    s.pipelines_by_id = {}  # pid -> Pipeline (latest object)

    # Resource hints per operator (identified by python object identity).
    # key: (pipeline_id, op_key) -> hint
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Map op_key -> pipeline_id for attributing results.
    s.op_to_pipeline = {}

    # Failure bookkeeping.
    s.pipeline_failures = {}  # pid -> int
    s.dead_pipelines = set()

    # Policy knobs
    s.scan_limit = 64  # how many queue items to consider per pick
    s.batch_quanta = 10  # allow 1 batch op after this many high-priority ops scheduled (when HP backlog exists)
    s.batch_starve_ticks = 60  # if batch waits too long (in scheduler ticks), ignore batch gating
    s.max_failures_before_dead = 8  # only for non-resource errors


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _get_param(name, default=None):
        params = getattr(s, "params", None)
        if params is None:
            return default
        try:
            if isinstance(params, dict):
                return params.get(name, default)
        except Exception:
            pass
        return getattr(params, name, default)

    def _op_key(op):
        # Use object identity to avoid collisions across pipelines with similar op_id fields.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _queue(pri):
        return s.wait_q[pri]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_failures.pop(pid, None)
        # Lazy removal from queues (skipped when encountered).

    def _wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - int(meta.get("enqueued_tick", s.ticks)))

    def _remaining_ops_estimate(status):
        # Approximate remaining work as count of not-completed operators.
        # Use broad states; require_parents_complete=False to count blocked nodes too.
        try:
            states = {
                OperatorState.PENDING,
                OperatorState.FAILED,
                OperatorState.ASSIGNED,
                OperatorState.RUNNING,
                OperatorState.SUSPENDING,
            }
            return len(status.get_ops(states, require_parents_complete=False))
        except Exception:
            return 999999

    def _pool_cpu_scale(pool):
        mc = float(pool.max_cpu_pool)
        if mc >= 1024:
            return 2.0
        if mc >= 512:
            return 1.75
        if mc >= 256:
            return 1.5
        if mc >= 128:
            return 1.25
        return 1.0

    def _default_cpu(pool, pri, effective_backlog):
        # Favor moderate per-op CPU to maximize throughput under sublinear scaling.
        scale = _pool_cpu_scale(pool)
        if pri == Priority.QUERY:
            base = int(8 * scale)
            min_cpu, max_cpu = 4, 20
        elif pri == Priority.INTERACTIVE:
            base = int(8 * scale)
            min_cpu, max_cpu = 4, 16
        else:
            base = int(4 * scale)
            min_cpu, max_cpu = 2, 12

        # Under high backlog, shrink to increase concurrency.
        # effective_backlog is roughly "how many ops want service".
        if pool.max_cpu_pool > 0:
            # Rough "capacity in ops" at this base size.
            cap_ops = max(1, int(pool.max_cpu_pool / max(1, base)))
            if effective_backlog > cap_ops * 3:
                base = max(min_cpu, int(base * 0.65))
            elif effective_backlog > cap_ops * 2:
                base = max(min_cpu, int(base * 0.80))

        if base < min_cpu:
            base = min_cpu
        if base > max_cpu:
            base = max_cpu
        if base > pool.max_cpu_pool:
            base = int(pool.max_cpu_pool)
        if base < 1:
            base = 1
        return base

    def _default_ram(pool, pri, cpu_req):
        # Start small to avoid RAM head-of-line blocking; rely on OOM-driven hints to increase.
        # Cap to prevent massive over-allocation on large-memory pools.
        if pri == Priority.QUERY:
            base, per_cpu, cap = 16.0, 1.5, 96.0
        elif pri == Priority.INTERACTIVE:
            base, per_cpu, cap = 12.0, 1.5, 72.0
        else:
            base, per_cpu, cap = 10.0, 1.0, 64.0

        ram = max(base, float(cpu_req) * per_cpu)

        # Also allow a tiny pool-proportional floor, but keep it bounded.
        try:
            ram = max(ram, min(24.0, float(pool.max_ram_pool) * 0.005))
        except Exception:
            pass

        if ram > cap:
            ram = cap
        if ram > pool.max_ram_pool:
            ram = float(pool.max_ram_pool)
        if ram < 1.0:
            ram = 1.0
        return ram

    def _size_request(pool, pri, pid, op, effective_backlog):
        opk = _op_key(op)

        # CPU
        cpu_hint = s.op_cpu_hint.get((pid, opk))
        if cpu_hint is None:
            cpu_req = _default_cpu(pool, pri, effective_backlog)
        else:
            cpu_req = int(cpu_hint)

        # RAM
        ram_hint = s.op_ram_hint.get((pid, opk))
        if ram_hint is None:
            ram_req = _default_ram(pool, pri, cpu_req)
        else:
            ram_req = float(ram_hint)

        # Clamp to pool capacity
        if cpu_req > pool.max_cpu_pool:
            cpu_req = int(pool.max_cpu_pool)
        if cpu_req < 1:
            cpu_req = 1
        if ram_req > pool.max_ram_pool:
            ram_req = float(pool.max_ram_pool)
        if ram_req < 1.0:
            ram_req = 1.0

        return int(cpu_req), float(ram_req)

    # --- Ingest/update pipelines ---
    for p in pipelines or []:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid in s.dead_pipelines:
            continue

        # If pipeline already finished, don't enqueue.
        try:
            if p.runtime_status().is_pipeline_successful():
                _drop_pipeline(pid)
                continue
        except Exception:
            pass

        if pid not in s.enqueued:
            _queue(p.priority).append(pid)
            s.enqueued.add(pid)
            s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}

    # --- Process results: adjust hints on resource failures ---
    for r in results or []:
        if not (hasattr(r, "ops") and r.ops):
            continue

        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = False

        for op in r.ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue

            if failed:
                s.pipeline_failures[pid] = int(s.pipeline_failures.get(pid, 0)) + 1

                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    prev = s.op_ram_hint.get((pid, opk))
                    alloc = getattr(r, "ram", None)
                    base = float(alloc) if (alloc is not None and float(alloc) > 0) else (float(prev) if prev is not None else 1.0)
                    new_hint = max(base * 1.8, (float(prev) * 1.5 if prev is not None else 0.0), 1.0)
                    # Hard cap: never exceed the largest pool RAM (if available).
                    try:
                        max_pool_ram = max(float(s.executor.pools[i].max_ram_pool) for i in range(s.executor.num_pools))
                        if new_hint > max_pool_ram:
                            new_hint = max_pool_ram
                    except Exception:
                        pass
                    s.op_ram_hint[(pid, opk)] = new_hint

                elif _is_timeout_error(err):
                    prev = s.op_cpu_hint.get((pid, opk))
                    alloc = getattr(r, "cpu", None)
                    base = int(alloc) if (alloc is not None and int(alloc) > 0) else (int(prev) if prev is not None else 1)
                    new_hint = max(int(base * 2), int(prev * 2) if prev is not None else 0, 1)
                    # Cap against biggest pool CPU
                    try:
                        max_pool_cpu = max(int(s.executor.pools[i].max_cpu_pool) for i in range(s.executor.num_pools))
                        if new_hint > max_pool_cpu:
                            new_hint = max_pool_cpu
                    except Exception:
                        pass
                    s.op_cpu_hint[(pid, opk)] = new_hint

                else:
                    # Only kill on repeated non-resource errors to avoid dropping lots of work.
                    if int(s.pipeline_failures.get(pid, 0)) >= int(s.max_failures_before_dead):
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

    # Fast-path exit
    if not (pipelines or results):
        return [], []

    suspensions = []
    assignments = []

    # --- Helpers for picking work ---
    scheduled_pipeline_ids = set()

    q_query = _queue(Priority.QUERY)
    q_inter = _queue(Priority.INTERACTIVE)
    q_batch = _queue(Priority.BATCH_PIPELINE)

    def _effective_backlog():
        # Weighted backlog to decide how aggressively to shrink CPU sizing.
        return int(len(q_query) * 4 + len(q_inter) * 2 + len(q_batch) * 1)

    def _hp_backlog():
        return (len(q_query) + len(q_inter)) > 0

    s.hp_ops_since_batch = int(getattr(s, "hp_ops_since_batch", 0))

    def _batch_is_starved():
        # If any batch has waited "too long", allow it even if HP backlog exists.
        for pid in q_batch:
            if pid in s.dead_pipelines:
                continue
            if _wait_ticks(pid) >= int(s.batch_starve_ticks):
                return True
        return False

    def _pick_next(pri, pool, avail_cpu, avail_ram, effective_backlog):
        """
        Returns (pid, op, cpu_req, ram_req) or (None, None, None, None).
        Uses round-robin scanning + short-pipeline bias + aging within the scanned window.
        """
        q = _queue(pri)
        if not q:
            return None, None, None, None

        best = None  # tuple(score, pid, op, cpu_req, ram_req)
        scan = min(len(q), int(s.scan_limit))

        for _ in range(scan):
            pid = q.pop(0)
            q.append(pid)

            if pid in s.dead_pipelines:
                continue
            if pid in scheduled_pipeline_ids:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            try:
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass

            try:
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = []

            if not ops:
                continue

            op = ops[0]

            cpu_req, ram_req = _size_request(pool, pri, pid, op, effective_backlog)

            if ram_req > float(avail_ram):
                continue

            # Allow shrinking CPU to fit, but avoid giving too little to be useful.
            if cpu_req > int(avail_cpu):
                cpu_req = int(avail_cpu)
            if cpu_req < 1:
                continue

            # Score: older pipelines and shorter remaining work first.
            age = _wait_ticks(pid)
            remaining = _remaining_ops_estimate(status)

            # Priority-specific weights (queries strongly favor low latency; batch favors throughput but still ages).
            if pri == Priority.QUERY:
                score = age * 6 - remaining * 2 - int(s.pipeline_failures.get(pid, 0)) * 3
            elif pri == Priority.INTERACTIVE:
                score = age * 4 - remaining * 2 - int(s.pipeline_failures.get(pid, 0)) * 2
            else:
                score = age * 2 - remaining * 1 - int(s.pipeline_failures.get(pid, 0)) * 1

            if (best is None) or (score > best[0]):
                best = (score, pid, op, int(cpu_req), float(ram_req))

        if best is None:
            return None, None, None, None

        _, pid, op, cpu_req, ram_req = best
        return pid, op, cpu_req, ram_req

    # --- Main scheduling loop: fill each pool while respecting priority + controlled batch admission ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Keep assigning while resources remain.
        while avail_cpu >= 1 and avail_ram >= 1.0:
            effective_backlog = _effective_backlog()
            hp_backlog = _hp_backlog()
            batch_starved = _batch_is_starved()

            # Batch gating: when HP backlog exists, only allow batch occasionally (quanta) or if starved.
            allow_batch_now = (not hp_backlog) or batch_starved or (s.hp_ops_since_batch >= int(s.batch_quanta))

            picked = False

            # 1) QUERY
            pid, op, cpu_req, ram_req = _pick_next(Priority.QUERY, pool, avail_cpu, avail_ram, effective_backlog)
            if pid is not None:
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=int(cpu_req),
                        ram=float(ram_req),
                        priority=Priority.QUERY,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )
                s.op_to_pipeline[_op_key(op)] = pid
                avail_cpu -= int(cpu_req)
                avail_ram -= float(ram_req)
                scheduled_pipeline_ids.add(pid)
                s.hp_ops_since_batch += 1
                picked = True
                continue

            # 2) INTERACTIVE
            pid, op, cpu_req, ram_req = _pick_next(Priority.INTERACTIVE, pool, avail_cpu, avail_ram, effective_backlog)
            if pid is not None:
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=int(cpu_req),
                        ram=float(ram_req),
                        priority=Priority.INTERACTIVE,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )
                s.op_to_pipeline[_op_key(op)] = pid
                avail_cpu -= int(cpu_req)
                avail_ram -= float(ram_req)
                scheduled_pipeline_ids.add(pid)
                s.hp_ops_since_batch += 1
                picked = True
                continue

            # 3) BATCH (only if allowed)
            if allow_batch_now:
                pid, op, cpu_req, ram_req = _pick_next(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram, effective_backlog)
                if pid is not None:
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=int(cpu_req),
                            ram=float(ram_req),
                            priority=Priority.BATCH_PIPELINE,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )
                    s.op_to_pipeline[_op_key(op)] = pid
                    avail_cpu -= int(cpu_req)
                    avail_ram -= float(ram_req)
                    scheduled_pipeline_ids.add(pid)
                    s.hp_ops_since_batch = 0
                    picked = True
                    continue

            if not picked:
                break

    return suspensions, assignments
