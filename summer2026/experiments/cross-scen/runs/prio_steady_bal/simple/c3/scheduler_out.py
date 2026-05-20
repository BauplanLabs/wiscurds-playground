@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved policy for priority-heavy steady workload:
      - Avoids massive per-op over-allocation (was causing extreme head-of-line blocking).
      - Guarantees INTERACTIVE progress under QUERY floods via pool partitioning + weighted fairness.
      - Allows limited parallelism within a pipeline (important for DAGs with multiple ready ops).
      - OOM-aware RAM ramp (fast convergence) with conservative starting RAM to keep OOM rare.
      - Light SRPT/aging bias within each priority to improve completion rate (timeouts dominate score).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()
    s.pipeline_by_id = {}
    s.pipeline_meta = {}  # pid -> {enqueued_tick, total_ops_est}

    # Per-op hints (mainly for retries)
    s.op_ram_hint = {}  # (pid, opk) -> ram
    s.op_cpu_hint = {}  # (pid, opk) -> cpu
    s.op_to_pipeline = {}  # opk -> pid

    # Failure tracking
    s.pipeline_non_oom_failures = {}  # pid -> count
    s.op_oom_failures = {}  # (pid, opk) -> count
    s.dead_pipelines = set()

    # Weighted fair scheduling (per pool)
    s.deficit = {}  # pool_id -> {pri -> deficit_cpu_units}
    s.deficit_cap_mult = 6.0

    # Quanta tuned to strongly protect INTERACTIVE completion while keeping QUERY latency good
    # (Deficits are in "CPU units per scheduler tick")
    s.quantum = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 14.0,
        Priority.BATCH_PIPELINE: 3.0,
    }

    # Scanning/selection knobs
    s.scan_limit = 28          # scan this many queue entries to find a good fit
    s.ready_ops_scan = 6       # scan this many ready ops within a pipeline for "best fit"
    s.compact_every = 37       # periodic lazy queue compaction

    # Per-pipeline parallelism cap per scheduler step (across all pools)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }
    s.max_total_ops_per_pipeline_per_tick = 4

    # Starvation / urgency knobs
    s.interactive_starve_ticks = 10
    s.batch_starve_ticks = 60

    # Retry knobs
    s.max_non_oom_pipeline_failures = 2
    s.max_oom_retries_per_op = 6


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _get_param(name, default=None):
        prm = getattr(s, "params", None)
        if prm is None:
            return default
        if isinstance(prm, dict):
            return prm.get(name, default)
        return getattr(prm, name, default)

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cannot allocate memory" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _estimate_total_ops(p):
        # Best-effort estimate; only used for mild SRPT bias.
        vals = getattr(p, "values", None)
        if vals is None:
            return 1
        try:
            return max(1, len(vals))
        except Exception:
            return 1

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_by_id.pop(pid, None)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_non_oom_failures.pop(pid, None)

    def _pipeline_age(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _compact_queues():
        # Remove dead/nonexistent pipeline ids to keep scans efficient.
        for pri, q in s.wait_q.items():
            if not q:
                continue
            new_q = []
            for pid in q:
                if pid in s.dead_pipelines:
                    continue
                if pid not in s.enqueued:
                    continue
                if pid not in s.pipeline_by_id:
                    continue
                new_q.append(pid)
            s.wait_q[pri] = new_q

    def _estimate_remaining_ops(status):
        # Count non-completed ops best-effort; used only for tie-breaking.
        try:
            states = [
                OperatorState.PENDING,
                OperatorState.ASSIGNED,
                OperatorState.RUNNING,
                OperatorState.SUSPENDING,
                OperatorState.FAILED,
            ]
            ops = status.get_ops(states, require_parents_complete=False)
            return max(1, len(ops))
        except Exception:
            return 1

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _pool_roles(num_pools):
        """
        Partition pools to guarantee INTERACTIVE progress:
          - If >=2 pools: at least 1 INTERACTIVE pool when interactive backlog exists.
          - Prefer keeping at least one QUERY-focused pool.
          - Batch gets a dedicated pool only at larger sizes/backlog.
        """
        has_q = len(s.wait_q[Priority.QUERY]) > 0
        has_i = len(s.wait_q[Priority.INTERACTIVE]) > 0
        has_b = len(s.wait_q[Priority.BATCH_PIPELINE]) > 0

        if num_pools <= 1:
            return ["MIXED"]

        # Defaults biased toward interactive to improve completion rate (timeouts dominate score).
        n_i = 1 if has_i else 0
        n_b = 1 if (has_b and num_pools >= 6) else 0

        # Increase interactive pools if backlog is large vs query.
        if has_i:
            qi = max(1, len(s.wait_q[Priority.QUERY]))
            ii = len(s.wait_q[Priority.INTERACTIVE])
            if ii > qi // 2 and num_pools >= 4:
                n_i = max(n_i, 2)
            if ii > qi and num_pools >= 8:
                n_i = max(n_i, 3)

        n_q = num_pools - n_i - n_b
        if has_q:
            n_q = max(1, n_q)
        else:
            n_q = max(0, n_q)

        # Fix sum if over/under
        while n_q + n_i + n_b > num_pools:
            if n_q > 1:
                n_q -= 1
            elif n_b > 0:
                n_b -= 1
            else:
                n_i = max(0, n_i - 1)

        while n_q + n_i + n_b < num_pools:
            if has_i:
                n_i += 1
            else:
                n_q += 1

        roles = []
        roles.extend(["INTERACTIVE"] * n_i)
        roles.extend(["QUERY"] * n_q)
        roles.extend(["BATCH"] * n_b)

        # If we somehow got none query but queries exist, steal one from batch/interactive.
        if has_q and "QUERY" not in roles:
            for idx in range(len(roles)):
                if roles[idx] != "QUERY":
                    roles[idx] = "QUERY"
                    break
        return roles[:num_pools]

    def _base_sizes(pool, role, pri, age_ticks, remaining_ops_est):
        """
        Pick conservative-but-not-tiny defaults:
          - QUERY: small CPU + modest RAM to maximize concurrency (queries are many/short).
          - INTERACTIVE: more CPU/RAM to hit completion; still capped for concurrency.
          - BATCH: moderate sizes.
        Role biases:
          - INTERACTIVE pool gives interactive more CPU, query very small (backfill only).
          - QUERY pool keeps query responsive, still allows interactive progress.
        """
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # RAM baselines (GB-ish units in sim)
        if pri == Priority.QUERY:
            ram0 = 14.0
        elif pri == Priority.INTERACTIVE:
            ram0 = 44.0
        else:
            ram0 = 28.0

        # CPU baselines
        if pri == Priority.QUERY:
            cpu0 = max_cpu * 0.10
        elif pri == Priority.INTERACTIVE:
            cpu0 = max_cpu * 0.22
        else:
            cpu0 = max_cpu * 0.16

        # Role adjustments
        if role == "INTERACTIVE":
            if pri == Priority.INTERACTIVE:
                cpu0 = max(cpu0, max_cpu * 0.26)
                ram0 = max(ram0, 52.0)
            elif pri == Priority.QUERY:
                cpu0 = min(cpu0, max_cpu * 0.07)
        elif role == "QUERY":
            if pri == Priority.QUERY:
                cpu0 = max(cpu0, max_cpu * 0.12)
            elif pri == Priority.INTERACTIVE:
                cpu0 = max(cpu0, max_cpu * 0.18)
        elif role == "BATCH":
            if pri == Priority.BATCH_PIPELINE:
                cpu0 = max(cpu0, max_cpu * 0.20)
                ram0 = max(ram0, 36.0)
            elif pri == Priority.QUERY:
                cpu0 = min(cpu0, max_cpu * 0.08)

        # SRPT-ish: if only 1 op left, slightly boost CPU to finish pipelines (reduces penalties)
        if remaining_ops_est <= 1:
            cpu0 *= 1.25

        # Aging: if waited long, boost CPU (esp. INTERACTIVE)
        if age_ticks >= s.batch_starve_ticks and pri == Priority.BATCH_PIPELINE:
            cpu0 *= 1.35
            ram0 *= 1.15
        if age_ticks >= s.interactive_starve_ticks and pri == Priority.INTERACTIVE:
            cpu0 *= 1.25

        # Clamp to sane per-op caps (avoid old "one op eats a whole pool" issue)
        if pri == Priority.QUERY:
            cpu_min, cpu_max = 2.0, max(6.0, max_cpu * 0.25)
            ram_min, ram_max = 6.0, max(24.0, max_ram * 0.18)
        elif pri == Priority.INTERACTIVE:
            cpu_min, cpu_max = 4.0, max(16.0, max_cpu * 0.55)
            ram_min, ram_max = 16.0, max(80.0, max_ram * 0.30)
        else:
            cpu_min, cpu_max = 2.0, max(10.0, max_cpu * 0.40)
            ram_min, ram_max = 10.0, max(64.0, max_ram * 0.26)

        cpu0 = _clamp(cpu0, cpu_min, cpu_max)
        ram0 = _clamp(ram0, ram_min, ram_max)

        # Final clamp to pool maxima
        cpu0 = _clamp(cpu0, 1.0, float(max_cpu))
        ram0 = _clamp(ram0, 1.0, float(max_ram))
        return cpu0, ram0

    def _size_request(pool, role, pri, pid, op):
        opk = _op_key(op)
        age = _pipeline_age(pid)

        p = s.pipeline_by_id.get(pid)
        remaining = 1
        if p is not None:
            try:
                remaining = _estimate_remaining_ops(p.runtime_status())
            except Exception:
                remaining = 1

        # Start from baseline; then apply per-op hints if any
        cpu0, ram0 = _base_sizes(pool, role, pri, age, remaining)

        hint_ram = s.op_ram_hint.get((pid, opk))
        hint_cpu = s.op_cpu_hint.get((pid, opk))

        if hint_ram is not None:
            ram0 = max(ram0, float(hint_ram))
        if hint_cpu is not None:
            cpu0 = max(cpu0, float(hint_cpu))

        # Avoid absurd values
        cpu0 = _clamp(cpu0, 1.0, float(pool.max_cpu_pool))
        ram0 = _clamp(ram0, 1.0, float(pool.max_ram_pool))

        # Round to cleaner units; simulator accepts floats but integers are safer.
        cpu_req = int(cpu0 + 0.9999)
        ram_req = int(ram0 + 0.9999)

        cpu_req = max(1, min(cpu_req, int(pool.max_cpu_pool)))
        ram_req = max(1, min(ram_req, int(pool.max_ram_pool)))
        return cpu_req, ram_req

    def _init_deficit_if_needed(pool_id):
        if pool_id in s.deficit:
            return
        s.deficit[pool_id] = {
            Priority.QUERY: 0.0,
            Priority.INTERACTIVE: 0.0,
            Priority.BATCH_PIPELINE: 0.0,
        }

    def _add_quanta(pool, pool_id, interactive_urgent=False, batch_urgent=False):
        _init_deficit_if_needed(pool_id)
        scale = float(pool.max_cpu_pool) / 64.0 if getattr(pool, "max_cpu_pool", 0) else 1.0
        if scale <= 0:
            scale = 1.0

        # Urgency boosts (completion rate)
        q_i = s.quantum[Priority.INTERACTIVE] * (1.45 if interactive_urgent else 1.0)
        q_b = s.quantum[Priority.BATCH_PIPELINE] * (1.30 if batch_urgent else 1.0)
        q_q = s.quantum[Priority.QUERY]

        s.deficit[pool_id][Priority.QUERY] += q_q * scale
        s.deficit[pool_id][Priority.INTERACTIVE] += q_i * scale
        s.deficit[pool_id][Priority.BATCH_PIPELINE] += q_b * scale

        cap = float(pool.max_cpu_pool) * float(s.deficit_cap_mult)
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.deficit[pool_id][pri] > cap:
                s.deficit[pool_id][pri] = cap

    def _approx_oldest_age(pri, sample=10):
        q = s.wait_q[pri]
        if not q:
            return 0
        m = 0
        n = min(sample, len(q))
        for i in range(n):
            pid = q[i]
            if pid in s.pipeline_meta:
                age = _pipeline_age(pid)
                if age > m:
                    m = age
        return m

    def _pick_pipeline_and_ops(pri, pool, role, avail_cpu, avail_ram, scheduled_counts):
        """
        Scan a bounded window of the queue and pick a pipeline with ready ops that fit.
        Mild bias toward:
          - fewer remaining ops (SRPT-ish)
          - older pipelines (aging)
        Then return a list of ops to schedule (up to per-pipeline cap and fit constraints).
        """
        q = s.wait_q[pri]
        if not q:
            return None, []

        best_pid = None
        best_ops = []
        best_key = None  # (remaining_ops, -age, idx)
        scanned = 0

        # Rotate while scanning so we keep round-robin fairness.
        limit = min(len(q), s.scan_limit)
        for idx in range(limit):
            pid = q.pop(0)
            q.append(pid)

            scanned += 1
            if pid in s.dead_pipelines:
                continue
            if pid not in s.enqueued:
                continue

            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue

            # Pipeline completion check
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                status = None

            # Cap per-pipeline scheduling per tick (across pools)
            if scheduled_counts.get(pid, 0) >= s.max_total_ops_per_pipeline_per_tick:
                continue

            # Get ready ops
            ready_ops = []
            try:
                ready_ops = p.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                ready_ops = []

            if not ready_ops:
                continue

            age = _pipeline_age(pid)
            remaining_est = 1
            try:
                remaining_est = _estimate_remaining_ops(p.runtime_status())
            except Exception:
                remaining_est = s.pipeline_meta.get(pid, {}).get("total_ops_est", 1) or 1

            # Find a subset of ready ops that fit now; prefer smaller RAM to pack.
            # Also enforce per-priority/pipeline per-tick cap.
            per_pri_cap = s.max_ops_per_pipeline_per_tick.get(pri, 1)
            already = scheduled_counts.get(pid, 0)
            cap_left = max(0, min(per_pri_cap, s.max_total_ops_per_pipeline_per_tick) - already)
            if cap_left <= 0:
                continue

            candidates = []
            scan_ops = ready_ops[: max(1, s.ready_ops_scan)]
            for op in scan_ops:
                cpu_req, ram_req = _size_request(pool, role, pri, pid, op)
                if cpu_req <= avail_cpu and ram_req <= avail_ram:
                    candidates.append((ram_req, cpu_req, op))
            if not candidates:
                continue

            # Select up to cap_left ops, smallest RAM first to maximize packing.
            candidates.sort(key=lambda x: (x[0], x[1]))
            chosen = [t[2] for t in candidates[:cap_left]]

            key = (int(remaining_est), -int(age), idx)
            if best_key is None or key < best_key:
                best_key = key
                best_pid = pid
                best_ops = chosen

        return best_pid, best_ops

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            # Refresh object reference (state updates in-place, but be safe).
            s.pipeline_by_id[pid] = p
            continue
        s.enqueued.add(pid)
        s.pipeline_by_id[pid] = p
        s.pipeline_meta[pid] = {
            "enqueued_tick": s.ticks,
            "total_ops_est": _estimate_total_ops(p),
        }
        _queue_for_priority(p.priority).append(pid)

    # Periodic queue compaction
    if s.ticks % s.compact_every == 0:
        _compact_queues()

    # --- Process results (OOM ramp + limited non-OOM retries) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        is_failed = False
        try:
            is_failed = bool(r.failed())
        except Exception:
            is_failed = bool(getattr(r, "error", None))

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.pop(opk, None)  # cleanup mapping as soon as we can
            if pid is None:
                continue

            if pid in s.dead_pipelines:
                continue

            if is_failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    # Fast RAM ramp with a safety floor; clamp to pool max later.
                    alloc = getattr(r, "ram", None)
                    prev = s.op_ram_hint.get((pid, opk), None)
                    base = None
                    if alloc is not None and alloc > 0:
                        base = float(alloc)
                    elif prev is not None and prev > 0:
                        base = float(prev)
                    else:
                        base = 16.0

                    # Increase by 1.8x (slightly gentler than 2x to reduce overshoot),
                    # but ensure meaningful jump.
                    new_hint = max(base * 1.8, base + 12.0)
                    s.op_ram_hint[(pid, opk)] = new_hint

                    # Also slightly bump CPU on repeated OOM (often correlated with spill/pressure patterns).
                    of = s.op_oom_failures.get((pid, opk), 0) + 1
                    s.op_oom_failures[(pid, opk)] = of
                    if of >= 2:
                        prev_cpu = s.op_cpu_hint.get((pid, opk), None)
                        alloc_cpu = getattr(r, "cpu", None)
                        cpu_base = float(alloc_cpu) if (alloc_cpu is not None and alloc_cpu > 0) else (float(prev_cpu) if prev_cpu else 6.0)
                        s.op_cpu_hint[(pid, opk)] = max(cpu_base * 1.25, cpu_base + 2.0)

                    # If hopelessly retrying, give up on this op/pipeline.
                    if of >= s.max_oom_retries_per_op:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    # Non-OOM failure: allow a couple retries, then stop.
                    s.pipeline_non_oom_failures[pid] = s.pipeline_non_oom_failures.get(pid, 0) + 1
                    if s.pipeline_non_oom_failures[pid] > s.max_non_oom_pipeline_failures:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success: optionally reduce inflated hints a bit (avoid persistent over-allocation on retries).
                prev = s.op_ram_hint.get((pid, opk), None)
                if prev is not None:
                    # Gentle decay toward baseline over time (only if it was heavily inflated).
                    s.op_ram_hint[(pid, opk)] = max(8.0, float(prev) * 0.95)

    # If nothing to do, exit quickly
    if (not pipelines) and (not results) and (not s.wait_q[Priority.QUERY]) and (not s.wait_q[Priority.INTERACTIVE]) and (not s.wait_q[Priority.BATCH_PIPELINE]):
        return [], []

    suspensions = []
    assignments = []

    # Urgency signals
    interactive_urgent = _approx_oldest_age(Priority.INTERACTIVE) >= s.interactive_starve_ticks
    batch_urgent = _approx_oldest_age(Priority.BATCH_PIPELINE) >= s.batch_starve_ticks

    # Per-tick per-pipeline schedule cap
    scheduled_counts = {}

    # Determine pool roles for this tick
    roles = _pool_roles(s.executor.num_pools)

    # Main scheduling loop per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        role = roles[pool_id] if pool_id < len(roles) else "MIXED"

        _add_quanta(pool, pool_id, interactive_urgent=interactive_urgent, batch_urgent=batch_urgent)

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Keep a small set of "no progress" priorities to avoid infinite loops when nothing fits.
        stalled = {
            Priority.QUERY: 0,
            Priority.INTERACTIVE: 0,
            Priority.BATCH_PIPELINE: 0,
        }

        def _pri_order():
            # Primary role first, but allow deficit-driven choices.
            if role == "INTERACTIVE":
                primary = Priority.INTERACTIVE
            elif role == "BATCH":
                primary = Priority.BATCH_PIPELINE
            elif role == "QUERY":
                primary = Priority.QUERY
            else:
                # Mixed: if interactive urgent, bias interactive slightly
                primary = Priority.INTERACTIVE if interactive_urgent else Priority.QUERY

            # Build list sorted by (is_primary, deficit desc, urgency)
            pris = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            def score(pri):
                prim = 1 if pri == primary else 0
                deficit = s.deficit.get(pool_id, {}).get(pri, 0.0)
                urg = 0
                if pri == Priority.INTERACTIVE and interactive_urgent:
                    urg = 1
                if pri == Priority.BATCH_PIPELINE and batch_urgent:
                    urg = 1
                return (prim, urg, deficit)

            pris.sort(key=lambda p: score(p), reverse=True)
            return pris

        # Fill pool resources
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            progressed = False

            for pri in _pri_order():
                if stalled[pri] >= 3:
                    continue
                if not s.wait_q[pri]:
                    stalled[pri] += 1
                    continue

                # Need enough deficit to justify scheduling; but allow some work-conserving behavior
                # to avoid idling when deficits are temporarily low.
                deficit = s.deficit.get(pool_id, {}).get(pri, 0.0)
                if deficit < 1.0 and (len(s.wait_q[pri]) > 0):
                    # Work-conserving: allow scheduling anyway, but don't go too negative.
                    pass

                pid, ops_to_schedule = _pick_pipeline_and_ops(pri, pool, role, avail_cpu, avail_ram, scheduled_counts)
                if pid is None or not ops_to_schedule:
                    stalled[pri] += 1
                    continue

                p = s.pipeline_by_id.get(pid)
                if p is None:
                    stalled[pri] += 1
                    continue

                # Schedule selected ops (usually 1, sometimes a few)
                for op in ops_to_schedule:
                    if avail_cpu < 1.0 or avail_ram < 1.0:
                        break

                    # Enforce global per-pipeline cap
                    if scheduled_counts.get(pid, 0) >= s.max_total_ops_per_pipeline_per_tick:
                        break

                    cpu_req, ram_req = _size_request(pool, role, pri, pid, op)

                    # If it doesn't fit, skip it (try other op/pipeline)
                    if cpu_req > avail_cpu or ram_req > avail_ram:
                        continue

                    # If deficit is small, cap CPU a bit (helps prevent one class from consuming all in a burst)
                    deficit_now = s.deficit.get(pool_id, {}).get(pri, 0.0)
                    if deficit_now < cpu_req and deficit_now > 0:
                        # Don't shrink interactive too aggressively; queries can shrink more.
                        if pri == Priority.QUERY:
                            cpu_req = max(1, int(deficit_now))
                        elif pri == Priority.BATCH_PIPELINE:
                            cpu_req = max(1, int(min(cpu_req, deficit_now + 2)))
                        else:
                            cpu_req = max(2, cpu_req)  # keep interactive reasonably sized

                        if cpu_req > avail_cpu:
                            continue

                    assignment = Assignment(
                        ops=[op],
                        cpu=int(cpu_req),
                        ram=int(ram_req),
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                    assignments.append(assignment)

                    # Track mapping so results can be attributed
                    s.op_to_pipeline[_op_key(op)] = pid

                    # Update local resources and deficits
                    avail_cpu -= float(cpu_req)
                    avail_ram -= float(ram_req)

                    _init_deficit_if_needed(pool_id)
                    s.deficit[pool_id][pri] -= float(cpu_req)
                    if s.deficit[pool_id][pri] < -float(pool.max_cpu_pool):
                        s.deficit[pool_id][pri] = -float(pool.max_cpu_pool)

                    scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                    progressed = True

                if progressed:
                    break

            if not progressed:
                break

    return suspensions, assignments
