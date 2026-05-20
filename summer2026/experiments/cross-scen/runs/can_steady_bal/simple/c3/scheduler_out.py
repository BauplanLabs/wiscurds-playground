@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focusing on:
      - Much smaller, CPU/RAM-balanced per-op sizing (fixes severe under-parallelism).
      - OOM-aware retries with per-op + per-operator-type RAM learning.
      - Share-based (deficit) fairness between INTERACTIVE and BATCH, while keeping QUERY snappy.
      - Deadline/aging boost to reduce timeouts (incomplete pipelines are heavily penalized).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()

    # pipeline_id -> {"enqueued_tick": int, "total_ops": int|None}
    s.pipeline_meta = {}

    # (pipeline_id, op_key) -> {"ram": float|None, "cpu": float|None, "fails": int, "ooms": int}
    s.op_hints = {}

    # op_type_key -> {"ram": float, "seen": int}
    # Stored as a conservative (high-ish) value to reduce future OOMs.
    s.optype_hints = {}

    # op_key -> pipeline_id
    s.op_to_pipeline = {}

    # op_key -> op_type_key
    s.opk_to_type = {}

    # pool_id -> {Priority -> deficit_cpu(float)}
    s.pool_deficit = {}

    # Tunables (chosen to improve throughput/completion while keeping QUERY latency low)
    s.shares = {
        Priority.QUERY: 0.22,
        Priority.INTERACTIVE: 0.34,
        Priority.BATCH_PIPELINE: 0.44,
    }

    # Per-pipeline per-tick op cap (prevents a single pipeline from consuming all slots in bursts)
    s.per_tick_pipeline_cap = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Scan depth per queue when picking an op (trade-off: fairness/latency vs overhead)
    s.pick_scan = 10

    # Aging / urgency knobs (ticks are assumed roughly time-like in the simulator)
    s.urgent_time_left_frac = 0.25   # consider urgent when <= 25% of max_job_seconds left
    s.urgent_min_ticks = 45          # absolute floor (helps if max_job_seconds is small/unknown)


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _get_param(name, default):
        params = getattr(s, "params", None)
        if params is None:
            return default
        if isinstance(params, dict):
            return params.get(name, default)
        return getattr(params, name, default)

    max_job_seconds = _get_param("max_job_seconds", 360)

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _op_type_key(op):
        # Prefer stable semantic identifiers if present; fall back to class name.
        k = (
            getattr(op, "name", None)
            or getattr(op, "op_name", None)
            or getattr(op, "operator_name", None)
            or getattr(op, "func_name", None)
            or getattr(op, "sql_name", None)
        )
        if k:
            return ("name", str(k))
        return ("class", str(getattr(op, "__class__", type(op)).__name__))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pri):
        return s.wait_q[pri]

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)

    def _enqueued_tick(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id) or {}
        return meta.get("enqueued_tick", s.ticks)

    def _age_ticks(pipeline_id):
        return max(0, s.ticks - _enqueued_tick(pipeline_id))

    def _time_left_ticks(pipeline_id):
        # Approximate deadline: arrival/enqueue + max_job_seconds
        return max_job_seconds - _age_ticks(pipeline_id)

    def _is_urgent(pipeline_id):
        urgent_left = max(s.urgent_min_ticks, int(max_job_seconds * s.urgent_time_left_frac))
        return _time_left_ticks(pipeline_id) <= urgent_left

    def _try_total_ops(p):
        try:
            v = getattr(p, "values", None)
            if v is None:
                return None
            return len(v)
        except Exception:
            return None

    def _remaining_ops(p, status):
        meta = s.pipeline_meta.get(p.pipeline_id)
        if meta is None:
            meta = {"enqueued_tick": s.ticks, "total_ops": None}
            s.pipeline_meta[p.pipeline_id] = meta

        total = meta.get("total_ops", None)
        if total is None:
            total = _try_total_ops(p)
            meta["total_ops"] = total

        if not total or total <= 0:
            return 10**9

        try:
            completed = len(status.get_ops([OperatorState.COMPLETED], require_parents_complete=False))
        except Exception:
            completed = 0
        rem = total - completed
        if rem <= 0:
            rem = 1
        return rem

    def _ceil_div(a, b):
        return (a + b - 1) // b

    def _base_cpu(pool, pri, backlog):
        # Use modest per-op CPU to unlock parallelism; adapt down under heavy backlog.
        maxc = pool.max_cpu_pool
        if pri == Priority.QUERY:
            base = max(2, _ceil_div(int(maxc), 8))   # 64 -> 8
            minc = 2
        elif pri == Priority.INTERACTIVE:
            base = max(2, _ceil_div(int(maxc), 12))  # 64 -> 6
            minc = 2
        else:
            base = max(1, _ceil_div(int(maxc), 16))  # 64 -> 4
            minc = 1

        # Backlog pressure scaling: increase parallelism when queues are huge.
        # (Keep a floor so we don't make single ops unbearably slow.)
        if backlog > 300:
            scaled = max(minc, int(base / 3))
        elif backlog > 120:
            scaled = max(minc, int(base / 2))
        else:
            scaled = base

        # Keep a hard cap to avoid accidental pool hogging.
        cap = max(2, int(maxc * 0.50))
        if scaled > cap:
            scaled = cap
        return float(scaled)

    def _base_ram(pool, pri):
        # Low baseline; RAM is then balanced to CPU via RAM-per-CPU to avoid OOMs.
        maxr = pool.max_ram_pool
        if pri == Priority.QUERY:
            frac = 0.010
            abs_min = 6.0
        elif pri == Priority.INTERACTIVE:
            frac = 0.008
            abs_min = 5.0
        else:
            frac = 0.006
            abs_min = 4.0
        try:
            v = float(maxr) * float(frac)
        except Exception:
            v = abs_min
        if v < abs_min:
            v = abs_min
        return float(v)

    def _size_request(pool, pri, pipeline_id, op, backlog):
        opk = _op_key(op)
        typek = s.opk_to_type.get(opk)
        if typek is None:
            typek = _op_type_key(op)
            s.opk_to_type[opk] = typek

        hint = s.op_hints.get((pipeline_id, opk), None)
        type_hint = s.optype_hints.get(typek, None)

        cpu = _base_cpu(pool, pri, backlog)
        if hint and hint.get("cpu"):
            cpu = max(cpu, float(hint["cpu"]))

        # RAM strategy:
        #  - start with small per-priority floor
        #  - then allocate RAM roughly proportional to CPU (keeps RAM from being the bottleneck and reduces OOM)
        #  - then apply learned hints (per-op and per-type)
        ram = _base_ram(pool, pri)

        maxc = float(pool.max_cpu_pool) if pool.max_cpu_pool else 1.0
        maxr = float(pool.max_ram_pool) if pool.max_ram_pool else 1.0
        ram_per_cpu = maxr / maxc if maxc > 0 else ram

        if pri == Priority.QUERY:
            ram_mult = 0.95
        elif pri == Priority.INTERACTIVE:
            ram_mult = 0.85
        else:
            ram_mult = 0.75

        ram_balanced = ram_per_cpu * float(cpu) * float(ram_mult)
        if ram_balanced > ram:
            ram = ram_balanced

        if type_hint and type_hint.get("ram"):
            ram = max(ram, float(type_hint["ram"]))

        if hint and hint.get("ram"):
            ram = max(ram, float(hint["ram"]))

        # Clip to pool capacity
        if cpu < 1.0:
            cpu = 1.0
        if ram < 1.0:
            ram = 1.0
        if cpu > float(pool.max_cpu_pool):
            cpu = float(pool.max_cpu_pool)
        if ram > float(pool.max_ram_pool):
            ram = float(pool.max_ram_pool)

        return cpu, ram

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        _queue_for_priority(p.priority).append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {
            "enqueued_tick": s.ticks,
            "total_ops": _try_total_ops(p),
        }

    # --- Process results (learn RAM; react to OOM) ---
    for r in results:
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            typek = s.opk_to_type.get(opk)
            if typek is None:
                typek = _op_type_key(op)
                s.opk_to_type[opk] = typek

            h = s.op_hints.get((pid, opk))
            if h is None:
                h = {"ram": None, "cpu": None, "fails": 0, "ooms": 0}
                s.op_hints[(pid, opk)] = h

            if hasattr(r, "failed") and r.failed():
                h["fails"] += 1
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    h["ooms"] += 1
                    alloc_ram = getattr(r, "ram", None)
                    prev_ram = h["ram"] if (h["ram"] is not None and h["ram"] > 0) else None
                    base = None
                    if alloc_ram is not None and alloc_ram > 0:
                        base = float(alloc_ram)
                    elif prev_ram is not None:
                        base = float(prev_ram)
                    else:
                        base = 4.0

                    new_ram = max(base * 2.0, 4.0)
                    h["ram"] = new_ram

                    # Conservative per-type increase to reduce future OOMs.
                    th = s.optype_hints.get(typek)
                    if th is None:
                        s.optype_hints[typek] = {"ram": new_ram, "seen": 1}
                    else:
                        th["seen"] = int(th.get("seen", 0)) + 1
                        if new_ram > float(th.get("ram", 0.0)):
                            th["ram"] = new_ram
                else:
                    # Unknown failure: modestly increase both CPU and RAM for the next retry.
                    alloc_cpu = getattr(r, "cpu", None)
                    alloc_ram = getattr(r, "ram", None)

                    prev_cpu = h["cpu"] if (h["cpu"] is not None and h["cpu"] > 0) else None
                    prev_ram = h["ram"] if (h["ram"] is not None and h["ram"] > 0) else None

                    base_cpu = float(alloc_cpu) if (alloc_cpu is not None and alloc_cpu > 0) else (float(prev_cpu) if prev_cpu else 1.0)
                    base_ram = float(alloc_ram) if (alloc_ram is not None and alloc_ram > 0) else (float(prev_ram) if prev_ram else 4.0)

                    h["cpu"] = max(1.0, base_cpu * 1.25)
                    h["ram"] = max(1.0, base_ram * 1.25)
            else:
                # Success: update per-type RAM hint to a conservative high-ish value.
                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is not None and alloc_ram > 0:
                    th = s.optype_hints.get(typek)
                    if th is None:
                        s.optype_hints[typek] = {"ram": float(alloc_ram), "seen": 1}
                    else:
                        th["seen"] = int(th.get("seen", 0)) + 1
                        # Keep it from monotonically increasing forever: slight decay toward recent successes.
                        prev = float(th.get("ram", 0.0))
                        new = float(alloc_ram)
                        th["ram"] = max(prev * 0.985, new)

    suspensions = []
    assignments = []

    # Tick-local cap tracking
    scheduled_count = {}  # pipeline_id -> count scheduled this tick

    def _pipeline_cap(pri):
        return int(s.per_tick_pipeline_cap.get(pri, 1))

    def _pick_from_queue(q, pri, pool, pool_id, avail_cpu, avail_ram, urgent_only):
        if not q:
            return None

        n = len(q)
        scan = s.pick_scan if s.pick_scan > 0 else 1
        scan = scan if scan < n else n

        best = None  # (score_tuple, pipeline, op, cpu, ram)
        backlog = n

        for _ in range(scan):
            p = q.pop(0)
            pid = p.pipeline_id

            # If already scheduled too much for this pipeline this tick, rotate it back.
            cap = _pipeline_cap(p.priority)
            if scheduled_count.get(pid, 0) >= cap:
                q.append(p)
                continue

            status = p.runtime_status()

            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            if urgent_only and (not _is_urgent(pid)):
                q.append(p)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            opk = _op_key(op)

            cpu_req, ram_req = _size_request(pool, pri, pid, op, backlog)

            # If per-op RAM hint exceeds currently available RAM, skip (avoid guaranteed OOM/retry churn).
            h = s.op_hints.get((pid, opk))
            if h and h.get("ram") and float(h["ram"]) > float(avail_ram):
                q.append(p)
                continue

            # Fit to current availability
            if cpu_req > float(avail_cpu):
                cpu_req = float(avail_cpu)
            if ram_req > float(avail_ram):
                ram_req = float(avail_ram)

            if cpu_req < 1.0 or ram_req < 1.0:
                q.append(p)
                continue

            # Score: urgent first, then smallest remaining ops (finish pipelines), then oldest, then higher priority
            rem = _remaining_ops(p, status)
            age = _age_ticks(pid)
            pr_rank = 0 if pri == Priority.QUERY else (1 if pri == Priority.INTERACTIVE else 2)

            if urgent_only:
                score = (_time_left_ticks(pid), pr_rank, rem, -age)
            else:
                score = (rem, -age, pr_rank)

            if best is None or score < best[0]:
                best = (score, p, op, cpu_req, ram_req)

            q.append(p)

        if best is None:
            return None

        _, p, op, cpu_req, ram_req = best
        return (p, op, cpu_req, ram_req, pool_id)

    def _ensure_pool_deficit(pool_id, pool):
        if pool_id in s.pool_deficit:
            return
        s.pool_deficit[pool_id] = {
            Priority.QUERY: 0.0,
            Priority.INTERACTIVE: 0.0,
            Priority.BATCH_PIPELINE: 0.0,
        }

    def _add_deficit(pool_id, pool):
        d = s.pool_deficit[pool_id]
        maxc = float(pool.max_cpu_pool) if pool.max_cpu_pool else 1.0
        for pri, share in s.shares.items():
            d[pri] = float(d.get(pri, 0.0)) + float(share) * maxc

    # --- Main scheduling loop ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        _ensure_pool_deficit(pool_id, pool)
        _add_deficit(pool_id, pool)
        deficit = s.pool_deficit[pool_id]

        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            picked = None

            # 1) Urgency boost: pull near-deadline work first across all priorities.
            best_urgent = None
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = _queue_for_priority(pri)
                cand = _pick_from_queue(q, pri, pool, pool_id, avail_cpu, avail_ram, urgent_only=True)
                if cand is None:
                    continue
                p, op, cpu_req, ram_req, _ = cand
                # Prefer smallest time-left (computed inside score in urgent picker), so just compare directly here too.
                tl = _time_left_ticks(p.pipeline_id)
                pr_rank = 0 if pri == Priority.QUERY else (1 if pri == Priority.INTERACTIVE else 2)
                key = (tl, pr_rank)
                if best_urgent is None or key < best_urgent[0]:
                    best_urgent = (key, cand)

            if best_urgent is not None:
                picked = best_urgent[1]
            else:
                # 2) Normal selection:
                #    - Try QUERY first (low latency), but don't let it fully starve others due to deficits.
                qQ = _queue_for_priority(Priority.QUERY)
                candQ = _pick_from_queue(qQ, Priority.QUERY, pool, pool_id, avail_cpu, avail_ram, urgent_only=False)

                if candQ is not None and (deficit[Priority.QUERY] >= 1.0 or (not _queue_for_priority(Priority.INTERACTIVE) and not _queue_for_priority(Priority.BATCH_PIPELINE))):
                    picked = candQ
                else:
                    # Choose between INTERACTIVE and BATCH by deficit (share-based fairness).
                    pri_a = Priority.INTERACTIVE
                    pri_b = Priority.BATCH_PIPELINE
                    if float(deficit.get(pri_b, 0.0)) > float(deficit.get(pri_a, 0.0)):
                        pri_a, pri_b = pri_b, pri_a

                    candA = _pick_from_queue(_queue_for_priority(pri_a), pri_a, pool, pool_id, avail_cpu, avail_ram, urgent_only=False)
                    if candA is not None:
                        picked = candA
                    else:
                        candB = _pick_from_queue(_queue_for_priority(pri_b), pri_b, pool, pool_id, avail_cpu, avail_ram, urgent_only=False)
                        if candB is not None:
                            picked = candB
                        else:
                            # Fall back to QUERY if we didn't try it above or it was blocked by deficit logic.
                            if candQ is not None:
                                picked = candQ

            if picked is None:
                break

            p, op, cpu_req, ram_req, _ = picked

            # Enforce per-pipeline cap again (in case it changed due to earlier picks in this loop)
            pid = p.pipeline_id
            cap = _pipeline_cap(p.priority)
            if scheduled_count.get(pid, 0) >= cap:
                break

            a = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(a)

            # Attribute results to a pipeline.
            s.op_to_pipeline[_op_key(op)] = pid

            # Update deficits (CPU-based accounting)
            deficit[p.priority] = float(deficit.get(p.priority, 0.0)) - float(cpu_req)

            # Consume resources
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            scheduled_count[pid] = scheduled_count.get(pid, 0) + 1

    return suspensions, assignments
