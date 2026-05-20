@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on completion rate + low latency:

    Key changes vs previous version:
      - Avoid pool-max proportional sizing (which collapsed concurrency as cluster scaled).
      - Weighted round-robin across priorities (prevents INTERACTIVE starvation).
      - CPU/RAM "quanta" sized for parallelism; RAM scales with pool's RAM/CPU ratio.
      - Aging: long-waiting pipelines get gradual CPU boosts (helps tail + timeouts).
      - OOM-aware RAM hints retained; limited retries for non-OOM failures (avoid deadloops).
      - Always schedule from internal queues (no early-exit when no new arrivals/results).
    """
    s.ticks = 0

    # Per-priority pipeline queues (store Pipeline objects).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Round-robin cursors per priority queue.
    s.q_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track which pipeline_ids are currently known/enqueued.
    s.enqueued = set()

    # Metadata per pipeline_id.
    # { pipeline_id: {"enqueued_tick": int, "priority": Priority} }
    s.pipeline_meta = {}

    # OOM-derived minimum safe RAM per (pipeline_id, op_key).
    s.op_ram_hint = {}

    # Failure counts per (pipeline_id, op_key).
    s.op_fail_count = {}

    # Map op_key -> pipeline_id for attributing ExecutionResult back to a pipeline.
    s.op_to_pipeline = {}

    # Pipelines we stop scheduling (exceeded retry caps / impossible).
    s.dead_pipelines = set()

    # Weighted round-robin cycle across priorities.
    # Give INTERACTIVE substantial share to avoid timeouts; keep QUERY favored.
    s.rr_weights = {
        Priority.QUERY: 5,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 1,
    }
    s.rr_cycle = (
        [Priority.QUERY] * s.rr_weights[Priority.QUERY]
        + [Priority.INTERACTIVE] * s.rr_weights[Priority.INTERACTIVE]
        + [Priority.BATCH_PIPELINE] * s.rr_weights[Priority.BATCH_PIPELINE]
    )
    s.rr_i = 0

    # CPU sizing (favor parallelism; burst via aging).
    s.cpu_base = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 2,
    }
    s.cpu_min = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }
    s.cpu_max = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 8,
    }

    # Aging: every N ticks waited adds +1 vCPU (up to cpu_max).
    s.cpu_age_div = 20

    # RAM sizing: tie to pool RAM/CPU ratio so CPU-limited packing also fills RAM well.
    # Minimum absolute floors (units are whatever the simulator uses; typically GB).
    s.ram_floor = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 4,
    }
    s.ram_factor = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.05,
        Priority.BATCH_PIPELINE: 0.95,
    }

    # Per-tick cap on number of ops scheduled per pipeline (prevents one DAG from hogging).
    s.per_tick_pipeline_cap = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Retry caps.
    s.max_oom_retries = 5
    s.max_other_retries = 2

    # Periodic cleanup to keep queues small.
    s.cleanup_every = 50


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _compute_cpu_req(pri, pid):
        base = s.cpu_base.get(pri, 2)
        mx = s.cpu_max.get(pri, base)
        waited = _pipeline_wait_ticks(pid)
        bump = 0
        if s.cpu_age_div and s.cpu_age_div > 0:
            bump = waited // s.cpu_age_div
        cpu = base + bump
        if cpu > mx:
            cpu = mx
        if cpu < 1:
            cpu = 1
        return cpu

    def _compute_ram_req(pool, pri, cpu_req, pid, op):
        # Pool ratio: RAM per CPU (work-conserving packing across both dimensions).
        max_cpu = getattr(pool, "max_cpu_pool", 1) or 1
        max_ram = getattr(pool, "max_ram_pool", 1) or 1
        ram_per_cpu = max_ram / max(1.0, float(max_cpu))

        base_floor = s.ram_floor.get(pri, 1)
        factor = s.ram_factor.get(pri, 1.0)

        base = max(base_floor, float(cpu_req) * ram_per_cpu * float(factor))

        opk = _op_key(op)
        hint = s.op_ram_hint.get((pid, opk), None)
        if hint is not None:
            if hint > base:
                base = hint

        if base > max_ram:
            base = max_ram
        if base < 1:
            base = 1
        return base

    def _call_failed(r):
        fn = getattr(r, "failed", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:
                return False
        return False

    def _cleanup_queues():
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                s.q_cursor[pri] = 0
                continue
            new_q = []
            for p in q:
                if p is None:
                    continue
                pid = p.pipeline_id
                if pid in s.dead_pipelines:
                    continue
                try:
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        _drop_pipeline(pid)
                        continue
                except Exception:
                    pass
                new_q.append(p)
            s.wait_q[pri] = new_q
            if not new_q:
                s.q_cursor[pri] = 0
            else:
                s.q_cursor[pri] = s.q_cursor[pri] % len(new_q)

    # --- Ingest pipelines (new or newly visible) ---
    for p in pipelines or []:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue
        except Exception:
            pass

        if pid in s.enqueued:
            continue

        pri = p.priority
        if pri not in s.wait_q:
            pri = Priority.BATCH_PIPELINE

        s.wait_q[pri].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "priority": pri}

    # --- Process results (update OOM hints and retry accounting) ---
    for r in results or []:
        failed = _call_failed(r)
        ops = getattr(r, "ops", None) or []
        err = getattr(r, "error", None)
        is_oom = failed and _is_oom_error(err)

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None) or getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            key = (pid, opk)

            if failed:
                prev = s.op_fail_count.get(key, 0)
                cnt = prev + 1
                s.op_fail_count[key] = cnt

                if is_oom:
                    # Aggressive RAM growth to converge quickly; keep it monotonic.
                    prev_hint = s.op_ram_hint.get(key, None)
                    alloc = getattr(r, "ram", None)
                    base = None
                    if alloc is not None and alloc > 0:
                        base = float(alloc)
                    elif prev_hint is not None and prev_hint > 0:
                        base = float(prev_hint)
                    else:
                        base = 1.0

                    # Multiply; also ensure it strictly increases.
                    new_hint = base * 2.0
                    if prev_hint is not None and new_hint <= prev_hint:
                        new_hint = float(prev_hint) * 1.5
                    if new_hint < 1:
                        new_hint = 1.0
                    s.op_ram_hint[key] = new_hint

                    if cnt >= s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    if cnt >= s.max_other_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success: prune mapping (optional) and keep hints for future retries.
                if opk in s.op_to_pipeline:
                    # Only delete if it points to same pid (avoid accidental collisions).
                    if s.op_to_pipeline.get(opk, None) == pid:
                        s.op_to_pipeline.pop(opk, None)

    if s.cleanup_every and (s.ticks % s.cleanup_every == 0):
        _cleanup_queues()

    # --- Scheduling ---
    num_pools = s.executor.num_pools
    pools = s.executor.pools

    avail_cpu = [pools[i].avail_cpu_pool for i in range(num_pools)]
    avail_ram = [pools[i].avail_ram_pool for i in range(num_pools)]

    suspensions = []
    assignments = []

    scheduled_count = {}  # pipeline_id -> ops scheduled this tick

    def _any_pool_can_fit(pri):
        cpu_min = s.cpu_min.get(pri, 1)
        for i in range(num_pools):
            if avail_cpu[i] >= cpu_min and avail_ram[i] >= 1:
                return True
        return False

    def _pick_schedulable(pri):
        q = s.wait_q.get(pri, [])
        if not q:
            return None

        n = len(q)
        if n == 0:
            return None

        # Scan up to n items (round-robin) to find a runnable op that fits somewhere.
        scanned = 0
        while scanned < n and q:
            n = len(q)
            idx = s.q_cursor[pri] % n
            s.q_cursor[pri] = idx + 1
            p = q[idx]
            scanned += 1

            if p is None:
                q.pop(idx)
                if q:
                    s.q_cursor[pri] = s.q_cursor[pri] % len(q)
                else:
                    s.q_cursor[pri] = 0
                continue

            pid = p.pipeline_id

            if pid in s.dead_pipelines:
                q.pop(idx)
                if q:
                    s.q_cursor[pri] = s.q_cursor[pri] % len(q)
                else:
                    s.q_cursor[pri] = 0
                continue

            try:
                st = p.runtime_status()
            except Exception:
                # If status is unavailable, skip for now.
                continue

            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                q.pop(idx)
                if q:
                    s.q_cursor[pri] = s.q_cursor[pri] % len(q)
                else:
                    s.q_cursor[pri] = 0
                continue

            cap = s.per_tick_pipeline_cap.get(pri, 1)
            if scheduled_count.get(pid, 0) >= cap:
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]

            desired_cpu = _compute_cpu_req(pri, pid)
            cpu_min = s.cpu_min.get(pri, 1)

            best = None
            best_score = None

            # Choose best-fit pool (min leftover RAM, then leftover CPU).
            for pool_id in range(num_pools):
                if avail_cpu[pool_id] < cpu_min or avail_ram[pool_id] < 1:
                    continue

                pool = pools[pool_id]
                max_cpu_pool = getattr(pool, "max_cpu_pool", 1) or 1

                cpu_req = desired_cpu
                if cpu_req > max_cpu_pool:
                    cpu_req = max_cpu_pool
                if cpu_req > avail_cpu[pool_id]:
                    cpu_req = int(avail_cpu[pool_id])
                if cpu_req < cpu_min:
                    continue
                if cpu_req < 1:
                    cpu_req = 1

                ram_req = _compute_ram_req(pool, pri, cpu_req, pid, op)

                # If we have a hint bigger than pool availability, this op won't fit here now.
                opk = _op_key(op)
                hint = s.op_ram_hint.get((pid, opk), None)
                if hint is not None and hint > avail_ram[pool_id]:
                    continue

                if ram_req > avail_ram[pool_id]:
                    continue

                # Best-fit primarily on RAM to reduce fragmentation; slight CPU tie-break.
                score = (avail_ram[pool_id] - ram_req) * 1.0 + (avail_cpu[pool_id] - cpu_req) * 0.05
                if best is None or score < best_score:
                    best = (p, op, pool_id, cpu_req, ram_req)
                    best_score = score

            if best is None:
                continue

            return best

        return None

    # Work-conserving WRR loop: keep assigning until no further progress.
    max_iterations = 100000  # safety
    iters = 0
    while iters < max_iterations:
        iters += 1

        # Stop if no pool has even minimal resources for any priority with backlog.
        any_possible = False
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.wait_q.get(pri) and _any_pool_can_fit(pri):
                any_possible = True
                break
        if not any_possible:
            break

        progressed = False

        # Try up to a full RR cycle to find something schedulable.
        cycle_len = len(s.rr_cycle) if s.rr_cycle else 0
        tries = cycle_len if cycle_len > 0 else 3

        for _ in range(tries):
            if not s.rr_cycle:
                pri = Priority.QUERY
            else:
                pri = s.rr_cycle[s.rr_i % len(s.rr_cycle)]
                s.rr_i = (s.rr_i + 1) % len(s.rr_cycle)

            picked = _pick_schedulable(pri)
            if picked is None:
                continue

            p, op, pool_id, cpu_req, ram_req = picked

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Map op -> pipeline for result attribution.
            s.op_to_pipeline[_op_key(op)] = p.pipeline_id

            # Update availabilities.
            avail_cpu[pool_id] -= cpu_req
            avail_ram[pool_id] -= ram_req

            scheduled_count[p.pipeline_id] = scheduled_count.get(p.pipeline_id, 0) + 1

            progressed = True
            break

        if not progressed:
            break

    return suspensions, assignments
