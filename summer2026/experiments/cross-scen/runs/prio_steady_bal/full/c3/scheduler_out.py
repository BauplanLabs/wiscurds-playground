@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Queues hold pipeline_ids; pipeline objects tracked in s.active
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.wait_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.active = {}            # pipeline_id -> Pipeline
    s.enqueued = set()       # pipeline_ids currently tracked
    s.pipeline_meta = {}     # pipeline_id -> {"enqueued_tick": int}

    # Failure handling
    s.dead_pipelines = set()

    # Execution attribution + OOM learning
    s.op_to_pipeline = {}    # op_key -> pipeline_id
    s.op_next_ram = {}       # (pipeline_id, op_key) -> next RAM to try
    s.op_last_oom_ram = {}   # (pipeline_id, op_key) -> largest RAM that OOM'd
    s.op_oom_retries = {}    # (pipeline_id, op_key) -> count
    s.op_fail_retries = {}   # (pipeline_id, op_key) -> non-OOM failures count

    # Knobs (kept simple/fast)
    s.max_new_assignments_per_tick = 48
    s.scan_limit_per_pick = 24
    s.queue_compact_threshold = 2048

    s.max_oom_retries_per_op = 5
    s.max_non_oom_retries_per_op = 2

    # Allow some intra-pipeline parallelism (wide DAGs), but prevent single pipeline dominance
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Default sizing (absolute-ish, capped; avoids the scale-with-cluster pitfall)
    s.base_ram_guess = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    s.min_ram = 1.0
    s.max_ram_cap = 256.0  # per-op cap unless OOM forces higher (still clipped by pool.max_ram_pool)

    s.cpu_frac = {
        Priority.QUERY: 0.10,
        Priority.INTERACTIVE: 0.08,
        Priority.BATCH_PIPELINE: 0.05,
    }
    s.min_cpu = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.max_cpu = {
        Priority.QUERY: 24.0,
        Priority.INTERACTIVE: 20.0,
        Priority.BATCH_PIPELINE: 16.0,
    }

    # Weighted fairness via deficit (CPU-cost-based), plus simple "no starvation" guards.
    # Units are "CPU quanta" (see cost_cpu_unit).
    s.cost_cpu_unit = 4.0
    s.quantum = {
        Priority.QUERY: 40.0,        # ~10x
        Priority.INTERACTIVE: 24.0,  # slightly > 5x to avoid interactive timeouts
        Priority.BATCH_PIPELINE: 6.0,
    }

    num_pools = getattr(s.executor, "num_pools", 1) or 1
    s.deficit = []
    s.since_interactive = []
    s.since_batch = []
    for _ in range(num_pools):
        s.deficit.append({
            Priority.QUERY: 0.0,
            Priority.INTERACTIVE: 0.0,
            Priority.BATCH_PIPELINE: 0.0,
        })
        s.since_interactive.append(0)
        s.since_batch.append(0)

    # Global max RAM across pools (used to cap OOM ramp hints safely)
    try:
        s.global_max_ram = max(p.max_ram_pool for p in s.executor.pools) if s.executor.pools else 1.0
    except Exception:
        s.global_max_ram = 1.0


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.active.pop(pid, None)
        s.pipeline_meta.pop(pid, None)

    def _q_len(pri):
        q = s.wait_q[pri]
        h = s.wait_head[pri]
        n = len(q) - h
        return n if n > 0 else 0

    def _maybe_compact(pri):
        q = s.wait_q[pri]
        h = s.wait_head[pri]
        if h <= 0:
            return
        if h >= s.queue_compact_threshold or h > (len(q) // 2):
            s.wait_q[pri] = q[h:]
            s.wait_head[pri] = 0

    def _rotate_pop(pri):
        q = s.wait_q[pri]
        h = s.wait_head[pri]
        if h >= len(q):
            return None
        pid = q[h]
        s.wait_head[pri] = h + 1
        return pid

    def _rotate_push(pri, pid):
        s.wait_q[pri].append(pid)

    # --- Ingest new pipelines (pipelines is assumed to be new arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            # refresh object reference if needed
            s.active[pid] = p
            continue
        s.active[pid] = p
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.wait_q[p.priority].append(pid)

    # --- Process results: learn OOM RAM and handle failures ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.pop(opk, None)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue

            key = (pid, opk)

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    # OOM: ramp RAM quickly, bounded retries
                    prev_oom = s.op_oom_retries.get(key, 0) + 1
                    s.op_oom_retries[key] = prev_oom

                    alloc_ram = getattr(r, "ram", None)
                    if alloc_ram is None or alloc_ram <= 0:
                        alloc_ram = s.op_next_ram.get(key, s.min_ram)

                    last = s.op_last_oom_ram.get(key, 0.0)
                    if alloc_ram > last:
                        last = float(alloc_ram)
                    s.op_last_oom_ram[key] = last

                    if prev_oom > s.max_oom_retries_per_op:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

                    # Next attempt: at least 2x last OOM allocation (fast convergence)
                    next_ram = max(last * 2.0, s.op_next_ram.get(key, 0.0) * 1.5, s.min_ram)
                    if next_ram > s.global_max_ram:
                        next_ram = float(s.global_max_ram)
                    s.op_next_ram[key] = next_ram
                else:
                    # Non-OOM failure: retry a couple times (in case it's transient), then give up
                    prev = s.op_fail_retries.get(key, 0) + 1
                    s.op_fail_retries[key] = prev
                    if prev > s.max_non_oom_retries_per_op:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

    # Keep queues from growing unbounded
    for pri in s.priorities:
        _maybe_compact(pri)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    # Add quantum to deficits once per tick
    for pool_id in range(num_pools):
        d = s.deficit[pool_id]
        d[Priority.QUERY] += s.quantum[Priority.QUERY]
        d[Priority.INTERACTIVE] += s.quantum[Priority.INTERACTIVE]
        d[Priority.BATCH_PIPELINE] += s.quantum[Priority.BATCH_PIPELINE]

    scheduled_counts = {}  # pipeline_id -> count scheduled this tick

    # Simple backlog signal to reduce per-op CPU under heavy load (more concurrency)
    backlog_total = _q_len(Priority.QUERY) + _q_len(Priority.INTERACTIVE) + _q_len(Priority.BATCH_PIPELINE)

    def _cpu_request(pool, pri, avail_cpu):
        # Base target from pool size, capped, then adapt down under pressure
        base = pool.max_cpu_pool * s.cpu_frac[pri]
        if base < s.min_cpu[pri]:
            base = s.min_cpu[pri]
        if base > s.max_cpu[pri]:
            base = s.max_cpu[pri]

        # Pressure adjustment (more concurrency when backlog is large)
        if backlog_total >= 300:
            base = max(s.min_cpu[pri], base * 0.60)
        elif backlog_total >= 120:
            base = max(s.min_cpu[pri], base * 0.75)

        # Quantize a bit (avoid thrashing tiny fractional CPU)
        cpu = float(int(base)) if base >= 2.0 else float(base)
        if cpu < 1.0:
            cpu = 1.0
        if cpu > avail_cpu:
            cpu = float(avail_cpu)
        if cpu < 1.0:
            return 0.0
        return cpu

    def _ram_request(pool, pri, pid, op, avail_ram):
        opk = _op_key(op)
        key = (pid, opk)

        # Start with learned next RAM if present; otherwise a small-ish guess.
        ram = s.op_next_ram.get(key, None)
        if ram is None:
            ram = s.base_ram_guess[pri]
            if ram > s.max_ram_cap:
                ram = s.max_ram_cap

        # Never exceed pool max
        if ram > pool.max_ram_pool:
            ram = float(pool.max_ram_pool)
        if ram < s.min_ram:
            ram = s.min_ram

        # Do not downscale RAM below the chosen request; if not enough headroom, skip
        if ram > avail_ram:
            return 0.0
        return float(ram)

    def _pick_schedulable_op(pri, pool_id, pool, avail_cpu, avail_ram):
        q = s.wait_q[pri]
        if s.wait_head[pri] >= len(q):
            return None, None, None, None, None

        scan = s.scan_limit_per_pick
        while scan > 0 and s.wait_head[pri] < len(q):
            scan -= 1
            pid = _rotate_pop(pri)
            if pid is None:
                break

            p = s.active.get(pid, None)
            if p is None or pid not in s.enqueued or pid in s.dead_pipelines:
                continue

            # Per-tick per-pipeline cap
            max_ops = s.max_ops_per_pipeline_per_tick.get(p.priority, 1)
            if scheduled_counts.get(pid, 0) >= max_ops:
                _rotate_push(pri, pid)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                _rotate_push(pri, pid)
                continue

            op = ops[0]

            # RAM must fit (correctness); CPU can be scaled down
            ram_req = _ram_request(pool, pri, pid, op, avail_ram)
            if ram_req <= 0.0:
                _rotate_push(pri, pid)
                continue

            cpu_req = _cpu_request(pool, pri, avail_cpu)
            if cpu_req <= 0.0:
                _rotate_push(pri, pid)
                continue

            # Keep pipeline in rotation for future ops
            _rotate_push(pri, pid)
            return p, pid, op, cpu_req, ram_req

        return None, None, None, None, None

    def _choose_priority(pool_id):
        # Starvation guards: ensure interactive/batch make progress even under constant query load.
        if _q_len(Priority.INTERACTIVE) > 0 and s.since_interactive[pool_id] >= 3:
            return Priority.INTERACTIVE
        if _q_len(Priority.BATCH_PIPELINE) > 0 and s.since_batch[pool_id] >= 12:
            return Priority.BATCH_PIPELINE

        # Otherwise, pick highest deficit among non-empty queues; tie-break by priority importance.
        d = s.deficit[pool_id]
        best = None
        best_val = None
        for pri in s.priorities:
            if _q_len(pri) <= 0:
                continue
            val = d.get(pri, 0.0)
            if best is None or val > best_val or (val == best_val and pri == Priority.QUERY):
                best = pri
                best_val = val
        return best

    remaining = s.max_new_assignments_per_tick
    for pool_id in range(num_pools):
        if remaining <= 0:
            break
        pool = s.executor.pools[pool_id]

        # Schedule multiple ops into this pool, but keep per-tick work bounded.
        while remaining > 0 and local_cpu[pool_id] >= 1.0 and local_ram[pool_id] >= 1.0:
            pri = _choose_priority(pool_id)
            if pri is None:
                break

            # Try up to 3 priorities (in case chosen class has only blocked/non-fitting work)
            tried = set()
            chosen = None
            for _ in range(3):
                if pri is None or pri in tried:
                    # pick the next best by deficit
                    d = s.deficit[pool_id]
                    alt = None
                    alt_val = None
                    for cand in s.priorities:
                        if cand in tried or _q_len(cand) <= 0:
                            continue
                        val = d.get(cand, 0.0)
                        if alt is None or val > alt_val or (val == alt_val and cand == Priority.QUERY):
                            alt = cand
                            alt_val = val
                    pri = alt
                    if pri is None:
                        break
                tried.add(pri)

                p, pid, op, cpu_req, ram_req = _pick_schedulable_op(
                    pri, pool_id, pool, local_cpu[pool_id], local_ram[pool_id]
                )
                if p is not None:
                    chosen = (p, pid, op, cpu_req, ram_req, pri)
                    break

            if chosen is None:
                break

            p, pid, op, cpu_req, ram_req, pri_used = chosen

            # Final safety: respect pool maxima and current headroom
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                break
            if cpu_req > pool.max_cpu_pool:
                cpu_req = float(pool.max_cpu_pool)
            if ram_req > pool.max_ram_pool:
                ram_req = float(pool.max_ram_pool)
            if cpu_req < 1.0 or ram_req < 1.0:
                break

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

            # Attribute results back to pipeline
            s.op_to_pipeline[_op_key(op)] = pid

            # Update local headroom
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            # Update per-tick caps
            scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1

            # Update deficits and starvation counters
            cost = cpu_req / s.cost_cpu_unit
            if cost < 1.0:
                cost = 1.0
            s.deficit[pool_id][pri_used] -= cost

            if pri_used == Priority.INTERACTIVE:
                s.since_interactive[pool_id] = 0
            else:
                s.since_interactive[pool_id] += 1

            if pri_used == Priority.BATCH_PIPELINE:
                s.since_batch[pool_id] = 0
            else:
                s.since_batch[pool_id] += 1

            remaining -= 1

    return suspensions, assignments
