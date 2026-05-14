from collections import deque

@register_scheduler_init(key="scheduler_high_006")
def scheduler_high_006_init(s):
    # Priority queues (FIFO within class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Prevent duplicate enqueues if the simulator passes "all pipelines" each tick
    s.known_pipeline_ids = set()

    # Simple clock for aging
    s.tick = 0
    s.pipeline_meta = {}  # pipeline_id -> {"arrival_tick": int}

    # OOM learning (per operator instance)
    s.op_ram_hint = {}        # id(op) -> ram_gb (next try should be >=)
    s.op_oom_retries = {}     # id(op) -> int
    s.op_fail_retries = {}    # id(op) -> int (non-OOM)

    # Placement hints
    s.op_pool_hint = {}       # id(op) -> pool_id to try first (e.g., after OOM)

    # Attribution
    s.op_to_pipeline = {}     # id(op) -> pipeline_id

    # Pipelines we stop scheduling (persistent non-OOM errors / repeated OOM)
    s.pipeline_permanent_fail = set()

    # Controls
    s.aging_threshold_ticks = 80

    # Retry limits
    s.max_oom_retries_per_op = 6
    s.max_non_oom_retries_per_op = 2

    # Protect query tail latency when sharing pool 0 with background work
    s.pool0_nonquery_reserve_cpu_frac_idle = 0.10
    s.pool0_nonquery_reserve_ram_frac_idle = 0.10
    s.pool0_nonquery_reserve_cpu_frac_busy = 0.25
    s.pool0_nonquery_reserve_ram_frac_busy = 0.25

    # Fairness between INTERACTIVE and BATCH on shared capacity
    s.interactive_cpu_share = 0.60

    # Per-pipeline concurrency caps (per scheduling tick)
    s.cap_query_ops_per_pipeline_per_tick = 2
    s.cap_interactive_ops_per_pipeline_per_tick = 2
    s.cap_batch_ops_per_pipeline_per_tick = 2

    # Safety limit to avoid runaway scheduling loops
    s.max_assignments_per_tick = 512


@register_scheduler(key="scheduler_high_006")
def scheduler_high_006_scheduler(s, results, pipelines):
    s.tick += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _ensure_pipeline_meta(p):
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"arrival_tick": s.tick}

    def _is_aged_batch(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return False
        return (s.tick - meta["arrival_tick"]) >= s.aging_threshold_ticks

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _coerce_ram_to_gb(x, pool_max_ram_gb):
        try:
            v = float(x)
        except Exception:
            return None
        if not (v > 0.0):
            return None

        # Heuristics: if value looks like bytes, convert to GB
        if v >= 1e8:
            v = v / (1024.0 ** 3)
        # If it looks like MB (e.g., 32768), convert to GB
        elif v > (pool_max_ram_gb * 4.0) and v < 1e7:
            v = v / 1024.0

        if v <= 0.0:
            return None
        return v

    def _guess_op_min_ram_gb(op, pool_max_ram_gb):
        # Try common attribute names; fall back to None.
        cand_names = (
            "ram_min", "min_ram", "min_ram_gb", "min_memory", "memory_min", "mem_min",
            "required_ram", "ram_required", "ram_req", "mem_required",
            "peak_ram", "peak_memory",
            "ram", "memory",
        )
        for name in cand_names:
            if hasattr(op, name):
                v = _coerce_ram_to_gb(getattr(op, name), pool_max_ram_gb)
                if v is None:
                    continue
                # Clamp to pool max for sanity
                v = max(0.001, min(v, float(pool_max_ram_gb)))
                return v
        return None

    def _resource_profile(sched_class, pool_max_cpu, pool_max_ram):
        # Return (cpu_target, cpu_min, ram_floor_abs, ram_safety)
        # Targets emphasize throughput: small CPU slices, RAM near the minimum.
        if sched_class == "query":
            cpu_min = 2.0
            cpu_target = min(16.0, max(cpu_min, pool_max_cpu * 0.25))
            ram_floor = 6.0
            ram_safety = 1.30
            return cpu_target, cpu_min, ram_floor, ram_safety

        if sched_class == "interactive":
            cpu_min = 1.0
            cpu_target = min(8.0, max(cpu_min, pool_max_cpu * 0.125))
            ram_floor = 4.0
            ram_safety = 1.20
            return cpu_target, cpu_min, ram_floor, ram_safety

        # batch
        cpu_min = 1.0
        cpu_target = min(4.0, max(cpu_min, pool_max_cpu * 0.0625))
        ram_floor = 3.0
        ram_safety = 1.15
        return cpu_target, cpu_min, ram_floor, ram_safety

    def _queue_has_runnable(q, sample=32):
        # Conservative: look for at least one runnable op.
        n = min(len(q), sample)
        for _ in range(n):
            p = q.popleft()
            try:
                if p.pipeline_id in s.pipeline_permanent_fail:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    q.append(p)
                    return True
            finally:
                q.append(p)
        return False

    # Enqueue pipelines (only once).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(pid)
        _ensure_pipeline_meta(p)
        _queue_for_priority(p.priority).append(p)

    # Learn from results: OOM -> raise RAM hint; repeated non-OOM -> stop that pipeline.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if not failed:
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        pool_id = getattr(r, "pool_id", None)

        for op in r.ops:
            op_id = id(op)
            pid = s.op_to_pipeline.get(op_id)

            if is_oom:
                retries = s.op_oom_retries.get(op_id, 0) + 1
                s.op_oom_retries[op_id] = retries

                if pid is not None and retries > s.max_oom_retries_per_op:
                    s.pipeline_permanent_fail.add(pid)
                    continue

                attempted = max(0.001, float(getattr(r, "ram", 0.001)))
                if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                    max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)
                else:
                    max_ram = max(float(s.executor.pools[i].max_ram_pool) for i in range(s.executor.num_pools))

                new_hint = min(max_ram, attempted * 2.0)
                prev_hint = float(s.op_ram_hint.get(op_id, 0.0))
                s.op_ram_hint[op_id] = max(prev_hint, new_hint)

                # Prefer the largest-memory pool for the next retry (helps avoid repeated OOM)
                best_pool = max(range(s.executor.num_pools), key=lambda i: float(s.executor.pools[i].max_ram_pool))
                s.op_pool_hint[op_id] = best_pool
            else:
                retries = s.op_fail_retries.get(op_id, 0) + 1
                s.op_fail_retries[op_id] = retries
                if pid is not None and retries > s.max_non_oom_retries_per_op:
                    s.pipeline_permanent_fail.add(pid)

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]
    max_cpu = [float(s.executor.pools[i].max_cpu_pool) for i in range(num_pools)]
    max_ram = [float(s.executor.pools[i].max_ram_pool) for i in range(num_pools)]

    # Dynamic reservation on pool 0 for non-query work
    query_runnable_now = _queue_has_runnable(s.q_query)
    interactive_runnable_now = _queue_has_runnable(s.q_interactive)

    if query_runnable_now:
        pool0_reserve_cpu_frac = s.pool0_nonquery_reserve_cpu_frac_busy
        pool0_reserve_ram_frac = s.pool0_nonquery_reserve_ram_frac_busy
    else:
        pool0_reserve_cpu_frac = s.pool0_nonquery_reserve_cpu_frac_idle
        pool0_reserve_ram_frac = s.pool0_nonquery_reserve_ram_frac_idle

    def _effective_avail_for_nonquery(pool_id):
        if pool_id != 0:
            return avail_cpu[pool_id], avail_ram[pool_id]
        reserve_cpu = max_cpu[0] * pool0_reserve_cpu_frac
        reserve_ram = max_ram[0] * pool0_reserve_ram_frac
        return max(0.0, avail_cpu[0] - reserve_cpu), max(0.0, avail_ram[0] - reserve_ram)

    def _choose_pool_for_op(ram_req, cpu_target, cpu_min, sched_class, nonquery):
        # Returns (pool_id, cpu_alloc) or (None, None)
        # Query: pick most-headroom; others: best-fit packing by RAM then CPU.
        best_pool = None
        best_cpu_alloc = None
        best_key = None

        # Candidate pool order: try hinted pool first (if any) by caller; otherwise all pools.
        for pool_id in range(num_pools):
            if nonquery:
                eff_cpu, eff_ram = _effective_avail_for_nonquery(pool_id)
            else:
                eff_cpu, eff_ram = avail_cpu[pool_id], avail_ram[pool_id]

            if eff_cpu < cpu_min or eff_ram < ram_req:
                continue

            cpu_alloc = min(cpu_target, eff_cpu)
            if cpu_alloc < cpu_min:
                continue

            if sched_class == "query":
                # Prefer the most headroom
                key = (-eff_ram, -eff_cpu)
            else:
                # Best-fit: leave as little slack as possible (pack RAM tightly)
                key = (eff_ram - ram_req, eff_cpu - cpu_alloc)

            if best_key is None or key < best_key:
                best_key = key
                best_pool = pool_id
                best_cpu_alloc = cpu_alloc

        return best_pool, best_cpu_alloc

    scheduled_count = {}  # pipeline_id -> scheduled ops this tick

    def _pick_one_op(q, require_aged_batch=False):
        # Find one runnable op without head-of-line blocking; rotate queue.
        scan = min(len(q), 256)
        for _ in range(scan):
            p = q.popleft()
            try:
                pid = p.pipeline_id
                _ensure_pipeline_meta(p)

                if pid in s.pipeline_permanent_fail:
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                if require_aged_batch and (not _is_aged_batch(pid)):
                    continue

                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue

                return p, ops[0]
            finally:
                q.append(p)
        return None, None

    def _try_place_op(p, op, sched_class, nonquery):
        # Per-pipeline cap
        pid = p.pipeline_id
        already = scheduled_count.get(pid, 0)
        if p.priority == Priority.QUERY:
            cap = s.cap_query_ops_per_pipeline_per_tick
        elif p.priority == Priority.INTERACTIVE:
            cap = s.cap_interactive_ops_per_pipeline_per_tick
        else:
            cap = s.cap_batch_ops_per_pipeline_per_tick

        if already >= cap:
            return False

        # Pool-specific sizing will use the pool's max values; pick pool by checking feasibility.
        # Start with an estimated RAM requirement using the *largest* pool to avoid under-sizing.
        largest_pool = max(range(num_pools), key=lambda i: max_ram[i])
        _, _, ram_floor_abs, ram_safety = _resource_profile(sched_class, max_cpu[largest_pool], max_ram[largest_pool])

        op_id = id(op)
        hinted_ram = float(s.op_ram_hint.get(op_id, 0.0))

        guessed_min = _guess_op_min_ram_gb(op, max_ram[largest_pool])
        base_min = guessed_min if (guessed_min is not None) else ram_floor_abs
        ram_req = max(ram_floor_abs, base_min * ram_safety, hinted_ram)

        # Avoid asking for truly the entire pool unless forced by hint
        ram_req = min(ram_req, max_ram[largest_pool])

        # Prefer hinted pool if present (e.g., after OOM), otherwise try all pools.
        pool_hint = s.op_pool_hint.get(op_id)
        pool_order = []
        if pool_hint is not None and 0 <= int(pool_hint) < num_pools:
            pool_order.append(int(pool_hint))
        pool_order.extend([i for i in range(num_pools) if i not in pool_order])

        # Now pick the best feasible pool among pool_order.
        best_pool = None
        best_cpu_alloc = None
        best_key = None

        for pool_id in pool_order:
            cpu_target, cpu_min, ram_floor, ram_safety2 = _resource_profile(sched_class, max_cpu[pool_id], max_ram[pool_id])

            # Recompute ram_req for this pool (needed if pools are heterogeneous)
            guessed_min2 = _guess_op_min_ram_gb(op, max_ram[pool_id])
            base_min2 = guessed_min2 if (guessed_min2 is not None) else ram_floor
            ram_req2 = max(ram_floor, base_min2 * ram_safety2, hinted_ram)
            ram_req2 = min(ram_req2, max_ram[pool_id])

            if nonquery:
                eff_cpu, eff_ram = _effective_avail_for_nonquery(pool_id)
            else:
                eff_cpu, eff_ram = avail_cpu[pool_id], avail_ram[pool_id]

            if eff_ram < ram_req2 or eff_cpu < cpu_min:
                continue

            cpu_alloc = min(cpu_target, eff_cpu)
            if cpu_alloc < cpu_min:
                continue

            if sched_class == "query":
                key = (-eff_ram, -eff_cpu)
            else:
                key = (eff_ram - ram_req2, eff_cpu - cpu_alloc)

            if best_key is None or key < best_key:
                best_key = key
                best_pool = pool_id
                best_cpu_alloc = cpu_alloc
                ram_req = ram_req2  # keep the pool-specific ram

        if best_pool is None:
            return False

        # Place it
        assignments.append(
            Assignment(
                ops=[op],
                cpu=best_cpu_alloc,
                ram=ram_req,
                priority=p.priority,
                pool_id=best_pool,
                pipeline_id=pid,
            )
        )

        # Accounting
        if nonquery and best_pool == 0:
            # We already ensured we wouldn't consume reserved headroom by using effective_avail;
            # deduct from real avail.
            pass

        avail_cpu[best_pool] -= best_cpu_alloc
        avail_ram[best_pool] -= ram_req

        scheduled_count[pid] = already + 1
        s.op_to_pipeline[op_id] = pid

        # If we successfully place it, drop any pool hint (fresh start); keep RAM hint.
        if op_id in s.op_pool_hint:
            del s.op_pool_hint[op_id]

        return True

    def _schedule_query_phase():
        # Aggressively schedule queries first (lowest latency), using full pool capacity.
        tries = 0
        max_tries = max(128, len(s.q_query) * 2)
        while len(assignments) < s.max_assignments_per_tick and tries < max_tries:
            # If no capacity anywhere, stop.
            if all(avail_cpu[i] <= 0.0 or avail_ram[i] <= 0.0 for i in range(num_pools)):
                break

            p, op = _pick_one_op(s.q_query, require_aged_batch=False)
            if p is None:
                break

            placed = _try_place_op(p, op, sched_class="query", nonquery=False)
            if placed:
                tries = 0
            else:
                tries += 1

    def _schedule_interactive_and_batch_phase():
        # Share remaining capacity between interactive and batch to avoid starving either class.
        # Non-query tasks respect headroom reservation on pool 0.
        def _nonquery_total_effective_cpu():
            total = 0.0
            for i in range(num_pools):
                eff_cpu, _ = _effective_avail_for_nonquery(i)
                total += eff_cpu
            return total

        initial_cpu = _nonquery_total_effective_cpu()
        interactive_budget = initial_cpu * float(s.interactive_cpu_share)

        interactive_used = 0.0

        # First pass: INTERACTIVE (and aged BATCH spill-in) up to budget
        tries = 0
        max_tries = max(256, (len(s.q_interactive) + len(s.q_batch)) * 2)

        while len(assignments) < s.max_assignments_per_tick and tries < max_tries:
            eff_cpu_total = _nonquery_total_effective_cpu()
            if eff_cpu_total <= 0.0:
                break

            if interactive_used >= interactive_budget:
                break

            p, op = _pick_one_op(s.q_interactive, require_aged_batch=False)
            if p is None:
                # Spill-in: schedule some aged batch as "interactive-class" to prevent starvation
                p, op = _pick_one_op(s.q_batch, require_aged_batch=True)
                if p is None:
                    break

            before = sum(avail_cpu)
            placed = _try_place_op(p, op, sched_class="interactive", nonquery=True)
            after = sum(avail_cpu)
            if placed:
                interactive_used += max(0.0, before - after)
                tries = 0
            else:
                tries += 1

        # Second pass: BATCH (no budget limit, still respects pool 0 headroom)
        tries = 0
        max_tries = max(512, len(s.q_batch) * 3)

        while len(assignments) < s.max_assignments_per_tick and tries < max_tries:
            eff_cpu_total = _nonquery_total_effective_cpu()
            if eff_cpu_total <= 0.0:
                break

            p, op = _pick_one_op(s.q_batch, require_aged_batch=False)
            if p is None:
                break

            placed = _try_place_op(p, op, sched_class="batch", nonquery=True)
            if placed:
                tries = 0
            else:
                tries += 1

        # Third pass: if batch couldn't use all remaining capacity, let interactive fill slack
        tries = 0
        max_tries = max(256, len(s.q_interactive) * 2)
        while len(assignments) < s.max_assignments_per_tick and tries < max_tries:
            eff_cpu_total = _nonquery_total_effective_cpu()
            if eff_cpu_total <= 0.0:
                break

            p, op = _pick_one_op(s.q_interactive, require_aged_batch=False)
            if p is None:
                break

            placed = _try_place_op(p, op, sched_class="interactive", nonquery=True)
            if placed:
                tries = 0
            else:
                tries += 1

    _schedule_query_phase()
    _schedule_interactive_and_batch_phase()

    return suspensions, assignments
