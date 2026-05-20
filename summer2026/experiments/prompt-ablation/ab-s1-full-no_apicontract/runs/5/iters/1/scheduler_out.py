from collections import deque


@register_scheduler_init(key="scheduler_high_006")
def scheduler_high_006_init(s):
    s.tick = 0

    # Active (candidate) pipeline queues by priority. Store pipeline_id to de-dup cheaply.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()
    s.in_q_query = set()
    s.in_q_interactive = set()
    s.in_q_batch = set()

    # Keep pipeline objects for runtime_status() lookups.
    s.pipelines_by_id = {}

    # Pipeline metadata and failure tracking.
    s.pipeline_meta = {}  # pipeline_id -> dict(arrival_tick, next_eligible_tick)
    s.pipeline_permanent_fail = set()

    # Memory discovery via OOM feedback (per-operator, per-pipeline floors).
    s.op_info = {}  # id(op) -> dict(oom, nonoom, lb_ram, hint_ram, last_ram, pri)
    s.pipeline_ram_floor = {}  # pipeline_id -> ram_floor (raised after OOMs)

    # Map operator object identity to its pipeline_id (learned when we schedule it).
    s.op_to_pipeline = {}

    # Retry bounds (must be finite).
    s.max_oom_retries_per_op = 5
    s.max_nonoom_retries_per_op = 2

    # Per-tick scheduling bounds.
    s.max_new_assignments_per_tick = 16
    s.max_candidates_per_place = 48  # bounded scan within an active queue

    # Base per-op allocations (absolute, not fractions of pool size).
    s.base_cpu_query = 6.0
    s.base_cpu_interactive = 4.0
    s.base_cpu_batch = 2.0

    s.max_cpu_query = 16.0
    s.max_cpu_interactive = 12.0
    s.max_cpu_batch = 6.0

    # RAM in "GB-like" units (sim uses same units as pool max).
    # Slightly higher initial RAM reduces OOM churn (most OOMs were on first-try sizing).
    s.base_ram_query = 32.0
    s.base_ram_interactive = 24.0
    s.base_ram_batch = 18.0

    # Soft "no-oom-yet" caps to prevent wasting huge RAM on unknown ops.
    s.soft_cap_ram_query = 256.0
    s.soft_cap_ram_interactive = 192.0
    s.soft_cap_ram_batch = 160.0

    # Batch admission reserve (avoid locking out high priority without preemption).
    # Keep a modest absolute reserve and a small fractional reserve; take the larger, then cap it.
    s.batch_reserve_cpu_abs = 12.0
    s.batch_reserve_cpu_frac = 0.03
    s.batch_reserve_cpu_cap = 48.0

    s.batch_reserve_ram_abs = 64.0
    s.batch_reserve_ram_frac = 0.03
    s.batch_reserve_ram_cap = 512.0

    # Backoff (in ticks) for pipelines we couldn't place due to headroom.
    s.place_fail_backoff_ticks = 5

    # Structural safeguard:
    # Some cluster scales may expose pool-level totals while placement still requires per-container
    # sizing consistent with a fixed "node" shape. Use a conservative effective per-container cap
    # derived from pool (cpu, ram) totals assuming a 64-vCPU node unit when pools aggregate nodes.
    s.node_cpu_unit = 64.0
    s.ram_safety_margin = 0.25  # avoid boundary/rounding issues at max


@register_scheduler(key="scheduler_high_006")
def scheduler_high_006_scheduler(s, results, pipelines):
    s.tick += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _q_structs_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query, s.in_q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive, s.in_q_interactive
        return s.q_batch, s.in_q_batch

    def _ensure_pipeline_meta(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"arrival_tick": s.tick, "next_eligible_tick": 0}

    def _activate_pipeline_id(pid, pri, delay_ticks=0):
        if pid is None:
            return
        if pid in s.pipeline_permanent_fail:
            return
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return
        _ensure_pipeline_meta(p)
        if delay_ticks > 0:
            nt = s.tick + delay_ticks
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                prev = meta.get("next_eligible_tick", 0)
                if nt > prev:
                    meta["next_eligible_tick"] = nt

        q, in_q = _q_structs_for_priority(pri)
        if pid not in in_q:
            q.append(pid)
            in_q.add(pid)

    def _activate_pipeline(p, delay_ticks=0):
        s.pipelines_by_id[p.pipeline_id] = p
        _ensure_pipeline_meta(p)
        _activate_pipeline_id(p.pipeline_id, p.priority, delay_ticks=delay_ticks)

    def _effective_max_ram(max_cpu_i, max_ram_i):
        mr = float(max_ram_i or 0.0)
        mc = float(max_cpu_i or 0.0)
        if mr <= 0.0:
            return 0.0
        if mc <= 0.0:
            return max(0.0, mr - s.ram_safety_margin)
        # If a pool aggregates multiple node-units, cap per-container RAM to the per-node share.
        eff = mr
        if mc > s.node_cpu_unit + 1e-9:
            eff = mr * (s.node_cpu_unit / mc)
        eff = min(mr, eff)
        eff = max(0.0, eff - s.ram_safety_margin)
        return eff

    def _pool_best_fit(pri, cpu_req, ram_req, avail_cpu, avail_ram, max_cpu, max_ram, highpri_ready):
        # Return pool_id or None
        best_pool = None
        best_score = None

        for i in range(s.executor.num_pools):
            if cpu_req > avail_cpu[i] or ram_req > avail_ram[i]:
                continue

            if pri == Priority.BATCH_PIPELINE:
                # Enforce reserve to avoid locking out query/interactive (no preemption available).
                reserve_cpu = s.batch_reserve_cpu_abs
                reserve_cpu = max(reserve_cpu, s.batch_reserve_cpu_frac * max_cpu[i])
                reserve_cpu = min(reserve_cpu, s.batch_reserve_cpu_cap)

                reserve_ram = s.batch_reserve_ram_abs
                reserve_ram = max(reserve_ram, s.batch_reserve_ram_frac * max_ram[i])
                reserve_ram = min(reserve_ram, s.batch_reserve_ram_cap)

                # If there is runnable high-priority work, be a bit more conservative.
                if highpri_ready:
                    reserve_cpu = min(s.batch_reserve_cpu_cap, reserve_cpu * 1.35)
                    reserve_ram = min(s.batch_reserve_ram_cap, reserve_ram * 1.35)

                if (avail_cpu[i] - cpu_req) < reserve_cpu or (avail_ram[i] - ram_req) < reserve_ram:
                    continue

            # Best-fit packing to improve utilization and reduce fragmentation.
            # Minimize normalized leftover.
            cpu_left = (avail_cpu[i] - cpu_req) / (max_cpu[i] + 1e-9)
            ram_left = (avail_ram[i] - ram_req) / (max_ram[i] + 1e-9)
            score = cpu_left + ram_left

            if best_score is None or score < best_score:
                best_score = score
                best_pool = i

        return best_pool

    def _base_cpu_ram_for_priority(pri):
        if pri == Priority.QUERY:
            return s.base_cpu_query, s.base_ram_query, s.max_cpu_query, s.soft_cap_ram_query
        if pri == Priority.INTERACTIVE:
            return s.base_cpu_interactive, s.base_ram_interactive, s.max_cpu_interactive, s.soft_cap_ram_interactive
        return s.base_cpu_batch, s.base_ram_batch, s.max_cpu_batch, s.soft_cap_ram_batch

    def _compute_req(pri, pid, op, pool_id, avail_cpu_i, avail_ram_i, max_cpu_i, max_ram_i, highpri_ready):
        base_cpu, base_ram, max_cpu_cap, soft_ram_cap = _base_cpu_ram_for_priority(pri)
        op_id = id(op)

        info = s.op_info.get(op_id)
        if info is None:
            info = {"oom": 0, "nonoom": 0, "lb_ram": 0.0, "hint_ram": 0.0, "last_ram": 0.0, "pri": pri}
            s.op_info[op_id] = info
        else:
            info["pri"] = pri

        eff_max_ram_i = _effective_max_ram(max_cpu_i, max_ram_i)

        # RAM target:
        # - start from base
        # - respect pipeline floor (if any)
        # - if we have a hint from prior OOM, allocate at/above it
        # - if we have a known lower bound (failed at lb), allocate above it
        pipeline_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
        lb = float(info.get("lb_ram", 0.0) or 0.0)
        hint = float(info.get("hint_ram", 0.0) or 0.0)

        ram_target = max(base_ram, pipeline_floor, hint)

        # First-try (no history): use a slightly higher probe size when the pool is very empty
        # to reduce systematic first-try OOMs on larger clusters.
        if lb <= 0.0 and hint <= 0.0:
            ram_free_frac = float(avail_ram_i or 0.0) / (float(max_ram_i or 0.0) + 1e-9)
            if ram_free_frac > 0.75:
                ram_target = max(ram_target, base_ram * 1.60)
            elif ram_free_frac > 0.55:
                ram_target = max(ram_target, base_ram * 1.30)

        if lb > 0.0:
            ram_target = max(ram_target, lb * 1.45, lb + 10.0)

        # Prevent wasting enormous RAM on unknown ops (unless OOM history says we need it).
        if lb <= 0.0 and hint <= 0.0 and ram_target > soft_ram_cap:
            ram_target = soft_ram_cap

        # Hard cap by effective per-container maximum (plus safety).
        if eff_max_ram_i > 0.0:
            ram_target = min(ram_target, eff_max_ram_i)
        else:
            return None, None

        # If we're retrying after OOM (lb/hint), do not run with insufficient RAM.
        # But never block scheduling purely because a hint exceeds the effective max.
        min_required_ram = base_ram
        if lb > 0.0:
            min_required_ram = max(min_required_ram, min(eff_max_ram_i, lb * 1.05))
        if hint > 0.0:
            min_required_ram = max(min_required_ram, min(eff_max_ram_i, hint))

        if float(avail_ram_i or 0.0) + 1e-9 < min_required_ram:
            return None, None

        ram_req = min(ram_target, float(avail_ram_i or 0.0))
        if ram_req + 1e-9 < min_required_ram:
            return None, None

        # CPU target:
        cpu_target = base_cpu

        # Opportunistically scale CPU up a bit when the pool is very empty, especially for high priority.
        cpu_free_frac = float(avail_cpu_i or 0.0) / (float(max_cpu_i or 0.0) + 1e-9)
        if pri != Priority.BATCH_PIPELINE:
            if cpu_free_frac > 0.75:
                cpu_target = min(max_cpu_cap, base_cpu * 1.8)
            elif cpu_free_frac > 0.50:
                cpu_target = min(max_cpu_cap, base_cpu * 1.4)
        else:
            # Batch only scales up when there isn't runnable high-priority work and pool is empty.
            if (not highpri_ready) and cpu_free_frac > 0.70:
                cpu_target = min(max_cpu_cap, base_cpu * 1.8)
            elif (not highpri_ready) and cpu_free_frac > 0.50:
                cpu_target = min(max_cpu_cap, base_cpu * 1.4)

        cpu_target = min(cpu_target, float(max_cpu_i or 0.0))
        cpu_req = min(cpu_target, float(avail_cpu_i or 0.0))

        # Avoid vanishing allocations.
        if cpu_req <= 0.0 or ram_req <= 0.0:
            return None, None

        return cpu_req, ram_req

    # 1) Register new pipelines (arrivals) and activate them.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        _ensure_pipeline_meta(p)
        _activate_pipeline(p)

    # 2) Learn from results and re-activate pipelines whose state may have changed.
    for r in results:
        # Re-activate pipeline on both completion and failure (next ops may become runnable).
        pid = getattr(r, "pipeline_id", None)

        # Update op -> pipeline mapping if possible.
        if pid is not None and getattr(r, "ops", None):
            for op in r.ops:
                s.op_to_pipeline[id(op)] = pid

        if pid is None and getattr(r, "ops", None):
            # Fall back to op_to_pipeline mapping.
            for op in r.ops:
                pid = s.op_to_pipeline.get(id(op))
                if pid is not None:
                    break

        pri = getattr(r, "priority", None)

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if getattr(r, "ops", None):
            is_oom = failed and _is_oom_error(getattr(r, "error", None))
            pool_id = getattr(r, "pool_id", None)

            pool_max_ram = None
            pool_max_cpu = None
            pool_eff_max_ram = None
            if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                pool_max_ram = float(s.executor.pools[pool_id].max_ram_pool or 0.0)
                pool_max_cpu = float(s.executor.pools[pool_id].max_cpu_pool or 0.0)
                pool_eff_max_ram = _effective_max_ram(pool_max_cpu, pool_max_ram)

            attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if attempted_ram <= 0.0:
                attempted_ram = 0.0

            for op in r.ops:
                op_id = id(op)
                info = s.op_info.get(op_id)
                if info is None:
                    info = {"oom": 0, "nonoom": 0, "lb_ram": 0.0, "hint_ram": 0.0, "last_ram": 0.0, "pri": pri}
                    s.op_info[op_id] = info

                info["last_ram"] = max(float(info.get("last_ram", 0.0) or 0.0), attempted_ram)
                if pri is not None:
                    info["pri"] = pri

                if failed:
                    if is_oom:
                        info["oom"] = int(info.get("oom", 0) or 0) + 1
                        # Update lower bound and next hint.
                        if attempted_ram > 0.0:
                            info["lb_ram"] = max(float(info.get("lb_ram", 0.0) or 0.0), attempted_ram)

                        # Raise pipeline floor a bit to reduce repeated OOMs within the same pipeline.
                        if pid is not None and attempted_ram > 0.0:
                            prev_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
                            bump_floor = max(attempted_ram * 1.18, attempted_ram + 10.0)
                            if pool_eff_max_ram is not None and pool_eff_max_ram > 0.0:
                                bump_floor = min(bump_floor, float(pool_eff_max_ram))
                            s.pipeline_ram_floor[pid] = max(prev_floor, bump_floor)

                        # Next hint: exponential-ish growth, capped by effective per-container max.
                        prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                        next_hint = attempted_ram * 1.60 if attempted_ram > 0.0 else prev_hint * 1.60
                        if next_hint <= 0.0:
                            next_hint = prev_hint if prev_hint > 0.0 else 0.0

                        lbv = float(info.get("lb_ram", 0.0) or 0.0)
                        next_hint = max(next_hint, lbv * 1.35, lbv + 12.0)

                        if pool_eff_max_ram is not None and pool_eff_max_ram > 0.0:
                            next_hint = min(next_hint, float(pool_eff_max_ram))

                        info["hint_ram"] = max(prev_hint, next_hint)

                        # If too many OOMs, fail the pipeline to avoid infinite retries.
                        if pid is not None and info["oom"] > s.max_oom_retries_per_op:
                            s.pipeline_permanent_fail.add(pid)

                    else:
                        info["nonoom"] = int(info.get("nonoom", 0) or 0) + 1
                        # Mildly increase RAM hint on non-OOM failure (often still resource-related).
                        prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                        if attempted_ram > 0.0:
                            bump = attempted_ram * 1.15
                            if pool_eff_max_ram is not None and pool_eff_max_ram > 0.0:
                                bump = min(bump, float(pool_eff_max_ram))
                            info["hint_ram"] = max(prev_hint, bump)

                        if pid is not None and info["nonoom"] > s.max_nonoom_retries_per_op:
                            s.pipeline_permanent_fail.add(pid)

        # Reactivate pipeline unless we permanently failed it.
        if pid is not None and pid not in s.pipeline_permanent_fail:
            p_obj = s.pipelines_by_id.get(pid)
            if p_obj is not None:
                _activate_pipeline_id(pid, p_obj.priority, delay_ticks=0)

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    avail_cpu = [float(s.executor.pools[i].avail_cpu_pool or 0.0) for i in range(num_pools)]
    avail_ram = [float(s.executor.pools[i].avail_ram_pool or 0.0) for i in range(num_pools)]
    max_cpu = [float(s.executor.pools[i].max_cpu_pool or 0.0) for i in range(num_pools)]
    max_ram = [float(s.executor.pools[i].max_ram_pool or 0.0) for i in range(num_pools)]

    scheduled_pipeline_ids = set()

    def _highpri_ready():
        # These are "active candidates" (pipelines likely to have runnable ops).
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _place_one(pri):
        q, in_q = _q_structs_for_priority(pri)
        if not q:
            return False

        hi_ready = _highpri_ready()

        for _ in range(s.max_candidates_per_place):
            if not q:
                return False

            pid = q.popleft()
            in_q.discard(pid)

            if pid in s.pipeline_permanent_fail:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            meta = s.pipeline_meta.get(pid)
            if meta is not None and meta.get("next_eligible_tick", 0) > s.tick:
                # Not eligible yet; keep it active but delayed.
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                continue

            if pid in scheduled_pipeline_ids:
                # At most one op per pipeline per tick.
                _activate_pipeline_id(pid, pri, delay_ticks=1)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # No runnable work now; wait for results to reactivate.
                continue

            op = ops[0]
            s.op_to_pipeline[id(op)] = pid

            # Find best pool and size. Try per-pool best fit.
            chosen_pool = None
            chosen_cpu = None
            chosen_ram = None
            best_score = None

            for pool_id in range(num_pools):
                if avail_cpu[pool_id] <= 0.0 or avail_ram[pool_id] <= 0.0:
                    continue

                cpu_req, ram_req = _compute_req(
                    pri=pri,
                    pid=pid,
                    op=op,
                    pool_id=pool_id,
                    avail_cpu_i=avail_cpu[pool_id],
                    avail_ram_i=avail_ram[pool_id],
                    max_cpu_i=max_cpu[pool_id],
                    max_ram_i=max_ram[pool_id],
                    highpri_ready=hi_ready,
                )
                if cpu_req is None or ram_req is None:
                    continue

                # Enforce batch reserve + choose best-fit pool.
                pool_ok = _pool_best_fit(
                    pri=pri,
                    cpu_req=cpu_req,
                    ram_req=ram_req,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    max_cpu=max_cpu,
                    max_ram=max_ram,
                    highpri_ready=hi_ready,
                )
                if pool_ok is None or pool_ok != pool_id:
                    continue

                # Score (lower is better) for tie-breaking among pools that pass reserve.
                cpu_left = (avail_cpu[pool_id] - cpu_req) / (max_cpu[pool_id] + 1e-9)
                ram_left = (avail_ram[pool_id] - ram_req) / (max_ram[pool_id] + 1e-9)
                score = cpu_left + ram_left

                if best_score is None or score < best_score:
                    best_score = score
                    chosen_pool = pool_id
                    chosen_cpu = cpu_req
                    chosen_ram = ram_req

            if chosen_pool is None:
                # Couldn't place now; requeue with backoff to avoid hot-spinning.
                if meta is None:
                    _ensure_pipeline_meta(p)
                    meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["next_eligible_tick"] = max(meta.get("next_eligible_tick", 0), s.tick + s.place_fail_backoff_ticks)
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                return False

            # Final structural clamps (fixes rare scale-specific pool accounting differences).
            eff_max_ram = _effective_max_ram(max_cpu[chosen_pool], max_ram[chosen_pool])
            if eff_max_ram > 0.0:
                chosen_ram = min(chosen_ram, eff_max_ram)
            # Also clamp to current pool availability snapshot (never increases RAM/CPU).
            chosen_ram = min(chosen_ram, float(s.executor.pools[chosen_pool].avail_ram_pool or 0.0))
            chosen_cpu = min(chosen_cpu, float(s.executor.pools[chosen_pool].avail_cpu_pool or 0.0))
            chosen_ram = max(0.0, chosen_ram)
            chosen_cpu = max(0.0, chosen_cpu)

            if chosen_cpu <= 0.0 or chosen_ram <= 0.0:
                # Treat as place-failed; retry later.
                if meta is None:
                    _ensure_pipeline_meta(p)
                    meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["next_eligible_tick"] = max(meta.get("next_eligible_tick", 0), s.tick + s.place_fail_backoff_ticks)
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                return False

            # Place assignment.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=p.priority,
                    pool_id=chosen_pool,
                    pipeline_id=pid,
                )
            )
            avail_cpu[chosen_pool] -= chosen_cpu
            avail_ram[chosen_pool] -= chosen_ram
            scheduled_pipeline_ids.add(pid)

            # If there are multiple runnable ops right now (parallelism), re-activate quickly.
            if len(ops) > 1:
                _activate_pipeline_id(pid, pri, delay_ticks=1)

            return True

        return False

    # Priority-weighted per-tick budgets (keep query snappy, but push interactive completion).
    remaining = int(s.max_new_assignments_per_tick)

    def _drain(pri, budget):
        nonlocal remaining
        placed_any = False
        while budget > 0 and remaining > 0:
            # Stop if the cluster is effectively full.
            if all((avail_cpu[i] <= 0.0 or avail_ram[i] <= 0.0) for i in range(num_pools)):
                return placed_any
            if not _place_one(pri):
                return placed_any
            placed_any = True
            budget -= 1
            remaining -= 1
        return placed_any

    # Dynamic budgets: if higher-priority queues are empty, reallocate.
    qn = len(s.q_query)
    in_n = len(s.q_interactive)
    bn = len(s.q_batch)

    # Base split tuned for this workload mix (many interactive + batch).
    b_query = 4
    b_inter = 10
    b_batch = 2

    # If no query candidates, shift to interactive/batch.
    if qn == 0:
        b_inter += 2
        b_batch += 1
        b_query = 0

    # If interactive is empty, let batch fill more.
    if in_n == 0:
        b_batch += 3
        if b_query > 0:
            b_query += 1
        b_inter = 0

    # If batch is empty, allocate to interactive.
    if bn == 0:
        b_inter += b_batch
        b_batch = 0

    total_budget = b_query + b_inter + b_batch
    if total_budget > 0:
        scale = min(1.0, float(s.max_new_assignments_per_tick) / float(total_budget))
        b_query = int(b_query * scale)
        b_inter = int(b_inter * scale)
        b_batch = int(b_batch * scale)

    # Ensure we don't end up with all zeros when work exists.
    if remaining > 0 and qn > 0 and b_query == 0:
        b_query = 1
    if remaining > 0 and in_n > 0 and b_inter == 0:
        b_inter = 1
    if remaining > 0 and bn > 0 and b_batch == 0:
        b_batch = 1

    # Execute in strict priority order (no preemption, so keep it simple).
    _drain(Priority.QUERY, b_query)
    _drain(Priority.INTERACTIVE, b_inter)
    _drain(Priority.BATCH_PIPELINE, b_batch)

    # If we still have headroom and budget, opportunistically fill (interactive first, then batch).
    if remaining > 0:
        _drain(Priority.INTERACTIVE, remaining)
    if remaining > 0:
        _drain(Priority.BATCH_PIPELINE, remaining)

    return suspensions, assignments
