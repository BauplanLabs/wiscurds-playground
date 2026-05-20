@register_scheduler_init(key="scheduler_high_006")
def scheduler_high_006_init(s):
    from collections import deque

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
    # NOTE: op objects are stable across retries; we learn per-id(op).
    s.op_info = {}  # id(op) -> dict(oom, nonoom, timeout, lb_ram, hint_ram, last_ram, hint_cpu, last_cpu, pri)
    s.pipeline_ram_floor = {}  # pipeline_id -> ram_floor (raised after OOMs)

    # Cross-pipeline learning by operator "kind" (cheap proxy: type(op).__name__).
    s.op_kind_cache = {}  # id(op) -> kind_str
    s.kind_info = {}  # (priority, kind_str) -> dict(lb_ram, hint_ram, oom, timeout, hint_cpu)

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
    s.base_ram_query = 24.0
    s.base_ram_interactive = 16.0
    s.base_ram_batch = 12.0

    # Soft "no-oom-yet" caps to prevent wasting huge RAM on unknown ops.
    s.soft_cap_ram_query = 192.0
    s.soft_cap_ram_interactive = 128.0
    s.soft_cap_ram_batch = 96.0

    # Cap how much a single OOM in a pipeline can inflate subsequent ops (unless the op itself learned higher).
    s.pipeline_floor_cap_mult_query = 6.0
    s.pipeline_floor_cap_mult_interactive = 5.0
    s.pipeline_floor_cap_mult_batch = 4.0

    # Batch admission reserve (avoid locking out high priority without preemption).
    # Keep a modest absolute reserve and a small fractional reserve; take the larger, then cap it.
    # Slightly relaxed vs prior version to avoid batch starvation at small scales.
    s.batch_reserve_cpu_abs = 8.0
    s.batch_reserve_cpu_frac = 0.02
    s.batch_reserve_cpu_cap = 48.0

    s.batch_reserve_ram_abs = 48.0
    s.batch_reserve_ram_frac = 0.02
    s.batch_reserve_ram_cap = 512.0

    # Backoff (in ticks) for pipelines we couldn't place due to headroom.
    s.place_fail_backoff_ticks = 5

    # Floating safety: quantize allocations downward to avoid tiny float-sum overallocations (seen on some scales).
    s.alloc_quant = 1000.0  # 0.001 granularity
    s.alloc_eps = 1e-6

    # More accurate notion of "runnable high priority exists" to avoid permanently over-reserving for batch.
    s.hi_runnable_until_tick = 0
    s.hi_runnable_hold_ticks = 250  # ~2.5s if 100 ticks/s

    # Batch aging: if batch has waited long, relax reserve slightly (still keep some headroom).
    s.batch_urgent_age_ticks = 6000  # ~60s if 100 ticks/s


@register_scheduler(key="scheduler_high_006")
def scheduler_high_006_scheduler(s, results, pipelines):
    s.tick += 1

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

    def _quant_floor(x):
        if x is None:
            return None
        if x <= 0.0:
            return 0.0
        q = s.alloc_quant
        return float(int(x * q)) / q

    def _cap_pos(x, avail):
        if x is None:
            return None
        if avail is None:
            return None
        if avail <= 0.0:
            return 0.0
        v = x
        if v > avail:
            v = avail
        # leave a tiny epsilon to avoid float edge overallocation
        v = v - s.alloc_eps
        if v < 0.0:
            v = 0.0
        return _quant_floor(v)

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

    def _pipeline_floor_cap_mult(pri):
        if pri == Priority.QUERY:
            return s.pipeline_floor_cap_mult_query
        if pri == Priority.INTERACTIVE:
            return s.pipeline_floor_cap_mult_interactive
        return s.pipeline_floor_cap_mult_batch

    def _pool_best_fit(pri, cpu_req, ram_req, avail_cpu, avail_ram, max_cpu, max_ram, highpri_ready, batch_age_ticks=0):
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
                    reserve_cpu = min(s.batch_reserve_cpu_cap, reserve_cpu * 1.20)
                    reserve_ram = min(s.batch_reserve_ram_cap, reserve_ram * 1.20)

                # If batch is very old, relax reserve a bit to avoid full starvation.
                if batch_age_ticks >= s.batch_urgent_age_ticks:
                    reserve_cpu = max(0.0, reserve_cpu * 0.70)
                    reserve_ram = max(0.0, reserve_ram * 0.70)

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

    def _op_kind(op):
        op_id = id(op)
        k = s.op_kind_cache.get(op_id)
        if k is not None:
            return k
        try:
            k = type(op).__name__
        except Exception:
            k = "op"
        if not k:
            k = "op"
        s.op_kind_cache[op_id] = k
        return k

    def _kind_rec(pri, kind):
        key = (pri, kind)
        rec = s.kind_info.get(key)
        if rec is None:
            rec = {"lb_ram": 0.0, "hint_ram": 0.0, "oom": 0, "timeout": 0, "hint_cpu": 0.0}
            s.kind_info[key] = rec
        return rec

    def _compute_req(pri, pid, op, pool_id, avail_cpu_i, avail_ram_i, max_cpu_i, max_ram_i, highpri_ready):
        base_cpu, base_ram, max_cpu_cap, soft_ram_cap = _base_cpu_ram_for_priority(pri)
        op_id = id(op)

        info = s.op_info.get(op_id)
        if info is None:
            info = {
                "oom": 0,
                "nonoom": 0,
                "timeout": 0,
                "lb_ram": 0.0,
                "hint_ram": 0.0,
                "last_ram": 0.0,
                "hint_cpu": 0.0,
                "last_cpu": 0.0,
                "pri": pri,
            }
            s.op_info[op_id] = info
        else:
            info["pri"] = pri

        kind = _op_kind(op)
        krec = _kind_rec(pri, kind)

        # RAM target:
        pipeline_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
        lb = float(info.get("lb_ram", 0.0) or 0.0)
        hint = float(info.get("hint_ram", 0.0) or 0.0)

        kind_lb = float(krec.get("lb_ram", 0.0) or 0.0)
        kind_hint = float(krec.get("hint_ram", 0.0) or 0.0)

        # Cap pipeline floor so one heavy op doesn't force huge RAM for all ops in the pipeline.
        if lb <= 0.0 and hint <= 0.0:
            cap_mult = _pipeline_floor_cap_mult(pri)
            if pipeline_floor > base_ram * cap_mult:
                pipeline_floor = base_ram * cap_mult

        ram_target = max(base_ram, pipeline_floor)

        # If this op has local history, trust it.
        if hint > 0.0:
            ram_target = max(ram_target, hint)
        # Otherwise, use kind-level hint/lower-bound to reduce first-try OOMs.
        elif kind_hint > 0.0:
            ram_target = max(ram_target, kind_hint)
        elif kind_lb > 0.0:
            ram_target = max(ram_target, kind_lb * 1.20 + 8.0)

        # If we have a known lower bound (OOM'd at lb), allocate above it.
        if lb > 0.0:
            ram_target = max(ram_target, lb * 1.25 + 12.0, lb + 16.0)

        # Prevent wasting enormous RAM on first-try ops (unless kind/lb/hint says we need it).
        if lb <= 0.0 and hint <= 0.0 and kind_lb <= 0.0 and kind_hint <= 0.0 and ram_target > soft_ram_cap:
            ram_target = soft_ram_cap

        # Hard cap by pool maximum.
        if ram_target > max_ram_i:
            ram_target = max_ram_i

        # Minimum RAM we are willing to run with:
        min_required_ram = base_ram
        if lb > 0.0:
            min_required_ram = max(min_required_ram, lb * 1.05)
        if hint > 0.0:
            min_required_ram = max(min_required_ram, hint * 0.98)
        if hint <= 0.0 and lb <= 0.0 and kind_hint > 0.0:
            min_required_ram = max(min_required_ram, kind_hint * 0.98)

        if avail_ram_i + 1e-12 < min_required_ram:
            return None, None

        ram_req = _cap_pos(ram_target, avail_ram_i)
        if ram_req is None or ram_req <= 0.0:
            return None, None
        if ram_req + 1e-9 < min_required_ram:
            return None, None

        # CPU target:
        cpu_target = base_cpu

        cpu_hint = float(info.get("hint_cpu", 0.0) or 0.0)
        kind_cpu_hint = float(krec.get("hint_cpu", 0.0) or 0.0)
        if cpu_hint > 0.0:
            cpu_target = max(cpu_target, cpu_hint)
        elif kind_cpu_hint > 0.0:
            cpu_target = max(cpu_target, kind_cpu_hint)

        # Opportunistically scale CPU up a bit when the pool is very empty.
        cpu_free_frac = avail_cpu_i / (max_cpu_i + 1e-9)
        if pri != Priority.BATCH_PIPELINE:
            if cpu_free_frac > 0.75:
                cpu_target = max(cpu_target, min(max_cpu_cap, base_cpu * 1.8))
            elif cpu_free_frac > 0.50:
                cpu_target = max(cpu_target, min(max_cpu_cap, base_cpu * 1.4))
        else:
            if (not highpri_ready) and cpu_free_frac > 0.70:
                cpu_target = max(cpu_target, min(max_cpu_cap, base_cpu * 1.8))
            elif (not highpri_ready) and cpu_free_frac > 0.50:
                cpu_target = max(cpu_target, min(max_cpu_cap, base_cpu * 1.4))

        if cpu_target > max_cpu_i:
            cpu_target = max_cpu_i

        cpu_req = _cap_pos(cpu_target, avail_cpu_i)
        if cpu_req is None or cpu_req <= 0.0:
            return None, None

        return cpu_req, ram_req

    # 1) Register new pipelines (arrivals) and activate them.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        _ensure_pipeline_meta(p)
        _activate_pipeline(p)

    # 2) Learn from results and re-activate pipelines whose state may have changed.
    for r in results:
        pid = getattr(r, "pipeline_id", None)

        # Update op -> pipeline mapping if possible.
        if pid is not None and getattr(r, "ops", None):
            for op in r.ops:
                s.op_to_pipeline[id(op)] = pid

        if pid is None and getattr(r, "ops", None):
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

        err = getattr(r, "error", None)
        is_oom = failed and _is_oom_error(err)
        is_timeout = failed and (not is_oom) and _is_timeout_error(err)

        pool_id = getattr(r, "pool_id", None)
        pool_max_ram = None
        if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
            try:
                pool_max_ram = s.executor.pools[pool_id].max_ram_pool
            except Exception:
                pool_max_ram = None

        attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
        attempted_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

        if getattr(r, "ops", None):
            for op in r.ops:
                op_id = id(op)

                if pid is not None:
                    s.op_to_pipeline[op_id] = pid

                if pri is None:
                    # Best-effort fallback: infer priority from stored info.
                    prior = s.op_info.get(op_id)
                    if prior is not None:
                        pri = prior.get("pri", None)

                info = s.op_info.get(op_id)
                if info is None:
                    info = {
                        "oom": 0,
                        "nonoom": 0,
                        "timeout": 0,
                        "lb_ram": 0.0,
                        "hint_ram": 0.0,
                        "last_ram": 0.0,
                        "hint_cpu": 0.0,
                        "last_cpu": 0.0,
                        "pri": pri,
                    }
                    s.op_info[op_id] = info

                if pri is not None:
                    info["pri"] = pri

                if attempted_ram > 0.0:
                    info["last_ram"] = max(float(info.get("last_ram", 0.0) or 0.0), attempted_ram)
                if attempted_cpu > 0.0:
                    info["last_cpu"] = max(float(info.get("last_cpu", 0.0) or 0.0), attempted_cpu)

                kind = _op_kind(op)
                if pri is not None:
                    krec = _kind_rec(pri, kind)
                else:
                    krec = None

                if failed:
                    if is_oom:
                        info["oom"] = int(info.get("oom", 0) or 0) + 1
                        if attempted_ram > 0.0:
                            info["lb_ram"] = max(float(info.get("lb_ram", 0.0) or 0.0), attempted_ram)

                        # Raise pipeline floor slightly (less aggressive than before).
                        if pid is not None and attempted_ram > 0.0:
                            prev_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
                            # Small bump only; avoids blowing up pipeline-wide allocations.
                            s.pipeline_ram_floor[pid] = max(prev_floor, attempted_ram * 1.03, attempted_ram + 4.0)

                        # Next hint: faster convergence with OOM count, capped by pool max.
                        lb = float(info.get("lb_ram", 0.0) or 0.0)
                        prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                        oom_n = int(info.get("oom", 0) or 0)

                        growth = 1.65 + 0.25 * float(min(oom_n, 3))  # 1st~1.90, 2nd~2.15, 3rd~2.40+
                        next_hint = 0.0
                        if lb > 0.0:
                            next_hint = max(next_hint, lb * growth, lb + 16.0 * (2 ** min(oom_n, 4)))
                        if attempted_ram > 0.0:
                            next_hint = max(next_hint, attempted_ram * (growth - 0.10), attempted_ram + 24.0)

                        if pool_max_ram is not None and pool_max_ram > 0.0:
                            if next_hint > float(pool_max_ram):
                                next_hint = float(pool_max_ram)

                        info["hint_ram"] = max(prev_hint, next_hint)

                        # Kind-level learning: only increase from OOM signals.
                        if krec is not None and attempted_ram > 0.0:
                            krec["oom"] = int(krec.get("oom", 0) or 0) + 1
                            krec["lb_ram"] = max(float(krec.get("lb_ram", 0.0) or 0.0), attempted_ram)
                            klb = float(krec.get("lb_ram", 0.0) or 0.0)
                            prev_khint = float(krec.get("hint_ram", 0.0) or 0.0)
                            k_growth = 1.45 + 0.15 * float(min(int(krec.get("oom", 0) or 0), 3))  # gentler than per-op
                            khint = max(prev_khint, klb * k_growth, klb + 16.0)
                            if pool_max_ram is not None and pool_max_ram > 0.0:
                                khint = min(khint, float(pool_max_ram))
                            krec["hint_ram"] = khint

                        if pid is not None and info["oom"] > s.max_oom_retries_per_op:
                            s.pipeline_permanent_fail.add(pid)

                    else:
                        # Non-OOM failures: could be CPU starvation/timeouts; bump CPU hint on timeouts.
                        info["nonoom"] = int(info.get("nonoom", 0) or 0) + 1

                        if is_timeout:
                            info["timeout"] = int(info.get("timeout", 0) or 0) + 1

                            # Increase CPU hint for this op (and kind) based on attempted CPU or base.
                            if pri is not None:
                                base_cpu, _, max_cpu_cap, _ = _base_cpu_ram_for_priority(pri)
                            else:
                                base_cpu, max_cpu_cap = 2.0, 16.0

                            prev_chint = float(info.get("hint_cpu", 0.0) or 0.0)
                            seed = attempted_cpu if attempted_cpu > 0.0 else base_cpu
                            next_chint = min(max_cpu_cap, max(seed * 1.60, seed + 1.0, base_cpu * 1.40))
                            info["hint_cpu"] = max(prev_chint, next_chint)

                            if krec is not None:
                                prev_kchint = float(krec.get("hint_cpu", 0.0) or 0.0)
                                krec["timeout"] = int(krec.get("timeout", 0) or 0) + 1
                                k_next = min(max_cpu_cap, max(seed * 1.45, seed + 1.0, base_cpu * 1.25))
                                krec["hint_cpu"] = max(prev_kchint, k_next)
                        else:
                            # Mild RAM bump on other failures.
                            if attempted_ram > 0.0:
                                prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                                bump = attempted_ram * 1.15 + 8.0
                                if pool_max_ram is not None:
                                    bump = min(bump, float(pool_max_ram))
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
    avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]
    max_cpu = [float(s.executor.pools[i].max_cpu_pool) for i in range(num_pools)]
    max_ram = [float(s.executor.pools[i].max_ram_pool) for i in range(num_pools)]

    scheduled_pipeline_ids = set()

    def _highpri_ready():
        # More accurate than queue-nonempty: maintain a short-lived flag when we actually see runnable/blocked hi-pri ops.
        if s.hi_runnable_until_tick > s.tick:
            return True
        # Fall back to cheap heuristic.
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
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                continue

            if pid in scheduled_pipeline_ids:
                _activate_pipeline_id(pid, pri, delay_ticks=1)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            s.op_to_pipeline[id(op)] = pid

            # Find best pool and size.
            chosen_pool = None
            chosen_cpu = None
            chosen_ram = None
            best_score = None

            batch_age = 0
            if pri == Priority.BATCH_PIPELINE and meta is not None:
                try:
                    batch_age = int(s.tick - int(meta.get("arrival_tick", s.tick)))
                except Exception:
                    batch_age = 0

            # If we have runnable query/interactive ops but cannot place them due to headroom, hold the hi_ready flag.
            if pri != Priority.BATCH_PIPELINE:
                s.hi_runnable_until_tick = max(s.hi_runnable_until_tick, s.tick + s.hi_runnable_hold_ticks)

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
                    batch_age_ticks=batch_age,
                )
                if pool_ok is None or pool_ok != pool_id:
                    continue

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

            # Final clamp/quantize right before assignment to avoid any float-sum edge overallocations.
            chosen_cpu = _cap_pos(float(chosen_cpu), avail_cpu[chosen_pool])
            chosen_ram = _cap_pos(float(chosen_ram), avail_ram[chosen_pool])
            if chosen_cpu is None or chosen_ram is None or chosen_cpu <= 0.0 or chosen_ram <= 0.0:
                if meta is None:
                    _ensure_pipeline_meta(p)
                    meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["next_eligible_tick"] = max(meta.get("next_eligible_tick", 0), s.tick + 1)
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                return False

            if chosen_cpu > avail_cpu[chosen_pool] + 1e-12 or chosen_ram > avail_ram[chosen_pool] + 1e-12:
                if meta is None:
                    _ensure_pipeline_meta(p)
                    meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["next_eligible_tick"] = max(meta.get("next_eligible_tick", 0), s.tick + 1)
                _activate_pipeline_id(pid, pri, delay_ticks=0)
                return False

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
            if avail_cpu[chosen_pool] < 0.0:
                avail_cpu[chosen_pool] = 0.0
            if avail_ram[chosen_pool] < 0.0:
                avail_ram[chosen_pool] = 0.0
            scheduled_pipeline_ids.add(pid)

            # If there are multiple runnable ops right now (parallelism), re-activate quickly.
            if len(ops) > 1:
                _activate_pipeline_id(pid, pri, delay_ticks=1)

            # Track that we are actively running hi-priority work.
            if pri != Priority.BATCH_PIPELINE:
                s.hi_runnable_until_tick = max(s.hi_runnable_until_tick, s.tick + s.hi_runnable_hold_ticks)

            return True

        return False

    remaining = int(s.max_new_assignments_per_tick)

    def _drain(pri, budget):
        nonlocal remaining
        placed_any = False
        while budget > 0 and remaining > 0:
            if all((avail_cpu[i] <= 0.0 or avail_ram[i] <= 0.0) for i in range(num_pools)):
                return placed_any
            if not _place_one(pri):
                return placed_any
            placed_any = True
            budget -= 1
            remaining -= 1
        return placed_any

    qn = len(s.q_query)
    in_n = len(s.q_interactive)
    bn = len(s.q_batch)

    # Base split tuned for this workload mix (many interactive + batch).
    b_query = 4
    b_inter = 9
    b_batch = 3

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

    if remaining > 0 and qn > 0 and b_query == 0:
        b_query = 1
    if remaining > 0 and in_n > 0 and b_inter == 0:
        b_inter = 1
    if remaining > 0 and bn > 0 and b_batch == 0:
        b_batch = 1

    _drain(Priority.QUERY, b_query)
    _drain(Priority.INTERACTIVE, b_inter)
    _drain(Priority.BATCH_PIPELINE, b_batch)

    if remaining > 0:
        _drain(Priority.INTERACTIVE, remaining)
    if remaining > 0:
        _drain(Priority.BATCH_PIPELINE, remaining)

    return suspensions, assignments
