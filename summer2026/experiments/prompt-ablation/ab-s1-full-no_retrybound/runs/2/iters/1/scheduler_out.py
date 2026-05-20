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

    # Delayed activation wheel: tick -> list[(pipeline_id, priority)]
    s.delay_wheel = {}

    # Keep pipeline objects for runtime_status() lookups.
    s.pipelines_by_id = {}

    # Pipeline metadata and failure tracking.
    s.pipeline_meta = {}  # pipeline_id -> dict(arrival_tick, next_eligible_tick)
    s.pipeline_permanent_fail = set()

    # Memory discovery via OOM feedback (per-operator, per-pipeline floors).
    s.op_info = {}  # id(op) -> dict(oom, nonoom, lb_ram, hint_ram, last_ram, pri, sig)
    s.pipeline_ram_floor = {}  # pipeline_id -> ram_floor (raised after OOMs)

    # Cross-pipeline learning: per-(priority, op_signature) floors/hints.
    s.sig_info = {
        Priority.QUERY: {},
        Priority.INTERACTIVE: {},
        Priority.BATCH_PIPELINE: {},
    }  # pri -> sig -> dict(oom, nonoom, lb_ram, hint_ram, ok_ram, last_ram)

    # Map operator object identity to its pipeline_id (learned when we schedule it).
    s.op_to_pipeline = {}

    # Retry bounds (must be finite).
    s.max_oom_retries_per_op = 7
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

    # Batch admission reserve (avoid locking out high priority without preemption).
    # Keep a modest absolute reserve and a small fractional reserve; take the larger, then cap it.
    s.batch_reserve_cpu_abs = 10.0
    s.batch_reserve_cpu_frac = 0.02
    s.batch_reserve_cpu_cap = 48.0

    s.batch_reserve_ram_abs = 48.0
    s.batch_reserve_ram_frac = 0.02
    s.batch_reserve_ram_cap = 512.0

    # Backoff (in ticks) for pipelines we couldn't place due to headroom.
    s.place_fail_backoff_ticks = 5


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

    def _schedule_delayed(pid, pri, target_tick):
        if pid is None:
            return
        if pid in s.pipeline_permanent_fail:
            return
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return
        _ensure_pipeline_meta(p)
        meta = s.pipeline_meta.get(pid)
        if meta is None:
            return
        prev = int(meta.get("next_eligible_tick", 0) or 0)
        if target_tick > prev:
            meta["next_eligible_tick"] = int(target_tick)
        target = int(meta.get("next_eligible_tick", 0) or 0)
        if target <= s.tick:
            # eligible now; enqueue immediately
            q, in_q = _q_structs_for_priority(pri)
            if pid not in in_q:
                q.append(pid)
                in_q.add(pid)
            return
        s.delay_wheel.setdefault(target, []).append((pid, pri))

    def _activate_pipeline_id(pid, pri, delay_ticks=0):
        if pid is None:
            return
        if pid in s.pipeline_permanent_fail:
            return
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return
        _ensure_pipeline_meta(p)
        meta = s.pipeline_meta.get(pid)
        if meta is None:
            return

        if delay_ticks and delay_ticks > 0:
            _schedule_delayed(pid, pri, s.tick + int(delay_ticks))
            return

        # Immediate activation: clear any backoff.
        meta["next_eligible_tick"] = 0
        q, in_q = _q_structs_for_priority(pri)
        if pid not in in_q:
            q.append(pid)
            in_q.add(pid)

    def _activate_pipeline(p, delay_ticks=0):
        s.pipelines_by_id[p.pipeline_id] = p
        _ensure_pipeline_meta(p)
        _activate_pipeline_id(p.pipeline_id, p.priority, delay_ticks=delay_ticks)

    def _base_cpu_ram_for_priority(pri):
        if pri == Priority.QUERY:
            return s.base_cpu_query, s.base_ram_query, s.max_cpu_query, s.soft_cap_ram_query
        if pri == Priority.INTERACTIVE:
            return s.base_cpu_interactive, s.base_ram_interactive, s.max_cpu_interactive, s.soft_cap_ram_interactive
        return s.base_cpu_batch, s.base_ram_batch, s.max_cpu_batch, s.soft_cap_ram_batch

    def _op_signature(pri, op):
        # Conservative but stable-ish signature; keep cheap.
        try:
            tname = type(op).__name__
        except Exception:
            tname = "op"

        tag = None
        val = None
        for attr in ("name", "op_name", "func_name", "query_name", "path", "sql", "query"):
            try:
                v = getattr(op, attr, None)
            except Exception:
                v = None
            if v:
                tag = attr
                try:
                    if isinstance(v, str):
                        vv = v
                    else:
                        vv = str(v)
                except Exception:
                    vv = None
                if vv:
                    vv = vv.strip()
                    if len(vv) > 64:
                        vv = vv[:64]
                    val = (vv, len(str(v)))
                break

        # Separate across priority to avoid cross-class contamination.
        return (pri, tname, tag, val)

    def _sig_bucket(pri, sig):
        d = s.sig_info.get(pri)
        if d is None:
            d = {}
            s.sig_info[pri] = d
        info = d.get(sig)
        if info is None:
            info = {"oom": 0, "nonoom": 0, "lb_ram": 0.0, "hint_ram": 0.0, "ok_ram": 0.0, "last_ram": 0.0}
            d[sig] = info
        return info

    def _pool_ok_and_score(pri, pool_id, cpu_req, ram_req, avail_cpu, avail_ram, max_cpu, max_ram, highpri_ready):
        if cpu_req > avail_cpu[pool_id] or ram_req > avail_ram[pool_id]:
            return None

        if pri == Priority.BATCH_PIPELINE:
            reserve_cpu = s.batch_reserve_cpu_abs
            reserve_cpu = max(reserve_cpu, s.batch_reserve_cpu_frac * max_cpu[pool_id])
            reserve_cpu = min(reserve_cpu, s.batch_reserve_cpu_cap)

            reserve_ram = s.batch_reserve_ram_abs
            reserve_ram = max(reserve_ram, s.batch_reserve_ram_frac * max_ram[pool_id])
            reserve_ram = min(reserve_ram, s.batch_reserve_ram_cap)

            if highpri_ready:
                reserve_cpu = min(s.batch_reserve_cpu_cap, reserve_cpu * 1.20)
                reserve_ram = min(s.batch_reserve_ram_cap, reserve_ram * 1.20)

            if (avail_cpu[pool_id] - cpu_req) < reserve_cpu or (avail_ram[pool_id] - ram_req) < reserve_ram:
                return None

        cpu_left = (avail_cpu[pool_id] - cpu_req) / (max_cpu[pool_id] + 1e-9)
        ram_left = (avail_ram[pool_id] - ram_req) / (max_ram[pool_id] + 1e-9)
        return cpu_left + ram_left

    def _compute_req(pri, pid, op, avail_cpu_i, avail_ram_i, max_cpu_i, max_ram_i, highpri_ready):
        base_cpu, base_ram, max_cpu_cap, soft_ram_cap = _base_cpu_ram_for_priority(pri)
        op_id = id(op)

        info = s.op_info.get(op_id)
        if info is None:
            info = {"oom": 0, "nonoom": 0, "lb_ram": 0.0, "hint_ram": 0.0, "last_ram": 0.0, "pri": pri, "sig": None}
            s.op_info[op_id] = info
        else:
            info["pri"] = pri

        sig = info.get("sig")
        if sig is None:
            sig = _op_signature(pri, op)
            info["sig"] = sig
        sigi = _sig_bucket(pri, sig)

        # Adaptive base RAM: be a bit more generous on larger pools (reduces OOM churn).
        ram_boost = 1.0
        if max_ram_i >= 1500.0:
            ram_boost = 1.12
        if max_ram_i >= 3000.0:
            ram_boost = 1.25
        if max_ram_i >= 6000.0:
            ram_boost = 1.35
        base_ram_eff = base_ram * ram_boost
        soft_ram_cap_eff = min(max_ram_i, soft_ram_cap * ram_boost)

        # Combine per-op and per-signature OOM learning.
        pipeline_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)

        op_lb = float(info.get("lb_ram", 0.0) or 0.0)
        op_hint = float(info.get("hint_ram", 0.0) or 0.0)

        sig_lb = float(sigi.get("lb_ram", 0.0) or 0.0)
        sig_hint = float(sigi.get("hint_ram", 0.0) or 0.0)
        sig_ok = float(sigi.get("ok_ram", 0.0) or 0.0)

        lb = max(op_lb, sig_lb)
        hint = max(op_hint, sig_hint)

        # If we have a known "ok" size for this signature and no OOM signals, keep pipeline floor from exploding.
        if (lb <= 0.0) and (hint <= 0.0) and sig_ok > 0.0 and pipeline_floor > 0.0:
            pipeline_floor = min(pipeline_floor, sig_ok * 1.25)

        # Cap pipeline floor by priority soft cap (prevents one huge op from bloating whole pipeline).
        if pipeline_floor > 0.0:
            pipeline_floor = min(pipeline_floor, soft_ram_cap_eff)

        ram_target = max(base_ram_eff, pipeline_floor, hint)
        if lb > 0.0:
            ram_target = max(ram_target, lb * 1.35, lb + 12.0)

        # Prevent wasting enormous RAM on first-try ops (unless OOM history says we need it).
        if lb <= 0.0 and hint <= 0.0 and ram_target > soft_ram_cap_eff:
            ram_target = soft_ram_cap_eff

        ram_target = min(ram_target, max_ram_i)

        # If retrying after OOM (lb/hint), require enough RAM to make progress.
        min_required_ram = base_ram_eff
        if lb > 0.0:
            min_required_ram = max(min_required_ram, lb * 1.02)
        if hint > 0.0:
            min_required_ram = max(min_required_ram, hint)

        if avail_ram_i + 1e-9 < min_required_ram:
            return None, None

        ram_req = min(ram_target, avail_ram_i)
        # Trim tiny float noise to reduce risk of simulator-side overalloc checks.
        ram_req = float(int(ram_req * 1000000.0)) / 1000000.0
        if ram_req + 1e-9 < min_required_ram:
            return None, None

        # CPU target:
        cpu_target = base_cpu
        cpu_free_frac = avail_cpu_i / (max_cpu_i + 1e-9)

        if pri != Priority.BATCH_PIPELINE:
            if cpu_free_frac > 0.75:
                cpu_target = min(max_cpu_cap, base_cpu * 1.8)
            elif cpu_free_frac > 0.50:
                cpu_target = min(max_cpu_cap, base_cpu * 1.4)
        else:
            if (not highpri_ready) and cpu_free_frac > 0.70:
                cpu_target = min(max_cpu_cap, base_cpu * 1.8)
            elif (not highpri_ready) and cpu_free_frac > 0.50:
                cpu_target = min(max_cpu_cap, base_cpu * 1.4)

        cpu_target = min(cpu_target, max_cpu_i)
        cpu_req = min(cpu_target, avail_cpu_i)
        cpu_req = float(int(cpu_req * 1000000.0)) / 1000000.0

        if cpu_req <= 0.0 or ram_req <= 0.0:
            return None, None
        return cpu_req, ram_req

    # 0) Process delayed activations due on this tick.
    due = s.delay_wheel.pop(s.tick, None)
    if due:
        for pid, pri in due:
            if pid in s.pipeline_permanent_fail:
                continue
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            meta = s.pipeline_meta.get(pid)
            if meta is not None and int(meta.get("next_eligible_tick", 0) or 0) > s.tick:
                continue
            _activate_pipeline_id(pid, pri, delay_ticks=0)

    # 1) Register new pipelines (arrivals) and activate them.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        _ensure_pipeline_meta(p)
        _activate_pipeline(p)

    # 2) Learn from results and re-activate pipelines whose state may have changed.
    for r in results:
        pid = getattr(r, "pipeline_id", None)

        if pid is not None and getattr(r, "ops", None):
            for op in r.ops:
                s.op_to_pipeline[id(op)] = pid

        if pid is None and getattr(r, "ops", None):
            for op in r.ops:
                pid = s.op_to_pipeline.get(id(op))
                if pid is not None:
                    break

        pri = getattr(r, "priority", None)
        p_obj = s.pipelines_by_id.get(pid) if pid is not None else None
        if pri is None and p_obj is not None:
            pri = p_obj.priority
        if pri is None:
            pri = Priority.BATCH_PIPELINE

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if getattr(r, "ops", None):
            is_oom = failed and _is_oom_error(getattr(r, "error", None))
            pool_id = getattr(r, "pool_id", None)
            pool_max_ram = None
            if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                pool_max_ram = float(s.executor.pools[pool_id].max_ram_pool)

            attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if attempted_ram < 0.0:
                attempted_ram = 0.0

            for op in r.ops:
                op_id = id(op)
                info = s.op_info.get(op_id)
                if info is None:
                    info = {"oom": 0, "nonoom": 0, "lb_ram": 0.0, "hint_ram": 0.0, "last_ram": 0.0, "pri": pri, "sig": None}
                    s.op_info[op_id] = info
                info["pri"] = pri
                info["last_ram"] = max(float(info.get("last_ram", 0.0) or 0.0), attempted_ram)

                sig = info.get("sig")
                if sig is None:
                    sig = _op_signature(pri, op)
                    info["sig"] = sig
                sigi = _sig_bucket(pri, sig)
                sigi["last_ram"] = max(float(sigi.get("last_ram", 0.0) or 0.0), attempted_ram)

                if not failed:
                    # Track smallest known successful size (best-effort).
                    if attempted_ram > 0.0:
                        prev_ok = float(sigi.get("ok_ram", 0.0) or 0.0)
                        if prev_ok <= 0.0:
                            sigi["ok_ram"] = attempted_ram
                        else:
                            sigi["ok_ram"] = min(prev_ok, attempted_ram)
                    continue

                if is_oom:
                    info["oom"] = int(info.get("oom", 0) or 0) + 1
                    sigi["oom"] = int(sigi.get("oom", 0) or 0) + 1

                    if attempted_ram > 0.0:
                        info["lb_ram"] = max(float(info.get("lb_ram", 0.0) or 0.0), attempted_ram)
                        sigi["lb_ram"] = max(float(sigi.get("lb_ram", 0.0) or 0.0), attempted_ram)

                    # Raise pipeline floor a bit but cap it (avoid bloating later ops).
                    if pid is not None and attempted_ram > 0.0 and p_obj is not None:
                        _, _, _, soft_cap = _base_cpu_ram_for_priority(p_obj.priority)
                        prev_floor = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
                        new_floor = attempted_ram * 1.05
                        new_floor = min(new_floor, float(soft_cap))
                        s.pipeline_ram_floor[pid] = max(prev_floor, new_floor)

                    # Next hint: faster growth after repeated OOMs (reduces churn).
                    oom_n = int(info.get("oom", 0) or 0)
                    growth = 1.90 + 0.15 * min(oom_n, 3)  # up to 2.35
                    prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                    next_hint = attempted_ram * growth if attempted_ram > 0.0 else prev_hint * growth
                    lbv = float(info.get("lb_ram", 0.0) or 0.0)
                    next_hint = max(next_hint, lbv * 1.40, lbv + 16.0)

                    if pool_max_ram is not None and pool_max_ram > 0.0:
                        next_hint = min(next_hint, pool_max_ram)

                    info["hint_ram"] = max(prev_hint, next_hint)

                    # Signature hint too.
                    prev_sh = float(sigi.get("hint_ram", 0.0) or 0.0)
                    sigi["hint_ram"] = max(prev_sh, next_hint)

                    # If we already tried (near) max RAM and still OOM, stop.
                    if pid is not None and pool_max_ram is not None and pool_max_ram > 0.0 and attempted_ram >= 0.98 * pool_max_ram:
                        s.pipeline_permanent_fail.add(pid)

                    # If too many OOMs, fail the pipeline to avoid infinite retries.
                    if pid is not None and int(info.get("oom", 0) or 0) > s.max_oom_retries_per_op:
                        s.pipeline_permanent_fail.add(pid)
                else:
                    info["nonoom"] = int(info.get("nonoom", 0) or 0) + 1
                    sigi["nonoom"] = int(sigi.get("nonoom", 0) or 0) + 1

                    prev_hint = float(info.get("hint_ram", 0.0) or 0.0)
                    if attempted_ram > 0.0:
                        bump = attempted_ram * 1.15
                        if pool_max_ram is not None and pool_max_ram > 0.0:
                            bump = min(bump, pool_max_ram)
                        info["hint_ram"] = max(prev_hint, bump)
                        sigi["hint_ram"] = max(float(sigi.get("hint_ram", 0.0) or 0.0), bump)

                    if pid is not None and int(info.get("nonoom", 0) or 0) > s.max_nonoom_retries_per_op:
                        s.pipeline_permanent_fail.add(pid)

        if pid is not None and pid not in s.pipeline_permanent_fail:
            if p_obj is not None:
                _activate_pipeline_id(pid, p_obj.priority, delay_ticks=0)

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    snap_avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    snap_avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]
    max_cpu = [float(s.executor.pools[i].max_cpu_pool) for i in range(num_pools)]
    max_ram = [float(s.executor.pools[i].max_ram_pool) for i in range(num_pools)]

    # Local mutable headroom tracking.
    avail_cpu = list(snap_avail_cpu)
    avail_ram = list(snap_avail_ram)

    scheduled_pipeline_ids = set()

    def _highpri_ready():
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
            if meta is not None:
                ne = int(meta.get("next_eligible_tick", 0) or 0)
                if ne > s.tick:
                    _activate_pipeline_id(pid, pri, delay_ticks=(ne - s.tick))
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
                    avail_cpu_i=avail_cpu[pool_id],
                    avail_ram_i=avail_ram[pool_id],
                    max_cpu_i=max_cpu[pool_id],
                    max_ram_i=max_ram[pool_id],
                    highpri_ready=hi_ready,
                )
                if cpu_req is None or ram_req is None:
                    continue

                score = _pool_ok_and_score(
                    pri=pri,
                    pool_id=pool_id,
                    cpu_req=cpu_req,
                    ram_req=ram_req,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    max_cpu=max_cpu,
                    max_ram=max_ram,
                    highpri_ready=hi_ready,
                )
                if score is None:
                    continue

                if best_score is None or score < best_score:
                    best_score = score
                    chosen_pool = pool_id
                    chosen_cpu = cpu_req
                    chosen_ram = ram_req

            if chosen_pool is None:
                # Couldn't place now; requeue with backoff to avoid hot-spinning.
                _activate_pipeline_id(pid, pri, delay_ticks=s.place_fail_backoff_ticks)
                return False

            # Hard local accounting guard.
            if chosen_cpu > avail_cpu[chosen_pool] + 1e-9 or chosen_ram > avail_ram[chosen_pool] + 1e-9:
                _activate_pipeline_id(pid, pri, delay_ticks=s.place_fail_backoff_ticks)
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
            scheduled_pipeline_ids.add(pid)

            if len(ops) > 1:
                _activate_pipeline_id(pid, pri, delay_ticks=1)

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

    # Slightly more interactive-forward split (helps this workload on small clusters).
    b_query = 3
    b_inter = 10
    b_batch = 3

    if qn == 0:
        b_inter += 2
        b_batch += 1
        b_query = 0

    if in_n == 0:
        b_batch += 3
        if b_query > 0:
            b_query += 1
        b_inter = 0

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

    # Final safety filter: ensure we never exceed per-pool snapshot headroom (prevents scale aborts).
    if assignments and num_pools > 0:
        used_cpu = [0.0] * num_pools
        used_ram = [0.0] * num_pools
        filtered = []
        for a in assignments:
            pid = int(getattr(a, "pool_id", 0) or 0)
            if pid < 0 or pid >= num_pools:
                continue
            cpu = float(getattr(a, "cpu", 0.0) or 0.0)
            ram = float(getattr(a, "ram", 0.0) or 0.0)
            if cpu <= 0.0 or ram <= 0.0:
                continue
            if used_cpu[pid] + cpu <= snap_avail_cpu[pid] + 1e-9 and used_ram[pid] + ram <= snap_avail_ram[pid] + 1e-9:
                filtered.append(a)
                used_cpu[pid] += cpu
                used_ram[pid] += ram
        assignments = filtered

    return suspensions, assignments
