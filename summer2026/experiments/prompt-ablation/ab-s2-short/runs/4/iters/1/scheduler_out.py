@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority queues (append-only; we lazy-skip inactive pipelines and periodically compact).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_ptr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Active pipelines (arrived and not yet terminal).
    s.active = set()  # pipeline_id
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}

    # Inflight tracking for fairness/caps (best-effort; decremented on results).
    s.pipeline_inflight = {}  # pipeline_id -> int
    s.pool_inflight_cpu = []  # list[dict[Priority,float]]
    s.pool_inflight_ram = []  # list[dict[Priority,float]]
    s.pool_deficit = []  # list[dict[Priority,float]]

    # Per-op tracking (for inflight decrement and signature learning).
    # op_key -> {"pid":..., "pri":..., "pool":..., "cpu":..., "ram":..., "sig":...}
    s.op_info = {}

    # Signature-based RAM learning (bounds):
    # - low: last known insufficient allocation (OOM at this RAM)
    # - high: last known sufficient allocation (success at this RAM)
    s.sig_ram_low = {}          # sig -> float
    s.sig_ram_high = {}         # sig -> float
    s.sig_success_streak = {}   # sig -> int
    s.sig_oom_count = {}        # sig -> int

    # Knobs
    s.scan_limit = {
        Priority.QUERY: 80,
        Priority.INTERACTIVE: 120,
        Priority.BATCH_PIPELINE: 160,
    }

    # Default sizing (kept intentionally small to maximize parallelism; RAM bumps handle outliers).
    s.default_cpu = {
        Priority.QUERY: 3.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.default_ram = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 12.0,
        Priority.BATCH_PIPELINE: 10.0,
    }

    # Per-pipeline inflight caps (avoid a single pipeline hogging a pool under bursts).
    s.max_inflight_per_pipeline = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Priority weights for work-conserving fairness (prevents INTERACTIVE/BATCH starvation).
    s.weight = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 7.0,
        Priority.BATCH_PIPELINE: 3.0,
    }

    # Soft caps (only enforced when there is higher-priority backlog).
    s.batch_cpu_cap_frac_when_hi_waiting = 0.55
    s.batch_ram_cap_frac_when_hi_waiting = 0.70
    s.query_cpu_cap_frac_when_interactive_waiting = 0.60

    # Headroom to keep QUERY latency bounded without relying on preemption.
    s.query_hard_reserve_cpu = 1.0
    s.query_hard_reserve_ram = 4.0
    s.query_soft_reserve_cpu = 2.0
    s.query_soft_reserve_ram = 8.0

    # Additional headroom to keep INTERACTIVE moving (applied only against BATCH).
    s.interactive_soft_reserve_cpu = 2.0
    s.interactive_soft_reserve_ram = 8.0

    # Queue compaction
    s.compact_every = 50


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

    def _safe_len(x):
        try:
            return len(x)
        except Exception:
            return 0

    def _to_float(x, default=0.0):
        try:
            v = float(x)
            if v != v:  # NaN
                return default
            return v
        except Exception:
            return default

    def _ensure_pool_state():
        n = s.executor.num_pools
        if _safe_len(s.pool_inflight_cpu) == n:
            return
        s.pool_inflight_cpu = []
        s.pool_inflight_ram = []
        s.pool_deficit = []
        for _ in range(n):
            s.pool_inflight_cpu.append({
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            })
            s.pool_inflight_ram.append({
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            })
            s.pool_deficit.append({
                Priority.QUERY: 0.0,
                Priority.INTERACTIVE: 0.0,
                Priority.BATCH_PIPELINE: 0.0,
            })

    def _op_signature(p, op):
        # Try to build a reasonably stable signature across pipelines/operators.
        # Prefer semantic identifiers; fall back to class name.
        for attr in (
            "name", "op_name", "kind", "type", "op_type", "func_name", "udf_name", "sql"
        ):
            v = getattr(op, attr, None)
            if v is None:
                continue
            try:
                sv = str(v)
            except Exception:
                continue
            if not sv:
                continue
            if attr == "sql" and len(sv) > 120:
                sv = sv[:120]
            return f"{attr}:{sv}"
        try:
            return f"class:{op.__class__.__name__}"
        except Exception:
            return "op:unknown"

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _compact_queues_if_needed():
        if s.ticks % s.compact_every != 0:
            return
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            old = s.wait_q[pri]
            if not old:
                s.rr_ptr[pri] = 0
                continue
            new = []
            for p in old:
                try:
                    pid = p.pipeline_id
                except Exception:
                    continue
                if pid in s.active:
                    new.append(p)
            s.wait_q[pri] = new
            s.rr_ptr[pri] = 0

    def _is_terminal_failed(p):
        # If a pipeline is not successful and has no incomplete ops in any state, treat as terminal.
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                return False
            incomplete = st.get_ops(
                {
                    OperatorState.PENDING,
                    OperatorState.FAILED,
                    OperatorState.ASSIGNED,
                    OperatorState.RUNNING,
                    OperatorState.SUSPENDING,
                },
                require_parents_complete=False,
            )
            return (not incomplete)
        except Exception:
            return False

    def _drop_pipeline(pid):
        s.active.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_inflight.pop(pid, None)

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.active:
            return
        s.active.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.pipeline_inflight[pid] = s.pipeline_inflight.get(pid, 0)
        s.wait_q[p.priority].append(p)

    def _learn_from_result(r, op, info):
        sig = info.get("sig")
        alloc_ram = _to_float(getattr(r, "ram", None), default=_to_float(info.get("ram", 0.0), default=0.0))
        if alloc_ram <= 0:
            alloc_ram = _to_float(info.get("ram", 0.0), default=0.0)

        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                prev_low = s.sig_ram_low.get(sig, 0.0)
                s.sig_ram_low[sig] = max(prev_low, alloc_ram)
                s.sig_oom_count[sig] = s.sig_oom_count.get(sig, 0) + 1
                s.sig_success_streak[sig] = 0

                # If we had a "high" that is no longer plausible, clear it.
                hi = s.sig_ram_high.get(sig, None)
                lo = s.sig_ram_low.get(sig, None)
                if hi is not None and lo is not None and hi <= lo * 1.02:
                    s.sig_ram_high.pop(sig, None)
            else:
                # Unknown failure: don't overreact; just reset streak so we don't try to reduce RAM.
                s.sig_success_streak[sig] = 0
        else:
            # Success: tighten high bound.
            if alloc_ram > 0:
                prev_high = s.sig_ram_high.get(sig, None)
                if prev_high is None:
                    s.sig_ram_high[sig] = alloc_ram
                else:
                    s.sig_ram_high[sig] = min(prev_high, alloc_ram)
            s.sig_success_streak[sig] = s.sig_success_streak.get(sig, 0) + 1

    def _ram_request(pri, pool, sig):
        # Pick RAM using learned bounds, otherwise defaults.
        # Goal: safe and reasonably small; converge quickly on OOM.
        default_ram = _to_float(s.default_ram.get(pri, 10.0), default=10.0)

        lo = s.sig_ram_low.get(sig, None)
        hi = s.sig_ram_high.get(sig, None)

        if hi is not None and hi > 0:
            # Safety margin.
            ram = hi * 1.05
        elif lo is not None and lo > 0:
            # Jump above last insufficient allocation.
            # More aggressive jumps for repeated OOMs.
            ooms = s.sig_oom_count.get(sig, 0)
            mult = 2.0 if ooms <= 1 else (2.4 if ooms == 2 else 2.8)
            ram = max(default_ram, lo * mult)
        else:
            ram = default_ram

        # Clamp
        if ram < 1.0:
            ram = 1.0
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        return ram

    def _cpu_request(pri, pool, ram_req, has_other_backlog, pool_avail_cpu):
        cpu = _to_float(s.default_cpu.get(pri, 1.0), default=1.0)

        # Give a bit more CPU for large-memory ops (often heavy).
        if ram_req >= 64.0:
            cpu = max(cpu, 2.0)
        if ram_req >= 128.0:
            cpu = max(cpu, 3.0)

        # If no other backlog and lots of CPU is idle, scale up slightly to improve makespan.
        if not has_other_backlog and pool_avail_cpu >= pool.max_cpu_pool * 0.50:
            cpu = min(max(cpu, 2.0), 4.0)

        if cpu < 1.0:
            cpu = 1.0
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        return cpu

    def _can_schedule_under_caps(pool_id, pri, cpu_req, ram_req, has_query_waiting, has_interactive_waiting):
        pool = s.executor.pools[pool_id]

        if pri == Priority.BATCH_PIPELINE and (has_query_waiting or has_interactive_waiting):
            cpu_cap = pool.max_cpu_pool * s.batch_cpu_cap_frac_when_hi_waiting
            ram_cap = pool.max_ram_pool * s.batch_ram_cap_frac_when_hi_waiting
            if s.pool_inflight_cpu[pool_id][Priority.BATCH_PIPELINE] + cpu_req > cpu_cap:
                return False
            if s.pool_inflight_ram[pool_id][Priority.BATCH_PIPELINE] + ram_req > ram_cap:
                return False

        if pri == Priority.QUERY and has_interactive_waiting:
            cpu_cap = pool.max_cpu_pool * s.query_cpu_cap_frac_when_interactive_waiting
            if s.pool_inflight_cpu[pool_id][Priority.QUERY] + cpu_req > cpu_cap:
                return False

        return True

    def _pick_op_from_priority(pri, pool_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram, has_query_waiting, has_interactive_waiting):
        q = s.wait_q.get(pri, [])
        n = _safe_len(q)
        if n == 0:
            return None, None, None, None, None

        start = s.rr_ptr.get(pri, 0)
        scan = min(n, int(s.scan_limit.get(pri, 100)))

        cap = int(s.max_inflight_per_pipeline.get(pri, 1))

        for i in range(scan):
            idx = (start + i) % n
            p = q[idx]
            try:
                pid = p.pipeline_id
            except Exception:
                continue

            if pid not in s.active:
                continue

            # Drop terminal pipelines early.
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
                if _is_terminal_failed(p):
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass

            # Per-pipeline inflight cap
            if s.pipeline_inflight.get(pid, 0) >= cap:
                continue

            # Runnable ops
            try:
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = []

            if not ops:
                continue

            op = ops[0]
            sig = _op_signature(p, op)

            ram_req = _ram_request(pri, s.executor.pools[pool_id], sig)

            # Reserve checks (for headroom without preemption).
            # - QUERY: no reserve enforcement
            # - INTERACTIVE: keep query headroom
            # - BATCH: keep query + interactive headroom
            if pri != Priority.QUERY:
                if (avail_ram - ram_req) < reserve_ram:
                    continue

            # CPU request may be trimmed down to fit availability (safe; only affects runtime).
            has_other_backlog = False
            if pri == Priority.QUERY:
                has_other_backlog = (_safe_len(s.wait_q[Priority.INTERACTIVE]) > 0) or (_safe_len(s.wait_q[Priority.BATCH_PIPELINE]) > 0)
            elif pri == Priority.INTERACTIVE:
                has_other_backlog = (_safe_len(s.wait_q[Priority.QUERY]) > 0) or (_safe_len(s.wait_q[Priority.BATCH_PIPELINE]) > 0)
            else:
                has_other_backlog = (_safe_len(s.wait_q[Priority.QUERY]) > 0) or (_safe_len(s.wait_q[Priority.INTERACTIVE]) > 0)

            cpu_req = _cpu_request(pri, s.executor.pools[pool_id], ram_req, has_other_backlog, avail_cpu)
            if cpu_req > avail_cpu:
                cpu_req = avail_cpu
            if cpu_req < 1.0:
                continue

            if pri != Priority.QUERY:
                if (avail_cpu - cpu_req) < reserve_cpu:
                    continue

            # Soft caps by priority to avoid starvation.
            if not _can_schedule_under_caps(pool_id, pri, cpu_req, ram_req, has_query_waiting, has_interactive_waiting):
                continue

            # Advance RR pointer past this index for next time.
            s.rr_ptr[pri] = (idx + 1) % max(1, n)
            return p, op, cpu_req, ram_req, sig

        # If we scanned without finding anything, still advance RR pointer a bit to prevent lockstep.
        if n > 0:
            s.rr_ptr[pri] = (start + scan) % n
        return None, None, None, None, None

    _ensure_pool_state()

    # Ingest new pipelines
    for p in pipelines:
        try:
            _enqueue_pipeline(p)
        except Exception:
            continue

    # Process results: decrement inflight and learn RAM needs
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            info = s.op_info.pop(opk, None)
            if info is None:
                continue

            pid = info.get("pid")
            pri = info.get("pri")
            pool_id = info.get("pool")

            cpu = _to_float(info.get("cpu", 0.0), default=0.0)
            ram = _to_float(info.get("ram", 0.0), default=0.0)

            if 0 <= int(pool_id) < _safe_len(s.pool_inflight_cpu):
                s.pool_inflight_cpu[pool_id][pri] = max(0.0, s.pool_inflight_cpu[pool_id][pri] - cpu)
                s.pool_inflight_ram[pool_id][pri] = max(0.0, s.pool_inflight_ram[pool_id][pri] - ram)

            if pid is not None:
                s.pipeline_inflight[pid] = max(0, int(s.pipeline_inflight.get(pid, 0)) - 1)

            _learn_from_result(r, op, info)

    _compact_queues_if_needed()

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Scheduling
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = _to_float(pool.avail_cpu_pool, default=0.0)
        avail_ram = _to_float(pool.avail_ram_pool, default=0.0)

        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Backlog heuristics (queue length; may include blocked pipelines, but works well enough).
        has_query_waiting = _safe_len(s.wait_q[Priority.QUERY]) > 0
        has_interactive_waiting = _safe_len(s.wait_q[Priority.INTERACTIVE]) > 0
        has_hi_waiting = has_query_waiting or has_interactive_waiting

        # Reserves: protect QUERY always (small), more if QUERY backlog exists.
        query_reserve_cpu = s.query_hard_reserve_cpu + (s.query_soft_reserve_cpu if has_query_waiting else 0.0)
        query_reserve_ram = s.query_hard_reserve_ram + (s.query_soft_reserve_ram if has_query_waiting else 0.0)

        # Additional reserve to keep INTERACTIVE moving (applied only against BATCH).
        interactive_reserve_cpu = (s.interactive_soft_reserve_cpu if has_interactive_waiting else 0.0)
        interactive_reserve_ram = (s.interactive_soft_reserve_ram if has_interactive_waiting else 0.0)

        # Limit total work per step per pool (prevents pathological loops).
        max_assign = int(max(32, pool.max_cpu_pool * 4))
        did = 0

        # Small QUERY burst to keep tail latency down.
        query_burst = 2
        while query_burst > 0 and did < max_assign and avail_cpu >= 1.0 and avail_ram >= 1.0:
            p, op, cpu_req, ram_req, sig = _pick_op_from_priority(
                Priority.QUERY,
                pool_id,
                avail_cpu,
                avail_ram,
                reserve_cpu=0.0,
                reserve_ram=0.0,
                has_query_waiting=has_query_waiting,
                has_interactive_waiting=has_interactive_waiting,
            )
            if p is None:
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            opk = _op_key(op)
            s.op_info[opk] = {
                "pid": p.pipeline_id,
                "pri": p.priority,
                "pool": pool_id,
                "cpu": cpu_req,
                "ram": ram_req,
                "sig": sig,
            }
            s.pipeline_inflight[p.pipeline_id] = int(s.pipeline_inflight.get(p.pipeline_id, 0)) + 1
            s.pool_inflight_cpu[pool_id][p.priority] += _to_float(cpu_req, default=0.0)
            s.pool_inflight_ram[pool_id][p.priority] += _to_float(ram_req, default=0.0)

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            did += 1
            query_burst -= 1

        # Fair, work-conserving fill using deficits.
        # We enforce headroom via reserves against INTERACTIVE and BATCH.
        guard = 0
        while did < max_assign and avail_cpu >= 1.0 and avail_ram >= 1.0:
            guard += 1
            if guard > max_assign * 4:
                break

            # Update backlog flags (cheap heuristics).
            has_query_waiting = _safe_len(s.wait_q[Priority.QUERY]) > 0
            has_interactive_waiting = _safe_len(s.wait_q[Priority.INTERACTIVE]) > 0
            has_hi_waiting = has_query_waiting or has_interactive_waiting

            query_reserve_cpu = s.query_hard_reserve_cpu + (s.query_soft_reserve_cpu if has_query_waiting else 0.0)
            query_reserve_ram = s.query_hard_reserve_ram + (s.query_soft_reserve_ram if has_query_waiting else 0.0)

            interactive_reserve_cpu = (s.interactive_soft_reserve_cpu if has_interactive_waiting else 0.0)
            interactive_reserve_ram = (s.interactive_soft_reserve_ram if has_interactive_waiting else 0.0)

            # Add weight to deficits for priorities that have any queued pipelines.
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if _safe_len(s.wait_q[pri]) > 0:
                    s.pool_deficit[pool_id][pri] = s.pool_deficit[pool_id].get(pri, 0.0) + _to_float(s.weight.get(pri, 1.0), default=1.0)

            # Try priorities in descending deficit order; fall back to strict order on ties.
            prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            prios.sort(key=lambda pri: (s.pool_deficit[pool_id].get(pri, 0.0), 1 if pri == Priority.QUERY else (0.5 if pri == Priority.INTERACTIVE else 0.0)), reverse=True)

            scheduled_any = False
            for pri in prios:
                if _safe_len(s.wait_q[pri]) == 0:
                    continue

                if pri == Priority.QUERY:
                    reserve_cpu = 0.0
                    reserve_ram = 0.0
                elif pri == Priority.INTERACTIVE:
                    reserve_cpu = query_reserve_cpu
                    reserve_ram = query_reserve_ram
                else:
                    reserve_cpu = query_reserve_cpu + interactive_reserve_cpu
                    reserve_ram = query_reserve_ram + interactive_reserve_ram

                p, op, cpu_req, ram_req, sig = _pick_op_from_priority(
                    pri,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_cpu,
                    reserve_ram=reserve_ram,
                    has_query_waiting=has_query_waiting,
                    has_interactive_waiting=has_interactive_waiting,
                )
                if p is None:
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

                opk = _op_key(op)
                s.op_info[opk] = {
                    "pid": p.pipeline_id,
                    "pri": p.priority,
                    "pool": pool_id,
                    "cpu": cpu_req,
                    "ram": ram_req,
                    "sig": sig,
                }
                s.pipeline_inflight[p.pipeline_id] = int(s.pipeline_inflight.get(p.pipeline_id, 0)) + 1
                s.pool_inflight_cpu[pool_id][p.priority] += _to_float(cpu_req, default=0.0)
                s.pool_inflight_ram[pool_id][p.priority] += _to_float(ram_req, default=0.0)

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                did += 1

                # Charge deficit by a cost proxy (roughly proportional to CPU).
                cost = max(1.0, _to_float(cpu_req, default=1.0) / 1.0)
                s.pool_deficit[pool_id][pri] = s.pool_deficit[pool_id].get(pri, 0.0) - cost

                scheduled_any = True
                break

            if not scheduled_any:
                break

    return suspensions, assignments
