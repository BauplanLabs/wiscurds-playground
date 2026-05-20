@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Priority queues implemented as (list, head_index) for amortized O(1) pop-left.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.wait_h = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # De-dup and metadata
    s.enqueued = set()  # pipeline_id
    s.pid_to_pipeline = {}  # pipeline_id -> Pipeline

    # Per-pipeline metadata (lightweight, used for cooldown/backoff)
    # {pid: {"enqueued_tick": int, "cooldown_until": int}}
    s.pipeline_meta = {}

    # Per-operator memory learning via OOM feedback
    # floor: largest RAM allocation that OOM'd (must exceed next time)
    s.op_oom_floor = {}  # (pid, opk) -> ram
    s.op_ram_hint = {}   # (pid, opk) -> next ram to try

    # Failure accounting
    s.op_fail_count = {}  # (pid, opk) -> int
    s.max_op_failures = 6  # total failures (OOM or other) before giving up on that pipeline/op

    # Map operator key -> pipeline_id for attributing results
    s.op_to_pid = {}

    # Keep pipelines we decided to abandon to avoid infinite churn
    s.dead_pipelines = set()

    # Scheduling knobs (chosen for bursty CPU-bound traces: avoid giant single-op allocations)
    s.max_new_assignments_per_tick = 32
    s.scan_limit = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 24,
    }

    # Per-tick cap: limit how many ops from the same pipeline can be scheduled (to reduce hogging)
    s.per_tick_pipeline_cap = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Default request sizes (units match executor pool units; in provided scales RAM is in GB)
    s.base_cpu = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.base_ram = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # Backoff when a pipeline is checked but cannot be scheduled this tick
    s.cooldown_ticks_no_runnable = 20
    s.cooldown_ticks_no_fit = 10

    # OOM growth factor
    s.oom_growth = 2.0

    # Batch throttling: keep high-priority headroom (especially important without preemption)
    s.max_batch_assignments_per_tick = 10


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

    def _q_len(pri):
        q = s.wait_q[pri]
        h = s.wait_h[pri]
        n = len(q) - h
        return n if n > 0 else 0

    def _q_pop_left(pri):
        q = s.wait_q[pri]
        h = s.wait_h[pri]
        if h >= len(q):
            return None
        item = q[h]
        h += 1
        s.wait_h[pri] = h
        # Occasional compaction
        if h > 64 and h > (len(q) // 2):
            s.wait_q[pri] = q[h:]
            s.wait_h[pri] = 0
        return item

    def _q_append(pri, item):
        s.wait_q[pri].append(item)

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pid_to_pipeline.pop(pid, None)
        s.pipeline_meta.pop(pid, None)

    def _ensure_meta(pid):
        m = s.pipeline_meta.get(pid)
        if m is None:
            m = {"enqueued_tick": s.ticks, "cooldown_until": 0}
            s.pipeline_meta[pid] = m
        return m

    def _probe_has_pending(pri, probe=8):
        # Cheap approximation to know whether high-priority work remains.
        q = s.wait_q[pri]
        h = s.wait_h[pri]
        end = min(len(q), h + probe)
        for i in range(h, end):
            p = q[i]
            if p is None:
                continue
            pid = getattr(p, "pipeline_id", None)
            if pid is None or pid in s.dead_pipelines:
                continue
            # Checking runtime status is the only reliable way to see completion.
            st = p.runtime_status()
            if not st.is_pipeline_successful():
                return True
        return False

    def _size_request(pri, pool, pid, op, local_cpu_avail):
        # RAM: learned from OOM, otherwise start small to improve concurrency.
        opk = _op_key(op)
        base_ram = s.base_ram[pri]
        floor = s.op_oom_floor.get((pid, opk))
        hint = s.op_ram_hint.get((pid, opk))
        if hint is not None:
            ram_req = hint
        elif floor is not None:
            ram_req = max(base_ram, floor * 1.25)
        else:
            ram_req = base_ram

        # CPU: small fixed sizes; allow mild boost if pool is very empty or pipeline is old.
        base_cpu = s.base_cpu[pri]
        meta = s.pipeline_meta.get(pid)
        age = 0
        if meta is not None:
            age = s.ticks - meta.get("enqueued_tick", s.ticks)

        cpu_req = base_cpu
        # Age-based boost to reduce timeout risk on long-waiting pipelines
        if age >= 20000:
            cpu_req = min(cpu_req * 1.75, base_cpu + 8.0)
        elif age >= 8000:
            cpu_req = min(cpu_req * 1.35, base_cpu + 4.0)

        # If the pool is very empty, boost high priority a bit to reduce tail latency.
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            if pool.max_cpu_pool > 0 and (local_cpu_avail / pool.max_cpu_pool) >= 0.70:
                cpu_req = min(cpu_req + (4.0 if pri == Priority.QUERY else 2.0), base_cpu + 6.0)

        # Clamp
        if cpu_req < 1.0:
            cpu_req = 1.0
        if ram_req < 1.0:
            ram_req = 1.0
        if cpu_req > pool.max_cpu_pool:
            cpu_req = float(pool.max_cpu_pool)
        if ram_req > pool.max_ram_pool:
            ram_req = float(pool.max_ram_pool)

        return cpu_req, ram_req

    # --- Process results: update OOM hints and failure counts ---
    touched_pids = set()
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pid.get(opk)
            if pid is None:
                continue
            touched_pids.add(pid)

            if hasattr(r, "failed") and r.failed():
                key = (pid, opk)
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                # If too many failures, abandon this pipeline to prevent runaway retries
                if s.op_fail_count[key] >= s.max_op_failures:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)
                    continue

                if _is_oom_error(getattr(r, "error", None)):
                    pool_id = getattr(r, "pool_id", 0)
                    if pool_id is None or pool_id >= s.executor.num_pools:
                        pool_id = 0
                    pool = s.executor.pools[pool_id]

                    alloc = getattr(r, "ram", None)
                    if alloc is None or alloc <= 0:
                        alloc = s.op_ram_hint.get(key, s.base_ram.get(r.priority, 8.0))

                    prev_floor = s.op_oom_floor.get(key, 0.0)
                    floor = alloc if alloc > prev_floor else prev_floor
                    s.op_oom_floor[key] = floor

                    # Next attempt: jump upward aggressively but stay within pool max
                    next_ram = max(s.base_ram.get(r.priority, 8.0), floor * s.oom_growth)
                    if next_ram > pool.max_ram_pool:
                        next_ram = float(pool.max_ram_pool)

                    # If we already hit max and still OOM, abandon
                    if next_ram >= pool.max_ram_pool and floor >= pool.max_ram_pool:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

                    s.op_ram_hint[key] = next_ram

    # Opportunistically drop pipelines that completed as a consequence of recent results
    for pid in list(touched_pids):
        if pid in s.dead_pipelines:
            continue
        p = s.pid_to_pipeline.get(pid)
        if p is None:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _drop_pipeline(pid)

    # --- Ingest new pipelines (assumed to be new arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            # Keep freshest object reference (safe if simulator reuses objects; no-op otherwise)
            s.pid_to_pipeline[pid] = p
            continue
        s.enqueued.add(pid)
        s.pid_to_pipeline[pid] = p
        _ensure_meta(pid)
        _q_append(p.priority, p)

    # Quick exit
    if not results and not pipelines and _q_len(Priority.QUERY) == 0 and _q_len(Priority.INTERACTIVE) == 0 and _q_len(Priority.BATCH_PIPELINE) == 0:
        return [], []

    # --- Scheduling ---
    suspensions = []
    assignments = []

    # Local resource accounting (must manage ourselves within tick)
    local_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    scheduled_per_pid = {}  # pid -> count this tick
    batch_assigned = 0

    hp_present = _probe_has_pending(Priority.QUERY, probe=6) or _probe_has_pending(Priority.INTERACTIVE, probe=10)

    def _eligible_pipeline_slot(pid, pri):
        cap = s.per_tick_pipeline_cap.get(pri, 1)
        return scheduled_per_pid.get(pid, 0) < cap

    def _mark_scheduled(pid):
        scheduled_per_pid[pid] = scheduled_per_pid.get(pid, 0) + 1

    def _pick_candidate(pri):
        n = _q_len(pri)
        if n <= 0:
            return None

        scans = s.scan_limit.get(pri, 16)
        if scans > n:
            scans = n

        for _ in range(scans):
            p = _q_pop_left(pri)
            if p is None:
                break
            pid = p.pipeline_id

            if pid in s.dead_pipelines:
                # Drop silently
                _drop_pipeline(pid)
                continue

            # Always re-append unless we drop the pipeline
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            meta = _ensure_meta(pid)
            if s.ticks < meta.get("cooldown_until", 0):
                _q_append(pri, p)
                continue

            if not _eligible_pipeline_slot(pid, pri):
                _q_append(pri, p)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                meta["cooldown_until"] = max(meta.get("cooldown_until", 0), s.ticks + s.cooldown_ticks_no_runnable)
                _q_append(pri, p)
                continue

            op = ops[0]
            _q_append(pri, p)
            return (p, op)

        return None

    def _choose_pool_for(pri, cpu_req, ram_req):
        # Prefer pool 0 for high-priority when possible (helps isolation if multiple pools exist)
        best_pool = None
        best_score = None

        # For batch, prefer non-zero pools if they exist
        if pri == Priority.BATCH_PIPELINE and s.executor.num_pools > 1:
            pool_range = list(range(1, s.executor.num_pools)) + [0]
        else:
            pool_range = list(range(s.executor.num_pools))

        for pool_id in pool_range:
            pool = s.executor.pools[pool_id]

            if local_cpu[pool_id] < cpu_req or local_ram[pool_id] < ram_req:
                continue

            # Batch throttling/headroom: if high priority likely present, keep some slack.
            if pri == Priority.BATCH_PIPELINE:
                if hp_present:
                    # Require leaving room for at least one query+interactive op on this pool
                    reserve_cpu = min(pool.max_cpu_pool, s.base_cpu[Priority.QUERY] + s.base_cpu[Priority.INTERACTIVE])
                    reserve_ram = min(pool.max_ram_pool, s.base_ram[Priority.QUERY] + s.base_ram[Priority.INTERACTIVE])
                    if (local_cpu[pool_id] - cpu_req) < reserve_cpu:
                        continue
                    if (local_ram[pool_id] - ram_req) < reserve_ram:
                        continue

                # On multi-pool clusters, keep pool 0 biased toward latency (avoid filling it with batch)
                if s.executor.num_pools > 1 and pool_id == 0 and hp_present:
                    # Only allow batch on pool0 if it's largely idle
                    if pool.max_cpu_pool > 0 and (local_cpu[pool_id] / pool.max_cpu_pool) < 0.80:
                        continue
                    if pool.max_ram_pool > 0 and (local_ram[pool_id] / pool.max_ram_pool) < 0.80:
                        continue

            # Score: prefer more headroom; add a small bonus for pool0 for high priority.
            score = local_cpu[pool_id] + 0.001 * local_ram[pool_id]
            if pri in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                score += 5.0

            if best_score is None or score > best_score:
                best_score = score
                best_pool = pool_id

        return best_pool

    # Main loop: repeatedly try to schedule QUERY > INTERACTIVE > BATCH
    for _ in range(s.max_new_assignments_per_tick):
        pri_to_try = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        scheduled_any = False
        for pri in pri_to_try:
            if pri == Priority.BATCH_PIPELINE:
                if batch_assigned >= s.max_batch_assignments_per_tick:
                    continue

            cand = _pick_candidate(pri)
            if cand is None:
                continue

            p, op = cand
            pid = p.pipeline_id

            # Compute request sizes using the best-available pool's headroom as context
            # (we'll pick an actual pool next, but we can use max local_cpu as a proxy for boost decisions)
            max_local_cpu = 0.0
            max_pool = None
            for i in range(s.executor.num_pools):
                if local_cpu[i] > max_local_cpu:
                    max_local_cpu = local_cpu[i]
                    max_pool = s.executor.pools[i]
            if max_pool is None:
                continue

            cpu_req, ram_req = _size_request(pri, max_pool, pid, op, max_local_cpu)

            # Choose pool placement
            pool_id = _choose_pool_for(pri, cpu_req, ram_req)
            if pool_id is None:
                # Backoff pipeline a bit if it cannot fit anywhere right now
                meta = _ensure_meta(pid)
                meta["cooldown_until"] = max(meta.get("cooldown_until", 0), s.ticks + s.cooldown_ticks_no_fit)
                continue

            # Final clamp to what's available
            if cpu_req > local_cpu[pool_id]:
                cpu_req = local_cpu[pool_id]
            if ram_req > local_ram[pool_id]:
                ram_req = local_ram[pool_id]
            if cpu_req < 1.0 or ram_req < 1.0:
                meta = _ensure_meta(pid)
                meta["cooldown_until"] = max(meta.get("cooldown_until", 0), s.ticks + s.cooldown_ticks_no_fit)
                continue

            # Re-validate assignability just before assignment creation
            st = p.runtime_status()
            assignable = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not assignable:
                meta = _ensure_meta(pid)
                meta["cooldown_until"] = max(meta.get("cooldown_until", 0), s.ticks + s.cooldown_ticks_no_runnable)
                continue
            # Ensure we assign the same op we selected if it's still assignable; else take the first assignable.
            opk_selected = _op_key(op)
            op_final = None
            for o in assignable:
                if _op_key(o) == opk_selected:
                    op_final = o
                    break
            if op_final is None:
                op_final = assignable[0]

            assignments.append(
                Assignment(
                    ops=[op_final],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Attribute results back to pipeline
            s.op_to_pid[_op_key(op_final)] = pid

            # Update local resources
            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req
            _mark_scheduled(pid)

            if pri == Priority.BATCH_PIPELINE:
                batch_assigned += 1

            scheduled_any = True
            break  # restart with QUERY-first on next assignment attempt

        if not scheduled_any:
            break

    return suspensions, assignments
