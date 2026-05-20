@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Ticks / time
    s.ticks = 0

    # Pipeline registry
    s.known_pipelines = set()          # pipeline_id
    s.pipelines_by_id = {}             # pipeline_id -> Pipeline
    s.pipeline_meta = {}               # pipeline_id -> dict

    # Ready queues (pipeline_id), per priority, with O(1) pop-left via head index
    s.ready_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.ready_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.ready_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Inflight tracking: pipeline_id -> count of assigned/running ops (we schedule at most 1, but be safe)
    s.inflight_count = {}

    # Operator attribution and retry state (keyed by operator object identity)
    s.op_to_pipeline = {}              # op_key -> pipeline_id
    s.op_ram_hint = {}                 # (pipeline_id, op_key) -> next ram to try
    s.op_retries = {}                  # (pipeline_id, op_key) -> retry count

    # Dead pipelines (stop retrying/scheduling)
    s.dead_pipelines = set()

    # Sizing knobs (CPU in vCPUs, RAM in GB units as used by the simulator)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.min_cpu = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 2,
    }
    s.max_cpu = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 8,
    }

    s.base_ram = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 10,
        Priority.BATCH_PIPELINE: 10,
    }
    s.min_ram = 2

    # OOM retry policy
    s.max_retries_per_op = 5
    s.ram_growth = 1.8  # multiplicative bump on OOM

    # Non-query scheduling pattern: 2x interactive, then 1x batch (when queries are empty)
    s.nonquery_phase = 0  # cycles 0,1,2

    # Starvation protection (in ticks)
    s.batch_starve_ticks = 250
    s.interactive_starve_ticks = 200

    # Pool preference (computed at runtime, depends on num_pools)
    s.hi_pool_frac = 0.25  # fraction of pools treated as "latency pools" for query/interactive

    # Compact queues periodically
    s.compact_every_ticks = 50


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Use object identity to avoid collisions across pipelines.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _get_pool_limits():
        num = s.executor.num_pools
        max_ram_any = 0
        max_cpu_any = 0
        for i in range(num):
            p = s.executor.pools[i]
            if p.max_ram_pool > max_ram_any:
                max_ram_any = p.max_ram_pool
            if p.max_cpu_pool > max_cpu_any:
                max_cpu_any = p.max_cpu_pool
        return max_cpu_any, max_ram_any

    def _mark_dead(pid):
        s.dead_pipelines.add(pid)
        s.known_pipelines.discard(pid)
        s.pipelines_by_id.pop(pid, None)
        s.pipeline_meta.pop(pid, None)
        s.inflight_count.pop(pid, None)
        # Remove from ready sets (stale queue entries will be skipped on pop)
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            s.ready_set[pri].discard(pid)

    def _pipeline_obj(pid):
        return s.pipelines_by_id.get(pid)

    def _pipeline_priority(pid):
        p = _pipeline_obj(pid)
        if p is None:
            return None
        return p.priority

    def _has_inflight(pid):
        return s.inflight_count.get(pid, 0) > 0

    def _inc_inflight(pid):
        s.inflight_count[pid] = s.inflight_count.get(pid, 0) + 1

    def _dec_inflight(pid):
        c = s.inflight_count.get(pid, 0) - 1
        if c <= 0:
            s.inflight_count.pop(pid, None)
        else:
            s.inflight_count[pid] = c

    def _maybe_compact(pri):
        h = s.ready_head[pri]
        q = s.ready_q[pri]
        if h > 0 and (h > 1024 or (h > 128 and h > (len(q) // 2))):
            s.ready_q[pri] = q[h:]
            s.ready_head[pri] = 0

    def _enqueue_ready(pid, pri, reason=None):
        if pid in s.dead_pipelines:
            return
        if _has_inflight(pid):
            return
        if pid in s.ready_set[pri]:
            return
        p = _pipeline_obj(pid)
        if p is None:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _mark_dead(pid)  # no longer needed anywhere
            return
        # Only enqueue if there is at least one runnable op
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            # Keep pipeline known but don't spam ready queue
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["blocked_tick"] = s.ticks
            return
        s.ready_q[pri].append(pid)
        s.ready_set[pri].add(pid)
        meta = s.pipeline_meta.get(pid)
        if meta is not None:
            meta["last_ready_tick"] = s.ticks
            if reason is not None:
                meta["last_ready_reason"] = reason

    def _pop_ready(pri):
        q = s.ready_q[pri]
        h = s.ready_head[pri]
        while h < len(q):
            pid = q[h]
            h += 1
            if pid not in s.ready_set[pri]:
                continue
            s.ready_set[pri].discard(pid)
            s.ready_head[pri] = h
            return pid
        s.ready_head[pri] = h
        _maybe_compact(pri)
        return None

    def _peek_oldest_age(pri, max_peek=12):
        q = s.ready_q[pri]
        h = s.ready_head[pri]
        best_age = None
        seen = 0
        i = h
        while i < len(q) and seen < max_peek:
            pid = q[i]
            i += 1
            if pid not in s.ready_set[pri]:
                continue
            meta = s.pipeline_meta.get(pid)
            if not meta:
                continue
            age = s.ticks - meta.get("arrival_tick", s.ticks)
            if best_age is None or age > best_age:
                best_age = age
            seen += 1
        return best_age if best_age is not None else 0

    def _pressure_scale(total_ready, total_avail_cpu):
        # More backlog => favor concurrency (smaller per-op CPU).
        if total_avail_cpu <= 0:
            return 1.0
        pressure = float(total_ready) / float(total_avail_cpu)
        if pressure > 8.0:
            return 0.6
        if pressure > 4.0:
            return 0.75
        if pressure > 2.0:
            return 0.9
        if pressure < 0.25:
            return 1.25
        if pressure < 0.5:
            return 1.10
        return 1.0

    def _cpu_request(pri, scale, pool_max_cpu):
        base = s.base_cpu[pri]
        mn = s.min_cpu[pri]
        mx = s.max_cpu[pri]
        desired = int(round(base * scale))
        if desired < mn:
            desired = mn
        if desired > mx:
            desired = mx
        if desired > pool_max_cpu:
            desired = int(pool_max_cpu)
        if desired < 1:
            desired = 1
        return desired

    def _ram_request(pri, pid, opk, max_ram_any):
        hint = s.op_ram_hint.get((pid, opk))
        if hint is not None:
            ram = hint
        else:
            ram = s.base_ram[pri]
        if ram < s.min_ram:
            ram = s.min_ram
        if ram > max_ram_any:
            ram = max_ram_any
        return ram

    # --- Ingest new pipelines (assumed to be newly arrived since last call) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        s.pipelines_by_id[pid] = p
        if pid not in s.known_pipelines:
            s.known_pipelines.add(pid)
            s.pipeline_meta[pid] = {
                "arrival_tick": s.ticks,
                "last_ready_tick": s.ticks,
                "last_progress_tick": s.ticks,
                "last_ready_reason": "arrival",
            }
        # Enqueue if ready and not inflight
        _enqueue_ready(pid, p.priority, reason="arrival_or_update")

    # --- Process results: update inflight, OOM-driven RAM bumps, retries, and requeue ---
    max_cpu_any, max_ram_any = _get_pool_limits()

    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk)
            if pid is None:
                continue

            # Mark op finished for inflight tracking
            _dec_inflight(pid)

            if pid in s.dead_pipelines:
                continue

            failed = False
            if hasattr(r, "failed"):
                try:
                    failed = bool(r.failed())
                except Exception:
                    failed = False

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    key = (pid, opk)
                    prev_retries = s.op_retries.get(key, 0) + 1
                    s.op_retries[key] = prev_retries

                    if prev_retries > s.max_retries_per_op:
                        _mark_dead(pid)
                        continue

                    alloc_ram = getattr(r, "ram", None)
                    if alloc_ram is None or alloc_ram <= 0:
                        alloc_ram = s.op_ram_hint.get(key, s.base_ram.get(_pipeline_priority(pid), 8))
                    prev_hint = s.op_ram_hint.get(key, None)
                    bumped = int(max(alloc_ram * s.ram_growth, (prev_hint * s.ram_growth if prev_hint else 0), alloc_ram + 1))
                    if bumped < s.min_ram:
                        bumped = s.min_ram
                    if bumped > max_ram_any:
                        bumped = max_ram_any
                    s.op_ram_hint[key] = bumped
                else:
                    # Non-OOM errors: allow a small number of retries via the normal FAILED state.
                    # If it keeps failing, cap retries and kill to avoid hangs.
                    key = (pid, opk)
                    prev_retries = s.op_retries.get(key, 0) + 1
                    s.op_retries[key] = prev_retries
                    if prev_retries > max(2, s.max_retries_per_op):
                        _mark_dead(pid)
                        continue
            else:
                meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["last_progress_tick"] = s.ticks

            # Requeue pipeline if it can run more and is no longer inflight
            if not _has_inflight(pid):
                p = _pipeline_obj(pid)
                if p is None:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _mark_dead(pid)
                    continue
                _enqueue_ready(pid, p.priority, reason="result_requeue")

    # Periodic compaction to keep queue lists bounded
    if s.ticks % s.compact_every_ticks == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            _maybe_compact(pri)

    # --- If nothing is ready, return ---
    ready_q_ct = len(s.ready_set[Priority.QUERY])
    ready_i_ct = len(s.ready_set[Priority.INTERACTIVE])
    ready_b_ct = len(s.ready_set[Priority.BATCH_PIPELINE])
    total_ready = ready_q_ct + ready_i_ct + ready_b_ct
    if total_ready <= 0:
        return [], []

    # --- Local pool resources snapshot (must decrement locally) ---
    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    total_avail_cpu = 0.0
    for i in range(num_pools):
        total_avail_cpu += float(local_cpu[i])

    scale = _pressure_scale(total_ready=total_ready, total_avail_cpu=total_avail_cpu)

    # Latency pools: prefer queries/interactive there when multiple pools exist
    hi_pool_count = int(max(1, round(num_pools * s.hi_pool_frac))) if num_pools > 0 else 0
    if hi_pool_count > num_pools:
        hi_pool_count = num_pools

    def _query_waiting():
        return len(s.ready_set[Priority.QUERY]) > 0

    def _hi_waiting():
        return (len(s.ready_set[Priority.QUERY]) > 0) or (len(s.ready_set[Priority.INTERACTIVE]) > 0)

    def _reserve_for_query(pool):
        # Only keep a small reserve when queries are waiting (no preemption available).
        if not _query_waiting():
            return 0, 0
        maxc = float(pool.max_cpu_pool)
        maxr = float(pool.max_ram_pool)
        rcpu = int(max(2, min(6, round(0.05 * maxc))))
        rram = int(max(4, min(32, round(0.01 * maxr))))
        return rcpu, rram

    def _choose_pool(pri, cpu_desired, ram_req):
        # Returns (pool_id, cpu_alloc) or (None, None)
        prefer_hi = (pri == Priority.QUERY) or (pri == Priority.INTERACTIVE)
        hi_waiting = _hi_waiting()

        # Candidate pools in preferred order
        if num_pools <= 1:
            pool_order = [0] if num_pools == 1 else []
        else:
            if prefer_hi:
                pool_order = list(range(0, hi_pool_count)) + list(range(hi_pool_count, num_pools))
            else:
                pool_order = list(range(hi_pool_count, num_pools)) + list(range(0, hi_pool_count))

        best_pool = None
        best_score = None
        best_cpu = None

        for i in pool_order:
            if i < 0 or i >= num_pools:
                continue
            if local_ram[i] < ram_req or local_cpu[i] < 1:
                continue

            pool = s.executor.pools[i]
            reserve_cpu, reserve_ram = _reserve_for_query(pool)

            # Avoid letting batch consume the last sliver needed to start queries.
            if pri == Priority.BATCH_PIPELINE and reserve_cpu > 0:
                if (local_cpu[i] - 1) < reserve_cpu or (local_ram[i] - ram_req) < reserve_ram:
                    continue

            cpu_alloc = cpu_desired
            if cpu_alloc > local_cpu[i]:
                cpu_alloc = int(local_cpu[i])
            if cpu_alloc < 1:
                continue

            # Scoring: best-fit on RAM; queries/interactive prefer more free CPU; batch avoids latency pools when hi work waits
            maxr = float(pool.max_ram_pool) if pool.max_ram_pool else 1.0
            maxc = float(pool.max_cpu_pool) if pool.max_cpu_pool else 1.0

            score = 0.0
            if prefer_hi and i >= hi_pool_count:
                score += 1000.0
            if (pri == Priority.BATCH_PIPELINE) and hi_waiting and (i < hi_pool_count):
                score += 200.0

            # Best-fit RAM packing
            score += float(local_ram[i] - ram_req) / maxr

            # CPU preference
            cpu_frac = float(local_cpu[i]) / maxc
            if prefer_hi:
                score -= 0.75 * cpu_frac
            else:
                score += 0.10 * cpu_frac

            if best_score is None or score < best_score:
                best_score = score
                best_pool = i
                best_cpu = cpu_alloc

        if best_pool is None:
            return None, None
        return best_pool, best_cpu

    def _next_priority():
        # Always take queries when available.
        if len(s.ready_set[Priority.QUERY]) > 0:
            return Priority.QUERY

        # Starvation protection: if oldest batch is too old, force batch; likewise interactive.
        if len(s.ready_set[Priority.BATCH_PIPELINE]) > 0 and len(s.ready_set[Priority.INTERACTIVE]) > 0:
            b_age = _peek_oldest_age(Priority.BATCH_PIPELINE)
            i_age = _peek_oldest_age(Priority.INTERACTIVE)
            if b_age >= s.batch_starve_ticks and b_age > i_age:
                return Priority.BATCH_PIPELINE
            if i_age >= s.interactive_starve_ticks and i_age >= b_age:
                return Priority.INTERACTIVE

        # Otherwise: 2 interactive, then 1 batch (if present)
        if len(s.ready_set[Priority.INTERACTIVE]) == 0 and len(s.ready_set[Priority.BATCH_PIPELINE]) == 0:
            return None
        if len(s.ready_set[Priority.BATCH_PIPELINE]) == 0:
            return Priority.INTERACTIVE
        if len(s.ready_set[Priority.INTERACTIVE]) == 0:
            return Priority.BATCH_PIPELINE

        phase = s.nonquery_phase % 3
        if phase in (0, 1):
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    suspensions = []
    assignments = []

    # Limit per-tick assignment count to avoid pathological bursts in very large clusters.
    # Still high enough to saturate resources.
    min_cpu_floor = 1.0
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if s.min_cpu[pri] < min_cpu_floor:
            min_cpu_floor = float(s.min_cpu[pri])
    if min_cpu_floor <= 0:
        min_cpu_floor = 1.0

    max_assignments = int(max(32.0, (total_avail_cpu / min_cpu_floor) * 1.25))

    stall = 0
    placed = 0

    while placed < max_assignments and stall < 80:
        pri = _next_priority()
        if pri is None:
            break

        # Attempt order: chosen priority first, then fallbacks
        if pri == Priority.QUERY:
            attempt_pris = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        elif pri == Priority.INTERACTIVE:
            attempt_pris = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            attempt_pris = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

        made_progress = False

        for try_pri in attempt_pris:
            pid = _pop_ready(try_pri)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue
            if _has_inflight(pid):
                continue

            p = _pipeline_obj(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _mark_dead(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Not runnable right now; keep it known and try later.
                _enqueue_ready(pid, try_pri, reason="blocked_no_assignable")
                continue

            op = ops[0]
            opk = _op_key(op)

            # Size request
            ram_req = _ram_request(try_pri, pid, opk, max_ram_any)
            # CPU depends on pool max, so compute a conservative desired first and clip per chosen pool.
            # Use global max as a starting point.
            cpu_desired_global = _cpu_request(try_pri, scale, max_cpu_any)

            # Choose pool + final CPU
            pool_id, cpu_alloc = _choose_pool(try_pri, cpu_desired_global, ram_req)
            if pool_id is None or cpu_alloc is None or cpu_alloc < 1:
                # Can't fit now; requeue and try something else.
                _enqueue_ready(pid, try_pri, reason="no_fit_resources")
                continue

            # Final safety clipping to local availability
            if cpu_alloc > local_cpu[pool_id]:
                cpu_alloc = int(local_cpu[pool_id])
            if ram_req > local_ram[pool_id]:
                ram_req = int(local_ram[pool_id])

            if cpu_alloc < 1 or ram_req < 1:
                _enqueue_ready(pid, try_pri, reason="no_fit_post_clip")
                continue

            # Assign
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Bookkeeping
            local_cpu[pool_id] -= cpu_alloc
            local_ram[pool_id] -= ram_req
            s.op_to_pipeline[opk] = pid
            _inc_inflight(pid)

            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_scheduled_tick"] = s.ticks

            placed += 1
            made_progress = True
            stall = 0

            # Advance non-query phase only when we actually schedule a non-query op while no query is waiting
            if try_pri != Priority.QUERY and len(s.ready_set[Priority.QUERY]) == 0:
                s.nonquery_phase = (s.nonquery_phase + 1) % 3

            break  # proceed to next placement

        if not made_progress:
            stall += 1

        # Stop if all pools are effectively full (no CPU or no RAM)
        any_room = False
        for i in range(num_pools):
            if local_cpu[i] >= 1 and local_ram[i] >= 1:
                any_room = True
                break
        if not any_room:
            break

    return suspensions, assignments
