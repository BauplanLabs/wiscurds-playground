@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # --- time / pressure ---
    s.ticks = 0
    s.hi_pressure_until_tick = 0  # keep batch conservative until this tick
    s.hi_pressure_cooldown_ticks = 450  # ~4.5s at 100 ticks/sec

    # --- queues (store Pipeline objects; lazy cleanup via s.enqueued) ---
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
    s.q_count = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # pipeline bookkeeping
    s.enqueued = set()          # pipeline_ids currently considered "active" in queues
    s.pipeline_pri = {}         # pipeline_id -> Priority
    s.pipeline_meta = {}        # pipeline_id -> {"enqueued_tick": int}
    s.dead_pipelines = set()    # pipeline_ids permanently failed (non-OOM or too many OOM retries)

    # op->pipeline attribution for results
    s.op_to_pipeline = {}       # op_key -> pipeline_id

    # OOM-driven RAM discovery
    s.op_ram_hint = {}          # (pipeline_id, op_key) -> next_ram_to_try
    s.op_oom_retries = {}       # (pipeline_id, op_key) -> int
    s.pipeline_ram_floor = {}   # pipeline_id -> ram_floor_for_unknown_ops (raised after any OOM)

    # bounded retries / failure handling
    s.max_oom_retries = 4
    s.max_non_oom_failures = 2
    s.pipeline_non_oom_failures = {}  # pipeline_id -> count

    # sizing knobs (CPU-bound workload => keep CPU per op moderate; prefer concurrency)
    s.mem_per_cpu_frac = 0.20  # ram_guess += cpu * (pool.max_ram/pool.max_cpu) * this
    s.base_ram_gb = {
        Priority.QUERY: 14.0,
        Priority.INTERACTIVE: 12.0,
        Priority.BATCH_PIPELINE: 10.0,
    }
    s.min_cpu = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }
    s.max_cpu_cap = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 16,
        Priority.BATCH_PIPELINE: 8,
    }

    # scheduling loop caps (keep per-tick work bounded)
    s.max_scan_per_pick = 48
    s.max_new_assignments_per_pool_per_tick = 24

    # allow a small amount of intra-pipeline parallelism for high priority
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


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

    def _drop_pipeline(pipeline_id):
        if pipeline_id in s.enqueued:
            s.enqueued.discard(pipeline_id)
            pri = s.pipeline_pri.get(pipeline_id, None)
            if pri is not None and s.q_count.get(pri, 0) > 0:
                s.q_count[pri] -= 1
        s.pipeline_meta.pop(pipeline_id, None)
        s.pipeline_pri.pop(pipeline_id, None)
        s.pipeline_ram_floor.pop(pipeline_id, None)
        s.pipeline_non_oom_failures.pop(pipeline_id, None)

    def _pipeline_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _cpu_target(pool, pri, backlog_count, wait_ticks):
        # Base CPU roughly grows with sqrt(cluster), but capped to keep concurrency high.
        max_cpu_pool = getattr(pool, "max_cpu_pool", 1) or 1
        root = int(max(1, (max_cpu_pool ** 0.5)))

        if pri == Priority.QUERY:
            base = max(8, min(s.max_cpu_cap[pri], 2 * root))
            # Slightly reduce if many queries queued.
            if backlog_count >= 12:
                base = max(s.min_cpu[pri], int(base * 0.75))
        elif pri == Priority.INTERACTIVE:
            base = max(4, min(s.max_cpu_cap[pri], root))
            # If interactive backlog is large, trade per-op CPU for more concurrency.
            if backlog_count >= 160:
                base = max(s.min_cpu[pri], int(base * 0.50))
            elif backlog_count >= 64:
                base = max(s.min_cpu[pri], int(base * 0.70))
        else:
            base = max(2, min(s.max_cpu_cap[pri], max(2, root // 2)))
            # Keep batch small by default.
            if backlog_count >= 256:
                base = max(1, int(base * 0.70))

        # Age boost for interactive (and a little for query) to reduce tail timeouts.
        boost = 1.0
        if pri == Priority.INTERACTIVE:
            if wait_ticks >= 24000:
                boost = 1.60
            elif wait_ticks >= 14000:
                boost = 1.35
            elif wait_ticks >= 8000:
                boost = 1.20
        elif pri == Priority.QUERY:
            if wait_ticks >= 12000:
                boost = 1.25
            elif wait_ticks >= 6000:
                boost = 1.12

        cpu = int(max(s.min_cpu[pri], min(s.max_cpu_cap[pri], round(base * boost))))
        if cpu < 1:
            cpu = 1
        if cpu > max_cpu_pool:
            cpu = int(max_cpu_pool)
        return cpu

    def _ram_target(pool, pri, pipeline_id, op, cpu_req):
        max_ram_pool = getattr(pool, "max_ram_pool", 1.0) or 1.0

        opk = _op_key(op)
        hint = s.op_ram_hint.get((pipeline_id, opk), None)
        if hint is not None:
            # small headroom above last-known-needed size (avoid repeated near-threshold OOM)
            ram = float(hint) * 1.05
        else:
            base = float(s.base_ram_gb.get(pri, 10.0))
            mem_per_cpu = 0.0
            max_cpu_pool = getattr(pool, "max_cpu_pool", 1.0) or 1.0
            try:
                mem_per_cpu = (float(max_ram_pool) / float(max_cpu_pool)) * float(s.mem_per_cpu_frac)
            except Exception:
                mem_per_cpu = 0.0
            ram = base + (float(cpu_req) * mem_per_cpu)

            floor = s.pipeline_ram_floor.get(pipeline_id, None)
            if floor is not None and ram < floor:
                ram = float(floor)

        if ram < 1.0:
            ram = 1.0
        if ram > float(max_ram_pool):
            ram = float(max_ram_pool)
        return ram

    # --- Ingest new arrivals (pipelines is expected to be "new arrivals") ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue

        pri = p.priority
        s.wait_q[pri].append(p)
        s.rr_ptr[pri] = min(s.rr_ptr[pri], max(0, len(s.wait_q[pri]) - 1))
        s.enqueued.add(pid)
        s.pipeline_pri[pid] = pri
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.q_count[pri] += 1

        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            until = s.ticks + s.hi_pressure_cooldown_ticks
            if until > s.hi_pressure_until_tick:
                s.hi_pressure_until_tick = until

    # --- Process results (learn RAM on OOM; bound retries; drop hopeless pipelines) ---
    for r in results:
        failed = False
        try:
            failed = bool(getattr(r, "failed") and r.failed())
        except Exception:
            failed = False

        ops = getattr(r, "ops", None) or []
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.pop(opk, None)
            if pid is None:
                continue

            if pid in s.dead_pipelines:
                continue

            if failed:
                err = getattr(r, "error", None)
                if _is_oom_error(err):
                    key = (pid, opk)
                    prev_retries = s.op_oom_retries.get(key, 0)
                    new_retries = prev_retries + 1
                    s.op_oom_retries[key] = new_retries

                    if new_retries > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

                    alloc = getattr(r, "ram", None)
                    prev_hint = s.op_ram_hint.get(key, None)

                    # bump factor tuned to converge quickly without huge overshoot
                    bump = 1.70
                    base = None
                    if alloc is not None and alloc > 0:
                        base = float(alloc)
                    elif prev_hint is not None:
                        base = float(prev_hint)
                    else:
                        base = 1.0

                    new_hint = base * bump
                    if prev_hint is not None:
                        # ensure monotonic growth
                        new_hint = max(new_hint, float(prev_hint) * 1.35)

                    # cap by pool max if available; else leave to clip at assignment time
                    if pool is not None:
                        try:
                            new_hint = min(float(getattr(pool, "max_ram_pool", new_hint)), float(new_hint))
                        except Exception:
                            pass

                    if new_hint < 1.0:
                        new_hint = 1.0

                    s.op_ram_hint[key] = float(new_hint)

                    # raise pipeline-wide floor so later unknown ops don't repeat tiny guesses
                    floor = s.pipeline_ram_floor.get(pid, 0.0) or 0.0
                    s.pipeline_ram_floor[pid] = max(float(floor), float(new_hint) * 0.80)
                else:
                    cnt = s.pipeline_non_oom_failures.get(pid, 0) + 1
                    s.pipeline_non_oom_failures[pid] = cnt
                    if cnt >= s.max_non_oom_failures:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

    if not pipelines and not results and (s.q_count[Priority.QUERY] == 0 and s.q_count[Priority.INTERACTIVE] == 0 and s.q_count[Priority.BATCH_PIPELINE] == 0):
        return [], []

    suspensions = []
    assignments = []

    # local per-tick accounting (required)
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # per-tick caps / de-dupe
    scheduled_ops = set()
    scheduled_pipeline_counts = {}

    def _can_schedule_more_from_pipeline(pid, pri):
        lim = s.max_ops_per_pipeline_per_tick.get(pri, 1)
        return scheduled_pipeline_counts.get(pid, 0) < lim

    def _note_pipeline_scheduled(pid):
        scheduled_pipeline_counts[pid] = scheduled_pipeline_counts.get(pid, 0) + 1

    def _batch_keep_free(pool):
        # Keep some headroom during/after bursts of query/interactive to avoid long batch occupying all capacity.
        # Cap to avoid huge idle on very large clusters.
        keep_cpu = min(32, max(8, int(getattr(pool, "max_cpu_pool", 1) * 0.05)))
        keep_ram = min(200.0, max(20.0, float(getattr(pool, "max_ram_pool", 1.0)) * 0.03))
        return keep_cpu, keep_ram

    def _batch_allowed(pool_id):
        if s.q_count[Priority.QUERY] > 0:
            return False
        if s.q_count[Priority.INTERACTIVE] == 0:
            return True

        pool = s.executor.pools[pool_id]
        keep_cpu, keep_ram = _batch_keep_free(pool)

        # Under recent high-priority pressure, be conservative on batch.
        if s.ticks < s.hi_pressure_until_tick:
            return (local_cpu[pool_id] > keep_cpu) and (local_ram[pool_id] > keep_ram)

        # Otherwise, allow batch only if there's some slack.
        return (local_cpu[pool_id] > max(1, keep_cpu // 2)) and (local_ram[pool_id] > max(1.0, keep_ram / 2.0))

    def _pick_from_priority(pri, pool_id):
        q = s.wait_q[pri]
        n = len(q)
        if n <= 0:
            return None

        pool = s.executor.pools[pool_id]
        avail_cpu = local_cpu[pool_id]
        avail_ram = local_ram[pool_id]

        if avail_cpu < 1 or avail_ram < 1:
            return None

        start = s.rr_ptr[pri] % n
        scans = min(s.max_scan_per_pick, n)

        # backlog estimate (non-None active in queue)
        backlog = s.q_count.get(pri, 0)

        for j in range(scans):
            idx = (start + j) % n
            p = q[idx]
            s.rr_ptr[pri] = (idx + 1) % n

            if p is None:
                continue

            pid = p.pipeline_id
            if pid not in s.enqueued or pid in s.dead_pipelines:
                q[idx] = None
                continue

            # limit per-tick per-pipeline parallelism
            if not _can_schedule_more_from_pipeline(pid, pri):
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                q[idx] = None
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            opk = _op_key(op)
            if opk in scheduled_ops:
                continue

            wait_ticks = _pipeline_wait_ticks(pid)
            cpu_req = _cpu_target(pool, pri, backlog, wait_ticks)
            if cpu_req > avail_cpu:
                cpu_req = int(avail_cpu)

            if cpu_req < s.min_cpu.get(pri, 1):
                continue

            ram_req = _ram_target(pool, pri, pid, op, cpu_req)
            if ram_req > avail_ram:
                # If we have a learned hint that doesn't fit, don't lowball; wait.
                hint = s.op_ram_hint.get((pid, opk), None)
                if hint is not None and float(hint) > float(avail_ram):
                    continue
                # Otherwise, try to fit by trimming the guess a bit (still risky, keep modest).
                ram_req = float(avail_ram)
                if ram_req < 1.0:
                    continue

            # final sanity
            if cpu_req < 1 or ram_req < 1:
                continue

            return (p, op, cpu_req, ram_req)

        return None

    # --- Main scheduling: fill each pool with QUERY > INTERACTIVE > BATCH, but keep batch conservative under pressure ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        per_pool_cap = s.max_new_assignments_per_pool_per_tick

        for _ in range(per_pool_cap):
            if local_cpu[pool_id] < 1 or local_ram[pool_id] < 1:
                break

            picked = None

            # Always attempt highest priorities first.
            picked = _pick_from_priority(Priority.QUERY, pool_id)
            if picked is None:
                picked = _pick_from_priority(Priority.INTERACTIVE, pool_id)

            if picked is None:
                if _batch_allowed(pool_id):
                    picked = _pick_from_priority(Priority.BATCH_PIPELINE, pool_id)
                else:
                    break

            if picked is None:
                break

            p, op, cpu_req, ram_req = picked

            # extra batch headroom enforcement (ensure we don't consume the last slack)
            if p.priority == Priority.BATCH_PIPELINE and s.q_count[Priority.INTERACTIVE] > 0:
                keep_cpu, keep_ram = _batch_keep_free(pool)
                if s.ticks < s.hi_pressure_until_tick:
                    if (local_cpu[pool_id] - cpu_req) < keep_cpu or (local_ram[pool_id] - ram_req) < keep_ram:
                        # skip batch for now
                        break

            # ensure we don't overallocate this tick
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # bookkeeping for results
            opk = _op_key(op)
            s.op_to_pipeline[opk] = p.pipeline_id
            scheduled_ops.add(opk)
            _note_pipeline_scheduled(p.pipeline_id)

            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

    return suspensions, assignments
