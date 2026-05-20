@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Pipelines ingestion cursor (supports both "append-only list" and "new-only list" modes)
    s.pipeline_cursor = 0

    # Pipeline registry
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Per-priority round-robin queues of pipeline_ids
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_pos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.in_queue = set()

    # Terminal pipeline sets
    s.dead_pipelines = set()   # permanently failed (retry cap exceeded)
    s.done_pipelines = set()   # successfully completed

    # Cooldown to avoid re-checking blocked pipelines every tick
    s.next_check_tick = {}  # pipeline_id -> tick

    # Result attribution
    s.op_to_pipeline = {}   # op_key -> pipeline_id

    # OOM learning (per operator and per pipeline)
    s.op_last_alloc = {}    # (pipeline_id, op_key) -> last requested ram
    s.op_last_oom = {}      # (pipeline_id, op_key) -> last ram that OOM'd
    s.op_oom_floor = {}     # (pipeline_id, op_key) -> max ram that has OOM'd so far
    s.op_retry_oom = {}     # (pipeline_id, op_key) -> oom retry count

    s.pipe_last_oom = {}    # pipeline_id -> last ram that OOM'd (any op)
    s.pipe_oom_floor = {}   # pipeline_id -> max ram that has OOM'd so far (any op)

    # Non-OOM failure tracking (rare in most sims, but don't infinite-retry)
    s.op_retry_nonoom = {}  # (pipeline_id, op_key) -> non-oom failure count

    # Scheduling knobs (small containers + high concurrency; learn RAM from OOM)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 2,
    }
    s.max_cpu = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 6,
    }
    s.base_ram = {
        Priority.QUERY: 40,
        Priority.INTERACTIVE: 32,
        Priority.BATCH_PIPELINE: 24,
    }

    # Keep headroom for higher priority work (helps without preemption)
    s.reserve_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 4,
    }
    s.reserve_ram = {
        Priority.QUERY: 40,
        Priority.INTERACTIVE: 32,
    }

    # Per-tick caps / limits (performance + prevent single pipeline hogging)
    s.max_new_assignments_per_tick = 32
    s.max_candidates_per_pick = 24
    s.max_ops_considered_per_pipeline = 4

    s.per_pipeline_tick_cap = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    s.block_cooldown_ticks = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 6,
    }

    s.max_oom_retries = 6
    s.max_nonoom_retries = 2

    # Bias to ensure batch makes progress even under constant interactive load
    s.interactive_tick_soft_cap = 14  # beyond this, prefer batch (after queries)

    # Pool round-robin cursor
    s.pool_cursor = 0

    # Max RAM across pools (hard ceiling for retries)
    max_ram = 0
    try:
        for i in range(s.executor.num_pools):
            r = s.executor.pools[i].max_ram_pool
            if r > max_ram:
                max_ram = r
    except Exception:
        max_ram = 0
    s.max_ram_any_pool = max_ram


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    # --- Ingest pipelines (robust to either "append-only ever-seen" or "new-only" input) ---
    new_pipes = pipelines or []
    if new_pipes:
        if s.pipeline_cursor > len(new_pipes):
            # Likely "new-only list"; reset cursor mode
            s.pipeline_cursor = 0

        if s.pipeline_cursor == 0:
            # If this is "new-only", process all; if append-only, this processes all only once.
            ingest_slice = new_pipes if len(new_pipes) <= 512 else new_pipes[-512:]
            # The slice cap prevents a pathological full-rescan if the simulator feeds a huge list unexpectedly.
            # Append-only mode still works because we advance the cursor below.
        else:
            ingest_slice = new_pipes[s.pipeline_cursor :]

        # Advance cursor for append-only mode
        s.pipeline_cursor = len(new_pipes)

        for p in ingest_slice:
            pid = p.pipeline_id
            if pid in s.dead_pipelines or pid in s.done_pipelines:
                continue
            s.pipelines_by_id[pid] = p
            if pid not in s.in_queue:
                s.q[p.priority].append(pid)
                s.in_queue.add(pid)
                # Check immediately
                s.next_check_tick[pid] = 0

    # --- Process results (learn RAM from OOM; cap retries) ---
    for r in (results or []):
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            if failed:
                err = getattr(r, "error", None)
                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is None or alloc_ram <= 0:
                    alloc_ram = s.op_last_alloc.get((pid, opk), 0)

                if _is_oom_error(err):
                    # Update OOM floors
                    prev_floor = s.op_oom_floor.get((pid, opk), 0)
                    if alloc_ram > prev_floor:
                        s.op_oom_floor[(pid, opk)] = alloc_ram
                    s.op_last_oom[(pid, opk)] = alloc_ram

                    pf = s.pipe_oom_floor.get(pid, 0)
                    if alloc_ram > pf:
                        s.pipe_oom_floor[pid] = alloc_ram
                    s.pipe_last_oom[pid] = alloc_ram

                    # Retry budgeting
                    cnt = s.op_retry_oom.get((pid, opk), 0) + 1
                    s.op_retry_oom[(pid, opk)] = cnt
                    if cnt > s.max_oom_retries or (s.max_ram_any_pool and alloc_ram >= s.max_ram_any_pool):
                        s.dead_pipelines.add(pid)
                        s.in_queue.discard(pid)
                else:
                    # Non-OOM failure: allow a couple retries, then fail pipeline to avoid hanging
                    cnt = s.op_retry_nonoom.get((pid, opk), 0) + 1
                    s.op_retry_nonoom[(pid, opk)] = cnt
                    if cnt > s.max_nonoom_retries:
                        s.dead_pipelines.add(pid)
                        s.in_queue.discard(pid)
            else:
                # Success: cleanup per-op retry bookkeeping to bound memory growth
                s.op_retry_oom.pop((pid, opk), None)
                s.op_retry_nonoom.pop((pid, opk), None)
                s.op_last_oom.pop((pid, opk), None)
                # Keep floors/last_alloc as they can help later ops in the same pipeline

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    # Snapshot available resources and track tick-local decrements (hard constraint)
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    # Tick-local counts
    per_pid_count = {}  # pipeline_id -> assignments scheduled this tick
    pri_count = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    def _queue_nonempty(pri):
        return len(s.q[pri]) > 0

    # Headroom caps (never reserve more than 25% of a pool)
    # (applied only when placing lower-priority work)
    def _reserve_limits(pool):
        cap_cpu = int(pool.max_cpu_pool * 0.25)
        cap_ram = pool.max_ram_pool * 0.25
        if cap_cpu < 0:
            cap_cpu = 0
        if cap_ram < 0:
            cap_ram = 0
        return cap_cpu, cap_ram

    def _predict_ram(pool, pri, pid, opk):
        # Start from a fixed base (not pool-sized fractions) and jump quickly on OOM.
        req = s.base_ram[pri]

        # Per-op last OOM (fast convergence)
        last_oom = s.op_last_oom.get((pid, opk), 0)
        if last_oom > 0:
            grow = int(last_oom * 1.8) + 1
            if grow > req:
                req = grow

        # Per-op OOM floor (safety)
        floor = s.op_oom_floor.get((pid, opk), 0)
        if floor > 0:
            grow = int(floor * 1.5) + 1
            if grow > req:
                req = grow

        # Per-pipeline last OOM (helps when ops in same pipeline have similar memory)
        pl_oom = s.pipe_last_oom.get(pid, 0)
        if pl_oom > 0:
            grow = int(pl_oom * 1.25) + 1
            if grow > req:
                req = grow

        # Per-pipeline floor
        pf = s.pipe_oom_floor.get(pid, 0)
        if pf > 0:
            grow = int(pf * 1.15) + 1
            if grow > req:
                req = grow

        if req < 1:
            req = 1
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
        return req

    def _cpu_request(pool, pri, cpu_avail, ram_avail, ram_req):
        base = s.base_cpu[pri]
        mx = s.max_cpu[pri]
        if base < 1:
            base = 1
        if mx < base:
            mx = base

        cpu_req = base
        if cpu_req > cpu_avail:
            cpu_req = int(cpu_avail)

        # If we're RAM-tight but CPU-rich, spend some extra CPU to reduce runtime.
        if cpu_req < mx and cpu_avail >= base * 2 and ram_avail <= ram_req * 2:
            boosted = base * 2
            if boosted > mx:
                boosted = mx
            if boosted > cpu_avail:
                boosted = int(cpu_avail)
            if boosted > cpu_req:
                cpu_req = boosted

        if cpu_req < 1:
            cpu_req = 1
        if cpu_req > pool.max_cpu_pool:
            cpu_req = int(pool.max_cpu_pool)
        return cpu_req

    def _remove_queue_entry(pri, idx, pos_ref):
        q = s.q[pri]
        last = q[-1]
        q[idx] = last
        q.pop()
        if idx < pos_ref:
            pos_ref -= 1
        if pos_ref < 0:
            pos_ref = 0
        if q and pos_ref >= len(q):
            pos_ref = 0
        return pos_ref

    def _pick_op_for_priority(pri, pool_id):
        pool = s.executor.pools[pool_id]
        cpu_avail = local_cpu[pool_id]
        ram_avail = local_ram[pool_id]
        if cpu_avail < 1 or ram_avail < 1:
            return None

        q = s.q[pri]
        if not q:
            return None

        pos = s.q_pos[pri]
        scanned = 0

        cooldown = s.block_cooldown_ticks[pri]
        if cooldown < 1:
            cooldown = 1

        per_pipe_cap = s.per_pipeline_tick_cap[pri]
        if per_pipe_cap < 1:
            per_pipe_cap = 1

        while scanned < s.max_candidates_per_pick and q:
            if pos >= len(q):
                pos = 0
            idx = pos
            pid = q[idx]
            pos = idx + 1
            scanned += 1

            # Drop dead/done/unknown pipelines eagerly (O(1) removal)
            if pid in s.dead_pipelines or pid in s.done_pipelines or pid not in s.pipelines_by_id:
                s.in_queue.discard(pid)
                pos = _remove_queue_entry(pri, idx, pos)
                continue

            # Per-pipeline per-tick cap
            c = per_pid_count.get(pid, 0)
            if c >= per_pipe_cap:
                continue

            # Cooldown to avoid repeatedly checking blocked pipelines
            nxt = s.next_check_tick.get(pid, 0)
            if s.ticks < nxt:
                continue

            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                s.in_queue.discard(pid)
                pos = _remove_queue_entry(pri, idx, pos)
                continue

            status = p.runtime_status()

            # Completed pipelines: remove from queue
            try:
                if status.is_pipeline_successful():
                    s.done_pipelines.add(pid)
                    s.in_queue.discard(pid)
                    s.next_check_tick.pop(pid, None)
                    pos = _remove_queue_entry(pri, idx, pos)
                    continue
            except Exception:
                # If status check fails for any reason, keep it queued but back off
                s.next_check_tick[pid] = s.ticks + cooldown
                continue

            # Fetch assignable ops
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                s.next_check_tick[pid] = s.ticks + cooldown
                continue

            # Choose an op that fits now (prefer smaller predicted RAM among a few)
            best = None  # (ram_req, op)
            lim = s.max_ops_considered_per_pipeline
            if lim < 1:
                lim = 1
            for op in ops[:lim]:
                opk = _op_key(op)

                # If already exceeded retry budget, fail pipeline
                if s.op_retry_oom.get((pid, opk), 0) > s.max_oom_retries:
                    s.dead_pipelines.add(pid)
                    s.in_queue.discard(pid)
                    break

                ram_req = _predict_ram(pool, pri, pid, opk)
                if ram_req <= ram_avail and ram_req >= 1:
                    if best is None or ram_req < best[0]:
                        best = (ram_req, op)

            if pid in s.dead_pipelines:
                s.in_queue.discard(pid)
                s.next_check_tick.pop(pid, None)
                pos = _remove_queue_entry(pri, idx, pos)
                continue

            if best is None:
                # Nothing in this pipeline fits right now; back off briefly
                s.next_check_tick[pid] = s.ticks + cooldown
                continue

            ram_req, op = best
            opk = _op_key(op)
            cpu_req = _cpu_request(pool, pri, cpu_avail, ram_avail, ram_req)

            # Must fit available (do not shrink RAM below prediction; CPU can shrink but must be >=1)
            if cpu_req > cpu_avail or ram_req > ram_avail or cpu_req < 1 or ram_req < 1:
                s.next_check_tick[pid] = s.ticks + cooldown
                continue

            s.q_pos[pri] = pos % len(q) if q else 0
            return pid, op, cpu_req, ram_req

        s.q_pos[pri] = pos % len(q) if q else 0
        return None

    # --- Main placement loop (global cap; pool RR; protect headroom) ---
    for _ in range(s.max_new_assignments_per_tick):
        # Find a pool with some headroom (RR)
        pool_id = -1
        for j in range(num_pools):
            k = (s.pool_cursor + j) % num_pools
            if local_cpu[k] >= 1 and local_ram[k] >= 1:
                pool_id = k
                break
        if pool_id < 0:
            break
        s.pool_cursor = (pool_id + 1) % num_pools

        pool = s.executor.pools[pool_id]
        cap_cpu, cap_ram = _reserve_limits(pool)

        q_backlog = _queue_nonempty(Priority.QUERY)
        i_backlog = _queue_nonempty(Priority.INTERACTIVE)
        b_backlog = _queue_nonempty(Priority.BATCH_PIPELINE)

        # Compute reservation targets (cap at 25% of pool to avoid wasting too much)
        res_q_cpu = s.reserve_cpu[Priority.QUERY] if q_backlog else 0
        res_q_ram = s.reserve_ram[Priority.QUERY] if q_backlog else 0
        res_i_cpu = s.reserve_cpu[Priority.INTERACTIVE] if i_backlog else 0
        res_i_ram = s.reserve_ram[Priority.INTERACTIVE] if i_backlog else 0

        if res_q_cpu > cap_cpu:
            res_q_cpu = cap_cpu
        if res_i_cpu > cap_cpu:
            res_i_cpu = cap_cpu

        if res_q_ram > cap_ram:
            res_q_ram = cap_ram
        if res_i_ram > cap_ram:
            res_i_ram = cap_ram

        # Priority order: always try query first.
        # After some interactive placements in the tick, prefer batch to ensure completion progress.
        order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pri_count[Priority.INTERACTIVE] >= s.interactive_tick_soft_cap and b_backlog:
            order = [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

        placed = None
        for pri in order:
            if pri == Priority.QUERY:
                cand = _pick_op_for_priority(pri, pool_id)
                if cand is None:
                    continue
                pid, op, cpu_req, ram_req = cand
                placed = (pri, pid, op, cpu_req, ram_req)
                break

            if pri == Priority.INTERACTIVE:
                # Ensure we keep query headroom if queries are waiting
                cand = _pick_op_for_priority(pri, pool_id)
                if cand is None:
                    continue
                pid, op, cpu_req, ram_req = cand
                if q_backlog:
                    if (local_cpu[pool_id] - cpu_req) < res_q_cpu or (local_ram[pool_id] - ram_req) < res_q_ram:
                        continue
                placed = (pri, pid, op, cpu_req, ram_req)
                break

            # BATCH
            if pri == Priority.BATCH_PIPELINE:
                if not b_backlog:
                    continue

                cand = _pick_op_for_priority(pri, pool_id)
                if cand is None:
                    continue
                pid, op, cpu_req, ram_req = cand

                # Keep combined headroom for query + interactive if they're backed up
                res_cpu = 0
                res_ram = 0
                if q_backlog:
                    res_cpu += res_q_cpu
                    res_ram += res_q_ram
                if i_backlog:
                    res_cpu += res_i_cpu
                    res_ram += res_i_ram

                if res_cpu > cap_cpu:
                    res_cpu = cap_cpu
                if res_ram > cap_ram:
                    res_ram = cap_ram

                if (local_cpu[pool_id] - cpu_req) < res_cpu or (local_ram[pool_id] - ram_req) < res_ram:
                    continue

                placed = (pri, pid, op, cpu_req, ram_req)
                break

        if placed is None:
            # Couldn't place anything on this pool this iteration
            continue

        pri, pid, op, cpu_req, ram_req = placed

        # Final safety: ensure resources still sufficient
        if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id] or cpu_req < 1 or ram_req < 1:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )

        # Bookkeeping for learning and result attribution
        opk = _op_key(op)
        s.op_to_pipeline[opk] = pid
        s.op_last_alloc[(pid, opk)] = ram_req

        # Tick-local accounting
        local_cpu[pool_id] -= cpu_req
        local_ram[pool_id] -= ram_req
        per_pid_count[pid] = per_pid_count.get(pid, 0) + 1
        pri_count[pri] += 1

    return suspensions, assignments
