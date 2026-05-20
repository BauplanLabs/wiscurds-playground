@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Active pipelines tracked by id
    s.pipelines_by_id = {}       # pid -> Pipeline
    s.pipeline_priority = {}     # pid -> Priority
    s.pipeline_enq_tick = {}     # pid -> tick enqueued

    # Per-priority ring-queues of pipeline_ids (store ids; objects in dict above)
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.q_stale = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.q_last_compact_tick = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Membership sets (active, not yet completed/dead)
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Operator -> pipeline attribution (use object id for safety)
    s.op_to_pipeline = {}        # op_obj_id -> pid

    # OOM-driven RAM escalation (per operator attempt)
    s.op_ram_hint = {}           # (pid, op_obj_id) -> next_ram_to_try
    s.op_oom_count = {}          # (pid, op_obj_id) -> oom retries
    s.op_fail_count = {}         # (pid, op_obj_id) -> total failures (any kind)

    # Pipeline-level RAM baseline (helps subsequent ops in same pipeline)
    s.pipeline_ram_base = {}     # pid -> ram baseline (max of seen allocs on oom/success)

    # Dead pipelines (give up scheduling after bounded retries)
    s.dead_pipelines = set()

    # Starvation / fairness signals
    s.last_dispatch_tick = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Knobs
    s.max_oom_retries = 6
    s.max_fail_retries = 2

    # Queue compaction controls
    s.compact_min_ticks = 200
    s.compact_stale_frac = 0.50

    # Scheduling limits
    s.max_scan_per_pick = 14
    s.max_ops_per_pipeline_per_tick = 1


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _drop_pipeline(pid):
        p = s.pipelines_by_id.pop(pid, None)
        pri = s.pipeline_priority.pop(pid, None)
        s.pipeline_enq_tick.pop(pid, None)
        s.pipeline_ram_base.pop(pid, None)
        if pri is not None:
            s.in_queue[pri].discard(pid)
        return p

    def _maybe_compact_queue(pri):
        q = s.q[pri]
        n = len(q)
        if n == 0:
            s.q_head[pri] = 0
            s.q_stale[pri] = 0
            return

        stale = s.q_stale[pri]
        if stale <= 0:
            return

        if (s.ticks - s.q_last_compact_tick[pri]) < s.compact_min_ticks:
            return

        if stale < int(n * s.compact_stale_frac):
            return

        # Preserve relative order by filtering the existing list.
        live = s.pipelines_by_id
        new_q = []
        for pid in q:
            if pid in live and pid in s.in_queue[pri]:
                new_q.append(pid)

        s.q[pri] = new_q
        s.q_head[pri] = 0
        s.q_stale[pri] = 0
        s.q_last_compact_tick[pri] = s.ticks

    def _default_shares(q_len, i_len, b_len):
        # Base shares: ensure INTERACTIVE makes progress while protecting QUERY.
        q_share = 0.60
        i_share = 0.35
        b_share = 0.05

        # If a class has no backlog, donate its share.
        if b_len <= 0:
            b_share = 0.0
            # donate proportionally to q/i
            tot = q_share + i_share
            if tot > 0:
                q_share = q_share / tot
                i_share = i_share / tot

        if i_len <= 0:
            q_share = min(0.95, q_share + i_share)
            i_share = 0.0

        if q_len <= 0:
            i_share = min(0.95, i_share + q_share)
            q_share = 0.0

        # Starvation boosts (small, bounded)
        if i_len > 0 and (s.ticks - s.last_dispatch_tick[Priority.INTERACTIVE]) > 80:
            boost = 0.10
            take = min(boost, q_share)
            q_share -= take
            i_share += take

        if b_len > 0 and (s.ticks - s.last_dispatch_tick[Priority.BATCH_PIPELINE]) > 400:
            boost = 0.03
            take_q = min(boost * 0.7, q_share)
            take_i = min(boost - take_q, i_share)
            q_share -= take_q
            i_share -= take_i
            b_share += (take_q + take_i)

        # Normalize to 1.0 if anything is nonzero
        tot = q_share + i_share + b_share
        if tot <= 0:
            return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        return {
            Priority.QUERY: q_share / tot,
            Priority.INTERACTIVE: i_share / tot,
            Priority.BATCH_PIPELINE: b_share / tot,
        }

    def _size_request(pool, pri, pid, op_obj_id, backlog_total):
        # CPU sizing: moderate per-op CPU to preserve concurrency; scale gently with pool size.
        if pri == Priority.QUERY:
            base_cpu = 8
            cpu_cap = 24
        elif pri == Priority.INTERACTIVE:
            base_cpu = 6
            cpu_cap = 16
        else:
            base_cpu = 4
            cpu_cap = 12

        mult = 1
        if pool.max_cpu_pool >= 512:
            mult = 3
        elif pool.max_cpu_pool >= 256:
            mult = 2
        elif pool.max_cpu_pool >= 128:
            mult = 2

        cpu_req = base_cpu * mult
        if cpu_req > cpu_cap:
            cpu_req = cpu_cap
        if cpu_req < 1:
            cpu_req = 1
        if cpu_req > pool.max_cpu_pool:
            cpu_req = int(pool.max_cpu_pool)

        # Under high backlog, prefer more concurrency.
        # Approx heuristic: if backlog is large relative to CPU, halve per-op CPU.
        if backlog_total > int(pool.max_cpu_pool * 1.5):
            cpu_req = max(2, cpu_req // 2)
        if backlog_total > int(pool.max_cpu_pool * 3.0):
            cpu_req = max(1, cpu_req // 2)

        # RAM sizing: start reasonable; bump quickly on OOM; also use pipeline baseline.
        if pri == Priority.QUERY:
            default_ram = 24
        elif pri == Priority.INTERACTIVE:
            default_ram = 32
        else:
            default_ram = 16

        base = s.pipeline_ram_base.get(pid, 0)
        if base is None:
            base = 0

        hint = s.op_ram_hint.get((pid, op_obj_id), 0)
        if hint is None:
            hint = 0

        ram_req = default_ram
        if base > ram_req:
            ram_req = base

        if hint > 0:
            # Slight safety margin above the last known failing allocation / hint.
            # Keep aggressive convergence but avoid huge jumps.
            ram_req = max(ram_req, int(hint * 1.15) + 1)

        if ram_req < 1:
            ram_req = 1
        if ram_req > pool.max_ram_pool:
            ram_req = int(pool.max_ram_pool)

        return int(cpu_req), int(ram_req)

    def _pick_candidate(pri, pool, avail_cpu, avail_ram, scheduled_pids, backlog_total):
        # Fast ring-queue scan; bounded attempts.
        if len(s.in_queue[pri]) == 0:
            return None

        q = s.q[pri]
        n = len(q)
        if n == 0:
            return None

        head = s.q_head[pri]
        scans = s.max_scan_per_pick
        if scans > n:
            scans = n

        for _ in range(scans):
            if n == 0:
                break

            if head >= n:
                head = 0

            pid = q[head]
            head += 1

            p = s.pipelines_by_id.get(pid)
            if p is None or pid not in s.in_queue[pri]:
                s.q_stale[pri] += 1
                continue

            if pid in s.dead_pipelines:
                s.q_stale[pri] += 1
                _drop_pipeline(pid)
                continue

            if pid in scheduled_pids:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                s.q_stale[pri] += 1
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            op_obj_id = id(op)

            cpu_req, ram_req = _size_request(pool, pri, pid, op_obj_id, backlog_total)

            # Hard fit checks
            if cpu_req > avail_cpu or ram_req > avail_ram:
                continue
            if cpu_req < 1 or ram_req < 1:
                continue

            # Cap RAM hint to pool (and kill if it can never fit)
            if ram_req > pool.max_ram_pool:
                s.dead_pipelines.add(pid)
                s.q_stale[pri] += 1
                _drop_pipeline(pid)
                continue

            s.q_head[pri] = head % n if n > 0 else 0
            return (p, pid, pri, op, op_obj_id, cpu_req, ram_req)

        s.q_head[pri] = head % n if n > 0 else 0
        _maybe_compact_queue(pri)
        return None

    # --- Ingest arrivals ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.pipelines_by_id:
            continue

        pri = p.priority
        s.pipelines_by_id[pid] = p
        s.pipeline_priority[pid] = pri
        s.pipeline_enq_tick[pid] = s.ticks
        s.in_queue[pri].add(pid)
        s.q[pri].append(pid)

    # --- Process results (bounded retry + OOM RAM hints) ---
    touched_pids = set()
    for r in results:
        failed = (hasattr(r, "failed") and r.failed())
        oom = failed and _is_oom_error(getattr(r, "error", None))

        ops = getattr(r, "ops", None) or []
        for op in ops:
            op_obj_id = id(op)
            pid = s.op_to_pipeline.get(op_obj_id)
            if pid is None:
                continue
            touched_pids.add(pid)

            key = (pid, op_obj_id)

            if failed:
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is None or alloc_ram <= 0:
                    alloc_ram = s.op_ram_hint.get(key, 0) or 1

                # Keep a pipeline baseline at least as large as observed allocs for stability
                prev_base = s.pipeline_ram_base.get(pid, 0) or 0
                if alloc_ram > prev_base:
                    s.pipeline_ram_base[pid] = int(alloc_ram)

                if oom:
                    s.op_oom_count[key] = s.op_oom_count.get(key, 0) + 1

                    prev_hint = s.op_ram_hint.get(key, 0) or 0
                    # Escalate fairly quickly, but not explosively.
                    nxt = int(max(alloc_ram * 1.7 + 1, alloc_ram + 8, prev_hint * 1.3 + 1 if prev_hint > 0 else 0))
                    if nxt < 1:
                        nxt = 1
                    s.op_ram_hint[key] = nxt

                    if s.op_oom_count[key] > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    if s.op_fail_count[key] > s.max_fail_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success: update pipeline baseline with allocated RAM (useful for subsequent ops)
                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is not None and alloc_ram > 0:
                    prev_base = s.pipeline_ram_base.get(pid, 0) or 0
                    if alloc_ram > prev_base:
                        s.pipeline_ram_base[pid] = int(alloc_ram)

                # Cleanup per-op state to keep memory bounded
                s.op_ram_hint.pop(key, None)
                s.op_oom_count.pop(key, None)
                s.op_fail_count.pop(key, None)
                s.op_to_pipeline.pop(op_obj_id, None)

    # Drop pipelines that became successful among those touched by results (bounded work)
    for pid in touched_pids:
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        try:
            if p.runtime_status().is_pipeline_successful():
                _drop_pipeline(pid)
        except Exception:
            # If runtime_status misbehaves, keep pipeline; simulator will surface errors elsewhere.
            pass

    # --- Scheduling ---
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    # Local resource snapshots (MUST track decrements ourselves)
    local_cpu = []
    local_ram = []
    total_max_cpu = 0
    for i in range(num_pools):
        pool = s.executor.pools[i]
        local_cpu.append(pool.avail_cpu_pool)
        local_ram.append(pool.avail_ram_pool)
        total_max_cpu += int(pool.max_cpu_pool)

    # Global caps
    q_len = len(s.in_queue[Priority.QUERY])
    i_len = len(s.in_queue[Priority.INTERACTIVE])
    b_len = len(s.in_queue[Priority.BATCH_PIPELINE])
    backlog_total = q_len + i_len + b_len

    shares = _default_shares(q_len, i_len, b_len)

    # Allow more scheduling work on larger clusters, but keep bounded
    max_assignments_total = int(total_max_cpu / 4)
    if max_assignments_total < 24:
        max_assignments_total = 24
    if max_assignments_total > 128:
        max_assignments_total = 128

    scheduled_pids = set()
    scheduled_counts = {}  # pid -> count scheduled this tick (cap per pipeline)

    def _can_schedule_more_for_pid(pid):
        c = scheduled_counts.get(pid, 0)
        return c < s.max_ops_per_pipeline_per_tick

    def _note_scheduled(pid, pri):
        scheduled_pids.add(pid)
        scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
        s.last_dispatch_tick[pri] = s.ticks

    # Two-pass: (1) enforce per-priority caps, (2) fill leftovers
    pri_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(num_pools):
        if len(assignments) >= max_assignments_total:
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = local_cpu[pool_id]
        avail_ram = local_ram[pool_id]

        if avail_cpu < 1 or avail_ram < 1:
            continue

        # Per-pool cap of new assignments (keeps per-tick work bounded)
        per_pool_cap = int(pool.max_cpu_pool / 4)
        if per_pool_cap < 8:
            per_pool_cap = 8
        if per_pool_cap > 48:
            per_pool_cap = 48

        used_cpu = {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
        used_ram = {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}

        cap_cpu = {
            Priority.QUERY: avail_cpu * shares[Priority.QUERY],
            Priority.INTERACTIVE: avail_cpu * shares[Priority.INTERACTIVE],
            Priority.BATCH_PIPELINE: avail_cpu * shares[Priority.BATCH_PIPELINE],
        }
        cap_ram = {
            Priority.QUERY: avail_ram * shares[Priority.QUERY],
            Priority.INTERACTIVE: avail_ram * shares[Priority.INTERACTIVE],
            Priority.BATCH_PIPELINE: avail_ram * shares[Priority.BATCH_PIPELINE],
        }

        def _try_schedule_one(pri, enforce_caps):
            nonlocal avail_cpu, avail_ram
            if avail_cpu < 1 or avail_ram < 1:
                return False

            if enforce_caps:
                # If this priority has exhausted both CPU and RAM caps, stop.
                if used_cpu[pri] >= cap_cpu[pri] and used_ram[pri] >= cap_ram[pri]:
                    return False

            cand = _pick_candidate(pri, pool, avail_cpu, avail_ram, scheduled_pids, backlog_total)
            if cand is None:
                return False

            p, pid, pri2, op, op_obj_id, cpu_req, ram_req = cand
            if pri2 != pri:
                return False
            if pid in s.dead_pipelines or pid not in s.pipelines_by_id:
                return False
            if not _can_schedule_more_for_pid(pid):
                return False

            # Enforce caps softly: if exceeding both caps, defer (unless we're in fill phase).
            if enforce_caps:
                if (used_cpu[pri] + cpu_req) > (cap_cpu[pri] + 0.01) and (used_ram[pri] + ram_req) > (cap_ram[pri] + 0.01):
                    return False

            if cpu_req > avail_cpu or ram_req > avail_ram or cpu_req < 1 or ram_req < 1:
                return False

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=int(cpu_req),
                    ram=int(ram_req),
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            s.op_to_pipeline[op_obj_id] = pid

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            used_cpu[pri] += cpu_req
            used_ram[pri] += ram_req
            _note_scheduled(pid, pri)

            return True

        # Pass 1: allocate per-priority shares
        made_progress = True
        while made_progress and avail_cpu >= 1 and avail_ram >= 1:
            if len(assignments) >= max_assignments_total:
                break
            if (len(assignments) % 1) == 0 and (len(assignments) - sum(1 for _ in [])) >= 0:
                pass  # no-op, keeps structure simple

            if (len(assignments) % 999999) == -1:
                break  # unreachable safeguard

            if (len(assignments) >= max_assignments_total) or (len(assignments) >= 10**9):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            if (len(assignments) >= max_assignments_total):
                break

            # stop when per-pool cap reached
            if (len(assignments) % (10**9)) < 0:
                break  # unreachable

            if (len(assignments) >= max_assignments_total):
                break

            # Enforce per-pool cap using how many we scheduled to this pool this tick:
            # (approx by counting assignments with pool_id; bounded by small cap)
            pool_assigned = 0
            # Bound the scan: only look at the tail where new assignments are appended.
            tail_scan = 64
            if tail_scan > len(assignments):
                tail_scan = len(assignments)
            for j in range(1, tail_scan + 1):
                if assignments[-j].pool_id == pool_id:
                    pool_assigned += 1
                if pool_assigned >= per_pool_cap:
                    break
            if pool_assigned >= per_pool_cap:
                break

            made_progress = False
            for pri in pri_order:
                if _try_schedule_one(pri, enforce_caps=True):
                    made_progress = True
                    break

        # Pass 2: fill any leftovers (prefer QUERY, then INTERACTIVE; batch last)
        made_progress = True
        while made_progress and avail_cpu >= 1 and avail_ram >= 1:
            if len(assignments) >= max_assignments_total:
                break

            pool_assigned = 0
            tail_scan = 64
            if tail_scan > len(assignments):
                tail_scan = len(assignments)
            for j in range(1, tail_scan + 1):
                if assignments[-j].pool_id == pool_id:
                    pool_assigned += 1
                if pool_assigned >= per_pool_cap:
                    break
            if pool_assigned >= per_pool_cap:
                break

            made_progress = False
            for pri in pri_order:
                if _try_schedule_one(pri, enforce_caps=False):
                    made_progress = True
                    break

        local_cpu[pool_id] = avail_cpu
        local_ram[pool_id] = avail_ram

    return suspensions, assignments
