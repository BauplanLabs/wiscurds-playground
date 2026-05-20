@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Ticks
    s.ticks = 0

    # Active pipelines (bookkeeping)
    s.in_system = set()  # pipeline_id
    s.pid_to_pipeline = {}  # pipeline_id -> Pipeline
    s.pid_to_priority = {}  # pipeline_id -> Priority

    # Per-priority run queues (round-robin via head pointer; lazily cleaned)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Enqueue tick for (light) aging/debug
    s.enq_tick = {}  # pipeline_id -> tick

    # Map operator object -> pipeline_id for attributing results back to a pipeline/op
    s.op_to_pid = {}  # op_key -> pipeline_id

    # Per-op retry / sizing hints (learned from failures)
    # key: (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_oom_count = {}
    s.op_fail_count = {}

    # Per-pipeline cleanup: track seen op_keys (small, <= ~8 ops/pipeline)
    s.pid_opkeys = {}  # pipeline_id -> list[op_key]

    # Retry/front-of-line queues for failed ops (small)
    s.retry_stack = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.retry_in = set()  # pipeline_id currently present in some retry_stack

    # Mark permanently dead only after bounded retries (avoid hangs)
    s.dead_pipelines = set()

    # Weighted RR sequence (protect query, but guarantee interactive progress)
    # Ratio ~ 10 : 5 : 1
    s.rr_seq = (
        [Priority.QUERY] * 10
        + [Priority.INTERACTIVE] * 5
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_pos = 0

    # Knobs (keep per-tick work bounded)
    s.max_new_assignments_per_tick = 64
    s.max_scan_per_pick = 24

    # Retry bounds
    s.max_oom_retries = 5
    s.max_nonoom_retries = 6

    # Sizing knobs (small default allocations; converge upward on OOM)
    s.ram_cpu_factor = 0.50   # fraction of pool RAM/CPU ratio to allocate per requested vCPU
    s.ram_growth = 1.80       # multiplicative bump on OOM
    s.ram_additive = 1.0      # additive bump (GB-ish) to avoid fencepost repeats

    # Minimum RAM floors by priority (GB-ish)
    s.min_ram = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    if not pipelines and not results:
        return [], []

    def _op_key(op):
        # Use object identity; stable within the simulator and avoids cross-pipeline id collisions.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.queues[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.queues[Priority.INTERACTIVE]
        return s.queues[Priority.BATCH_PIPELINE]

    def _retry_stack_for_priority(pri):
        if pri == Priority.QUERY:
            return s.retry_stack[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.retry_stack[Priority.INTERACTIVE]
        return s.retry_stack[Priority.BATCH_PIPELINE]

    def _retire_pipeline(pid):
        s.in_system.discard(pid)
        s.pid_to_pipeline.pop(pid, None)
        s.pid_to_priority.pop(pid, None)
        s.enq_tick.pop(pid, None)

        # Remove from retry bookkeeping (lazy for stacks; we use membership checks)
        s.retry_in.discard(pid)

        # Clean per-op state for this pipeline
        opks = s.pid_opkeys.pop(pid, None) or []
        for opk in opks:
            s.op_to_pid.pop(opk, None)
            s.op_ram_hint.pop((pid, opk), None)
            s.op_oom_count.pop((pid, opk), None)
            s.op_fail_count.pop((pid, opk), None)

    def _mark_dead(pid):
        if pid in s.dead_pipelines:
            return
        s.dead_pipelines.add(pid)
        _retire_pipeline(pid)

    # --- Ingest pipelines (assumed: new arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.in_system:
            continue
        s.in_system.add(pid)
        s.pid_to_pipeline[pid] = p
        s.pid_to_priority[pid] = p.priority
        s.enq_tick[pid] = s.ticks
        s.pid_opkeys[pid] = []
        _queue_for_priority(p.priority).append(p)

    # --- Process results: update OOM hints / retry counters ---
    touched_pids = set()
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = (hasattr(r, "failed") and r.failed())
        oom = failed and _is_oom_error(getattr(r, "error", None))

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pid.pop(opk, None)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue

            touched_pids.add(pid)

            k = (pid, opk)
            if failed:
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                if oom:
                    s.op_oom_count[k] = s.op_oom_count.get(k, 0) + 1

                    prev_hint = s.op_ram_hint.get(k, None)
                    alloc = getattr(r, "ram", None)
                    if alloc is None or alloc <= 0:
                        alloc = prev_hint if (prev_hint is not None and prev_hint > 0) else 1.0

                    new_hint = alloc * s.ram_growth + s.ram_additive
                    if prev_hint is not None:
                        alt = prev_hint * s.ram_growth + s.ram_additive
                        if alt > new_hint:
                            new_hint = alt
                    s.op_ram_hint[k] = new_hint

                    if s.op_oom_count.get(k, 0) > s.max_oom_retries:
                        _mark_dead(pid)
                        continue
                else:
                    if s.op_fail_count.get(k, 0) > s.max_nonoom_retries:
                        _mark_dead(pid)
                        continue

                # Prioritize failed pipelines for quicker convergence
                pri = s.pid_to_priority.get(pid, None)
                if pri is not None and pid not in s.retry_in and pid in s.in_system:
                    s.retry_in.add(pid)
                    _retry_stack_for_priority(pri).append(pid)
            else:
                # Success: clear per-op retry state (keep RAM hint; it may be useful for the next attempt in same op on transient OOM)
                s.op_fail_count.pop(k, None)
                s.op_oom_count.pop(k, None)

    # If any touched pipeline has finished, retire it (avoid future scans)
    for pid in list(touched_pids):
        p = s.pid_to_pipeline.get(pid, None)
        if p is None:
            continue
        try:
            if p.runtime_status().is_pipeline_successful():
                _retire_pipeline(pid)
        except Exception:
            # If status check fails, ignore and let normal queue scan handle it.
            pass

    # --- Scheduling ---
    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    def _cpu_target(pool, pri):
        # Prefer concurrency; scale mildly with pool size; cap to keep per-op sizing reasonable.
        base_scale = int(pool.max_cpu_pool / 64) if pool.max_cpu_pool >= 64 else 1
        if base_scale < 1:
            base_scale = 1

        if pri == Priority.BATCH_PIPELINE:
            tgt = base_scale
            if tgt < 1:
                tgt = 1
            if tgt > 2:
                tgt = 2
            return tgt

        # QUERY / INTERACTIVE: a bit more CPU, but still concurrency-friendly
        tgt = 2 * base_scale
        if tgt < 2:
            tgt = 2
        if tgt > 4:
            tgt = 4
        return tgt

    def _ram_per_cpu(pool):
        denom = pool.max_cpu_pool if pool.max_cpu_pool and pool.max_cpu_pool > 0 else 1.0
        return (pool.max_ram_pool / denom) * s.ram_cpu_factor

    def _size_for_pool(pool, pri, pid, opk, avail_cpu, avail_ram):
        if avail_cpu < 1 or avail_ram < 1:
            return None, None

        cpu_tgt = _cpu_target(pool, pri)
        if cpu_tgt > pool.max_cpu_pool:
            cpu_tgt = int(pool.max_cpu_pool)

        # Honor hints learned from OOM (do not clamp below hint)
        hint = s.op_ram_hint.get((pid, opk), None)
        if hint is not None and hint > pool.max_ram_pool:
            # Can't ever fit in this pool
            return None, None

        rpc = _ram_per_cpu(pool)
        min_ram = s.min_ram.get(pri, 4.0)

        max_cpu_try = int(avail_cpu)
        if max_cpu_try > cpu_tgt:
            max_cpu_try = cpu_tgt
        if max_cpu_try > pool.max_cpu_pool:
            max_cpu_try = int(pool.max_cpu_pool)
        if max_cpu_try < 1:
            return None, None

        # Try decreasing CPU to fit RAM/CPU availability
        for cpu in range(max_cpu_try, 0, -1):
            ram = cpu * rpc
            if ram < min_ram:
                ram = min_ram
            if hint is not None and ram < hint:
                ram = hint

            if ram > pool.max_ram_pool:
                ram = pool.max_ram_pool

            if cpu <= avail_cpu and ram <= avail_ram and cpu >= 1 and ram >= 1:
                return cpu, ram

        return None, None

    def _choose_pool_and_size(pri, pid, opk):
        best = None  # (waste, pool_id, cpu, ram)
        for pool_id in range(num_pools):
            if local_cpu[pool_id] < 1 or local_ram[pool_id] < 1:
                continue
            pool = s.executor.pools[pool_id]
            cpu, ram = _size_for_pool(pool, pri, pid, opk, local_cpu[pool_id], local_ram[pool_id])
            if cpu is None:
                continue

            # Best-fit-ish: prefer placements that leave less normalized waste.
            waste = ((local_cpu[pool_id] - cpu) / (pool.max_cpu_pool if pool.max_cpu_pool > 0 else 1.0)) + (
                (local_ram[pool_id] - ram) / (pool.max_ram_pool if pool.max_ram_pool > 0 else 1.0)
            )
            if best is None or waste < best[0]:
                best = (waste, pool_id, cpu, ram)
        if best is None:
            return None, None, None
        return best[1], best[2], best[3]

    def _compact_queue(pri):
        # Occasional compaction to remove retired/dead pipelines; bounded frequency.
        q = s.queues[pri]
        if not q:
            s.q_head[pri] = 0
            return
        newq = []
        for p in q:
            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.dead_pipelines:
                continue
            if pid not in s.in_system:
                continue
            newq.append(p)
        s.queues[pri] = newq
        s.q_head[pri] = 0

    # Rare compaction (keeps queue scanning fast)
    if s.ticks % 2000 == 0:
        _compact_queue(Priority.QUERY)
        _compact_queue(Priority.INTERACTIVE)
        _compact_queue(Priority.BATCH_PIPELINE)

    # Ensure we don't assign more than one op per pipeline per tick (prevents duplicates within same scheduler call)
    scheduled_pids = set()

    def _pick_from_retry(pri):
        stack = _retry_stack_for_priority(pri)
        # Try a few retry candidates (stack is typically tiny)
        for _ in range(4):
            if not stack:
                return None, None, None
            pid = stack.pop()
            s.retry_in.discard(pid)
            if pid in s.dead_pipelines or pid not in s.in_system:
                continue
            if pid in scheduled_pids:
                continue
            p = s.pid_to_pipeline.get(pid, None)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _retire_pipeline(pid)
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]
            return p, op, _op_key(op)
        return None, None, None

    def _pick_from_queue(pri):
        q = s.queues[pri]
        n = len(q)
        if n == 0:
            return None, None, None

        head = s.q_head[pri]
        scan = s.max_scan_per_pick
        if scan > n:
            scan = n

        picked = None
        for j in range(scan):
            idx = head + j
            if idx >= n:
                idx -= n
            p = q[idx]
            if p is None:
                continue
            pid = p.pipeline_id

            if pid in s.dead_pipelines or pid not in s.in_system:
                q[idx] = None
                continue
            if pid in scheduled_pids:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                q[idx] = None
                _retire_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]
            opk = _op_key(op)
            picked = (p, op, opk, idx)
            break

        # Advance head for fairness (even if no pick)
        if n > 0:
            if picked is not None:
                s.q_head[pri] = picked[3] + 1
                if s.q_head[pri] >= n:
                    s.q_head[pri] -= n
            else:
                s.q_head[pri] = head + scan
                if s.q_head[pri] >= n:
                    s.q_head[pri] %= n

        if picked is None:
            return None, None, None
        return picked[0], picked[1], picked[2]

    def _pick_candidate(pri):
        # Prefer retry candidates first for the same priority
        p, op, opk = _pick_from_retry(pri)
        if p is not None:
            return p, op, opk
        return _pick_from_queue(pri)

    def _fallback_order(primary):
        if primary == Priority.QUERY:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if primary == Priority.INTERACTIVE:
            return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    assignments = []
    suspensions = []

    for _ in range(s.max_new_assignments_per_tick):
        # Stop if no resources anywhere
        any_cpu = 0.0
        any_ram = 0.0
        for i in range(num_pools):
            any_cpu += local_cpu[i]
            any_ram += local_ram[i]
        if any_cpu < 1 or any_ram < 1:
            break

        primary = s.rr_seq[s.rr_pos]
        s.rr_pos += 1
        if s.rr_pos >= len(s.rr_seq):
            s.rr_pos = 0

        placed = False
        for pri in _fallback_order(primary):
            p, op, opk = _pick_candidate(pri)
            if p is None:
                continue

            pid = p.pipeline_id
            if pid in scheduled_pids:
                continue
            if pid in s.dead_pipelines or pid not in s.in_system:
                continue

            pool_id, cpu, ram = _choose_pool_and_size(p.priority, pid, opk)
            if pool_id is None:
                # Can't fit anywhere right now; try another pipeline/priority
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Track result attribution + cleanup keys
            s.op_to_pid[opk] = pid
            s.pid_opkeys.setdefault(pid, []).append(opk)

            # Local accounting (snapshot-safe)
            local_cpu[pool_id] -= cpu
            local_ram[pool_id] -= ram

            scheduled_pids.add(pid)
            placed = True
            break

        if not placed:
            break

    return suspensions, assignments
