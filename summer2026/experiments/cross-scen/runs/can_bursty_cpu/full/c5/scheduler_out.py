@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # ---- tick/time ----
    s.ticks = 0

    # ---- per-priority pipeline id queues (store pipeline_id, not objects) ----
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.qpos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # pipeline_id -> latest Pipeline object
    s.pipeline_by_id = {}

    # membership / metadata
    s.enqueued = set()  # pipeline_ids currently tracked (queued)
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}

    # pipeline_id -> next tick we should bother probing for runnable ops
    s.next_probe_tick = {}

    # ---- failure / sizing learning ----
    # (pipeline_id, op_key) -> ram_hint (next allocation target)
    s.op_ram_hint = {}
    # (pipeline_id, op_key) -> count
    s.op_oom_retries = {}
    s.op_fail_retries = {}

    # pipeline_id -> max ram that has OOM'd anywhere in this pipeline (lightweight generalization)
    s.pipeline_oom_ram_floor = {}

    # op_key -> pipeline_id (for attributing results)
    s.op_to_pipeline = {}

    # permanently failed pipelines (bounded retry)
    s.dead_pipelines = set()

    # ---- knobs: keep per-tick work bounded ----
    s.max_new_assignments_per_tick = 128
    s.max_new_assignments_per_pool_per_tick = 32
    s.max_pipeline_checks_per_pick = 24
    s.max_candidate_ops_to_check = 6

    # Backoff for pipelines that currently have no runnable ops
    s.block_backoff_ticks = 5

    # Per-pipeline per-tick cap (avoid accidental double-assign of same op due to stale status)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Retry caps
    s.max_oom_retries_per_op = 5
    s.max_nonoom_retries_per_op = 2

    # Compaction to prevent queue lists from growing forever
    s.compact_every_ticks = 2000


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return (
            ("oom" in msg)
            or ("out of memory" in msg)
            or ("memoryerror" in msg)
            or ("killed process" in msg)
            or ("killed" in msg and "memory" in msg)
        )

    def _drop_pipeline(pipeline_id):
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)
        s.next_probe_tick.pop(pipeline_id, None)
        s.pipeline_by_id.pop(pipeline_id, None)
        s.pipeline_oom_ram_floor.pop(pipeline_id, None)

    def _priority_queue(pri):
        if pri == Priority.QUERY:
            return s.q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.q[Priority.INTERACTIVE]
        return s.q[Priority.BATCH_PIPELINE]

    def _record_pipeline(p):
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid in s.dead_pipelines:
            return
        if pid in s.enqueued:
            return
        _priority_queue(p.priority).append(pid)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.next_probe_tick[pid] = s.ticks

    def _wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _base_cpu_target(pool, pri, wait_t):
        # CPU-bound workload: allocate a moderate per-op CPU that grows sublinearly with pool size.
        # Using sqrt(max_cpu) prevents pathological over-allocation at large cluster sizes.
        max_cpu = pool.max_cpu_pool
        try:
            sqrt_cpu = int((max_cpu ** 0.5) or 1)
        except Exception:
            sqrt_cpu = 8

        if pri == Priority.QUERY:
            factor = 1.5
            cap = 24
            min_cpu = 4
        elif pri == Priority.INTERACTIVE:
            factor = 1.0
            cap = 16
            min_cpu = 2
        else:
            factor = 0.7
            cap = 8
            min_cpu = 1

        target = int(sqrt_cpu * factor)
        if target < min_cpu:
            target = min_cpu
        if target > cap:
            target = cap

        # Mild aging boost (helps long-waiting interactive without exploding allocations)
        if wait_t >= 200 and pri != Priority.BATCH_PIPELINE:
            boosted = int(target * 1.25)
            if boosted > cap:
                boosted = cap
            if boosted > target:
                target = boosted

        # Never exceed pool max
        if target > max_cpu:
            target = int(max_cpu)
        if target < 1:
            target = 1
        return target, min_cpu

    def _base_ram_target(pool, pri):
        # Start with small absolute RAM (not a fraction of pool), then learn upward on OOM.
        max_ram = pool.max_ram_pool
        # Keep within reasonable range across pool sizes.
        if pri == Priority.QUERY:
            base = 12.0
        elif pri == Priority.INTERACTIVE:
            base = 14.0
        else:
            base = 10.0

        # If pools are tiny in some scale, cap to a fraction of pool to fit.
        frac_cap = max_ram * 0.20
        if frac_cap > 0 and base > frac_cap:
            base = frac_cap

        if base < 1.0:
            base = 1.0
        if base > max_ram:
            base = max_ram
        return base

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        opk = _op_key(op)

        wait_t = _wait_ticks(pid)
        cpu_target, min_cpu = _base_cpu_target(pool, pri, wait_t)

        # CPU can be reduced to fit (down to min_cpu).
        cpu_req = cpu_target
        if cpu_req > avail_cpu:
            cpu_req = int(avail_cpu)
        if cpu_req < min_cpu:
            return None, None

        max_ram = pool.max_ram_pool
        base_ram = _base_ram_target(pool, pri)

        # Generalize within pipeline: if anything in pipeline OOM'd at X, don't go below ~X next time.
        pipe_floor = s.pipeline_oom_ram_floor.get(pid, 0.0)

        hint = s.op_ram_hint.get((pid, opk), None)
        if hint is None:
            ram_req = base_ram
            if pipe_floor and ram_req < pipe_floor:
                ram_req = pipe_floor
            # If we must, allow shrinking unknown RAM request to fit the pool.
            if ram_req > avail_ram:
                ram_req = avail_ram
        else:
            # If we have a hint, do not shrink below it (avoid repeated OOM).
            # Add a small cushion to converge with fewer OOMs.
            ram_req = hint * 1.15
            if ram_req < hint:
                ram_req = hint
            if pipe_floor and ram_req < pipe_floor:
                ram_req = pipe_floor
            if ram_req > avail_ram:
                return None, None

        if ram_req < 1.0:
            ram_req = 1.0
        if ram_req > max_ram:
            ram_req = max_ram
        if ram_req > avail_ram:
            # Still can't fit (e.g., avail_ram < 1)
            return None, None

        return cpu_req, ram_req

    # --- ingest new pipelines (assumed to be newly arrived) ---
    for p in pipelines:
        _record_pipeline(p)

    # --- process results: update OOM hints / retry counts and re-probe pipelines quickly ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            # Always re-probe this pipeline soon: completion/failure unblocks downstream work.
            s.next_probe_tick[pid] = s.ticks

            if hasattr(r, "failed") and r.failed():
                err = getattr(r, "error", None)
                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is None or alloc_ram <= 0:
                    alloc_ram = None

                if _is_oom_error(err):
                    key = (pid, opk)
                    prev = s.op_ram_hint.get(key, None)
                    prev_retry = s.op_oom_retries.get(key, 0) + 1
                    s.op_oom_retries[key] = prev_retry

                    if prev_retry > s.max_oom_retries_per_op:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

                    # Increase aggressively but bounded; prefer using the allocation that OOM'd.
                    base = prev if (prev is not None and prev > 0) else 1.0
                    if alloc_ram is not None and alloc_ram > base:
                        base = alloc_ram

                    # Multiplicative increase to converge quickly.
                    new_hint = base * 1.7
                    # Ensure monotonic increase.
                    if prev is not None and new_hint <= prev:
                        new_hint = prev * 1.4

                    # Update pipeline floor too (helps similar ops in the same pipeline).
                    floor = s.pipeline_oom_ram_floor.get(pid, 0.0)
                    if alloc_ram is not None and alloc_ram > floor:
                        s.pipeline_oom_ram_floor[pid] = alloc_ram
                    elif prev is not None and prev > floor:
                        s.pipeline_oom_ram_floor[pid] = prev

                    s.op_ram_hint[key] = new_hint
                else:
                    key = (pid, opk)
                    prev_retry = s.op_fail_retries.get(key, 0) + 1
                    s.op_fail_retries[key] = prev_retry
                    if prev_retry > s.max_nonoom_retries_per_op:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                        continue

            # Clean up the attribution mapping entry (avoid unbounded growth).
            # (If an op retries, it will be re-added on next assignment.)
            if opk in s.op_to_pipeline:
                del s.op_to_pipeline[opk]

    # Periodic queue compaction to remove dropped/dead pipeline ids.
    if s.compact_every_ticks and (s.ticks % s.compact_every_ticks == 0):
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.q[pri]
            if not q:
                s.qpos[pri] = 0
                continue
            new_q = []
            for pid in q:
                if pid in s.enqueued and pid not in s.dead_pipelines and pid in s.pipeline_by_id:
                    new_q.append(pid)
            s.q[pri] = new_q
            if not new_q:
                s.qpos[pri] = 0
            else:
                s.qpos[pri] = s.qpos[pri] % len(new_q)

    # --- scheduling ---
    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    scheduled_ops = set()
    scheduled_pipeline_counts = {}  # pid -> count assigned this tick
    total_new = 0

    def _pipeline_op_limit(pid, pri):
        lim = s.max_ops_per_pipeline_per_tick.get(pri, 1)
        used = scheduled_pipeline_counts.get(pid, 0)
        return used < lim

    def _note_scheduled(pid):
        scheduled_pipeline_counts[pid] = scheduled_pipeline_counts.get(pid, 0) + 1
        # Avoid probing it repeatedly within the same tick across pools.
        s.next_probe_tick[pid] = s.ticks + 1

    def _scan_pick(pri, pool_id):
        q = s.q[pri]
        n = len(q)
        if n == 0:
            return None

        start = s.qpos[pri]
        checks = 0
        while checks < s.max_pipeline_checks_per_pick and checks < n:
            idx = (start + checks) % n
            checks += 1
            pid = q[idx]

            if pid not in s.enqueued or pid in s.dead_pipelines:
                continue

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                continue

            if s.next_probe_tick.get(pid, 0) > s.ticks:
                continue

            if not _pipeline_op_limit(pid, pri):
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                s.next_probe_tick[pid] = s.ticks + s.block_backoff_ticks
                continue

            op = None
            # Pick first runnable op not already scheduled this tick (avoid dup assignment).
            for cand in ops[: s.max_candidate_ops_to_check]:
                ok = _op_key(cand)
                if ok in scheduled_ops:
                    continue
                op = cand
                break
            if op is None:
                s.next_probe_tick[pid] = s.ticks + 1
                continue

            cpu_req, ram_req = _size_request(
                s.executor.pools[pool_id],
                pri,
                pid,
                op,
                local_cpu[pool_id],
                local_ram[pool_id],
            )
            if cpu_req is None or ram_req is None:
                # If it's a RAM fit issue due to learned hint, delay slightly to avoid hot-looping.
                s.next_probe_tick[pid] = s.ticks + 2
                continue

            # Advance round-robin cursor past this pipeline for fairness.
            if n > 0:
                s.qpos[pri] = (idx + 1) % n
            else:
                s.qpos[pri] = 0

            return (p, pid, op, cpu_req, ram_req)

        # If we scanned some, move cursor forward a bit to avoid getting stuck.
        if n > 0:
            s.qpos[pri] = (start + checks) % n
        else:
            s.qpos[pri] = 0
        return None

    # Strict priority: QUERY > INTERACTIVE > BATCH
    pri_order = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    for pool_id in range(num_pools):
        if total_new >= s.max_new_assignments_per_tick:
            break

        pool = s.executor.pools[pool_id]
        per_pool_new = 0

        while (
            per_pool_new < s.max_new_assignments_per_pool_per_tick
            and total_new < s.max_new_assignments_per_tick
            and local_cpu[pool_id] >= 1
            and local_ram[pool_id] >= 1
        ):
            picked = None
            for pri in pri_order:
                cand = _scan_pick(pri, pool_id)
                if cand is not None:
                    picked = (pri, cand)
                    break

            if picked is None:
                break

            pri, (p, pid, op, cpu_req, ram_req) = picked

            # Final safety: ensure still has resources (local counters are authoritative this tick)
            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                s.next_probe_tick[pid] = s.ticks + 1
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            opk = _op_key(op)
            s.op_to_pipeline[opk] = pid
            scheduled_ops.add(opk)
            _note_scheduled(pid)

            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req
            per_pool_new += 1
            total_new += 1

    return suspensions, assignments
