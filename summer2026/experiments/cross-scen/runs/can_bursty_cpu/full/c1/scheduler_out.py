@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority circular queues implemented as (list + head index) to avoid O(n) pop(0).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.wait_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track active pipelines (not yet successful, not permanently failed).
    s.active = set()  # pipeline_id
    s.known = set()   # pipeline_id (seen at least once)

    # Approximate counts (updated when we *notice* completion/death during scans).
    s.active_counts = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-pipeline metadata
    s.pipeline_meta = {}  # pipeline_id -> {"enq_tick": int, "priority": Priority}

    # Failure-driven resource hints and bounded retry counters (per operator instance).
    # Keyed by (pipeline_id, op_key).
    s.ram_hint = {}
    s.cpu_hint = {}
    s.oom_retries = {}
    s.fail_retries = {}

    # Map op_key -> pipeline_id so we can attribute ExecutionResult back to a pipeline.
    # We store multiple keys per op (stable id fields + id(op)) to improve match likelihood.
    s.op_to_pipeline = {}

    # Permanently failed pipelines (stop scheduling them).
    s.dead_pipelines = set()

    # Throttle knobs
    s.max_new_assignments_per_tick = 32
    s.max_new_assignments_per_pool_per_tick = 24
    s.scan_cap_per_pick = 32

    # Priority service shaping: allow a little batch even under constant interactive load.
    s.batch_every_nonbatch = 5
    s.nonbatch_since_batch = 0

    # Retry caps
    s.max_oom_retries = 5
    s.max_timeout_retries = 3
    s.max_other_fail_retries = 2

    # Periodic queue compaction (drop dead/completed pipeline objects from lists)
    s.last_compact_tick = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.compact_interval_ticks = 400  # keep rare; compaction is O(queue)


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_keys(op):
        keys = []
        for attr in ("op_id", "operator_id", "id", "key", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                keys.append(v)
        keys.append(id(op))
        # Dedup while preserving order
        seen = set()
        out = []
        for k in keys:
            if k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def _primary_op_key(op):
        v = getattr(op, "op_id", None)
        if v is not None:
            return v
        v = getattr(op, "operator_id", None)
        if v is not None:
            return v
        return id(op)

    def _err_kind(err):
        if err is None:
            return None
        msg = str(err).lower()
        if ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg):
            return "oom"
        # Common OOM-kill-ish strings (keep conservative to avoid misclassifying):
        if ("killed" in msg and "memory" in msg) or ("oom-kill" in msg):
            return "oom"
        if ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg) or ("time limit" in msg):
            return "timeout"
        return "other"

    def _drop_pipeline(pid):
        if pid in s.active:
            s.active.discard(pid)
            meta = s.pipeline_meta.get(pid)
            if meta:
                pri = meta.get("priority")
                if pri in s.active_counts and s.active_counts[pri] > 0:
                    s.active_counts[pri] -= 1
        s.pipeline_meta.pop(pid, None)

    def _maybe_compact(pri):
        if s.ticks - s.last_compact_tick[pri] < s.compact_interval_ticks:
            return
        q = s.wait_q[pri]
        if not q:
            s.wait_head[pri] = 0
            s.last_compact_tick[pri] = s.ticks
            return
        # Only compact if there's likely a lot of dead weight.
        # Heuristic: if queue is moderately large, rebuild.
        if len(q) < 128:
            return
        new_q = []
        for p in q:
            if p is None:
                continue
            pid = getattr(p, "pipeline_id", None)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue
            if pid not in s.active:
                continue
            new_q.append(p)
        s.wait_q[pri] = new_q
        s.wait_head[pri] = 0
        s.last_compact_tick[pri] = s.ticks

    def _base_cpu(pri):
        # Calibrated to match cluster RAM:CPU ratio (~7.8 GB per vCPU on all scales here).
        if pri == Priority.QUERY:
            return 8
        if pri == Priority.INTERACTIVE:
            return 6
        return 4

    def _max_cpu(pri, pool):
        # Keep modest to preserve concurrency; sublinear CPU scaling means very large CPU is wasteful.
        if pri == Priority.QUERY:
            return min(16, int(pool.max_cpu_pool))
        if pri == Priority.INTERACTIVE:
            return min(12, int(pool.max_cpu_pool))
        return min(8, int(pool.max_cpu_pool))

    def _base_ram(pri, pool):
        # Moderate starting point; should allow high concurrency without huge over-allocation.
        # OOM feedback will push up when needed.
        if pri == Priority.QUERY:
            return min(48, pool.max_ram_pool)
        if pri == Priority.INTERACTIVE:
            return min(40, pool.max_ram_pool)
        return min(32, pool.max_ram_pool)

    def _desired_resources(pool, pri, pid, op):
        opk = _primary_op_key(op)
        key = (pid, opk)

        # RAM: never clip downward to fit; if it doesn't fit, wait for headroom or another pool.
        ram = s.ram_hint.get(key)
        if ram is None:
            ram = _base_ram(pri, pool)
        # Enforce above previous OOM boundary with a small safety margin if present.
        # (We encode that directly into ram_hint on OOM; still guard here.)
        if ram < 1:
            ram = 1
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        # CPU: clip to availability later; it's safe to reduce CPU (slower, but not incorrect).
        cpu = s.cpu_hint.get(key)
        if cpu is None:
            cpu = _base_cpu(pri)

        # Light-load boost: if pool is mostly idle, give more CPU to cut latency.
        # Under load, stick to base to maximize throughput.
        try:
            idle_frac = float(pool.avail_cpu_pool) / float(pool.max_cpu_pool) if pool.max_cpu_pool else 0.0
        except Exception:
            idle_frac = 0.0

        if cpu < _base_cpu(pri):
            cpu = _base_cpu(pri)

        if idle_frac > 0.80:
            cpu = max(cpu, _base_cpu(pri) * 2)
        if idle_frac > 0.93:
            cpu = max(cpu, _base_cpu(pri) * 3)

        cpu_cap = _max_cpu(pri, pool)
        if cpu > cpu_cap:
            cpu = cpu_cap
        if cpu < 1:
            cpu = 1

        return int(cpu), ram

    def _pick_next(pri, pool, avail_cpu, avail_ram, scheduled_pids, scheduled_opkeys, per_pid_limit):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None

        h = s.wait_head[pri] % n
        scanned = 0
        scan_cap = s.scan_cap_per_pick if s.scan_cap_per_pick > 0 else 1

        while scanned < scan_cap and n > 0:
            p = q[h]
            h = (h + 1) % n
            scanned += 1

            if p is None:
                continue

            pid = getattr(p, "pipeline_id", None)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                if pid in s.active:
                    _drop_pipeline(pid)
                continue
            if pid not in s.active:
                # Already completed/dropped; skip.
                continue

            # Per-tick per-pipeline limit (allow some intra-pipeline parallelism for top priorities).
            cur = scheduled_pids.get(pid, 0)
            if cur >= per_pid_limit:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            # Pick the first assignable op we haven't already assigned this tick.
            chosen = None
            for op in ops[:3]:
                opk = _primary_op_key(op)
                if (pid, opk) in scheduled_opkeys:
                    continue
                chosen = op
                break
            if chosen is None:
                continue

            cpu_desired, ram_desired = _desired_resources(pool, pri, pid, chosen)

            # Must fit RAM; CPU can be clipped down but require at least 1.
            if ram_desired > avail_ram:
                continue
            if avail_cpu < 1:
                continue

            cpu_assigned = cpu_desired
            if cpu_assigned > avail_cpu:
                cpu_assigned = int(avail_cpu)
            if cpu_assigned < 1:
                continue

            s.wait_head[pri] = h
            return p, chosen, cpu_assigned, ram_desired

        s.wait_head[pri] = h
        return None

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.known:
            continue
        s.known.add(pid)

        if pid in s.dead_pipelines:
            continue

        s.active.add(pid)
        s.pipeline_meta[pid] = {"enq_tick": s.ticks, "priority": p.priority}
        if p.priority in s.wait_q:
            s.wait_q[p.priority].append(p)
        else:
            s.wait_q[Priority.BATCH_PIPELINE].append(p)
            s.pipeline_meta[pid]["priority"] = Priority.BATCH_PIPELINE
        pri = s.pipeline_meta[pid]["priority"]
        if pri in s.active_counts:
            s.active_counts[pri] += 1

    # --- Process results (OOM/timeout-aware hints, bounded retries) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
            pool = s.executor.pools[pool_id]

        failed = False
        if hasattr(r, "failed") and r.failed():
            failed = True

        kind = None
        if failed:
            kind = _err_kind(getattr(r, "error", None))

        for op in ops:
            opk = _primary_op_key(op)

            pid = None
            # Try multiple possible keys for robustness.
            for k in _op_keys(op):
                pid = s.op_to_pipeline.get(k)
                if pid is not None:
                    break
            if pid is None:
                continue

            key = (pid, opk)

            # This op result is terminal for this attempt; clear mapping entries to avoid unbounded growth.
            for k in _op_keys(op):
                if s.op_to_pipeline.get(k) == pid:
                    s.op_to_pipeline.pop(k, None)

            if pid in s.dead_pipelines:
                continue

            if not failed:
                # Success: reset failure hints for this op instance (it won't run again unless retried elsewhere).
                s.oom_retries.pop(key, None)
                s.fail_retries.pop(key, None)
                s.cpu_hint.pop(key, None)
                # Keep ram_hint as-is; it doesn't matter much after success but can help if op is retried (rare).
                continue

            # Failed: update hints and retry counters.
            if kind == "oom":
                cnt = s.oom_retries.get(key, 0) + 1
                s.oom_retries[key] = cnt

                alloc_ram = getattr(r, "ram", None)
                if alloc_ram is None or alloc_ram <= 0:
                    alloc_ram = s.ram_hint.get(key, _base_ram(s.pipeline_meta.get(pid, {}).get("priority", Priority.BATCH_PIPELINE), pool) if pool else 1)

                # Bump fairly aggressively to converge quickly, but avoid wild over-allocation.
                # Use pool cap if available.
                new_ram = max(float(alloc_ram) * 1.8, float(alloc_ram) + 8.0)
                if pool is not None:
                    if new_ram > pool.max_ram_pool:
                        new_ram = pool.max_ram_pool
                if new_ram < 1:
                    new_ram = 1

                s.ram_hint[key] = new_ram

                # If we keep OOMing even near cap, stop thrashing.
                if cnt >= s.max_oom_retries + 1:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)

            elif kind == "timeout":
                cnt = s.fail_retries.get(key, 0) + 1
                s.fail_retries[key] = cnt

                alloc_cpu = getattr(r, "cpu", None)
                if alloc_cpu is None or alloc_cpu < 1:
                    alloc_cpu = 1

                # Increase CPU to reduce runtime on retry (CPU-bound workload).
                new_cpu = int(max(int(alloc_cpu) + 2, int(alloc_cpu * 1.5)))
                if pool is not None:
                    cap = _max_cpu(s.pipeline_meta.get(pid, {}).get("priority", Priority.BATCH_PIPELINE), pool)
                    if new_cpu > cap:
                        new_cpu = cap
                if new_cpu < 1:
                    new_cpu = 1
                s.cpu_hint[key] = new_cpu

                if cnt >= s.max_timeout_retries + 1:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)

            else:
                cnt = s.fail_retries.get(key, 0) + 1
                s.fail_retries[key] = cnt

                # Mild CPU bump on retry for unknown failures.
                alloc_cpu = getattr(r, "cpu", None)
                if alloc_cpu is None or alloc_cpu < 1:
                    alloc_cpu = 1
                new_cpu = int(max(int(alloc_cpu) + 1, int(alloc_cpu * 1.2)))
                if pool is not None:
                    cap = _max_cpu(s.pipeline_meta.get(pid, {}).get("priority", Priority.BATCH_PIPELINE), pool)
                    if new_cpu > cap:
                        new_cpu = cap
                if new_cpu < 1:
                    new_cpu = 1
                s.cpu_hint[key] = new_cpu

                if cnt >= s.max_other_fail_retries + 1:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(pid)

    # Periodically compact queues to remove dead/completed pipelines from lists.
    _maybe_compact(Priority.QUERY)
    _maybe_compact(Priority.INTERACTIVE)
    _maybe_compact(Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    if not s.active:
        return suspensions, assignments

    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    # Track per-tick limits
    scheduled_pids = {}     # pipeline_id -> count scheduled this tick
    scheduled_opkeys = set()  # (pipeline_id, op_key) scheduled this tick

    total_cap = s.max_new_assignments_per_tick
    if total_cap < 1:
        total_cap = 1

    per_pool_cap = s.max_new_assignments_per_pool_per_tick
    if per_pool_cap < 1:
        per_pool_cap = 1

    # Dynamic per-pipeline per-tick limits by priority
    per_pid_limit = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Fill pools; prioritize QUERY, then INTERACTIVE; allow occasional BATCH to prevent total starvation.
    for pool_id in range(s.executor.num_pools):
        if len(assignments) >= total_cap:
            break

        pool = s.executor.pools[pool_id]
        made = 0

        while made < per_pool_cap and len(assignments) < total_cap:
            if local_cpu[pool_id] < 1 or local_ram[pool_id] < 1:
                break

            # Decide priority order for this placement attempt.
            # Always *try* QUERY first; if no runnable op found, fall through.
            if s.active_counts.get(Priority.QUERY, 0) > 0:
                pri_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                if (s.active_counts.get(Priority.BATCH_PIPELINE, 0) > 0) and (s.nonbatch_since_batch >= s.batch_every_nonbatch):
                    pri_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
                else:
                    pri_order = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            picked = None
            for pri in pri_order:
                if s.active_counts.get(pri, 0) <= 0:
                    continue
                picked = _pick_next(
                    pri=pri,
                    pool=pool,
                    avail_cpu=local_cpu[pool_id],
                    avail_ram=local_ram[pool_id],
                    scheduled_pids=scheduled_pids,
                    scheduled_opkeys=scheduled_opkeys,
                    per_pid_limit=per_pid_limit.get(pri, 1),
                )
                if picked is not None:
                    break

            if picked is None:
                break

            p, op, cpu_req, ram_req = picked
            pid = p.pipeline_id
            opk = _primary_op_key(op)

            # Final safety: only assign currently-assignable ops (re-check on this tick).
            status = p.runtime_status()
            assignable = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            ok = False
            for aop in assignable[:5]:
                if _primary_op_key(aop) == opk:
                    ok = True
                    break
            if not ok:
                # Skip stale pick.
                scheduled_opkeys.add((pid, opk))
                continue

            if cpu_req > local_cpu[pool_id] or ram_req > local_ram[pool_id]:
                break

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

            # Map op keys back to pipeline id for result attribution.
            for k in _op_keys(op):
                s.op_to_pipeline[k] = pid

            local_cpu[pool_id] -= cpu_req
            local_ram[pool_id] -= ram_req

            scheduled_pids[pid] = scheduled_pids.get(pid, 0) + 1
            scheduled_opkeys.add((pid, opk))

            made += 1
            if p.priority == Priority.BATCH_PIPELINE:
                s.nonbatch_since_batch = 0
            else:
                s.nonbatch_since_batch += 1

    return suspensions, assignments
