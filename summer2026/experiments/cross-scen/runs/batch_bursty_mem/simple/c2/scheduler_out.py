from typing import List, Tuple

@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    s.enqueued = set()
    s.finished_pipelines = set()
    s.dead_pipelines = set()

    # Per-pipeline metadata
    # {pid: {"enqueued_tick": int, "last_scheduled_tick": int}}
    s.pipeline_meta = {}

    # Per-op hints derived from failures
    # key: (pid, opk) -> amount
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_fail_count = {}       # (pid, opk) -> int
    s.pipe_fail_count = {}     # pid -> int

    # opk -> pid attribution (best-effort)
    s.op_to_pipeline = {}
    # pid -> set(opk) that we've ever scheduled (for cleanup)
    s.pipeline_ops_seen = {}

    # Scheduling knobs (tuned for bursty, batch-heavy, memory-bound training)
    s.scan_limit = 50
    s.compact_every = 25

    # RR sequence to guarantee batch progress (and avoid starving interactive under bursts)
    # (when a priority has no schedulable work, we skip it)
    s.rr_seq = [
        Priority.QUERY,
        Priority.INTERACTIVE,
        Priority.QUERY,
        Priority.BATCH_PIPELINE,
        Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
        Priority.QUERY,
        Priority.BATCH_PIPELINE,
    ]
    s.rr_ptr = [0 for _ in range(getattr(s.executor, "num_pools", 1) or 1)]

    # Pool reservation: if multiple pools, keep pool 0 biased to QUERY/INTERACTIVE
    s.reserve_pool0_for_hp = True

    # Base sizing (small, concurrency-friendly; learn up on failures)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 2,
    }
    s.base_ram = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 16,
        Priority.BATCH_PIPELINE: 24,
    }
    s.min_ram_floor = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 8,
    }

    # Per-pipeline parallelism per scheduler step (allow some DAG parallelism)
    s.max_ops_per_pipeline_per_step = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Headroom kept to admit high-priority work without preemption
    s.headroom_cpu_frac = 0.06
    s.headroom_ram_frac = 0.06
    s.headroom_cpu_abs = 2
    s.headroom_ram_abs = 16

    # Retry controls
    s.max_op_retries = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 10,
        Priority.BATCH_PIPELINE: 8,
    }
    s.max_pipe_failures = {
        Priority.QUERY: 12,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 10,
    }

    # Backlog adaptive CPU (increase concurrency under heavy bursts)
    s.backlog_cpu_downshift_threshold = 250  # total queued pipelines
    s.batch_cpu_downshift_to = 1

    # Caps to avoid "one container eats a VM" behavior
    s.cpu_cap_abs = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 12,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        for attr in ("op_id", "operator_id", "id", "key", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return v
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time limit" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _pipeline_total_ops(p):
        vals = getattr(p, "values", None)
        if vals is None:
            return 1
        try:
            n = len(vals)
            return n if n > 0 else 1
        except Exception:
            return 1

    def _pipeline_completed_ops(status):
        try:
            return len(status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False))
        except Exception:
            return 0

    def _pipeline_remaining_ops(p, status):
        total = _pipeline_total_ops(p)
        done = _pipeline_completed_ops(status)
        rem = total - done
        return rem if rem > 0 else 1

    def _ensure_pipeline_meta(pid):
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_scheduled_tick": s.ticks}

    def _idle_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("last_scheduled_tick", meta.get("enqueued_tick", s.ticks)))

    def _drop_pipeline(pid, mark_finished=False, mark_dead=False):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        if mark_finished:
            s.finished_pipelines.add(pid)
        if mark_dead:
            s.dead_pipelines.add(pid)

        seen = s.pipeline_ops_seen.pop(pid, None)
        if seen:
            for opk in seen:
                if s.op_to_pipeline.get(opk) == pid:
                    s.op_to_pipeline.pop(opk, None)
                s.op_ram_hint.pop((pid, opk), None)
                s.op_cpu_hint.pop((pid, opk), None)
                s.op_fail_count.pop((pid, opk), None)
        s.pipe_fail_count.pop(pid, None)

    def _cluster_caps():
        max_ram = 0
        max_cpu = 0
        for pool in s.executor.pools:
            if pool.max_ram_pool > max_ram:
                max_ram = pool.max_ram_pool
            if pool.max_cpu_pool > max_cpu:
                max_cpu = pool.max_cpu_pool
        if max_ram <= 0:
            max_ram = 1
        if max_cpu <= 0:
            max_cpu = 1
        return max_cpu, max_ram

    cluster_max_cpu, cluster_max_ram = _cluster_caps()

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.finished_pipelines or pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(pid)
        _ensure_pipeline_meta(pid)

    # --- Process results: adapt RAM/CPU hints; avoid premature blacklisting ---
    for r in results or []:
        failed = False
        if hasattr(r, "failed"):
            try:
                failed = r.failed()
            except Exception:
                failed = False

        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        err = getattr(r, "error", None)
        is_oom = failed and _is_oom_error(err)
        is_to = failed and _is_timeout_error(err)

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue
            if pid in s.finished_pipelines or pid in s.dead_pipelines:
                continue

            key = (pid, opk)

            if failed:
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
                s.pipe_fail_count[pid] = s.pipe_fail_count.get(pid, 0) + 1

                # RAM hint
                if is_oom:
                    prev = s.op_ram_hint.get(key, None)
                    alloc = getattr(r, "ram", None)
                    base = None
                    if alloc is not None:
                        try:
                            if alloc > 0:
                                base = alloc
                        except Exception:
                            base = None
                    if base is None:
                        base = prev if prev is not None else s.base_ram.get(Priority.BATCH_PIPELINE, 16)
                    new_ram = base * 2
                    if prev is not None:
                        new_ram = max(new_ram, prev * 2)
                    # clip at cluster max (we'll clip again per pool)
                    if new_ram > cluster_max_ram:
                        new_ram = cluster_max_ram
                    s.op_ram_hint[key] = new_ram

                # CPU hint
                if is_to:
                    prevc = s.op_cpu_hint.get(key, None)
                    allocc = getattr(r, "cpu", None)
                    basec = None
                    if allocc is not None:
                        try:
                            if allocc > 0:
                                basec = allocc
                        except Exception:
                            basec = None
                    if basec is None:
                        basec = prevc if prevc is not None else s.base_cpu.get(Priority.BATCH_PIPELINE, 2)
                    new_cpu = basec * 1.5
                    if prevc is not None:
                        new_cpu = max(new_cpu, prevc * 1.5)
                    if new_cpu > cluster_max_cpu:
                        new_cpu = cluster_max_cpu
                    # keep as a number; we'll ceil-ish later
                    s.op_cpu_hint[key] = new_cpu

                # Generic failure: gently bump both (but don't explode resource usage)
                if (not is_oom) and (not is_to):
                    prev = s.op_ram_hint.get(key, None)
                    if prev is None:
                        prev = s.base_ram.get(Priority.BATCH_PIPELINE, 16)
                    bumped = prev * 1.25
                    if bumped > cluster_max_ram:
                        bumped = cluster_max_ram
                    s.op_ram_hint[key] = bumped

                    prevc = s.op_cpu_hint.get(key, None)
                    if prevc is None:
                        prevc = s.base_cpu.get(Priority.BATCH_PIPELINE, 2)
                    bumpedc = prevc * 1.25
                    if bumpedc > cluster_max_cpu:
                        bumpedc = cluster_max_cpu
                    s.op_cpu_hint[key] = bumpedc

            else:
                # On success, we keep hints (they're usually small); no action needed.
                pass

    # Periodic queue compaction (remove completed/dead entries to keep scans fast)
    if s.ticks % s.compact_every == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            newq = []
            for p in s.wait_q[pri]:
                pid = p.pipeline_id
                if pid in s.finished_pipelines or pid in s.dead_pipelines:
                    continue
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(pid, mark_finished=True)
                        continue
                except Exception:
                    pass
                newq.append(p)
            s.wait_q[pri] = newq

    total_backlog = len(s.wait_q[Priority.QUERY]) + len(s.wait_q[Priority.INTERACTIVE]) + len(s.wait_q[Priority.BATCH_PIPELINE])

    def _headroom(pool):
        hc = pool.max_cpu_pool * s.headroom_cpu_frac
        hr = pool.max_ram_pool * s.headroom_ram_frac
        if hc < s.headroom_cpu_abs:
            hc = s.headroom_cpu_abs
        if hr < s.headroom_ram_abs:
            hr = s.headroom_ram_abs
        if hc > pool.max_cpu_pool:
            hc = pool.max_cpu_pool
        if hr > pool.max_ram_pool:
            hr = pool.max_ram_pool
        return hc, hr

    def _cpu_int(x):
        try:
            xi = int(x)
            if xi < 1:
                return 1
            return xi
        except Exception:
            return 1

    def _size_request(pool, pri, pid, op, keep_small_for_concurrency=True):
        opk = _op_key(op)
        key = (pid, opk)

        # Base CPU
        cpu_base = s.base_cpu.get(pri, 2)
        if keep_small_for_concurrency and pri == Priority.BATCH_PIPELINE and total_backlog >= s.backlog_cpu_downshift_threshold:
            cpu_base = min(cpu_base, s.batch_cpu_downshift_to)

        # Apply per-op CPU hint (float allowed internally)
        cpu_hint = s.op_cpu_hint.get(key, None)
        if cpu_hint is None:
            cpu_req = cpu_base
        else:
            try:
                cpu_req = cpu_hint
            except Exception:
                cpu_req = cpu_base

        # Cap CPU to avoid VM-hogging
        cpu_cap = s.cpu_cap_abs.get(pri, 12)
        if cpu_cap > pool.max_cpu_pool:
            cpu_cap = pool.max_cpu_pool
        if cpu_req > cpu_cap:
            cpu_req = cpu_cap
        if cpu_req > pool.max_cpu_pool:
            cpu_req = pool.max_cpu_pool
        cpu_req = _cpu_int(cpu_req)

        # RAM: start small, learn up on OOM; add small safety margin
        ram_hint = s.op_ram_hint.get(key, None)
        if ram_hint is None:
            ram_req = s.base_ram.get(pri, 16)
        else:
            ram_req = ram_hint

        floor = s.min_ram_floor.get(pri, 1)
        if ram_req < floor:
            ram_req = floor

        # Safety margin helps avoid repeated OOM if hint is close to true minimum.
        try:
            ram_req = ram_req * 1.10
        except Exception:
            pass

        # Clip
        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool
        if ram_req < 1:
            ram_req = 1
        try:
            ram_req = float(ram_req)
        except Exception:
            ram_req = 1.0

        return cpu_req, ram_req

    def _should_kill_pipeline(p, pid):
        # Only kill after substantial evidence it's not going to succeed.
        # Avoid killing on early timeouts/unknowns.
        pri = p.priority
        pf = s.pipe_fail_count.get(pid, 0)
        if pf < s.max_pipe_failures.get(pri, 10):
            return False

        # If any op has exceeded retry budget, consider kill.
        max_retries = s.max_op_retries.get(pri, 8)
        seen = s.pipeline_ops_seen.get(pid, None) or set()
        for opk in seen:
            if s.op_fail_count.get((pid, opk), 0) >= max_retries:
                # If it's still failing at cluster caps, likely hopeless.
                rh = s.op_ram_hint.get((pid, opk), 0) or 0
                ch = s.op_cpu_hint.get((pid, opk), 0) or 0
                if rh >= cluster_max_ram or ch >= cluster_max_cpu:
                    return True
                # Otherwise keep trying.
        return False

    suspensions = []
    assignments = []

    # Track per-step per-pipeline scheduling (allow limited parallelism)
    scheduled_ops_per_pid = {}

    # Quick indicator of high-priority backlog
    hp_backlog = (len(s.wait_q[Priority.QUERY]) + len(s.wait_q[Priority.INTERACTIVE])) > 0

    def _pick_candidate_from_queue(q, pool, pri, avail_cpu, avail_ram, enforce_headroom):
        if not q:
            return None, None, None, None

        best_idx = None
        best = None

        scan_n = len(q)
        if scan_n > s.scan_limit:
            scan_n = s.scan_limit

        hc, hr = _headroom(pool)

        for i in range(scan_n):
            p = q[i]
            pid = p.pipeline_id

            if pid in s.finished_pipelines or pid in s.dead_pipelines:
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            try:
                if status.is_pipeline_successful():
                    _drop_pipeline(pid, mark_finished=True)
                    continue
            except Exception:
                pass

            if _should_kill_pipeline(p, pid):
                _drop_pipeline(pid, mark_dead=True)
                continue

            lim = s.max_ops_per_pipeline_per_step.get(pri, 1)
            if scheduled_ops_per_pid.get(pid, 0) >= lim:
                continue

            # Runnable ops (respect DAG)
            try:
                runnable = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                runnable = []
            if not runnable:
                continue

            op = runnable[0]

            # Size and fit
            cpu_req, ram_req = _size_request(pool, pri, pid, op, keep_small_for_concurrency=True)
            if cpu_req > avail_cpu or ram_req > avail_ram:
                continue

            # Enforce headroom only for batch when HP exists (to reduce HP queueing without preemption)
            if enforce_headroom and pri == Priority.BATCH_PIPELINE and hp_backlog:
                if (avail_cpu - cpu_req) < hc or (avail_ram - ram_req) < hr:
                    continue

            # Ranking: prefer pipelines closer to completion and/or currently idle (reduce timeouts)
            rem = _pipeline_remaining_ops(p, status)
            idle = _idle_ticks(pid)

            # Bias to finish started pipelines: if some ops already completed, reduce effective remaining
            done = _pipeline_completed_ops(status)
            started_factor = 0.80 if done > 0 else 1.00

            # Lower rank is better
            rank = (rem * 10.0) * started_factor - (min(idle, 800) * 0.06)

            # Tie-breaker: pack RAM a bit (prefer larger RAM if equal rank, to reduce fragmentation later)
            if best is None:
                best = (rank, -ram_req)
                best_idx = i
            else:
                cand = (rank, -ram_req)
                if cand < best:
                    best = cand
                    best_idx = i

        if best_idx is None:
            return None, None, None, None

        p = q.pop(best_idx)
        q.append(p)  # rotate for fairness
        pid = p.pipeline_id
        status = p.runtime_status()
        op = (status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or [None])[0]
        if op is None:
            return None, None, None, None

        cpu_req, ram_req = _size_request(pool, pri, pid, op, keep_small_for_concurrency=True)
        if cpu_req > avail_cpu or ram_req > avail_ram:
            return None, None, None, None

        # Re-check headroom constraint at the end (avail may have changed between scan and commit)
        if enforce_headroom and pri == Priority.BATCH_PIPELINE and hp_backlog:
            hc, hr = _headroom(pool)
            if (avail_cpu - cpu_req) < hc or (avail_ram - ram_req) < hr:
                return None, None, None, None

        return p, op, cpu_req, ram_req

    # --- Main scheduling loop per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if pool_id >= len(s.rr_ptr):
            s.rr_ptr.append(0)

        # If pool0 reserved and HP backlog exists, strongly bias to HP on pool0
        pool0_hp_only = (s.reserve_pool0_for_hp and pool_id == 0 and s.executor.num_pools > 1 and hp_backlog)

        # Fill the pool
        while avail_cpu >= 1 and avail_ram >= 1:
            scheduled = False

            start_ptr = s.rr_ptr[pool_id]
            tried = 0

            while tried < len(s.rr_seq):
                pri = s.rr_seq[s.rr_ptr[pool_id] % len(s.rr_seq)]
                s.rr_ptr[pool_id] = (s.rr_ptr[pool_id] + 1) % len(s.rr_seq)
                tried += 1

                if pool0_hp_only and pri == Priority.BATCH_PIPELINE:
                    continue

                q = _queue_for_priority(pri)
                p, op, cpu_req, ram_req = _pick_candidate_from_queue(
                    q=q,
                    pool=pool,
                    pri=pri,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    enforce_headroom=True,
                )
                if p is None:
                    continue

                pid = p.pipeline_id
                opk = _op_key(op)

                # Track attribution for adaptive hints
                s.op_to_pipeline[opk] = pid
                if pid not in s.pipeline_ops_seen:
                    s.pipeline_ops_seen[pid] = set()
                s.pipeline_ops_seen[pid].add(opk)

                # Update pipeline meta (for starvation/idle tracking)
                _ensure_pipeline_meta(pid)
                s.pipeline_meta[pid]["last_scheduled_tick"] = s.ticks

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)

                avail_cpu -= cpu_req
                avail_ram -= ram_req

                scheduled_ops_per_pid[pid] = scheduled_ops_per_pid.get(pid, 0) + 1
                scheduled = True
                break

            if not scheduled:
                # If we made no progress in RR loop, stop filling this pool
                # (prevents tight loop when only non-fitting tasks remain)
                break

    return suspensions, assignments
