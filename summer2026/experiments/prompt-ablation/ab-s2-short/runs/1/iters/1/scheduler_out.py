@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.q_new = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_started = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    s.enqueued = set()
    s.dead_pipelines = set()

    # pipeline_id -> {"enqueued_tick": int}
    s.pipeline_meta = {}

    # pipeline_id -> {"total": int, "completed": int}
    s.pipeline_prog = {}

    # pipeline_id -> bool (has at least one successful op completion observed)
    s.started_pipelines = set()

    # pipeline_id -> failures count (non-OOM); used for blacklisting truly broken pipelines
    s.non_oom_failures = {}

    # (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}

    # op_key -> pipeline_id (for attributing results)
    s.op_to_pipeline = {}

    # (pipeline_id, op_key) -> (pool_id, priority, cpu, ram) inflight accounting
    s.op_inflight = {}

    # per-pool inflight totals by priority (kept in sync with op_inflight)
    s.pool_cpu_inflight = []
    s.pool_ram_inflight = []

    # per-pool, per-priority mixing counter to occasionally admit "new" pipelines even when many are started
    s.pool_mix_ctr = []

    # knobs
    s.scan_limit_started = 32
    s.scan_limit_new = 24
    s.max_non_oom_failures = 4
    s.max_assignments_per_step = 6000

    # CPU share targets (work-conserving; QUERY is always opportunistic above target)
    s.target_cpu_share = {
        Priority.QUERY: 0.10,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.55,
    }


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _ensure_pool_state():
        n = s.executor.num_pools
        if len(s.pool_cpu_inflight) != n:
            s.pool_cpu_inflight = []
            s.pool_ram_inflight = []
            s.pool_mix_ctr = []
            for _ in range(n):
                s.pool_cpu_inflight.append(
                    {
                        Priority.QUERY: 0.0,
                        Priority.INTERACTIVE: 0.0,
                        Priority.BATCH_PIPELINE: 0.0,
                    }
                )
                s.pool_ram_inflight.append(
                    {
                        Priority.QUERY: 0.0,
                        Priority.INTERACTIVE: 0.0,
                        Priority.BATCH_PIPELINE: 0.0,
                    }
                )
                s.pool_mix_ctr.append(
                    {
                        Priority.QUERY: 0,
                        Priority.INTERACTIVE: 0,
                        Priority.BATCH_PIPELINE: 0,
                    }
                )

    def _estimate_total_ops(p):
        vals = getattr(p, "values", None)
        try:
            if isinstance(vals, dict):
                return max(1, len(vals))
            if isinstance(vals, list) or isinstance(vals, tuple):
                return max(1, len(vals))
        except Exception:
            pass
        # Fallback: unknown, but pipelines are small (<=14); use a conservative default to still enable "remaining" ordering.
        return 10

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_prog.pop(pid, None)
        s.non_oom_failures.pop(pid, None)
        s.started_pipelines.discard(pid)

    def _queue_len(pri):
        return len(s.q_started[pri]) + len(s.q_new[pri])

    def _any_work(pri):
        return _queue_len(pri) > 0

    def _base_sizes(pool, pri, backlog_total):
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU: fixed-ish slices so bigger clusters increase parallelism (avoids constant-concurrency across cluster sizes).
        if max_cpu >= 64:
            cpu_q, cpu_i, cpu_b = 8, 4, 2
        elif max_cpu >= 32:
            cpu_q, cpu_i, cpu_b = 6, 3, 1
        elif max_cpu >= 16:
            cpu_q, cpu_i, cpu_b = 4, 2, 1
        else:
            cpu_q, cpu_i, cpu_b = 2, 1, 1

        # If global backlog is very large, bias toward more parallelism for background work.
        if backlog_total >= 2500 and max_cpu >= 64:
            cpu_b = 1
        if backlog_total >= 3500 and max_cpu >= 64:
            cpu_i = 3

        # RAM: modest absolute defaults; OOM will quickly bump per-op via hints.
        # Keep fairly safe for interactive/query; batch starts smaller.
        ram_q = 12.0
        ram_i = 10.0
        ram_b = 7.0

        # Clip to pool capacity
        ram_q = min(ram_q, float(max_ram))
        ram_i = min(ram_i, float(max_ram))
        ram_b = min(ram_b, float(max_ram))

        if pri == Priority.QUERY:
            return float(cpu_q), float(ram_q)
        if pri == Priority.INTERACTIVE:
            return float(cpu_i), float(ram_i)
        return float(cpu_b), float(ram_b)

    def _remaining_ops_est(pid):
        prog = s.pipeline_prog.get(pid)
        if not prog:
            return 999999
        total = prog.get("total", 0) or 0
        comp = prog.get("completed", 0) or 0
        if total <= 0:
            return 999999
        rem = total - comp
        if rem < 1:
            rem = 1
        return rem

    def _cleanup_queues_light(pri, max_checks=64):
        # Light cleanup to keep queues from accumulating completed/dead pipelines.
        # Only scans a prefix of each queue to keep overhead bounded.
        for which in ("q_started", "q_new"):
            q = getattr(s, which)[pri]
            if not q:
                continue
            checks = min(len(q), max_checks)
            kept = []
            for i in range(checks):
                p = q[i]
                pid = p.pipeline_id
                if pid in s.dead_pipelines:
                    _drop_pipeline(pid)
                    continue
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(pid)
                        continue
                except Exception:
                    pass
                kept.append(p)
            if checks < len(q):
                kept.extend(q[checks:])
            getattr(s, which)[pri] = kept

    def _pick_from_queue_best(q, pri, pool, avail_cpu, avail_ram, scheduled_ops, scheduled_per_pipeline, max_ops_per_pipeline, base_cpu, base_ram):
        # Best-of-scan selection: prefer pipelines already started and closer to completion.
        if not q:
            return None

        best_idx = None
        best_pick = None  # (rank_tuple, pipeline, op, cpu_req, ram_req)

        scan = min(len(q), s.scan_limit_started if pri != Priority.BATCH_PIPELINE else s.scan_limit_new)
        for idx in range(scan):
            p = q[idx]
            pid = p.pipeline_id

            if pid in s.dead_pipelines:
                continue

            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                st = None

            # Per-pipeline limit per scheduler step
            if scheduled_per_pipeline.get(pid, 0) >= max_ops_per_pipeline:
                continue

            if st is None:
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not ops:
                continue

            # Pick the first runnable op not already scheduled in this scheduler step
            op = None
            opk = None
            for cand in ops:
                ck = _op_key(cand)
                if ck not in scheduled_ops:
                    op = cand
                    opk = ck
                    break
            if op is None:
                continue

            # Size request
            hint = s.op_ram_hint.get((pid, opk), None)
            ram_req = float(hint) if (hint is not None and hint > 0) else float(base_ram)
            ram_req = min(ram_req, float(pool.max_ram_pool))
            if ram_req < 1.0:
                ram_req = 1.0

            cpu_req = float(base_cpu)
            if cpu_req < 1.0:
                cpu_req = 1.0
            if cpu_req > float(pool.max_cpu_pool):
                cpu_req = float(pool.max_cpu_pool)

            # Fit check (RAM must fit; CPU can be downsized to availability but not < 1)
            if avail_ram < ram_req:
                continue
            if avail_cpu < 1.0:
                continue
            if avail_cpu < cpu_req:
                cpu_req = float(avail_cpu)
                if cpu_req < 1.0:
                    continue

            started_flag = 0 if (pid in s.started_pipelines) else 1
            rem = _remaining_ops_est(pid)
            meta = s.pipeline_meta.get(pid, None)
            age = (s.ticks - meta.get("enqueued_tick", s.ticks)) if meta else 0
            # Rank: started first, then fewer remaining ops, then older, then smaller RAM.
            rank = (started_flag, rem, -age, ram_req)

            if best_pick is None or rank < best_pick[0]:
                best_pick = (rank, p, op, cpu_req, ram_req)
                best_idx = idx

        if best_pick is None:
            return None

        # Rotate selected pipeline to the back (round-robin among active candidates)
        p = q.pop(best_idx)
        q.append(p)
        _, p, op, cpu_req, ram_req = best_pick
        return p, op, cpu_req, ram_req

    def _pick_pipeline_op(pri, pool, pool_id, avail_cpu, avail_ram, scheduled_ops, scheduled_per_pipeline, base_cpu, base_ram):
        # Per-step max ops per pipeline (lets parallel branches run, but avoids flooding a single DAG)
        if pri == Priority.QUERY:
            max_ops_per_pipeline = 2
        elif pri == Priority.INTERACTIVE:
            max_ops_per_pipeline = 2
        else:
            max_ops_per_pipeline = 1

        # Prefer started pipelines, but occasionally admit new ones to avoid never-starting starvation.
        # k means: do k picks from started, then 1 from new (if new exists).
        if pri == Priority.QUERY:
            k = 3
        elif pri == Priority.INTERACTIVE:
            k = 4
        else:
            k = 6

        started_q = s.q_started[pri]
        new_q = s.q_new[pri]

        # Lazy migration: if a pipeline is marked started but still sits in new queue, migrate when encountered.
        # We'll do a small migration pass on the head of new_q for amortized cost.
        mig_scan = min(len(new_q), 12)
        if mig_scan > 0:
            kept = []
            for i in range(mig_scan):
                p = new_q[i]
                pid = p.pipeline_id
                if pid in s.started_pipelines:
                    started_q.append(p)
                else:
                    kept.append(p)
            if mig_scan < len(new_q):
                kept.extend(new_q[mig_scan:])
            s.q_new[pri] = kept
            new_q = s.q_new[pri]

        use_started = True
        if new_q and started_q:
            ctr = s.pool_mix_ctr[pool_id][pri]
            if ctr >= k:
                use_started = False

        picked = None
        if use_started and started_q:
            picked = _pick_from_queue_best(
                started_q, pri, pool, avail_cpu, avail_ram,
                scheduled_ops, scheduled_per_pipeline, max_ops_per_pipeline,
                base_cpu, base_ram
            )
            if picked is not None:
                s.pool_mix_ctr[pool_id][pri] = s.pool_mix_ctr[pool_id][pri] + 1
                return picked

        # Reset counter when we pull from new (or if started empty)
        if new_q:
            picked = _pick_from_queue_best(
                new_q, pri, pool, avail_cpu, avail_ram,
                scheduled_ops, scheduled_per_pipeline, max_ops_per_pipeline,
                base_cpu, base_ram
            )
            if picked is not None:
                s.pool_mix_ctr[pool_id][pri] = 0
                return picked

        # Fallback: if we tried new and failed but started exists, try started anyway
        if started_q:
            picked = _pick_from_queue_best(
                started_q, pri, pool, avail_cpu, avail_ram,
                scheduled_ops, scheduled_per_pipeline, max_ops_per_pipeline,
                base_cpu, base_ram
            )
            if picked is not None:
                s.pool_mix_ctr[pool_id][pri] = min(s.pool_mix_ctr[pool_id][pri] + 1, k + 1)
                return picked

        return None

    _ensure_pool_state()

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        pri = p.priority
        s.q_new[pri].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.pipeline_prog[pid] = {"total": _estimate_total_ops(p), "completed": 0}

    # Process results: update inflight accounting and OOM-derived RAM hints
    for r in results:
        failed = (hasattr(r, "failed") and r.failed())
        oom = _is_oom_error(getattr(r, "error", None)) if failed else False

        for op in (getattr(r, "ops", []) or []):
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            # Remove inflight accounting for this op (if we recorded it)
            infl_key = (pid, opk)
            infl = s.op_inflight.pop(infl_key, None)
            if infl is not None:
                pool_id, pri, cpu_amt, ram_amt = infl
                s.pool_cpu_inflight[pool_id][pri] = max(0.0, s.pool_cpu_inflight[pool_id][pri] - float(cpu_amt))
                s.pool_ram_inflight[pool_id][pri] = max(0.0, s.pool_ram_inflight[pool_id][pri] - float(ram_amt))

            if not failed:
                # Mark pipeline as started and update progress
                s.started_pipelines.add(pid)
                prog = s.pipeline_prog.get(pid)
                if prog is not None:
                    prog["completed"] = (prog.get("completed", 0) or 0) + 1
            else:
                if oom:
                    prev = s.op_ram_hint.get((pid, opk), None)
                    alloc = getattr(r, "ram", None)
                    base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev) if (prev is not None and prev > 0) else 1.0)
                    new_hint = max(base * 2.0, (float(prev) * 1.7 if (prev is not None and prev > 0) else 0.0), 1.0)
                    s.op_ram_hint[(pid, opk)] = new_hint
                else:
                    cnt = s.non_oom_failures.get(pid, 0) + 1
                    s.non_oom_failures[pid] = cnt
                    if cnt >= s.max_non_oom_failures:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

    # Periodic light cleanup
    if (s.ticks % 17) == 0 or (len(results) > 0 and (s.ticks % 5) == 0):
        _cleanup_queues_light(Priority.QUERY, max_checks=96)
        _cleanup_queues_light(Priority.INTERACTIVE, max_checks=96)
        _cleanup_queues_light(Priority.BATCH_PIPELINE, max_checks=96)

    suspensions = []
    assignments = []

    scheduled_ops = set()
    scheduled_per_pipeline = {}

    backlog_total = _queue_len(Priority.QUERY) + _queue_len(Priority.INTERACTIVE) + _queue_len(Priority.BATCH_PIPELINE)

    # Scheduling
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        infl_cpu = s.pool_cpu_inflight[pool_id]
        planned_cpu = {
            Priority.QUERY: 0.0,
            Priority.INTERACTIVE: 0.0,
            Priority.BATCH_PIPELINE: 0.0,
        }

        have_q = _any_work(Priority.QUERY)
        have_i = _any_work(Priority.INTERACTIVE)
        have_b = _any_work(Priority.BATCH_PIPELINE)

        # Headroom to keep at least one QUERY and one INTERACTIVE admission possible when they exist.
        # This helps avoid "fill with batch then can't preempt" pathologies.
        base_cpu_q, base_ram_q = _base_sizes(pool, Priority.QUERY, backlog_total)
        base_cpu_i, base_ram_i = _base_sizes(pool, Priority.INTERACTIVE, backlog_total)
        base_cpu_b, base_ram_b = _base_sizes(pool, Priority.BATCH_PIPELINE, backlog_total)

        headroom_cpu = 0.0
        headroom_ram = 0.0
        if have_q:
            headroom_cpu += min(base_cpu_q, float(pool.max_cpu_pool))
            headroom_ram += min(base_ram_q, float(pool.max_ram_pool))
        if have_i:
            headroom_cpu += min(base_cpu_i, float(pool.max_cpu_pool))
            headroom_ram += min(base_ram_i, float(pool.max_ram_pool))

        # Soft caps to prevent batch from saturating the whole pool when higher priorities are present
        max_cpu = float(pool.max_cpu_pool)
        batch_cpu_cap = (0.72 * max_cpu) if (have_q or have_i) else max_cpu
        инт_cpu_cap = (0.80 * max_cpu) if have_b else max_cpu  # interactive cap only when batch exists

        # Work-conserving scheduling loop
        while avail_cpu >= 1.0 and avail_ram >= 1.0 and len(assignments) < s.max_assignments_per_step:
            # 1) Always try QUERY first (if any), since it's highest priority
            picked = _pick_pipeline_op(
                Priority.QUERY, pool, pool_id, avail_cpu, avail_ram,
                scheduled_ops, scheduled_per_pipeline, base_cpu_q, base_ram_q
            )
            if picked is not None:
                p, op, cpu_req, ram_req = picked
                pid = p.pipeline_id
                opk = _op_key(op)

                assignment = Assignment(
                    ops=[op],
                    cpu=float(cpu_req),
                    ram=float(ram_req),
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)

                s.op_to_pipeline[opk] = pid
                s.op_inflight[(pid, opk)] = (pool_id, p.priority, float(cpu_req), float(ram_req))
                s.pool_cpu_inflight[pool_id][p.priority] += float(cpu_req)
                s.pool_ram_inflight[pool_id][p.priority] += float(ram_req)

                scheduled_ops.add(opk)
                scheduled_per_pipeline[pid] = scheduled_per_pipeline.get(pid, 0) + 1

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                planned_cpu[p.priority] += float(cpu_req)
                continue

            # 2) Choose between INTERACTIVE and BATCH based on CPU-share deficits vs targets, with soft caps.
            # Compute current+planned usage fractions
            cur_i = (infl_cpu[Priority.INTERACTIVE] + planned_cpu[Priority.INTERACTIVE]) / max(1.0, max_cpu)
            cur_b = (infl_cpu[Priority.BATCH_PIPELINE] + planned_cpu[Priority.BATCH_PIPELINE]) / max(1.0, max_cpu)

            def_i = s.target_cpu_share[Priority.INTERACTIVE] - cur_i
            def_b = s.target_cpu_share[Priority.BATCH_PIPELINE] - cur_b

            # Apply soft caps when the other class exists
            if have_b and (infl_cpu[Priority.INTERACTIVE] + planned_cpu[Priority.INTERACTIVE]) >= инт_cpu_cap:
                def_i = -999.0
            if (have_q or have_i) and (infl_cpu[Priority.BATCH_PIPELINE] + planned_cpu[Priority.BATCH_PIPELINE]) >= batch_cpu_cap:
                def_b = -999.0

            # Decide order
            if def_i >= def_b:
                first, second = Priority.INTERACTIVE, Priority.BATCH_PIPELINE
            else:
                first, second = Priority.BATCH_PIPELINE, Priority.INTERACTIVE

            made_progress = False
            for pri_try in (first, second):
                if pri_try == Priority.INTERACTIVE and not have_i:
                    continue
                if pri_try == Priority.BATCH_PIPELINE and not have_b:
                    continue

                base_cpu, base_ram = (base_cpu_i, base_ram_i) if pri_try == Priority.INTERACTIVE else (base_cpu_b, base_ram_b)

                # Guard: don't schedule batch into the last headroom slice when higher-priority work exists
                if pri_try == Priority.BATCH_PIPELINE and (have_q or have_i):
                    # If we'd consume headroom needed for query/interactive, stop scheduling batch for now.
                    if (avail_cpu - 1.0) < max(0.0, headroom_cpu - 1.0) or (avail_ram - 1.0) < max(0.0, headroom_ram - 1.0):
                        continue

                picked2 = _pick_pipeline_op(
                    pri_try, pool, pool_id, avail_cpu, avail_ram,
                    scheduled_ops, scheduled_per_pipeline, base_cpu, base_ram
                )
                if picked2 is None:
                    continue

                p, op, cpu_req, ram_req = picked2
                pid = p.pipeline_id
                opk = _op_key(op)

                assignment = Assignment(
                    ops=[op],
                    cpu=float(cpu_req),
                    ram=float(ram_req),
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)

                s.op_to_pipeline[opk] = pid
                s.op_inflight[(pid, opk)] = (pool_id, p.priority, float(cpu_req), float(ram_req))
                s.pool_cpu_inflight[pool_id][p.priority] += float(cpu_req)
                s.pool_ram_inflight[pool_id][p.priority] += float(ram_req)

                scheduled_ops.add(opk)
                scheduled_per_pipeline[pid] = scheduled_per_pipeline.get(pid, 0) + 1

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                planned_cpu[p.priority] += float(cpu_req)

                made_progress = True
                break

            if not made_progress:
                break

    return suspensions, assignments
