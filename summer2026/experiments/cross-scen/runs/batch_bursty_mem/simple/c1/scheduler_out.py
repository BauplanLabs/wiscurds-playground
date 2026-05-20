@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved, completion-oriented, resource-packing scheduler.

    Key changes vs prior version:
      - Much smaller default CPU/RAM per op to increase concurrency and throughput.
      - Multi-pool partitioning: dedicate a fraction of pools to QUERY+INTERACTIVE.
      - No "blacklist on non-OOM": retries for timeout-like failures by increasing CPU;
        bounded retries for other errors.
      - Pipeline selection favors "fewest remaining ops" (complete more pipelines) + aging.
      - Light best-fit packing across runnable ops (when multiple are ready).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()
    s.pipeline_meta = {}  # pipeline_id -> {"enqueued_tick": int}

    # Result attribution + per-op retry hints
    s.op_to_pipeline = {}          # op_key -> pipeline_id
    s.op_ram_hint = {}             # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}             # (pipeline_id, op_key) -> cpu
    s.op_attempts = {}             # (pipeline_id, op_key) -> attempts

    # Pipeline-level guardrails
    s.dead_pipelines = set()       # give up scheduling these
    s.pipeline_failures = {}       # pipeline_id -> count (all failures)

    # Arrival timing for burst buffering / headroom logic
    s.last_hi_arrival_tick = 0
    s.last_query_arrival_tick = 0

    # Tuning knobs
    s.cleanup_every = 25
    s.scan_limit = 100
    s.max_attempts = 6
    s.max_other_attempts = 2
    s.hi_grace_ticks = 12

    # Default sizing (fractions of per-pool max). These are intentionally small to
    # increase parallelism on memory-bound workloads.
    s.base_cpu_frac = {
        Priority.QUERY: 0.15,            # ~9 on 64c
        Priority.INTERACTIVE: 0.10,      # ~6 on 64c
        Priority.BATCH_PIPELINE: 0.08,   # ~5 on 64c
    }
    s.base_ram_frac = {
        Priority.QUERY: 0.05,            # ~25GB on 500GB
        Priority.INTERACTIVE: 0.08,      # ~40GB on 500GB
        Priority.BATCH_PIPELINE: 0.08,   # ~40GB on 500GB
    }

    # Absolute minimums (units match simulator pools)
    s.min_cpu = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }
    s.min_ram = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 12,
    }

    # Relaxation for backfilling tiny fragments (only when we don't have explicit hints)
    s.relax_cpu_frac = 0.60
    s.relax_ram_frac = 0.70


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Use object identity; stable for the lifetime of the simulation object graph.
        return id(op)

    def _as_int(x, default=0):
        try:
            if x is None:
                return default
            if isinstance(x, bool):
                return int(x)
            if isinstance(x, int):
                return x
            return int(round(float(x)))
        except Exception:
            return default

    def _ceil_pos(x):
        # Small helper without imports; assumes x >= 0.
        xi = int(x)
        return xi if x == xi else xi + 1

    def _is_dead(pid):
        return pid in s.dead_pipelines

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - _as_int(meta.get("enqueued_tick"), s.ticks))

    def _error_kind(err):
        if err is None:
            return "other"
        msg = str(err).lower()
        if ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg):
            return "oom"
        if ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg) or ("time limit" in msg):
            return "timeout"
        return "other"

    def _pool_max_any():
        max_cpu = 1
        max_ram = 1
        for p in s.executor.pools:
            if p.max_cpu_pool > max_cpu:
                max_cpu = p.max_cpu_pool
            if p.max_ram_pool > max_ram:
                max_ram = p.max_ram_pool
        return max_cpu, max_ram

    def _base_size(pool, pri):
        cpu = _as_int(pool.max_cpu_pool * s.base_cpu_frac.get(pri, 0.08), 1)
        ram = _as_int(pool.max_ram_pool * s.base_ram_frac.get(pri, 0.08), 1)
        cpu = max(cpu, s.min_cpu.get(pri, 1), 1)
        ram = max(ram, s.min_ram.get(pri, 1), 1)
        cpu = min(cpu, _as_int(pool.max_cpu_pool, cpu))
        ram = min(ram, _as_int(pool.max_ram_pool, ram))
        return cpu, ram

    def _hinted_size(pool, pri, pid, opk):
        base_cpu, base_ram = _base_size(pool, pri)
        cpu = s.op_cpu_hint.get((pid, opk), base_cpu)
        ram = s.op_ram_hint.get((pid, opk), base_ram)
        cpu = max(_as_int(cpu, base_cpu), 1)
        ram = max(_as_int(ram, base_ram), 1)
        cpu = min(cpu, _as_int(pool.max_cpu_pool, cpu))
        ram = min(ram, _as_int(pool.max_ram_pool, ram))
        return cpu, ram, base_cpu, base_ram

    def _size_that_fits(pool, pri, pid, op, avail_cpu, avail_ram, strict):
        opk = _op_key(op)
        cpu, ram, base_cpu, base_ram = _hinted_size(pool, pri, pid, opk)

        has_cpu_hint = (pid, opk) in s.op_cpu_hint
        has_ram_hint = (pid, opk) in s.op_ram_hint

        if strict:
            if cpu > avail_cpu or ram > avail_ram:
                return None
            return cpu, ram

        # Relax only if we don't have explicit hints.
        min_cpu_allowed = s.min_cpu.get(pri, 1)
        min_ram_allowed = s.min_ram.get(pri, 1)

        if has_cpu_hint:
            min_cpu_allowed = max(min_cpu_allowed, cpu)
        else:
            min_cpu_allowed = max(min_cpu_allowed, _as_int(base_cpu * s.relax_cpu_frac, min_cpu_allowed))

        if has_ram_hint:
            min_ram_allowed = max(min_ram_allowed, ram)
        else:
            min_ram_allowed = max(min_ram_allowed, _as_int(base_ram * s.relax_ram_frac, min_ram_allowed))

        cpu2 = min(cpu, avail_cpu)
        ram2 = min(ram, avail_ram)

        if cpu2 < min_cpu_allowed or ram2 < min_ram_allowed:
            return None
        return cpu2, ram2

    def _total_ops(p):
        vals = getattr(p, "values", None)
        if vals is None:
            return 0
        try:
            return len(vals)
        except Exception:
            # Fallback: try to iterate
            try:
                return sum(1 for _ in vals)
            except Exception:
                return 0

    def _remaining_ops(p, status):
        total = _total_ops(p)
        if total <= 0:
            # Conservative fallback: count non-completed ops via states we can query
            try:
                completed = len(status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False))
                return max(0, total - completed)
            except Exception:
                return 999999
        try:
            completed = len(status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False))
            return max(0, total - completed)
        except Exception:
            return total

    def _queue_cleanup():
        # Filter out dead and already-successful pipelines to keep scans cheap.
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                continue
            new_q = []
            for p in q:
                pid = p.pipeline_id
                if _is_dead(pid):
                    continue
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _drop_pipeline(pid)
                        continue
                except Exception:
                    pass
                new_q.append(p)
            s.wait_q[pri] = new_q

    # ---- Ingest newly arrived pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        s.wait_q[p.priority].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.last_hi_arrival_tick = s.ticks
        if p.priority == Priority.QUERY:
            s.last_query_arrival_tick = s.ticks

    # ---- Process execution results (adaptive retries) ----
    max_cpu_any, max_ram_any = _pool_max_any()

    for r in results:
        ops = getattr(r, "ops", []) or []
        failed = False
        if hasattr(r, "failed"):
            try:
                failed = bool(r.failed())
            except Exception:
                failed = False

        if not ops:
            continue

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue

            if not failed:
                # Success: best-effort cleanup (helps memory footprint; correctness unaffected).
                s.op_attempts.pop((pid, opk), None)
                s.op_cpu_hint.pop((pid, opk), None)
                s.op_ram_hint.pop((pid, opk), None)
                continue

            kind = _error_kind(getattr(r, "error", None))
            s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1
            attempts = s.op_attempts.get((pid, opk), 0) + 1
            s.op_attempts[(pid, opk)] = attempts

            # Base off the actual allocation that failed, if available.
            alloc_cpu = _as_int(getattr(r, "cpu", None), 0)
            alloc_ram = _as_int(getattr(r, "ram", None), 0)

            prev_cpu = _as_int(s.op_cpu_hint.get((pid, opk), 0), 0)
            prev_ram = _as_int(s.op_ram_hint.get((pid, opk), 0), 0)

            if kind == "oom":
                base = alloc_ram if alloc_ram > 0 else (prev_ram if prev_ram > 0 else 1)
                new_ram = max(base * 2, prev_ram * 2 if prev_ram > 0 else 0, 1)
                if new_ram > max_ram_any:
                    new_ram = max_ram_any
                s.op_ram_hint[(pid, opk)] = new_ram

            elif kind == "timeout":
                # Increase CPU more gently than RAM (sublinear scaling); still converges.
                base = alloc_cpu if alloc_cpu > 0 else (prev_cpu if prev_cpu > 0 else 1)
                new_cpu = max(base + 1, _as_int(base * 1.7, base + 1), prev_cpu + 1 if prev_cpu > 0 else 0)
                if new_cpu > max_cpu_any:
                    new_cpu = max_cpu_any
                s.op_cpu_hint[(pid, opk)] = new_cpu

            else:
                # Unknown errors: allow a small number of retries; then give up.
                pass

            # Give up conditions (avoid infinite churn)
            if attempts >= s.max_attempts:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)
                continue

            if kind == "other" and attempts >= s.max_other_attempts:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)
                continue

            # If we've already hit near-max resources and still fail, declare dead.
            if kind == "oom" and s.op_ram_hint.get((pid, opk), 0) >= int(0.98 * max_ram_any) and attempts >= 2:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)
                continue
            if kind == "timeout" and s.op_cpu_hint.get((pid, opk), 0) >= int(0.98 * max_cpu_any) and attempts >= 2:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid)
                continue

    # Periodic cleanup to keep queues efficient
    if s.cleanup_every > 0 and (s.ticks % s.cleanup_every == 0):
        _queue_cleanup()

    suspensions = []
    assignments = []

    # ---- Pool partitioning ----
    num_pools = s.executor.num_pools
    q_len = len(s.wait_q[Priority.QUERY])
    i_len = len(s.wait_q[Priority.INTERACTIVE])
    b_len = len(s.wait_q[Priority.BATCH_PIPELINE])

    hi_backlog = q_len + i_len
    total_backlog = hi_backlog + b_len

    if num_pools <= 1:
        hi_count = 1
    else:
        hi_share = (float(hi_backlog) / float(total_backlog)) if total_backlog > 0 else 0.25
        # Keep some dedicated HI pools even if HI is small; cap so batch always has capacity.
        hi_frac = 0.15 + 0.70 * hi_share
        if hi_frac < 0.15:
            hi_frac = 0.15
        if hi_frac > 0.60:
            hi_frac = 0.60
        hi_count = _ceil_pos(num_pools * hi_frac)
        if b_len > 0 and hi_count >= num_pools:
            hi_count = num_pools - 1
        if hi_count < 1:
            hi_count = 1

    hi_pool_ids = list(range(0, hi_count))
    batch_pool_ids = list(range(hi_count, num_pools))

    # Sort each group by available RAM (helps fit large-memory retries)
    try:
        hi_pool_ids.sort(key=lambda pid: s.executor.pools[pid].avail_ram_pool, reverse=True)
        batch_pool_ids.sort(key=lambda pid: s.executor.pools[pid].avail_ram_pool, reverse=True)
    except Exception:
        pass

    pool_order = hi_pool_ids + batch_pool_ids

    # ---- Candidate picking ----
    def _pick_best_from_queue(pri, pool, avail_cpu, avail_ram, strict, scan_limit):
        q = s.wait_q[pri]
        if not q:
            return None

        n = len(q)
        limit = min(n, scan_limit)

        best = None
        best_idx = -1

        # Pre-calc for a small headroom on HI pools so queries can start quickly in bursts.
        query_base_cpu, query_base_ram = _base_size(pool, Priority.QUERY)
        keep_query_headroom = False
        if pri != Priority.QUERY:
            recent_query = (s.ticks - s.last_query_arrival_tick) <= s.hi_grace_ticks
            has_query_backlog = len(s.wait_q[Priority.QUERY]) > 0
            if (recent_query or has_query_backlog):
                keep_query_headroom = True

        for idx in range(limit):
            p = q[idx]
            pid = p.pipeline_id

            if _is_dead(pid):
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            try:
                if status.is_pipeline_successful():
                    s.dead_pipelines.discard(pid)
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass

            try:
                runnable = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                runnable = []

            if not runnable:
                continue

            # Consider a few runnable ops; prefer ones that fit with smallest footprint.
            best_local = None
            best_local_size = None
            consider_k = 3 if len(runnable) > 1 else 1

            for op in runnable[:consider_k]:
                opk = _op_key(op)

                # Skip if hints exceed this pool's max (can't ever fit here).
                if (pid, opk) in s.op_ram_hint and s.op_ram_hint[(pid, opk)] > pool.max_ram_pool:
                    continue
                if (pid, opk) in s.op_cpu_hint and s.op_cpu_hint[(pid, opk)] > pool.max_cpu_pool:
                    continue

                sized = _size_that_fits(pool, pri, pid, op, avail_cpu, avail_ram, strict)
                if sized is None:
                    continue
                cpu_req, ram_req = sized

                # Enforce query headroom on HI pools when we're scheduling non-query.
                if keep_query_headroom and (pool in [s.executor.pools[i] for i in hi_pool_ids]):
                    # Only enforce if we actually have enough resources to keep headroom.
                    if avail_cpu >= query_base_cpu * 2 and avail_ram >= query_base_ram * 2:
                        if (avail_cpu - cpu_req) < query_base_cpu or (avail_ram - ram_req) < query_base_ram:
                            continue

                # Prefer smaller ops to pack more, but also prioritize near-complete pipelines.
                if best_local is None:
                    best_local = op
                    best_local_size = (cpu_req, ram_req)
                else:
                    # Prefer smaller RAM first, then smaller CPU
                    if ram_req < best_local_size[1] or (ram_req == best_local_size[1] and cpu_req < best_local_size[0]):
                        best_local = op
                        best_local_size = (cpu_req, ram_req)

            if best_local is None:
                continue

            cpu_req, ram_req = best_local_size
            wait = _pipeline_wait_ticks(pid)
            rem = _remaining_ops(p, status)
            attempts = s.op_attempts.get((pid, _op_key(best_local)), 0)

            # Rank: fewer remaining ops -> older -> more retry attempts (finish retries) -> smaller footprint.
            rank = (rem, -wait, -attempts, ram_req, cpu_req)

            if best is None or rank < best[0]:
                best = (rank, p, best_local, cpu_req, ram_req)
                best_idx = idx

        if best is None:
            return None

        # Rotate chosen pipeline to the end for fairness without losing its place entirely.
        try:
            chosen_p = q.pop(best_idx)
            q.append(chosen_p)
        except Exception:
            pass

        _, p, op, cpu_req, ram_req = best
        return p, op, cpu_req, ram_req

    def _ready_count_approx(pri, cap=80):
        q = s.wait_q[pri]
        if not q:
            return 0
        cnt = 0
        limit = min(len(q), cap)
        for idx in range(limit):
            p = q[idx]
            pid = p.pipeline_id
            if _is_dead(pid):
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
                if ops:
                    cnt += 1
            except Exception:
                continue
        return cnt

    # Approximate ready pressure to balance INTERACTIVE vs BATCH fairly.
    ready_q = _ready_count_approx(Priority.QUERY, cap=60)
    ready_i = _ready_count_approx(Priority.INTERACTIVE, cap=120)
    ready_b = _ready_count_approx(Priority.BATCH_PIPELINE, cap=200)

    # ---- Main scheduling loop ----
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = _as_int(pool.avail_cpu_pool, 0)
        avail_ram = _as_int(pool.avail_ram_pool, 0)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pool_is_hi = pool_id in set(hi_pool_ids)

        # Keep filling the pool.
        while avail_cpu >= 1 and avail_ram >= 1:
            picked = None

            # 1) Always try QUERY first (strict, then relaxed).
            if ready_q > 0 or len(s.wait_q[Priority.QUERY]) > 0:
                picked = _pick_best_from_queue(Priority.QUERY, pool, avail_cpu, avail_ram, True, s.scan_limit)
                if picked is None:
                    picked = _pick_best_from_queue(Priority.QUERY, pool, avail_cpu, avail_ram, False, s.scan_limit)

            if picked is None:
                # 2) Choose between INTERACTIVE and BATCH by outstanding weighted demand.
                # Use approximate "ready counts" to avoid biasing toward blocked pipelines.
                # Total objective weights roughly balance INTERACTIVE vs BATCH in this workload,
                # so use queue-weighted fairness, not strict priority.
                i_demand = (ready_i if ready_i > 0 else i_len) * 5
                b_demand = (ready_b if ready_b > 0 else b_len) * 1

                if pool_is_hi and (ready_i > 0 or i_len > 0):
                    pri_order = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
                else:
                    pri_order = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE] if i_demand >= b_demand else [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

                # Strict pass first, then relaxed backfill.
                for strict in (True, False):
                    for pri in pri_order:
                        # On HI pools, avoid batch if there is any interactive backlog (unless nothing fits).
                        if pool_is_hi and pri == Priority.BATCH_PIPELINE and (ready_i > 0 or i_len > 0):
                            continue
                        cand = _pick_best_from_queue(pri, pool, avail_cpu, avail_ram, strict, s.scan_limit)
                        if cand is not None:
                            picked = cand
                            break
                    if picked is not None:
                        break

                # If still nothing on HI pools and no HI backlog, allow batch as backfill.
                if picked is None and pool_is_hi and (ready_i == 0 and i_len == 0) and (ready_b > 0 or b_len > 0):
                    picked = _pick_best_from_queue(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram, True, s.scan_limit)
                    if picked is None:
                        picked = _pick_best_from_queue(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram, False, s.scan_limit)

            if picked is None:
                break

            p, op, cpu_req, ram_req = picked

            # Final safety clamps
            cpu_req = max(1, min(_as_int(cpu_req, 1), avail_cpu))
            ram_req = max(1, min(_as_int(ram_req, 1), avail_ram))

            if cpu_req < 1 or ram_req < 1:
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
            s.op_to_pipeline[_op_key(op)] = p.pipeline_id

            avail_cpu -= cpu_req
            avail_ram -= ram_req

        # end while fill pool

    return suspensions, assignments
