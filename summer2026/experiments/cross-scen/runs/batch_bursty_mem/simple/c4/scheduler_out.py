@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on:
      - Much higher concurrency (smaller default CPU/RAM per op) to avoid deadline/timeout collapse.
      - Strong protection for QUERY/INTERACTIVE via soft reservations + optional "latency pools".
      - Failure-adaptive retries:
          * OOM -> increase RAM hint
          * timeout -> increase CPU hint
      - Completion-rate optimization: within a priority, prefer pipelines closer to finishing (fewer remaining ops),
        while still honoring aging to prevent starvation.
    """
    s.ticks = 0

    # Per-priority round-robin queues of Pipeline objects.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Bookkeeping
    s.enqueued = set()          # pipeline_id currently tracked/queued
    s.finished = set()          # pipeline_id completed successfully (best-effort)
    s.pipeline_meta = {}        # pipeline_id -> {"enqueued_tick": int, "last_progress_tick": int}

    # Per-op adaptive hints
    # Keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_fail_count = {}

    # Map op_key -> pipeline_id to attribute ExecutionResult to a pipeline.
    s.op_to_pipeline = {}

    # High-priority burst handling
    s.last_high_arrival_tick = -10**9
    s.high_hold_ticks = 6  # keep headroom for a few ticks after any QUERY/INTERACTIVE arrival

    # Scheduling knobs
    s.scan_limit = 40  # max pipelines to scan per pick (per priority, per pool)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Default sizing (absolute RAM in "pool units", typically GB; CPU in vCPU).
    # These are intentionally modest to increase concurrency; OOM/timeout adapt upward quickly.
    s.base_ram_abs = {
        Priority.QUERY: 28.0,
        Priority.INTERACTIVE: 64.0,
        Priority.BATCH_PIPELINE: 56.0,
    }
    s.base_ram_frac_cap = {
        Priority.QUERY: 0.25,        # never give a single query op >25% of a pool by default
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # CPU fractions of a pool (rounded), plus caps to avoid wasting on sublinear scaling.
    s.base_cpu_frac = {
        Priority.QUERY: 0.25,        # 64 -> 16
        Priority.INTERACTIVE: 0.19,  # 64 -> 12
        Priority.BATCH_PIPELINE: 0.125,  # 64 -> 8
    }
    s.base_cpu_cap = {
        Priority.QUERY: 24.0,
        Priority.INTERACTIVE: 20.0,
        Priority.BATCH_PIPELINE: 16.0,
    }
    s.min_cpu = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Retry adaptation factors
    s.oom_ram_mult = 1.85
    s.timeout_cpu_mult = 1.55
    s.other_bump_mult = 1.20

    # Soft reservations (when high-priority present/recent):
    # Keep some headroom so QUERY/INTERACTIVE don't get stuck behind long batch ops.
    s.reserve_cpu_frac = 0.18
    s.reserve_ram_frac = 0.18

    # Batch aging (anti-starvation)
    s.batch_starve_ticks = 55


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    def _pipeline_wait_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _pipeline_stale_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("last_progress_tick", meta.get("enqueued_tick", s.ticks)))

    def _remaining_ops_estimate(p, status):
        # Estimate remaining work: count non-completed operators if possible.
        try:
            remaining_states = {
                OperatorState.PENDING,
                OperatorState.FAILED,
                OperatorState.ASSIGNED,
                OperatorState.RUNNING,
                OperatorState.SUSPENDING,
            }
            rem = len(status.get_ops(remaining_states, require_parents_complete=False))
            if rem > 0:
                return rem
        except Exception:
            pass

        # Fallback: use total - completed if pipeline.values is sized.
        try:
            total = len(getattr(p, "values", []))
            if total <= 0:
                return 1
            completed = len(status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False))
            return max(1, total - completed)
        except Exception:
            return 1

    def _base_cpu(pool, pri):
        max_cpu = float(pool.max_cpu_pool)
        base = max(s.min_cpu[pri], round(max_cpu * float(s.base_cpu_frac[pri])))
        base = min(base, float(s.base_cpu_cap[pri]), max_cpu)
        return max(1.0, base)

    def _base_ram(pool, pri):
        max_ram = float(pool.max_ram_pool)
        base_abs = float(s.base_ram_abs[pri])
        frac_cap = float(s.base_ram_frac_cap[pri])
        base = min(base_abs, max_ram * frac_cap, max_ram)
        return max(1.0, base)

    def _size_request(pool, pri, pid, op, remaining_ops):
        opk = _op_key(op)
        key = (pid, opk)

        # Start from modest defaults.
        cpu_req = _base_cpu(pool, pri)
        ram_req = _base_ram(pool, pri)

        # Apply hints from past failures.
        hinted_cpu = s.op_cpu_hint.get(key, None)
        hinted_ram = s.op_ram_hint.get(key, None)
        if hinted_cpu is not None:
            cpu_req = max(cpu_req, float(hinted_cpu))
        if hinted_ram is not None:
            ram_req = max(ram_req, float(hinted_ram))

        # If repeated failures, nudge both up slightly.
        fails = int(s.op_fail_count.get(key, 0))
        if fails >= 2:
            cpu_req *= 1.10
            ram_req *= 1.15

        # Completion bias: if close to finishing, give a bit more CPU to reduce end-to-end latency.
        if remaining_ops <= 2:
            cpu_req *= 1.25

        # Clip to pool limits.
        cpu_req = min(max(1.0, cpu_req), float(pool.max_cpu_pool))
        ram_req = min(max(1.0, ram_req), float(pool.max_ram_pool))

        return cpu_req, ram_req, opk

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.finished:
            continue
        if pid in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_progress_tick": s.ticks}
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.last_high_arrival_tick = s.ticks

    # --- Process results (adaptive hints) ---
    for r in results:
        failed = (hasattr(r, "failed") and r.failed())
        ops = getattr(r, "ops", []) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            # Mark progress if succeeded.
            if not failed:
                meta = s.pipeline_meta.get(pid)
                if meta is not None:
                    meta["last_progress_tick"] = s.ticks
                continue

            # Failed: update fail count and hints
            key = (pid, opk)
            s.op_fail_count[key] = int(s.op_fail_count.get(key, 0)) + 1

            err = getattr(r, "error", None)

            if _is_oom_error(err):
                prev = s.op_ram_hint.get(key, None)
                alloc = getattr(r, "ram", None)
                base = None
                if alloc is not None and float(alloc) > 0:
                    base = float(alloc)
                elif prev is not None:
                    base = float(prev)
                else:
                    # If we don't know, start from a conservative bump above the default.
                    # Use priority as a proxy.
                    pri = getattr(r, "priority", Priority.BATCH_PIPELINE)
                    base = float(s.base_ram_abs.get(pri, 56.0))

                new_hint = max(base * float(s.oom_ram_mult), (float(prev) * float(s.oom_ram_mult) if prev is not None else 0.0))
                s.op_ram_hint[key] = new_hint

            elif _is_timeout_error(err):
                prev = s.op_cpu_hint.get(key, None)
                alloc = getattr(r, "cpu", None)
                base = None
                if alloc is not None and float(alloc) > 0:
                    base = float(alloc)
                elif prev is not None:
                    base = float(prev)
                else:
                    pri = getattr(r, "priority", Priority.BATCH_PIPELINE)
                    base = float(s.min_cpu.get(pri, 2.0))

                new_hint = max(base * float(s.timeout_cpu_mult), (float(prev) * float(s.timeout_cpu_mult) if prev is not None else 0.0))
                s.op_cpu_hint[key] = new_hint

            else:
                # Unknown failure: mild bump to improve chances without exploding allocations.
                prev_ram = s.op_ram_hint.get(key, None)
                prev_cpu = s.op_cpu_hint.get(key, None)
                if prev_ram is not None:
                    s.op_ram_hint[key] = float(prev_ram) * float(s.other_bump_mult)
                if prev_cpu is not None:
                    s.op_cpu_hint[key] = float(prev_cpu) * float(s.other_bump_mult)

    # --- Helpers for backlog / reservations ---
    def _has_unfinished_in_queue(q):
        # Best-effort cleanup / detection without full scans.
        # Scan a small prefix to keep this O(1) average.
        scan = min(len(q), 25)
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id
            if pid in s.finished:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    s.finished.add(pid)
                    _drop_pipeline(pid)
                    continue
            except Exception:
                pass
            return True
        # If queue is large, assume non-empty rather than miss backlog (conservative).
        return len(q) > 0

    high_waiting = _has_unfinished_in_queue(s.wait_q[Priority.QUERY]) or _has_unfinished_in_queue(s.wait_q[Priority.INTERACTIVE])
    reserve_active = high_waiting or ((s.ticks - s.last_high_arrival_tick) <= int(s.high_hold_ticks))

    # Dedicated latency pools when we have multiple pools.
    num_pools = int(s.executor.num_pools)
    if num_pools >= 2:
        latency_pool_count = max(1, num_pools // 4)
        latency_pools = set(range(latency_pool_count))
    else:
        latency_pools = set()

    suspensions = []
    assignments = []

    # Tick-local anti-duplication / fairness
    scheduled_ops = set()  # op_key scheduled this tick
    scheduled_count_by_pipeline = {}  # pid -> count scheduled this tick

    # --- Candidate picker ---
    def _pick_candidate(pri, pool, avail_cpu, avail_ram, allow_batch_here, reserve_cpu, reserve_ram):
        q = _queue_for_priority(pri)
        if not q:
            return None

        scan = min(int(s.scan_limit), len(q))
        best = None  # (sort_key, pipeline, op, cpu_req, ram_req, opk)

        for _ in range(scan):
            p = q.pop(0)
            pid = p.pipeline_id

            # Drop if finished
            if pid in s.finished:
                continue

            # Ensure we keep pipeline around unless definitively done
            try:
                status = p.runtime_status()
            except Exception:
                q.append(p)
                continue

            if status.is_pipeline_successful():
                s.finished.add(pid)
                _drop_pipeline(pid)
                continue

            # Batch anti-starvation: if batch has waited very long, treat it as slightly more urgent.
            wait_ticks = _pipeline_wait_ticks(pid)
            stale_ticks = _pipeline_stale_ticks(pid)
            age = max(wait_ticks, stale_ticks)

            # Per-pipeline per-tick cap (prevents one pipeline dominating when DAG branches)
            cap = int(s.max_ops_per_pipeline_per_tick.get(pri, 1))
            used = int(scheduled_count_by_pipeline.get(pid, 0))
            if used >= cap:
                q.append(p)
                continue

            # Find an assignable op that we haven't already scheduled this tick
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                q.append(p)
                continue

            chosen_op = None
            chosen_opk = None
            for cand in op_list:
                candk = _op_key(cand)
                if candk in scheduled_ops:
                    continue
                chosen_op = cand
                chosen_opk = candk
                break

            if chosen_op is None:
                q.append(p)
                continue

            remaining_ops = _remaining_ops_estimate(p, status)
            cpu_req, ram_req, opk = _size_request(pool, pri, pid, chosen_op, remaining_ops)

            # Batch gating on latency pools / reservations
            if pri == Priority.BATCH_PIPELINE:
                if not allow_batch_here:
                    q.append(p)
                    continue
                if reserve_active:
                    if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                        # Try to keep headroom for high priority.
                        # Allow very old batch to break through to avoid total starvation.
                        if age < int(s.batch_starve_ticks):
                            q.append(p)
                            continue

            # Fit check (RAM strict; CPU can be downsized a bit to fill fragments)
            # Downsize CPU to what's available, but keep per-priority minimum.
            cpu_eff = cpu_req
            if cpu_eff > avail_cpu:
                cpu_eff = float(avail_cpu)
            if cpu_eff < float(s.min_cpu.get(pri, 1.0)):
                q.append(p)
                continue

            # RAM must fit (do not downsize below request/hint).
            if ram_req > avail_ram:
                q.append(p)
                continue

            # Selection objective:
            #   1) prefer fewer remaining ops (complete pipelines)
            #   2) prefer older/staler (anti-starvation)
            #   3) prefer larger RAM to reduce fragmentation / increase utilization
            # For batch, give extra age boost once starving.
            age_boost = age
            if pri == Priority.BATCH_PIPELINE and age >= int(s.batch_starve_ticks):
                age_boost = age + 1000

            sort_key = (int(remaining_ops), -int(age_boost), -float(ram_req))

            if best is None or sort_key < best[0]:
                best = (sort_key, p, chosen_op, cpu_eff, float(ram_req), opk)

            q.append(p)

        if best is None:
            return None
        _, p, op, cpu_eff, ram_req, opk = best
        return p, op, cpu_eff, ram_req, opk

    # --- Main scheduling loop (fill pools) ---
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Reservations only meaningful if active; otherwise keep fully utilized.
        if reserve_active:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac)
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # In dedicated latency pools, avoid batch whenever high-priority is pending/recent,
        # but still allow batch if there's no runnable high-priority work to prevent idling.
        is_latency_pool = (pool_id in latency_pools)
        allow_batch_default = True
        if is_latency_pool and reserve_active:
            allow_batch_default = False

        # Fill the pool greedily.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            picked = None

            # Always try high priority first
            for pri in (Priority.QUERY, Priority.INTERACTIVE):
                cand = _pick_candidate(pri, pool, avail_cpu, avail_ram, allow_batch_here=True, reserve_cpu=0.0, reserve_ram=0.0)
                if cand is None:
                    continue
                picked = (pri, cand)
                break

            if picked is None:
                # No runnable high priority fit; try batch with gating.
                cand = _pick_candidate(
                    Priority.BATCH_PIPELINE,
                    pool,
                    avail_cpu,
                    avail_ram,
                    allow_batch_here=allow_batch_default or (not high_waiting),
                    reserve_cpu=reserve_cpu,
                    reserve_ram=reserve_ram,
                )
                if cand is not None:
                    picked = (Priority.BATCH_PIPELINE, cand)

            if picked is None:
                break

            pri, (p, op, cpu_req, ram_req, opk) = picked

            # Create assignment
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

            # Track for result attribution
            s.op_to_pipeline[opk] = p.pipeline_id

            # Update tick-local state
            scheduled_ops.add(opk)
            scheduled_count_by_pipeline[p.pipeline_id] = int(scheduled_count_by_pipeline.get(p.pipeline_id, 0)) + 1

            # Consume resources
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

    return suspensions, assignments
