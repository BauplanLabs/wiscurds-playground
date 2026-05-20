@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Deterministic, priority-aware scheduler tuned for bursty CPU-bound workloads:
    # - Avoids pool-size-proportional over-allocation (prevents low concurrency on big clusters)
    # - Uses small default RAM, adaptive RAM on OOM
    # - Uses moderate default CPU, adaptive CPU on timeouts
    # - Soft headroom reservation (no preemption needed) to protect high-priority tail latency
    # - Run-to-completion bias within each priority (finish in-flight pipelines sooner)
    s.ticks = 0

    # Queues store pipeline_ids for compactness/dedup.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipeline objects and enqueue state.
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline (first seen)
    s.enqueued = set()              # pipeline_ids currently considered active by scheduler
    s.dead_pipelines = set()        # pipeline_ids we stop scheduling after repeated hard failures
    s.pipeline_meta = {}            # pipeline_id -> {"enqueued_tick": int}
    s.pipeline_total_ops = {}       # pipeline_id -> int (best-effort)
    s.pipeline_inflight = {}        # pipeline_id -> int

    # Per-op adaptive sizing and retry tracking.
    # Keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}
    s.op_retry_count = {}

    # Map op_key -> (pipeline_id, priority) for result attribution and inflight accounting.
    s.op_to_pipeline = {}
    s.op_to_priority = {}

    # Headroom reservation control (protect against burst arrivals without preemption).
    s.last_high_arrival_tick = -10**12
    s.reserve_hold_ticks = 6  # keep headroom for this many scheduler steps after a high-priority arrival

    # Candidate search / fairness knobs.
    s.max_candidates = 30
    s.compact_every = 9
    s.batch_every = 6
    s.batch_starve_ticks = 36

    # Per-priority inflight caps (allow limited parallelism within a DAG).
    s.max_inflight_per_pipeline = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Base sizing (cluster-size invariant); adaptive hints adjust up when needed.
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 4,
    }
    s.min_cpu = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }
    s.max_cpu = {
        Priority.QUERY: 16,
        Priority.INTERACTIVE: 24,
        Priority.BATCH_PIPELINE: 12,
    }

    # RAM is deliberately modest by default for this CPU-bound workload; OOM bumps correct underestimates.
    s.base_ram = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.max_ram_multiplier = 0.95  # never request more than this fraction of pool RAM for one op

    # Aging-based CPU boost for long-waiting interactive/query work.
    s.cpu_age_step = 10
    s.cpu_age_increment = 2

    # Retry limits (avoid infinite failure loops that cause systemic timeouts).
    s.max_retries_timeout = 6
    s.max_retries_oom = 6
    s.max_retries_other = 2


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Prefer stable identifiers if present; fall back to id(op).
        return (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "id", None)
            or getattr(op, "key", None)
            or getattr(op, "uuid", None)
            or id(op)
        )

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg) or ("time limit" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _best_effort_total_ops(p):
        pid = p.pipeline_id
        if pid in s.pipeline_total_ops:
            return s.pipeline_total_ops[pid]
        vals = getattr(p, "values", None)
        total = None
        try:
            if vals is None:
                total = None
            elif hasattr(vals, "__len__"):
                total = len(vals)
            else:
                total = None
        except Exception:
            total = None
        if total is None or total <= 0:
            # Median is ~4; keep a sane default for progress heuristics.
            total = 4
        s.pipeline_total_ops[pid] = total
        return total

    def _estimate_remaining_ops(status):
        active_states = {
            OperatorState.PENDING,
            OperatorState.ASSIGNED,
            OperatorState.RUNNING,
            OperatorState.SUSPENDING,
            OperatorState.FAILED,
        }
        try:
            ops = status.get_ops(active_states, require_parents_complete=False)
            return len(ops) if ops is not None else 0
        except Exception:
            return 0

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            return
        if pid in s.enqueued:
            return
        try:
            if p.runtime_status().is_pipeline_successful():
                return
        except Exception:
            pass

        s.pipelines_by_id[pid] = p
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks}
        s.pipeline_inflight.setdefault(pid, 0)
        _best_effort_total_ops(p)

        _queue_for_priority(p.priority).append(pid)

        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.last_high_arrival_tick = s.ticks

    def _finish_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        s.pipeline_inflight.pop(pid, None)
        # Keep pipelines_by_id and total ops caches; harmless and avoids churn.

    def _mark_dead(pid):
        s.dead_pipelines.add(pid)
        _finish_pipeline(pid)

    def _pipeline_age_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _reserve_active():
        return (s.ticks - s.last_high_arrival_tick) <= s.reserve_hold_ticks

    def _reserve_cpu_query(pool):
        # Small, constant-ish reservation so a query can start immediately even if batch filled the pool.
        base = 4
        try:
            scaled = int(max(2, min(6, pool.max_cpu_pool * 0.08)))
        except Exception:
            scaled = 4
        return max(base, scaled)

    def _reserve_cpu_high(pool):
        # Reserve enough to start at least one query or interactive promptly.
        base = 8
        try:
            scaled = int(max(4, min(16, pool.max_cpu_pool * 0.14)))
        except Exception:
            scaled = 8
        return max(base, scaled)

    def _reserve_ram_high(pool):
        # RAM is plentiful in this workload; keep a modest headroom anyway.
        try:
            return min(pool.max_ram_pool * 0.10, 64)
        except Exception:
            return 32

    def _priority_pick_order():
        # Default: QUERY > INTERACTIVE > BATCH.
        # Anti-starvation: occasionally let batch contend earlier, and boost very old batch.
        if s.ticks % s.batch_every == 0:
            return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

        # If any batch has waited too long, let it contend earlier (after queries).
        q = s.wait_q[Priority.BATCH_PIPELINE]
        for i in range(min(len(q), 25)):
            pid = q[i]
            if pid in s.enqueued and pid not in s.dead_pipelines:
                if _pipeline_age_ticks(pid) >= s.batch_starve_ticks:
                    return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _compact_queues_if_needed():
        if s.ticks % s.compact_every != 0:
            return
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pri]
            if not q:
                continue
            new_q = []
            seen = set()
            for pid in q:
                if pid in seen:
                    continue
                seen.add(pid)
                if pid in s.dead_pipelines:
                    continue
                if pid not in s.enqueued:
                    continue
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue
                try:
                    if p.runtime_status().is_pipeline_successful():
                        _finish_pipeline(pid)
                        continue
                except Exception:
                    pass
                new_q.append(pid)
            s.wait_q[pri] = new_q

    def _size_request(pool, pri, pid, op):
        opk = _op_key(op)

        age = _pipeline_age_ticks(pid)

        # CPU
        cpu = s.op_cpu_hint.get((pid, opk), s.base_cpu[pri])
        if pri in (Priority.QUERY, Priority.INTERACTIVE) and age >= s.cpu_age_step:
            cpu = cpu + (age // s.cpu_age_step) * s.cpu_age_increment

        # Opportunistic scale-up for queries when there is ample free CPU (reduce tail latency).
        try:
            if pri == Priority.QUERY and pool.avail_cpu_pool >= 16:
                cpu = max(cpu, 12)
        except Exception:
            pass

        # Clamp CPU to policy + pool limits
        try:
            cpu = min(cpu, s.max_cpu[pri], pool.max_cpu_pool)
        except Exception:
            cpu = min(cpu, s.max_cpu[pri])
        cpu = max(cpu, s.min_cpu[pri], 1)

        # RAM
        ram = s.op_ram_hint.get((pid, opk), s.base_ram[pri])
        try:
            ram_cap = pool.max_ram_pool * s.max_ram_multiplier
            ram = min(ram, ram_cap, pool.max_ram_pool)
        except Exception:
            pass
        ram = max(ram, 1)

        return cpu, ram

    def _score_pipeline(pri, pid, p, status):
        age = _pipeline_age_ticks(pid)
        inflight = s.pipeline_inflight.get(pid, 0)
        remain = _estimate_remaining_ops(status)
        total = s.pipeline_total_ops.get(pid) or _best_effort_total_ops(p)
        if total <= 0:
            total = max(1, remain)
        progress = (total - remain) / float(total)

        # Weights tuned for "complete everything; protect QUERY/INTERACTIVE latency".
        if pri == Priority.QUERY:
            age_w, remain_w, prog_w, inflight_w = 5.0, 10.0, 10.0, 6.0
        elif pri == Priority.INTERACTIVE:
            age_w, remain_w, prog_w, inflight_w = 3.0, 7.0, 8.0, 5.0
        else:
            age_w, remain_w, prog_w, inflight_w = 1.2, 4.5, 4.0, 4.0

        # Higher is better.
        return (age_w * age) + (prog_w * progress * 10.0) - (remain_w * remain) - (inflight_w * inflight)

    def _pick_candidate(pri, pool, avail_cpu, avail_ram, reserve_cpu_needed, reserve_ram_needed):
        q = s.wait_q[pri]
        if not q:
            return None

        best = None  # (score, idx, pid, op, cpu_req, ram_req)
        limit = min(len(q), s.max_candidates)

        for idx in range(limit):
            pid = q[idx]
            if pid in s.dead_pipelines:
                continue
            if pid not in s.enqueued:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            # Completed -> drop
            try:
                if status.is_pipeline_successful():
                    _finish_pipeline(pid)
                    continue
            except Exception:
                pass

            inflight = s.pipeline_inflight.get(pid, 0)
            if inflight >= s.max_inflight_per_pipeline.get(pri, 1):
                continue

            try:
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                ops = []
            if not ops:
                continue

            # Prefer retrying FAILED ops first if possible (they block progress).
            op = None
            if len(ops) == 1:
                op = ops[0]
            else:
                # Heuristic: prioritize ops with higher retry count.
                best_op = ops[0]
                best_rc = -1
                for cand in ops[:6]:
                    opk = _op_key(cand)
                    rc = s.op_retry_count.get((pid, opk), 0)
                    if rc > best_rc:
                        best_rc = rc
                        best_op = cand
                op = best_op

            cpu_req, ram_req = _size_request(pool, pri, pid, op)
            opk = _op_key(op)

            # RAM: if we have a RAM hint (typically after OOM), treat it as a hard minimum.
            ram_hint = s.op_ram_hint.get((pid, opk), None)
            if ram_hint is not None and ram_hint > avail_ram:
                continue
            if ram_req > avail_ram:
                ram_req = avail_ram

            # CPU: can downshift if needed to fit.
            if cpu_req > avail_cpu:
                cpu_req = avail_cpu
            if cpu_req < s.min_cpu[pri]:
                continue

            # Respect headroom reservations.
            if (avail_cpu - cpu_req) < reserve_cpu_needed:
                continue
            if (avail_ram - ram_req) < reserve_ram_needed:
                continue

            score = _score_pipeline(pri, pid, p, status)
            cand = (score, idx, pid, op, cpu_req, ram_req)
            if best is None or cand[0] > best[0]:
                best = cand

        if best is None:
            return None

        _, idx, pid, op, cpu_req, ram_req = best

        # Rotate selected pid to the end for fairness.
        try:
            picked_pid = q.pop(idx)
            q.append(picked_pid)
        except Exception:
            pass

        return pid, op, cpu_req, ram_req

    # ---- Ingest pipelines (new arrivals and/or first time seen) ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        # Update stored reference (in case the simulator provides a fresher object).
        s.pipelines_by_id[pid] = p
        if pid not in s.enqueued:
            _enqueue_pipeline(p)

    # ---- Process results (adaptive sizing + inflight accounting) ----
    for r in results:
        ops = getattr(r, "ops", None) or []
        pri_r = getattr(r, "priority", None)
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.pop(opk, None)
            pri = s.op_to_priority.pop(opk, None) or pri_r

            if pid is not None:
                s.pipeline_inflight[pid] = max(0, s.pipeline_inflight.get(pid, 0) - 1)

            if not failed or pid is None or pri is None:
                continue

            err = getattr(r, "error", None)
            key = (pid, opk)
            rc = s.op_retry_count.get(key, 0) + 1
            s.op_retry_count[key] = rc

            if _is_oom_error(err):
                # Increase RAM aggressively; keep CPU moderate.
                prev = s.op_ram_hint.get(key, s.base_ram.get(pri, 4))
                alloc = getattr(r, "ram", None)
                base = alloc if (alloc is not None and alloc > 0) else prev
                new_ram = max(prev * 2, base * 2, prev + 2)
                s.op_ram_hint[key] = new_ram

                if rc >= s.max_retries_oom:
                    _mark_dead(pid)

            elif _is_timeout_error(err):
                # Increase CPU; this workload is CPU-bound.
                prev = s.op_cpu_hint.get(key, s.base_cpu.get(pri, 4))
                new_cpu = max(prev + 2, int(prev * 1.5) + 1)
                # Keep within policy max; pool max will be applied at scheduling time.
                new_cpu = min(new_cpu, s.max_cpu.get(pri, new_cpu))
                s.op_cpu_hint[key] = new_cpu

                if rc >= s.max_retries_timeout and prev >= s.max_cpu.get(pri, prev):
                    _mark_dead(pid)

            else:
                # Unknown/non-resource failure: limited retries, slight CPU bump.
                prev = s.op_cpu_hint.get(key, s.base_cpu.get(pri, 4))
                s.op_cpu_hint[key] = min(prev + 1, s.max_cpu.get(pri, prev + 1))
                if rc >= s.max_retries_other:
                    _mark_dead(pid)

    _compact_queues_if_needed()

    suspensions = []
    assignments = []

    # ---- Main scheduling loop ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        reserve_on = _reserve_active()

        # Per-pool fill until we cannot place more.
        while avail_cpu >= 1 and avail_ram >= 1:
            placed = False

            for pri in _priority_pick_order():
                # Compute headroom to keep for potential high-priority bursts.
                reserve_cpu_needed = 0
                reserve_ram_needed = 0

                if reserve_on:
                    if pri == Priority.BATCH_PIPELINE:
                        reserve_cpu_needed = _reserve_cpu_high(pool)
                        reserve_ram_needed = _reserve_ram_high(pool)
                    elif pri == Priority.INTERACTIVE:
                        reserve_cpu_needed = _reserve_cpu_query(pool)
                        reserve_ram_needed = 0

                    # Never require more reservation than what is currently available.
                    if reserve_cpu_needed >= avail_cpu:
                        reserve_cpu_needed = max(0, int(avail_cpu) - 1)
                    if reserve_ram_needed >= avail_ram:
                        reserve_ram_needed = max(0, avail_ram - 1)

                picked = _pick_candidate(
                    pri=pri,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    reserve_cpu_needed=reserve_cpu_needed,
                    reserve_ram_needed=reserve_ram_needed,
                )
                if picked is None:
                    continue

                pid, op, cpu_req, ram_req = picked
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue

                # Final clamps (defensive).
                if cpu_req > avail_cpu:
                    cpu_req = avail_cpu
                if ram_req > avail_ram:
                    ram_req = avail_ram
                if cpu_req < 1 or ram_req < 1:
                    continue

                # Create assignment.
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

                # Track for results and inflight accounting.
                opk = _op_key(op)
                s.op_to_pipeline[opk] = pid
                s.op_to_priority[opk] = p.priority
                s.pipeline_inflight[pid] = s.pipeline_inflight.get(pid, 0) + 1

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                placed = True
                break

            if not placed:
                break

    return suspensions, assignments
