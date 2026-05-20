@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Priority + throughput focused scheduler.

    Goals for this workload:
      - Finish INTERACTIVE pipelines (previously starved -> 0 completed).
      - Increase overall completion rate by boosting concurrency (smaller per-op cpu/ram).
      - Avoid OOM by using per-operator min-RAM if exposed; otherwise conservative defaults + OOM backoff.
      - Reduce interference by partitioning pools: a fraction "HP pools" biased to QUERY/INTERACTIVE.
      - No preemption (not reliably supported by public interface).
    """
    s.ticks = 0

    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued = set()

    # pipeline_id -> metadata
    # {"enqueued_tick": int, "total_ops": int|None, "last_progress_tick": int}
    s.pipeline_meta = {}

    # (pipeline_id, op_key) -> ram_hint (GB)
    s.op_ram_hint = {}

    # op_key -> pipeline_id (best effort attribution on results)
    s.op_to_pipeline = {}

    # Per-(pipeline, op) failure counters (to avoid infinite churn)
    s.op_fail_count = {}

    # Remember last arrival tick per priority for short-term reservations
    s.last_arrival_tick = {
        Priority.QUERY: -10**9,
        Priority.INTERACTIVE: -10**9,
        Priority.BATCH_PIPELINE: -10**9,
    }

    # Scheduling knobs
    s.scan_limit = 40

    # Allow some intra-pipeline parallelism but prevent runaway duplicate assignment in same tick.
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 3,
    }

    # Pool partitioning: fraction of pools biased toward QUERY/INTERACTIVE
    s.hp_pool_frac = 0.25

    # Weighted round-robin ratios between INTERACTIVE and BATCH depending on pool type
    # (QUERY is always strict-first everywhere)
    s.hp_pool_wrr = {Priority.INTERACTIVE: 5, Priority.BATCH_PIPELINE: 1}
    s.batch_pool_wrr = {Priority.INTERACTIVE: 1, Priority.BATCH_PIPELINE: 3}
    s.pool_wrr_state = {}  # pool_id -> {"seq": [Priority...], "idx": int, "is_hp": bool}

    # Reservation behavior: keep headroom briefly after last arrival
    s.reserve_hold_ticks = 10

    # Aging thresholds (ticks) to prioritize old pipelines to avoid timeouts/penalties
    s.urgent_age_ticks = {
        Priority.QUERY: 30,
        Priority.INTERACTIVE: 50,
        Priority.BATCH_PIPELINE: 80,
    }

    # If a pipeline has waited a long time, give it slightly more CPU to help finish before timeout.
    s.boost_age_ticks = {
        Priority.QUERY: 60,
        Priority.INTERACTIVE: 90,
        Priority.BATCH_PIPELINE: 120,
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

    def _safe_len(x):
        try:
            return len(x)
        except Exception:
            return None

    def _get_total_ops(p):
        vals = getattr(p, "values", None)
        if vals is None:
            return None
        n = _safe_len(vals)
        if n is not None:
            return n
        # Avoid exhausting iterators/generators; return unknown.
        return None

    def _reserve_active(pri):
        q = s.wait_q.get(pri, [])
        if q:
            return True
        last = s.last_arrival_tick.get(pri, -10**9)
        return (s.ticks - last) <= s.reserve_hold_ticks

    def _default_cpu_target(pool, pri, age_ticks):
        max_cpu = float(pool.max_cpu_pool)
        if pri == Priority.QUERY:
            base = max(2, int(round(max_cpu * 0.125)))  # ~8 on 64
            cap = max(4, int(round(max_cpu * 0.25)))    # cap ~16 on 64
        elif pri == Priority.INTERACTIVE:
            base = max(2, int(round(max_cpu * 0.095)))  # ~6 on 64
            cap = max(4, int(round(max_cpu * 0.20)))    # cap ~13 on 64
        else:
            base = max(1, int(round(max_cpu * 0.0625))) # ~4 on 64
            cap = max(2, int(round(max_cpu * 0.15)))    # cap ~10 on 64

        if age_ticks >= s.boost_age_ticks.get(pri, 10**9):
            base = min(cap, max(base, int(round(base * 1.5)) + 1))
        return max(1, min(base, cap, int(round(max_cpu))))

    def _default_ram_target(pool, pri, age_ticks):
        max_ram = float(pool.max_ram_pool)
        # Conservative-but-not-huge defaults (GB), tuned to increase concurrency vs prior 150-175GB allocations.
        if pri == Priority.QUERY:
            base = min(64.0, max_ram * 0.14)
        elif pri == Priority.INTERACTIVE:
            base = min(48.0, max_ram * 0.12)
        else:
            base = min(40.0, max_ram * 0.10)

        # Small bump for very old pipelines to reduce OOM risk on "last chance" runs.
        if age_ticks >= s.boost_age_ticks.get(pri, 10**9):
            base = min(max_ram, base * 1.25)

        return max(1.0, min(base, max_ram))

    def _op_min_ram(op):
        # Try common attribute names used by generators/simulators.
        for name in (
            "min_ram",
            "min_ram_gb",
            "ram_min",
            "ram_min_gb",
            "memory_min",
            "memory_min_gb",
            "mem_min",
            "mem_min_gb",
            "required_ram",
            "required_ram_gb",
        ):
            v = getattr(op, name, None)
            if v is None:
                continue
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except Exception:
                continue
        return None

    def _size_request(pool, pri, pipeline_id, op, age_ticks):
        opk = _op_key(op)
        max_ram = float(pool.max_ram_pool)

        # RAM: prefer explicit min-ram if available; otherwise use default; apply OOM hint if present.
        min_ram = _op_min_ram(op)
        if min_ram is not None:
            # Small safety margin to avoid boundary OOMs.
            ram_req = min(max_ram, max(1.0, min_ram * 1.15))
        else:
            ram_req = _default_ram_target(pool, pri, age_ticks)

        hint = s.op_ram_hint.get((pipeline_id, opk), None)
        if hint is not None:
            try:
                ram_req = max(ram_req, float(hint))
            except Exception:
                pass

        ram_req = max(1.0, min(ram_req, max_ram))

        # CPU: small targets to increase throughput and reduce HoL blocking.
        cpu_req = float(_default_cpu_target(pool, pri, age_ticks))
        cpu_req = max(1.0, min(cpu_req, float(pool.max_cpu_pool)))

        return cpu_req, ram_req

    def _ensure_pool_wrr(pool_id, is_hp):
        st = s.pool_wrr_state.get(pool_id)
        if st is not None and st.get("is_hp") == is_hp and st.get("seq"):
            return st
        weights = s.hp_pool_wrr if is_hp else s.batch_pool_wrr
        seq = []
        for _ in range(int(weights.get(Priority.INTERACTIVE, 0))):
            seq.append(Priority.INTERACTIVE)
        for _ in range(int(weights.get(Priority.BATCH_PIPELINE, 0))):
            seq.append(Priority.BATCH_PIPELINE)
        if not seq:
            seq = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        st = {"seq": seq, "idx": 0, "is_hp": is_hp}
        s.pool_wrr_state[pool_id] = st
        return st

    def _next_non_query_priority(pool_id, is_hp):
        st = _ensure_pool_wrr(pool_id, is_hp)
        seq = st["seq"]
        idx = st["idx"]
        pri = seq[idx % len(seq)]
        st["idx"] = (idx + 1) % len(seq)
        return pri

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        # Don't enqueue already completed pipelines (can happen if 'pipelines' includes all known).
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            pass

        pri = p.priority
        s.wait_q[pri].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {
            "enqueued_tick": s.ticks,
            "total_ops": _get_total_ops(p),
            "last_progress_tick": s.ticks,
        }
        s.last_arrival_tick[pri] = s.ticks

    # --- Process results (update RAM hints on OOM; cleanup op->pipeline mappings) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = (hasattr(r, "failed") and r.failed())
        oom = failed and _is_oom_error(getattr(r, "error", None))

        # Use pool max RAM if we need to clamp hint growth; best-effort.
        r_pool_id = getattr(r, "pool_id", None)
        r_pool_max_ram = None
        if r_pool_id is not None:
            try:
                r_pool_max_ram = float(s.executor.pools[int(r_pool_id)].max_ram_pool)
            except Exception:
                r_pool_max_ram = None

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            # Progress signal: any completion (success or fail) means the pipeline is active.
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_progress_tick"] = s.ticks

            if failed:
                key = (pid, opk)
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

                if oom:
                    prev = s.op_ram_hint.get(key, None)
                    alloc = getattr(r, "ram", None)
                    base = None
                    try:
                        if alloc is not None and float(alloc) > 0:
                            base = float(alloc)
                    except Exception:
                        base = None

                    if base is None and prev is not None:
                        try:
                            base = float(prev)
                        except Exception:
                            base = None
                    if base is None:
                        base = 1.0

                    # Exponential backoff, but not too aggressive to avoid instantly monopolizing the VM.
                    new_hint = base * 1.8
                    if prev is not None:
                        try:
                            new_hint = max(new_hint, float(prev) * 1.5)
                        except Exception:
                            pass
                    if r_pool_max_ram is not None:
                        new_hint = min(new_hint, r_pool_max_ram)
                    s.op_ram_hint[key] = max(1.0, new_hint)

            # Avoid unbounded growth of op_to_pipeline map.
            # (The op object is stable; we can delete once we see a result for it.)
            try:
                del s.op_to_pipeline[opk]
            except Exception:
                pass

    suspensions = []
    assignments = []

    # Nothing to do
    if not s.wait_q[Priority.QUERY] and not s.wait_q[Priority.INTERACTIVE] and not s.wait_q[Priority.BATCH_PIPELINE]:
        return suspensions, assignments

    # Per-tick tracking to prevent duplicate op assignment from the same snapshot.
    scheduled_ops_this_tick = set()  # op_key
    scheduled_ops_per_pipeline = {}  # pid -> count

    def _pipeline_age_ticks(pid):
        meta = s.pipeline_meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - int(meta.get("enqueued_tick", s.ticks)))

    def _pipeline_remaining_ops_estimate(p, status):
        pid = p.pipeline_id
        meta = s.pipeline_meta.get(pid)
        total = meta.get("total_ops") if meta else None
        if total is None:
            return 999
        try:
            completed = status.get_ops({OperatorState.COMPLETED}, require_parents_complete=False)
            c = len(completed) if completed is not None else 0
            rem = int(total) - int(c)
            return rem if rem > 0 else 0
        except Exception:
            return 999

    def _pick_candidate_from_queue(q, pool, avail_cpu, avail_ram, pri, reserve_cpu, reserve_ram):
        """
        Scan up to s.scan_limit pipelines, rotating the queue for fairness.
        Choose a runnable op that fits with reserves, preferring:
          - very old pipelines (avoid timeout penalties)
          - then small remaining-op pipelines (SRPT-ish)
          - then older within that
        Return (pipeline, op, cpu_req, ram_req) or (None, None, None, None).
        """
        if not q:
            return None, None, None, None

        best = None  # (score_tuple, pipeline, op, cpu_req, ram_req)
        nscan = min(len(q), int(s.scan_limit))

        for _ in range(nscan):
            p = q.pop(0)
            pid = p.pipeline_id

            # Skip if no longer tracked/enqueued (shouldn't happen often)
            if pid not in s.enqueued:
                continue

            # Drop completed pipelines quickly
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    _drop_pipeline(pid)
                    continue
            except Exception:
                # If status fails, keep it to avoid accidental drops
                q.append(p)
                continue

            # Per-pipeline per-tick cap
            cap = int(s.max_ops_per_pipeline_per_tick.get(pri, 1))
            used = int(scheduled_ops_per_pipeline.get(pid, 0))
            if used >= cap:
                q.append(p)
                continue

            # Find runnable ops; avoid duplicates already scheduled this tick
            try:
                runnable = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                runnable = []

            op = None
            for cand in runnable:
                opk = _op_key(cand)
                if opk in scheduled_ops_this_tick:
                    continue
                op = cand
                break

            if op is None:
                q.append(p)
                continue

            age = _pipeline_age_ticks(pid)
            cpu_req, ram_req = _size_request(pool, pri, pid, op, age)

            # If known hint is larger than pool capacity, cannot run here ever; keep queued (may run elsewhere).
            hint = s.op_ram_hint.get((pid, _op_key(op)), None)
            if hint is not None:
                try:
                    if float(hint) > float(pool.max_ram_pool):
                        q.append(p)
                        continue
                except Exception:
                    pass

            # Fit check with reserves
            if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                q.append(p)
                continue

            # Scoring: urgent old pipelines first; else SRPT-ish by remaining ops.
            rem_ops = _pipeline_remaining_ops_estimate(p, status)
            urgent = 1 if age >= int(s.urgent_age_ticks.get(pri, 10**9)) else 0
            # Lower is better:
            #   - urgent first (0)
            #   - then fewer remaining ops
            #   - then older first (negative age)
            score = (0 if urgent else 1, rem_ops, -age)

            if best is None or score < best[0]:
                best = (score, p, op, cpu_req, ram_req)

            q.append(p)

        if best is None:
            return None, None, None, None
        return best[1], best[2], best[3], best[4]

    # Determine pool partition
    num_pools = int(getattr(s.executor, "num_pools", 1) or 1)
    hp_count = int(round(num_pools * float(s.hp_pool_frac)))
    if hp_count < 1:
        hp_count = 1
    if hp_count > num_pools:
        hp_count = num_pools

    # Schedule HP pools first to pull down QUERY/INTERACTIVE latency.
    pool_order = list(range(num_pools))

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        is_hp = pool_id < hp_count

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Dynamic reservations
        # Always keep a small baseline for queries; keep more if recently active.
        q_active = _reserve_active(Priority.QUERY)
        i_active = _reserve_active(Priority.INTERACTIVE)

        # Reserve enough to place at least one "typical" QUERY quickly.
        q_res_cpu = float(_default_cpu_target(pool, Priority.QUERY, 0)) if q_active else 2.0
        q_res_ram = max(16.0, _default_ram_target(pool, Priority.QUERY, 0) * 0.5) if q_active else 16.0

        # Reserve enough to place ~two interactive ops in batch-heavy regions.
        i_res_cpu = float(_default_cpu_target(pool, Priority.INTERACTIVE, 0)) * (1.0 if is_hp else 2.0) if i_active else 0.0
        i_res_ram = _default_ram_target(pool, Priority.INTERACTIVE, 0) * (1.0 if is_hp else 2.0) if i_active else 0.0

        # Don't reserve more than pool capacity.
        q_res_cpu = min(q_res_cpu, float(pool.max_cpu_pool))
        q_res_ram = min(q_res_ram, float(pool.max_ram_pool))
        i_res_cpu = min(i_res_cpu, float(pool.max_cpu_pool))
        i_res_ram = min(i_res_ram, float(pool.max_ram_pool))

        # Fill the pool with small assignments; strict QUERY first.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            picked = False

            # 1) Strict priority for QUERY everywhere.
            if s.wait_q[Priority.QUERY]:
                p, op, cpu_req, ram_req = _pick_candidate_from_queue(
                    s.wait_q[Priority.QUERY],
                    pool,
                    avail_cpu,
                    avail_ram,
                    Priority.QUERY,
                    reserve_cpu=0.0,
                    reserve_ram=0.0,
                )
                if p is not None:
                    pid = p.pipeline_id
                    opk = _op_key(op)
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=float(cpu_req),
                            ram=float(ram_req),
                            priority=Priority.QUERY,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )
                    s.op_to_pipeline[opk] = pid
                    scheduled_ops_this_tick.add(opk)
                    scheduled_ops_per_pipeline[pid] = int(scheduled_ops_per_pipeline.get(pid, 0)) + 1
                    avail_cpu -= float(cpu_req)
                    avail_ram -= float(ram_req)
                    picked = True
                    continue

            # 2) Non-query: choose between INTERACTIVE and BATCH via pool-type WRR.
            #    HP pools favor INTERACTIVE; batch pools favor BATCH.
            non_query_pri = _next_non_query_priority(pool_id, is_hp)

            # If HP pool, try interactive first; if empty, allow batch.
            # If batch pool, try batch first; allow interactive spillover.
            for attempt in range(2):
                pri = non_query_pri if attempt == 0 else (Priority.BATCH_PIPELINE if non_query_pri == Priority.INTERACTIVE else Priority.INTERACTIVE)
                q = s.wait_q[pri]
                if not q:
                    continue

                # Reserves:
                #   - INTERACTIVE must leave QUERY reserve.
                #   - BATCH must leave QUERY + INTERACTIVE reserve.
                if pri == Priority.INTERACTIVE:
                    reserve_cpu = q_res_cpu
                    reserve_ram = q_res_ram
                else:
                    reserve_cpu = q_res_cpu + i_res_cpu
                    reserve_ram = q_res_ram + i_res_ram

                p, op, cpu_req, ram_req = _pick_candidate_from_queue(
                    q,
                    pool,
                    avail_cpu,
                    avail_ram,
                    pri,
                    reserve_cpu=reserve_cpu,
                    reserve_ram=reserve_ram,
                )
                if p is None:
                    continue

                pid = p.pipeline_id
                opk = _op_key(op)
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=float(cpu_req),
                        ram=float(ram_req),
                        priority=pri,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )
                s.op_to_pipeline[opk] = pid
                scheduled_ops_this_tick.add(opk)
                scheduled_ops_per_pipeline[pid] = int(scheduled_ops_per_pipeline.get(pid, 0)) + 1
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                picked = True
                break

            if not picked:
                break

    return suspensions, assignments
