@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Queues hold pipeline_ids (not pipeline objects) to avoid duplicates.
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.q_count = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Active pipelines and metadata.
    s.active = set()  # pipeline_ids currently tracked in queues
    # meta: pipeline_id -> {"enqueued_tick": int, "priority": Priority}
    s.meta = {}

    # Latest pipeline object for each pipeline_id (updated on every callback).
    s.pipelines = {}

    # Map stable-ish operator key -> pipeline_id (for attributing results).
    s.op_to_pipeline = {}

    # Per-op RAM hint (typically increases on OOM): (pipeline_id, op_key) -> ram
    s.ram_hint = {}
    s.oom_count = {}          # (pipeline_id, op_key) -> int
    s.nonoom_fail_count = {}  # (pipeline_id, op_key) -> int

    # Avoid wasting resources forever on repeated non-OOM failing ops (rare, but guard).
    s.pipeline_blacklist = set()

    # Housekeeping
    s.compact_every = 25
    s.scan_limit_query = 25
    s.scan_limit_interactive = 50
    s.scan_limit_batch = 80
    s.max_nonoom_retries_per_op = 4


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "key", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed" in msg and "mem" in msg)

    def _deactivate(pid):
        if pid not in s.active:
            return
        s.active.discard(pid)
        meta = s.meta.pop(pid, None)
        if meta is not None:
            pri = meta.get("priority", None)
            if pri in s.q_count and s.q_count[pri] > 0:
                s.q_count[pri] -= 1

    def _compact_queue(pri):
        q = s.q[pri]
        if not q:
            s.cursor[pri] = 0
            s.q_count[pri] = 0
            return
        new_q = []
        for pid in q:
            if pid is None:
                continue
            if pid in s.active and pid not in s.pipeline_blacklist:
                new_q.append(pid)
        s.q[pri] = new_q
        s.cursor[pri] = 0
        s.q_count[pri] = len(new_q)

    def _pipeline_wait_ticks(pid):
        meta = s.meta.get(pid)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _has_assignable(pri, max_checks):
        q = s.q[pri]
        n = len(q)
        if n == 0:
            return False
        checks = 0
        idx0 = s.cursor[pri] % n if n > 0 else 0
        i = 0
        while i < n and checks < max_checks:
            idx = (idx0 + i) % n
            pid = q[idx]
            i += 1
            if pid is None or pid not in s.active or pid in s.pipeline_blacklist:
                continue
            p = s.pipelines.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _deactivate(pid)
                q[idx] = None
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            checks += 1
            if ops:
                return True
        return False

    def _base_cpu(pool, pri, backlog):
        max_cpu = pool.max_cpu_pool
        if pri == Priority.BATCH_PIPELINE:
            # Aim: many concurrent memory-bound ops.
            cpu = max(2.0, min(8.0, float(int(max_cpu // 16) or 2)))
            if backlog > 250:
                cpu = max(2.0, cpu * 0.85)
            return cpu
        if pri == Priority.INTERACTIVE:
            cpu = max(2.0, min(12.0, float(int(max_cpu // 12) or 2)))
            if backlog > 120:
                cpu = max(2.0, cpu * 0.90)
            return cpu
        # QUERY
        cpu = max(4.0, min(16.0, float(int(max_cpu // 8) or 4)))
        if backlog > 40:
            cpu = max(4.0, cpu * 0.92)
        return cpu

    def _base_ram(pool, pri, backlog):
        max_ram = pool.max_ram_pool
        if pri == Priority.BATCH_PIPELINE:
            # Target parallelism ~12 by RAM, capped to avoid huge over-allocation on big clusters.
            ram = max(12.0, min(192.0, max_ram / 12.0))
            if backlog > 250:
                ram = max(10.0, ram * 0.90)
            return ram
        if pri == Priority.INTERACTIVE:
            # Slightly larger than batch.
            ram = max(16.0, min(256.0, max_ram / 10.0))
            if backlog > 120:
                ram = max(14.0, ram * 0.92)
            return ram
        # QUERY
        ram = max(24.0, min(384.0, max_ram / 8.0))
        if backlog > 40:
            ram = max(20.0, ram * 0.92)
        return ram

    def _min_cpu(pri):
        if pri == Priority.QUERY:
            return 2.0
        if pri == Priority.INTERACTIVE:
            return 2.0
        return 1.0

    def _min_ram(pri):
        if pri == Priority.QUERY:
            return 16.0
        if pri == Priority.INTERACTIVE:
            return 12.0
        return 8.0

    def _choose_op(pid, ops, pri):
        # If multiple ops are runnable, prefer the one with smallest known RAM need for batch
        # (helps packing & throughput); otherwise just take the first.
        if not ops:
            return None
        if pri != Priority.BATCH_PIPELINE or len(ops) == 1:
            return ops[0]
        best = ops[0]
        best_need = None
        limit = 4 if len(ops) > 4 else len(ops)
        for i in range(limit):
            op = ops[i]
            need = s.ram_hint.get((pid, _op_key(op)), None)
            need_val = need if (need is not None) else 0.0
            if best_need is None or need_val < best_need:
                best = op
                best_need = need_val
        return best

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        backlog = s.q_count.get(pri, 0)

        cpu_tgt = _base_cpu(pool, pri, backlog)
        ram_tgt = _base_ram(pool, pri, backlog)

        # Apply per-op hint (from OOM feedback).
        opk = _op_key(op)
        hint = s.ram_hint.get((pid, opk), None)
        if hint is not None:
            if hint > pool.max_ram_pool:
                hint = pool.max_ram_pool
            ram_tgt = max(ram_tgt, hint)

        # Clip to pool maxima.
        cpu_tgt = min(cpu_tgt, pool.max_cpu_pool)
        ram_tgt = min(ram_tgt, pool.max_ram_pool)

        # Opportunistic CPU downscale if needed.
        if avail_cpu < cpu_tgt:
            cpu_tgt = avail_cpu

        # RAM: be stricter for QUERY/INTERACTIVE to avoid avoidable OOM churn.
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            if avail_ram < ram_tgt:
                return None, None
            ram_req = ram_tgt
        else:
            # Batch: allow taking leftover RAM if above min and no hint demands more.
            if avail_ram < _min_ram(pri):
                return None, None
            ram_req = min(ram_tgt, avail_ram)
            if hint is not None and ram_req < hint:
                return None, None

        # Enforce minimums.
        if cpu_tgt < _min_cpu(pri):
            return None, None
        if ram_req < _min_ram(pri):
            return None, None

        return cpu_tgt, ram_req

    def _pick(pri, pool, avail_cpu, avail_ram, scheduled_counts, per_pipeline_cap, scan_limit, prefer_oldest):
        q = s.q[pri]
        n = len(q)
        if n == 0:
            return None

        start = s.cursor[pri] % n if n > 0 else 0
        best = None  # (score, idx, pid, op, cpu, ram)
        scanned = 0
        i = 0
        while i < n and scanned < scan_limit:
            idx = (start + i) % n
            i += 1
            pid = q[idx]
            if pid is None:
                continue
            if pid in s.pipeline_blacklist or pid not in s.active:
                q[idx] = None
                continue

            p = s.pipelines.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _deactivate(pid)
                q[idx] = None
                continue

            cap_used = scheduled_counts.get(pid, 0)
            if cap_used >= per_pipeline_cap:
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            scanned += 1
            if not ops:
                continue

            op = _choose_op(pid, ops, pri)
            if op is None:
                continue

            cpu_req, ram_req = _size_request(pool, pri, pid, op, avail_cpu, avail_ram)
            if cpu_req is None:
                continue
            if cpu_req > avail_cpu or ram_req > avail_ram:
                continue

            score = _pipeline_wait_ticks(pid) if prefer_oldest else 0
            if best is None or score > best[0]:
                best = (score, idx, pid, op, cpu_req, ram_req)
                if not prefer_oldest:
                    break

        if best is None:
            return None

        _, idx, pid, op, cpu_req, ram_req = best
        # Move cursor past the selected index for round-robin-ish fairness.
        if n > 0:
            s.cursor[pri] = (idx + 1) % n
        return pid, op, cpu_req, ram_req

    # ---- Ingest/update pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines[pid] = p
        if pid in s.pipeline_blacklist:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _deactivate(pid)
            continue

        if pid not in s.active:
            s.active.add(pid)
            s.meta[pid] = {"enqueued_tick": s.ticks, "priority": p.priority}
            s.q[p.priority].append(pid)
            s.q_count[p.priority] += 1

    # ---- Process results (OOM learning; limited non-OOM retry guard) ----
    for r in results:
        r_pri = getattr(r, "priority", None)
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        ops_in_res = getattr(r, "ops", []) or []
        for op in ops_in_res:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue
            key = (pid, opk)

            failed = (hasattr(r, "failed") and r.failed())
            if not failed:
                s.nonoom_fail_count.pop(key, None)
                continue

            err = getattr(r, "error", None)
            if _is_oom_error(err):
                prev_hint = s.ram_hint.get(key, None)
                alloc = getattr(r, "ram", None)

                base = 1.0
                if prev_hint is not None and prev_hint > base:
                    base = prev_hint
                if alloc is not None and alloc > base:
                    base = alloc

                cnt = s.oom_count.get(key, 0) + 1
                s.oom_count[key] = cnt

                # Faster convergence for high-priority OOMs; moderate for batch.
                if r_pri in (Priority.QUERY, Priority.INTERACTIVE):
                    mult = 2.3 if cnt <= 2 else 1.7
                    add = 32.0
                else:
                    mult = 2.0 if cnt <= 2 else 1.6
                    add = 16.0

                new_hint = base * mult + add
                if pool is not None and new_hint > pool.max_ram_pool:
                    new_hint = pool.max_ram_pool
                s.ram_hint[key] = new_hint
            else:
                c = s.nonoom_fail_count.get(key, 0) + 1
                s.nonoom_fail_count[key] = c
                if c >= s.max_nonoom_retries_per_op:
                    s.pipeline_blacklist.add(pid)
                    _deactivate(pid)

    # ---- Periodic compaction ----
    if s.ticks % s.compact_every == 0:
        _compact_queue(Priority.QUERY)
        _compact_queue(Priority.INTERACTIVE)
        _compact_queue(Priority.BATCH_PIPELINE)

    # If nothing to do, exit quickly.
    if (not pipelines) and (not results):
        return [], []

    # ---- Scheduling ----
    suspensions = []
    assignments = []
    scheduled_counts = {}  # pipeline_id -> ops scheduled this tick

    has_query_now = _has_assignable(Priority.QUERY, 30)
    has_inter_now = _has_assignable(Priority.INTERACTIVE, 40)
    high_runnable_now = has_query_now or has_inter_now

    pool_ids = list(range(s.executor.num_pools))
    pool_ids = sorted(
        pool_ids,
        key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool),
        reverse=True,
    )
    if s.executor.num_pools > 1 and 0 in pool_ids:
        # Prefer pool 0 earlier (useful if it's effectively the "interactive" pool in some configs).
        pool_ids = [0] + [i for i in pool_ids if i != 0]

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        high_bias_pool = (s.executor.num_pools == 1) or (pool_id == 0)

        # Reservation: keep some headroom to avoid blocking incoming/query/interactive work.
        # Keep it modest to not kill batch throughput.
        base_res_cpu = min(8.0, pool.max_cpu_pool * 0.05) if high_bias_pool else 0.0
        base_res_ram = min(64.0, pool.max_ram_pool * 0.05) if high_bias_pool else 0.0

        if high_bias_pool and high_runnable_now:
            # Reserve enough for at least one query-ish and one interactive-ish container.
            q_cpu = _base_cpu(pool, Priority.QUERY, s.q_count.get(Priority.QUERY, 0))
            q_ram = _base_ram(pool, Priority.QUERY, s.q_count.get(Priority.QUERY, 0))
            i_cpu = _base_cpu(pool, Priority.INTERACTIVE, s.q_count.get(Priority.INTERACTIVE, 0))
            i_ram = _base_ram(pool, Priority.INTERACTIVE, s.q_count.get(Priority.INTERACTIVE, 0))
            res_cpu = max(base_res_cpu, min(pool.max_cpu_pool * 0.35, q_cpu + i_cpu))
            res_ram = max(base_res_ram, min(pool.max_ram_pool * 0.35, q_ram + i_ram))
        else:
            res_cpu = base_res_cpu
            res_ram = base_res_ram

        # Cycles to ensure batch keeps making progress while protecting interactive.
        if high_bias_pool:
            cycle = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE, Priority.BATCH_PIPELINE]
        else:
            cycle = [Priority.BATCH_PIPELINE, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            made = False

            # Always try QUERY first (strict priority).
            picked = _pick(
                Priority.QUERY,
                pool,
                avail_cpu,
                avail_ram,
                scheduled_counts,
                per_pipeline_cap=2,
                scan_limit=s.scan_limit_query,
                prefer_oldest=False,
            )
            if picked is not None:
                pid, op, cpu_req, ram_req = picked
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=Priority.QUERY,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )
                s.op_to_pipeline[_op_key(op)] = pid
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                made = True
                continue

            # Then follow the per-pool cycle for INTERACTIVE/BATCH fairness.
            for pri in cycle:
                if pri == Priority.INTERACTIVE:
                    per_cap = 2
                    scan_lim = s.scan_limit_interactive
                    prefer_old = True
                else:
                    per_cap = 1
                    scan_lim = s.scan_limit_batch
                    prefer_old = True

                # On high-bias pools, prevent batch from consuming reserved headroom.
                if high_bias_pool and pri == Priority.BATCH_PIPELINE and high_runnable_now:
                    # Estimate typical batch request to decide if we can afford another batch.
                    b_cpu = _base_cpu(pool, Priority.BATCH_PIPELINE, s.q_count.get(Priority.BATCH_PIPELINE, 0))
                    b_ram = _base_ram(pool, Priority.BATCH_PIPELINE, s.q_count.get(Priority.BATCH_PIPELINE, 0))
                    if (avail_cpu - b_cpu) < res_cpu or (avail_ram - b_ram) < res_ram:
                        continue

                picked = _pick(
                    pri,
                    pool,
                    avail_cpu,
                    avail_ram,
                    scheduled_counts,
                    per_pipeline_cap=per_cap,
                    scan_limit=scan_lim,
                    prefer_oldest=prefer_old,
                )
                if picked is None:
                    continue

                pid, op, cpu_req, ram_req = picked
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pri,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )
                s.op_to_pipeline[_op_key(op)] = pid
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
                made = True
                break

            if not made:
                break

    return suspensions, assignments
