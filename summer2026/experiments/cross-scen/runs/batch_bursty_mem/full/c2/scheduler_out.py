class _RRQueue:
    __slots__ = ("buf", "head")

    def __init__(self):
        self.buf = []
        self.head = 0

    def __len__(self):
        return len(self.buf) - self.head

    def push(self, x):
        self.buf.append(x)

    def pop(self):
        if self.head >= len(self.buf):
            return None
        x = self.buf[self.head]
        self.head += 1
        # Periodic compaction to keep memory/time bounded.
        if self.head > 128 and self.head * 2 > len(self.buf):
            self.buf = self.buf[self.head :]
            self.head = 0
        return x


@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.q = {
        Priority.QUERY: _RRQueue(),
        Priority.INTERACTIVE: _RRQueue(),
        Priority.BATCH_PIPELINE: _RRQueue(),
    }

    # pipeline_id -> Pipeline
    s.pipelines = {}

    # pipeline_ids currently present in queues (best-effort; stale queue entries are tolerated)
    s.enqueued = set()

    # pipeline_ids that we will no longer attempt to run
    s.dead = set()

    # pipeline_id -> tick when we should next re-check for runnable ops
    s.next_check = {}

    # pipeline_id -> multiplicative RAM bias (learned from OOMs in this pipeline)
    s.pipe_ram_mult = {}

    # (pipeline_id, op_key) -> dict
    #   floor_ram: largest RAM allocation that has OOM'd (lower bound)
    #   ok_ram: smallest RAM allocation observed to succeed (soft upper bound)
    #   oom_tries: consecutive OOM tries
    #   fail_tries: consecutive non-OOM failures
    #   last_ram: last RAM we requested (for bookkeeping)
    s.op_info = {}

    # op_key -> pipeline_id attribution (best-effort)
    s.opk_to_pid = {}

    # Tuning knobs (keep simple + fast)
    s.max_scan_per_pick = 8
    s.max_assignments_per_pool_per_tick_cap = 128
    s.max_oom_retries_per_op = 6
    s.max_non_oom_retries_per_op = 3

    # Cooldowns (ticks) when pipeline has no runnable ops
    s.cooldown = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 4,
    }


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
            or ("cuda out of memory" in msg)
            or ("allocation failed" in msg)
        )

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipelines.pop(pid, None)
        s.next_check.pop(pid, None)
        s.pipe_ram_mult.pop(pid, None)

    def _mark_dead(pid):
        if pid in s.dead:
            return
        s.dead.add(pid)
        _drop_pipeline(pid)

    # --- Ingest new pipelines (assumed to be new arrivals; duplicates tolerated) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead:
            continue
        if pid not in s.pipelines:
            s.pipelines[pid] = p
        if pid not in s.enqueued:
            s.q[p.priority].push(pid)
            s.enqueued.add(pid)
            s.next_check[pid] = s.ticks
            if pid not in s.pipe_ram_mult:
                s.pipe_ram_mult[pid] = 1.0

    # --- Process execution results (learn RAM needs, cap retries) ---
    for r in results:
        r_ops = getattr(r, "ops", None) or []
        r_failed = False
        failed_fn = getattr(r, "failed", None)
        if callable(failed_fn):
            r_failed = bool(failed_fn())
        else:
            r_failed = getattr(r, "error", None) is not None

        r_err = getattr(r, "error", None)
        is_oom = r_failed and _is_oom_error(r_err)

        r_pid = getattr(r, "pipeline_id", None)
        r_pool_id = getattr(r, "pool_id", None)

        pool_max_ram = None
        if r_pool_id is not None and 0 <= int(r_pool_id) < s.executor.num_pools:
            pool_max_ram = s.executor.pools[int(r_pool_id)].max_ram_pool

        r_ram = getattr(r, "ram", None)
        try:
            r_ram_i = int(r_ram) if r_ram is not None else None
        except Exception:
            r_ram_i = None

        for op in r_ops:
            opk = _op_key(op)
            pid = r_pid if r_pid is not None else s.opk_to_pid.get(opk)
            if pid is None:
                continue

            key = (pid, opk)
            info = s.op_info.get(key)
            if info is None:
                info = {
                    "floor_ram": 0,
                    "ok_ram": 0,
                    "oom_tries": 0,
                    "fail_tries": 0,
                    "last_ram": 0,
                }
                s.op_info[key] = info

            alloc_ram = r_ram_i if (r_ram_i is not None and r_ram_i > 0) else int(info.get("last_ram", 0) or 0)

            if r_failed:
                if is_oom:
                    info["oom_tries"] = int(info.get("oom_tries", 0) or 0) + 1
                    info["fail_tries"] = 0
                    if alloc_ram > int(info.get("floor_ram", 0) or 0):
                        info["floor_ram"] = alloc_ram
                    # if we ever saw "ok", invalidate it once we exceed it
                    if int(info.get("ok_ram", 0) or 0) <= int(info.get("floor_ram", 0) or 0):
                        info["ok_ram"] = 0

                    # Pipeline-level RAM bias: if one op OOMs, siblings often do too.
                    m = float(s.pipe_ram_mult.get(pid, 1.0) or 1.0)
                    m = min(4.0, m * 1.4)
                    s.pipe_ram_mult[pid] = m

                    # Give up if unschedulable or too many OOMs.
                    if pool_max_ram is not None and int(info["floor_ram"]) >= int(pool_max_ram):
                        _mark_dead(pid)
                    elif int(info["oom_tries"]) > int(s.max_oom_retries_per_op):
                        _mark_dead(pid)
                else:
                    info["fail_tries"] = int(info.get("fail_tries", 0) or 0) + 1
                    info["oom_tries"] = 0
                    if int(info["fail_tries"]) >= int(s.max_non_oom_retries_per_op):
                        _mark_dead(pid)
            else:
                # Success: remember the smallest known-good RAM, decay pipeline bias slowly.
                info["oom_tries"] = 0
                info["fail_tries"] = 0
                if alloc_ram > 0:
                    ok_prev = int(info.get("ok_ram", 0) or 0)
                    info["ok_ram"] = alloc_ram if ok_prev <= 0 else min(ok_prev, alloc_ram)

                m = float(s.pipe_ram_mult.get(pid, 1.0) or 1.0)
                if m > 1.0:
                    s.pipe_ram_mult[pid] = max(1.0, m * 0.97)

    # --- Scheduling ---
    suspensions = []
    assignments = []

    # Snapshot availability; maintain tick-local counters (required by simulator).
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    scheduled_pids = set()

    # Static RAM bases (GB-ish). Keep moderate: allow concurrency; converge upward on OOM.
    base_ram_by_pri = {
        Priority.QUERY: 24,
        Priority.INTERACTIVE: 96,
        Priority.BATCH_PIPELINE: 96,
    }
    min_ram_by_pri = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 16,
        Priority.BATCH_PIPELINE: 16,
    }

    # Quota shares for new assignments when all classes backlogged.
    # Batch must progress continuously to avoid timeout penalties.
    shares = {
        Priority.QUERY: 0.12,
        Priority.INTERACTIVE: 0.18,
        Priority.BATCH_PIPELINE: 0.70,
    }

    def _cpu_targets(pool):
        # Scale gently with pool size to limit per-tick assignment count on large clusters,
        # but avoid the "one op consumes the pool" failure mode.
        try:
            max_cpu = int(pool.max_cpu_pool)
        except Exception:
            max_cpu = 1
        unit = max(4, max_cpu // 64)
        if unit > 16:
            unit = 16

        cpu_b = max(4, unit)
        cpu_i = max(6, min(32, unit * 2))
        cpu_q = max(8, min(48, unit * 3))

        # Never exceed pool max
        if cpu_b > max_cpu:
            cpu_b = max_cpu
        if cpu_i > max_cpu:
            cpu_i = max_cpu
        if cpu_q > max_cpu:
            cpu_q = max_cpu

        return {Priority.QUERY: cpu_q, Priority.INTERACTIVE: cpu_i, Priority.BATCH_PIPELINE: cpu_b}, unit

    def _estimate_ram(pool, pri, pid, op):
        opk = _op_key(op)
        info = s.op_info.get((pid, opk))
        floor_ram = int(info.get("floor_ram", 0) or 0) if info else 0
        ok_ram = int(info.get("ok_ram", 0) or 0) if info else 0
        oom_tries = int(info.get("oom_tries", 0) or 0) if info else 0

        m = float(s.pipe_ram_mult.get(pid, 1.0) or 1.0)
        base = float(base_ram_by_pri[pri]) * m

        # If we've OOM'd repeatedly, escalate base quickly (in addition to floor-based bump).
        if oom_tries > 0:
            if oom_tries == 1:
                base *= 1.35
            elif oom_tries == 2:
                base *= 1.70
            else:
                base *= 2.10

        ram = int(base)

        if ok_ram > 0:
            ram = max(ram, ok_ram)
        if floor_ram > 0:
            # Ensure we grow beyond the known failing point.
            ram = max(ram, int(floor_ram * 2.0) + 1)

        ram = max(ram, int(min_ram_by_pri[pri]))

        try:
            max_ram = int(pool.max_ram_pool)
        except Exception:
            max_ram = ram
        if ram > max_ram:
            ram = max_ram
        return ram

    def _try_pick_one(pri, pool_id, pool, avail_cpu, avail_ram, cpu_tgt):
        q = s.q[pri]
        n = len(q)
        if n <= 0:
            return None

        scans = s.max_scan_per_pick
        if scans > n:
            scans = n

        for _ in range(scans):
            pid = q.pop()
            if pid is None:
                break

            # Drop/skip stale
            if pid in s.dead:
                continue
            p = s.pipelines.get(pid)
            if p is None:
                s.enqueued.discard(pid)
                s.next_check.pop(pid, None)
                s.pipe_ram_mult.pop(pid, None)
                continue

            # Round-robin: reinsert unless we drop it
            # Avoid multiple dispatches from same pipeline in one tick
            if pid in scheduled_pids:
                q.push(pid)
                continue

            # Cooldown for blocked pipelines
            nct = s.next_check.get(pid, 0)
            if nct and s.ticks < int(nct):
                q.push(pid)
                continue

            st = p.runtime_status()

            if st.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                s.next_check[pid] = s.ticks + int(s.cooldown.get(pri, 2))
                q.push(pid)
                continue

            op = ops[0]
            opk = _op_key(op)

            cpu_req = int(cpu_tgt.get(pri, 1))
            if cpu_req < 1:
                cpu_req = 1
            if cpu_req > int(pool.max_cpu_pool):
                cpu_req = int(pool.max_cpu_pool)

            # CPU can always shrink-to-fit (sublinear scaling).
            if cpu_req > avail_cpu:
                cpu_req = int(avail_cpu)
            if cpu_req < 1:
                q.push(pid)
                continue

            ram_req = _estimate_ram(pool, pri, pid, op)

            # RAM: only shrink-to-fit if we have no learned lower bound and no known-good value.
            info = s.op_info.get((pid, opk))
            floor_ram = int(info.get("floor_ram", 0) or 0) if info else 0
            ok_ram = int(info.get("ok_ram", 0) or 0) if info else 0
            allow_shrink = (floor_ram <= 0 and ok_ram <= 0)

            if ram_req > avail_ram:
                if allow_shrink and int(avail_ram) >= int(min_ram_by_pri[pri]):
                    ram_req = int(avail_ram)
                else:
                    # Try later; maybe another pool/tick has room.
                    s.next_check[pid] = s.ticks + 1
                    q.push(pid)
                    continue

            if ram_req < int(min_ram_by_pri[pri]):
                q.push(pid)
                continue

            # Bookkeeping for learning / attribution
            key = (pid, opk)
            info2 = s.op_info.get(key)
            if info2 is None:
                info2 = {
                    "floor_ram": 0,
                    "ok_ram": 0,
                    "oom_tries": 0,
                    "fail_tries": 0,
                    "last_ram": 0,
                }
                s.op_info[key] = info2
            info2["last_ram"] = int(ram_req)
            s.opk_to_pid[opk] = pid

            a = Assignment(
                ops=[op],
                cpu=int(cpu_req),
                ram=int(ram_req),
                priority=p.priority,
                pool_id=int(pool_id),
                pipeline_id=pid,
            )

            scheduled_pids.add(pid)
            s.next_check[pid] = s.ticks  # allow immediate re-check next tick
            q.push(pid)
            return a, int(cpu_req), int(ram_req)

        return None

    priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = local_cpu[pool_id]
        avail_ram = local_ram[pool_id]

        if avail_cpu < 1 or avail_ram < 1:
            continue

        cpu_tgt, cpu_unit = _cpu_targets(pool)

        # Cap the number of new assignments from this pool this tick (perf bound).
        # Use cpu_unit to approximate how many containers we'd like at steady state.
        try:
            cap = int(avail_cpu // max(1, cpu_unit)) + 2
        except Exception:
            cap = 16
        if cap < 8:
            cap = 8
        if cap > s.max_assignments_per_pool_per_tick_cap:
            cap = s.max_assignments_per_pool_per_tick_cap

        # Compute quotas; enforce that batch gets at least half of the starts when backlogged.
        has = {pri: (len(s.q[pri]) > 0) for pri in priorities}

        q_quota = 0
        i_quota = 0
        b_quota = 0

        if has[Priority.QUERY]:
            q_quota = max(1, int(cap * float(shares[Priority.QUERY])))
        if has[Priority.INTERACTIVE]:
            i_quota = max(1, int(cap * float(shares[Priority.INTERACTIVE])))

        used = q_quota + i_quota
        if has[Priority.BATCH_PIPELINE]:
            b_quota = cap - used
            b_min = max(1, int(cap * 0.50))
            if b_quota < b_min:
                # Reduce interactive first, then query, to keep batch moving.
                need = b_min - b_quota
                take_i = min(i_quota, need)
                i_quota -= take_i
                need -= take_i
                if need > 0:
                    take_q = min(q_quota, need)
                    q_quota -= take_q
                    need -= take_q
                b_quota = cap - (q_quota + i_quota)
        else:
            # No batch backlog: give leftovers to interactive then query
            b_quota = 0
            leftover = cap - (q_quota + i_quota)
            if leftover > 0 and has[Priority.INTERACTIVE]:
                i_quota += leftover
            elif leftover > 0 and has[Priority.QUERY]:
                q_quota += leftover

        quotas = {
            Priority.QUERY: int(q_quota),
            Priority.INTERACTIVE: int(i_quota),
            Priority.BATCH_PIPELINE: int(b_quota),
        }

        # Interleave starts Q -> I -> B to reduce tail latency while preserving throughput.
        plan = []
        while len(plan) < cap and (quotas[Priority.QUERY] > 0 or quotas[Priority.INTERACTIVE] > 0 or quotas[Priority.BATCH_PIPELINE] > 0):
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if len(plan) >= cap:
                    break
                if quotas[pri] > 0:
                    plan.append(pri)
                    quotas[pri] -= 1

        # Execute plan with bounded fallback attempts.
        for pri in plan:
            if avail_cpu < 1 or avail_ram < 1:
                break

            picked = _try_pick_one(pri, pool_id, pool, avail_cpu, avail_ram, cpu_tgt)
            if picked is None:
                # Fallback: try other classes quickly to keep utilization high.
                for alt in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    if alt == pri:
                        continue
                    picked = _try_pick_one(alt, pool_id, pool, avail_cpu, avail_ram, cpu_tgt)
                    if picked is not None:
                        break

            if picked is None:
                # Nothing runnable/fittable in this pool right now.
                break

            a, cpu_used, ram_used = picked
            # Hard constraint: do not exceed tick-local availability.
            if cpu_used > avail_cpu or ram_used > avail_ram:
                break

            assignments.append(a)
            avail_cpu -= cpu_used
            avail_ram -= ram_used

        local_cpu[pool_id] = avail_cpu
        local_ram[pool_id] = avail_ram

    return suspensions, assignments
