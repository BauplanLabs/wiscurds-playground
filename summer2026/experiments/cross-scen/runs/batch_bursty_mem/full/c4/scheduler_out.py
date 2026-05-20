@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_pos = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    s.enqueued = set()  # pipeline_id
    s.active = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }
    s.pipeline_enq_tick = {}  # pipeline_id -> tick
    s.dead_pipelines = set()  # pipeline_id -> do not schedule anymore (unrunnable)

    # op_state[(pipeline_id, op_key)] = [oom_ram, ok_ram, oom_retries, nonoom_fails, last_ram, last_cpu]
    s.op_state = {}
    # sig_state[sig] = [oom_ram, ok_ram]
    s.sig_state = {}

    # Used only as a fallback attribution path
    s.op_to_pid = {}

    # Scheduling knobs (absolute units; DO NOT scale with pool size)
    s.base_ram = {
        Priority.QUERY: 48.0,
        Priority.INTERACTIVE: 40.0,
        Priority.BATCH_PIPELINE: 32.0,
    }
    s.base_cpu = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.min_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.max_cpu = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 12.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    s.ram_growth = 1.8
    s.ram_success_buffer = 1.05

    s.max_oom_retries = 5
    s.max_nonoom_fails = 3

    # Per-tick work bounds
    s.max_scan_per_try = 24
    s.max_new_assignments_per_tick_base = 64  # scaled by num_pools mildly below

    # Allow slightly more parallelism for queries (helps p99 without starving others)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Interleave interactive and batch so batch progresses even under interactive backlog
    s.ib_pattern = [
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
        Priority.BATCH_PIPELINE,
    ]
    s.ib_idx = 0

    # Keep scheduling across ticks until we fail to place anything
    s.last_made = 1

    # Occasional queue compaction (avoid long-term stale buildup)
    s.compact_every = 5000


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    OOM = 0
    OK = 1
    OOMR = 2
    NOOMF = 3
    LAST_RAM = 4
    LAST_CPU = 5

    SIG_OOM = 0
    SIG_OK = 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _op_sig(op):
        # Coarse-but-stable signature for cross-pipeline learning; keep it cheap.
        cls = getattr(op, "__class__", None)
        cls_name = getattr(cls, "__name__", None) if cls is not None else None
        name = getattr(op, "name", None) or getattr(op, "op_name", None) or getattr(op, "fn_name", None)
        kind = getattr(op, "kind", None) or getattr(op, "op_type", None) or getattr(op, "type", None)
        # Keep signature small; avoid large SQL blobs.
        if isinstance(name, str) and len(name) > 64:
            name = name[:64]
        if isinstance(kind, str) and len(kind) > 64:
            kind = kind[:64]
        return (cls_name, kind, name)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return (
            ("oom" in msg)
            or ("out of memory" in msg)
            or ("memoryerror" in msg)
            or ("cuda out of memory" in msg)
            or ("memory limit" in msg)
        )

    def _drop_pipeline(pid, pri=None):
        s.enqueued.discard(pid)
        s.pipeline_enq_tick.pop(pid, None)
        if pri is not None:
            s.active.get(pri, set()).discard(pid)
        else:
            s.active[Priority.QUERY].discard(pid)
            s.active[Priority.INTERACTIVE].discard(pid)
            s.active[Priority.BATCH_PIPELINE].discard(pid)

    def _maybe_compact():
        if s.ticks % s.compact_every != 0:
            return
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.q[pri]
            if len(q) <= 128:
                continue
            new_q = []
            for p in q:
                if p is None:
                    continue
                pid = getattr(p, "pipeline_id", None)
                if pid is None:
                    continue
                if pid in s.enqueued and pid not in s.dead_pipelines:
                    new_q.append(p)
            s.q[pri] = new_q
            if new_q:
                s.q_pos[pri] %= len(new_q)
            else:
                s.q_pos[pri] = 0

    # --- Ingest new pipelines (assumed to be new arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            continue
        pri = p.priority
        s.q[pri].append(p)
        s.enqueued.add(pid)
        s.active[pri].add(pid)
        s.pipeline_enq_tick[pid] = s.ticks

    # --- Process results (learn RAM from OOM, track success) ---
    for r in results:
        is_failed = False
        if hasattr(r, "failed"):
            try:
                is_failed = bool(r.failed())
            except Exception:
                is_failed = bool(getattr(r, "error", None))
        else:
            is_failed = bool(getattr(r, "error", None))

        alloc_ram_r = getattr(r, "ram", None)
        alloc_cpu_r = getattr(r, "cpu", None)

        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            sig = _op_sig(op)

            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = s.op_to_pid.get(opk, None)
            if pid is None:
                continue

            key = (pid, opk)
            st = s.op_state.get(key)
            if st is None:
                st = [0.0, 0.0, 0, 0, 0.0, 0.0]
                s.op_state[key] = st

            # Prefer executor-reported allocation; fall back to last requested.
            alloc_ram = alloc_ram_r if (alloc_ram_r is not None and alloc_ram_r > 0) else st[LAST_RAM]
            alloc_cpu = alloc_cpu_r if (alloc_cpu_r is not None and alloc_cpu_r > 0) else st[LAST_CPU]

            if is_failed:
                if _is_oom_error(getattr(r, "error", None)):
                    if alloc_ram and alloc_ram > st[OOM]:
                        st[OOM] = float(alloc_ram)
                    st[OOMR] += 1

                    sigst = s.sig_state.get(sig)
                    if sigst is None:
                        sigst = [0.0, 0.0]
                        s.sig_state[sig] = sigst
                    if alloc_ram and alloc_ram > sigst[SIG_OOM]:
                        sigst[SIG_OOM] = float(alloc_ram)

                    # If we already hit max RAM for that pool and still OOM, it may be unrunnable here.
                    pool_id = getattr(r, "pool_id", None)
                    if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                        pool_max = s.executor.pools[pool_id].max_ram_pool
                        if alloc_ram is not None and pool_max is not None and pool_max > 0:
                            if alloc_ram >= pool_max - 1e-9:
                                # If no pool can exceed this, stop burning retries.
                                can_exceed_somewhere = False
                                for i in range(s.executor.num_pools):
                                    if s.executor.pools[i].max_ram_pool > alloc_ram + 1e-9:
                                        can_exceed_somewhere = True
                                        break
                                if not can_exceed_somewhere:
                                    s.dead_pipelines.add(pid)
                                    _drop_pipeline(pid)

                    if st[OOMR] > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    st[NOOMF] += 1
                    if st[NOOMF] >= s.max_nonoom_fails:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
            else:
                # Success
                if alloc_ram and alloc_ram > 0:
                    if st[OK] <= 0:
                        st[OK] = float(alloc_ram)
                    else:
                        # Track the smallest successful allocation we observed
                        st[OK] = float(min(st[OK], alloc_ram))
                st[NOOMF] = 0

                sigst = s.sig_state.get(sig)
                if sigst is None:
                    sigst = [0.0, 0.0]
                    s.sig_state[sig] = sigst
                if alloc_ram and alloc_ram > 0:
                    if sigst[SIG_OK] <= 0:
                        sigst[SIG_OK] = float(alloc_ram)
                    else:
                        sigst[SIG_OK] = float(min(sigst[SIG_OK], alloc_ram))

    _maybe_compact()

    # If nothing changed and we couldn't place anything last time, skip work.
    if not pipelines and not results and s.last_made == 0:
        return [], []

    # --- Build assignments ---
    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    # Prefer keeping pool 0 as a low-latency lane if multiple pools exist.
    hi_pools = [0] if num_pools >= 1 else []
    non_hi_pools = [i for i in range(num_pools) if i not in hi_pools]

    has_q = len(s.active[Priority.QUERY]) > 0
    has_i = len(s.active[Priority.INTERACTIVE]) > 0

    # Headroom in the hi pool for prompt starts (only meaningful with >1 pool).
    if num_pools >= 2:
        headroom_cpu = (s.base_cpu[Priority.QUERY] if has_q else 0.0) + (s.base_cpu[Priority.INTERACTIVE] if has_i else 0.0)
        headroom_ram = (s.base_ram[Priority.QUERY] if has_q else 0.0) + (s.base_ram[Priority.INTERACTIVE] if has_i else 0.0)
    else:
        headroom_cpu = 0.0
        headroom_ram = 0.0

    scheduled_count_by_pid = {}
    assignments = []
    suspensions = []

    # Limit query placements per tick so interactive+batch keep flowing.
    query_cap = max(4, 2 * num_pools)
    query_made = 0

    max_new = s.max_new_assignments_per_tick_base
    if num_pools > 1:
        max_new = max_new + 16 * (num_pools - 1)

    def _rr_next_pipeline(pri):
        q = s.q[pri]
        if not q:
            return None
        pos = s.q_pos[pri]
        if pos >= len(q):
            pos = 0
        p = q[pos]
        pos += 1
        if pos >= len(q):
            pos = 0
        s.q_pos[pri] = pos
        return p

    def _ram_plan(pid, opk, sig, pri):
        base = float(s.base_ram[pri])
        st = s.op_state.get((pid, opk))
        op_oom = float(st[OOM]) if st is not None and st[OOM] > 0 else 0.0
        op_ok = float(st[OK]) if st is not None and st[OK] > 0 else 0.0

        sigst = s.sig_state.get(sig)
        sig_oom = float(sigst[SIG_OOM]) if sigst is not None and sigst[SIG_OOM] > 0 else 0.0
        sig_ok = float(sigst[SIG_OK]) if sigst is not None and sigst[SIG_OK] > 0 else 0.0

        lower = max(op_oom, sig_oom)

        if op_ok > 0:
            target = op_ok * float(s.ram_success_buffer)
            if lower > 0 and target <= lower + 1e-9:
                target = lower + 1.0
            return target, lower

        # Use signature OK as a gentle bump for new ops, but cap its influence.
        if sig_ok > 0:
            target = max(base, min(sig_ok, base * 4.0))
        else:
            target = base

        if lower > 0:
            target = max(target, max(lower * float(s.ram_growth), lower + 1.0))

        return target, lower

    def _cpu_plan(pid, opk, pri):
        base = float(s.base_cpu[pri])
        cap = float(s.max_cpu[pri])
        st = s.op_state.get((pid, opk))
        nonoom_fails = int(st[NOOMF]) if st is not None else 0
        # If it's failing for non-OOM reasons, try a bit more CPU (could help borderline timeouts).
        cpu = base + 2.0 * float(nonoom_fails)
        if cpu > cap:
            cpu = cap
        if cpu < float(s.min_cpu[pri]):
            cpu = float(s.min_cpu[pri])
        return cpu

    def _choose_pool(pri, ram_target, lower_bound, cpu_des, min_cpu):
        # Return (pool_id, cpu_alloc, ram_alloc) or (None, None, None)
        # Pool preference: queries + interactive prefer hi pool; batch prefers non-hi.
        if num_pools == 1:
            pref = [0]
        else:
            if pri == Priority.BATCH_PIPELINE:
                pref = non_hi_pools + hi_pools
            else:
                pref = hi_pools + non_hi_pools

        best = None
        best_score = None

        for pool_id in pref:
            pool = s.executor.pools[pool_id]

            if local_ram[pool_id] <= 0 or local_cpu[pool_id] < min_cpu:
                continue

            # If we already know we must go above lower_bound, the pool must allow it.
            if lower_bound > 0 and pool.max_ram_pool <= lower_bound + 1e-9:
                continue

            ram_alloc = ram_target
            if ram_alloc > pool.max_ram_pool:
                ram_alloc = pool.max_ram_pool

            if lower_bound > 0 and ram_alloc <= lower_bound + 1e-9:
                continue

            if ram_alloc > local_ram[pool_id] + 1e-9:
                continue

            cpu_alloc = cpu_des
            if cpu_alloc > pool.max_cpu_pool:
                cpu_alloc = pool.max_cpu_pool
            if cpu_alloc > local_cpu[pool_id]:
                cpu_alloc = local_cpu[pool_id]

            if cpu_alloc < min_cpu:
                continue

            # Headroom protection: keep hi pool responsive (only when multiple pools exist).
            if num_pools >= 2 and pri == Priority.BATCH_PIPELINE and pool_id in hi_pools:
                if (local_cpu[pool_id] - cpu_alloc) < headroom_cpu - 1e-9:
                    continue
                if (local_ram[pool_id] - ram_alloc) < headroom_ram - 1e-9:
                    continue

            # Scoring:
            # - Query/Interactive: prefer hi pool; then more remaining headroom.
            # - Batch: best-fit on RAM to reduce fragmentation.
            if pri == Priority.BATCH_PIPELINE:
                leftover_ram = local_ram[pool_id] - ram_alloc
                # smaller leftover is better; secondary prefer pools with more leftover cpu
                score = (leftover_ram, -float(local_cpu[pool_id]))
            else:
                is_hi = 0 if (pool_id in hi_pools) else 1
                score = (is_hi, -float(local_ram[pool_id]), -float(local_cpu[pool_id]))

            if best is None or score < best_score:
                best = (pool_id, cpu_alloc, ram_alloc)
                best_score = score

        if best is None:
            return None, None, None
        return best

    def _try_place_one(pri):
        q = s.q[pri]
        if not q:
            return False

        scan = min(s.max_scan_per_try, len(q))
        for _ in range(scan):
            p = _rr_next_pipeline(pri)
            if p is None:
                return False

            pid = getattr(p, "pipeline_id", None)
            if pid is None:
                continue

            if pid not in s.enqueued:
                continue
            if pid in s.dead_pipelines:
                _drop_pipeline(pid, pri)
                continue

            # Per-pipeline per-tick cap
            cur = scheduled_count_by_pid.get(pid, 0)
            if cur >= s.max_ops_per_pipeline_per_tick[pri]:
                continue

            status = p.runtime_status()

            if status.is_pipeline_successful():
                _drop_pipeline(pid, pri)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            opk = _op_key(op)
            sig = _op_sig(op)

            ram_target, lower = _ram_plan(pid, opk, sig, pri)
            cpu_des = _cpu_plan(pid, opk, pri)
            min_cpu = float(s.min_cpu[pri])

            pool_id, cpu_alloc, ram_alloc = _choose_pool(pri, ram_target, lower, cpu_des, min_cpu)
            if pool_id is None:
                continue

            # Final safety checks
            if ram_alloc <= 0 or cpu_alloc <= 0:
                continue
            if ram_alloc > local_ram[pool_id] + 1e-9 or cpu_alloc > local_cpu[pool_id] + 1e-9:
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Accounting
            local_cpu[pool_id] -= cpu_alloc
            local_ram[pool_id] -= ram_alloc
            scheduled_count_by_pid[pid] = cur + 1

            # Remember for result attribution & fallback allocations
            s.op_to_pid[opk] = pid
            st = s.op_state.get((pid, opk))
            if st is None:
                st = [0.0, 0.0, 0, 0, 0.0, 0.0]
                s.op_state[(pid, opk)] = st
            st[LAST_RAM] = float(ram_alloc)
            st[LAST_CPU] = float(cpu_alloc)

            return True

        return False

    made = 0
    while made < max_new:
        pri_order = []

        # Queries get first chance but are capped so interactive+batch make progress.
        if has_q and query_made < query_cap:
            pri_order.append(Priority.QUERY)

        # Interleave interactive and batch
        chosen = s.ib_pattern[s.ib_idx]
        s.ib_idx += 1
        if s.ib_idx >= len(s.ib_pattern):
            s.ib_idx = 0

        other = Priority.BATCH_PIPELINE if chosen == Priority.INTERACTIVE else Priority.INTERACTIVE
        pri_order.extend([chosen, other])

        # If we didn't prioritize queries (cap hit), still allow them as a fallback.
        if has_q and Priority.QUERY not in pri_order:
            pri_order.append(Priority.QUERY)

        placed = False
        for pri in pri_order:
            if pri == Priority.QUERY and (not has_q or query_made >= query_cap):
                continue
            if _try_place_one(pri):
                made += 1
                placed = True
                if pri == Priority.QUERY:
                    query_made += 1
                break

        if not placed:
            break

    s.last_made = made
    return suspensions, assignments
