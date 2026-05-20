@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    # Tick counter
    s.ticks = 0

    # Active pipelines (tracked by pipeline_id)
    s.enqueued = set()
    s.pipeline_by_id = {}     # pipeline_id -> Pipeline
    s.pipeline_meta = {}      # pipeline_id -> {"priority": pri, "enq_tick": int}

    # Per-priority "wait lists" (append-only) + round-robin cursor.
    s.wait_list = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Count of currently-active (not yet completed/dead) pipelines per priority.
    s.active_count = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Operator learning state (keyed by (pipeline_id, op_key))
    # Values:
    #   oom_ram: max RAM allocation that OOM'd
    #   ok_ram:  RAM allocation that succeeded (if any)
    #   cpu_hint: CPU allocation hint (e.g., after timeout)
    #   retries: total retry count
    #   non_oom_failures: count of non-OOM failures
    #   last_ram/last_cpu: last attempted allocs
    s.op_state = {}

    # Map operator key -> pipeline_id (for attributing ExecutionResult back)
    s.op_to_pipeline = {}

    # Per-priority deficit round robin state (operator-level fairness)
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Tunables (kept simple + fast)
    s.max_new_assignments_per_tick = 64
    s.max_scan_per_pick = 32

    # Retry policy
    s.max_retries_per_op = 6
    s.max_non_oom_failures_per_op = 2

    # OOM / timeout bump behavior
    s.oom_growth = 1.8
    s.oom_add = 2.0
    s.timeout_cpu_growth = 1.8
    s.timeout_cpu_add = 1.0

    # Base resource sizing (absolute, not proportional to pool size)
    s.base_cpu = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.min_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.base_ram = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 16.0,
        Priority.BATCH_PIPELINE: 12.0,
    }
    s.min_ram = 2.0

    # DRR quantum (slightly interactive-forward to avoid massive interactive timeouts)
    s.quantum_base = {
        Priority.QUERY: 14,
        Priority.INTERACTIVE: 12,
        Priority.BATCH_PIPELINE: 1,
    }
    s.deficit_cap = 64  # avoid huge bursts after idle periods

    # Batch as "leftover" policy knobs
    s.batch_free_frac_cpu = 0.12
    s.batch_free_frac_ram = 0.12

    # Occasional cleanup of stale wait_list entries (append-only lists)
    s._cleanup_every_ticks = 5000


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        # Use object identity; stable and unique within the simulator process lifetime.
        return id(op)

    def _err_str(err):
        if err is None:
            return ""
        try:
            return str(err).lower()
        except Exception:
            return ""

    def _is_oom_error(err):
        msg = _err_str(err)
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _is_timeout_error(err):
        msg = _err_str(err)
        return ("timeout" in msg) or ("timed out" in msg) or ("time limit" in msg)

    def _drop_pipeline(pid):
        if pid not in s.enqueued:
            return
        s.enqueued.discard(pid)
        meta = s.pipeline_meta.pop(pid, None)
        if meta is not None:
            pri = meta.get("priority", None)
            if pri in s.active_count and s.active_count[pri] > 0:
                s.active_count[pri] -= 1
        s.pipeline_by_id.pop(pid, None)

    # --- Ingest new pipelines (assumed to be arrivals for this tick) ---
    for p in (pipelines or []):
        pid = p.pipeline_id
        if pid in s.enqueued:
            # Update reference in case simulator hands a new object wrapper
            s.pipeline_by_id[pid] = p
            continue
        s.enqueued.add(pid)
        s.pipeline_by_id[pid] = p
        s.pipeline_meta[pid] = {"priority": p.priority, "enq_tick": s.ticks}
        s.wait_list[p.priority].append(pid)
        s.active_count[p.priority] += 1

    # --- Process results: learn RAM/CPU needs and retry boundaries ---
    for r in (results or []):
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = False

        err = getattr(r, "error", None)
        is_oom = failed and _is_oom_error(err)
        is_to = failed and (not is_oom) and _is_timeout_error(err)

        r_cpu = getattr(r, "cpu", None)
        r_ram = getattr(r, "ram", None)

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)
            if pid is None:
                continue

            st_key = (pid, opk)
            st = s.op_state.get(st_key)
            if st is None:
                st = {
                    "oom_ram": 0.0,
                    "ok_ram": None,
                    "cpu_hint": None,
                    "retries": 0,
                    "non_oom_failures": 0,
                    "last_ram": None,
                    "last_cpu": None,
                }
                s.op_state[st_key] = st

            # Prefer explicit r.ram / r.cpu, else fall back to last attempted.
            alloc_ram = r_ram if (r_ram is not None and r_ram > 0) else st.get("last_ram", None)
            alloc_cpu = r_cpu if (r_cpu is not None and r_cpu > 0) else st.get("last_cpu", None)

            if failed:
                st["retries"] += 1

                if is_oom:
                    if alloc_ram is not None and alloc_ram > 0:
                        if alloc_ram > st["oom_ram"]:
                            st["oom_ram"] = float(alloc_ram)
                else:
                    st["non_oom_failures"] += 1
                    if is_to and alloc_cpu is not None and alloc_cpu > 0:
                        prev = st["cpu_hint"] if (st["cpu_hint"] is not None) else 0.0
                        bumped = float(alloc_cpu) * s.timeout_cpu_growth + s.timeout_cpu_add
                        if bumped > prev:
                            st["cpu_hint"] = bumped

                # Enforce retry caps (avoid infinite loops)
                if st["retries"] > s.max_retries_per_op or st["non_oom_failures"] > s.max_non_oom_failures_per_op:
                    _drop_pipeline(pid)

            else:
                # Success: record a known-good RAM allocation (don’t try to shrink; only used if retried)
                if alloc_ram is not None and alloc_ram > 0:
                    ok = st.get("ok_ram", None)
                    if ok is None or float(alloc_ram) < ok:
                        st["ok_ram"] = float(alloc_ram)

            # This op mapping is no longer needed after a terminal result.
            # (If the same op is retried, it will be re-mapped on re-assignment.)
            if opk in s.op_to_pipeline:
                del s.op_to_pipeline[opk]

    # --- Optional cleanup of stale entries in append-only wait lists ---
    if s.ticks % s._cleanup_every_ticks == 0:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            lst = s.wait_list[pri]
            if not lst:
                s.rr_cursor[pri] = 0
                continue
            # Rebuild list to contain only still-enqueued pipeline_ids
            new_lst = []
            for pid in lst:
                if pid in s.enqueued:
                    new_lst.append(pid)
            s.wait_list[pri] = new_lst
            # Clamp cursor
            if new_lst:
                s.rr_cursor[pri] = s.rr_cursor[pri] % len(new_lst)
            else:
                s.rr_cursor[pri] = 0

    # If nothing active, do nothing.
    if (s.active_count[Priority.QUERY] + s.active_count[Priority.INTERACTIVE] + s.active_count[Priority.BATCH_PIPELINE]) == 0:
        return [], []

    # --- Build local availability snapshots (must self-account within this tick) ---
    num_pools = s.executor.num_pools
    pools = s.executor.pools
    local_cpu = [pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [pools[i].avail_ram_pool for i in range(num_pools)]
    max_cpu = [pools[i].max_cpu_pool for i in range(num_pools)]
    max_ram = [pools[i].max_ram_pool for i in range(num_pools)]

    total_free_cpu = 0.0
    total_free_ram = 0.0
    total_max_cpu = 0.0
    total_max_ram = 0.0
    for i in range(num_pools):
        total_free_cpu += float(local_cpu[i])
        total_free_ram += float(local_ram[i])
        total_max_cpu += float(max_cpu[i])
        total_max_ram += float(max_ram[i])

    # --- Update DRR deficits ---
    # Slightly dynamic: if interactive backlog dominates, bias quantum toward interactive to avoid timeouts.
    qn = dict(s.quantum_base)
    q_active = s.active_count[Priority.QUERY]
    i_active = s.active_count[Priority.INTERACTIVE]
    if i_active > q_active and i_active > 0:
        qn[Priority.INTERACTIVE] = qn[Priority.INTERACTIVE] + 2
        qn[Priority.QUERY] = max(10, qn[Priority.QUERY] - 1)
    elif q_active > (2 * i_active + 10):
        qn[Priority.QUERY] = qn[Priority.QUERY] + 2

    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.deficit[pri] = min(s.deficit_cap, s.deficit[pri] + int(qn[pri]))

    # --- Candidate selection helpers ---
    scheduled_pids = set()
    blocked_pids = set()

    def _pick_runnable(pri):
        """Return (pipeline, op) or (None, None). Scan a bounded number of candidates."""
        if s.active_count[pri] <= 0:
            return None, None

        lst = s.wait_list[pri]
        n = len(lst)
        if n <= 0:
            return None, None

        scans = 0
        max_scans = s.max_scan_per_pick if s.max_scan_per_pick < n else n
        while scans < max_scans:
            idx = s.rr_cursor[pri] % n
            s.rr_cursor[pri] += 1
            pid = lst[idx]
            scans += 1

            if pid in blocked_pids:
                continue
            if pid not in s.enqueued:
                continue
            if pid in scheduled_pids:
                continue

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                _drop_pipeline(pid)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            op = ops[0]
            return p, op

        return None, None

    def _desired_resources_for(pool_id, pri, pid, opk):
        """Return (cpu_req, ram_req, hard_ram). hard_ram means don't under-allocate (avoid repeat OOM churn)."""
        st = s.op_state.get((pid, opk), None)
        hard_ram = False

        # CPU
        cpu_req = float(s.base_cpu[pri])
        if st is not None:
            ch = st.get("cpu_hint", None)
            if ch is not None and ch > cpu_req:
                cpu_req = float(ch)

        # RAM
        ram_req = float(s.base_ram[pri])
        if st is not None:
            ok = st.get("ok_ram", None)
            if ok is not None and ok > 0:
                ram_req = float(ok)
                hard_ram = True
            else:
                oom_ram = float(st.get("oom_ram", 0.0) or 0.0)
                if oom_ram > 0:
                    ram_req = oom_ram * float(s.oom_growth) + float(s.oom_add)
                    hard_ram = True

        # Enforce minima
        if cpu_req < float(s.min_cpu[pri]):
            cpu_req = float(s.min_cpu[pri])
        if ram_req < float(s.min_ram):
            ram_req = float(s.min_ram)

        # Clip to pool maxima
        if cpu_req > float(max_cpu[pool_id]):
            cpu_req = float(max_cpu[pool_id])
        if ram_req > float(max_ram[pool_id]):
            ram_req = float(max_ram[pool_id])
            hard_ram = True  # at ceiling; don't under-allocate

        return cpu_req, ram_req, hard_ram

    def _choose_pool_for(pri, pid, opk, cpu_req, ram_req, hard_ram):
        """Pick a pool and final (cpu, ram). CPU can scale down; RAM is treated as hard if learned."""
        best = None
        best_score = None

        for pool_id in range(num_pools):
            # RAM feasibility
            if float(local_ram[pool_id]) < float(ram_req):
                continue

            # CPU feasibility at least min_cpu
            min_cpu = float(s.min_cpu[pri])
            if float(local_cpu[pool_id]) < min_cpu:
                continue

            # CPU can scale down to what's available (but not below min_cpu)
            cpu_alloc = float(cpu_req)
            if cpu_alloc > float(local_cpu[pool_id]):
                cpu_alloc = float(local_cpu[pool_id])
            if cpu_alloc < min_cpu:
                continue

            # For learned RAM (hard), don't under-allocate (we already enforced local_ram >= ram_req)
            ram_alloc = float(ram_req)

            # Score: prefer full CPU, then best-fit RAM packing, then best-fit CPU packing.
            full_cpu = 1 if float(local_cpu[pool_id]) >= float(cpu_req) else 0
            ram_left = float(local_ram[pool_id]) - ram_alloc
            cpu_left = float(local_cpu[pool_id]) - cpu_alloc
            score = (full_cpu, -ram_left, -cpu_left)

            if best is None or score > best_score:
                best = (pool_id, cpu_alloc, ram_alloc)
                best_score = score

        return best

    def _batch_allowed():
        # Treat batch as leftovers when higher priorities are present and the cluster is tight.
        if (s.active_count[Priority.QUERY] + s.active_count[Priority.INTERACTIVE]) <= 0:
            return True
        cpu_ok = total_free_cpu >= (total_max_cpu * float(s.batch_free_frac_cpu))
        ram_ok = total_free_ram >= (total_max_ram * float(s.batch_free_frac_ram))
        return cpu_ok and ram_ok

    # --- Scheduling loop ---
    assignments = []
    suspensions = []

    def _priority_try_order():
        # Prefer positive deficit; tie-break by true priority (QUERY > INTERACTIVE > BATCH).
        pris = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        pris.sort(key=lambda p: (s.deficit[p], 1 if p == Priority.BATCH_PIPELINE else 2 if p == Priority.INTERACTIVE else 3), reverse=True)
        return pris

    made_progress = True
    attempts = 0
    while attempts < s.max_new_assignments_per_tick and made_progress:
        made_progress = False

        # If no pool has any reasonable headroom, stop early.
        any_headroom = False
        for i in range(num_pools):
            if float(local_cpu[i]) >= 1.0 and float(local_ram[i]) >= float(s.min_ram):
                any_headroom = True
                break
        if not any_headroom:
            break

        pri_order = _priority_try_order()

        picked_any = False
        for pri in pri_order:
            if s.active_count[pri] <= 0:
                continue

            if pri == Priority.BATCH_PIPELINE and not _batch_allowed():
                continue

            # If everyone has <=0 deficit, still be work-conserving: allow QUERY/INTERACTIVE first.
            if (s.deficit[Priority.QUERY] <= 0 and s.deficit[Priority.INTERACTIVE] <= 0 and s.deficit[Priority.BATCH_PIPELINE] <= 0):
                if pri == Priority.BATCH_PIPELINE and (s.active_count[Priority.QUERY] + s.active_count[Priority.INTERACTIVE]) > 0:
                    continue
            else:
                if s.deficit[pri] <= 0:
                    continue

            p, op = _pick_runnable(pri)
            if p is None:
                continue

            pid = p.pipeline_id
            opk = _op_key(op)

            # Enforce retry cap before scheduling again (if it has already exceeded, drop now)
            st = s.op_state.get((pid, opk), None)
            if st is not None and st.get("retries", 0) > s.max_retries_per_op:
                _drop_pipeline(pid)
                blocked_pids.add(pid)
                continue

            # Choose best pool for this operator
            # Evaluate per-pool desired resources only once using a conservative baseline derived from pool 0,
            # then rely on pool-specific clipping in _desired_resources_for. (Keeps it fast.)
            # Still, we compute per-pool in the loop to respect max_cpu/max_ram differences.
            best_choice = None
            best_choice_score = None

            for pool_id in range(num_pools):
                cpu_req, ram_req, hard_ram = _desired_resources_for(pool_id, pri, pid, opk)

                # Batch can opportunistically shrink RAM to fit small fragments (only if not learned/hard).
                if pri == Priority.BATCH_PIPELINE and not hard_ram:
                    # Try a small set of RAM options (bounded constant work).
                    ram_options = [ram_req, 8.0, 4.0, float(s.min_ram)]
                else:
                    ram_options = [ram_req]

                for rr in ram_options:
                    if rr < float(s.min_ram):
                        continue
                    if rr > float(max_ram[pool_id]):
                        continue
                    # Check feasibility quickly
                    if float(local_ram[pool_id]) < float(rr):
                        continue
                    if float(local_cpu[pool_id]) < float(s.min_cpu[pri]):
                        continue

                    cpu_alloc = cpu_req
                    if cpu_alloc > float(local_cpu[pool_id]):
                        cpu_alloc = float(local_cpu[pool_id])
                    if cpu_alloc < float(s.min_cpu[pri]):
                        continue

                    # Score pools: prefer full CPU and best-fit RAM
                    full_cpu = 1 if float(local_cpu[pool_id]) >= float(cpu_req) else 0
                    ram_left = float(local_ram[pool_id]) - float(rr)
                    cpu_left = float(local_cpu[pool_id]) - float(cpu_alloc)
                    score = (full_cpu, -ram_left, -cpu_left)

                    if best_choice is None or score > best_choice_score:
                        best_choice = (pool_id, float(cpu_alloc), float(rr), float(cpu_req), float(rr), hard_ram)
                        best_choice_score = score

            if best_choice is None:
                blocked_pids.add(pid)
                continue

            pool_id, cpu_alloc, ram_alloc, _, _, _ = best_choice

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Map op to pipeline for result attribution
            s.op_to_pipeline[opk] = pid

            # Record last attempted resources for learning
            st_key = (pid, opk)
            st = s.op_state.get(st_key)
            if st is None:
                st = {
                    "oom_ram": 0.0,
                    "ok_ram": None,
                    "cpu_hint": None,
                    "retries": 0,
                    "non_oom_failures": 0,
                    "last_ram": None,
                    "last_cpu": None,
                }
                s.op_state[st_key] = st
            st["last_ram"] = float(ram_alloc)
            st["last_cpu"] = float(cpu_alloc)

            # Local accounting (critical: executor snapshots don't decrement automatically)
            local_cpu[pool_id] = float(local_cpu[pool_id]) - float(cpu_alloc)
            local_ram[pool_id] = float(local_ram[pool_id]) - float(ram_alloc)
            total_free_cpu -= float(cpu_alloc)
            total_free_ram -= float(ram_alloc)

            # DRR cost is 1 per operator scheduled
            s.deficit[pri] -= 1

            scheduled_pids.add(pid)
            attempts += 1
            made_progress = True
            picked_any = True
            break

        if not picked_any:
            break

    return suspensions, assignments
