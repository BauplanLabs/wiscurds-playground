@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority ring queues of pipeline_ids (avoid O(n) pop(0)).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.q_head = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.q_none = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Active pipeline registry
    s.pipeline_by_id = {}   # pipeline_id -> Pipeline
    s.enqueued = set()      # pipeline_ids currently tracked/enqueued
    s.enqueue_tick = {}     # pipeline_id -> tick enqueued
    s.cooldown_until = {}   # pipeline_id -> tick; skip re-checking until then

    # Failure / retry tracking
    s.dead_pipelines = set()  # pipeline_ids to stop scheduling

    # Operator-level learned memory + retry caps
    s.op_to_pid = {}          # id(op) -> pipeline_id
    s.pipeline_opkeys = {}    # pipeline_id -> set(id(op)) for cleanup
    s.op_ram_hint = {}        # id(op) -> next RAM to try
    s.op_oom_retries = {}     # id(op) -> count
    s.op_fail_retries = {}    # id(op) -> non-OOM failure count

    # Scheduling knobs (keep per-tick work bounded)
    s.scan_limit = 32
    s.max_new_assignments_per_tick = 48
    s.max_new_assignments_per_pool = 16

    # CPU share guardrails per pool per tick (prevents starvation under strict priority)
    s.cpu_share = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # How many ops from the same pipeline we allow to be ASSIGNED in a single tick
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Retry bounds (hard-stop to prevent infinite loops)
    s.max_oom_retries = 5
    s.max_non_oom_retries = 3

    # Priority selection sequence (soft guidance; budgets enforce fairness)
    s.pri_seq = [
        Priority.QUERY,
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
    ]
    s.seq_ptr = {}  # pool_id -> index into pri_seq


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _drop_pipeline(pid):
        s.enqueued.discard(pid)
        s.pipeline_by_id.pop(pid, None)
        s.enqueue_tick.pop(pid, None)
        s.cooldown_until.pop(pid, None)

        opkeys = s.pipeline_opkeys.pop(pid, None)
        if opkeys:
            for ok in opkeys:
                s.op_to_pid.pop(ok, None)
                s.op_ram_hint.pop(ok, None)
                s.op_oom_retries.pop(ok, None)
                s.op_fail_retries.pop(ok, None)

    def _maybe_compact_queue(pri):
        q = s.wait_q[pri]
        none_ct = s.q_none[pri]
        if none_ct < 256:
            return
        if len(q) < 128:
            return
        # Avoid expensive compaction on very large queues.
        if len(q) > 4096:
            return
        if none_ct < (len(q) // 2):
            return

        new_q = []
        for pid in q:
            if pid is None:
                continue
            if pid in s.dead_pipelines:
                continue
            if pid not in s.pipeline_by_id:
                continue
            new_q.append(pid)

        s.wait_q[pri] = new_q
        s.q_head[pri] = 0
        s.q_none[pri] = 0

    def _min_cpu(pri):
        if pri == Priority.BATCH_PIPELINE:
            return 1.0
        return 2.0

    def _cpu_target(pool, pri):
        # Sqrt scaling: bigger clusters grant more CPU per op, but cap to avoid low concurrency.
        mc = float(getattr(pool, "max_cpu_pool", 1.0) or 1.0)
        root = mc ** 0.5
        if pri == Priority.QUERY:
            return max(4.0, min(48.0, root * 1.5))
        if pri == Priority.INTERACTIVE:
            return max(2.0, min(32.0, root * 1.0))
        return max(1.0, min(24.0, root * 0.75))

    def _ram_base(pool, pri):
        # Conservative-but-not-huge defaults; capped so larger clusters don't balloon per-op RAM.
        mr = float(getattr(pool, "max_ram_pool", 1.0) or 1.0)
        if pri == Priority.QUERY:
            base = max(12.0, min(32.0, mr * 0.010))
        elif pri == Priority.INTERACTIVE:
            base = max(10.0, min(24.0, mr * 0.008))
        else:
            base = max(8.0, min(20.0, mr * 0.006))
        return min(base, mr)

    def _ram_target(pool, pri, op_ok):
        mr = float(getattr(pool, "max_ram_pool", 1.0) or 1.0)
        base = _ram_base(pool, pri)
        hint = s.op_ram_hint.get(op_ok, None)
        if hint is None:
            return min(max(1.0, base), mr)
        return min(max(1.0, max(base, float(hint))), mr)

    def _pick_op(pri, pool_id, cpu_left, ram_left, budget_left, scheduled_counts, enforce_budget):
        q = s.wait_q[pri]
        n = len(q)
        if n == 0:
            return None

        head = s.q_head[pri]
        pool = s.executor.pools[pool_id]
        minc = _min_cpu(pri)

        attempts = s.scan_limit
        if n < attempts:
            attempts = n

        for _ in range(attempts):
            idx = head
            pid = q[idx]
            head += 1
            if head >= n:
                head = 0

            if pid is None:
                continue
            if pid in s.dead_pipelines:
                q[idx] = None
                s.q_none[pri] += 1
                continue

            p = s.pipeline_by_id.get(pid, None)
            if p is None:
                q[idx] = None
                s.q_none[pri] += 1
                continue

            # Per-tick per-pipeline cap
            if scheduled_counts.get(pid, 0) >= s.max_ops_per_pipeline_per_tick.get(pri, 1):
                continue

            # Skip pipelines we recently found blocked
            if s.cooldown_until.get(pid, 0) > s.ticks:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                q[idx] = None
                s.q_none[pri] += 1
                _drop_pipeline(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Likely blocked on running parents; don't re-check immediately.
                s.cooldown_until[pid] = s.ticks + (2 if pri != Priority.BATCH_PIPELINE else 3)
                continue

            op = ops[0]
            op_ok = id(op)

            # RAM fit check first (more likely to constrain)
            ram_req = _ram_target(pool, pri, op_ok)
            if ram_req > ram_left:
                continue

            # CPU sizing + fairness budget
            cpu_req = min(_cpu_target(pool, pri), cpu_left)
            if cpu_req < minc:
                continue

            if enforce_budget:
                if budget_left < minc:
                    return None
                if cpu_req > budget_left:
                    cpu_req = budget_left
                if cpu_req < minc:
                    return None

            # Good candidate
            s.q_head[pri] = head
            return pid, p, op, cpu_req, ram_req

        s.q_head[pri] = head
        _maybe_compact_queue(pri)
        return None

    # --- Ingest new pipelines (pipelines param is assumed "new arrivals") ---
    for p in pipelines or []:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            # Refresh object reference in case simulator reuses wrappers
            s.pipeline_by_id[pid] = p
            continue
        s.enqueued.add(pid)
        s.pipeline_by_id[pid] = p
        s.enqueue_tick[pid] = s.ticks
        s.cooldown_until[pid] = 0
        s.pipeline_opkeys[pid] = set()

        s.wait_q[p.priority].append(pid)

    # --- Process results (update OOM hints / retry counters; drop completed pipelines promptly) ---
    for r in results or []:
        failed = False
        if hasattr(r, "failed"):
            failed = r.failed()

        oom = _is_oom_error(getattr(r, "error", None)) if failed else False
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        for op in (getattr(r, "ops", None) or []):
            op_ok = id(op)
            pid = s.op_to_pid.get(op_ok, None)
            if pid is None:
                continue

            # Pipeline likely unblocked now; check again soon.
            s.cooldown_until[pid] = 0

            if failed:
                if oom:
                    prev_hint = s.op_ram_hint.get(op_ok, None)
                    alloc = getattr(r, "ram", None)
                    try:
                        alloc = float(alloc) if alloc is not None else None
                    except Exception:
                        alloc = None

                    base = prev_hint if prev_hint is not None else (alloc if alloc is not None and alloc > 0 else 1.0)
                    # Aggressive but bounded growth to converge quickly.
                    new_hint = max(base * 1.8, base + 4.0)
                    if pool is not None:
                        mr = float(getattr(pool, "max_ram_pool", 1.0) or 1.0)
                        if new_hint > mr:
                            new_hint = mr
                    s.op_ram_hint[op_ok] = new_hint

                    s.op_oom_retries[op_ok] = s.op_oom_retries.get(op_ok, 0) + 1
                    if s.op_oom_retries[op_ok] > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)
                else:
                    s.op_fail_retries[op_ok] = s.op_fail_retries.get(op_ok, 0) + 1
                    if s.op_fail_retries[op_ok] > s.max_non_oom_retries:
                        s.dead_pipelines.add(pid)
                        _drop_pipeline(pid)

        # If we can attribute this result to a pipeline, drop it immediately when done.
        # (We may not always be able to, so this is best-effort.)
        any_ops = getattr(r, "ops", None) or []
        if any_ops:
            pid0 = s.op_to_pid.get(id(any_ops[0]), None)
            if pid0 is not None:
                p0 = s.pipeline_by_id.get(pid0, None)
                if p0 is not None:
                    st0 = p0.runtime_status()
                    if st0.is_pipeline_successful():
                        _drop_pipeline(pid0)

    # Nothing to do
    if not s.pipeline_by_id:
        return [], []

    suspensions = []
    assignments = []

    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    scheduled_counts = {}  # pipeline_id -> assigned ops this tick (cap per pipeline)

    # --- Main scheduling loop (per pool, bounded assignments, fair CPU budgets) ---
    total_left = s.max_new_assignments_per_tick

    for pool_id in range(s.executor.num_pools):
        if total_left <= 0:
            break

        cpu_av = float(local_cpu[pool_id])
        ram_av = float(local_ram[pool_id])
        if cpu_av < 1.0 or ram_av < 1.0:
            continue

        # Per-pool per-tick CPU budgets to prevent QUERY-only saturation.
        bq = cpu_av * s.cpu_share[Priority.QUERY]
        bi = cpu_av * s.cpu_share[Priority.INTERACTIVE]
        bb = cpu_av * s.cpu_share[Priority.BATCH_PIPELINE]
        budgets = {
            Priority.QUERY: bq,
            Priority.INTERACTIVE: bi,
            Priority.BATCH_PIPELINE: bb,
        }

        if pool_id not in s.seq_ptr:
            s.seq_ptr[pool_id] = 0
        seq_ptr = s.seq_ptr[pool_id]

        per_pool_left = s.max_new_assignments_per_pool
        if per_pool_left > total_left:
            per_pool_left = total_left

        while per_pool_left > 0 and cpu_av >= 1.0 and ram_av >= 1.0:
            # Choose a soft-guidance priority, then fall back to important-first.
            pri_try = s.pri_seq[seq_ptr]
            seq_ptr += 1
            if seq_ptr >= len(s.pri_seq):
                seq_ptr = 0

            picked = None

            # Pass 1: enforce budgets
            for pri in (pri_try, Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if budgets.get(pri, 0.0) < _min_cpu(pri):
                    continue
                picked = _pick_op(
                    pri=pri,
                    pool_id=pool_id,
                    cpu_left=cpu_av,
                    ram_left=ram_av,
                    budget_left=budgets.get(pri, 0.0),
                    scheduled_counts=scheduled_counts,
                    enforce_budget=True,
                )
                if picked is not None:
                    break

            # Pass 2: if budgets block work but resources remain, allow borrowing to avoid idling
            if picked is None:
                for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    picked = _pick_op(
                        pri=pri,
                        pool_id=pool_id,
                        cpu_left=cpu_av,
                        ram_left=ram_av,
                        budget_left=cpu_av,  # effectively unlimited vs this pool's remaining CPU
                        scheduled_counts=scheduled_counts,
                        enforce_budget=False,
                    )
                    if picked is not None:
                        break

            if picked is None:
                break

            pid, p, op, cpu_req, ram_req = picked

            # Final safety checks (no overallocation)
            if cpu_req > cpu_av or ram_req > ram_av:
                # Shouldn't happen, but avoid aborting the simulation.
                s.cooldown_until[pid] = s.ticks + 1
                continue

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

            # Attribute op -> pipeline for result processing; track for cleanup
            op_ok = id(op)
            s.op_to_pid[op_ok] = pid
            if pid in s.pipeline_opkeys:
                s.pipeline_opkeys[pid].add(op_ok)

            # Update tick-local accounting
            cpu_av -= cpu_req
            ram_av -= ram_req
            local_cpu[pool_id] = cpu_av
            local_ram[pool_id] = ram_av

            if p.priority in budgets:
                # If we scheduled via "borrow" pass, budgets may go negative; clamp to 0.
                budgets[p.priority] = budgets.get(p.priority, 0.0) - cpu_req
                if budgets[p.priority] < 0.0:
                    budgets[p.priority] = 0.0

            scheduled_counts[pid] = scheduled_counts.get(pid, 0) + 1
            s.cooldown_until[pid] = 0

            per_pool_left -= 1
            total_left -= 1
            if total_left <= 0:
                break

        s.seq_ptr[pool_id] = seq_ptr

    return suspensions, assignments
