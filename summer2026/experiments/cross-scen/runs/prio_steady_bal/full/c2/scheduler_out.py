_OOM_SUBSTRINGS = (
    "oom",
    "out of memory",
    "memoryerror",
    "cuda out of memory",
    "cannot allocate memory",
)


def _op_key(op):
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or id(op)


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    for s in _OOM_SUBSTRINGS:
        if s in msg:
            return True
    return False


def _op_sig(op):
    # Best-effort stable-ish signature for cross-pipeline learning.
    # Keep it cheap: a few small strings only.
    cls = type(op).__name__
    for attr in (
        "name",
        "op_name",
        "udf_name",
        "function_name",
        "fn_name",
        "sql_name",
        "query_name",
        "kind",
    ):
        v = getattr(op, attr, None)
        if v is None:
            continue
        try:
            sv = str(v)
        except Exception:
            continue
        if sv:
            if len(sv) > 80:
                sv = sv[:80]
            return (cls, attr, sv)
    return (cls,)


def _base_ram(pool, pri):
    # Use small absolute-ish defaults (capped) to avoid over-reservation.
    # RAM units in the simulator are GB in provided outputs.
    m = pool.max_ram_pool
    if pri == Priority.QUERY:
        # ~15GB on 500GB pool, capped.
        r = m * 0.03
        if r < 8:
            r = 8
        if r > 28:
            r = 28
        return r
    if pri == Priority.INTERACTIVE:
        r = m * 0.035
        if r < 10:
            r = 10
        if r > 36:
            r = 36
        return r
    # batch
    r = m * 0.02
    if r < 6:
        r = 6
    if r > 24:
        r = 24
    return r


def _base_cpu(pool, pri):
    # Favor concurrency; cap CPU-per-op to avoid queue buildup.
    mc = pool.max_cpu_pool
    if pri == Priority.QUERY:
        c = int(mc / 32) if mc >= 32 else 1
        if c < 2:
            c = 2
        if c > 16:
            c = 16
        return c
    if pri == Priority.INTERACTIVE:
        c = int(mc / 28) if mc >= 28 else 1
        if c < 2:
            c = 2
        if c > 12:
            c = 12
        return c
    c = int(mc / 48) if mc >= 48 else 1
    if c < 1:
        c = 1
    if c > 8:
        c = 8
    return c


@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    s.ticks = 0

    # Per-priority queues hold pipeline_ids (not objects) to avoid duplicates and allow lazy cleanup.
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

    # pipeline_id -> Pipeline
    s.pipelines = {}
    # pipeline_id -> enqueue tick (for optional aging)
    s.enq_tick = {}

    # Permanently stop scheduling a pipeline after too many OOM retries or non-OOM failure.
    s.dead = set()

    # Operator attribution and learning
    # op_key -> pipeline_id (best-effort)
    s.op_to_pid = {}
    # (pipeline_id, op_key) -> op_sig
    s.op_to_sig = {}

    # Retry / OOM learning
    s.op_retry = {}          # (pid, opk) -> retry_count (any failure)
    s.op_oom_lb = {}         # (pid, opk) -> largest ram that OOM'd
    s.sig_oom_lb = {}        # sig -> largest ram that OOM'd
    s.sig_ok = {}            # sig -> smallest successful allocated ram observed
    s.sig_ok_n = {}          # sig -> success count (for that sig)

    # Deficit scheduling to avoid starving INTERACTIVE under heavy QUERY load.
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    # Tuned to keep QUERY dominant while ensuring INTERACTIVE completes.
    s.quantum = {
        Priority.QUERY: 18.0,
        Priority.INTERACTIVE: 12.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Limits (performance + bounded retries)
    s.max_oom_retries = 6
    s.max_new_assignments_per_tick = 96
    s.max_new_assignments_per_pool = 24
    s.max_scan_per_pick = 32

    # Lazy queue cleanup
    s.last_compact_tick = 0
    s.compact_every = 500


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    # --- Ingest arrivals (assumed to be new arrivals for this tick) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead:
            continue
        # Update object reference (even if already tracked).
        s.pipelines[pid] = p
        if pid in s.enq_tick:
            continue
        s.enq_tick[pid] = s.ticks
        s.q[p.priority].append(pid)

    # --- Process results: learn OOM thresholds; stop hopeless pipelines; learn successful RAM sizes ---
    for r in results:
        failed = hasattr(r, "failed") and r.failed()
        ops = getattr(r, "ops", None) or []
        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pid.get(opk, None)
            if pid is None:
                continue

            sig = s.op_to_sig.get((pid, opk), None)
            if sig is None:
                sig = _op_sig(op)
                s.op_to_sig[(pid, opk)] = sig

            if failed:
                key = (pid, opk)
                s.op_retry[key] = s.op_retry.get(key, 0) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    alloc = getattr(r, "ram", None)
                    try:
                        alloc_val = float(alloc) if alloc is not None else 0.0
                    except Exception:
                        alloc_val = 0.0
                    if alloc_val <= 0.0:
                        alloc_val = s.op_oom_lb.get(key, 0.0) or 1.0

                    prev = s.op_oom_lb.get(key, 0.0)
                    if alloc_val > prev:
                        s.op_oom_lb[key] = alloc_val

                    prevs = s.sig_oom_lb.get(sig, 0.0)
                    if alloc_val > prevs:
                        s.sig_oom_lb[sig] = alloc_val

                    if s.op_retry[key] >= s.max_oom_retries:
                        s.dead.add(pid)
                else:
                    # Non-OOM failure: treat as terminal for this workload.
                    s.dead.add(pid)
            else:
                # Success: record smallest successful RAM seen for this signature.
                alloc = getattr(r, "ram", None)
                try:
                    alloc_val = float(alloc) if alloc is not None else 0.0
                except Exception:
                    alloc_val = 0.0
                if alloc_val > 0.0:
                    ok = s.sig_ok.get(sig, None)
                    if ok is None or alloc_val < ok:
                        s.sig_ok[sig] = alloc_val
                    s.sig_ok_n[sig] = s.sig_ok_n.get(sig, 0) + 1

    # --- Update deficits (fairness across priorities) ---
    s.deficit[Priority.QUERY] += s.quantum[Priority.QUERY]
    s.deficit[Priority.INTERACTIVE] += s.quantum[Priority.INTERACTIVE]
    s.deficit[Priority.BATCH_PIPELINE] += s.quantum[Priority.BATCH_PIPELINE]

    # Mild batch anti-starvation pulse.
    if s.ticks % 60 == 0:
        s.deficit[Priority.BATCH_PIPELINE] += 4.0

    # --- Lazy queue compaction (cheap: remove dead/unknown ids; no status checks here) ---
    if (s.ticks - s.last_compact_tick) >= s.compact_every:
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            old = s.q[pri]
            if old:
                new = []
                for pid in old:
                    if pid is None:
                        continue
                    if pid in s.dead:
                        continue
                    if pid not in s.pipelines and pid not in s.enq_tick:
                        continue
                    new.append(pid)
                s.q[pri] = new
                if s.q_pos[pri] >= len(new):
                    s.q_pos[pri] = 0
        s.last_compact_tick = s.ticks

    suspensions = []
    assignments = []

    # Tick-local caches to avoid repeated runtime_status / signature work.
    assignable_cache = {}  # pid -> op or None
    status_cache = {}      # pid -> runtime_status
    sig_cache = {}         # (pid, opk) -> sig
    scheduled_pids = set()

    # Track pool snapshots locally (hard constraint).
    num_pools = s.executor.num_pools
    local_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(num_pools)]
    local_ram = [s.executor.pools[i].avail_ram_pool for i in range(num_pools)]

    # Ramp-up: allow more initial placements so the cluster doesn't start underfilled.
    total_cap = s.max_new_assignments_per_tick * (2 if s.ticks <= 50 else 1)

    def pick_priority(no_pri):
        # Choose among priorities with the highest deficit, while maintaining QUERY preference in ties.
        best_pri = None
        best_def = -1e18

        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if pri in no_pri:
                continue
            q = s.q[pri]
            if not q:
                continue
            d = s.deficit.get(pri, 0.0)
            if d > best_def:
                best_def = d
                best_pri = pri
            elif d == best_def and best_pri is not None:
                # Tie-break by priority order: QUERY > INTERACTIVE > BATCH
                if pri == Priority.QUERY:
                    best_pri = pri
                elif pri == Priority.INTERACTIVE and best_pri == Priority.BATCH_PIPELINE:
                    best_pri = pri

        # If all deficits are exhausted, still try strict priority to keep progress.
        if best_pri is None:
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if pri in no_pri:
                    continue
                if s.q[pri]:
                    return pri
            return None

        return best_pri

    def pick_pid_op(pri):
        q = s.q[pri]
        if not q:
            return None, None

        n = len(q)
        if n == 0:
            return None, None

        pos = s.q_pos.get(pri, 0)
        if pos >= n:
            pos = 0

        scanned = 0
        while scanned < s.max_scan_per_pick and scanned < n:
            if pos >= n:
                pos = 0
            idx = pos
            pid = q[idx]
            pos += 1
            scanned += 1

            if pid is None:
                continue
            if pid in s.dead:
                q[idx] = None
                continue
            if pid in scheduled_pids:
                continue

            p = s.pipelines.get(pid, None)
            if p is None:
                # Might be older entry; keep it for now but don't spend time.
                continue

            st = status_cache.get(pid, None)
            if st is None:
                st = p.runtime_status()
                status_cache[pid] = st

            if st.is_pipeline_successful():
                # Clean up lazily.
                q[idx] = None
                s.pipelines.pop(pid, None)
                s.enq_tick.pop(pid, None)
                continue

            if pid in assignable_cache:
                op = assignable_cache[pid]
            else:
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                op = ops[0] if ops else None
                assignable_cache[pid] = op

            if op is None:
                continue

            s.q_pos[pri] = pos if pos < n else 0
            return pid, op

        s.q_pos[pri] = pos if pos < n else 0
        return None, None

    def size_ram(pool, pri, pid, op):
        opk = _op_key(op)
        key = (pid, opk)

        sig = sig_cache.get(key, None)
        if sig is None:
            sig = s.op_to_sig.get(key, None)
            if sig is None:
                sig = _op_sig(op)
                s.op_to_sig[key] = sig
            sig_cache[key] = sig

        base = _base_ram(pool, pri)

        lb1 = s.op_oom_lb.get(key, 0.0)
        lb2 = s.sig_oom_lb.get(sig, 0.0)
        lb = lb1 if lb1 >= lb2 else lb2

        ok = s.sig_ok.get(sig, None)

        # If we've seen an OOM lower bound, allocate above it.
        if lb and lb > 0.0:
            ram = lb * 1.6 + 1.0
            if ok is not None and ok > lb:
                # Prefer a known successful smallest allocation if available.
                if ok < ram:
                    ram = ok
            if ram < base:
                ram = base
        else:
            # No OOM: start small; if we have a successful hint, use it (it should already be near-minimal).
            if ok is not None and ok > 0.0:
                ram = ok
                if ram < base:
                    ram = base
                # For batch only: gentle down-probing after many successes to reduce over-reservation.
                if pri == Priority.BATCH_PIPELINE and s.sig_ok_n.get(sig, 0) >= 12:
                    probe = ram * 0.9
                    if probe >= base:
                        ram = probe
            else:
                ram = base

        if ram < 1.0:
            ram = 1.0
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        return ram, sig, opk

    # --- Main placement loop ---
    for pool_id in range(num_pools):
        if len(assignments) >= total_cap:
            break

        pool = s.executor.pools[pool_id]
        placed_in_pool = 0

        # Per-pool attempt loop; bounded to avoid spending too long per tick.
        attempts = 0
        while (
            placed_in_pool < s.max_new_assignments_per_pool
            and len(assignments) < total_cap
            and local_cpu[pool_id] >= 1
            and local_ram[pool_id] >= 1
        ):
            attempts += 1
            if attempts > (s.max_new_assignments_per_pool * 5):
                break

            no_pri = set()
            pri = pick_priority(no_pri)
            if pri is None:
                break

            # Try a small number of priority fallbacks within this pool.
            found = False
            for _ in range(3):
                pid, op = pick_pid_op(pri)
                if pid is None:
                    no_pri.add(pri)
                    pri = pick_priority(no_pri)
                    if pri is None:
                        break
                    continue

                # Size request
                cpu_req = _base_cpu(pool, pri)
                if cpu_req > local_cpu[pool_id]:
                    cpu_req = int(local_cpu[pool_id])
                if cpu_req < 1:
                    cpu_req = 1

                ram_req, sig, opk = size_ram(pool, pri, pid, op)
                if ram_req > local_ram[pool_id]:
                    # Can't fit now; try another priority/op.
                    no_pri.add(pri)
                    pri = pick_priority(no_pri)
                    if pri is None:
                        break
                    continue

                # Hard cap retries: if this op has failed too many times (any failure), give up.
                rkey = (pid, opk)
                if s.op_retry.get(rkey, 0) >= s.max_oom_retries and pid in s.dead:
                    no_pri.add(pri)
                    pri = pick_priority(no_pri)
                    if pri is None:
                        break
                    continue

                # Assign
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

                s.op_to_pid[opk] = pid
                s.op_to_sig[(pid, opk)] = sig

                local_cpu[pool_id] -= cpu_req
                local_ram[pool_id] -= ram_req

                # Consume deficit (cost per op).
                d = s.deficit.get(pri, 0.0) - 1.0
                s.deficit[pri] = d if d > 0.0 else 0.0

                scheduled_pids.add(pid)
                placed_in_pool += 1
                found = True
                break

            if not found:
                break

    return suspensions, assignments
