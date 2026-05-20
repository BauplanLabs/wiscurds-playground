@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """
    Improved priority-aware scheduler focused on high completion rate + low weighted latency.

    Key changes vs prior version:
      - Stop sizing CPU/RAM as a fraction of pool max (which kept concurrency ~constant across cluster sizes).
      - Use small, mostly fixed per-op CPU/RAM to increase concurrency and reduce queueing-driven timeouts.
      - Add TIMEOUT-aware CPU bumps (like the prior OOM-aware RAM bumps) to converge on needed CPU.
      - Weighted fairness between QUERY and INTERACTIVE (alternating-first with starvation aging).
      - Allow limited intra-pipeline parallelism when multiple ops are runnable (DAG fan-out).
      - Opportunistic batch only when high-priority work cannot use resources (no preemption needed).
    """
    s.ticks = 0

    # Per-priority pipeline queues (store Pipeline objects; round-robin via cursors).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track pipelines we've already enqueued (and completion/dead sets to prevent re-enqueue churn).
    s.enqueued = set()
    s.done_pipelines = set()
    s.dead_pipelines = set()

    # Per-pipeline metadata
    # { pipeline_id: {"enqueued_tick": int, "last_scheduled_tick": int} }
    s.pipeline_meta = {}

    # Per-op resource hints learned from failures
    # keys: (pipeline_id, op_key) -> hint_value
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Map op_key -> pipeline_id for attributing results back to pipeline/op
    s.op_to_pipeline = {}

    # Track repeated failures to avoid infinite thrash on non-resource errors
    # keys: (pipeline_id, op_key, err_class) -> count
    s.fail_counts = {}

    # Resource defaults (in the same units as pool.avail_* fields)
    s.base_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 4,
    }
    s.base_ram = {
        Priority.QUERY: 12,
        Priority.INTERACTIVE: 16,
        Priority.BATCH_PIPELINE: 24,
    }

    # Caps to prevent single ops from consuming an entire pool and killing concurrency
    s.max_cpu = {
        Priority.QUERY: 32,
        Priority.INTERACTIVE: 32,
        Priority.BATCH_PIPELINE: 16,
    }
    s.max_ram = {
        Priority.QUERY: 96,
        Priority.INTERACTIVE: 128,
        Priority.BATCH_PIPELINE: 192,
    }

    # Minimum floors when we are forced to "fit" into remaining fragments (only for non-hinted ops)
    s.min_cpu_floor_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.min_ram_floor_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Intra-pipeline parallelism cap per tick (helps DAG fan-out, avoids pipeline hogging)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Starvation prevention: prefer pipelines that haven't been scheduled recently
    s.starve_ticks = {
        Priority.QUERY: 50,
        Priority.INTERACTIVE: 35,
        Priority.BATCH_PIPELINE: 120,
    }

    # Queue scanning limits to keep scheduler fast under high concurrency
    s.scan_limit = {
        Priority.QUERY: 64,
        Priority.INTERACTIVE: 64,
        Priority.BATCH_PIPELINE: 64,
    }

    # Batch gating: don't start long/low-priority work if we're down to the last few "slots"
    s.batch_headroom_cpu = 2 * (s.base_cpu[Priority.QUERY] + s.base_cpu[Priority.INTERACTIVE])
    s.batch_headroom_ram = 2 * (s.base_ram[Priority.QUERY] + s.base_ram[Priority.INTERACTIVE])

    # Housekeeping
    s.compact_every = 25
    s.max_total_assignments_per_step = 8000
    s.non_resource_fail_limit = 3


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    s.ticks += 1

    def _op_key(op):
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op)

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

    def _meta(pid):
        m = s.pipeline_meta.get(pid)
        if m is None:
            m = {"enqueued_tick": s.ticks, "last_scheduled_tick": s.ticks}
            s.pipeline_meta[pid] = m
        if "last_scheduled_tick" not in m:
            m["last_scheduled_tick"] = m.get("enqueued_tick", s.ticks)
        return m

    def _service_gap_ticks(pid):
        m = s.pipeline_meta.get(pid)
        if not m:
            return 0
        return max(0, s.ticks - m.get("last_scheduled_tick", m.get("enqueued_tick", s.ticks)))

    def _drop_pipeline(pid, mark_done=False):
        s.enqueued.discard(pid)
        s.pipeline_meta.pop(pid, None)
        if mark_done:
            s.done_pipelines.add(pid)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _size_request(pool, pri, pid, op, avail_cpu, avail_ram):
        """
        Returns (cpu_req, ram_req, fits_bool).
        Policy:
          - Start from base sizing.
          - If we have hints (OOM->RAM, TIMEOUT->CPU), honor them and do not downsize below hint.
          - Otherwise allow slight downsize to fit remaining fragments (bounded by floors).
          - Enforce per-priority caps to preserve concurrency.
        """
        opk = _op_key(op)

        base_cpu = float(s.base_cpu[pri])
        base_ram = float(s.base_ram[pri])

        cpu_cap = float(min(pool.max_cpu_pool, s.max_cpu[pri]))
        ram_cap = float(min(pool.max_ram_pool, s.max_ram[pri]))

        cpu_hint = s.op_cpu_hint.get((pid, opk), None)
        ram_hint = s.op_ram_hint.get((pid, opk), None)

        desired_cpu = float(cpu_hint) if (cpu_hint is not None and cpu_hint > 0) else base_cpu
        desired_ram = float(ram_hint) if (ram_hint is not None and ram_hint > 0) else base_ram

        desired_cpu = _clamp(desired_cpu, 1.0, max(1.0, cpu_cap))
        desired_ram = _clamp(desired_ram, 1.0, max(1.0, ram_cap))

        # If hinted, require meeting the hint (don't squeeze below it).
        if cpu_hint is not None and desired_cpu > avail_cpu:
            return None, None, False
        if ram_hint is not None and desired_ram > avail_ram:
            return None, None, False

        # Otherwise, allow fit-down to availability with a floor (prevents zeroing).
        min_cpu_floor = max(1.0, base_cpu * float(s.min_cpu_floor_frac[pri]))
        min_ram_floor = max(1.0, base_ram * float(s.min_ram_floor_frac[pri]))

        cpu_req = desired_cpu
        ram_req = desired_ram

        if cpu_req > avail_cpu:
            cpu_req = float(avail_cpu)
        if ram_req > avail_ram:
            ram_req = float(avail_ram)

        if cpu_req < min_cpu_floor or ram_req < min_ram_floor:
            return None, None, False

        cpu_req = _clamp(cpu_req, 1.0, float(pool.max_cpu_pool))
        ram_req = _clamp(ram_req, 1.0, float(pool.max_ram_pool))
        return cpu_req, ram_req, True

    def _compact_queue(pri):
        q = s.wait_q[pri]
        if not q:
            return
        new_q = []
        for p in q:
            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.done_pipelines or pid in s.dead_pipelines:
                continue
            # Drop successfully completed pipelines aggressively.
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid, mark_done=True)
                continue
            new_q.append(p)
        s.wait_q[pri] = new_q
        if new_q:
            s.rr_cursor[pri] %= len(new_q)
        else:
            s.rr_cursor[pri] = 0

    # --- Ingest new pipelines (arrivals) ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.done_pipelines or pid in s.dead_pipelines:
            continue
        if pid in s.enqueued:
            # Ensure metadata exists even if already enqueued.
            _meta(pid)
            continue
        s.wait_q[p.priority].append(p)
        s.enqueued.add(pid)
        s.pipeline_meta[pid] = {"enqueued_tick": s.ticks, "last_scheduled_tick": s.ticks}

    # --- Process results (learn from failures: OOM -> RAM bump, TIMEOUT -> CPU bump) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        err = getattr(r, "error", None)
        failed = (hasattr(r, "failed") and r.failed())

        for op in ops:
            opk = _op_key(op)
            pid = s.op_to_pipeline.get(opk, None)

            # Cleanup mapping entry once we've seen a result for this op execution.
            if opk in s.op_to_pipeline:
                del s.op_to_pipeline[opk]

            if pid is None:
                continue
            if pid in s.dead_pipelines or pid in s.done_pipelines:
                continue

            if not failed:
                # Success: reset non-resource failure counters for this op.
                for k in (
                    (pid, opk, "non_resource"),
                    (pid, opk, "oom"),
                    (pid, opk, "timeout"),
                ):
                    if k in s.fail_counts:
                        del s.fail_counts[k]
                continue

            # Failure handling
            if _is_oom_error(err):
                key = (pid, opk, "oom")
                s.fail_counts[key] = s.fail_counts.get(key, 0) + 1

                prev = s.op_ram_hint.get((pid, opk), None)
                alloc = getattr(r, "ram", None)
                base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev) if (prev is not None and prev > 0) else float(s.base_ram.get(getattr(r, "priority", None), 16)))
                # Aggressive but capped later by pool max in sizing.
                new_hint = max(base * 1.75, (float(prev) * 1.5 if prev is not None else 0.0), float(s.base_ram[Priority.INTERACTIVE]))
                s.op_ram_hint[(pid, opk)] = new_hint
                continue

            if _is_timeout_error(err):
                key = (pid, opk, "timeout")
                s.fail_counts[key] = s.fail_counts.get(key, 0) + 1

                prev = s.op_cpu_hint.get((pid, opk), None)
                alloc = getattr(r, "cpu", None)
                base = float(alloc) if (alloc is not None and alloc > 0) else (float(prev) if (prev is not None and prev > 0) else float(s.base_cpu.get(getattr(r, "priority", None), 8)))
                new_hint = max(base * 1.5, (float(prev) * 1.25 if prev is not None else 0.0), float(s.base_cpu[Priority.INTERACTIVE]))
                s.op_cpu_hint[(pid, opk)] = new_hint
                continue

            # Other failures: avoid endless retries that burn capacity.
            key = (pid, opk, "non_resource")
            s.fail_counts[key] = s.fail_counts.get(key, 0) + 1
            if s.fail_counts[key] >= s.non_resource_fail_limit:
                s.dead_pipelines.add(pid)
                _drop_pipeline(pid, mark_done=False)

    if s.ticks % s.compact_every == 0:
        _compact_queue(Priority.QUERY)
        _compact_queue(Priority.INTERACTIVE)
        _compact_queue(Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    # Per-tick cap on how many ops we schedule from the same pipeline (supports DAG fan-out).
    scheduled_ops_per_pipeline = {}

    def _can_schedule_more(pid, pri):
        cap = s.max_ops_per_pipeline_per_tick[pri]
        return scheduled_ops_per_pipeline.get(pid, 0) < cap

    def _bump_scheduled(pid):
        scheduled_ops_per_pipeline[pid] = scheduled_ops_per_pipeline.get(pid, 0) + 1
        m = _meta(pid)
        m["last_scheduled_tick"] = s.ticks

    def _pick_from_priority(pri, pool, avail_cpu, avail_ram):
        """
        Pick one runnable op from the priority queue using round-robin + aging.
        Returns (pipeline, op, cpu_req, ram_req) or (None, None, None, None).
        """
        q = s.wait_q[pri]
        if not q:
            return None, None, None, None

        n = len(q)
        if n == 0:
            return None, None, None, None

        cursor = s.rr_cursor[pri] % n
        scan_n = s.scan_limit.get(pri, 64)
        scan_n = int(scan_n) if scan_n is not None else 64
        if scan_n <= 0:
            scan_n = 64
        scan_n = min(n, scan_n)

        best = None  # (gap, idx, p, op, cpu, ram)
        starve_thresh = int(s.starve_ticks.get(pri, 0) or 0)

        for i in range(scan_n):
            idx = (cursor + i) % n
            p = q[idx]
            if p is None:
                continue
            pid = p.pipeline_id

            if pid in s.done_pipelines or pid in s.dead_pipelines:
                continue

            if not _can_schedule_more(pid, pri):
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(pid, mark_done=True)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not ops:
                continue

            op = ops[0]
            cpu_req, ram_req, fits = _size_request(pool, pri, pid, op, avail_cpu, avail_ram)
            if not fits:
                continue

            gap = _service_gap_ticks(pid)

            # Prefer starved pipelines; otherwise take first fit to keep scanning cheap.
            if gap >= starve_thresh:
                if best is None or gap > best[0]:
                    best = (gap, idx, p, op, cpu_req, ram_req)
            else:
                if best is None:
                    best = (gap, idx, p, op, cpu_req, ram_req)

            # If we found a starved candidate, keep scanning a bit to find the most starved.
            # If not starved, exit early for throughput.
            if best is not None and best[0] < starve_thresh:
                break

        if best is None:
            # Advance cursor slightly to avoid getting stuck on unschedulable heads.
            s.rr_cursor[pri] = (cursor + 1) % n
            return None, None, None, None

        _, idx, p, op, cpu_req, ram_req = best
        s.rr_cursor[pri] = (idx + 1) % n
        return p, op, cpu_req, ram_req

    def _approx_runnable(pri, sample=16):
        q = s.wait_q[pri]
        if not q:
            return 0
        n = len(q)
        if n == 0:
            return 0
        cursor = s.rr_cursor[pri] % n
        sample = min(n, max(1, int(sample)))
        cnt = 0
        for i in range(sample):
            p = q[(cursor + i) % n]
            if p is None:
                continue
            pid = p.pipeline_id
            if pid in s.done_pipelines or pid in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if ops:
                cnt += 1
        return cnt

    # Decide whether to prefer INTERACTIVE first vs QUERY first this step (fairness + backlog sensitivity).
    runnable_q = _approx_runnable(Priority.QUERY, sample=20)
    runnable_i = _approx_runnable(Priority.INTERACTIVE, sample=20)
    if runnable_i > runnable_q:
        first_pick = Priority.INTERACTIVE
        second_pick = Priority.QUERY
    else:
        # Alternate to avoid systematic bias when roughly equal.
        if s.ticks % 2 == 0:
            first_pick = Priority.INTERACTIVE
            second_pick = Priority.QUERY
        else:
            first_pick = Priority.QUERY
            second_pick = Priority.INTERACTIVE

    # --- Main scheduling: fill each pool with many small containers to reduce queueing timeouts ---
    total_assignments_budget = s.max_total_assignments_per_step

    for pool_id in range(s.executor.num_pools):
        if total_assignments_budget <= 0:
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Safety: if pool reports tiny negatives due to float math
        if avail_cpu < 0:
            avail_cpu = 0.0
        if avail_ram < 0:
            avail_ram = 0.0

        # Attempt to keep scheduling while we have meaningful resources.
        # (floors are enforced by _size_request)
        guard = 0
        while avail_cpu >= 1.0 and avail_ram >= 1.0 and total_assignments_budget > 0:
            guard += 1
            if guard > 20000:
                break

            picked = False

            # Try high-priority first/second (alternating), then batch opportunistically.
            for pri in (first_pick, second_pick):
                p, op, cpu_req, ram_req = _pick_from_priority(pri, pool, avail_cpu, avail_ram)
                if p is None:
                    continue

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
                _bump_scheduled(p.pipeline_id)

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                total_assignments_budget -= 1
                picked = True
                break

            if picked:
                continue

            # If neither QUERY nor INTERACTIVE fit, try BATCH if we have headroom,
            # otherwise stop to avoid filling the last fragments with batch and blocking.
            if avail_cpu < float(s.batch_headroom_cpu) or avail_ram < float(s.batch_headroom_ram):
                break

            p, op, cpu_req, ram_req = _pick_from_priority(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
            if p is None:
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
            _bump_scheduled(p.pipeline_id)

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            total_assignments_budget -= 1

    return suspensions, assignments
