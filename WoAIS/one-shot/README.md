# One-Shot Scheduler Evaluation

## Overview

This one-shot experiment evaluates whether an LLM can produce effective schedulers from a single generation pass. For each reasoning effort level, we generate 50 candidate policies and evaluate them on the same trace set. The core evaluation metric is **latency** (median over traces per policy), and cross-effort comparisons focus on both **functional success rate** and **beat-baseline rate**.

## Files

- `experiment.py` — generates schedulers via one-shot LLM calls
- `analyze.py` — evaluates all schedulers in a `schedulers-{effort}/` directory; uses the modified `simulation_utils.py` (with `signal.alarm(60s)` per trace) and adds a hard `subprocess.run(timeout=700s)` as a second layer.
- `plot.py` — generates PDF charts from `analyze.py` output
- `config.py` — shared configuration

---

## Timeout

95% of evaluated schedulers complete within 60s/trace, and schedulers under this threshold actually achieve better latency on average (median 143.5) than those exceeding it (median 255.8). For reference, the naive baseline takes 1.6s/trace and the built-in priority scheduler ~10s/trace. Note: slow schedulers (>60s/trace) can still produce good results, but may take 1–2 hours to fully evaluate, which is impractical for the iterative loop in `main.py` in my opinion.
