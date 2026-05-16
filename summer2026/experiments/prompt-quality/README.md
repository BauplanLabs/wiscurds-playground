# prompt-quality

Current prompt/feedback quality audit for `tool/iterate.py`.

- Starting point: `experiments/oneshot/schedulers/high/scheduler_high_006.py`.
- Trace: `scenarios/traces/v1/bench_canonical_train.csv`.
- Variable: prompt contents and feedback diagnostics, not the seed scheduler.
- Goal: make `prompt_system.txt` + `prompt_user.txt` rich enough that a strong
  systems reader can identify concrete scheduler fixes.
- Runs: `runs/1` and `runs/2` are tracked reference runs from the initial audit.

