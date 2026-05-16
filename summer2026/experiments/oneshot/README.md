# oneshot

Retained 01 experiment: generate schedulers in one shot and compare reasoning
effort levels.

- Starting point: no seed scheduler. Each `scheduler_*.py` is generated directly
  from the one-shot scheduler prompt.
- Variable: model reasoning effort (`none`, `low`, `medium`, `high`).
- Evaluation: retained `analysis.jsonl`, `probes.csv`, and `plots/fig1.pdf`.
- Current use: baseline scheduler pool for later iteration experiments.

