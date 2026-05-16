# oneshot-est

Retained 02 experiment: one-shot schedulers under different runtime-estimation
noise settings.

- Starting point: no iterative seed. Schedulers live in
  `schedulers/low/scheduler_est_*.py`.
- Variable: estimator condition (`no_estimation`, `sigma_0.0`, `sigma_0.75`,
  `sigma_1.5`).
- Evaluation: retained `analysis.jsonl`, `probes.csv`, and `plots/fig2.pdf`.
- Current use: archived estimator-noise comparison, not the active path.

