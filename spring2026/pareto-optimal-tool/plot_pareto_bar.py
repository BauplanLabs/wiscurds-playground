#!/usr/bin/env python3
"""
plot_pareto_bar.py — Bar chart: % Pareto-optimal schedulers per reasoning level.

Produces results/pareto/pareto_pct_bar.pdf (and .png).

Usage:
    python plot_pareto_bar.py
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

RESULTS_DIR = Path(__file__).resolve().parent / "results" / "pareto"
GROUPS      = ["none", "low", "medium", "high"]
LABELS      = ["None", "Low", "Medium", "High"]

# ── collect numbers ───────────────────────────────────────────────────────────

functional_counts = []
pareto_counts     = []
pct_pareto        = []

for group in GROUPS:
    jsonl_path  = RESULTS_DIR / group / "results.jsonl"
    pareto_path = RESULTS_DIR / group / "pareto.csv"

    records = [json.loads(l) for l in jsonl_path.open() if l.strip()]
    ok      = [r for r in records if r.get("ok")]

    pareto_names = set()
    with pareto_path.open() as f:
        for row in csv.DictReader(f):
            pareto_names.add(row["filename"])

    n_func   = len(ok)
    n_pareto = len([r for r in ok if r["filename"] in pareto_names])
    pct      = 100.0 * n_pareto / n_func if n_func else 0.0

    functional_counts.append(n_func)
    pareto_counts.append(n_pareto)
    pct_pareto.append(pct)

# ── plot ──────────────────────────────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(5, 3.8))

colors = ["#4878cf", "#6acc65", "#d65f5f", "#b47cc7"]
bars = ax.bar(LABELS, pct_pareto, color=colors, width=0.55, zorder=3)

# annotate each bar with "N/M (X%)"
for bar, pct, n_p, n_f in zip(bars, pct_pareto, pareto_counts, functional_counts):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 0.7,
        f"{n_p}/{n_f}",
        ha="center", va="bottom", fontsize=9, color="#333333",
    )

ax.set_xlabel("Reasoning effort level", fontsize=11)
ax.set_ylabel("Pareto-optimal schedulers (%)", fontsize=11)
ax.set_title("Fraction of functional schedulers\nthat are Pareto-optimal", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
ax.set_ylim(0, max(pct_pareto) * 1.25)
ax.yaxis.grid(True, linestyle="--", alpha=0.6, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()

out_pdf = RESULTS_DIR / "pareto_pct_bar.pdf"
out_png = RESULTS_DIR / "pareto_pct_bar.png"
fig.savefig(out_pdf, bbox_inches="tight")
fig.savefig(out_png, bbox_inches="tight", dpi=150)
print(f"Saved -> {out_pdf}")
print(f"Saved -> {out_png}")
plt.show()
