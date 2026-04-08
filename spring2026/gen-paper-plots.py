import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

OUTPUT_DIR = Path("paper-plots")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _save(fig, name: str, tight: bool = True) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / name
    if tight:
        fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")


def _style(ax) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _ygrid(ax) -> None:
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)

def plot_cdf(ax, values, label, color="black", linewidth=1.0):
    sorted_vals = np.sort(values)
    cdf = np.arange(1, len(sorted_vals) + 1) / len(sorted_vals) * 100
    ax.step(sorted_vals, cdf, where="post", label=label, color=color, linewidth=linewidth)


def geometric_mean(values):
    product = 1
    for v in values:
        product *= v
    return product ** (1 / len(values))


def load_analysis(path):
    records = []
    with open(path) as f:
        for line in f:
            records.append(json.loads(line))
    return pd.DataFrame(records)


def wilson_interval(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 1.0
    p = k / n
    denom = 1 + z ** 2 / n
    center = (p + z ** 2 / (2 * n)) / denom
    margin = z * (p * (1 - p) / n + z ** 2 / (4 * n ** 2)) ** 0.5 / denom
    return max(0.0, center - margin), min(1.0, center + margin)


# ---------------------------------------------------------------------------
# Probe pass-rate plots
# ---------------------------------------------------------------------------

PROBE_LABELS = {
    "syntax":            "Syntax",
    "valid_scheduler":   "Valid",
    "basic_run":         "Basic",
    "retry_run":         "Retry",
    "suspend_run":       "Suspend",
    "grouping":          "Grouping",
    "overcommit":        "Overcommit",
    "priority_ordering": "Priority-Order",
    "starvation":        "Starvation",
    "no_deadlock":       "No-Deadlock",
}

EFFORT_ORDER  = ["none", "low", "medium", "high"]
EFFORT_COLORS = {"none": "#c6dbef", "low": "#6baed6", "medium": "#2171b5", "high": "#08306b"}


def _load_probe_csv(effort: str) -> pd.DataFrame | None:
    path = f"one-shot/analyses/schedulers-{effort}.csv"
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return None


def _load_probe_data() -> dict[str, dict[str, float]]:
    probes = list(PROBE_LABELS.keys())
    data: dict[str, dict[str, float]] = {}
    for effort in EFFORT_ORDER:
        df = _load_probe_csv(effort)
        if df is None:
            continue
        rates = {}
        for probe in probes:
            if probe not in df.columns:
                rates[probe] = float("nan")
                continue
            col = df[probe]
            valid = col[col != "skipped"]
            rates[probe] = (valid == "pass").sum() / len(valid) * 100 if len(valid) > 0 else float("nan")
        data[effort] = rates
    return data


def _draw_probe_pass_rates(ax, data: dict, bar_width_total: float = 0.7):
    probes = list(PROBE_LABELS.keys())
    efforts = [e for e in EFFORT_ORDER if e in data]
    n_efforts = len(efforts)
    bar_width = bar_width_total / n_efforts
    cluster_centers = np.arange(len(probes))

    for i, effort in enumerate(efforts):
        offsets = cluster_centers + (i - (n_efforts - 1) / 2) * bar_width
        values = [data[effort].get(p, float("nan")) for p in probes]
        ax.bar(offsets, values, width=bar_width, label=effort.capitalize(),
               color=EFFORT_COLORS[effort], zorder=2, linewidth=0)

    ax.set_xticks(cluster_centers)
    ax.set_xticklabels([PROBE_LABELS[p] for p in probes], fontsize=4, rotation=90, ha="center")
    ax.set_ylabel("% Passing", fontsize=5)
    ax.tick_params(axis="y", labelsize=5)
    ax.set_ylim(0, 100)
    _ygrid(ax)
    _style(ax)
    ax.legend(frameon=False, fontsize=4, loc="lower left")


# ---------------------------------------------------------------------------
# Latency CDF plots
# ---------------------------------------------------------------------------

def _load_latency_data():
    no_est = load_analysis("one-shot/output/schedulers-low/analysis.jsonl")
    est    = load_analysis("one-shot-est/output-low-v2/sigma_0.0/analysis.jsonl")

    no_est_before = len(no_est)
    no_est = no_est.dropna(subset=["metric_values"])
    print(f"No Estimates: dropped {no_est_before - len(no_est)}/{no_est_before} rows with missing metric_values")

    est_before = len(est)
    est = est.dropna(subset=["metric_values"])
    print(f"With Estimates: dropped {est_before - len(est)}/{est_before} rows with missing metric_values")

    no_est["geo_mean"] = no_est["metric_values"].map(geometric_mean)
    est["geo_mean"]    = est["metric_values"].map(geometric_mean)
    return no_est, est


def _draw_latency_cdf(ax, no_est, est):
    plot_cdf(ax, no_est["geo_mean"].dropna(), "No Estimates",   color="#e83030", linewidth=1.0)
    plot_cdf(ax, est["geo_mean"].dropna(),    "With Estimates", color="#7b2d8b", linewidth=1.0)
    ax.set_xlabel("Latency (Geometric Mean, Seconds)")
    ax.set_ylabel("% of Schedulers")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 100)
    _style(ax)
    ax.legend(frameon=False)


def _latency_by_effort() -> dict[str, np.ndarray]:
    """Return {effort: array of geo-mean latencies} from one-shot output."""
    records = _load_effort_records()
    grouped: dict[str, list] = {}
    for r in records:
        effort = str(r.get("reasoning_effort", "unknown"))
        mv = r.get("metric_values")
        if mv is not None:
            grouped.setdefault(effort, []).append(geometric_mean(mv))
    return {e: np.array(v) for e, v in grouped.items()}


def _draw_latency_cdf_by_effort(ax, data: dict[str, np.ndarray]):
    efforts = [e for e in EFFORT_ORDER if e in data]
    for effort in efforts:
        plot_cdf(ax, data[effort], effort.capitalize(), color=EFFORT_COLORS.get(effort, "gray"))
    ax.set_xlabel("Latency (s)", fontsize=5)
    ax.set_ylabel("% of Schedulers", fontsize=5)
    ax.tick_params(axis="both", labelsize=5)
    ax.set_xlim(0, 1000)
    ax.set_ylim(0, 100)
    _style(ax)
    ax.legend(frameon=False, fontsize=4)


# ---------------------------------------------------------------------------
# Analysis JSONL loading (for success-rate / failure-mode plots)
# ---------------------------------------------------------------------------

def _load_effort_records() -> list[dict]:
    """Discover and load all analysis.jsonl files under one-shot/output/."""
    records = []
    output_root = Path("one-shot/output")
    if not output_root.exists():
        return records
    for jf in sorted(output_root.rglob("analysis.jsonl")):
        for line in jf.read_text().splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _build_summary_rows(records: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for r in records:
        effort = str(r.get("reasoning_effort", "unknown"))
        grouped.setdefault(effort, []).append(r)

    known   = [e for e in EFFORT_ORDER if e in grouped]
    unknown = sorted(set(grouped) - set(EFFORT_ORDER))
    ordered = known + unknown

    rows = []
    for effort in ordered:
        rr = grouped[effort]
        n  = len(rr)
        func_n = sum(1 for r in rr if bool(r.get("functional")))
        beat_n = sum(1 for r in rr if bool(r.get("beats_baseline")))
        func_lo, func_hi = wilson_interval(func_n, n)
        beat_lo, beat_hi = wilson_interval(beat_n, n)

        wait_samples = [float(r["generation_seconds"]) for r in rr if r.get("generation_seconds") is not None]
        if not wait_samples:
            wait_samples = [float(r["llm_first_attempt_wait_seconds"]) for r in rr
                            if r.get("llm_first_attempt_wait_seconds") is not None]

        rows.append({
            "reasoning_effort":         effort,
            "n":                        n,
            "functional_rate":          func_n / n if n else float("nan"),
            "functional_lo":            func_lo,
            "functional_hi":            func_hi,
            "beat_baseline_rate":       beat_n / n if n else float("nan"),
            "beat_baseline_lo":         beat_lo,
            "beat_baseline_hi":         beat_hi,
            "mean_wait_seconds":        sum(wait_samples) / len(wait_samples) if wait_samples else float("nan"),
            "failure_counts":           dict(Counter(str(r.get("failure_mode", "unknown")) for r in rr)),
        })
    return rows


def _build_change_vs_medium(rows: list[dict]) -> list[dict]:
    medium = next((r for r in rows if r["reasoning_effort"] == "medium"), None)
    if medium is None:
        return []
    m_func = medium["functional_rate"]
    m_beat = medium["beat_baseline_rate"]
    out = []
    for r in rows:
        if r["reasoning_effort"] == "medium":
            continue
        out.append({
            **r,
            "func_pct_change": (r["functional_rate"]   - m_func) / m_func * 100 if m_func else float("nan"),
            "beat_pct_change": (r["beat_baseline_rate"] - m_beat) / m_beat * 100 if m_beat else float("nan"),
        })
    return out


# ---------------------------------------------------------------------------
# Success-rate / failure-mode draw functions
# ---------------------------------------------------------------------------

# Grayscale: solid = functional, hatched = beats baseline.
# Color for each effort level comes from EFFORT_COLORS.

def _draw_success_rates(ax, rows: list[dict]):
    efforts = [r["reasoning_effort"] for r in rows]
    x = np.arange(len(rows))
    w = 0.35

    for i, r in enumerate(rows):
        c = EFFORT_COLORS.get(r["reasoning_effort"], "#888888")
        func_pct = r["functional_rate"] * 100
        beat_pct = r["beat_baseline_rate"] * 100
        func_err = [[( r["functional_rate"]   - r["functional_lo"])  * 100],
                    [(r["functional_hi"]  - r["functional_rate"])   * 100]]
        beat_err = [[(r["beat_baseline_rate"] - r["beat_baseline_lo"]) * 100],
                    [(r["beat_baseline_hi"] - r["beat_baseline_rate"]) * 100]]

        ax.bar(x[i] - w / 2, func_pct, w, color=c, zorder=2)
        ax.bar(x[i] + w / 2, beat_pct, w, color=c, hatch="//", zorder=2)
        ax.errorbar(x[i] - w / 2, func_pct, yerr=func_err,
                    fmt="none", ecolor="black", capsize=3, linewidth=1.0, zorder=3)
        ax.errorbar(x[i] + w / 2, beat_pct, yerr=beat_err,
                    fmt="none", ecolor="black", capsize=3, linewidth=1.0, zorder=3)

    ax.set_xticks(x, efforts)
    ax.set_ylabel("Success Rate (%)")
    ax.set_ylim(0, 100)
    _ygrid(ax)
    _style(ax)
    ax.legend(
        handles=[Patch(facecolor="#555555", label="Functional"),
                 Patch(facecolor="#555555", hatch="//", label="Beats baseline")],
        frameon=False, fontsize=8,
    )


def _draw_wait_time(ax, rows: list[dict]):
    efforts = [r["reasoning_effort"] for r in rows]
    valid   = [(e, r["mean_wait_seconds"]) for e, r in zip(efforts, rows)
               if not np.isnan(r["mean_wait_seconds"])]
    if not valid:
        ax.text(0.5, 0.5, "No wait-time data", ha="center", va="center", transform=ax.transAxes)
        return
    labels, values = zip(*valid)
    x = np.arange(len(labels))
    colors = [EFFORT_COLORS.get(e, "#888888") for e in labels]

    bars = ax.bar(x, values, color=colors, zorder=2)
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{v:.1f}s", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Mean LLM Wait Time (s)")
    _ygrid(ax)
    _style(ax)


def _draw_change_vs_medium(ax, change_rows: list[dict]):
    if not change_rows:
        ax.text(0.5, 0.5, "Need medium effort data", ha="center", va="center", transform=ax.transAxes)
        return
    efforts = [r["reasoning_effort"] for r in change_rows]
    x = np.arange(len(change_rows))
    w = 0.35

    for i, r in enumerate(change_rows):
        c = EFFORT_COLORS.get(r["reasoning_effort"], "#888888")
        ax.bar(x[i] - w / 2, r["func_pct_change"], w, color=c, zorder=2)
        ax.bar(x[i] + w / 2, r["beat_pct_change"], w, color=c, hatch="//", zorder=2)

    ax.axhline(0, color="black", linewidth=0.8, alpha=0.6)
    ax.set_xticks(x, efforts)
    ax.set_ylabel("Change vs. Medium (%)")
    _ygrid(ax)
    _style(ax)
    ax.legend(
        handles=[Patch(facecolor="#555555", label="Functional"),
                 Patch(facecolor="#555555", hatch="//", label="Beats baseline")],
        frameon=False, fontsize=8,
    )


# Grayscale palette for failure modes, ordered by severity.
_FAILURE_COLORS = {
    "success":            "#222222",
    "simulation_timeout": "#777777",
    "simulation_error":   "#aaaaaa",
    "exec_error":         "#cccccc",
}


def _draw_failure_modes(ax, rows: list[dict]):
    all_modes = sorted({k for r in rows for k in r["failure_counts"]})
    if not all_modes:
        return
    # Put success first
    if "success" in all_modes:
        all_modes = ["success"] + [m for m in all_modes if m != "success"]

    efforts  = [r["reasoning_effort"] for r in rows]
    x        = np.arange(len(rows))
    bottoms  = np.zeros(len(rows))
    fallback = ["#" + ('%02x' % max(0, 180 - i * 30)) * 3 for i in range(len(all_modes))]

    for j, mode in enumerate(all_modes):
        vals   = np.array([r["failure_counts"].get(mode, 0) for r in rows], dtype=float)
        color  = _FAILURE_COLORS.get(mode, fallback[j % len(fallback)])
        ax.bar(x, vals, bottom=bottoms, label=mode, color=color, zorder=2)
        bottoms += vals

    ax.set_xticks(x, efforts)
    ax.set_ylabel("Count")
    _ygrid(ax)
    _style(ax)
    ax.legend(frameon=False, fontsize=8)


# ---------------------------------------------------------------------------
# Subcommand functions
# ---------------------------------------------------------------------------

def probe_pass_rates(args):
    data = _load_probe_data()
    if not data:
        print("No probe CSV files found in one-shot/analyses/. Run run_probes.py on a directory first.")
        return
    fig, ax = plt.subplots(figsize=(7, 2.5))
    _draw_probe_pass_rates(ax, data, bar_width_total=0.7)
    _save(fig, "probe-pass-rates.pdf")


def one_shot_latency_cdf(args):
    no_est, est = _load_latency_data()
    fig, ax = plt.subplots(figsize=(3.5, 2.5))
    _draw_latency_cdf(ax, no_est, est)
    _save(fig, "one-shot-latency-cdf.pdf")


def probe_and_cdf(args):
    data = _load_probe_data()
    if not data:
        print("No probe CSV files found in one-shot/analyses/. Run run_probes.py on a directory first.")
        return
    no_est, est = _load_latency_data()

    fig, (ax_probes, ax_cdf) = plt.subplots(
        1, 2, figsize=(3.5, 2.2),
        gridspec_kw={"width_ratios": [1, 1]},
    )

    _draw_probe_pass_rates(ax_probes, data, bar_width_total=0.6)
    ax_probes.yaxis.grid(False)
    ax_probes.set_title("(a) Scheduler Properties", loc="center", fontsize=7, pad=3)
    ax_probes.set_ylabel("% Passing", fontsize=7)
    ax_probes.tick_params(axis="y", labelsize=6)
    plt.setp(ax_probes.get_xticklabels(), rotation=90, ha="center", fontsize=6)
    ax_probes.legend(frameon=False, fontsize=5, ncol=3,
                     loc="lower center", bbox_to_anchor=(0.5, 1.18),
                     handlelength=1.0, handleheight=0.8)

    _draw_latency_cdf(ax_cdf, no_est, est)
    ax_cdf.autoscale(axis="x", tight=True)
    ax_cdf.set_xlim(left=0)
    ax_cdf.set_title("(b) Latency Distribution", loc="center", fontsize=7, pad=3)
    ax_cdf.set_xlabel("Latency (s)", fontsize=7)
    ax_cdf.set_ylabel("% of Schedulers", fontsize=7)
    ax_cdf.tick_params(labelsize=6)
    ax_cdf.legend(frameon=False, fontsize=5, ncol=2,
                  loc="lower center", bbox_to_anchor=(0.5, 1.18))

    fig.tight_layout(pad=0.3, w_pad=0.5)
    _save(fig, "probe-and-cdf.pdf", tight=False)


def plot1(args):
    probe_data = _load_probe_data()
    if not probe_data:
        print("No probe CSV files found in one-shot/analyses/.")
        return
    latency_data = _latency_by_effort()
    if not latency_data:
        print("No latency data found in one-shot/output/.")
        return
    fig, (ax_probes, ax_cdf) = plt.subplots(
        1, 2, figsize=(3.3, 1.8),
        gridspec_kw={"width_ratios": [1, 1]},
    )
    _draw_probe_pass_rates(ax_probes, probe_data, bar_width_total=0.65)
    ax_probes.get_legend().remove()
    ax_probes.set_title("(a) Scheduler Properties", fontsize=6)
    _draw_latency_cdf_by_effort(ax_cdf, latency_data)
    ax_cdf.get_legend().remove()
    ax_cdf.set_title("(b) Latency Distribution", fontsize=6)
    # shared legend at top
    handles, labels = ax_cdf.get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, fontsize=4, ncol=4,
               loc="upper center", bbox_to_anchor=(0.5, 1.05))
    _save(fig, "plot1.pdf")


def success_rates(args):
    records = _load_effort_records()
    if not records:
        print("No analysis records found in one-shot/output/.")
        return
    rows = _build_summary_rows(records)
    fig, ax = plt.subplots(figsize=(5, 2.5))
    _draw_success_rates(ax, rows)
    _save(fig, "success-rates.pdf")


def wait_time(args):
    records = _load_effort_records()
    if not records:
        print("No analysis records found in one-shot/output/.")
        return
    rows = _build_summary_rows(records)
    fig, ax = plt.subplots(figsize=(3.5, 2.5))
    _draw_wait_time(ax, rows)
    _save(fig, "wait-time.pdf")


def change_vs_medium(args):
    records = _load_effort_records()
    if not records:
        print("No analysis records found in one-shot/output/.")
        return
    rows        = _build_summary_rows(records)
    change_rows = _build_change_vs_medium(rows)
    fig, ax = plt.subplots(figsize=(4, 2.5))
    _draw_change_vs_medium(ax, change_rows)
    _save(fig, "change-vs-medium.pdf")


def failure_modes(args):
    records = _load_effort_records()
    if not records:
        print("No analysis records found in one-shot/output/.")
        return
    rows = _build_summary_rows(records)
    fig, ax = plt.subplots(figsize=(5, 2.5))
    _draw_failure_modes(ax, rows)
    _save(fig, "failure-modes.pdf")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate paper plots")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("probe-pass-rates",    help="Clustered bar chart: pass rate per probe per effort level")
    sub.add_parser("one-shot-latency-cdf", help="CDF of one-shot latency")
    sub.add_parser("probe-and-cdf",       help="Combined: probe pass rates (left) + latency CDF (right)")
    sub.add_parser("plot1",               help="Probe pass rates (left) + latency CDF by effort level (right)")
    sub.add_parser("success-rates",       help="Success rate + beats-baseline per effort level with 95pct CI")
    sub.add_parser("wait-time",           help="Mean LLM generation time per effort level")
    sub.add_parser("change-vs-medium",    help="Percent change in success rates relative to medium effort")
    sub.add_parser("failure-modes",       help="Stacked failure mode counts per effort level")
    sub.add_parser("all",                 help="Run all subcommands")

    args = parser.parse_args()
    commands = {
        "probe-pass-rates":     probe_pass_rates,
        "one-shot-latency-cdf": one_shot_latency_cdf,
        "probe-and-cdf":        probe_and_cdf,
        "plot1":                plot1,
        "success-rates":        success_rates,
        "wait-time":            wait_time,
        "change-vs-medium":     change_vs_medium,
        "failure-modes":        failure_modes,
    }
    if args.command == "all":
        for name, func in commands.items():
            print(f"--- {name} ---")
            func(args)
    elif args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
