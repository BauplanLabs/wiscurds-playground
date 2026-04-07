import argparse
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def plot_cdf(ax, values, label, color="black"):
    sorted_vals = np.sort(values)
    cdf = np.arange(1, len(sorted_vals) + 1) / len(sorted_vals) * 100
    ax.step(sorted_vals, cdf, where="post", label=label, color=color)


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


PROBE_LABELS = {
    "syntax":            "Syntax",
    "valid_scheduler":   "Valid",
    "basic_run":         "Basic",
    "retry_run":         "Retry",
    "suspend_run":       "Suspend",
    "grouping":          "Grouping",
    "overcommit":        "Overcommit",
    "priority_ordering": "Priority\nOrder",
    "starvation":        "Starvation",
    "no_deadlock":       "No\nDeadlock",
}

EFFORT_ORDER = ["low", "medium", "high"]
EFFORT_COLORS = {"low": "#aaaaaa", "medium": "#555555", "high": "#000000"}


def _load_probe_csv(effort: str) -> pd.DataFrame | None:
    path = f"one-shot/analyses/schedulers-{effort}.csv"
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return None


def _load_probe_data():
    """Load and compute pass rates from all available effort CSVs."""
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
    n_probes = len(probes)
    bar_width = bar_width_total / n_efforts
    cluster_centers = np.arange(n_probes)

    for i, effort in enumerate(efforts):
        offsets = cluster_centers + (i - (n_efforts - 1) / 2) * bar_width
        values = [data[effort].get(p, float("nan")) for p in probes]
        ax.bar(offsets, values, width=bar_width, label=effort.capitalize(),
               color=EFFORT_COLORS[effort], zorder=2)

    ax.set_xticks(cluster_centers)
    ax.set_xticklabels([PROBE_LABELS[p] for p in probes], fontsize=7)
    ax.set_ylabel("% Schedulers Passing")
    ax.set_ylim(0, 100)
    ax.yaxis.grid(True, linewidth=0.4, color="#dddddd", zorder=1)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, fontsize=8)


def _load_latency_data():
    no_est = load_analysis("one-shot/output/schedulers-low/analysis.jsonl")
    est = load_analysis("one-shot-est/output-low-v2/sigma_0.0/analysis.jsonl")

    no_est_before = len(no_est)
    no_est = no_est.dropna(subset=["metric_values"])
    print(f"No Estimates: dropped {no_est_before - len(no_est)}/{no_est_before} rows with missing metric_values")

    est_before = len(est)
    est = est.dropna(subset=["metric_values"])
    print(f"With Estimates: dropped {est_before - len(est)}/{est_before} rows with missing metric_values")

    no_est["geo_mean"] = no_est["metric_values"].map(geometric_mean)
    est["geo_mean"] = est["metric_values"].map(geometric_mean)
    return no_est, est


def _draw_latency_cdf(ax, no_est, est):
    plot_cdf(ax, no_est["geo_mean"].dropna(), "No Estimates", color="black")
    plot_cdf(ax, est["geo_mean"].dropna(), "With Estimates", color="red")
    ax.set_xlabel("Latency (Geometric Mean, Seconds)")
    ax.set_ylabel("% of Schedulers")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 100)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False)


def probe_pass_rates(args):
    data = _load_probe_data()
    if not data:
        print("No probe CSV files found in one-shot/analyses/. Run run_probes.py on a directory first.")
        return
    fig, ax = plt.subplots(figsize=(7, 2.5))
    _draw_probe_pass_rates(ax, data, bar_width_total=0.7)
    fig.tight_layout()
    import os
    os.makedirs("paper-plots", exist_ok=True)
    plt.savefig("paper-plots/probe-pass-rates.pdf")
    plt.close(fig)
    print("Saved paper-plots/probe-pass-rates.pdf")


def one_shot_latency_cdf(args):
    no_est, est = _load_latency_data()
    fig, ax = plt.subplots(figsize=(3.5, 2.5))
    _draw_latency_cdf(ax, no_est, est)
    fig.tight_layout()
    import os
    os.makedirs("paper-plots", exist_ok=True)
    plt.savefig("paper-plots/one-shot-latency-cdf.pdf")
    plt.close(fig)


def probe_and_cdf(args):
    data = _load_probe_data()
    if not data:
        print("No probe CSV files found in one-shot/analyses/. Run run_probes.py on a directory first.")
        return
    no_est, est = _load_latency_data()

    fig, (ax_probes, ax_cdf) = plt.subplots(
        1, 2, figsize=(10.5, 2.5),
        gridspec_kw={"width_ratios": [2, 1]},
    )
    _draw_probe_pass_rates(ax_probes, data, bar_width_total=0.95)
    _draw_latency_cdf(ax_cdf, no_est, est)
    fig.tight_layout()
    import os
    os.makedirs("paper-plots", exist_ok=True)
    plt.savefig("paper-plots/probe-and-cdf.pdf")
    plt.close(fig)
    print("Saved paper-plots/probe-and-cdf.pdf")


def main():
    parser = argparse.ArgumentParser(description="Generate paper plots")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("one-shot-latency-cdf", help="CDF of one-shot latency")
    sub.add_parser("probe-pass-rates", help="Clustered bar chart: pass rate per probe per effort level")
    sub.add_parser("probe-and-cdf", help="Combined figure: probe pass rates (left) + latency CDF (right)")
    sub.add_parser("all", help="Run all subcommands")

    args = parser.parse_args()
    commands = {
        "one-shot-latency-cdf": one_shot_latency_cdf,
        "probe-pass-rates": probe_pass_rates,
        "probe-and-cdf": probe_and_cdf,
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
