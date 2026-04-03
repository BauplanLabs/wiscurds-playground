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


def one_shot_latency_cdf(args):
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

    fig, ax = plt.subplots(figsize=(3.5, 2.5))
    plot_cdf(ax, no_est["geo_mean"].dropna(), "No Estimates", color="black")
    plot_cdf(ax, est["geo_mean"].dropna(), "With Estimates", color="red")
    ax.set_xlabel("Latency (Geometric Mean, Seconds)")
    ax.set_ylabel("% of Schedulers")
    ax.set_xlim(left=0)
    ax.set_ylim(0, 100)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False)
    fig.tight_layout()
    plt.savefig("paper-plots/one-shot-latency-cdf.pdf")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate paper plots")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("one-shot-latency-cdf", help="CDF of one-shot latency")
    sub.add_parser("all", help="Run all subcommands")

    args = parser.parse_args()
    commands = {
        "one-shot-latency-cdf": one_shot_latency_cdf,
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
