"""
Plot Exp 1 results: Comparative experiments.

Generates:
  - Line charts: Latency / Visited Cells / Scan Range Intervals vs query range
  - Bar charts:  Latency / Visited Cells / Scan Range Intervals vs distribution
"""

from __future__ import annotations
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from visualization.plot_utils import (
    apply_style, method_style, save_fig, add_legend,
    METRIC_LABELS, DIST_LABELS
)

METHODS_ORDER = ["LETI", "TShape", "XZ*", "LMSFC", "BMTree"]
METRICS = ["avg_latency_ms", "avg_visited_cells", "avg_scan_range_intervals"]


def plot_query_range(df: pd.DataFrame, output_dir: str):
    """Line plots: x = query_range_m, y = each metric, one line per method."""
    apply_style()

    # Filter rows where param_name starts with 'query_range_m'
    sub = df[df["param_name"].str.startswith("query_range_m")].copy()
    # Extract range value (stored as string in param_value)
    sub["range_m"] = sub["param_value"].astype(int)

    for metric in METRICS:
        fig, ax = plt.subplots(figsize=(6, 4))
        for method in METHODS_ORDER:
            m_df = sub[sub["method"] == method].sort_values("range_m")
            if m_df.empty:
                continue
            s = method_style(method)
            ax.plot(m_df["range_m"], m_df[metric],
                    color=s["color"], marker=s["marker"],
                    linestyle=s["linestyle"], label=s["label"],
                    linewidth=1.8, markersize=6)
        ax.set_xlabel("Query Range (m)")
        ax.set_ylabel(METRIC_LABELS.get(metric, metric))
        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} vs Query Range")
        add_legend(ax)
        save_fig(fig, os.path.join(output_dir, "figures"),
                 f"exp1a_{metric}_vs_query_range")


def plot_distribution(df: pd.DataFrame, output_dir: str):
    """Grouped bar charts: x = distribution, y = each metric."""
    apply_style()

    sub = df[df["param_name"] == "distribution"].copy()

    for metric in METRICS:
        fig, ax = plt.subplots(figsize=(6, 4))
        dists   = ["UNI", "SKE", "GUS"]
        n_methods = len(METHODS_ORDER)
        bar_width = 0.15
        x = range(len(dists))

        for i, method in enumerate(METHODS_ORDER):
            m_df = sub[sub["method"] == method].copy()
            vals = [
                m_df[m_df["param_value"] == d][metric].mean()
                for d in dists
            ]
            s = method_style(method)
            offset = (i - n_methods / 2 + 0.5) * bar_width
            ax.bar(
                [xi + offset for xi in x], vals,
                width=bar_width, color=s["color"],
                label=s["label"], alpha=0.85
            )

        ax.set_xticks(list(x))
        ax.set_xticklabels([DIST_LABELS.get(d, d) for d in dists])
        ax.set_ylabel(METRIC_LABELS.get(metric, metric))
        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} vs Distribution")
        add_legend(ax)
        save_fig(fig, os.path.join(output_dir, "figures"),
                 f"exp1b_{metric}_vs_distribution")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default="results/comparison")
    parser.add_argument("--output_dir",  default="results/comparison")
    args = parser.parse_args()

    csv_a = os.path.join(args.results_dir, "exp1a_query_range_results.csv")
    csv_b = os.path.join(args.results_dir, "exp1b_distribution_results.csv")

    if os.path.isfile(csv_a):
        df_a = pd.read_csv(csv_a)
        plot_query_range(df_a, args.output_dir)
        print("[plot_comparison] Exp1a plots generated.")
    else:
        print(f"[plot_comparison] Skipping Exp1a (file not found: {csv_a})")

    if os.path.isfile(csv_b):
        df_b = pd.read_csv(csv_b)
        plot_distribution(df_b, args.output_dir)
        print("[plot_comparison] Exp1b plots generated.")
    else:
        print(f"[plot_comparison] Skipping Exp1b (file not found: {csv_b})")


if __name__ == "__main__":
    main()
