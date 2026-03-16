"""
Plot Exp 2 results: Sensitivity analysis.

Generates line plots for each metric as a function of:
  - resolution  (x-axis: 6–10)
  - minTrajs    (x-axis: 1–5)
  - alpha×beta  (x-axis: 2×2 … 5×5, index size only)
"""

from __future__ import annotations
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

from visualization.plot_utils import (
    apply_style, method_style, save_fig, add_legend, METRIC_LABELS
)

METRICS_RESOLUTION = [
    "avg_latency_ms",
    "avg_visited_cells",
    "avg_valid_cells",
    "avg_scan_range_intervals",
    "training_time_h",
    "index_size_kb",
]

METHODS_RESOLUTION = ["LETI", "TShape", "XZ*", "LMSFC", "BMTree"]
METHODS_INDEXSIZE  = ["LETI", "TShape", "XZ*"]


def plot_param_sweep(df: pd.DataFrame, param: str,
                     metrics: list, methods: list, output_dir: str):
    apply_style()
    sub = df[df["param_name"] == param].copy()
    # Convert param_value to numeric if possible
    try:
        sub[param] = sub["param_value"].astype(int)
    except ValueError:
        sub[param] = sub["param_value"]

    for metric in metrics:
        fig, ax = plt.subplots(figsize=(5.5, 3.8))
        for method in methods:
            m_df = sub[sub["method"] == method].sort_values(param)
            if m_df.empty or m_df[metric].isna().all():
                continue
            s = method_style(method)
            ax.plot(m_df[param], m_df[metric],
                    color=s["color"], marker=s["marker"],
                    linestyle=s["linestyle"], label=s["label"],
                    linewidth=1.8, markersize=6)
        ax.set_xlabel(param.replace("_", " ").title())
        ax.set_ylabel(METRIC_LABELS.get(metric, metric))
        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} vs {param}")
        add_legend(ax)
        save_fig(fig, os.path.join(output_dir, "figures"),
                 f"exp2_{metric}_vs_{param}")


def plot_alphabeta_indexsize(df: pd.DataFrame, output_dir: str):
    apply_style()
    sub = df[df["param_name"] == "alpha_beta"].copy()
    fig, ax = plt.subplots(figsize=(5.5, 3.8))
    for method in METHODS_INDEXSIZE:
        m_df = sub[sub["method"] == method]
        if m_df.empty:
            continue
        s = method_style(method)
        ax.bar(m_df["param_value"], m_df["index_size_kb"],
               color=s["color"], alpha=0.8, label=s["label"], width=0.4)
    ax.set_xlabel("alpha × beta")
    ax.set_ylabel(METRIC_LABELS["index_size_kb"])
    ax.set_title("Index Size vs alpha × beta")
    add_legend(ax)
    save_fig(fig, os.path.join(output_dir, "figures"),
             "exp2_index_size_vs_alpha_beta")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default="results/sensitivity")
    parser.add_argument("--output_dir",  default="results/sensitivity")
    args = parser.parse_args()

    csv_path = os.path.join(args.results_dir, "exp2_sensitivity_results.csv")
    if not os.path.isfile(csv_path):
        print(f"[plot_sensitivity] File not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    plot_param_sweep(df, "resolution", METRICS_RESOLUTION, METHODS_RESOLUTION, args.output_dir)
    plot_param_sweep(df, "minTrajs",   METRICS_RESOLUTION, METHODS_RESOLUTION, args.output_dir)
    plot_alphabeta_indexsize(df, args.output_dir)
    print("[plot_sensitivity] All sensitivity plots generated.")


if __name__ == "__main__":
    main()
