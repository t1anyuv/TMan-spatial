"""
Plot Exp 3 results: Ablation study.

Grouped bar chart comparing LETI variants on T-Drive and CD-Taxi.
Metric: Latency (ms)
"""

from __future__ import annotations
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from visualization.plot_utils import (
    apply_style, method_style, save_fig, add_legend
)

ABLATION_ORDER = ["LETI", "LETI (LMSFC)", "LETI (LBMT)", "LETI -ab"]
DATASET_LABELS = {"tdrive": "T-Drive", "cdtaxi": "CD-Taxi"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default="results/ablation")
    parser.add_argument("--output_dir",  default="results/ablation")
    args = parser.parse_args()

    csv_path = os.path.join(args.results_dir, "exp3_ablation_results.csv")
    if not os.path.isfile(csv_path):
        print(f"[plot_ablation] File not found: {csv_path}")
        return

    apply_style()
    df = pd.read_csv(csv_path)
    datasets = [d for d in df["dataset"].unique() if d in DATASET_LABELS]

    fig, axes = plt.subplots(1, len(datasets), figsize=(4 * len(datasets), 4), sharey=True)
    if len(datasets) == 1:
        axes = [axes]

    bar_width = 0.55
    x_pos     = np.arange(len(ABLATION_ORDER))

    for ax, dataset in zip(axes, datasets):
        sub = df[df["dataset"] == dataset]
        vals = [
            sub[sub["method"] == v]["avg_latency_ms"].mean()
            for v in ABLATION_ORDER
        ]
        colors = [method_style(v)["color"] for v in ABLATION_ORDER]
        bars   = ax.bar(x_pos, vals, width=bar_width, color=colors, alpha=0.88)

        ax.set_xticks(x_pos)
        ax.set_xticklabels([v.replace(" ", "\n") for v in ABLATION_ORDER],
                           fontsize=8)
        ax.set_title(DATASET_LABELS.get(dataset, dataset))
        ax.set_ylabel("Average Latency (ms)" if ax == axes[0] else "")

        for bar, val in zip(bars, vals):
            if val and val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() * 1.01,
                        f"{val:.1f}", ha="center", va="bottom", fontsize=8)

    fig.suptitle("Ablation Study — Latency (ms)", fontsize=12, fontweight="bold")
    plt.tight_layout()
    save_fig(fig, os.path.join(args.output_dir, "figures"), "exp3_ablation_latency")
    print("[plot_ablation] Ablation plot generated.")


if __name__ == "__main__":
    main()
