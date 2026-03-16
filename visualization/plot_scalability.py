"""
Plot Exp 4 results: Scalability experiments.

  4.1  Line chart: Latency vs data scale (×100 … ×2000)
  4.2  Line chart: Latency vs node count (2 … 8)
"""

from __future__ import annotations
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

from visualization.plot_utils import (
    apply_style, method_style, save_fig, add_legend
)

SCALE_ORDER = ["x100", "x500", "x1000", "x2000"]
SCALE_LABELS = {"x100": "×100", "x500": "×500",
                "x1000": "×1000", "x2000": "×2000"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default="results/scalability")
    parser.add_argument("--output_dir",  default="results/scalability")
    args = parser.parse_args()

    csv_path = os.path.join(args.results_dir, "exp4_scalability_results.csv")
    if not os.path.isfile(csv_path):
        print(f"[plot_scalability] File not found: {csv_path}")
        return

    apply_style()
    df = pd.read_csv(csv_path)
    s  = method_style("LETI")

    # ---- 4.1 Data scale ----
    scale_df = df[df["param_name"] == "data_scale"].copy()
    scale_df["order"] = scale_df["param_value"].map(
        {v: i for i, v in enumerate(SCALE_ORDER)}
    )
    scale_df = scale_df.sort_values("order")

    fig, ax = plt.subplots(figsize=(5.5, 3.8))
    ax.plot(range(len(scale_df)), scale_df["avg_latency_ms"],
            color=s["color"], marker=s["marker"],
            linestyle=s["linestyle"], linewidth=1.8, markersize=7, label="LETI")
    ax.set_xticks(range(len(scale_df)))
    ax.set_xticklabels([SCALE_LABELS.get(v, v) for v in scale_df["param_value"]])
    ax.set_xlabel("Data Scale (×base)")
    ax.set_ylabel("Average Latency (ms)")
    ax.set_title("Scalability: Latency vs Data Scale (CD-Taxi, 4 nodes)")
    add_legend(ax)
    save_fig(fig, os.path.join(args.output_dir, "figures"),
             "exp4_latency_vs_data_scale")

    # ---- 4.2 Node count ----
    node_df = df[df["param_name"] == "nodes"].copy()
    node_df["node_int"] = node_df["param_value"].astype(int)
    node_df = node_df.sort_values("node_int")

    fig, ax = plt.subplots(figsize=(5.5, 3.8))
    ax.plot(node_df["node_int"], node_df["avg_latency_ms"],
            color=s["color"], marker=s["marker"],
            linestyle=s["linestyle"], linewidth=1.8, markersize=7, label="LETI")
    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Average Latency (ms)")
    ax.set_title("Scalability: Latency vs Node Count (CD-Taxi, base scale)")
    ax.set_xticks(node_df["node_int"].tolist())
    add_legend(ax)
    save_fig(fig, os.path.join(args.output_dir, "figures"),
             "exp4_latency_vs_nodes")

    print("[plot_scalability] Scalability plots generated.")


if __name__ == "__main__":
    main()
