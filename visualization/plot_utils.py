"""
Shared plotting utilities: color palette, method styles, figure saving.
"""

from __future__ import annotations
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ── Consistent style across all experiments ──────────────────────────────────

METHOD_STYLES: dict = {
    "LETI":          {"color": "#e41a1c", "marker": "o", "linestyle": "-",  "label": "LETI (ours)"},
    "TShape":        {"color": "#377eb8", "marker": "s", "linestyle": "--", "label": "TShape (TMan)"},
    "XZ*":           {"color": "#4daf4a", "marker": "^", "linestyle": "-.", "label": "XZ* (Trass)"},
    "LMSFC":         {"color": "#984ea3", "marker": "D", "linestyle": ":",  "label": "LMSFC (VLDB23)"},
    "BMTree":        {"color": "#ff7f00", "marker": "v", "linestyle": "--", "label": "BMTree (2025)"},
    "LETI (LMSFC)":  {"color": "#a65628", "marker": "p", "linestyle": "--", "label": "LETI (LMSFC)"},
    "LETI (LBMT)":   {"color": "#f781bf", "marker": "h", "linestyle": "-.", "label": "LETI (LBMT)"},
    "LETI -ab":      {"color": "#999999", "marker": "x", "linestyle": ":",  "label": "LETI -ab"},
}

METRIC_LABELS = {
    "avg_latency_ms":           "Average Latency (ms)",
    "avg_visited_cells":        "Visited Cells",
    "avg_valid_cells":          "Valid Cells",
    "avg_scan_range_intervals": "Scan Range Intervals",
    "index_size_kb":            "Index Size (KB)",
    "training_time_h":          "Training Time (h)",
}

DIST_LABELS = {
    "UNI": "Uniform",
    "SKE": "Skewed",
    "GUS": "Gaussian",
}


def apply_style():
    plt.rcParams.update({
        "font.family":   "sans-serif",
        "font.size":     11,
        "axes.grid":     True,
        "grid.alpha":    0.3,
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "figure.dpi":    150,
    })


def method_style(name: str) -> dict:
    return METHOD_STYLES.get(name, {"color": "#333333", "marker": "o",
                                     "linestyle": "-", "label": name})


def save_fig(fig: plt.Figure, output_dir: str, filename: str):
    os.makedirs(output_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        path = os.path.join(output_dir, f"{filename}.{ext}")
        fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {os.path.join(output_dir, filename)}.[pdf|png]")


def add_legend(ax: plt.Axes, loc: str = "best"):
    ax.legend(loc=loc, framealpha=0.9, fontsize=9)
