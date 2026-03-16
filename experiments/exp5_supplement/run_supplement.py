"""
Exp 5 — Supplementary Experiments

  5.1  Cost Model Validation
       Compare cost model predicted cost vs actual query time across
       query ranges [100, 500, 1000, 1500, 2000 m].
       Validates that the cost-model effectively guides RLSFC training.

  5.2  Multi Query Type Evaluation
       Compare LETI vs all baselines on:
         - Similarity Search (SS)
         - top-k query
       Distribution: SKE, Query range: 500 m
"""

from __future__ import annotations
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tqdm import tqdm

from experiments.base_experiment import (
    BaseExperiment, load_segments, load_queries, run_queries
)
from evaluation.metrics import ExperimentMetrics, Stopwatch
from methods.leti.leti_index  import LETIIndex
from methods.baselines.tshape import TShapeIndex
from methods.baselines.xzstar import XZStarIndex
from methods.baselines.lmsfc  import LMSFCIndex
from methods.baselines.bmtree import BMTreeIndex


def get_cost_model_prediction(method: LETIIndex, window) -> float:
    """
    Query the LETI cost model for a predicted cost value.

    TODO: implement by calling the actual RLSFC cost model.
    Returns a predicted cost (arbitrary unit, proportional to latency).
    """
    return 0.0


class SupplementaryExperiment(BaseExperiment):
    def __init__(self, config_path: str):
        super().__init__(config_path,
                         results_subdir="supplement",
                         exp_name="exp5_supplement")

    # ------------------------------------------------------------------
    # 5.1  Cost Model Validation
    # ------------------------------------------------------------------

    def run_cost_model(self, segments, windows_by_range: dict,
                       dataset: str) -> list:
        cfg = self.cfg
        p   = cfg["params"]
        method = LETIIndex({
            "resolution": p["resolution"], "alpha": p["alpha"],
            "beta": p["beta"], "minTrajs": p["minTrajs"],
            "curve_type": "RLSFC", "adaptive": True,
        })
        method.build(segments)

        rows = []
        for range_m, windows in windows_by_range.items():
            for w in windows:
                predicted = get_cost_model_prediction(method, w)
                with Stopwatch() as sw:
                    method.query(w, query_type="SRQ")
                rows.append({
                    "dataset":        dataset,
                    "query_range_m":  range_m,
                    "predicted_cost": predicted,
                    "actual_latency_ms": sw.elapsed_ms,
                })
        return rows

    # ------------------------------------------------------------------
    # 5.2  Multi Query Type
    # ------------------------------------------------------------------

    def run_query_types(self, segments, windows, dataset: str) -> list:
        cfg       = self.cfg
        p         = cfg["params"]
        query_types = cfg["sweep"]["query_types"]

        methods = {
            "LETI":   LETIIndex({"resolution": p["resolution"], "alpha": p["alpha"],
                                  "beta": p["beta"], "minTrajs": p["minTrajs"],
                                  "curve_type": "RLSFC", "adaptive": True}),
            "TShape": TShapeIndex(),
            "XZ*":    XZStarIndex({"grid": [2, 2]}),
            "LMSFC":  LMSFCIndex(),
            "BMTree": BMTreeIndex(),
        }
        for mname, method in methods.items():
            method.build(segments)

        all_metrics = []
        for qt in tqdm(query_types, desc="query type sweep"):
            for mname, method in methods.items():
                qrs = run_queries(method, windows, query_type=qt)
                m = ExperimentMetrics(
                    method      = mname,
                    dataset     = dataset,
                    param_name  = "query_type",
                    param_value = qt,
                )
                for qr in qrs:
                    m.record_query(qr)
                m.finalize()
                all_metrics.append(m)
        return all_metrics

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        cfg       = self.cfg
        dataset   = cfg["dataset"]["default"]
        seg_path  = self._get_dataset_path(dataset)
        query_dir = os.path.join("data", "processed", "queries")
        dist      = "SKE"
        range_m   = cfg["query"]["range_m"]

        print(f"[Exp5] Loading segments from {seg_path} ...")
        segments = load_segments(seg_path)
        windows  = load_queries(query_dir, dataset, dist, range_m)

        # 5.1 Cost model validation
        print("\n[Exp5] 5.1 Cost model validation ...")
        windows_by_range = {
            r: load_queries(query_dir, dataset, dist, r)
            for r in cfg["sweep"]["query_ranges"]
        }
        cost_rows = self.run_cost_model(segments, windows_by_range, dataset)
        # Save as CSV directly
        import csv
        cost_path = os.path.join(self.results_dir, "exp5_cost_model_results.csv")
        if cost_rows:
            with open(cost_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(cost_rows[0].keys()))
                writer.writeheader()
                writer.writerows(cost_rows)
            print(f"  Saved → {cost_path}")

        # 5.2 Multi query type evaluation
        print("\n[Exp5] 5.2 Multi query type evaluation ...")
        qt_metrics = self.run_query_types(segments, windows, dataset)
        self.logger.save(qt_metrics, suffix="query_types")

        print("\n[Exp5] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()
    SupplementaryExperiment(args.config).run()
