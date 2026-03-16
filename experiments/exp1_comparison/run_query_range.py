"""
Exp 1a — Comparative Experiment: Varying Query Range

Vary query window side length (100, 500, 1000, 1500, 2000 m) and compare:
  LETI vs TShape vs XZ* vs LMSFC vs BMTree

Metrics: Latency (ms), Visited Cells, Scan Range Intervals
Datasets: T-Drive (UNI), T-Drive (SKE), T-Drive (GUS)  +  CD-Taxi analogues
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
from evaluation.metrics import ExperimentMetrics
from methods.leti.leti_index       import LETIIndex
from methods.baselines.tshape      import TShapeIndex
from methods.baselines.xzstar      import XZStarIndex
from methods.baselines.lmsfc       import LMSFCIndex
from methods.baselines.bmtree      import BMTreeIndex


def get_methods(cfg: dict):
    p = cfg["params"]
    return {
        "LETI":   LETIIndex({"resolution": p["resolution"], "alpha": p["alpha"],
                              "beta": p["beta"], "minTrajs": p["minTrajs"],
                              "curve_type": "RLSFC", "adaptive": True}),
        "TShape": TShapeIndex(),
        "XZ*":    XZStarIndex({"grid": [2, 2]}),
        "LMSFC":  LMSFCIndex(),
        "BMTree": BMTreeIndex(),
    }


class QueryRangeExperiment(BaseExperiment):
    def __init__(self, config_path: str):
        super().__init__(config_path,
                         results_subdir="comparison",
                         exp_name="exp1a_query_range")

    def run(self):
        cfg      = self.cfg
        datasets = [cfg["dataset"]["default"]]
        ranges   = cfg["sweep"]["query_ranges"]
        dists    = cfg["sweep"]["distributions"]
        query_dir = os.path.join("data", "processed", "queries")

        all_metrics = []

        for dataset in datasets:
            seg_path = self._get_dataset_path(dataset)
            print(f"\n[Exp1a] Loading segments from {seg_path} ...")
            segments = load_segments(seg_path)

            methods = get_methods(cfg)

            print(f"[Exp1a] Building indexes for dataset={dataset} ...")
            for mname, method in methods.items():
                print(f"  Building {mname} ...")
                method.build(segments)

            for dist in dists:
                for range_m in tqdm(ranges, desc=f"{dataset}/{dist} query-range sweep"):
                    windows = load_queries(query_dir, dataset, dist, range_m)
                    for mname, method in methods.items():
                        query_results = run_queries(
                            method, windows,
                            query_type=cfg["query"]["type"]
                        )
                        m = ExperimentMetrics(
                            method      = mname,
                            dataset     = dataset,
                            param_name  = "query_range_m",
                            param_value = str(range_m),
                        )
                        # Also tag the distribution used
                        m.param_name = f"query_range_m|dist={dist}"
                        for qr in query_results:
                            m.record_query(qr)
                        m.index_size_kb    = method.index_size_kb()
                        m.training_time_h  = method.training_time_h()
                        m.finalize()
                        all_metrics.append(m)

        self.logger.save(all_metrics, suffix="results")
        print("\n[Exp1a] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()
    QueryRangeExperiment(args.config).run()
