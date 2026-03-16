"""
Exp 3 — Ablation Study

Compare LETI variants on both datasets (T-Drive + CD-Taxi):

  | Variant        | Change vs LETI                             |
  |----------------|--------------------------------------------|
  | LETI           | Full system (RLSFC + adaptive params)      |
  | LETI (LMSFC)   | Replace RLSFC with LMSFC curve             |
  | LETI (LBMT)    | Replace RLSFC with LBMT curve              |
  | LETI -ab       | Fixed parameters, no adaptive tuning       |

Metric: Latency (ms)
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
from methods.leti.leti_index import LETIIndex

ABLATION_VARIANTS = {
    "LETI":          {"curve_type": "RLSFC", "adaptive": True},
    "LETI (LMSFC)":  {"curve_type": "LMSFC", "adaptive": True},
    "LETI (LBMT)":   {"curve_type": "LBMT",  "adaptive": True},
    "LETI -ab":      {"curve_type": "RLSFC", "adaptive": False},
}


class AblationExperiment(BaseExperiment):
    def __init__(self, config_path: str):
        super().__init__(config_path,
                         results_subdir="ablation",
                         exp_name="exp3_ablation")

    def run(self):
        cfg       = self.cfg
        datasets  = list(cfg["dataset"].keys())
        # Only run on tdrive and cdtaxi (skip meta-key 'default')
        datasets  = [d for d in datasets if d in ("tdrive", "cdtaxi")]
        query_dir = os.path.join("data", "processed", "queries")
        range_m   = cfg["query"]["range_m"]
        dist      = cfg["query"]["distribution"]

        all_metrics = []

        for dataset in datasets:
            seg_path = self._get_dataset_path(dataset)
            print(f"\n[Exp3] Dataset: {dataset}")
            segments = load_segments(seg_path)
            windows  = load_queries(query_dir, dataset, dist, range_m)

            for variant_name, extra_params in tqdm(
                ABLATION_VARIANTS.items(), desc=f"Ablation ({dataset})"
            ):
                p = cfg["params"]
                params = {
                    "resolution": p["resolution"],
                    "alpha":      p["alpha"],
                    "beta":       p["beta"],
                    "minTrajs":   p["minTrajs"],
                    **extra_params,
                }
                method = LETIIndex(params)
                method.build(segments)
                qrs = run_queries(method, windows,
                                  query_type=cfg["query"]["type"])
                m = ExperimentMetrics(
                    method      = variant_name,
                    dataset     = dataset,
                    param_name  = "variant",
                    param_value = variant_name,
                )
                for qr in qrs:
                    m.record_query(qr)
                m.index_size_kb   = method.index_size_kb()
                m.training_time_h = method.training_time_h()
                m.finalize()
                all_metrics.append(m)

        self.logger.save(all_metrics, suffix="results")
        print("\n[Exp3] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()
    AblationExperiment(args.config).run()
