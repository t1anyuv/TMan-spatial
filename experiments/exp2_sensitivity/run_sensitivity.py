"""
Exp 2 — Sensitivity Analysis

Sweep LETI's own parameters and report all metrics:

  2.1  Training Time    vs  resolution  (minTrajs=3, SKE)
  2.2  Training Time    vs  minTrajs    (resolution=8, SKE)
  2.3  Latency          vs  resolution  (minTrajs=3, SKE)
  2.4  Latency          vs  minTrajs    (resolution=8, SKE)
  2.5  Valid Cells       vs  resolution  (minTrajs=3, SKE)
  2.6  Valid Cells       vs  minTrajs    (resolution=8, SKE)
  2.7  Visited Cells    vs  resolution  (minTrajs=3, SKE)
  2.8  Visited Cells    vs  minTrajs    (resolution=8, SKE)
  2.9  ScanRangeIntervals vs resolution (minTrajs=3, SKE)
  2.10 ScanRangeIntervals vs minTrajs   (resolution=8, SKE)
  2.11 Index Size        vs  resolution
  2.12 Index Size        vs  minTrajs
  2.13 Index Size        vs  alpha×beta  (resolution=8, minTrajs=3)

Also sweeps baselines (TShape, XZ*, LMSFC, BMTree) for Visited Cells and
Scan Range Intervals (they don't have resolution/minTrajs, use LETI params
as the x-axis and compare at their fixed operating point).
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
from methods.leti.leti_index  import LETIIndex
from methods.baselines.tshape import TShapeIndex
from methods.baselines.xzstar import XZStarIndex
from methods.baselines.lmsfc  import LMSFCIndex
from methods.baselines.bmtree import BMTreeIndex

FIXED_DIST    = "SKE"
FIXED_RES     = 8
FIXED_MINTRAJ = 3


class SensitivityExperiment(BaseExperiment):
    def __init__(self, config_path: str):
        super().__init__(config_path,
                         results_subdir="sensitivity",
                         exp_name="exp2_sensitivity")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_leti(self, resolution: int, min_trajs: int,
                    alpha: int = 3, beta: int = 3) -> LETIIndex:
        idx = LETIIndex({
            "resolution": resolution, "minTrajs": min_trajs,
            "alpha": alpha, "beta": beta,
            "curve_type": "RLSFC", "adaptive": True,
        })
        return idx

    def _eval_leti(self, idx: LETIIndex, segments, windows,
                   param_name: str, param_value: str,
                   dataset: str) -> ExperimentMetrics:
        idx.build(segments)
        qrs = run_queries(idx, windows)
        m = ExperimentMetrics(
            method      = "LETI",
            dataset     = dataset,
            param_name  = param_name,
            param_value = param_value,
        )
        for qr in qrs:
            m.record_query(qr)
        m.index_size_kb   = idx.index_size_kb()
        m.training_time_h = idx.training_time_h()
        m.finalize()
        return m

    def _eval_baseline(self, method, segments, windows,
                       param_name: str, param_value: str,
                       dataset: str) -> ExperimentMetrics:
        method.build(segments)
        qrs = run_queries(method, windows)
        m = ExperimentMetrics(
            method      = method.name,
            dataset     = dataset,
            param_name  = param_name,
            param_value = param_value,
        )
        for qr in qrs:
            m.record_query(qr)
        m.index_size_kb   = method.index_size_kb()
        m.training_time_h = method.training_time_h()
        m.finalize()
        return m

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self):
        cfg       = self.cfg
        dataset   = cfg["dataset"]["default"]
        seg_path  = self._get_dataset_path(dataset)
        query_dir = os.path.join("data", "processed", "queries")
        range_m   = cfg["query"]["range_m"]

        print(f"[Exp2] Loading segments from {seg_path} ...")
        segments = load_segments(seg_path)
        windows  = load_queries(query_dir, dataset, FIXED_DIST, range_m)

        all_metrics = []

        # ---- 2.1-2.2 / 2.3-2.4 / 2.5-2.6 / 2.7-2.8 / 2.9-2.10 / 2.11-2.12 ----
        # Sweep resolution (fix minTrajs=3)
        print("[Exp2] Sweeping resolution ...")
        for res in tqdm(cfg["sweep"]["resolution"], desc="resolution sweep"):
            idx = self._build_leti(resolution=res, min_trajs=FIXED_MINTRAJ)
            all_metrics.append(
                self._eval_leti(idx, segments, windows,
                                "resolution", str(res), dataset)
            )
            for Cls in [TShapeIndex, XZStarIndex, LMSFCIndex, BMTreeIndex]:
                bl = Cls()
                all_metrics.append(
                    self._eval_baseline(bl, segments, windows,
                                        "resolution", str(res), dataset)
                )

        # Sweep minTrajs (fix resolution=8)
        print("[Exp2] Sweeping minTrajs ...")
        for mt in tqdm(cfg["sweep"]["minTrajs"], desc="minTrajs sweep"):
            idx = self._build_leti(resolution=FIXED_RES, min_trajs=mt)
            all_metrics.append(
                self._eval_leti(idx, segments, windows,
                                "minTrajs", str(mt), dataset)
            )
            for Cls in [TShapeIndex, XZStarIndex, LMSFCIndex, BMTreeIndex]:
                bl = Cls()
                all_metrics.append(
                    self._eval_baseline(bl, segments, windows,
                                        "minTrajs", str(mt), dataset)
                )

        # ---- 2.13 Index Size vs alpha×beta ----
        print("[Exp2] Sweeping alpha×beta ...")
        for ab in tqdm(cfg["sweep"]["alpha_beta"], desc="alpha×beta sweep"):
            a, b = ab
            idx = self._build_leti(resolution=FIXED_RES,
                                   min_trajs=FIXED_MINTRAJ,
                                   alpha=a, beta=b)
            idx.build(segments)
            m = ExperimentMetrics(
                method      = "LETI",
                dataset     = dataset,
                param_name  = "alpha_beta",
                param_value = f"{a}x{b}",
            )
            m.index_size_kb   = idx.index_size_kb()
            m.training_time_h = idx.training_time_h()
            # No queries needed for index-size-only entry; finalize with zeros
            m.finalize()
            all_metrics.append(m)

            for Cls, name in [(TShapeIndex, "TShape"), (XZStarIndex, "XZ*")]:
                bl = Cls()
                bl.build(segments)
                bm = ExperimentMetrics(
                    method      = name,
                    dataset     = dataset,
                    param_name  = "alpha_beta",
                    param_value = f"{a}x{b}",
                )
                bm.index_size_kb = bl.index_size_kb()
                bm.finalize()
                all_metrics.append(bm)

        self.logger.save(all_metrics, suffix="results")
        print("\n[Exp2] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()
    SensitivityExperiment(args.config).run()
