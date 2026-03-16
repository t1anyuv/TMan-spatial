"""
Exp 4 — Scalability Experiments (Distributed Setting)

Uses the CD-Taxi dataset (distributed default).

  4.1  Latency vs data scale  : ×100, ×500, ×1000, ×2000
  4.2  Latency vs node count  : 2, 4, 6, 8 nodes

NOTE: Actual distributed execution requires a running HBase + distributed
cluster. This script assumes each (scale, node) configuration is accessible
through environment variables or a cluster config file. Replace the stub
`run_distributed_query()` with your cluster-facing API call.
"""

from __future__ import annotations
import argparse
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tqdm import tqdm

from experiments.base_experiment import BaseExperiment, load_segments, load_queries
from evaluation.metrics import ExperimentMetrics, Stopwatch
from methods.leti.leti_index import LETIIndex


def run_distributed_query(method: LETIIndex, window, node_count: int,
                           query_type: str = "SRQ") -> dict:
    """
    Execute one query in a distributed setting with `node_count` nodes.

    TODO: Replace with actual distributed cluster call.
    This stub simulates by calling the local query method.
    """
    with Stopwatch() as sw:
        out = method.query(window, query_type=query_type)
    out["latency_ms"] = sw.elapsed_ms
    return out


class ScalabilityExperiment(BaseExperiment):
    def __init__(self, config_path: str):
        super().__init__(config_path,
                         results_subdir="scalability",
                         exp_name="exp4_scalability")

    def run(self):
        cfg       = self.cfg
        dataset   = "cdtaxi"
        seg_path  = self._get_dataset_path(dataset)
        query_dir = os.path.join("data", "processed", "queries")
        range_m   = cfg["query"]["range_m"]
        dist      = cfg["query"]["distribution"]
        scales    = cfg["sweep"]["data_scales"]
        nodes     = cfg["sweep"]["nodes"]

        print(f"[Exp4] Loading base segments from {seg_path} ...")
        base_segments = load_segments(seg_path)
        windows = load_queries(query_dir, dataset, dist, range_m)

        all_metrics = []

        # ---- 4.1 Data scale sweep (fix nodes = default) ----
        default_nodes = cfg["distributed"]["nodes"]
        print(f"\n[Exp4] 4.1 Data scale sweep (nodes={default_nodes}) ...")
        for scale in tqdm(scales, desc="data scale sweep"):
            # Replicate base segments to simulate larger dataset
            scaled_segs = (base_segments * scale)[:len(base_segments) * scale]
            p = cfg["params"]
            method = LETIIndex({
                "resolution": p["resolution"], "alpha": p["alpha"],
                "beta": p["beta"], "minTrajs": p["minTrajs"],
                "curve_type": "RLSFC", "adaptive": True,
            })
            method.build(scaled_segs)
            m = ExperimentMetrics(
                method      = "LETI",
                dataset     = dataset,
                param_name  = "data_scale",
                param_value = f"x{scale}",
            )
            for w in windows:
                out = run_distributed_query(method, w, node_count=default_nodes)
                from evaluation.metrics import QueryResult
                m.record_query(QueryResult(
                    latency_ms           = out.get("latency_ms", 0.0),
                    visited_cells        = out.get("visited_cells", 0),
                    valid_cells          = out.get("valid_cells", 0),
                    scan_range_intervals = out.get("scan_range_intervals", 0),
                ))
            m.finalize()
            all_metrics.append(m)

        # ---- 4.2 Node count sweep (fix scale = ×1) ----
        print(f"\n[Exp4] 4.2 Node count sweep (data=base) ...")
        p = cfg["params"]
        method_base = LETIIndex({
            "resolution": p["resolution"], "alpha": p["alpha"],
            "beta": p["beta"], "minTrajs": p["minTrajs"],
            "curve_type": "RLSFC", "adaptive": True,
        })
        method_base.build(base_segments)

        for node_count in tqdm(nodes, desc="node count sweep"):
            m = ExperimentMetrics(
                method      = "LETI",
                dataset     = dataset,
                param_name  = "nodes",
                param_value = str(node_count),
            )
            for w in windows:
                out = run_distributed_query(method_base, w,
                                             node_count=node_count)
                from evaluation.metrics import QueryResult
                m.record_query(QueryResult(
                    latency_ms           = out.get("latency_ms", 0.0),
                    visited_cells        = out.get("visited_cells", 0),
                    valid_cells          = out.get("valid_cells", 0),
                    scan_range_intervals = out.get("scan_range_intervals", 0),
                ))
            m.finalize()
            all_metrics.append(m)

        self.logger.save(all_metrics, suffix="results")
        print("\n[Exp4] Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()
    ScalabilityExperiment(args.config).run()
