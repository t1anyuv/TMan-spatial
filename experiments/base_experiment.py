"""
Abstract base class for experiment runners.
"""

from __future__ import annotations
import csv
import os
import yaml
import time
from abc import ABC, abstractmethod
from typing import List, Tuple

from evaluation.metrics import ExperimentMetrics, QueryResult, Stopwatch
from evaluation.logger  import ResultLogger

Segment     = Tuple[float, float, float, float]
QueryWindow = Tuple[float, float, float, float]


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_segments(processed_path: str) -> List[Segment]:
    """Load pre-processed trajectory segments from TSV."""
    segs = []
    fpath = os.path.join(processed_path, "segments.tsv")
    if not os.path.isfile(fpath):
        raise FileNotFoundError(
            f"Segment file not found: {fpath}\n"
            f"Run data/preprocess/preprocess_tdrive.py or preprocess_cdtaxi.py first."
        )
    with open(fpath, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            segs.append((
                float(row["start_lon"]), float(row["start_lat"]),
                float(row["end_lon"]),   float(row["end_lat"]),
            ))
    return segs


def load_queries(query_dir: str, dataset: str,
                 distribution: str, range_m: int) -> List[QueryWindow]:
    """Load pre-generated query windows from CSV."""
    fname = f"{dataset}_{distribution}_{range_m}m.csv"
    fpath = os.path.join(query_dir, fname)
    if not os.path.isfile(fpath):
        raise FileNotFoundError(
            f"Query file not found: {fpath}\n"
            f"Run data/preprocess/query_generator.py first."
        )
    windows = []
    with open(fpath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            windows.append((
                float(row["lon_min"]), float(row["lat_min"]),
                float(row["lon_max"]), float(row["lat_max"]),
            ))
    return windows


def run_queries(method, windows: List[QueryWindow],
                query_type: str = "SRQ", k: int = 10) -> List[QueryResult]:
    """Execute a list of query windows against a built index."""
    results = []
    for w in windows:
        with Stopwatch() as sw:
            out = method.query(w, query_type=query_type, k=k)
        results.append(QueryResult(
            latency_ms           = sw.elapsed_ms,
            visited_cells        = out.get("visited_cells", 0),
            valid_cells          = out.get("valid_cells", 0),
            scan_range_intervals = out.get("scan_range_intervals", 0),
        ))
    return results


class BaseExperiment(ABC):
    """
    Template method pattern for experiment execution.

    Subclasses implement `run()` which should:
      1. Load config, segments, and queries
      2. Build each method's index
      3. Execute queries
      4. Record ExperimentMetrics
      5. Call self.logger.save(metrics_list, suffix=...)
    """

    def __init__(self, config_path: str, results_subdir: str, exp_name: str):
        self.cfg          = load_config(config_path)
        self.results_dir  = os.path.join(self.cfg["output"]["results_dir"], results_subdir)
        self.logger       = ResultLogger(
            results_dir     = self.results_dir,
            experiment_name = exp_name,
            fmt             = self.cfg["output"].get("log_format", "csv"),
        )
        os.makedirs(self.results_dir, exist_ok=True)

    @abstractmethod
    def run(self) -> None: ...

    def _get_dataset_path(self, dataset_key: str) -> str:
        return self.cfg["dataset"][dataset_key]["path"]
