"""
Evaluation metrics for trajectory spatial index experiments.

Metrics tracked:
  - latency_ms          : average query execution time (milliseconds)
  - visited_cells       : number of spatial cells accessed per query
  - valid_cells         : effective (non-pruned) cells per query
  - scan_range_intervals: number of HBase range scan intervals per query
  - index_size_kb       : total index size in KB
  - training_time_h     : index construction / model training time in hours
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional
import statistics


@dataclass
class QueryResult:
    latency_ms: float
    visited_cells: int
    valid_cells: int
    scan_range_intervals: int


@dataclass
class IndexStats:
    index_size_kb: float
    training_time_h: float


@dataclass
class ExperimentMetrics:
    """Aggregated metrics across a batch of queries."""
    method: str
    dataset: str
    param_name: Optional[str]   = None
    param_value: Optional[str]  = None

    # Query metrics (averages)
    avg_latency_ms: float           = 0.0
    avg_visited_cells: float        = 0.0
    avg_valid_cells: float          = 0.0
    avg_scan_range_intervals: float = 0.0

    # Stddev (for error bars)
    std_latency_ms: float           = 0.0
    std_visited_cells: float        = 0.0

    # Index metrics
    index_size_kb: float            = 0.0
    training_time_h: float          = 0.0

    # Raw lists stored temporarily during aggregation (excluded from CSV output)
    _latencies:  List[float] = field(default_factory=list, repr=False)
    _visited:    List[int]   = field(default_factory=list, repr=False)
    _valid:      List[int]   = field(default_factory=list, repr=False)
    _intervals:  List[int]   = field(default_factory=list, repr=False)

    def record_query(self, result: QueryResult):
        self._latencies.append(result.latency_ms)
        self._visited.append(result.visited_cells)
        self._valid.append(result.valid_cells)
        self._intervals.append(result.scan_range_intervals)

    def finalize(self):
        """Compute aggregated statistics from recorded query results."""
        if self._latencies:
            self.avg_latency_ms           = statistics.mean(self._latencies)
            self.std_latency_ms           = statistics.stdev(self._latencies) if len(self._latencies) > 1 else 0.0
            self.avg_visited_cells        = statistics.mean(self._visited)
            self.std_visited_cells        = statistics.stdev(self._visited) if len(self._visited) > 1 else 0.0
            self.avg_valid_cells          = statistics.mean(self._valid)
            self.avg_scan_range_intervals = statistics.mean(self._intervals)

    def to_dict(self) -> dict:
        d = asdict(self)
        for key in ["_latencies", "_visited", "_valid", "_intervals"]:
            d.pop(key, None)
        return d


class Stopwatch:
    """Simple context-manager stopwatch returning elapsed time in ms."""

    def __init__(self):
        self._start: float = 0.0
        self.elapsed_ms: float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000.0
