"""
LETI Index — stub implementation.

Replace the body of each method with your actual LETI implementation.
The interface contract (method signatures and return shapes) must be preserved.
"""

from __future__ import annotations
import time
from typing import List

from methods.base import BaseMethod, Segment, QueryWindow


class LETIIndex(BaseMethod):
    """
    LETI: Learned Efficient Trajectory Index.

    Supported curve_type values:
      - 'RLSFC'  : Reinforcement-Learning guided Space-Filling Curve (default)
      - 'LMSFC'  : SMBO-optimized LSFC (ablation)
      - 'LBMT'   : Learned BMTree curve (ablation)

    When adaptive=False the parameter tuning is disabled (ablation: LETI -ab).
    """

    def __init__(self, params: dict | None = None):
        params = params or {}
        super().__init__(name="LETI", params=params)
        self.resolution  = params.get("resolution", 8)
        self.alpha       = params.get("alpha", 3)
        self.beta        = params.get("beta", 3)
        self.min_trajs   = params.get("minTrajs", 3)
        self.curve_type  = params.get("curve_type", "RLSFC")
        self.adaptive    = params.get("adaptive", True)
        self._build_time_s: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, segments: List[Segment], **kwargs) -> None:
        """
        Build the LETI index.

        TODO: replace with real implementation.
        """
        t0 = time.perf_counter()

        # ---- insert LETI index construction here ----
        # 1. Learn / apply SFC (RLSFC / LMSFC / LBMT)
        # 2. Partition space using (resolution, alpha, beta, minTrajs)
        # 3. Map trajectory segments to space-filling-curve encoded cells
        # 4. Build HBase / Redis index structures
        # ---------------------------------------------

        self._build_time_s = time.perf_counter() - t0

    def query(self, window: QueryWindow, query_type: str = "SRQ", k: int = 10) -> dict:
        """
        Execute a spatial query against the LETI index.

        Args:
            window     : (lon_min, lat_min, lon_max, lat_max)
            query_type : 'SRQ' | 'SS' | 'top-k'
            k          : number of results for top-k queries

        Returns:
            dict with 'results', 'visited_cells', 'valid_cells', 'scan_range_intervals'

        TODO: replace with real implementation.
        """
        return {
            "results":               [],
            "visited_cells":         0,
            "valid_cells":           0,
            "scan_range_intervals":  0,
        }

    def index_size_kb(self) -> float:
        """
        Return total index size (HBase table + Redis auxiliary) in KB.

        TODO: replace with real measurement.
        """
        return 0.0

    def training_time_h(self) -> float:
        return self._build_time_s / 3600.0
