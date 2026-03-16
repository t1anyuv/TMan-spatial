"""
Abstract base class for all spatial trajectory index methods.

Each concrete method (LETI, TShape, XZ*, LMSFC, BMTree) must subclass
BaseMethod and implement the four abstract methods below.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Tuple, Any

# A trajectory segment is represented as (start_lon, start_lat, end_lon, end_lat)
Segment = Tuple[float, float, float, float]

# A spatial query window: (lon_min, lat_min, lon_max, lat_max)
QueryWindow = Tuple[float, float, float, float]


class BaseMethod(ABC):
    """
    Interface for trajectory spatial index methods.

    Subclasses must implement:
      - build(segments, **kwargs)   → build / train the index
      - query(window, **kwargs)     → execute a spatial range query
      - index_size_kb()             → report index storage size
      - training_time_h()           → report total training/build time
    """

    def __init__(self, name: str, params: dict):
        self.name   = name
        self.params = params
        self._index: Any = None

    @abstractmethod
    def build(self, segments: List[Segment], **kwargs) -> None:
        """
        Build (and train) the index from a list of trajectory segments.

        Args:
            segments: list of (start_lon, start_lat, end_lon, end_lat) tuples
            **kwargs: method-specific build arguments
        """
        ...

    @abstractmethod
    def query(self, window: QueryWindow, **kwargs) -> dict:
        """
        Execute a spatial range query.

        Args:
            window: (lon_min, lat_min, lon_max, lat_max)
            **kwargs: method-specific query arguments (e.g. query_type, k)

        Returns:
            dict with keys:
              - 'results'              : list of matching segment IDs
              - 'visited_cells'        : int
              - 'valid_cells'          : int
              - 'scan_range_intervals' : int
        """
        ...

    @abstractmethod
    def index_size_kb(self) -> float:
        """Return the total index size in KB."""
        ...

    @abstractmethod
    def training_time_h(self) -> float:
        """Return the total training / build time in hours."""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, params={self.params})"
