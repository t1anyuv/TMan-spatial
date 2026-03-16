"""
Result logger: writes ExperimentMetrics to CSV and JSON files.
"""

from __future__ import annotations
import csv
import json
import os
from typing import List

from evaluation.metrics import ExperimentMetrics


class ResultLogger:
    def __init__(self, results_dir: str, experiment_name: str, fmt: str = "csv"):
        self.results_dir    = results_dir
        self.experiment_name = experiment_name
        self.fmt            = fmt
        os.makedirs(results_dir, exist_ok=True)

    def _path(self, suffix: str) -> str:
        return os.path.join(self.results_dir, f"{self.experiment_name}_{suffix}.{self.fmt}")

    def save(self, metrics_list: List[ExperimentMetrics], suffix: str = "results"):
        if self.fmt == "csv":
            self._save_csv(metrics_list, suffix)
        elif self.fmt == "json":
            self._save_json(metrics_list, suffix)
        else:
            raise ValueError(f"Unsupported format: {self.fmt}")

    def _save_csv(self, metrics_list: List[ExperimentMetrics], suffix: str):
        path = self._path(suffix)
        if not metrics_list:
            return
        fieldnames = [k for k in metrics_list[0].to_dict().keys()]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for m in metrics_list:
                writer.writerow(m.to_dict())
        print(f"[ResultLogger] Saved {len(metrics_list)} rows → {path}")

    def _save_json(self, metrics_list: List[ExperimentMetrics], suffix: str):
        path = self._path(suffix)
        data = [m.to_dict() for m in metrics_list]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"[ResultLogger] Saved {len(metrics_list)} entries → {path}")

    def append_row(self, row: dict, suffix: str = "results"):
        """Append a single row to an existing CSV (creates header on first call)."""
        path = self._path(suffix)
        exists = os.path.isfile(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                writer.writeheader()
            writer.writerow(row)
