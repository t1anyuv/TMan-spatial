"""
BaseJavaMethod — bridge between Python experiment runner and Java JAR-based baselines.

All three baselines (TMan / LMSFC / BMTree) are packaged as executable JARs
and stored under experiments/jars/.  This class invokes them via subprocess
and parses their JSON output into the standard BaseMethod interface.

Expected JAR CLI protocol
─────────────────────────
Each JAR must support two sub-commands:

  BUILD
  ─────
  java -jar <jar_path> build \
       --data    <segments_tsv>   \
       --index   <index_out_dir>  \
       --params  '<json_params>'

  Stdout (JSON):
    {"status": "ok", "training_time_s": 123.4, "index_size_kb": 5678.9}

  QUERY
  ─────
  java -jar <jar_path> query \
       --index       <index_dir>    \
       --window      "lon_min,lat_min,lon_max,lat_max" \
       --query_type  SRQ|SS|top-k   \
       --k           10

  Stdout (JSON, one line per query OR batched):
    {"visited_cells": 42, "valid_cells": 18,
     "scan_range_intervals": 7, "result_count": 5}

  BATCH QUERY (preferred for large workloads)
  ─────────────────────────────────────────────
  java -jar <jar_path> batch_query \
       --index       <index_dir>   \
       --queries     <queries_csv> \
       --query_type  SRQ

  Stdout (JSON list):
    [{"visited_cells":..., "valid_cells":..., "scan_range_intervals":...}, ...]

If a JAR does not yet exist the method silently returns zero-filled metrics,
so the Python experiment runner can still produce a skeleton result CSV.
"""

from __future__ import annotations
import json
import os
import subprocess
import tempfile
import time
from typing import List

from methods.base import BaseMethod, Segment, QueryWindow

JAVA_BIN = os.environ.get("JAVA_BIN", "java")
JAR_DIR  = os.path.join("experiments", "jars")


def _jar_exists(jar_path: str) -> bool:
    return os.path.isfile(jar_path)


def _run_java(args: List[str], timeout: int = 3600) -> dict | list:
    """Run a Java command and parse its stdout as JSON."""
    result = subprocess.run(
        [JAVA_BIN] + args,
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Java process failed (exit {result.returncode}):\n{result.stderr}"
        )
    return json.loads(result.stdout.strip())


class BaseJavaMethod(BaseMethod):
    """
    Abstract adapter for JAR-packaged baselines running on the TMan-spatial
    infrastructure.

    Subclasses only need to set:
      - self.jar_path   : path to the JAR file
      - self.main_class : (optional) fully-qualified Java main class name;
                          omit if the JAR defines a Main-Class manifest entry
    """

    jar_path:   str = ""
    main_class: str = ""

    def __init__(self, name: str, params: dict):
        super().__init__(name, params)
        self._index_dir:      str   = ""
        self._build_time_s:   float = 0.0
        self._index_size_kb:  float = 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _java_args(self, sub_cmd: str, extra: List[str]) -> List[str]:
        """Build the java argument list, respecting main_class vs jar mode."""
        if self.main_class:
            return ["-cp", self.jar_path, self.main_class, sub_cmd] + extra
        return ["-jar", self.jar_path, sub_cmd] + extra

    def _write_segments_tsv(self, segments: List[Segment],
                             tmp_dir: str) -> str:
        """Write segments to a temporary TSV that the JAR can read."""
        import csv
        path = os.path.join(tmp_dir, "segments.tsv")
        with open(path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["seg_id", "start_lon", "start_lat",
                              "end_lon", "end_lat"])
            for i, seg in enumerate(segments):
                writer.writerow([i, seg[0], seg[1], seg[2], seg[3]])
        return path

    def _write_queries_csv(self, windows: List[QueryWindow],
                            tmp_dir: str) -> str:
        import csv
        path = os.path.join(tmp_dir, "queries.csv")
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["lon_min", "lat_min", "lon_max", "lat_max"])
            writer.writerows(windows)
        return path

    # ------------------------------------------------------------------
    # BaseMethod interface
    # ------------------------------------------------------------------

    def build(self, segments: List[Segment], **kwargs) -> None:
        if not _jar_exists(self.jar_path):
            print(f"  [WARN] JAR not found: {self.jar_path} — skipping build.")
            return

        # Persist the index to a stable directory next to the JAR
        self._index_dir = os.path.join(
            JAR_DIR, f"{self.name}_index"
        )
        os.makedirs(self._index_dir, exist_ok=True)

        with tempfile.TemporaryDirectory() as tmp:
            data_tsv   = self._write_segments_tsv(segments, tmp)
            params_str = json.dumps(self.params)
            args = self._java_args("build", [
                "--data",   data_tsv,
                "--index",  self._index_dir,
                "--params", params_str,
            ])
            t0  = time.perf_counter()
            out = _run_java(args)
            self._build_time_s  = time.perf_counter() - t0
            self._index_size_kb = float(out.get("index_size_kb", 0.0))
            if "training_time_s" in out:
                self._build_time_s = float(out["training_time_s"])

    def query(self, window: QueryWindow,
              query_type: str = "SRQ", k: int = 10) -> dict:
        if not _jar_exists(self.jar_path) or not self._index_dir:
            return {"results": [], "visited_cells": 0,
                    "valid_cells": 0, "scan_range_intervals": 0}
        w_str = f"{window[0]},{window[1]},{window[2]},{window[3]}"
        args  = self._java_args("query", [
            "--index",      self._index_dir,
            "--window",     w_str,
            "--query_type", query_type,
            "--k",          str(k),
        ])
        out = _run_java(args)
        return {
            "results":               out.get("results", []),
            "visited_cells":         int(out.get("visited_cells", 0)),
            "valid_cells":           int(out.get("valid_cells", 0)),
            "scan_range_intervals":  int(out.get("scan_range_intervals", 0)),
        }

    def batch_query(self, windows: List[QueryWindow],
                    query_type: str = "SRQ", k: int = 10) -> List[dict]:
        """
        Execute a batch of queries in a single JVM invocation.
        Preferred over calling query() in a loop for large window sets.
        """
        if not _jar_exists(self.jar_path) or not self._index_dir:
            return [{"visited_cells": 0, "valid_cells": 0,
                     "scan_range_intervals": 0}] * len(windows)

        with tempfile.TemporaryDirectory() as tmp:
            q_csv = self._write_queries_csv(windows, tmp)
            args  = self._java_args("batch_query", [
                "--index",      self._index_dir,
                "--queries",    q_csv,
                "--query_type", query_type,
                "--k",          str(k),
            ])
            return _run_java(args)

    def index_size_kb(self) -> float:
        return self._index_size_kb

    def training_time_h(self) -> float:
        return self._build_time_s / 3600.0
