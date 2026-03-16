"""
TShape (TMan) baseline adapter — invokes experiments/jars/TMan.jar.

TMan-spatial provides the underlying spatial storage and indexing
infrastructure on which all comparative experiments are run.

JAR location : experiments/jars/TMan.jar
Main class   : (defined in JAR manifest; no --main_class needed)

Build command issued by BaseJavaMethod:
  java -jar experiments/jars/TMan.jar build \
       --data    <segments.tsv>             \
       --index   experiments/jars/TShape_index \
       --params  '{"resolution": 8}'

Query command:
  java -jar experiments/jars/TMan.jar query  \
       --index       experiments/jars/TShape_index \
       --window      "lon_min,lat_min,lon_max,lat_max" \
       --query_type  SRQ

Expected JSON stdout (query):
  {"visited_cells": 42, "valid_cells": 18,
   "scan_range_intervals": 7, "result_count": 5}
"""

from __future__ import annotations
import os
from methods.base_java import BaseJavaMethod, JAR_DIR


class TShapeIndex(BaseJavaMethod):
    """
    TShape (TMan): XZ-Ordering with fixed minimum space-unit subdivision.

    All comparison experiments execute on top of the TMan-spatial framework
    provided by TMan.jar.
    """

    jar_path   = os.path.join(JAR_DIR, "TMan.jar")
    main_class = ""          # use JAR manifest Main-Class

    def __init__(self, params: dict | None = None):
        super().__init__(name="TShape", params=params or {})
