"""
BMTree (2025) baseline adapter — invokes experiments/jars/BMTree.jar.

Source reference : https://github.com/gravesprite/Learned-BMTree
JAR location     : experiments/jars/BMTree.jar

Build command:
  java -jar experiments/jars/BMTree.jar build \
       --data    <segments.tsv>               \
       --index   experiments/jars/BMTree_index \
       --params  '{}'

Query command:
  java -jar experiments/jars/BMTree.jar query \
       --index       experiments/jars/BMTree_index \
       --window      "lon_min,lat_min,lon_max,lat_max" \
       --query_type  SRQ

Expected JSON stdout (query):
  {"visited_cells": ..., "valid_cells": ...,
   "scan_range_intervals": ..., "result_count": ...}
"""

from __future__ import annotations
import os
from methods.base_java import BaseJavaMethod, JAR_DIR


class BMTreeIndex(BaseJavaMethod):
    """
    BMTree: BMTree structure with LSFC parameter optimization. 2025.
    Runs on TMan-spatial infrastructure via BMTree.jar.
    """

    jar_path   = os.path.join(JAR_DIR, "BMTree.jar")
    main_class = ""

    def __init__(self, params: dict | None = None):
        super().__init__(name="BMTree", params=params or {})
