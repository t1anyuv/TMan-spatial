"""
LMSFC (VLDB 2023) baseline adapter — invokes experiments/jars/LMSFC.jar.

Source reference : https://github.com/Cyan9061/LMSFC-Algorithm-Replication
JAR location     : experiments/jars/LMSFC.jar

Build command:
  java -jar experiments/jars/LMSFC.jar build \
       --data    <segments.tsv>              \
       --index   experiments/jars/LMSFC_index \
       --params  '{}'

Query command:
  java -jar experiments/jars/LMSFC.jar query \
       --index       experiments/jars/LMSFC_index \
       --window      "lon_min,lat_min,lon_max,lat_max" \
       --query_type  SRQ

Expected JSON stdout (query):
  {"visited_cells": ..., "valid_cells": ...,
   "scan_range_intervals": ..., "result_count": ...}
"""

from __future__ import annotations
import os
from methods.base_java import BaseJavaMethod, JAR_DIR


class LMSFCIndex(BaseJavaMethod):
    """
    LMSFC: SMBO-optimized Learned Space-Filling Curve. VLDB 2023.
    Runs on TMan-spatial infrastructure via LMSFC.jar.
    """

    jar_path   = os.path.join(JAR_DIR, "LMSFC.jar")
    main_class = ""

    def __init__(self, params: dict | None = None):
        super().__init__(name="LMSFC", params=params or {})
