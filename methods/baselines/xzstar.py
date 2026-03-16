"""
XZ* (Trass) baseline adapter.

XZ* is part of the TMan-spatial framework and is invoked through TMan.jar
using a dedicated sub-command or class parameter.

JAR location : experiments/jars/TMan.jar
Sub-command  : query_xzstar  (or pass --method xzstar to the standard commands)

Build command:
  java -jar experiments/jars/TMan.jar build \
       --method  xzstar                     \
       --data    <segments.tsv>             \
       --index   experiments/jars/XZStar_index \
       --params  '{"grid": [2, 2]}'

Query command:
  java -jar experiments/jars/TMan.jar query \
       --method      xzstar                 \
       --index       experiments/jars/XZStar_index \
       --window      "lon_min,lat_min,lon_max,lat_max" \
       --query_type  SRQ
"""

from __future__ import annotations
import os
from methods.base_java import BaseJavaMethod, JAR_DIR, _jar_exists, _run_java
from methods.base import Segment, QueryWindow
from typing import List
import json
import time
import tempfile


class XZStarIndex(BaseJavaMethod):
    """
    XZ* (Trass): XZ-Ordering with 2×2 space-unit subdivision.
    Part of TMan-spatial; invoked via TMan.jar with --method xzstar.
    """

    jar_path   = os.path.join(JAR_DIR, "TMan.jar")
    main_class = ""

    def __init__(self, params: dict | None = None):
        params = params or {"grid": [2, 2]}
        super().__init__(name="XZ*", params=params)

    # Override to inject --method xzstar into all commands
    def _java_args(self, sub_cmd: str, extra: List[str]) -> List[str]:
        base = super()._java_args(sub_cmd, [])
        return base + ["--method", "xzstar"] + extra
