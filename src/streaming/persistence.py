"""Persist derived Spark tables for the dashboard."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame


class Persistence:
    """Write Spark DataFrames into a folder hierarchy."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)

    def write(self, tables: Dict[str, DataFrame]) -> None:
        self.base_path.mkdir(parents=True, exist_ok=True)
        for name, frame in tables.items():
            target = self.base_path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.write.mode("overwrite").parquet(str(target))

