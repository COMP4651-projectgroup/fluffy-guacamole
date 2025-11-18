"""Configuration helpers for the Yelp dashboard MVP."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class DatasetConfig:
    """Paths to Yelp business/review JSON dumps."""

    business_path: str
    review_path: str
    limit: Optional[int] = None


@dataclass(frozen=True)
class OutputConfig:
    """Where Spark jobs should persist derived tables."""

    base_path: str = "./data/output"


@dataclass(frozen=True)
class GeoConfig:
    """Geo bucketing resolution."""

    cell_size_km: float = 2.0


@dataclass(frozen=True)
class StreamConfig:
    """Replay cadence when simulating events."""

    batch_size: int = 750
    delay_seconds: float = 0.1


@dataclass(frozen=True)
class DashboardConfig:
    """Dashboard defaults."""

    default_city: str = "Las Vegas"


@dataclass(frozen=True)
class AppConfig:
    """Aggregated configuration model."""

    dataset: DatasetConfig
    output: OutputConfig
    geo: GeoConfig
    stream: StreamConfig
    dashboard: DashboardConfig


def load_config(path: str | Path) -> AppConfig:
    """Parse a YAML config file into an AppConfig dataclass."""

    raw = _load_yaml(path)
    dataset_cfg = raw.get("dataset", {})
    output_cfg = raw.get("output", {})
    geo_cfg = raw.get("geo", {})
    stream_cfg = raw.get("stream", {})
    dashboard_cfg = raw.get("dashboard", {})

    dataset = DatasetConfig(
        business_path=str(dataset_cfg.get("business_path", "./yelp_dataset/business.json")),
        review_path=str(dataset_cfg.get("review_path", "./yelp_dataset/review.json")),
        limit=dataset_cfg.get("limit"),
    )
    output = OutputConfig(base_path=str(output_cfg.get("base_path", "./data/output")))
    geo = GeoConfig(cell_size_km=float(geo_cfg.get("cell_size_km", 2.0)))
    stream = StreamConfig(
        batch_size=int(stream_cfg.get("batch_size", 750)),
        delay_seconds=float(stream_cfg.get("delay_seconds", 0.1)),
    )
    dashboard = DashboardConfig(default_city=str(dashboard_cfg.get("default_city", "Las Vegas")))
    return AppConfig(dataset=dataset, output=output, geo=geo, stream=stream, dashboard=dashboard)


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a top-level mapping.")
    return data

