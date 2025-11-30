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
class SourceConfig:
    """Select ingestion mode (static vs streaming)."""

    type: str = "static"  # static | file_stream | kafka
    stream_path: Optional[str] = None
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_starting_offsets: str = "latest"
    checkpoint_path: Optional[str] = None
    trigger_seconds: int = 5


@dataclass(frozen=True)
class OutputConfig:
    """Where Spark jobs should persist derived tables."""

    base_path: str = "./data/output"


@dataclass(frozen=True)
class GeoConfig:
    """Geo bucketing resolution."""

    cell_size_km: float = 2.0
    dense_city_threshold: int = 0


@dataclass(frozen=True)
class AnalyticsConfig:
    """Windowed hotspot + anomaly detection parameters."""

    window_duration: str = "30 minutes"
    slide_duration: str = "10 minutes"
    emerging_score_threshold: float = 1.5
    rating_spike_threshold: float = 0.5
    min_events_for_hotspot: int = 5


@dataclass(frozen=True)
class StreamConfig:
    """Replay cadence when simulating events."""

    batch_size: int = 750
    delay_seconds: float = 0.1


@dataclass(frozen=True)
class DashboardConfig:
    """Dashboard defaults."""

    default_city: str = "Philadelphia"


@dataclass(frozen=True)
class AppConfig:
    """Aggregated configuration model."""

    dataset: DatasetConfig
    source: SourceConfig
    output: OutputConfig
    geo: GeoConfig
    stream: StreamConfig
    dashboard: DashboardConfig
    analytics: AnalyticsConfig


def load_config(path: str | Path) -> AppConfig:
    """Parse a YAML config file into an AppConfig dataclass."""

    raw = _load_yaml(path)
    dataset_cfg = raw.get("dataset", {})
    source_cfg = raw.get("source", {})
    output_cfg = raw.get("output", {})
    geo_cfg = raw.get("geo", {})
    stream_cfg = raw.get("stream", {})
    dashboard_cfg = raw.get("dashboard", {})
    analytics_cfg = raw.get("analytics", {})

    dataset = DatasetConfig(
        business_path=str(dataset_cfg.get("business_path", "./yelp_dataset/business.json")),
        review_path=str(dataset_cfg.get("review_path", "./yelp_dataset/review.json")),
        limit=dataset_cfg.get("limit"),
    )
    source = SourceConfig(
        type=str(source_cfg.get("type", "static")),
        stream_path=source_cfg.get("stream_path"),
        kafka_bootstrap_servers=source_cfg.get("kafka_bootstrap_servers"),
        kafka_topic=source_cfg.get("kafka_topic"),
        kafka_starting_offsets=str(source_cfg.get("kafka_starting_offsets", "latest")),
        checkpoint_path=source_cfg.get("checkpoint_path"),
        trigger_seconds=int(source_cfg.get("trigger_seconds", 5)),
    )
    output = OutputConfig(base_path=str(output_cfg.get("base_path", "./data/output")))
    geo = GeoConfig(
        cell_size_km=float(geo_cfg.get("cell_size_km", 2.0)),
        dense_city_threshold=int(geo_cfg.get("dense_city_threshold", 0)),
    )
    stream = StreamConfig(
        batch_size=int(stream_cfg.get("batch_size", 750)),
        delay_seconds=float(stream_cfg.get("delay_seconds", 0.1)),
    )
    dashboard = DashboardConfig(default_city=str(dashboard_cfg.get("default_city", "Las Vegas")))
    analytics = AnalyticsConfig(
        window_duration=str(analytics_cfg.get("window_duration", "30 minutes")),
        slide_duration=str(analytics_cfg.get("slide_duration", "10 minutes")),
        emerging_score_threshold=float(analytics_cfg.get("emerging_score_threshold", 1.5)),
        rating_spike_threshold=float(analytics_cfg.get("rating_spike_threshold", 0.5)),
        min_events_for_hotspot=int(analytics_cfg.get("min_events_for_hotspot", 5)),
    )
    return AppConfig(
        dataset=dataset,
        source=source,
        output=output,
        geo=geo,
        stream=stream,
        dashboard=dashboard,
        analytics=analytics,
    )


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a top-level mapping.")
    return data
