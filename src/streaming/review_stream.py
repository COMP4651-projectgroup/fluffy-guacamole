"""Entry point for the MVP Spark job."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.common.config import load_config
from src.common.geo import GeoBucketer
from src.ingest.ingestion_service import IngestionService
from src.ingest.sources import FileStreamReviewSource, KafkaReviewSource, StaticReviewSource
from src.streaming.aggregator import Aggregator
from src.streaming.persistence import Persistence
from src.streaming.rdd_builder import RDDBuilder


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Yelp reviews and compute aggregates.")
    parser.add_argument("--config", default="config/local.yaml", help="Path to YAML config.")
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder.appName("GeospatialYelpDashboard")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        ingestion = IngestionService(
            spark,
            business_path=config.dataset.business_path,
            review_path=config.dataset.review_path,
            limit=config.dataset.limit,
        )

        aggregator = Aggregator(
            spark,
            GeoBucketer(config.geo.cell_size_km),
            dense_city_threshold=config.geo.dense_city_threshold,
            window_duration=config.analytics.window_duration,
            slide_duration=config.analytics.slide_duration,
            emerging_score_threshold=config.analytics.emerging_score_threshold,
            rating_spike_threshold=config.analytics.rating_spike_threshold,
            min_events_for_hotspot=config.analytics.min_events_for_hotspot,
        )
        persistence = Persistence(config.output.base_path)

        if config.source.type == "static":
            source = StaticReviewSource(spark, ingestion)
            enriched_rdd = source.load()
            if enriched_rdd.isEmpty():
                raise RuntimeError("No enriched events available. Check dataset paths.")
            tables = aggregator.run(enriched_rdd)
            persistence.write(tables)
            print(f"Wrote aggregated tables to {config.output.base_path}")
        elif config.source.type == "file_stream":
            if not config.source.stream_path or not config.source.checkpoint_path:
                raise ValueError("stream_path and checkpoint_path must be set for file_stream mode.")
            source = FileStreamReviewSource(
                spark,
                ingestion,
                stream_path=config.source.stream_path,
                checkpoint_path=config.source.checkpoint_path,
                trigger_seconds=config.source.trigger_seconds,
            )
            query = source.start(aggregator, persistence)
            print("Started streaming file source. Waiting for termination...")
            query.awaitTermination()
        elif config.source.type == "kafka":
            if not config.source.kafka_bootstrap_servers or not config.source.kafka_topic or not config.source.checkpoint_path:
                raise ValueError("kafka_bootstrap_servers, kafka_topic, and checkpoint_path must be set for kafka mode.")
            source = KafkaReviewSource(
                spark,
                ingestion,
                bootstrap_servers=config.source.kafka_bootstrap_servers,
                topic=config.source.kafka_topic,
                checkpoint_path=config.source.checkpoint_path,
                starting_offsets=config.source.kafka_starting_offsets,
                trigger_seconds=config.source.trigger_seconds,
            )
            query = source.start(aggregator, persistence)
            print("Started Kafka source. Waiting for termination...")
            query.awaitTermination()
        else:
            raise ValueError(f"Unknown source.type: {config.source.type}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
