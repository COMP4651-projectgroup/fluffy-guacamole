"""Entry point for the MVP Spark job."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.common.config import load_config
from src.common.geo import GeoBucketer
from src.ingest.ingestion_service import IngestionService
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
        builder = RDDBuilder(spark, ingestion)
        enriched_rdd = builder.build_enriched_reviews()
        if enriched_rdd.isEmpty():
            raise RuntimeError("No enriched events available. Check dataset paths.")

        aggregator = Aggregator(spark, GeoBucketer(config.geo.cell_size_km))
        tables = aggregator.run(enriched_rdd)
        Persistence(config.output.base_path).write(tables)
        print(f"Wrote aggregated tables to {config.output.base_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

