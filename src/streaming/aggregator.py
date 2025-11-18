"""Spark aggregations that power the dashboard."""

from __future__ import annotations

from typing import Dict

from pyspark import RDD
from pyspark.sql import DataFrame, Row, SparkSession

from src.common.geo import GeoBucketer
from src.common.models import EnrichedReview


class Aggregator:
    """Calculates city, grid, and category KPIs from review streams."""

    def __init__(self, spark: SparkSession, geo_bucketer: GeoBucketer) -> None:
        self.spark = spark
        self.geo_bucketer = geo_bucketer

    def run(self, enriched_rdd: RDD) -> Dict[str, DataFrame]:
        enriched_rdd = enriched_rdd.cache()
        return {
            "city_metrics": self._city_metrics(enriched_rdd),
            "grid_hotspots": self._grid_hotspots(enriched_rdd),
            "category_leaders": self._category_leaders(enriched_rdd),
            "latest_reviews": self._latest_reviews(enriched_rdd),
        }

    def _city_metrics(self, enriched_rdd: RDD) -> DataFrame:
        keyed = enriched_rdd.map(
            lambda event: ((event.city, event.state), (event.review_stars, 1))
        )
        reduced = keyed.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        rows = reduced.map(
            lambda item: Row(
                city=item[0][0],
                state=item[0][1],
                review_count=int(item[1][1]),
                avg_stars=float(item[1][0] / item[1][1]),
            )
        )
        schema = "city string, state string, review_count long, avg_stars double"
        return self.spark.createDataFrame(rows, schema=schema)

    def _grid_hotspots(self, enriched_rdd: RDD) -> DataFrame:
        bucketer = self.geo_bucketer
        grid_pairs = enriched_rdd.map(
            lambda event: (
                (event.city, event.state, bucketer.bucket(event.latitude, event.longitude)),
                (event.review_stars, 1),
            )
        )
        reduced = grid_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        rows = reduced.map(
            lambda item: Row(
                city=item[0][0],
                state=item[0][1],
                grid_id=item[0][2].grid_id,
                latitude=float(item[0][2].latitude),
                longitude=float(item[0][2].longitude),
                review_count=int(item[1][1]),
                avg_stars=float(item[1][0] / item[1][1]),
            )
        )
        schema = "city string, state string, grid_id string, latitude double, longitude double, review_count long, avg_stars double"
        return self.spark.createDataFrame(rows, schema=schema)

    def _category_leaders(self, enriched_rdd: RDD) -> DataFrame:
        category_pairs = enriched_rdd.flatMap(_category_mapper)
        reduced = category_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        rows = reduced.map(
            lambda item: Row(
                city=item[0][0],
                category=item[0][1],
                review_count=int(item[1][1]),
                avg_stars=float(item[1][0] / max(item[1][1], 1)),
            )
        )
        schema = "city string, category string, review_count long, avg_stars double"
        return self.spark.createDataFrame(rows, schema=schema)

    def _latest_reviews(self, enriched_rdd: RDD, limit: int = 200) -> DataFrame:
        latest = enriched_rdd.takeOrdered(limit, key=lambda e: -e.review_date.timestamp())
        rows = [
            Row(
                review_id=event.review_id,
                business_id=event.business_id,
                business_name=event.business_name,
                city=event.city,
                state=event.state,
                stars=event.review_stars,
                review_date=event.review_date.isoformat(),
                summary=(event.review_text or "")[:280],
            )
            for event in latest
        ]
        schema = "review_id string, business_id string, business_name string, city string, state string, stars double, review_date string, summary string"
        return self.spark.createDataFrame(rows, schema=schema)


def _category_mapper(event: EnrichedReview):
    if not event.safe_categories:
        return []
    return [
        ((event.city, category), (event.review_stars, 1))
        for category in event.safe_categories
    ]
