"""Spark aggregations that power the dashboard."""

from __future__ import annotations

import math
from typing import Dict, List

from pyspark import RDD
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import Window, functions as F
from pyspark.sql import types as T

from src.common.geo import GeoBucketer
from src.common.models import EnrichedReview


class Aggregator:
    """Calculates city, grid, and category KPIs from review streams."""

    def __init__(
        self,
        spark: SparkSession,
        geo_bucketer: GeoBucketer,
        dense_city_threshold: int = 0,
        kmeans_seed: int = 7,
        window_duration: str = "30 minutes",
        slide_duration: str = "10 minutes",
        emerging_score_threshold: float = 1.5,
        rating_spike_threshold: float = 0.5,
        min_events_for_hotspot: int = 5,
    ) -> None:
        self.spark = spark
        self.geo_bucketer = geo_bucketer
        self.dense_city_threshold = max(int(dense_city_threshold), 0)
        self.kmeans_seed = kmeans_seed
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.emerging_score_threshold = float(emerging_score_threshold)
        self.rating_spike_threshold = float(rating_spike_threshold)
        self.min_events_for_hotspot = max(int(min_events_for_hotspot), 1)
        self._neighborhood_assignments_cache: DataFrame | None = None

    def run(self, enriched_rdd: RDD) -> Dict[str, DataFrame]:
        enriched_rdd = enriched_rdd.cache()
        window_hotspots = self._window_hotspots(enriched_rdd)
        return {
            "city_metrics": self._city_metrics(enriched_rdd),
            "grid_hotspots": self._grid_hotspots(enriched_rdd),
            "neighborhood_hotspots": self._neighborhood_hotspots(enriched_rdd),
            "window_hotspots": window_hotspots,
            "emerging_areas": self._emerging_areas(window_hotspots),
            "category_mix": self._category_mix(enriched_rdd),
            "city_activity_windows": self._city_activity_windows(enriched_rdd),
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

    def _neighborhood_hotspots(self, enriched_rdd: RDD) -> DataFrame:
        schema = T.StructType(
            [
                T.StructField("city", T.StringType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("neighborhood_id", T.StringType(), True),
                T.StructField("latitude", T.DoubleType(), True),
                T.StructField("longitude", T.DoubleType(), True),
                T.StructField("review_count", T.LongType(), True),
                T.StructField("avg_stars", T.DoubleType(), True),
            ]
        )
        if self.dense_city_threshold <= 0:
            return self.spark.createDataFrame([], schema=schema)

        events_df = self._event_frame(enriched_rdd).cache()
        business_df = (
            events_df.select(
                "business_id", "city", "state", "latitude", "longitude"
            )
            .dropna(subset=("latitude", "longitude"))
            .dropDuplicates(["business_id"])
        )
        assignments = self._neighborhood_assignments(business_df)
        if assignments is None or assignments.rdd.isEmpty():
            return self.spark.createDataFrame([], schema=schema)

        joined = events_df.join(assignments, on=["business_id", "city", "state"], how="inner")
        city_events = joined.select(
            "city",
            "state",
            "neighborhood_id",
            F.col("center_lat").alias("latitude"),
            F.col("center_lon").alias("longitude"),
            "review_stars",
        )
        aggregated = (
            city_events.groupBy("city", "state", "neighborhood_id", "latitude", "longitude")
            .agg(F.count("*").alias("review_count"), F.avg("review_stars").alias("avg_stars"))
            .select("city", "state", "neighborhood_id", "latitude", "longitude", "review_count", "avg_stars")
        )
        return aggregated

    def _window_hotspots(self, enriched_rdd: RDD) -> DataFrame:
        schema = T.StructType(
            [
                T.StructField("area_type", T.StringType(), True),
                T.StructField("area_id", T.StringType(), True),
                T.StructField("city", T.StringType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("latitude", T.DoubleType(), True),
                T.StructField("longitude", T.DoubleType(), True),
                T.StructField("window_start", T.TimestampType(), True),
                T.StructField("window_end", T.TimestampType(), True),
                T.StructField("event_count", T.LongType(), True),
                T.StructField("avg_stars", T.DoubleType(), True),
                T.StructField("baseline_count", T.DoubleType(), True),
                T.StructField("baseline_stars", T.DoubleType(), True),
                T.StructField("hotspot_score", T.DoubleType(), True),
                T.StructField("rating_change", T.DoubleType(), True),
            ]
        )

        grid_events = self._grid_event_frame(enriched_rdd)
        frames: List[DataFrame] = []
        if not grid_events.rdd.isEmpty():
            frames.append(
                self._window_by_area(grid_events, area_id_col="grid_id", area_type="grid")
            )

        # Neighborhood windows (dense cities only)
        events_df = self._event_frame(enriched_rdd)
        business_df = (
            events_df.select(
                "business_id", "city", "state", "latitude", "longitude"
            )
            .dropna(subset=("latitude", "longitude"))
            .dropDuplicates(["business_id"])
        )
        assignments = self._neighborhood_assignments(business_df)
        if assignments is not None and not assignments.rdd.isEmpty():
            neighborhood_events = (
                events_df.join(assignments, on=["business_id", "city", "state"], how="inner")
                .select(
                    "business_id",
                    "city",
                    "state",
                    F.col("neighborhood_id").alias("area_id"),
                    F.col("center_lat").alias("latitude"),
                    F.col("center_lon").alias("longitude"),
                    "review_stars",
                    "event_time",
                )
            )
            frames.append(
                self._window_by_area(
                    neighborhood_events,
                    area_id_col="area_id",
                    area_type="neighborhood",
                )
            )

        if not frames:
            return self.spark.createDataFrame([], schema=schema)

        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame)
        return result.select(schema.fieldNames())

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

    def _category_mix(self, enriched_rdd: RDD) -> DataFrame:
        events_df = self._category_event_frame(enriched_rdd)
        if events_df.rdd.isEmpty():
            schema = "area_type string, area_id string, city string, state string, latitude double, longitude double, category string, current_count long, current_share double, baseline_share double, anomaly_score double, window_end timestamp"
            return self.spark.createDataFrame([], schema=schema)

        latest_ts = events_df.agg(F.max("event_time").alias("max_ts")).collect()[0]["max_ts"]
        window_len = _duration_minutes(self.window_duration)
        current_window_start = F.expr(f"timestampadd(MINUTE, -{window_len}, timestamp('{latest_ts}'))")

        current = (
            events_df.filter(F.col("event_time") >= current_window_start)
            .groupBy("area_type", "area_id", "city", "state", "latitude", "longitude", "category")
            .agg(F.count("*").alias("current_count"))
        )
        current_totals = current.groupBy("area_type", "area_id").agg(F.sum("current_count").alias("total_current"))
        current = current.join(current_totals, on=["area_type", "area_id"], how="left").withColumn(
            "current_share", F.col("current_count") / F.when(F.col("total_current") > 0, F.col("total_current")).otherwise(F.lit(1))
        )

        baseline = (
            events_df.groupBy("area_type", "area_id", "city", "state", "latitude", "longitude", "category")
            .agg(F.count("*").alias("baseline_count"))
        )
        baseline_totals = baseline.groupBy("area_type", "area_id").agg(F.sum("baseline_count").alias("total_baseline"))
        baseline = baseline.join(baseline_totals, on=["area_type", "area_id"], how="left").withColumn(
            "baseline_share",
            F.col("baseline_count") / F.when(F.col("total_baseline") > 0, F.col("total_baseline")).otherwise(F.lit(1)),
        )

        joined = (
            current.join(
                baseline.select(
                    "area_type",
                    "area_id",
                    "category",
                    "baseline_share",
                ),
                on=["area_type", "area_id", "category"],
                how="left",
            )
            .withColumn("baseline_share", F.coalesce(F.col("baseline_share"), F.lit(0.0)))
            .withColumn("anomaly_score", F.col("current_share") - F.col("baseline_share"))
            .withColumn("window_end", F.lit(latest_ts).cast("timestamp"))
            .select(
                "area_type",
                "area_id",
                "city",
                "state",
                "latitude",
                "longitude",
                "category",
                "current_count",
                "current_share",
                "baseline_share",
                "anomaly_score",
                "window_end",
            )
        )
        return joined

    def _city_activity_windows(self, enriched_rdd: RDD) -> DataFrame:
        events_df = self._event_frame(enriched_rdd)
        if events_df.rdd.isEmpty():
            schema = "city string, state string, window_start timestamp, window_end timestamp, event_count long, avg_stars double"
            return self.spark.createDataFrame([], schema=schema)

        windowed = (
            events_df.groupBy(
                "city",
                "state",
                F.window("event_time", self.window_duration, self.slide_duration),
            )
            .agg(F.count("*").alias("event_count"), F.avg("review_stars").alias("avg_stars"))
            .withColumn("window_start", F.col("window").start.cast("timestamp"))
            .withColumn("window_end", F.col("window").end.cast("timestamp"))
            .drop("window")
        )
        return windowed

    def _event_frame(self, enriched_rdd: RDD) -> DataFrame:
        rows = enriched_rdd.map(
            lambda event: Row(
                business_id=event.business_id,
                city=event.city,
                state=event.state,
                latitude=float(event.latitude),
                longitude=float(event.longitude),
                review_stars=float(event.review_stars),
                event_time=event.review_date,
            )
        )
        schema = "business_id string, city string, state string, latitude double, longitude double, review_stars double, event_time timestamp"
        return self.spark.createDataFrame(rows, schema=schema)

    def _grid_event_frame(self, enriched_rdd: RDD) -> DataFrame:
        bucketer = self.geo_bucketer
        rows = enriched_rdd.map(
            lambda event: Row(
                business_id=event.business_id,
                city=event.city,
                state=event.state,
                **_grid_fields(event, bucketer),
                review_stars=float(event.review_stars),
                event_time=event.review_date,
            )
        )
        schema = "business_id string, city string, state string, grid_id string, latitude double, longitude double, review_stars double, event_time timestamp"
        return self.spark.createDataFrame(rows, schema=schema)

    @staticmethod
    def _pick_cluster_count(business_count: int) -> int:
        if business_count <= 1:
            return 1
        return max(2, min(12, int(math.sqrt(business_count))))

    def _neighborhood_assignments(self, business_df: DataFrame) -> DataFrame | None:
        if self._neighborhood_assignments_cache is not None:
            return self._neighborhood_assignments_cache

        if self.dense_city_threshold <= 0 or business_df.rdd.isEmpty():
            return None

        city_counts = business_df.groupBy("city").count()
        dense_cities = [
            row["city"]
            for row in city_counts.collect()
            if row["count"] >= self.dense_city_threshold
        ]
        if not dense_cities:
            return None

        assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
        frames: List[DataFrame] = []
        for city in dense_cities:
            city_businesses = business_df.filter(F.col("city") == city)
            business_count = city_businesses.count()
            if business_count == 0:
                continue
            features_df = assembler.transform(city_businesses)
            cluster_count = self._pick_cluster_count(business_count)
            model = KMeans(
                k=cluster_count,
                seed=self.kmeans_seed,
                featuresCol="features",
                predictionCol="cluster_id",
            ).fit(features_df)
            assignments = model.transform(features_df).select(
                "business_id", "city", "state", "cluster_id"
            )
            centers = [
                (int(idx), float(center[0]), float(center[1]))
                for idx, center in enumerate(model.clusterCenters())
            ]
            centers_df = self.spark.createDataFrame(
                centers, schema="cluster_id int, center_lat double, center_lon double"
            )
            with_centers = assignments.join(centers_df, on="cluster_id", how="inner")
            labeled = with_centers.withColumn(
                "neighborhood_id",
                F.concat_ws("-", F.col("city"), F.col("cluster_id")),
            ).select(
                "business_id",
                "city",
                "state",
                "neighborhood_id",
                "center_lat",
                "center_lon",
            )
            frames.append(labeled)

        if not frames:
            return None

        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame)
        self._neighborhood_assignments_cache = result
        return result

    def _window_by_area(
        self, events: DataFrame, area_id_col: str, area_type: str
    ) -> DataFrame:
        windowed = (
            events.groupBy(
                "city",
                "state",
                area_id_col,
                "latitude",
                "longitude",
                F.window("event_time", self.window_duration, self.slide_duration),
            )
            .agg(
                F.count("*").alias("event_count"),
                F.avg("review_stars").alias("avg_stars"),
            )
            .withColumn("window_start", F.col("window").start.cast("timestamp"))
            .withColumn("window_end", F.col("window").end.cast("timestamp"))
            .drop("window")
        )

        baseline = windowed.groupBy(
            "city", "state", area_id_col, "latitude", "longitude"
        ).agg(
            F.avg("event_count").alias("baseline_count"),
            F.avg("avg_stars").alias("baseline_stars"),
        )

        joined = windowed.join(
            baseline,
            on=["city", "state", area_id_col, "latitude", "longitude"],
            how="left",
        )

        with_scores = (
            joined.withColumn(
                "baseline_count", F.coalesce(F.col("baseline_count"), F.lit(0.0))
            )
            .withColumn(
                "baseline_stars", F.coalesce(F.col("baseline_stars"), F.col("avg_stars"))
            )
            .withColumn(
                "hotspot_score",
                F.col("event_count")
                / F.when(F.col("baseline_count") > 0, F.col("baseline_count")).otherwise(F.lit(1.0)),
            )
            .withColumn("rating_change", F.col("avg_stars") - F.col("baseline_stars"))
        )

        filtered = with_scores.filter(F.col("event_count") >= self.min_events_for_hotspot)
        return filtered.withColumn("area_type", F.lit(area_type)).withColumnRenamed(
            area_id_col, "area_id"
        )

    def _emerging_areas(self, window_hotspots: DataFrame) -> DataFrame:
        """Surface areas that look promising: higher volume and ratings vs baseline."""

        schema = T.StructType(
            [
                T.StructField("area_type", T.StringType(), True),
                T.StructField("area_id", T.StringType(), True),
                T.StructField("city", T.StringType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("latitude", T.DoubleType(), True),
                T.StructField("longitude", T.DoubleType(), True),
                T.StructField("window_start", T.TimestampType(), True),
                T.StructField("window_end", T.TimestampType(), True),
                T.StructField("event_count", T.LongType(), True),
                T.StructField("avg_stars", T.DoubleType(), True),
                T.StructField("baseline_count", T.DoubleType(), True),
                T.StructField("baseline_stars", T.DoubleType(), True),
                T.StructField("hotspot_score", T.DoubleType(), True),
                T.StructField("rating_change", T.DoubleType(), True),
            ]
        )
        if window_hotspots.rdd.isEmpty():
            return self.spark.createDataFrame([], schema=schema)

        emerging = window_hotspots.filter(
            (F.col("hotspot_score") >= self.emerging_score_threshold)
            & (F.col("rating_change") >= self.rating_spike_threshold)
            & (F.col("event_count") >= self.min_events_for_hotspot)
        )

        # keep the latest window per area
        window_spec = Window.partitionBy("city", "state", "area_id", "area_type").orderBy(
            F.col("window_end").desc()
        )
        ranked = emerging.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1).drop("rank")
        return ranked.select(schema.fieldNames())

    def _category_event_frame(self, enriched_rdd: RDD) -> DataFrame:
        """Explode categories across grid + neighborhood areas."""

        bucketer = self.geo_bucketer
        category_rows = enriched_rdd.flatMap(
            lambda event: [
                Row(
                    business_id=event.business_id,
                    city=event.city,
                    state=event.state,
                    area_type="grid",
                    area_id=bucketer.bucket(event.latitude, event.longitude).grid_id,
                    latitude=bucketer.bucket(event.latitude, event.longitude).latitude,
                    longitude=bucketer.bucket(event.latitude, event.longitude).longitude,
                    category=cat,
                    event_time=event.review_date,
                )
                for cat in event.safe_categories
            ]
        )
        grid_df = self.spark.createDataFrame(
            category_rows,
            schema="business_id string, city string, state string, area_type string, area_id string, latitude double, longitude double, category string, event_time timestamp",
        )

        # neighborhoods
        events_df = self._event_frame(enriched_rdd)
        business_df = (
            events_df.select("business_id", "city", "state", "latitude", "longitude")
            .dropna(subset=("latitude", "longitude"))
            .dropDuplicates(["business_id"])
        )
        assignments = self._neighborhood_assignments(business_df)
        if assignments is None or assignments.rdd.isEmpty():
            return grid_df

        neighborhood_rows = (
            enriched_rdd.flatMap(
                lambda event: [
                    (
                        event.business_id,
                        event.city,
                        event.state,
                        cat,
                        event.review_date,
                    )
                    for cat in event.safe_categories
                ]
            )
        )
        neighborhood_df = self.spark.createDataFrame(
            neighborhood_rows,
            schema="business_id string, city string, state string, category string, event_time timestamp",
        )
        neighborhood_df = (
            neighborhood_df.join(assignments, on=["business_id", "city", "state"], how="inner")
            .selectExpr(
                "business_id",
                "city",
                "state",
                "'neighborhood' as area_type",
                "neighborhood_id as area_id",
                "center_lat as latitude",
                "center_lon as longitude",
                "category",
                "event_time",
            )
        )

        return grid_df.unionByName(neighborhood_df)


def _grid_fields(event: EnrichedReview, bucketer: GeoBucketer) -> dict:
    cell = bucketer.bucket(event.latitude, event.longitude)
    return {
        "grid_id": cell.grid_id,
        "latitude": float(cell.latitude),
        "longitude": float(cell.longitude),
    }


def _duration_minutes(duration: str) -> int:
    """Parse durations like '30 minutes' into total minutes."""

    try:
        parts = duration.split()
        value = int(parts[0])
        unit = parts[1].lower()
        if "min" in unit:
            return value
        if "hour" in unit:
            return value * 60
    except Exception:
        pass
    return 30


def _category_mapper(event: EnrichedReview):
    if not event.safe_categories:
        return []
    return [
        ((event.city, category), (event.review_stars, 1))
        for category in event.safe_categories
    ]
