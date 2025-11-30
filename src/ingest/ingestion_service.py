"""Load Yelp datasets into Spark DataFrames."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

REVIEW_SCHEMA = T.StructType(
    [
        T.StructField("review_id", T.StringType(), True),
        T.StructField("business_id", T.StringType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("stars", T.DoubleType(), True),
        T.StructField("date", T.StringType(), True),
        T.StructField("text", T.StringType(), True),
    ]
)


class IngestionService:
    """CRUD wrapper for Yelp JSON dumps."""

    def __init__(
        self,
        spark: SparkSession,
        business_path: str,
        review_path: str,
        limit: Optional[int] = None,
    ) -> None:
        self.spark = spark
        self.business_path = business_path
        self.review_path = review_path
        self.limit = limit

    def load_businesses(self) -> DataFrame:
        """Return a cleaned DataFrame with coordinates + metadata."""

        df = self.spark.read.json(self.business_path)
        cleaned = (
            df.select(
                "business_id",
                "name",
                "city",
                "state",
                "latitude",
                "longitude",
                "categories",
                "stars",
            )
            .dropna(subset=("business_id", "city", "state"))
            .withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
            .withColumn("stars", F.col("stars").cast("double"))
        )
        return cleaned

    def load_reviews(self) -> DataFrame:
        """Return reviews sorted by event date for deterministic replay."""

        df = self.spark.read.schema(REVIEW_SCHEMA).json(self.review_path)
        cleaned = self._clean_reviews(df, order=True)
        if self.limit:
            cleaned = cleaned.limit(self.limit)
        return cleaned

    def _clean_reviews(self, df: DataFrame, order: bool = False) -> DataFrame:
        cleaned = df.select(
            "review_id",
            "business_id",
            "user_id",
            "stars",
            "date",
            "text",
        ).dropna(subset=("review_id", "business_id", "date"))

        cleaned = cleaned.withColumn("stars", F.col("stars").cast("double")).withColumn(
            "date_ts", F.to_timestamp("date")
        )
        if order:
            cleaned = cleaned.orderBy("date_ts")
        return cleaned

    @staticmethod
    def review_schema() -> T.StructType:
        return REVIEW_SCHEMA
