"""Build Spark RDDs that join reviews with business metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Tuple

from pyspark import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame, SparkSession

from src.common.models import BusinessRecord, EnrichedReview, ReviewRecord
from src.ingest.ingestion_service import IngestionService


class RDDBuilder:
    """Materialises enriched review streams for downstream analytics."""

    def __init__(self, spark: SparkSession, ingestion: IngestionService) -> None:
        self.spark = spark
        self.ingestion = ingestion

    def build_enriched_reviews(self) -> RDD:
        """Join reviews to businesses + geo metadata."""

        business_df = self.ingestion.load_businesses().dropna(
            subset=("latitude", "longitude")
        )
        review_df = self.ingestion.load_reviews()

        business_pairs = business_df.rdd.map(RDDBuilder._business_pair).cache()
        review_pairs = review_df.rdd.map(RDDBuilder._review_pair)

        joined = review_pairs.join(business_pairs)
        enriched = joined.map(_to_enriched_from_join)
        return enriched

    def build_enriched_from_frames(self, business_df: DataFrame, review_df: DataFrame) -> RDD:
        """Join provided business/review DataFrames into an enriched RDD."""

        business_df = business_df.dropna(subset=("latitude", "longitude"))
        review_df = self.ingestion._clean_reviews(review_df, order=False)

        business_pairs = business_df.rdd.map(RDDBuilder._business_pair).cache()
        review_pairs = review_df.rdd.map(RDDBuilder._review_pair)

        joined = review_pairs.join(business_pairs)
        return joined.map(_to_enriched_from_join)

    @staticmethod
    def _business_pair(row: Row) -> Tuple[str, BusinessRecord]:
        categories = tuple(parse_categories(row.categories))
        return (
            row.business_id,
            BusinessRecord(
                business_id=row.business_id,
                name=row.name or "Unknown Business",
                city=row.city or "Unknown City",
                state=row.state or "NA",
                latitude=float(row.latitude),
                longitude=float(row.longitude),
                categories=categories,
                stars=float(row.stars or 0.0),
            ),
        )

    @staticmethod
    def _review_pair(row: Row) -> Tuple[str, ReviewRecord]:
        return (
            row.business_id,
            ReviewRecord(
                review_id=row.review_id,
                business_id=row.business_id,
                user_id=row.user_id or "unknown_user",
                stars=float(row.stars or 0.0),
                date=parse_date(row.date),
                text=row.text or "",
            ),
        )

    @staticmethod
    def _to_enriched(review: ReviewRecord, business: BusinessRecord) -> EnrichedReview:
        return EnrichedReview(
            review_id=review.review_id,
            business_id=review.business_id,
            user_id=review.user_id,
            review_stars=review.stars,
            review_date=review.date,
            review_text=review.text,
            business_name=business.name,
            city=business.city,
            state=business.state,
            latitude=business.latitude,
            longitude=business.longitude,
            business_stars=business.stars,
            categories=business.categories,
        )


def _to_enriched_from_join(item):
    review, business = item[1]
    return RDDBuilder._to_enriched(review, business)


def parse_categories(raw_value: str | None) -> Iterable[str]:
    if not raw_value:
        return tuple()
    return tuple(cat.strip() for cat in raw_value.split(",") if cat and cat.strip())


def parse_date(value: str | None) -> datetime:
    """Convert Yelp review dates (YYYY-MM-DD) into datetime objects."""

    if not value:
        return datetime.fromtimestamp(0)
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        # Some datasets append timestamps, rely on best-effort parsing.
        return datetime.strptime(value.split(" ")[0], "%Y-%m-%d")
