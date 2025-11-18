from datetime import datetime

import pandas as pd

from src.common.models import EnrichedReview
from src.streaming.aggregator import Aggregator
from src.common.geo import GeoBucketer


def _event(city: str, stars: float, categories: tuple[str, ...]) -> EnrichedReview:
    return EnrichedReview(
        review_id=f"{city}-{stars}",
        business_id=f"{city}-biz",
        user_id="test-user",
        review_stars=stars,
        review_date=datetime(2020, 1, 1),
        review_text="Great!",
        business_name="Sample",
        city=city,
        state="NV",
        latitude=36.1699,
        longitude=-115.1398,
        business_stars=4.0,
        categories=categories,
    )


def test_aggregator_builds_city_metrics(spark):
    events = [
        _event("Las Vegas", 4.0, ("Mexican", "Tacos")),
        _event("Las Vegas", 2.0, ("Mexican",)),
        _event("Phoenix", 5.0, ("Pizza",)),
    ]
    rdd = spark.sparkContext.parallelize(events)
    aggregator = Aggregator(spark, GeoBucketer(cell_size_km=2.0))

    tables = aggregator.run(rdd)

    city_metrics = tables["city_metrics"].toPandas()
    vegas = city_metrics[city_metrics["city"] == "Las Vegas"].iloc[0]
    assert vegas["review_count"] == 2
    assert abs(vegas["avg_stars"] - 3.0) < 1e-6

    categories = tables["category_leaders"].toPandas()
    assert not categories.empty
    assert "Mexican" in categories["category"].values

