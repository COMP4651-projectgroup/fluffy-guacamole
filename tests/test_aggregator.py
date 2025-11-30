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


def test_neighborhood_hotspots_only_include_dense_cities(spark):
    def dense_event(city: str, idx: int) -> EnrichedReview:
        return EnrichedReview(
            review_id=f"{city}-{idx}",
            business_id=f"{city}-biz-{idx}",
            user_id="test-user",
            review_stars=4.0,
            review_date=datetime(2020, 1, 1),
            review_text="Great!",
            business_name="Sample",
            city=city,
            state="NV",
            latitude=36.0 + (idx * 0.01),
            longitude=-115.0 + (idx * 0.01),
            business_stars=4.0,
            categories=("Diner",),
        )

    dense_events = [dense_event("Dense City", i) for i in range(6)]
    sparse_events = [dense_event("Sparse City", i) for i in range(2)]
    rdd = spark.sparkContext.parallelize(dense_events + sparse_events)

    aggregator = Aggregator(
        spark,
        GeoBucketer(cell_size_km=2.0),
        dense_city_threshold=5,
    )

    neighborhoods = aggregator.run(rdd)["neighborhood_hotspots"].toPandas()

    assert not neighborhoods.empty
    assert set(neighborhoods["city"]) == {"Dense City"}
    assert neighborhoods["review_count"].sum() == len(dense_events)


def test_window_hotspots_surface_emerging_and_drops(spark):
    def timed_event(minute: int, stars: float) -> EnrichedReview:
        return EnrichedReview(
            review_id=f"ev-{minute}",
            business_id=f"biz-{minute}",
            user_id="u",
            review_stars=stars,
            review_date=datetime(2020, 1, 1, 0, minute),
            review_text="",
            business_name="Sample",
            city="Vegas",
            state="NV",
            latitude=36.1699,
            longitude=-115.1398,
            business_stars=4.0,
            categories=("Diner",),
        )

    events = [
        timed_event(0, 3.0),   # window1
        timed_event(20, 5.0),  # window2
        timed_event(25, 5.0),  # window2
        timed_event(40, 1.0),  # window3
    ]
    rdd = spark.sparkContext.parallelize(events)

    aggregator = Aggregator(
        spark,
        GeoBucketer(cell_size_km=2.0),
        window_duration="20 minutes",
        slide_duration="20 minutes",
        emerging_score_threshold=1.4,
        rating_spike_threshold=0.5,
        min_events_for_hotspot=1,
    )

    results = aggregator.run(rdd)
    windows = results["window_hotspots"].toPandas()
    assert not windows.empty
    assert set(windows["area_type"]) == {"grid"}

    emerging = results["emerging_areas"].toPandas()
    assert not emerging.empty
    drops = windows[windows["rating_change"] < 0]

    assert len(emerging) == 1
    assert emerging.iloc[0]["event_count"] == 2
    assert len(drops) == 1
    assert drops.iloc[0]["event_count"] == 1


def test_category_mix_tracks_current_and_baseline(spark):
    events = [
        EnrichedReview(
            review_id="1",
            business_id="b1",
            user_id="u1",
            review_stars=5.0,
            review_date=datetime(2020, 1, 1, 0, 10),
            review_text="",
            business_name="A",
            city="Vegas",
            state="NV",
            latitude=36.17,
            longitude=-115.14,
            business_stars=4.0,
            categories=("Pizza", "Italian"),
        ),
        EnrichedReview(
            review_id="2",
            business_id="b2",
            user_id="u2",
            review_stars=4.0,
            review_date=datetime(2020, 1, 1, 0, 15),
            review_text="",
            business_name="B",
            city="Vegas",
            state="NV",
            latitude=36.18,
            longitude=-115.15,
            business_stars=4.0,
            categories=("Mexican",),
        ),
    ]
    rdd = spark.sparkContext.parallelize(events)
    aggregator = Aggregator(
        spark,
        GeoBucketer(cell_size_km=2.0),
        window_duration="30 minutes",
        slide_duration="10 minutes",
    )

    category_mix = aggregator.run(rdd)["category_mix"].toPandas()
    assert not category_mix.empty
    assert set(category_mix["category"]) >= {"Pizza", "Italian", "Mexican"}
    assert (category_mix["current_count"] >= 1).any()
