"""Streamlit dashboard for the Geospatial Yelp MVP."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.common.config import load_config


@st.cache_data
def load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def main() -> None:
    config_path = Path(os.environ.get("YELP_CONFIG", "config/local.yaml"))
    config = load_config(config_path)
    base_path = Path(config.output.base_path)

    st.set_page_config(page_title="Yelp Activity Dashboard", layout="wide")
    st.title("Geospatial Yelp Activity (MVP)")
    st.caption("Simulated review replay with Spark RDD-based aggregations.")

    city_metrics = load_table(base_path / "city_metrics")
    grid_hotspots = load_table(base_path / "grid_hotspots")
    category_leaders = load_table(base_path / "category_leaders")
    recent_reviews = load_table(base_path / "latest_reviews")

    if city_metrics.empty or grid_hotspots.empty:
        st.warning("Run `python -m src.streaming.review_stream --config config/local.yaml` to generate aggregates.")
        return

    city_options = sorted(city_metrics["city"].unique())
    default_city = config.dashboard.default_city if config.dashboard.default_city in city_options else city_options[0]
    selected_city = st.sidebar.selectbox("City", options=city_options, index=city_options.index(default_city))
    min_reviews = st.sidebar.slider("Minimum hotspot reviews", 1, 200, value=25, step=5)

    st.subheader(f"Hotspots in {selected_city}")
    city_grids = grid_hotspots[grid_hotspots["city"] == selected_city]
    city_grids = city_grids[city_grids["review_count"] >= min_reviews]

    if city_grids.empty:
        st.info("No hotspots meet the filters yet.")
    else:
        midpoint = (city_grids["latitude"].mean(), city_grids["longitude"].mean())
        layer = pdk.Layer(
            "ColumnLayer",
            data=city_grids,
            get_position="[longitude, latitude]",
            get_elevation="review_count",
            elevation_scale=50,
            radius=150,
            get_fill_color="[255 - avg_stars * 20, avg_stars * 20, 100, 180]",
            auto_highlight=True,
        )
        deck = pdk.Deck(map_style="mapbox://styles/mapbox/dark-v11", initial_view_state=pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=10, pitch=40), layers=[layer])
        st.pydeck_chart(deck)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("City Leaderboard")
        leaderboard = city_metrics.sort_values(by="review_count", ascending=False).head(20)
        st.dataframe(leaderboard, use_container_width=True)

    with col2:
        st.subheader("Top Categories")
        filtered_categories = category_leaders[category_leaders["city"] == selected_city]
        filtered_categories = filtered_categories.sort_values(by="review_count", ascending=False).head(20)
        st.dataframe(filtered_categories, use_container_width=True)

    if not recent_reviews.empty:
        st.subheader("Recent Reviews")
        st.dataframe(recent_reviews.head(50), use_container_width=True)


if __name__ == "__main__":
    main()
