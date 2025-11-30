"""Streamlit dashboard for the Geospatial Yelp MVP."""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.common.config import load_config

CACHE_TTL = int(os.environ.get("YELP_UI_REFRESH_SECONDS", "60"))


@st.cache_data(ttl=CACHE_TTL)
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
    st.caption(f"Cache TTL: {CACHE_TTL}s (set YELP_UI_REFRESH_SECONDS to adjust).")
    if st.sidebar.button("Refresh data now"):
        keep_city = st.session_state.get("selected_city")
        st.cache_data.clear()
        if keep_city:
            st.session_state["selected_city"] = keep_city
        rerun_fn = getattr(st, "experimental_rerun", None) or getattr(st, "rerun", None)
        if rerun_fn:
            rerun_fn()

    city_metrics = load_table(base_path / "city_metrics")
    grid_hotspots = load_table(base_path / "grid_hotspots")
    neighborhood_hotspots = load_table(base_path / "neighborhood_hotspots")
    window_hotspots = load_table(base_path / "window_hotspots")
    emerging = load_table(base_path / "emerging_areas")
    category_mix = load_table(base_path / "category_mix")
    city_activity = load_table(base_path / "city_activity_windows")
    category_leaders = load_table(base_path / "category_leaders")
    recent_reviews = load_table(base_path / "latest_reviews")

    if city_metrics.empty or grid_hotspots.empty:
        st.warning("Run `python -m src.streaming.review_stream --config config/local.yaml` to generate aggregates.")
        return

    city_options = sorted(city_metrics["city"].unique())
    default_city = config.dashboard.default_city if config.dashboard.default_city in city_options else city_options[0]
    if "selected_city" not in st.session_state or st.session_state["selected_city"] not in city_options:
        st.session_state["selected_city"] = default_city
    selected_city = st.sidebar.selectbox("City", options=city_options, key="selected_city")
    min_reviews = st.sidebar.slider("Minimum hotspot reviews", 1, 200, value=5, step=1)
    hotspot_threshold = st.sidebar.slider("Hotspot score threshold", 0.5, 3.0, value=1.2, step=0.1)
    st.sidebar.info("hotspot_score = window event_count ÷ historical average event_count for the same area (baseline).")
    dense_threshold = config.geo.dense_city_threshold

    dense_cities = sorted(neighborhood_hotspots["city"].unique()) if not neighborhood_hotspots.empty else []
    has_neighborhoods = selected_city in dense_cities
    hotspot_options = ["Grid cells"]
    if has_neighborhoods:
        hotspot_options.append("Neighborhoods (K-Means)")
    hotspot_view = st.sidebar.radio("Hotspot geometry", options=hotspot_options, index=1 if "Neighborhoods (K-Means)" in hotspot_options else 0)
    st.sidebar.caption(
        f"K-Means neighborhoods appear when a city has >= {dense_threshold} businesses. "
        f"Available: {', '.join(dense_cities) if dense_cities else 'none yet'}."
    )

    city_mix = category_mix[category_mix["city"] == selected_city] if not category_mix.empty else pd.DataFrame()
    category_filter = "All"
    if not city_mix.empty:
        category_options = ["All"] + sorted(city_mix["category"].unique())
        category_filter = st.sidebar.selectbox("Category filter", options=category_options, index=0)

    st.subheader(f"Hotspots in {selected_city}")
    city_grids = grid_hotspots[grid_hotspots["city"] == selected_city]
    city_neighborhoods = neighborhood_hotspots[neighborhood_hotspots["city"] == selected_city]
    city_neighborhoods = city_neighborhoods[city_neighborhoods["review_count"] >= min_reviews]
    city_grids = city_grids[city_grids["review_count"] >= min_reviews]

    is_neighborhood_view = hotspot_view.startswith("Neighborhoods") and has_neighborhoods
    hotspot_df = city_neighborhoods if is_neighborhood_view else city_grids
    label_field = "neighborhood_id" if is_neighborhood_view else "grid_id"

    if hotspot_df.empty:
        st.info("No hotspots meet the filters yet.")
    else:
        midpoint = (hotspot_df["latitude"].mean(), hotspot_df["longitude"].mean())
        tooltip = {"text": f"{label_field}: {{{label_field}}}\nReviews: {{review_count}}\nAvg Stars: {{avg_stars}}"}
        layer = pdk.Layer(
            "ColumnLayer",
            data=hotspot_df,
            get_position="[longitude, latitude]",
            get_elevation="review_count",
            elevation_scale=50,
            radius=150,
            get_fill_color="[255 - avg_stars * 20, avg_stars * 20, 100, 180]",
            auto_highlight=True,
            pickable=True,
        )
        deck = pdk.Deck(
            map_style="mapbox://styles/mapbox/dark-v11",
            initial_view_state=pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=10, pitch=40),
            layers=[layer],
            tooltip=tooltip,
        )
        st.pydeck_chart(deck)
    
    st.subheader("Emerging areas")
    city_emerging = emerging[emerging["city"] == selected_city] if not emerging.empty else pd.DataFrame()
    if city_emerging.empty:
        st.info("Emerging areas will appear after the next replay.")
    else:
        st.dataframe(
            city_emerging.sort_values("hotspot_score", ascending=False)[
                [
                    "area_type",
                    "area_id",
                    "event_count",
                    "hotspot_score",
                    "avg_stars",
                    "rating_change",
                    "window_start",
                ]
            ],
            use_container_width=True,
        )

    st.subheader("Rolling Activity (windowed)")
    city_windows = window_hotspots[window_hotspots["city"] == selected_city] if not window_hotspots.empty else pd.DataFrame()
    if city_windows.empty:
        st.info("Windowed hotspot metrics will appear after the next replay.")
    else:
        city_windows = city_windows.copy()
        city_windows["window_start"] = pd.to_datetime(city_windows["window_start"])
        city_windows = city_windows[city_windows["event_count"] >= min_reviews]
        city_windows = city_windows[city_windows["hotspot_score"] >= hotspot_threshold]

        # Time range filter (guard against NaT)
        valid_times = city_windows.dropna(subset=["window_start"])
        if valid_times.empty:
            st.info("No valid window timestamps to chart.")
            return
        min_date = valid_times["window_start"].min().date()
        max_date = valid_times["window_start"].max().date()
        selected_dates = st.sidebar.date_input(
            "Time range (windowed)",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
            city_windows = city_windows[
                (city_windows["window_start"].dt.date >= start_date)
                & (city_windows["window_start"].dt.date <= end_date)
            ]

        area_types = sorted(city_windows["area_type"].unique())
        selected_area_type = st.radio("Area type", options=area_types, horizontal=True)
        area_ids = sorted(city_windows[city_windows["area_type"] == selected_area_type]["area_id"].unique())
        if category_filter != "All" and not city_mix.empty:
            matching_areas = set(
                city_mix[city_mix["category"] == category_filter]["area_id"].unique()
            )
            area_ids = [a for a in area_ids if a in matching_areas]
        if not area_ids:
            st.info("No areas match the filters.")
            return
        selected_area_id = st.selectbox("Area", options=area_ids)

        area_windows = city_windows[
            (city_windows["area_type"] == selected_area_type)
            & (city_windows["area_id"] == selected_area_id)
        ]

        if area_windows.empty:
            st.info("No windowed hotspots meet the filters.")
        else:
            area_windows = area_windows.sort_values("window_start")
            st.line_chart(area_windows.set_index("window_start")[["event_count", "avg_stars"]])
            st.dataframe(
                area_windows.sort_values("window_start", ascending=False)[
                    [
                        "area_type",
                        "area_id",
                        "window_start",
                        "event_count",
                        "hotspot_score",
                        "avg_stars",
                        "rating_change",
                    ]
                ],
                use_container_width=True,
            )

    st.subheader("Top categories right now")
    if city_mix.empty:
        st.info("Category mix will appear after the next replay.")
    else:
        available_area_types = sorted(city_mix["area_type"].unique())
        mix_area_type = st.radio("Category area type", options=available_area_types, horizontal=True)
        mix_area_ids = sorted(city_mix[city_mix["area_type"] == mix_area_type]["area_id"].unique())
        if category_filter != "All":
            mix_area_ids = [
                a
                for a in mix_area_ids
                if not city_mix[
                    (city_mix["area_type"] == mix_area_type)
                    & (city_mix["area_id"] == a)
                    & (city_mix["category"] == category_filter)
                ].empty
            ]
        if not mix_area_ids:
            st.info("No areas match the category filter.")
        else:
            mix_area_id = st.selectbox("Area (for categories)", options=mix_area_ids)
            area_mix = city_mix[
                (city_mix["area_type"] == mix_area_type)
                & (city_mix["area_id"] == mix_area_id)
            ].copy()
            if category_filter != "All":
                area_mix = area_mix[area_mix["category"] == category_filter]
            area_mix = area_mix.sort_values("current_share", ascending=False)
            st.bar_chart(area_mix.head(10).set_index("category")["current_share"])
            st.dataframe(
                area_mix[
                    [
                        "category",
                        "current_count",
                        "current_share",
                        "baseline_share",
                        "anomaly_score",
                    ]
                ],
                use_container_width=True,
            )

    st.subheader("City activity (sliding windows)")
    city_windows_overall = city_activity[city_activity["city"] == selected_city] if not city_activity.empty else pd.DataFrame()
    if city_windows_overall.empty:
        st.info("City activity windows will appear after the next replay.")
    else:
        city_windows_overall = city_windows_overall.copy()
        city_windows_overall["window_start"] = pd.to_datetime(city_windows_overall["window_start"])
        city_windows_overall = city_windows_overall.sort_values("window_start")
        max_points = 300
        if len(city_windows_overall) > max_points:
            step = max(1, math.ceil(len(city_windows_overall) / max_points))
            city_windows_overall = city_windows_overall.iloc[::step]
        st.line_chart(city_windows_overall.set_index("window_start")[["event_count", "avg_stars"]])

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
