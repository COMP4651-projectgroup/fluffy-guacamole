# fluffy-guacamole

Geospatial Real-Time Yelp Activity Dashboard

This MVP replays Yelp reviews with Spark, joins them to business coordinates, and publishes geospatial aggregates for a Streamlit dashboard.

## Dataset setup

1. Grab the Yelp Open Dataset tarball from https://business.yelp.com/data/resources/open-dataset/.
2. Extract it locally; the archive expands into a `yelp_dataset` directory containing the JSON files (businesses, reviews, etc.).
3. Move the `yelp_dataset` directory into this repository root so paths like `./yelp_dataset/business.json` exist for the ingestion scripts. If your files still use the original `yelp_academic_dataset_*.json` names, rename them to `business.json`, `review.json`, etc., or update `config/local.yaml` to point at the actual filenames.

## Project layout

```
src/
 ├── common/          # config, geo helpers, and shared dataclasses
 ├── ingest/          # Spark dataframe loaders
 ├── streaming/       # RDD builder, aggregations, and replay driver
 └── dashboard/       # Streamlit UI consuming the parquet outputs
config/
 └── local.yaml       # configurable dataset + geo parameters
data/output/          # parquet tables written by the streamer
tests/                # pytest suite (spark fixtures + unit tests)
```

## Prepare the app (first run)

1. Create a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Ensure the Yelp JSON filenames match the defaults (see Dataset setup above) **or** edit `config/local.yaml` so `dataset.business_path` / `dataset.review_path` point to your actual files.
4. Run the Spark replay job to materialize parquet outputs:
   ```
   python -m src.streaming.review_stream --config config/local.yaml
   ```

The replay job:

- Loads `business.json` + `review.json`.
- Builds an enriched RDD joining reviews to business lat/lon + categories.
- Buckets events into ~2 km grids and reduces them into:
  - `city_metrics/`
  - `grid_hotspots/`
  - `category_leaders/`
  - `latest_reviews/`
- Writes Parquet snapshots into `data/output/`.

These Parquet directories back the dashboard.

## Run the dashboard

After generating the parquet outputs, launch Streamlit:

```
streamlit run src/dashboard/app.py
```

If your config lives elsewhere, set `YELP_CONFIG=/path/to/config.yaml`. The UI exposes:

- Map of active hotspots filtered by city + review count threshold.
- City leaderboard table (volume + average rating).
- Top categories per city.
- Rolling feed of recent reviews.

## Tests

```
pytest -q
```

Tests cover geo bucketing, category parsing, and aggregation math atop a local Spark session.
