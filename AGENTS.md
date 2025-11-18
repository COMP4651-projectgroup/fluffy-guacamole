# Repository Guidelines

## Project Overview
Real-Time Geospatial Yelp Activity Dashboard with Apache Spark replays Yelp activity so people see nearby trends; Yelp stores cities, coordinates, and categories, but most analysis stays offline.

### Data Sources & Streaming
We ingest `business.json` (business_id, name, city/state, lat/lon, categories, stars), `review.json` (stars, text, user_id, business_id, date as event time), and `checkin.json` (ordered timestamps), join events to business coordinates, and replay them chronologically to mimic streams.

### Objectives, Architecture & Spark Model
- Goals: simulate review/check-in streams, compute Spark RDD + Structured Streaming aggregates, flag city/neighborhood/grid hotspots, track rating shifts and category leaders, and surface them in a map/time-series dashboard.
- Flow: data sources → feeders/Kafka → Spark Structured Streaming handles joins/windows/bucketing → RDD pipeline (`IngestionService → RDDBuilder → GeoBucketer → Aggregator → Persistence`) → Streamlit/folium dashboard reading Parquet/Delta outputs.
- Spark primer: RDDs are immutable distributed collections; transformations (map/filter/flatMap/mapPartitions/join/reduceByKey) stay lazy, actions (count/collect/take/saveAsTextFile/countByKey/foreach) trigger execution, and lineage handles recovery.

### Geospatial Modeling & Analytics
- Map events to cities, grid cells, geohashes, or K-Means areas so metrics stay area-based; sliding 30-minute windows (10-minute slide) output hotspot scores vs baselines, catch rating swings, and flag “emerging areas” when activity and ratings climb together.
- Category joins rank top cuisines now, compare mixes with historical norms, and aggregate by city for leaderboards/anomalies.

### Dashboard, Stack & Roadmap
Dashboard needs a hotspot heatmap, clickable cards with recent reviews, avg rating, and top categories, a time-series panel, and filters for city, category, time range, and hotspot thresholds. Stack: Yelp dataset, Python feeders/Kafka, Spark (RDD API, Structured Streaming, Spark SQL, window functions), geospatial bucketing (grid/geohash/cluster), Parquet/Delta sinks, Streamlit UI via folium or pydeck. Plan: Phases 1–5 cover explore/clean data, simulate streams, build RDD + geospatial logic, wire dashboard, and evaluate/demo; afterward add sentiment-aware maps, anomaly detection, and H3-style libraries.

## Contributor Workflow & Standards
Place code under `src/{ingest,streaming,dashboard,common}` with tests and keep large Yelp dumps or Parquet/Delta shards in git-ignored `data/`. Run `python -m venv .venv && source .venv/bin/activate`, `pip install -r requirements.txt`, `spark-submit src/streaming/review_stream.py --config config/local.yaml`, and `pytest tests -q`. Use 4-space indentation, type hints, Black, `ruff check .`, and `snake_case`; favor pure transforms and keep fixtures in `tests/fixtures/` with ≥80% coverage, mocking Kafka sinks or dashboard clients. Commits stay short/imperative; PRs cite issues, attach CLI output or screenshots, confirm lint/tests, highlight schema/config changes, never commit secrets or raw Yelp data, and document Spark UI ports/deployment notes.
