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
tests/                # pytest suite
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
   - `geo.cell_size_km` controls the grid size; `geo.dense_city_threshold` toggles K-Means neighborhoods when a city has at least that many businesses.
   - `analytics.window_duration` / `analytics.slide_duration` drive sliding windows for hotspot scores; thresholds under `analytics.*` gate emerging-area + anomaly flags.
4. Run the Spark replay job to materialize parquet outputs:
   ```
   python -m src.streaming.review_stream --config config/local.yaml
   ```

The replay job:

- Loads `business.json` + `review.json`.
- Builds an enriched RDD joining reviews to business lat/lon + categories.
- Buckets events into ~2 km grids; dense cities also run K-Means clustering over businesses (threshold defined by `geo.dense_city_threshold`) to derive neighborhoods.
- Emits parquet tables:
  - `city_metrics/`
  - `grid_hotspots/`
  - `neighborhood_hotspots/` (dense cities only)
  - `window_hotspots/` (sliding windows for hotspot score, rating changes, emerging-area flags per grid/neighborhood)
  - `category_leaders/`
  - `latest_reviews/`
- Writes Parquet snapshots into `data/output/`.

### Streaming vs static sources

By default the replay uses the static Yelp dataset (`source.type: static`). You can swap to streaming sources with the same review schema:

- File stream: `source.type: file_stream`, set `source.stream_path` to a folder of newline-delimited review JSON and `source.checkpoint_path` for Structured Streaming.
- Kafka: `source.type: kafka`, set `kafka_bootstrap_servers`, `kafka_topic`, `kafka_starting_offsets`, and `checkpoint_path`.

Both streaming modes reuse the same aggregation pipeline and append micro-batches to the parquet outputs.

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

## Dockerized run (dataset lives in repo)

The image includes Java 17 for Spark. With `yelp_dataset/` in the repo root, a single bind mount is enough.

- Build: `docker build -t fluffy:latest .`
- Run the replay job inside the container:
  ```
  docker run --rm \
    -v /home/ubuntu/fluffy-guacamole:/app \
    fluffy:latest \
    python -m src.streaming.review_stream --config config/local.yaml
  ```
  This uses `./yelp_dataset/...` from the repo (`/app/yelp_dataset/...` in the container). If your dataset lives elsewhere, mount it (e.g., `-v /data/yelp_dataset:/app/yelp_dataset:ro`) or update the paths in `config/local.yaml`.
- Run the dashboard container on localhost only (pair with nginx or an SSH tunnel for access):
  ```
  docker run -d --name fluffy-ui \
    -v /home/ubuntu/fluffy-guacamole:/app \
    -p 127.0.0.1:8501:8501 \
    fluffy:latest \
    streamlit run src/dashboard/app.py
  ```

## Nginx reverse proxy (front the dashboard)

Steps on EC2 instance:
- Install Nginx: `sudo apt-get update && sudo apt-get install -y nginx`.
- Run the dashboard using the Docker run command above.
- Create `/etc/nginx/sites-available/fluffy` with:
  ```
  server {
      listen 80;
      server_name _;

      client_max_body_size 10m;

      location / {
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "upgrade";
          proxy_set_header Host $host;
          proxy_set_header X-Real-IP $remote_addr;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header X-Forwarded-Proto $scheme;

          proxy_pass http://127.0.0.1:8501;
          proxy_read_timeout 300;
          proxy_send_timeout 300;
      }
  }
  ```
- Enable: `sudo ln -s /etc/nginx/sites-available/fluffy /etc/nginx/sites-enabled/ && sudo nginx -t && sudo systemctl reload nginx`.
- Remove the default site if needed: `sudo rm /etc/nginx/sites-enabled/default && sudo systemctl reload nginx`.
- Security groups: allow inbound 80/443; keep 8501 closed (only Nginx reaches it).
- Reload Nginx after config changes: `sudo systemctl reload nginx`.
