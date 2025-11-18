# fluffy-guacamole

Geospatial Real-Time Yelp Activity Dashboard

Use the businesses + reviews datasets.

Stream reviews and use business coordinates.

## Dataset setup

1. Grab the Yelp Open Dataset tarball from https://business.yelp.com/data/resources/open-dataset/.
2. Extract it locally; the archive expands into a `yelp_dataset` directory containing the JSON files (businesses, reviews, etc.).
3. Move the `yelp_dataset` directory into this repository root so paths like `./yelp_dataset/business.json` exist for the ingestion scripts.

Spark Analytics:
 • Reviews per neighborhood/city (streaming aggregations)
 • Real-time rating changes by area
 • Trending categories geographically
 • Using UDFs to map lat/long → grids or clusters

Dashboard:
 • Live map with review hotspots
 • Best-rated cuisines now
 • Real-time rating fluctuations by city

Why good: Spark + geospatial + real-time windows.
