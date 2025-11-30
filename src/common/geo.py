"""Geospatial helpers for grid-based aggregations."""

from __future__ import annotations

import math
from .models import GeoCell

KM_PER_DEGREE = 111.0  # Approx conversion for small areas


class GeoBucketer:
    """Maps latitude/longitude pairs into deterministic grid cells."""

    def __init__(self, cell_size_km: float = 2.0) -> None:
        self.cell_size_km = max(cell_size_km, 0.1)
        self.cell_size_degrees = self.cell_size_km / KM_PER_DEGREE

    def bucket(self, latitude: float, longitude: float) -> GeoCell:
        if latitude is None or longitude is None:
            raise ValueError("Latitude and longitude must be provided for geo bucketing.")

        lat_index = math.floor(latitude / self.cell_size_degrees)
        lon_index = math.floor(longitude / self.cell_size_degrees)
        center_lat = (lat_index + 0.5) * self.cell_size_degrees
        center_lon = (lon_index + 0.5) * self.cell_size_degrees
        grid_id = f"{lat_index}_{lon_index}"
        return GeoCell(grid_id=grid_id, latitude=round(center_lat, 6), longitude=round(center_lon, 6))
