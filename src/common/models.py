"""Dataclasses shared between the ingestion and analytics layers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Tuple


@dataclass(frozen=True)
class BusinessRecord:
    business_id: str
    name: str
    city: str
    state: str
    latitude: float
    longitude: float
    categories: Tuple[str, ...]
    stars: float


@dataclass(frozen=True)
class ReviewRecord:
    review_id: str
    business_id: str
    user_id: str
    stars: float
    date: datetime
    text: str


@dataclass(frozen=True)
class EnrichedReview:
    review_id: str
    business_id: str
    user_id: str
    review_stars: float
    review_date: datetime
    review_text: str
    business_name: str
    city: str
    state: str
    latitude: float
    longitude: float
    business_stars: float
    categories: Tuple[str, ...]

    @property
    def safe_categories(self) -> Tuple[str, ...]:
        """Avoid returning empty tuples in downstream aggregations."""

        return tuple(cat for cat in self.categories if cat)


@dataclass(frozen=True)
class GeoCell:
    """Represents a grid cell produced by the GeoBucketer."""

    grid_id: str
    latitude: float
    longitude: float

