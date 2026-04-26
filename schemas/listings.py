import json
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, field_validator


class ListingSource(BaseModel):
    source: str
    url: Optional[str] = None
    external_id: Optional[str] = None
    price: Optional[float] = None


class ListingItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    external_id: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    price: Optional[float] = None
    price_per_m2: Optional[float] = None
    rooms: Optional[int] = None
    area_total: Optional[float] = None
    floor: Optional[int] = None
    floors_total: Optional[int] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    h3_index: Optional[str] = None
    is_active: bool
    is_hot_deal: Optional[bool] = None
    discount_percent: Optional[float] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    thumbnail_url: Optional[str] = None
    photos: Optional[List[str]] = None
    sources: List[ListingSource] = []

    @field_validator("photos", mode="before")
    @classmethod
    def parse_photos(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v


class ListingsResponse(BaseModel):
    total: int
    items: list[ListingItem]


class BuildingPin(BaseModel):
    building_id: int
    address: Optional[str]
    latitude: float
    longitude: float
    h3_index: Optional[str]
    listings_count: int


class BuildingPinsResponse(BaseModel):
    items: list[BuildingPin]
