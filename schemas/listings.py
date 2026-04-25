from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class ListingItem(BaseModel):
    id: int
    external_id: str
    url: Optional[str]
    price: Optional[float]
    price_per_m2: Optional[float]
    rooms: Optional[int]
    area_total: Optional[float]
    floor: Optional[int]
    floors_total: Optional[int]
    address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    h3_index: Optional[str]
    is_active: bool
    is_hot_deal: Optional[bool]
    discount_percent: Optional[float]
    first_seen_at: Optional[datetime]
    last_seen_at: Optional[datetime]
    thumbnail_url: Optional[str] = None
    photos: Optional[List[str]] = None


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
