from datetime import datetime
from typing import Optional

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


class ListingsResponse(BaseModel):
    total: int
    items: list[ListingItem]
