from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class H3StatItem(BaseModel):
    h3_index: str
    rooms: int
    median_price_per_m2: Optional[float]
    avg_price_per_m2: Optional[float]
    listings_count: int
    calculated_at: Optional[datetime]


class PriceHistoryPoint(BaseModel):
    date: date
    median_price_per_m2: Optional[float]


class LiquidityItem(BaseModel):
    avg_days_on_market: Optional[int]
    median_days: Optional[int]


class H3DetailResponse(BaseModel):
    h3_index: str
    rooms: Optional[int]
    price_stats: Optional[H3StatItem]
    liquidity: Optional[LiquidityItem]
    price_history: list[PriceHistoryPoint]


class H3MapItem(BaseModel):
    h3_index: str
    rooms: int
    median_price_per_m2: Optional[float]
    listings_count: int
