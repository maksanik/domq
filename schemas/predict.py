from typing import Optional

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    latitude: float = Field(..., description="Широта квартиры")
    longitude: float = Field(..., description="Долгота квартиры")
    area_total: float = Field(..., gt=0, description="Общая площадь, м²")
    rooms: int = Field(..., ge=1, le=9, description="Количество комнат")
    floor: int = Field(..., ge=1, description="Этаж")
    floors_total: Optional[int] = Field(None, description="Этажей в доме")


class PredictResponse(BaseModel):
    predicted_price: Optional[float]
    price_per_m2_used: Optional[float]
    h3_index: Optional[str]
    listings_in_cell: Optional[int]
    method: str
    note: Optional[str]
