import json

import h3
from fastapi import APIRouter, Request

from schemas.predict import PredictRequest, PredictResponse

router = APIRouter(prefix="/predict-price", tags=["predict"])

H3_RESOLUTION = 9

# Коррекция на этаж: первый и последний этаж дешевле на 3%
FLOOR_DISCOUNT = 0.03


@router.post("", response_model=PredictResponse)
async def predict_price(request: Request, body: PredictRequest):
    """
    Rule-based оценка стоимости квартиры.

    Алгоритм:
    1. Переводим координаты в H3-индекс (resolution 9).
    2. Берём медианную цену м² для данной соты и комнатности из price_stats.
    3. Прогноз = медиана × площадь, с коррекцией на этаж (±3%).
    4. Сохраняем результат в price_predictions для истории.
    """
    pool = request.app.state.pool

    h3_index = h3.latlng_to_cell(body.latitude, body.longitude, H3_RESOLUTION)

    async with pool.acquire() as conn:
        stat = await conn.fetchrow(
            """
            SELECT median_price_per_m2, listings_count
            FROM price_stats
            WHERE h3_index = $1 AND rooms = $2
            """,
            h3_index,
            body.rooms,
        )

        if not stat or stat["median_price_per_m2"] is None:
            # Данных по соте нет — возвращаем пустой прогноз
            return PredictResponse(
                predicted_price=None,
                price_per_m2_used=None,
                h3_index=h3_index,
                listings_in_cell=None,
                method="median_h3",
                note="Недостаточно данных для этой локации",
            )

        price_per_m2 = float(stat["median_price_per_m2"])

        # Коррекция на этаж
        floor_factor = 1.0
        note = None
        if body.floor == 1:
            floor_factor = 1.0 - FLOOR_DISCOUNT
            note = f"Применена скидка {FLOOR_DISCOUNT * 100:.0f}% за первый этаж"
        elif body.floors_total and body.floor == body.floors_total:
            floor_factor = 1.0 - FLOOR_DISCOUNT
            note = f"Применена скидка {FLOOR_DISCOUNT * 100:.0f}% за последний этаж"

        predicted_price = round(price_per_m2 * body.area_total * floor_factor, 2)

        # Сохраняем прогноз в историю
        input_params = body.model_dump()
        input_params["h3_index"] = h3_index
        await conn.execute(
            """
            INSERT INTO price_predictions (input_params, predicted_price)
            VALUES ($1::jsonb, $2)
            """,
            json.dumps(input_params),
            predicted_price,
        )

    return PredictResponse(
        predicted_price=predicted_price,
        price_per_m2_used=price_per_m2,
        h3_index=h3_index,
        listings_in_cell=stat["listings_count"],
        method="median_h3",
        note=note,
    )
