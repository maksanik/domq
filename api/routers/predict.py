import asyncio
import json

import h3
from fastapi import APIRouter, Request

from schemas.predict import PredictRequest, PredictResponse

router = APIRouter(prefix="/predict-price", tags=["predict"])

H3_RESOLUTION = 9
MIN_LISTINGS = 5  # порог: при меньшем числе расширяем на соседей
FLOOR_DISCOUNT = 0.03
KNN_K = 10
# Географические константы для Москвы (широта ~55.75°)
_LAT_KM = 111.0
_LNG_KM = 111.0 * 0.636  # умножаем на cos(55.75°)


# ── Вспомогательные корутины ──────────────────────────────────────────────────


async def _get_h3_stat(pool, h3_index: str, rooms: int):
    """Медианная цена м² и число объявлений для соты."""
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT median_price_per_m2, listings_count
            FROM price_stats
            WHERE h3_index = $1 AND rooms = $2
            """,
            h3_index,
            rooms,
        )


async def _get_neighbor_stat(pool, h3_index: str, rooms: int):
    """Взвешенная медиана по disk(1) при нехватке данных в основной соте."""
    disk = list(h3.grid_disk(h3_index, 1))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT median_price_per_m2, listings_count
            FROM price_stats
            WHERE h3_index = ANY($1::text[]) AND rooms = $2
            """,
            disk,
            rooms,
        )
    valid = [r for r in rows if r["median_price_per_m2"] is not None]
    if not valid:
        return None, None, disk
    total_count = sum(r["listings_count"] for r in valid)
    weighted = (
        sum(r["median_price_per_m2"] * r["listings_count"] for r in valid) / total_count
    )
    return weighted, total_count, disk


async def _knn_predict(
    pool, lat: float, lng: float, rooms: int, area: float, floor: int, floors_total
):
    """
    KNN-оценка цены м² по k ближайшим активным объявлениям.

    Пространство признаков (нормализованное евклидово расстояние):
      d = sqrt(
            1.00 · d_geo_km² +
            0.40 · (Δarea/30)² +
            0.15 · Δfloor_ratio²
          )
    Вес объявления: w = 1 / (d + 0.05).
    Итог: взвешенное среднее price_per_m2 по top-k.
    """
    async with pool.acquire() as conn:
        candidates = await conn.fetch(
            """
            SELECT l.price_per_m2,
                   b.latitude, b.longitude,
                   f.area_total, f.floor, b.floors_total
            FROM listings l
            JOIN flats    f ON l.flat_id    = f.id
            JOIN buildings b ON f.building_id = b.id
            WHERE l.is_active = true
              AND f.rooms = $1
              AND b.latitude  BETWEEN $2 - 0.02 AND $2 + 0.02
              AND b.longitude BETWEEN $3 - 0.03 AND $3 + 0.03
              AND l.price_per_m2 IS NOT NULL
            LIMIT 500
            """,
            rooms,
            lat,
            lng,
        )

    if not candidates:
        return None, None

    target_fr = (floor / floors_total) if floors_total else 0.5

    distances = []
    for r in candidates:
        dlat_km = (float(r["latitude"]) - lat) * _LAT_KM
        dlng_km = (float(r["longitude"]) - lng) * _LNG_KM
        d_geo = (dlat_km**2 + dlng_km**2) ** 0.5

        d_area = abs(float(r["area_total"]) - area) / 30.0
        fr = (r["floor"] / r["floors_total"]) if r["floors_total"] else 0.5
        d_floor = abs(fr - target_fr)

        d = (1.0 * d_geo**2 + 0.4 * d_area**2 + 0.15 * d_floor**2) ** 0.5
        distances.append((d, float(r["price_per_m2"])))

    distances.sort(key=lambda x: x[0])
    top_k = distances[:KNN_K]

    weight_total = sum(1.0 / (d + 0.05) for d, _ in top_k)
    weighted_sum = sum((1.0 / (d + 0.05)) * ppm2 for d, ppm2 in top_k)
    return round(weighted_sum / weight_total, 2), len(top_k)


# ── Эндпоинт ─────────────────────────────────────────────────────────────────


@router.post("", response_model=PredictResponse)
async def predict_price(request: Request, body: PredictRequest):
    """
    Параллельная оценка стоимости квартиры двумя методами:

    Метод 1 — Медианная H3 (median_h3 / median_h3_neighbors):
      1. Координаты → H3-индекс (res. 9, ~174 м).
      2. Медианная цена м² из price_stats для соты × комнатность.
      3. Если объявлений < MIN_LISTINGS — расширяем на disk(1) (6 соседей).
      4. Прогноз = медиана × площадь × floor_factor.

    Метод 2 — KNN (k=10):
      1. Bbox ~2.2 км, фильтр по комнатности.
      2. Расстояние: sqrt(d_geo² + 0.4·(Δarea/30)² + 0.15·Δfloor_ratio²).
      3. Взвешенное среднее price_per_m2 по top-10.
      4. Прогноз = knn_ppm2 × площадь × floor_factor.

    Оба метода запускаются параллельно через asyncio.gather.
    """
    pool = request.app.state.pool

    h3_index = h3.latlng_to_cell(body.latitude, body.longitude, H3_RESOLUTION)

    # Параллельный запуск: H3-медиана и KNN
    stat, (knn_ppm2, knn_count) = await asyncio.gather(
        _get_h3_stat(pool, h3_index, body.rooms),
        _knn_predict(
            pool,
            body.latitude,
            body.longitude,
            body.rooms,
            body.area_total,
            body.floor,
            body.floors_total,
        ),
    )

    method = "median_h3"
    neighbor_cells = None

    need_expand = (
        not stat
        or stat["median_price_per_m2"] is None
        or stat["listings_count"] < MIN_LISTINGS
    )

    if need_expand:
        weighted, total_count, disk = await _get_neighbor_stat(
            pool, h3_index, body.rooms
        )
        if weighted is not None:
            stat = {"median_price_per_m2": weighted, "listings_count": total_count}
            method = "median_h3_neighbors"
            neighbor_cells = disk

    if not stat or stat["median_price_per_m2"] is None:
        return PredictResponse(
            predicted_price=None,
            price_per_m2_used=None,
            h3_index=h3_index,
            listings_in_cell=None,
            method="median_h3",
            note="Недостаточно данных для этой локации",
            neighbor_cells=None,
            knn_predicted_price=None,
            knn_price_per_m2=None,
            knn_listings_used=None,
        )

    price_per_m2 = float(stat["median_price_per_m2"])

    # Коррекция на этаж (применяется к обоим методам)
    floor_factor = 1.0
    note = None
    if body.floor == 1:
        floor_factor = 1.0 - FLOOR_DISCOUNT
        note = f"Применена скидка {FLOOR_DISCOUNT * 100:.0f}% за первый этаж"
    elif body.floors_total and body.floor == body.floors_total:
        floor_factor = 1.0 - FLOOR_DISCOUNT
        note = f"Применена скидка {FLOOR_DISCOUNT * 100:.0f}% за последний этаж"

    predicted_price = round(price_per_m2 * body.area_total * floor_factor, 2)
    knn_predicted = (
        round(knn_ppm2 * body.area_total * floor_factor, 2)
        if knn_ppm2 is not None
        else None
    )

    # Сохраняем прогноз в историю (оба метода)
    input_params = body.model_dump()
    input_params["h3_index"] = h3_index
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO price_predictions (input_params, predicted_price, knn_predicted_price)
            VALUES ($1::jsonb, $2, $3)
            """,
            json.dumps(input_params),
            predicted_price,
            knn_predicted,
        )

    return PredictResponse(
        predicted_price=predicted_price,
        price_per_m2_used=price_per_m2,
        h3_index=h3_index,
        listings_in_cell=stat["listings_count"],
        method=method,
        note=note,
        neighbor_cells=neighbor_cells,
        knn_predicted_price=knn_predicted,
        knn_price_per_m2=knn_ppm2,
        knn_listings_used=knn_count,
    )
