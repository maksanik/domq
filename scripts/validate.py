"""
Leave-one-out валидация методов оценки цены: H3-медиана и KNN.

Для каждого тестового объявления предсказание строится БЕЗ него самого
в выборке (AND l.id != $test_id), чтобы исключить data leakage.

H3-медиана пересчитывается на лету из таблицы listings (не из price_stats,
которая содержит агрегаты со всеми данными).

Запуск:
    python -m scripts.validate
    python -m scripts.validate --sample 500
"""

import argparse
import asyncio
import csv
import logging
import math
import statistics
from datetime import datetime
from pathlib import Path

import asyncpg
import h3

from config import BASE_DIR, DATABASE_DSN, setup_logging

KNN_K = 10
MIN_LISTINGS = 5
FLOOR_DISCOUNT = 0.03
_LAT_KM = 111.0
_LNG_KM = 111.0 * 0.636
DEFAULT_SAMPLE = 300

logger = logging.getLogger(__name__)


async def _knn_loo(
    conn,
    test_id: int,
    lat: float,
    lng: float,
    rooms: int,
    area: float,
    floor: int,
    floors_total,
):
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
          AND l.id != $4
        LIMIT 500
        """,
        rooms,
        lat,
        lng,
        test_id,
    )
    if not candidates:
        return None

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
    return weighted_sum / weight_total


async def _h3_median_loo(conn, test_id: int, h3_index: str, rooms: int):
    # На лету из listings, а не из price_stats — иначе test_id уже внутри агрегата
    row = await conn.fetchrow(
        """
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price_per_m2) AS median_ppm2,
               COUNT(*) AS cnt
        FROM listings l
        JOIN flats    f ON l.flat_id    = f.id
        JOIN buildings b ON f.building_id = b.id
        WHERE b.h3_index = $1
          AND f.rooms = $2
          AND l.is_active = true
          AND l.price_per_m2 IS NOT NULL
          AND l.id != $3
        """,
        h3_index,
        rooms,
        test_id,
    )
    if row and row["cnt"] >= MIN_LISTINGS:
        return float(row["median_ppm2"])

    # Fallback: disk(1) — медиана по соседним сотам
    disk = list(h3.grid_disk(h3_index, 1))
    row2 = await conn.fetchrow(
        """
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price_per_m2) AS median_ppm2,
               COUNT(*) AS cnt
        FROM listings l
        JOIN flats    f ON l.flat_id    = f.id
        JOIN buildings b ON f.building_id = b.id
        WHERE b.h3_index = ANY($1::text[])
          AND f.rooms = $2
          AND l.is_active = true
          AND l.price_per_m2 IS NOT NULL
          AND l.id != $3
        """,
        disk,
        rooms,
        test_id,
    )
    if row2 and row2["cnt"] > 0:
        return float(row2["median_ppm2"])
    return None


def _floor_factor(floor: int, floors_total) -> float:
    if floor == 1:
        return 1.0 - FLOOR_DISCOUNT
    if floors_total and floor == floors_total:
        return 1.0 - FLOOR_DISCOUNT
    return 1.0


def _print_metrics(name: str, valid: list[dict], key_pct: str, key_pred: str):
    if not valid:
        print(f"\n{name}: нет данных")
        return
    errors_pct = [r[key_pct] for r in valid]
    abs_errors = [abs(r[key_pred] - r["actual_price"]) for r in valid]
    sq_errors = [(r[key_pred] - r["actual_price"]) ** 2 for r in valid]
    print(f"\n{'=' * 52}")
    print(f"  {name}  (n={len(valid)})")
    print(f"{'=' * 52}")
    print(f"  MAPE:       {statistics.mean(errors_pct):.2f}%")
    print(f"  MedianAPE:  {statistics.median(errors_pct):.2f}%")
    print(f"  MAE:        {statistics.mean(abs_errors):>14,.0f} руб.")
    print(f"  RMSE:       {math.sqrt(statistics.mean(sq_errors)):>14,.0f} руб.")


async def run(dsn: str = DATABASE_DSN, sample_size: int = DEFAULT_SAMPLE):
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    try:
        logger.info(f"Выборка {sample_size} случайных объявлений...")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT l.id, l.price,
                       b.latitude, b.longitude, b.h3_index, b.floors_total,
                       f.rooms, f.area_total, f.floor
                FROM listings l
                JOIN flats    f ON l.flat_id    = f.id
                JOIN buildings b ON f.building_id = b.id
                WHERE l.is_active = true
                  AND l.price IS NOT NULL
                  AND l.price_per_m2 IS NOT NULL
                  AND b.latitude IS NOT NULL
                  AND b.h3_index IS NOT NULL
                  AND f.rooms IS NOT NULL
                  AND f.area_total IS NOT NULL
                  AND f.floor IS NOT NULL
                ORDER BY RANDOM()
                LIMIT $1
                """,
                sample_size,
            )

        if not rows:
            logger.error("Нет данных для валидации")
            return

        logger.info(f"Получено {len(rows)} объявлений, начинаю расчёт...")

        results = []
        skipped = 0

        async with pool.acquire() as conn:
            for i, r in enumerate(rows, 1):
                test_id = r["id"]
                lat = float(r["latitude"])
                lng = float(r["longitude"])
                rooms = r["rooms"]
                area = float(r["area_total"])
                floor = r["floor"]
                floors_total = r["floors_total"]
                h3_index = r["h3_index"]
                actual_price = float(r["price"])
                ff = _floor_factor(floor, floors_total)

                knn_ppm2 = await _knn_loo(
                    conn, test_id, lat, lng, rooms, area, floor, floors_total
                )
                h3_ppm2 = await _h3_median_loo(conn, test_id, h3_index, rooms)

                if knn_ppm2 is None and h3_ppm2 is None:
                    skipped += 1
                    continue

                knn_price = (
                    round(knn_ppm2 * area * ff, 2) if knn_ppm2 is not None else None
                )
                h3_price = (
                    round(h3_ppm2 * area * ff, 2) if h3_ppm2 is not None else None
                )

                results.append(
                    {
                        "listing_id": test_id,
                        "actual_price": actual_price,
                        "h3_predicted": h3_price,
                        "knn_predicted": knn_price,
                        "h3_error_pct": round(
                            abs(h3_price - actual_price) / actual_price * 100, 2
                        )
                        if h3_price
                        else None,
                        "knn_error_pct": round(
                            abs(knn_price - actual_price) / actual_price * 100, 2
                        )
                        if knn_price
                        else None,
                        "rooms": rooms,
                        "area": area,
                        "h3_index": h3_index,
                    }
                )

                if i % 50 == 0:
                    logger.info(f"  {i}/{len(rows)}...")

        logger.info(f"Готово. Пропущено (нет данных): {skipped}")

        h3_valid = [r for r in results if r["h3_error_pct"] is not None]
        knn_valid = [r for r in results if r["knn_error_pct"] is not None]

        _print_metrics("H3-медиана", h3_valid, "h3_error_pct", "h3_predicted")
        _print_metrics("KNN (k=10)", knn_valid, "knn_error_pct", "knn_predicted")

        reports_dir = Path(BASE_DIR) / "reports"
        reports_dir.mkdir(exist_ok=True)
        csv_path = (
            reports_dir / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"CSV сохранён: {csv_path}")

    finally:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LOO-валидация методов оценки цены")
    parser.add_argument(
        "--sample",
        type=int,
        default=DEFAULT_SAMPLE,
        help=f"Размер выборки (default: {DEFAULT_SAMPLE})",
    )
    args = parser.parse_args()
    setup_logging()
    asyncio.run(run(sample_size=args.sample))
