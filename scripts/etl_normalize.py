"""
ETL-пайплайн: нормализует listings_raw → h3_cells → buildings → flats → listings.

Порядок INSERT диктуется FK-зависимостями:
  listings_raw (уже есть) → h3_cells → buildings → flats → listings

Запуск после каждого прохода скрапера:
    python -m scripts.etl_normalize
"""

import asyncio
import logging

import asyncpg
import h3

from config import DATABASE_DSN, setup_logging

H3_RESOLUTION = 9  # ~174м — для аналитики по районам
H3_R11_RESOLUTION = 11  # ~27м — уровень здания, ключ дедупликации
AREA_TOLERANCE = 0.5  # допуск на совпадение площади квартиры, м²

logger = logging.getLogger(__name__)


async def upsert_h3_cells(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    cells: list[tuple[str, int]],
):
    await conn.executemany(
        """
        INSERT INTO h3_cells (h3_index, resolution)
        VALUES ($1, $2)
        ON CONFLICT (h3_index) DO NOTHING
        """,
        cells,
    )


async def get_or_create_building(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy, raw: dict
) -> int | None:
    """
    Возвращает building_id.
    Дедупликация по h3_index_r11 (H3 resolution 11, ~27м ≈ один дом).
    """
    lat = raw["latitude"]
    lon = raw["longitude"]
    if lat is None or lon is None:
        return None

    h3_r9 = h3.latlng_to_cell(float(lat), float(lon), H3_RESOLUTION)
    h3_r11 = h3.latlng_to_cell(float(lat), float(lon), H3_R11_RESOLUTION)

    await upsert_h3_cells(conn, [(h3_r9, H3_RESOLUTION), (h3_r11, H3_R11_RESOLUTION)])

    row = await conn.fetchrow(
        """
        INSERT INTO buildings (address, latitude, longitude, h3_index, h3_index_r11,
                               year_built, floors_total, material_type)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (h3_index_r11) DO UPDATE SET
            year_built    = COALESCE(buildings.year_built,    EXCLUDED.year_built),
            floors_total  = COALESCE(buildings.floors_total,  EXCLUDED.floors_total),
            material_type = COALESCE(buildings.material_type, EXCLUDED.material_type)
        RETURNING id
        """,
        raw.get("address_text"),
        lat,
        lon,
        h3_r9,
        h3_r11,
        raw.get("year_built"),
        raw.get("floors_total"),
        raw.get("material_type"),
    )
    return row["id"] if row is not None else None


async def get_or_create_flat(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    building_id: int,
    raw: dict,
) -> int:
    """
    Возвращает flat_id.
    Стратегия дедупликации:
    1. Если этот (source, external_id) уже есть в listings — переиспользуем flat_id.
    2. Иначе ищем по (building_id, rooms, floor) с допуском ±0.5м² на площадь.
    3. Не нашли — создаём новую запись.
    """
    # Шаг 1: external_id уже обрабатывался — переиспользуем flat_id
    existing = await conn.fetchrow(
        """
        SELECT l.flat_id
        FROM listings l
        JOIN listings_raw lr ON l.raw_id = lr.id
        WHERE lr.source = $1 AND lr.external_id = $2
        LIMIT 1
        """,
        raw["source"],
        raw["external_id"],
    )
    if existing:
        return existing["flat_id"]

    # Шаг 2: поиск существующей квартиры по параметрам
    area = raw.get("area_total")
    rooms = raw.get("rooms")
    floor = raw.get("floor")

    if area is not None and rooms is not None and floor is not None:
        match = await conn.fetchrow(
            """
            SELECT id FROM flats
            WHERE building_id = $1
              AND rooms = $2
              AND floor = $3
              AND ABS(area_total - $4) <= $5
            LIMIT 1
            """,
            building_id,
            rooms,
            floor,
            float(area),
            AREA_TOLERANCE,
        )
        if match:
            return match["id"]

    # Шаг 3: создаём новую запись квартиры
    row = await conn.fetchrow(
        """
        INSERT INTO flats (building_id, area_total, area_kitchen, rooms, floor)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        building_id,
        area,
        raw.get("area_kitchen"),
        rooms,
        floor,
    )
    if row is None:
        raise RuntimeError(
            f"INSERT INTO flats не вернул id для building_id={building_id}"
        )
    return row["id"]


async def upsert_listing(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    raw_id: int,
    flat_id: int,
    raw: dict,
):
    """
    Создаёт или обновляет запись в listings.
    При изменении цены или деактивации фиксирует snapshot.
    """
    price = raw.get("price")
    area = raw.get("area_total")
    price_per_m2 = (float(price) / float(area)) if price and area else None
    parsed_at = raw.get("parsed_at")
    is_active = raw.get("is_active", True)

    existing = await conn.fetchrow(
        "SELECT id, price FROM listings WHERE raw_id = $1", raw_id
    )

    if existing:
        listing_id = existing["id"]
        if existing["price"] != price:
            await conn.execute(
                """
                INSERT INTO listing_snapshots (listing_id, price, is_online, seen_at)
                VALUES ($1, $2, true, NOW())
                """,
                listing_id,
                price,
            )
        if not is_active:
            await conn.execute(
                """
                INSERT INTO listing_snapshots (listing_id, price, is_online, seen_at)
                VALUES ($1, $2, false, NOW())
                """,
                listing_id,
                existing["price"],
            )
        await conn.execute(
            """
            UPDATE listings
            SET price = $1, price_per_m2 = $2, last_seen_at = $3, is_active = $4
            WHERE id = $5
            """,
            price,
            price_per_m2,
            parsed_at,
            is_active,
            listing_id,
        )
    else:
        await conn.execute(
            """
            INSERT INTO listings (raw_id, flat_id, price, price_per_m2,
                                  is_active, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            """,
            raw_id,
            flat_id,
            price,
            price_per_m2,
            is_active,
            parsed_at,
        )


async def process_row(pool: asyncpg.Pool, raw: dict):
    raw_id = raw["id"]
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                building_id = await get_or_create_building(conn, raw)
                if building_id is None:
                    logger.warning(
                        f"raw_id={raw_id}: нет координат, пропускаем нормализацию зданий/квартир"
                    )
                    await conn.execute(
                        "UPDATE listings_raw SET normalized_at = NOW() WHERE id = $1",
                        raw_id,
                    )
                    return

                flat_id = await get_or_create_flat(conn, building_id, raw)
                await upsert_listing(conn, raw_id, flat_id, raw)
                await conn.execute(
                    "UPDATE listings_raw SET normalized_at = NOW() WHERE id = $1",
                    raw_id,
                )
    except Exception as e:
        logger.error(f"Ошибка обработки raw_id={raw_id}: {e}")


async def process_batch(pool: asyncpg.Pool, rows: list[dict]):
    """Обрабатывает батч параллельно, каждая строка — отдельное соединение из пула."""
    await asyncio.gather(*[process_row(pool, raw) for raw in rows])


async def run(dsn: str = DATABASE_DSN, batch_size: int = 2000):
    logger.info("Запуск ETL-нормализации")
    pool = await asyncpg.create_pool(dsn, min_size=4, max_size=20)

    try:
        total = 0
        while True:
            rows = await pool.fetch(
                """
                SELECT id, source, external_id, price, area_total, area_kitchen,
                       rooms, floor, floors_total,
                       latitude, longitude, address_text,
                       year_built, material_type, parsed_at,
                       is_active
                FROM listings_raw
                WHERE normalized_at IS NULL
                ORDER BY id
                LIMIT $1
                """,
                batch_size,
            )
            if not rows:
                logger.info(f"ETL завершён. Итого обработано: {total} строк")
                break

            logger.info(f"Батч: {len(rows)} строк (всего обработано: {total})")
            await process_batch(pool, [dict(r) for r in rows])
            total += len(rows)
    finally:
        await pool.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())
