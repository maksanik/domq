"""
Одноразовая очистка данных Avito:
  1. Удаляет долевые объявления (title содержит 'доля').
  2. Исправляет ошибочно спарсенную площадь (запятая вместо точки в числе)
     и сбрасывает нормализацию для повторной обработки через etl_normalize.

Запуск:
    python -m scripts.fix_avito_data
После завершения запустите:
    python -m scripts.etl_normalize
"""

import asyncio
import logging
import re

import asyncpg

from config import DATABASE_DSN, setup_logging

logger = logging.getLogger(__name__)

_AREA_RE = re.compile(r"(\d+[,.]\d+|\d+)\s*м²")
_ROOMS_RE = re.compile(r"(\d+)-к\.")
_FLOOR_RE = re.compile(r"(\d+)/(\d+)\s*эт")


def _parse_title(title: str) -> tuple:
    rooms = area = floor = floors_total = None
    m = _ROOMS_RE.search(title)
    if m:
        rooms = int(m.group(1))
    elif "студия" in title.lower():
        rooms = 0
    m = _AREA_RE.search(title)
    if m:
        area = float(m.group(1).replace(",", "."))
    m = _FLOOR_RE.search(title)
    if m:
        floor = int(m.group(1))
        floors_total = int(m.group(2))
    return rooms, area, floor, floors_total


async def _cascade_delete_listings(
    conn: asyncpg.Connection, listing_ids: list[int]
) -> list[int]:
    """Удаляет listings + зависимые записи, возвращает список flat_id которые могут стать сиротами."""
    if not listing_ids:
        return []
    await conn.execute(
        "DELETE FROM deal_analysis WHERE listing_id = ANY($1::int[])", listing_ids
    )
    await conn.execute(
        "DELETE FROM listing_snapshots WHERE listing_id = ANY($1::int[])", listing_ids
    )
    await conn.execute(
        "UPDATE price_predictions SET listing_id = NULL WHERE listing_id = ANY($1::int[])",
        listing_ids,
    )
    flat_rows = await conn.fetch(
        "SELECT flat_id FROM listings WHERE id = ANY($1::int[])", listing_ids
    )
    flat_ids = [r["flat_id"] for r in flat_rows]
    await conn.execute("DELETE FROM listings WHERE id = ANY($1::int[])", listing_ids)
    return flat_ids


async def _delete_orphan_flats(conn: asyncpg.Connection, flat_ids: list[int]) -> int:
    """Удаляет флаты, у которых не осталось листингов."""
    if not flat_ids:
        return 0
    orphans = [
        r["id"]
        for r in await conn.fetch(
            """
            SELECT f.id FROM flats f
            WHERE f.id = ANY($1::int[])
              AND NOT EXISTS (SELECT 1 FROM listings l WHERE l.flat_id = f.id)
            """,
            flat_ids,
        )
    ]
    if orphans:
        await conn.execute("DELETE FROM flats WHERE id = ANY($1::int[])", orphans)
    return len(orphans)


async def remove_dolya_listings(conn: asyncpg.Connection) -> int:
    """Удаляет все авито-объявления с 'доля' в заголовке."""
    rows = await conn.fetch(
        """
        SELECT lr.id AS raw_id, l.id AS listing_id
        FROM listings_raw lr
        LEFT JOIN listings l ON l.raw_id = lr.id
        WHERE lr.source = 'avito'
          AND lower(lr.title) LIKE '%доля%'
        """
    )
    if not rows:
        logger.info("Долевых объявлений не найдено.")
        return 0

    raw_ids = [r["raw_id"] for r in rows]
    listing_ids = [r["listing_id"] for r in rows if r["listing_id"] is not None]

    flat_ids = await _cascade_delete_listings(conn, listing_ids)
    orphans = await _delete_orphan_flats(conn, flat_ids)
    await conn.execute("DELETE FROM listings_raw WHERE id = ANY($1::int[])", raw_ids)

    logger.info(
        f"Удалено долевых объявлений: {len(raw_ids)} "
        f"(листингов: {len(listing_ids)}, осиротевших flat: {orphans})"
    )
    return len(raw_ids)


async def fix_area_and_renormalize(conn: asyncpg.Connection) -> int:
    """Исправляет area_total для объявлений с неверно спарсенной площадью."""
    rows = await conn.fetch(
        """
        SELECT lr.id AS raw_id, lr.title,
               lr.area_total, lr.rooms, lr.floor, lr.floors_total,
               l.id AS listing_id
        FROM listings_raw lr
        LEFT JOIN listings l ON l.raw_id = lr.id
        WHERE lr.source = 'avito'
          AND lr.title IS NOT NULL
        """
    )

    to_fix: list[tuple] = []
    cascade_listing_ids: list[int] = []

    for row in rows:
        title = row["title"] or ""
        rooms, area, floor, floors_total = _parse_title(title)

        stored = float(row["area_total"]) if row["area_total"] is not None else None
        if area is None or stored is None or abs(area - stored) <= 0.05:
            continue

        to_fix.append((row["raw_id"], area, rooms, floor, floors_total))
        if row["listing_id"] is not None:
            cascade_listing_ids.append(row["listing_id"])

    if not to_fix:
        logger.info("Объявлений с неверной площадью не найдено.")
        return 0

    logger.info(f"Найдено объявлений с неверной площадью: {len(to_fix)}")

    flat_ids = await _cascade_delete_listings(conn, cascade_listing_ids)
    orphans = await _delete_orphan_flats(conn, flat_ids)
    logger.info(
        f"  Удалено нормализованных записей: {len(cascade_listing_ids)}, осиротевших flat: {orphans}"
    )

    for raw_id, area, rooms, floor, floors_total in to_fix:
        await conn.execute(
            """
            UPDATE listings_raw
            SET area_total   = $2,
                rooms        = $3,
                floor        = $4,
                floors_total = $5,
                normalized_at = NULL
            WHERE id = $1
            """,
            raw_id,
            area,
            rooms,
            floor,
            floors_total,
        )

    logger.info(
        f"Площадь исправлена и нормализация сброшена для {len(to_fix)} объявлений."
    )
    return len(to_fix)


async def run(dsn: str = DATABASE_DSN):
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            logger.info("=== Шаг 1: удаление долевых объявлений ===")
            dolya = await remove_dolya_listings(conn)

            logger.info("=== Шаг 2: исправление площади ===")
            fixed = await fix_area_and_renormalize(conn)

        logger.info(
            f"Готово. Удалено долевых: {dolya}. Исправлена площадь: {fixed}. "
            "Запустите 'python -m scripts.etl_normalize' для повторной нормализации."
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())
