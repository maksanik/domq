"""
Аналитические скрипты для вычисления агрегатов по H3-сотам.

Запуск после ETL-нормализации:
    python -m scripts.analytics

Выполняет:
  1. compute_price_stats()    — медиана и среднее цены м² по соте + комнатности
  2. compute_deal_analysis()  — флаг is_hot_deal для каждого активного объявления
  3. compute_price_history()  — ежедневный снапшот медианной цены (для графиков)
  4. compute_liquidity_stats() — средний срок экспозиции для снятых объявлений
"""

import asyncio
import logging

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from config import DATABASE_DSN, setup_logging

# Порог для флага «выгодное предложение», %
HOT_DEAL_THRESHOLD = 10.0

logger = logging.getLogger(__name__)


async def compute_price_stats(conn: PoolConnectionProxy | asyncpg.Connection):
    """
    Вычисляет медианную и среднюю цену м² по каждой (h3_index, rooms)-паре.
    Upsert: при повторном запуске обновляет существующие строки.
    """
    logger.info("Пересчёт price_stats...")
    await conn.execute(
        """
        INSERT INTO price_stats (h3_index, rooms, avg_price_per_m2,
                                 median_price_per_m2, listings_count, calculated_at)
        SELECT
            b.h3_index,
            f.rooms,
            ROUND(AVG(l.price_per_m2)::numeric, 2),
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price_per_m2)::numeric, 2),
            COUNT(*),
            NOW()
        FROM listings l
        JOIN flats    f ON l.flat_id    = f.id
        JOIN buildings b ON f.building_id = b.id
        WHERE l.is_active = true
          AND l.price_per_m2 IS NOT NULL
          AND b.h3_index IS NOT NULL
          AND f.rooms IS NOT NULL
        GROUP BY b.h3_index, f.rooms
        ON CONFLICT (h3_index, rooms) DO UPDATE SET
            avg_price_per_m2    = EXCLUDED.avg_price_per_m2,
            median_price_per_m2 = EXCLUDED.median_price_per_m2,
            listings_count      = EXCLUDED.listings_count,
            calculated_at       = EXCLUDED.calculated_at
        """
    )
    count = await conn.fetchval("SELECT COUNT(*) FROM price_stats")
    logger.info(f"price_stats: {count} записей")


async def compute_deal_analysis(
    conn: PoolConnectionProxy | asyncpg.Connection,
    threshold: float = HOT_DEAL_THRESHOLD,
):
    """
    Для каждого активного объявления сравнивает price_per_m2 с медианой соты.
    is_hot_deal = True, если скидка > threshold%.
    Upsert по listing_id.
    """
    logger.info("Пересчёт deal_analysis...")
    await conn.execute(
        """
        INSERT INTO deal_analysis (listing_id, median_price_per_m2, actual_price_per_m2,
                                   discount_percent, is_hot_deal, calculated_at)
        SELECT
            l.id                             AS listing_id,
            ps.median_price_per_m2,
            l.price_per_m2                   AS actual_price_per_m2,
            ROUND(
                ((ps.median_price_per_m2 - l.price_per_m2) / ps.median_price_per_m2 * 100)::numeric,
                2
            )                                AS discount_percent,
            ((ps.median_price_per_m2 - l.price_per_m2) / ps.median_price_per_m2 * 100) > $1
                                             AS is_hot_deal,
            NOW()
        FROM listings l
        JOIN flats     f  ON l.flat_id     = f.id
        JOIN buildings b  ON f.building_id = b.id
        JOIN price_stats ps ON ps.h3_index = b.h3_index AND ps.rooms = f.rooms
        WHERE l.is_active = true
          AND l.price_per_m2 IS NOT NULL
          AND ps.median_price_per_m2 IS NOT NULL
        ON CONFLICT (listing_id) DO UPDATE SET
            median_price_per_m2 = EXCLUDED.median_price_per_m2,
            actual_price_per_m2 = EXCLUDED.actual_price_per_m2,
            discount_percent    = EXCLUDED.discount_percent,
            is_hot_deal         = EXCLUDED.is_hot_deal,
            calculated_at       = EXCLUDED.calculated_at
        """,
        threshold,
    )
    hot = await conn.fetchval(
        "SELECT COUNT(*) FROM deal_analysis WHERE is_hot_deal = true"
    )
    total = await conn.fetchval("SELECT COUNT(*) FROM deal_analysis")
    logger.info(f"deal_analysis: {total} записей, из них hot deals: {hot}")


async def compute_price_history(conn: PoolConnectionProxy | asyncpg.Connection):
    """
    Ежедневный снапшот медианной цены м² по (h3_index, rooms, date).
    Вставляет только сегодняшний день — для пересчёта прошлых дат запустите вручную.
    """
    logger.info("Пересчёт price_history (сегодня)...")
    await conn.execute(
        """
        INSERT INTO price_history (h3_index, rooms, date, median_price_per_m2)
        SELECT
            b.h3_index,
            f.rooms,
            CURRENT_DATE,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price_per_m2)::numeric, 2)
        FROM listings l
        JOIN flats     f ON l.flat_id     = f.id
        JOIN buildings b ON f.building_id = b.id
        WHERE l.is_active = true
          AND l.price_per_m2 IS NOT NULL
          AND b.h3_index IS NOT NULL
          AND f.rooms IS NOT NULL
        GROUP BY b.h3_index, f.rooms
        ON CONFLICT (h3_index, rooms, date) DO UPDATE SET
            median_price_per_m2 = EXCLUDED.median_price_per_m2
        """
    )
    logger.info("price_history: снапшот за сегодня обновлён")


async def compute_liquidity_stats(conn: PoolConnectionProxy | asyncpg.Connection):
    """
    Вычисляет средний и медианный срок экспозиции для снятых объявлений.
    Работает только для объявлений, где is_active = false (сняты с публикации).
    Требует предварительного прохода rescan_scrapper.
    """
    logger.info("Пересчёт liquidity_stats...")
    await conn.execute(
        """
        INSERT INTO liquidity_stats (h3_index, rooms, avg_days_on_market,
                                     median_days, calculated_at)
        SELECT
            b.h3_index,
            f.rooms,
            ROUND(AVG(
                EXTRACT(EPOCH FROM (l.last_seen_at - l.first_seen_at)) / 86400
            ))::int,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (l.last_seen_at - l.first_seen_at)) / 86400
            )::int,
            NOW()
        FROM listings l
        JOIN flats     f ON l.flat_id     = f.id
        JOIN buildings b ON f.building_id = b.id
        WHERE l.is_active = false
          AND l.first_seen_at IS NOT NULL
          AND l.last_seen_at  IS NOT NULL
          AND l.last_seen_at > l.first_seen_at
          AND b.h3_index IS NOT NULL
          AND f.rooms IS NOT NULL
        GROUP BY b.h3_index, f.rooms
        HAVING COUNT(*) >= 3
        ON CONFLICT (h3_index, rooms) DO UPDATE SET
            avg_days_on_market = EXCLUDED.avg_days_on_market,
            median_days        = EXCLUDED.median_days,
            calculated_at      = EXCLUDED.calculated_at
        """
    )
    count = await conn.fetchval("SELECT COUNT(*) FROM liquidity_stats")
    logger.info(f"liquidity_stats: {count} записей")


async def run(dsn: str = DATABASE_DSN):
    logger.info("Запуск аналитических скриптов")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)

    try:
        async with pool.acquire() as conn:
            await compute_price_stats(conn)
            await compute_deal_analysis(conn)
            await compute_price_history(conn)
            await compute_liquidity_stats(conn)
        logger.info("Все аналитики пересчитаны")
    finally:
        await pool.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())
