from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from schemas.stats import (
    H3DetailResponse,
    H3MapItem,
    H3StatItem,
    LiquidityItem,
    PriceHistoryPoint,
)

router = APIRouter(prefix="/h3-stats", tags=["h3-stats"])


@router.get("/map", response_model=list[H3MapItem])
async def get_map_stats(
    request: Request,
    rooms: Optional[int] = None,
):
    """
    Все H3-соты с медианной ценой м².
    Используется фронтендом для отрисовки heatmap-слоя на карте.
    При rooms=null агрегирует все комнатности в одну строку на соту
    (взвешенное среднее медиан по listings_count).
    """
    pool = request.app.state.pool

    if rooms is not None:
        query = """
            SELECT h3_index, rooms, median_price_per_m2, listings_count
            FROM price_stats
            WHERE rooms = $1
        """
        params: list = [rooms]
    else:
        query = """
            SELECT
                h3_index,
                NULL::int AS rooms,
                SUM(median_price_per_m2 * listings_count)
                    / NULLIF(SUM(listings_count), 0) AS median_price_per_m2,
                SUM(listings_count) AS listings_count
            FROM price_stats
            GROUP BY h3_index
        """
        params = []

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [H3MapItem(**dict(r)) for r in rows]


@router.get("", response_model=H3DetailResponse)
async def get_cell_stats(
    request: Request,
    h3_index: str,
    rooms: Optional[int] = None,
):
    """
    Детальная аналитика по конкретной H3-соте:
    - медианная цена м²
    - срок ликвидности
    - история цен (для графика)
    """
    pool = request.app.state.pool

    async with pool.acquire() as conn:
        if rooms is not None:
            ps_row = await conn.fetchrow(
                """
                SELECT h3_index, rooms, avg_price_per_m2, median_price_per_m2,
                       listings_count, calculated_at
                FROM price_stats
                WHERE h3_index = $1 AND rooms = $2
                LIMIT 1
                """,
                h3_index,
                rooms,
            )
            lq_row = await conn.fetchrow(
                """
                SELECT avg_days_on_market, median_days
                FROM liquidity_stats
                WHERE h3_index = $1 AND rooms = $2
                LIMIT 1
                """,
                h3_index,
                rooms,
            )
            ph_rows = await conn.fetch(
                """
                SELECT date, median_price_per_m2
                FROM price_history
                WHERE h3_index = $1 AND rooms = $2
                ORDER BY date DESC
                LIMIT 90
                """,
                h3_index,
                rooms,
            )
        else:
            # Агрегат по всем комнатностям: взвешенное среднее медиан
            ps_row = await conn.fetchrow(
                """
                SELECT
                    $1::text AS h3_index,
                    NULL::int AS rooms,
                    SUM(avg_price_per_m2 * listings_count)
                        / NULLIF(SUM(listings_count), 0) AS avg_price_per_m2,
                    SUM(median_price_per_m2 * listings_count)
                        / NULLIF(SUM(listings_count), 0) AS median_price_per_m2,
                    SUM(listings_count) AS listings_count,
                    MAX(calculated_at) AS calculated_at
                FROM price_stats
                WHERE h3_index = $1
                """,
                h3_index,
            )
            lq_row = await conn.fetchrow(
                """
                SELECT
                    ROUND(AVG(avg_days_on_market))::int AS avg_days_on_market,
                    ROUND(AVG(median_days))::int AS median_days
                FROM liquidity_stats
                WHERE h3_index = $1
                """,
                h3_index,
            )
            ph_rows = await conn.fetch(
                """
                SELECT
                    date,
                    AVG(median_price_per_m2) AS median_price_per_m2
                FROM price_history
                WHERE h3_index = $1
                GROUP BY date
                ORDER BY date DESC
                LIMIT 90
                """,
                h3_index,
            )

    if not ps_row and not ph_rows:
        raise HTTPException(status_code=404, detail="H3 cell not found in stats")

    return H3DetailResponse(
        h3_index=h3_index,
        rooms=rooms,
        price_stats=H3StatItem(**dict(ps_row)) if ps_row else None,
        liquidity=LiquidityItem(**dict(lq_row)) if lq_row else None,
        price_history=[PriceHistoryPoint(**dict(r)) for r in ph_rows],
    )
