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
    """
    pool = request.app.state.pool

    query = """
        SELECT h3_index, rooms, median_price_per_m2, listings_count
        FROM price_stats
        WHERE 1=1
    """
    params: list = []

    if rooms is not None:
        query += " AND rooms = $1"
        params.append(rooms)

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
        # price_stats
        ps_row = await conn.fetchrow(
            """
            SELECT h3_index, rooms, avg_price_per_m2, median_price_per_m2,
                   listings_count, calculated_at
            FROM price_stats
            WHERE h3_index = $1 AND ($2::int IS NULL OR rooms = $2)
            LIMIT 1
            """,
            h3_index,
            rooms,
        )

        # liquidity_stats
        lq_row = await conn.fetchrow(
            """
            SELECT avg_days_on_market, median_days
            FROM liquidity_stats
            WHERE h3_index = $1 AND ($2::int IS NULL OR rooms = $2)
            LIMIT 1
            """,
            h3_index,
            rooms,
        )

        # price_history — последние 90 дней
        ph_rows = await conn.fetch(
            """
            SELECT date, median_price_per_m2
            FROM price_history
            WHERE h3_index = $1 AND ($2::int IS NULL OR rooms = $2)
            ORDER BY date DESC
            LIMIT 90
            """,
            h3_index,
            rooms,
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
