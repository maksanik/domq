from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from schemas.listings import ListingItem, ListingsResponse

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("", response_model=ListingsResponse)
async def get_listings(
    request: Request,
    rooms: Optional[int] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    h3_index: Optional[str] = None,
    is_active: Optional[bool] = True,
    is_hot_deal: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
):
    """
    Список объявлений с фильтрами.
    Возвращает данные для отрисовки пинов на карте.
    """
    pool = request.app.state.pool

    conditions = ["1=1"]
    params: list = []
    i = 1

    if rooms is not None:
        conditions.append(f"f.rooms = ${i}")
        params.append(rooms)
        i += 1
    if min_price is not None:
        conditions.append(f"l.price >= ${i}")
        params.append(min_price)
        i += 1
    if max_price is not None:
        conditions.append(f"l.price <= ${i}")
        params.append(max_price)
        i += 1
    if h3_index is not None:
        conditions.append(f"b.h3_index = ${i}")
        params.append(h3_index)
        i += 1
    if is_active is not None:
        conditions.append(f"l.is_active = ${i}")
        params.append(is_active)
        i += 1
    if is_hot_deal is not None:
        conditions.append(f"da.is_hot_deal = ${i}")
        params.append(is_hot_deal)
        i += 1

    where = " AND ".join(conditions)

    base_query = f"""
        FROM listings l
        JOIN flats      f  ON l.flat_id     = f.id
        JOIN buildings  b  ON f.building_id = b.id
        JOIN listings_raw lr ON l.raw_id    = lr.id
        LEFT JOIN deal_analysis da ON da.listing_id = l.id
        WHERE {where}
    """

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) {base_query}", *params)

        rows = await conn.fetch(
            f"""
            SELECT
                l.id,
                lr.external_id,
                lr.url,
                l.price,
                l.price_per_m2,
                f.rooms,
                f.area_total,
                f.floor,
                f.floors_total,
                b.address,
                b.latitude,
                b.longitude,
                b.h3_index,
                l.is_active,
                da.is_hot_deal,
                da.discount_percent,
                l.first_seen_at,
                l.last_seen_at
            {base_query}
            ORDER BY l.id DESC
            LIMIT ${i} OFFSET ${i + 1}
            """,
            *params,
            limit,
            offset,
        )

    return ListingsResponse(
        total=total,
        items=[ListingItem(**dict(r)) for r in rows],
    )


@router.get("/{listing_id}", response_model=ListingItem)
async def get_listing(request: Request, listing_id: int):
    """Детальная карточка одного объявления."""
    pool = request.app.state.pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                l.id,
                lr.external_id,
                lr.url,
                l.price,
                l.price_per_m2,
                f.rooms,
                f.area_total,
                f.floor,
                f.floors_total,
                b.address,
                b.latitude,
                b.longitude,
                b.h3_index,
                l.is_active,
                da.is_hot_deal,
                da.discount_percent,
                l.first_seen_at,
                l.last_seen_at
            FROM listings l
            JOIN flats      f  ON l.flat_id     = f.id
            JOIN buildings  b  ON f.building_id = b.id
            JOIN listings_raw lr ON l.raw_id    = lr.id
            LEFT JOIN deal_analysis da ON da.listing_id = l.id
            WHERE l.id = $1
            """,
            listing_id,
        )

    if not row:
        raise HTTPException(status_code=404, detail="Listing not found")

    return ListingItem(**dict(row))
