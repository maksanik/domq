from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from schemas.listings import (
    BuildingPin,
    BuildingPinsResponse,
    ListingItem,
    ListingSource,
    ListingsResponse,
)

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("/buildings", response_model=BuildingPinsResponse)
async def get_building_pins(
    request: Request,
    rooms: Optional[int] = None,
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lng: Optional[float] = None,
):
    """Здания с количеством активных объявлений для маркеров на карте.
    Дедупликация по округлённым координатам, фильтр по bounding box вьюпорта.
    """
    pool = request.app.state.pool

    conditions = [
        "l.is_active = true",
        "b.latitude IS NOT NULL",
        "b.longitude IS NOT NULL",
    ]
    params: list = []
    i = 1

    if rooms is not None:
        conditions.append(f"f.rooms = ${i}")
        params.append(rooms)
        i += 1
    if min_lat is not None:
        conditions.append(f"b.latitude >= ${i}")
        params.append(min_lat)
        i += 1
    if max_lat is not None:
        conditions.append(f"b.latitude <= ${i}")
        params.append(max_lat)
        i += 1
    if min_lng is not None:
        conditions.append(f"b.longitude >= ${i}")
        params.append(min_lng)
        i += 1
    if max_lng is not None:
        conditions.append(f"b.longitude <= ${i}")
        params.append(max_lng)
        i += 1

    where = " AND ".join(conditions)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                MIN(b.id)                               AS building_id,
                MIN(b.address)                          AS address,
                ROUND(b.latitude::numeric, 5)::float    AS latitude,
                ROUND(b.longitude::numeric, 5)::float   AS longitude,
                MAX(b.h3_index)                         AS h3_index,
                COUNT(*)                                AS listings_count
            FROM listings l
            JOIN flats f ON l.flat_id = f.id
            JOIN buildings b ON f.building_id = b.id
            WHERE {where}
            GROUP BY ROUND(b.latitude::numeric, 5), ROUND(b.longitude::numeric, 5)
            ORDER BY listings_count DESC
            """,
            *params,
        )

    return BuildingPinsResponse(items=[BuildingPin(**dict(r)) for r in rows])


_SORT_FIELD_MAP = {
    "id": "id",
    "price": "price",
    "price_per_m2": "price_per_m2",
    "discount_percent": "discount_percent",
    "area_total": "area_total",
}


@router.get("", response_model=ListingsResponse)
async def get_listings(
    request: Request,
    rooms: Optional[List[int]] = Query(default=None),
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_area: Optional[float] = None,
    max_area: Optional[float] = None,
    h3_index: Optional[str] = None,
    building_id: Optional[int] = None,
    is_active: Optional[bool] = True,
    is_hot_deal: Optional[bool] = None,
    sort_by: str = "id",
    sort_order: str = "desc",
    limit: int = 100,
    offset: int = 0,
):
    """
    Список объявлений с фильтрами.
    Возвращает данные для отрисовки пинов на карте.
    """
    pool = request.app.state.pool

    sort_field = _SORT_FIELD_MAP.get(sort_by, "l.id")
    sort_dir = "DESC" if sort_order.lower() != "asc" else "ASC"

    conditions = ["1=1"]
    params: list = []
    i = 1

    if rooms:
        placeholders = ", ".join(f"${i + j}" for j in range(len(rooms)))
        conditions.append(f"f.rooms IN ({placeholders})")
        params.extend(rooms)
        i += len(rooms)
    if min_price is not None:
        conditions.append(f"l.price >= ${i}")
        params.append(min_price)
        i += 1
    if max_price is not None:
        conditions.append(f"l.price <= ${i}")
        params.append(max_price)
        i += 1
    if min_area is not None:
        conditions.append(f"f.area_total >= ${i}")
        params.append(min_area)
        i += 1
    if max_area is not None:
        conditions.append(f"f.area_total <= ${i}")
        params.append(max_area)
        i += 1
    if h3_index is not None:
        conditions.append(f"b.h3_index = ${i}")
        params.append(h3_index)
        i += 1
    if building_id is not None:
        conditions.append(f"b.id = ${i}")
        params.append(building_id)
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

    # Выбираем по одному листингу на квартиру (flat_id) — с наименьшей ценой,
    # чтобы одна квартира с нескольких источников не дублировалась в списке.
    inner_query = f"""
        SELECT DISTINCT ON (l.flat_id)
            l.id,
            lr.source,
            lr.external_id,
            lr.url,
            l.price,
            l.price_per_m2,
            f.rooms,
            f.area_total,
            f.floor,
            b.floors_total,
            b.address,
            b.latitude,
            b.longitude,
            b.h3_index,
            l.is_active,
            da.is_hot_deal,
            da.discount_percent,
            l.first_seen_at,
            l.last_seen_at,
            lr.thumbnail_url,
            lr.photos_json AS photos
        FROM listings l
        JOIN flats      f  ON l.flat_id     = f.id
        JOIN buildings  b  ON f.building_id = b.id
        JOIN listings_raw lr ON l.raw_id    = lr.id
        LEFT JOIN deal_analysis da ON da.listing_id = l.id
        WHERE {where}
        ORDER BY l.flat_id, l.price ASC NULLS LAST
    """

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM ({inner_query}) _cnt",
            *params,
        )

        rows = await conn.fetch(
            f"""
            SELECT * FROM ({inner_query}) _best
            ORDER BY {sort_field} {sort_dir} NULLS LAST
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
    """Детальная карточка одного объявления со всеми источниками для этой квартиры."""
    pool = request.app.state.pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                l.id,
                l.flat_id,
                lr.source,
                lr.external_id,
                lr.url,
                l.price,
                l.price_per_m2,
                f.rooms,
                f.area_total,
                f.floor,
                b.floors_total,
                b.address,
                b.latitude,
                b.longitude,
                b.h3_index,
                l.is_active,
                da.is_hot_deal,
                da.discount_percent,
                l.first_seen_at,
                l.last_seen_at,
                lr.thumbnail_url,
                lr.photos_json AS photos
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

    async with pool.acquire() as conn:
        source_rows = await conn.fetch(
            """
            SELECT lr.source, lr.url, lr.external_id, l.price
            FROM listings l
            JOIN listings_raw lr ON l.raw_id = lr.id
            WHERE l.flat_id = $1 AND l.is_active = true
            ORDER BY l.price ASC NULLS LAST
            """,
            row["flat_id"],
        )

    data = dict(row)
    data["sources"] = [ListingSource(**dict(r)) for r in source_rows]
    return ListingItem(**data)
