from unittest.mock import AsyncMock

import pytest

from scripts.etl_normalize import (
    AREA_TOLERANCE,
    H3_R11_RESOLUTION,
    H3_RESOLUTION,
    get_or_create_building,
    get_or_create_flat,
    upsert_listing,
)


def test_h3_resolution_constant():
    assert H3_RESOLUTION == 9


def test_h3_r11_resolution_constant():
    assert H3_R11_RESOLUTION == 11


def test_area_tolerance_constant():
    assert AREA_TOLERANCE == 0.5


@pytest.fixture
def conn():
    c = AsyncMock()
    c.execute.return_value = None
    c.fetchrow.return_value = None
    c.fetchval.return_value = None
    return c


# --- get_or_create_building ---


async def test_building_returns_none_when_lat_is_none(conn):
    raw = {"latitude": None, "longitude": 37.62}
    result = await get_or_create_building(conn, raw)
    assert result is None
    conn.execute.assert_not_called()
    conn.fetchrow.assert_not_called()


async def test_building_returns_none_when_lon_is_none(conn):
    raw = {"latitude": 55.75, "longitude": None}
    result = await get_or_create_building(conn, raw)
    assert result is None
    conn.execute.assert_not_called()


async def test_building_calls_db_and_returns_id(conn):
    conn.fetchrow.return_value = {"id": 42}
    raw = {
        "latitude": 55.75,
        "longitude": 37.62,
        "address_text": "Москва",
        "year_built": 2005,
        "floors_total": 17,
        "material_type": "panel",
    }
    result = await get_or_create_building(conn, raw)
    assert result == 42
    # upsert_h3_cells sends both cells via a single executemany call
    conn.executemany.assert_called_once()
    cells_arg = conn.executemany.call_args.args[1]
    assert len(cells_arg) == 2
    conn.fetchrow.assert_called_once()


async def test_building_returns_none_when_fetchrow_returns_none(conn):
    conn.fetchrow.return_value = None
    raw = {
        "latitude": 55.75,
        "longitude": 37.62,
        "address_text": None,
        "year_built": None,
        "floors_total": None,
        "material_type": None,
    }
    result = await get_or_create_building(conn, raw)
    assert result is None


# --- upsert_listing ---


async def test_upsert_new_listing_inserts(conn):
    conn.fetchrow.return_value = None
    raw = {"price": 5_000_000, "area_total": 50.0, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=1, flat_id=10, raw=raw)
    conn.execute.assert_called_once()


async def test_upsert_price_per_m2_calculated_correctly(conn):
    conn.fetchrow.return_value = None
    raw = {"price": 5_000_000, "area_total": 50.0, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=1, flat_id=10, raw=raw)
    # INSERT args order: sql, raw_id, flat_id, price, price_per_m2, is_active, parsed_at
    args = conn.execute.call_args.args
    assert args[4] == pytest.approx(100_000.0)


async def test_upsert_price_per_m2_none_when_price_missing(conn):
    conn.fetchrow.return_value = None
    raw = {"price": None, "area_total": 50.0, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=1, flat_id=10, raw=raw)
    args = conn.execute.call_args.args
    assert args[4] is None


async def test_upsert_price_per_m2_none_when_area_missing(conn):
    conn.fetchrow.return_value = None
    raw = {"price": 5_000_000, "area_total": None, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=1, flat_id=10, raw=raw)
    args = conn.execute.call_args.args
    assert args[4] is None


async def test_upsert_existing_listing_price_changed_creates_snapshot(conn):
    conn.fetchrow.return_value = {"id": 99, "price": 4_500_000}
    raw = {"price": 5_000_000, "area_total": 50.0, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=5, flat_id=2, raw=raw)
    # snapshot INSERT + UPDATE = 2 execute calls
    assert conn.execute.call_count == 2


async def test_upsert_existing_listing_same_price_no_snapshot(conn):
    conn.fetchrow.return_value = {"id": 99, "price": 5_000_000}
    raw = {"price": 5_000_000, "area_total": 50.0, "parsed_at": None, "is_active": True}
    await upsert_listing(conn, raw_id=5, flat_id=2, raw=raw)
    # Only UPDATE, no snapshot
    assert conn.execute.call_count == 1


async def test_upsert_inactive_listing_creates_offline_snapshot(conn):
    conn.fetchrow.return_value = {"id": 99, "price": 5_000_000}
    raw = {
        "price": 5_000_000,
        "area_total": 50.0,
        "parsed_at": None,
        "is_active": False,
    }
    await upsert_listing(conn, raw_id=5, flat_id=2, raw=raw)
    # offline snapshot INSERT + UPDATE = 2 execute calls
    assert conn.execute.call_count == 2


# --- get_or_create_flat ---

RAW_FLAT = {
    "source": "cian",
    "external_id": "abc",
    "area_total": 50.0,
    "area_kitchen": None,
    "rooms": 2,
    "floor": 3,
}


async def test_flat_reuses_flat_id_when_external_id_already_listed(conn):
    """Step 1: external_id уже есть в listings — возвращаем flat_id без INSERT."""
    conn.fetchrow.return_value = {"flat_id": 42}
    result = await get_or_create_flat(conn, building_id=1, raw=RAW_FLAT)
    assert result == 42
    conn.execute.assert_not_called()


async def test_flat_matches_by_building_rooms_floor_area(conn):
    """Step 2: совпадение по параметрам — возвращаем id без INSERT."""
    conn.fetchrow.side_effect = [None, {"id": 7}]
    result = await get_or_create_flat(conn, building_id=5, raw=RAW_FLAT)
    assert result == 7
    conn.execute.assert_not_called()


async def test_flat_updates_kitchen_area_on_param_match(conn):
    """Step 2: при совпадении и наличии area_kitchen в raw — UPDATE."""
    conn.fetchrow.side_effect = [None, {"id": 7}]
    raw = {**RAW_FLAT, "area_kitchen": 10.5}
    await get_or_create_flat(conn, building_id=5, raw=raw)
    conn.execute.assert_called_once()


async def test_flat_no_kitchen_update_when_raw_kitchen_is_none(conn):
    """Step 2: при совпадении без area_kitchen — execute не вызывается."""
    conn.fetchrow.side_effect = [None, {"id": 7}]
    await get_or_create_flat(conn, building_id=5, raw=RAW_FLAT)
    conn.execute.assert_not_called()


async def test_flat_inserts_new_when_no_param_match(conn):
    """Step 3: нет совпадений — INSERT и возврат нового id."""
    conn.fetchrow.side_effect = [None, None, {"id": 99}]
    result = await get_or_create_flat(conn, building_id=5, raw=RAW_FLAT)
    assert result == 99


async def test_flat_raises_when_insert_returns_none(conn):
    """Step 3: INSERT вернул None — RuntimeError."""
    conn.fetchrow.side_effect = [None, None, None]
    with pytest.raises(RuntimeError):
        await get_or_create_flat(conn, building_id=5, raw=RAW_FLAT)


async def test_flat_skips_param_search_when_area_is_none(conn):
    """Без площади step 2 пропускается — сразу INSERT."""
    conn.fetchrow.side_effect = [None, {"id": 55}]
    raw = {**RAW_FLAT, "area_total": None}
    result = await get_or_create_flat(conn, building_id=5, raw=raw)
    assert result == 55
    assert conn.fetchrow.call_count == 2


async def test_flat_skips_param_search_when_rooms_is_none(conn):
    """Без rooms step 2 пропускается — сразу INSERT."""
    conn.fetchrow.side_effect = [None, {"id": 66}]
    raw = {**RAW_FLAT, "rooms": None}
    result = await get_or_create_flat(conn, building_id=5, raw=raw)
    assert result == 66
    assert conn.fetchrow.call_count == 2
