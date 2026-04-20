from unittest.mock import AsyncMock, patch

import pytest

from cian.range_scrapper import RangeScraper


@pytest.fixture
def scraper():
    return RangeScraper(user_data_dir="/tmp/test")


@pytest.fixture
def cian_page():
    page = AsyncMock()
    page.select_price_range = AsyncMock()
    page.get_listings_count = AsyncMock()
    page.select_room_count = AsyncMock()
    return page


# --- _find_next_price_chunk ---


async def test_find_chunk_within_limit_returns_full_range(scraper, cian_page):
    cian_page.get_listings_count.return_value = 500
    result = await scraper._find_next_price_chunk(cian_page, 0, 10_000_000)
    assert result["current_max"] == 10_000_000
    assert result["listings_count"] == 500
    assert cian_page.get_listings_count.call_count == 1


async def test_find_chunk_exactly_at_limit_returns_full_range(scraper, cian_page):
    cian_page.get_listings_count.return_value = 1000
    result = await scraper._find_next_price_chunk(cian_page, 0, 5_000_000)
    assert result["current_max"] == 5_000_000
    assert result["listings_count"] == 1000


async def test_find_chunk_binary_search_narrows_range(scraper, cian_page):
    call_n = [0]

    def counts():
        n = call_n[0]
        call_n[0] += 1
        if n == 0:
            return 3000  # full range: too many listings
        return 850  # binary search mid: fits

    cian_page.get_listings_count.side_effect = counts

    result = await scraper._find_next_price_chunk(cian_page, 0, 10_000_000)

    assert result["current_max"] < 10_000_000
    assert result["listings_count"] == 850
    # More than one call: initial + at least one search step
    assert cian_page.get_listings_count.call_count >= 2


async def test_find_chunk_zero_listings_returns_empty_range(scraper, cian_page):
    cian_page.get_listings_count.return_value = 0
    result = await scraper._find_next_price_chunk(cian_page, 0, 1_000_000)
    assert result["listings_count"] == 0


# --- load_price_ranges ---


async def test_load_price_ranges_returns_early_on_room_count_failure(
    scraper, cian_page
):
    cian_page.select_room_count.return_value = False
    await scraper.load_price_ranges(cian_page, rooms_number=3)
    cian_page.select_price_range.assert_not_called()
    cian_page.get_listings_count.assert_not_called()


async def test_load_price_ranges_skips_api_when_cache_hit(scraper, cian_page):
    cian_page.select_room_count.return_value = True

    mock_db = AsyncMock()
    # Two cache hits to advance current_min past ABSOLUTE_MAX_PRICE (500_000_000)
    mock_db.get_max_price.side_effect = [5_000_000, 499_999_999]

    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_db)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("cian.range_scrapper.DatabaseManager", return_value=mock_cm):
        await scraper.load_price_ranges(cian_page, rooms_number=2)

    cian_page.select_price_range.assert_not_called()
    cian_page.get_listings_count.assert_not_called()
    assert mock_db.get_max_price.call_count == 2
