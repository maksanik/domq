from datetime import datetime, timezone

import pytest

from cian.pagination_scrapper import PaginationScraper

FULL_OFFER = {
    "id": 123456,
    "fullUrl": "https://cian.ru/sale/flat/123456/",
    "title": "2-комн. квартира, 50 м², 5/17 эт.",
    "description": "Продаётся квартира",
    "geo": {
        "address": [{"name": "Москва"}, {"name": "ул. Ленина, 1"}],
        "coordinates": {"lat": 55.75, "lng": 37.62},
    },
    "bargainTerms": {"price": 5_000_000},
    "totalArea": 50.0,
    "kitchenArea": 12.0,
    "roomsCount": 2,
    "floorNumber": 5,
    "building": {"floorsCount": 17, "buildYear": 2005, "materialType": "panel"},
    "photos": [{"url": "http://img1"}, {"url": "http://img2"}, {"url": "http://img3"}],
    "creationDate": "2024-03-15T10:00:00Z",
}


@pytest.fixture
def scraper():
    return PaginationScraper(user_data_dir="/tmp/test_profile")


def test_extract_full_offer_fields(scraper):
    result = scraper._extract_listings([FULL_OFFER])
    assert len(result) == 1
    r = result[0]
    assert r["source"] == "cian"
    assert r["external_id"] == "123456"
    assert r["url"] == "https://cian.ru/sale/flat/123456/"
    assert r["price"] == 5_000_000
    assert r["area_total"] == 50.0
    assert r["area_kitchen"] == 12.0
    assert r["rooms"] == 2
    assert r["floor"] == 5
    assert r["floors_total"] == 17
    assert r["latitude"] == 55.75
    assert r["longitude"] == 37.62
    assert r["year_built"] == 2005
    assert r["material_type"] == "panel"
    assert r["images_count"] == 3
    assert r["has_photos"] is True


def test_extract_address_assembly(scraper):
    result = scraper._extract_listings([FULL_OFFER])
    assert result[0]["address_text"] == "Москва, ул. Ленина, 1"


def test_extract_address_skips_parts_with_empty_name(scraper):
    offer = {
        **FULL_OFFER,
        "id": 2,
        "geo": {
            "address": [{"name": "Москва"}, {"name": ""}, {"name": "ул. Ленина"}],
            "coordinates": {"lat": 55.75, "lng": 37.62},
        },
    }
    result = scraper._extract_listings([offer])
    assert result[0]["address_text"] == "Москва, ул. Ленина"


def test_extract_date_z_suffix_parsed_as_utc(scraper):
    result = scraper._extract_listings([FULL_OFFER])
    dt = result[0]["created_at"]
    assert isinstance(dt, datetime)
    assert dt == datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_extract_invalid_date_yields_none(scraper):
    offer = {**FULL_OFFER, "creationDate": "not-a-date"}
    result = scraper._extract_listings([offer])
    assert result[0]["created_at"] is None


def test_extract_missing_date_yields_none(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "creationDate"}
    result = scraper._extract_listings([offer])
    assert result[0]["created_at"] is None


def test_extract_no_photos(scraper):
    offer = {**FULL_OFFER, "photos": []}
    result = scraper._extract_listings([offer])
    assert result[0]["images_count"] == 0
    assert result[0]["has_photos"] is False


def test_extract_missing_geo_gives_none_coords_and_empty_address(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "geo"}
    result = scraper._extract_listings([offer])
    r = result[0]
    assert r["latitude"] is None
    assert r["longitude"] is None
    assert r["address_text"] == ""


def test_extract_missing_building_fields_are_none(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "building"}
    result = scraper._extract_listings([offer])
    r = result[0]
    assert r["floors_total"] is None
    assert r["year_built"] is None
    assert r["material_type"] is None


def test_extract_malformed_offer_skipped_valid_one_kept(scraper):
    bad_offer = {"not_id": "oops"}
    result = scraper._extract_listings([bad_offer, FULL_OFFER])
    assert len(result) == 1
    assert result[0]["external_id"] == "123456"


def test_extract_empty_offers_list(scraper):
    assert scraper._extract_listings([]) == []


def test_extract_multiple_offers(scraper):
    offer2 = {**FULL_OFFER, "id": 999, "bargainTerms": {"price": 7_000_000}}
    result = scraper._extract_listings([FULL_OFFER, offer2])
    assert len(result) == 2
    assert result[1]["external_id"] == "999"
    assert result[1]["price"] == 7_000_000
