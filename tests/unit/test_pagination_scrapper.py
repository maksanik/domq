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
    result = scraper._extract_listings([FULL_OFFER], rooms_number=2)
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
    result = scraper._extract_listings([FULL_OFFER], rooms_number=2)
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
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["address_text"] == "Москва, ул. Ленина"


def test_extract_date_z_suffix_parsed_as_utc(scraper):
    result = scraper._extract_listings([FULL_OFFER], rooms_number=2)
    dt = result[0]["created_at"]
    assert isinstance(dt, datetime)
    assert dt == datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_extract_invalid_date_yields_none(scraper):
    offer = {**FULL_OFFER, "creationDate": "not-a-date"}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["created_at"] is None


def test_extract_missing_date_yields_none(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "creationDate"}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["created_at"] is None


def test_extract_no_photos(scraper):
    offer = {**FULL_OFFER, "photos": []}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["images_count"] == 0
    assert result[0]["has_photos"] is False


def test_extract_missing_geo_gives_none_coords_and_empty_address(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "geo"}
    result = scraper._extract_listings([offer], rooms_number=2)
    r = result[0]
    assert r["latitude"] is None
    assert r["longitude"] is None
    assert r["address_text"] == ""


def test_extract_missing_building_fields_are_none(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "building"}
    result = scraper._extract_listings([offer], rooms_number=2)
    r = result[0]
    assert r["floors_total"] is None
    assert r["year_built"] is None
    assert r["material_type"] is None


def test_extract_malformed_offer_skipped_valid_one_kept(scraper):
    bad_offer = {"not_id": "oops"}
    result = scraper._extract_listings([bad_offer, FULL_OFFER], rooms_number=2)
    assert len(result) == 1
    assert result[0]["external_id"] == "123456"


def test_extract_empty_offers_list(scraper):
    assert scraper._extract_listings([], rooms_number=2) == []


def test_extract_multiple_offers(scraper):
    offer2 = {**FULL_OFFER, "id": 999, "bargainTerms": {"price": 7_000_000}}
    result = scraper._extract_listings([FULL_OFFER, offer2], rooms_number=2)
    assert len(result) == 2
    assert result[1]["external_id"] == "999"
    assert result[1]["price"] == 7_000_000


# --- студии ---


def test_studio_rooms_count_null_falls_back_to_chunk_rooms_number(scraper):
    """Cian возвращает roomsCount=null для студий — должны получить 0 из чанка."""
    offer = {**FULL_OFFER, "id": 1, "roomsCount": None}
    result = scraper._extract_listings([offer], rooms_number=0)
    assert result[0]["rooms"] == 0


def test_studio_rooms_count_explicit_zero_stays_zero(scraper):
    """Если API вернул roomsCount=0, значение должно остаться 0."""
    offer = {**FULL_OFFER, "id": 2, "roomsCount": 0}
    result = scraper._extract_listings([offer], rooms_number=0)
    assert result[0]["rooms"] == 0


def test_non_studio_rooms_count_null_falls_back_to_chunk_rooms_number(scraper):
    """Если roomsCount=null в 3-комнатном чанке, берём rooms_number=3."""
    offer = {**FULL_OFFER, "id": 3, "roomsCount": None}
    result = scraper._extract_listings([offer], rooms_number=3)
    assert result[0]["rooms"] == 3


def test_rooms_count_present_not_overridden_by_chunk(scraper):
    """Когда roomsCount есть в ответе, rooms_number из чанка не влияет."""
    offer = {**FULL_OFFER, "id": 4, "roomsCount": 2}
    result = scraper._extract_listings([offer], rooms_number=0)
    assert result[0]["rooms"] == 2


# --- фильтрация долей ---


def test_extract_flatShareSale_excluded(scraper):
    offer = {**FULL_OFFER, "id": 10, "category": "flatShareSale"}
    assert scraper._extract_listings([offer], rooms_number=2) == []


def test_extract_shareAmount_excluded(scraper):
    offer = {**FULL_OFFER, "id": 11, "shareAmount": 500_000}
    assert scraper._extract_listings([offer], rooms_number=2) == []


def test_extract_flatShareSale_excludes_only_itself(scraper):
    share = {**FULL_OFFER, "id": 10, "category": "flatShareSale"}
    result = scraper._extract_listings([share, FULL_OFFER], rooms_number=2)
    assert len(result) == 1
    assert result[0]["external_id"] == "123456"


# --- price: priceRur vs price ---


def test_extract_price_priceRur_takes_precedence(scraper):
    offer = {
        **FULL_OFFER,
        "id": 20,
        "bargainTerms": {"priceRur": 6_000_000, "price": 5_000_000},
    }
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["price"] == 6_000_000


def test_extract_price_falls_back_to_price_when_no_priceRur(scraper):
    offer = {**FULL_OFFER, "id": 21, "bargainTerms": {"price": 4_500_000}}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["price"] == 4_500_000


def test_extract_price_none_when_bargainTerms_absent(scraper):
    offer = {k: v for k, v in FULL_OFFER.items() if k != "bargainTerms"}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["price"] is None


# --- фото ---


def test_extract_thumbnail_url_from_first_photo(scraper):
    offer = {
        **FULL_OFFER,
        "id": 30,
        "photos": [
            {"thumbnailUrl": "http://thumb1", "thumbnail2Url": "http://t2_1"},
            {"thumbnailUrl": "http://thumb2", "thumbnail2Url": "http://t2_2"},
        ],
    }
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["thumbnail_url"] == "http://thumb1"


def test_extract_photos_json_only_contains_thumbnail2Url(scraper):
    offer = {
        **FULL_OFFER,
        "id": 31,
        "photos": [
            {"thumbnailUrl": "http://thumb1", "thumbnail2Url": "http://t2_1"},
            {"thumbnailUrl": "http://thumb2"},
            {"thumbnail2Url": "http://t2_3"},
        ],
    }
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["photos_json"] == ["http://t2_1", "http://t2_3"]


def test_extract_photos_json_none_when_no_thumbnail2Url(scraper):
    offer = {**FULL_OFFER, "id": 32, "photos": [{"url": "http://raw"}]}
    result = scraper._extract_listings([offer], rooms_number=2)
    assert result[0]["photos_json"] is None
