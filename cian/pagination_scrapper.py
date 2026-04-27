import argparse
import asyncio
import json
import logging
import math
import random
from datetime import datetime, timezone

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from cian.browser import get_browser_context
from cian.pages.base_page import BasePage
from db.db_manager import DatabaseManager
from config import setup_logging, USER_DATA_DIR

API_URL = "https://api.cian.ru/search-offers/v2/search-offers-desktop/"
OFFERS_PER_PAGE = 28
MAX_PAGES = 54

# Задержки между запросами (секунды)
PAGE_DELAY_MIN = 2.0
PAGE_DELAY_MAX = 3.0
CHUNK_DELAY_MIN = 3.0
CHUNK_DELAY_MAX = 5.0

# Retry-параметры
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 30.0  # базовая пауза при 429/503 (секунды)


class PaginationScraper:
    """Скрапер для сбора объявлений постранично по чанкам цен."""

    def __init__(self, user_data_dir: str):
        self.user_data_dir = user_data_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def _extract_listings(self, offers: list, rooms_number: int) -> list[dict]:
        """Извлекает все доступные поля из офферов Cian API и сохраняет в listings_raw."""
        result = []
        for offer in offers:
            if (
                offer.get("category") == "flatShareSale"
                or offer.get("shareAmount") is not None
            ):
                continue
            try:
                geo = offer.get("geo", {})
                address_parts = geo.get("address", [])
                address = ", ".join(p["name"] for p in address_parts if p.get("name"))

                # Координаты могут быть в geo.coordinates или geo.jk
                coords = geo.get("coordinates") or {}
                lat = coords.get("lat")
                lng = coords.get("lng")

                building = offer.get("building") or {}
                photos = offer.get("photos") or []

                # Дата публикации на Циане
                created_raw = offer.get("creationDate")
                created_at = None
                if created_raw:
                    try:
                        created_at = datetime.fromisoformat(
                            created_raw.replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        pass

                thumbnail_url = photos[0].get("thumbnailUrl") if photos else None
                photos_urls = [
                    p["thumbnail2Url"] for p in photos if p.get("thumbnail2Url")
                ]

                result.append(
                    {
                        "source": "cian",
                        "external_id": str(offer["id"]),
                        "url": offer.get("fullUrl"),
                        "title": offer.get("title"),
                        "description": offer.get("description"),
                        "price": (offer.get("bargainTerms") or {}).get("priceRur")
                        or (offer.get("bargainTerms") or {}).get("price"),
                        "area_total": offer.get("totalArea"),
                        "area_kitchen": offer.get("kitchenArea"),
                        "rooms": offer.get("roomsCount")
                        if offer.get("roomsCount") is not None
                        else rooms_number,
                        "floor": offer.get("floorNumber"),
                        "floors_total": building.get("floorsCount"),
                        "latitude": lat,
                        "longitude": lng,
                        "address_text": address,
                        "year_built": building.get("buildYear"),
                        "material_type": building.get("materialType"),
                        "images_count": len(photos),
                        "has_photos": len(photos) > 0,
                        "thumbnail_url": thumbnail_url,
                        "photos_json": photos_urls or None,
                        "created_at": created_at,
                    }
                )
            except Exception as e:
                self.logger.warning(f"Ошибка разбора оффера {offer.get('id')}: {e}")
        return result

    async def _fetch_page(
        self,
        context,
        rooms_number: int,
        min_price: int,
        max_price: int,
        page_num: int,
    ) -> dict:
        """Делает запрос к API Циана и возвращает данные страницы."""
        # rooms_number=0 (студия) → Cian API ожидает значение 9
        ROOM_API_VALUE = {0: 9}
        room_api = ROOM_API_VALUE.get(rooms_number, rooms_number)

        payload = {
            "jsonQuery": {
                "_type": "flatsale",
                "engine_version": {"type": "term", "value": 2},
                "region": {"type": "terms", "value": [1]},
                "room": {"type": "terms", "value": [room_api]},
                "price": {
                    "type": "range",
                    "value": {"gte": min_price, "lte": max_price},
                },
                "page": {"type": "term", "value": page_num},
            },
            "_liquiditySource": "web_serp",
        }

        self.logger.info(
            f"Запрос страницы {page_num} "
            f"(комн: {rooms_number}, цена: {min_price}-{max_price})"
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://www.cian.ru",
            "Referer": "https://www.cian.ru/kupit-kvartiru/",
            "sec-fetch-site": "same-site",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            response = await context.request.post(
                API_URL,
                data=json.dumps(payload),
                headers=headers,
            )

            if response.status == 429 or response.status == 503:
                wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 10)
                self.logger.warning(
                    f"HTTP {response.status} на стр. {page_num}, "
                    f"попытка {attempt}/{MAX_RETRIES}, ждём {wait:.0f}с"
                )
                await asyncio.sleep(wait)
                continue

            if not response.ok:
                self.logger.error(f"HTTP {response.status} на странице {page_num}")
                return {}

            return await response.json()

        self.logger.error(f"Страница {page_num}: все {MAX_RETRIES} попытки исчерпаны")
        return {}

    async def _scrape_chunk(
        self,
        context,
        db: DatabaseManager,
        rooms_number: int,
        min_price: int,
        max_price: int,
        listings_count: int,
    ):
        """Скрапит все страницы одного чанка и сохраняет в listings_raw."""
        saved = 0
        total_pages = None

        for page_num in range(1, MAX_PAGES + 1):
            data = await self._fetch_page(
                context, rooms_number, min_price, max_price, page_num
            )

            # На первой странице определяем реальное число страниц из ответа API
            if page_num == 1:
                api_count = (data.get("data") or {}).get("offerCount")
                if api_count is not None:
                    total_pages = min(math.ceil(api_count / OFFERS_PER_PAGE), MAX_PAGES)
                else:
                    total_pages = min(
                        math.ceil(listings_count / OFFERS_PER_PAGE), MAX_PAGES
                    )
                self.logger.info(
                    f"Чанк {rooms_number}к {min_price}-{max_price}: "
                    f"{api_count or listings_count} объявлений, {total_pages} страниц"
                )

            offers = data.get("data", {}).get("offersSerialized", [])
            if not offers:
                self.logger.info(f"Страница {page_num}: нет данных, завершение чанка")
                break

            listings = self._extract_listings(offers, rooms_number)
            for listing in listings:
                await db.save_raw_listing(listing)
                saved += 1

            self.logger.info(
                f"Страница {page_num}/{total_pages}: получено {len(listings)} объявлений"
            )

            if total_pages is not None and page_num >= total_pages:
                break

            delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            self.logger.info(f"Пауза {delay:.1f}с перед следующей страницей")
            await asyncio.sleep(delay)

        self.logger.info(f"Чанк завершён: итого сохранено {saved} объявлений")
        await db.mark_chunk_scraped(rooms_number, min_price)

    async def run(
        self,
        rescrape: bool = False,
        captcha_event: asyncio.Event | None = None,
        chunks: list[dict] | None = None,
    ):
        self.logger.info("Запуск Pagination Scraper")

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("Запуск браузера")
            context = await get_browser_context(p, self.user_data_dir)
            page = context.pages[0] if context.pages else await context.new_page()
            base_page = BasePage(page)

            await page.goto("https://www.cian.ru/kupit-kvartiru/")
            if captcha_event:
                await captcha_event.wait()
            else:
                await base_page.wait_for_human_captcha()

            async with DatabaseManager() as db:
                if chunks is None:
                    if rescrape:
                        count = await db.reset_all_chunks()
                        self.logger.info(f"--rescrape: сброшено {count} чанков")
                    chunks = await db.get_unscraped_chunks()

                self.logger.info(f"Чанков для скрапинга: {len(chunks)}")

                for i, chunk in enumerate(chunks):
                    await self._scrape_chunk(
                        context,
                        db,
                        chunk["rooms_number"],
                        chunk["min_price"],
                        chunk["max_price"],
                        chunk["listings_count"],
                    )
                    if i < len(chunks) - 1:
                        delay = random.uniform(CHUNK_DELAY_MIN, CHUNK_DELAY_MAX)
                        self.logger.info(f"Пауза {delay:.0f}с между чанками")
                        await asyncio.sleep(delay)

                remaining = await db.get_unscraped_chunks()
                if remaining:
                    self.logger.info(
                        f"{len(remaining)} чанков не скрапировано — деактивация пропущена"
                    )
                else:
                    today_start = datetime.now(timezone.utc).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    deactivated = await db.deactivate_unseen_listings(
                        today_start, source="cian"
                    )
                    self.logger.info(
                        f"Деактивировано пропавших объявлений: {deactivated}"
                    )

            self.logger.info("Скрапинг завершён")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")
            await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pagination scraper для Cian.ru")
    parser.add_argument(
        "--rescrape",
        action="store_true",
        help="Сбросить все чанки в 'не скрапировано' перед стартом",
    )
    args = parser.parse_args()

    setup_logging()
    scraper = PaginationScraper(user_data_dir=USER_DATA_DIR)
    asyncio.run(scraper.run(rescrape=args.rescrape))
