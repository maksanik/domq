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
PAGE_DELAY_MAX = 5.0
CHUNK_DELAY_MIN = 5.0
CHUNK_DELAY_MAX = 10.0

# Retry-параметры
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 30.0  # базовая пауза при 429/503 (секунды)


class PaginationScraper:
    """Скрапер для сбора объявлений постранично по чанкам цен."""

    def __init__(self, user_data_dir: str):
        self.user_data_dir = user_data_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def _extract_listings(self, offers: list) -> list[dict]:
        """Извлекает все доступные поля из офферов Cian API и сохраняет в listings_raw."""
        result = []
        for offer in offers:
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

                result.append(
                    {
                        "source": "cian",
                        "external_id": str(offer["id"]),
                        "url": offer.get("fullUrl"),
                        "title": offer.get("title"),
                        "description": offer.get("description"),
                        "price": offer.get("bargainTerms", {}).get("price"),
                        "area_total": offer.get("totalArea"),
                        "area_kitchen": offer.get("kitchenArea"),
                        "rooms": offer.get("roomsCount"),
                        "floor": offer.get("floorNumber"),
                        "floors_total": building.get("floorsCount"),
                        "latitude": lat,
                        "longitude": lng,
                        "address_text": address,
                        "year_built": building.get("buildYear"),
                        "material_type": building.get("materialType"),
                        "images_count": len(photos),
                        "has_photos": len(photos) > 0,
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
        payload = {
            "jsonQuery": {
                "_type": "flatsale",
                "engine_version": {"type": "term", "value": 2},
                "region": {"type": "terms", "value": [1]},
                "room": {"type": "terms", "value": [rooms_number]},
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
        total_pages = min(math.ceil(listings_count / OFFERS_PER_PAGE), MAX_PAGES)
        self.logger.info(
            f"Чанк {rooms_number}к {min_price}-{max_price}: "
            f"{listings_count} объявлений, {total_pages} страниц"
        )

        saved = 0
        for page_num in range(1, total_pages + 1):
            data = await self._fetch_page(
                context, rooms_number, min_price, max_price, page_num
            )

            offers = data.get("data", {}).get("offersSerialized", [])
            if not offers:
                self.logger.info(f"Страница {page_num}: нет данных, завершение чанка")
                break

            listings = self._extract_listings(offers)
            for listing in listings:
                await db.save_raw_listing(listing)
                saved += 1

            self.logger.info(
                f"Страница {page_num}/{total_pages}: получено {len(listings)} объявлений"
            )
            delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
            self.logger.info(f"Пауза {delay:.1f}с перед следующей страницей")
            await asyncio.sleep(delay)

        self.logger.info(f"Чанк завершён: итого сохранено {saved} объявлений")
        await db.mark_chunk_scraped(rooms_number, min_price)

    async def run(self):
        self.logger.info("Запуск Pagination Scraper")

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("Запуск браузера")
            context = await get_browser_context(p, self.user_data_dir)
            page = context.pages[0] if context.pages else await context.new_page()
            base_page = BasePage(page)

            await page.goto("https://www.cian.ru/kupit-kvartiru/")
            await base_page.wait_for_human_captcha()

            async with DatabaseManager() as db:
                chunks = await db.get_unscraped_chunks()
                self.logger.info(f"Чанков для скрапинга: {len(chunks)}")

                run_started_at = datetime.now(timezone.utc)

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

                deactivated = await db.deactivate_unseen_listings(run_started_at)
                self.logger.info(f"Деактивировано пропавших объявлений: {deactivated}")

            self.logger.info("Скрапинг завершён")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")
            await context.close()


if __name__ == "__main__":
    setup_logging()
    scraper = PaginationScraper(user_data_dir=USER_DATA_DIR)
    asyncio.run(scraper.run())
