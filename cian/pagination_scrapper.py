import asyncio
import json
import logging
import math

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from cian.browser import get_browser_context
from cian.pages.base_page import BasePage
from db.db_manager import DatabaseManager
from config import setup_logging, USER_DATA_DIR, DB_PATH

API_URL = "https://api.cian.ru/search-offers/v2/search-offers-desktop/"
OFFERS_PER_PAGE = 28
MAX_PAGES = 54


class PaginationScraper:
    """Скрапер для сбора объявлений постранично по чанкам цен."""

    def __init__(self, user_data_dir: str, db_path: str):
        self.user_data_dir = user_data_dir
        self.db_path = db_path
        self.logger = logging.getLogger(self.__class__.__name__)

    def _extract_listings(self, offers: list) -> list[dict]:
        """Извлекает нужные поля из списка офферов Cian API."""
        result = []
        for offer in offers:
            try:
                address_parts = offer.get("geo", {}).get("address", [])
                address = ", ".join(p["name"] for p in address_parts if p.get("name"))

                result.append(
                    {
                        "id": offer.get("id"),
                        "price": offer.get("bargainTerms", {}).get("price"),
                        "total_area": offer.get("totalArea"),
                        "rooms_count": offer.get("roomsCount"),
                        "floor_number": offer.get("floorNumber"),
                        "floors_count": offer.get("building", {}).get("floorsCount"),
                        "address": address,
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

        response = await context.request.post(
            API_URL,
            data=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "Origin": "https://www.cian.ru",
                "Referer": "https://www.cian.ru/",
            },
        )

        if not response.ok:
            self.logger.error(f"HTTP {response.status} на странице {page_num}")
            return {}

        return await response.json()

    async def _scrape_chunk(
        self,
        context,
        db: DatabaseManager,
        rooms_number: int,
        min_price: int,
        max_price: int,
        listings_count: int,
    ):
        """Скрапит все страницы одного чанка и сохраняет в БД."""
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
                await db.save_listing(listing)
                saved += 1

            self.logger.info(
                f"Страница {page_num}/{total_pages}: получено {len(listings)} объявлений"
            )
            await asyncio.sleep(0.5)

        self.logger.info(f"Чанк завершён: итого сохранено {saved} объявлений")
        await db.mark_chunk_scraped(rooms_number, min_price)

    async def run(self):
        self.logger.info("Запуск Pagination Scraper")

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("Запуск браузера")
            context = await get_browser_context(p, self.user_data_dir)
            page = context.pages[0] if context.pages else await context.new_page()
            base_page = BasePage(page)

            # Открываем Циан для инициализации сессии и куки
            await page.goto("https://www.cian.ru/kupit-kvartiru/")
            await base_page.wait_for_human_captcha()

            async with DatabaseManager(self.db_path) as db:
                chunks = await db.get_unscraped_chunks()
                self.logger.info(f"Чанков для скрапинга: {len(chunks)}")

                for chunk in chunks:
                    await self._scrape_chunk(
                        context,
                        db,
                        chunk["rooms_number"],
                        chunk["min_price"],
                        chunk["max_price"],
                        chunk["listings_count"],
                    )

            self.logger.info("Скрапинг завершён")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")
            await context.close()


if __name__ == "__main__":
    setup_logging()
    scraper = PaginationScraper(user_data_dir=USER_DATA_DIR, db_path=DB_PATH)
    asyncio.run(scraper.run())
