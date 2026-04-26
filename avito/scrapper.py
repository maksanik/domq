import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from cian.browser import get_browser_context
from cian.pages.base_page import BasePage
from config import AVITO_USER_DATA_DIR, setup_logging
from db.db_manager import DatabaseManager

AVITO_ITEMS_URL = "https://www.avito.ru/web/1/js/items"
MAX_PAGES = 100

PAGE_DELAY_MIN = 2.0
PAGE_DELAY_MAX = 3.5
CHUNK_DELAY_MIN = 3.0
CHUNK_DELAY_MAX = 5.0

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 30.0

# Фиксированные feature-флаги из рабочего curl-запроса
_FEATURE_FLAGS = {
    "features[imageAspectRatio]": "1:1",
    "features[noPlaceholders]": "true",
    "features[justSpa]": "true",
    "features[responsive]": "true",
    "features[useReload]": "true",
    "features[stickyCatalogFilters]": "false",
    "features[simpleCounters]": "true",
    "features[isRatingExperiment]": "true",
    "features[isContactsButtonRedesigned]": "false",
    "features[desktopPublishFromSerpTest]": "false",
    "features[desktopPinPositionVrTop]": "false",
    "features[desktopHideContextPositionOnReject]": "false",
    "features[desktopShowBigContextPositions]": "true",
    "features[desktopSpaInFilters]": "false",
    "features[isReMapPreviewAb]": "false",
    "features[isReItemNewViewAb]": "false",
    "features[isReNewSortAb]": "false",
    "features[isReItemXlAb]": "false",
    "features[isSplitAdvertBlock]": "false",
    "features[isShowWithPhotoFilter]": "true",
    "features[reverseVisualRubricator]": "false",
    "features[isReInterestingHouseAb]": "false",
    "features[jobsConsentDisclaimer]": "false",
    "features[altViewedBadgeDesktopAb]": "false",
    "features[isHideRecommendationsInfinite]": "false",
    "features[desktopGridRedesign][isReducedGridWidth]": "true",
    "features[ivaItemRedesign]": "true",
    "features[shouldSendRreLayoutEvents]": "false",
    "features[isRedesignZhkSerp]": "false",
    "features[isHotelsSnippetRedesign]": "false",
    "proprofile": "1",
    "useReload": "true",
    "spaFlow": "true",
}


def _parse_title(title: str) -> tuple[int | None, float | None, int | None, int | None]:
    """Извлекает комнаты, площадь, этаж и этажность из заголовка объявления."""
    rooms = None
    area = None
    floor = None
    floors_total = None

    m = re.search(r"(\d+)-к\.", title)
    if m:
        rooms = int(m.group(1))
    elif "студия" in title.lower():
        rooms = 0

    m = re.search(r"([\d.]+)\s*м²", title)
    if m:
        area = float(m.group(1))

    m = re.search(r"(\d+)/(\d+)\s*эт", title)
    if m:
        floor = int(m.group(1))
        floors_total = int(m.group(2))

    return rooms, area, floor, floors_total


class AvitoScraper:
    """Скрапер для сбора объявлений Avito по ценовым диапазонам из price_chunks."""

    def __init__(self, user_data_dir: str = AVITO_USER_DATA_DIR):
        self.user_data_dir = user_data_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def _build_url(self, min_price: int, max_price: int, page: int) -> str:
        params = {
            "categoryId": 24,
            "locationId": 637640,
            "cd": 0,
            "params[201]": 1059,
            "verticalCategoryId": 1,
            "rootCategoryId": 4,
            "localPriority": 0,
            "pmin": min_price,
            "pmax": max_price,
            "updateListOnly": "true",
            **_FEATURE_FLAGS,
        }
        if page > 1:
            params["p"] = page
        return f"{AVITO_ITEMS_URL}?{urlencode(params)}"

    def _extract_listings(self, items: list) -> list[dict]:
        result = []
        for item in items:
            if item.get("type") != "item":
                continue
            try:
                rooms, area_total, floor, floors_total = _parse_title(
                    item.get("title") or ""
                )

                coords = item.get("coords") or {}
                lat = float(coords["lat"]) if coords.get("lat") else None
                lng = float(coords["lng"]) if coords.get("lng") else None

                address_text = (item.get("geo") or {}).get("formattedAddress")

                price = (item.get("priceDetailed") or {}).get("value")

                allow_ts = item.get("allowTimeStamp")
                created_at = (
                    datetime.fromtimestamp(allow_ts / 1000, tz=timezone.utc)
                    if allow_ts
                    else None
                )

                images = item.get("images") or []
                first = images[0] if images else {}
                thumbnail_url = (
                    first.get("472x472") or first.get("432x432") or first.get("416x416")
                )
                photos_json = [img["864x864"] for img in images if img.get("864x864")]
                images_count = item.get("imagesCount") or len(images)

                url_path = item.get("urlPath") or ""
                result.append(
                    {
                        "source": "avito",
                        "external_id": str(item["id"]),
                        "url": f"https://www.avito.ru{url_path}",
                        "title": item.get("title"),
                        "description": item.get("description"),
                        "price": price,
                        "area_total": area_total,
                        "area_kitchen": None,
                        "rooms": rooms,
                        "floor": floor,
                        "floors_total": floors_total,
                        "latitude": lat,
                        "longitude": lng,
                        "address_text": address_text,
                        "year_built": None,
                        "material_type": None,
                        "images_count": images_count,
                        "has_photos": images_count > 0,
                        "thumbnail_url": thumbnail_url,
                        "photos_json": photos_json or None,
                        "created_at": created_at,
                    }
                )
            except Exception as e:
                self.logger.warning(f"Ошибка разбора объявления {item.get('id')}: {e}")
        return result

    async def _fetch_page(
        self, context, min_price: int, max_price: int, page: int
    ) -> dict:
        url = self._build_url(min_price, max_price, page)
        headers = {
            "Accept": "application/json",
            "Accept-Language": "ru,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "X-Source": "client-browser",
            "Referer": f"https://www.avito.ru/moskva/kvartiry/prodam?p={page}",
        }
        for attempt in range(1, MAX_RETRIES + 1):
            response = await context.request.get(url, headers=headers)
            if response.status in (429, 503):
                wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 10)
                self.logger.warning(
                    f"HTTP {response.status} стр.{page}, "
                    f"попытка {attempt}/{MAX_RETRIES}, ждём {wait:.0f}с"
                )
                await asyncio.sleep(wait)
                continue
            if not response.ok:
                self.logger.error(f"HTTP {response.status} на странице {page}")
                return {}
            return await response.json()
        self.logger.error(f"Страница {page}: все {MAX_RETRIES} попытки исчерпаны")
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
        saved = 0
        for page in range(1, MAX_PAGES + 1):
            self.logger.info(
                f"[Avito] Чанк {rooms_number}к {min_price}-{max_price}, стр.{page}"
            )
            data = await self._fetch_page(context, min_price, max_price, page)
            items = (data.get("catalog") or {}).get("items") or []
            if not items:
                self.logger.info(f"[Avito] Стр.{page}: нет данных, завершение чанка")
                break

            listings = self._extract_listings(items)
            for listing in listings:
                await db.save_raw_listing(listing)
                saved += 1

            self.logger.info(f"[Avito] Стр.{page}: получено {len(listings)} объявлений")

            if page < MAX_PAGES:
                delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
                await asyncio.sleep(delay)

        self.logger.info(f"[Avito] Чанк завершён: сохранено {saved} объявлений")
        await db.mark_chunk_scraped(rooms_number, min_price)

    async def run(
        self,
        rescrape: bool = False,
        captcha_event: asyncio.Event | None = None,
        chunks: list[dict] | None = None,
    ):
        self.logger.info("[Avito] Запуск скрапера")

        async with Stealth().use_async(async_playwright()) as p:
            context = await get_browser_context(p, self.user_data_dir)
            page = context.pages[0] if context.pages else await context.new_page()
            base_page = BasePage(page)

            await page.goto("https://www.avito.ru/moskva/kvartiry/prodam")
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

                self.logger.info(f"[Avito] Чанков для скрапинга: {len(chunks)}")

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
                        self.logger.info(f"[Avito] Пауза {delay:.0f}с между чанками")
                        await asyncio.sleep(delay)

                deactivated = await db.deactivate_unseen_listings(
                    run_started_at, source="avito"
                )
                self.logger.info(
                    f"[Avito] Деактивировано пропавших объявлений: {deactivated}"
                )

            self.logger.info("[Avito] Скрапинг завершён")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера Avito...")
            await context.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pagination scraper для Avito")
    parser.add_argument(
        "--rescrape",
        action="store_true",
        help="Сбросить все чанки в 'не скрапировано' перед стартом",
    )
    _args = parser.parse_args()

    setup_logging()
    scraper = AvitoScraper()
    asyncio.run(scraper.run(rescrape=_args.rescrape))
