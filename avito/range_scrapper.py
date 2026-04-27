import argparse
import asyncio
import logging
import random
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from cian.browser import get_browser_context
from cian.pages.base_page import BasePage
from config import AVITO_USER_DATA_DIR, setup_logging
from db.db_manager import DatabaseManager

AVITO_ITEMS_URL = "https://www.avito.ru/web/1/js/items"

MAX_LISTINGS = 500
IDEAL_MIN = 400
PRICE_STEP = 10_000
ABSOLUTE_MAX_PRICE = 200_000_000

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 30.0

_AVITO_ROOMS_PARAM = {
    0: 5695,  # студия
    1: 5696,
    2: 5697,
    3: 5698,
    4: 5699,
    5: 5700,
    6: 5701,
}

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


class AvitoRangeScraper:
    """Оркестратор сбора ценовых диапазонов (чанков) для Avito через API."""

    def __init__(self, user_data_dir: str = AVITO_USER_DATA_DIR):
        self.user_data_dir = user_data_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def _build_url(self, rooms_number: int, min_price: int, max_price: int) -> str:
        params = {
            "categoryId": 24,
            "locationId": 637640,
            "cd": 0,
            "params[201]": 1059,
            "params[549][0]": _AVITO_ROOMS_PARAM[rooms_number],
            "verticalCategoryId": 1,
            "rootCategoryId": 4,
            "localPriority": 0,
            "pmin": min_price,
            "pmax": max_price,
            "updateListOnly": "true",
            **_FEATURE_FLAGS,
        }
        return f"{AVITO_ITEMS_URL}?{urlencode(params)}"

    async def _fetch_count(
        self, context, rooms_number: int, min_price: int, max_price: int
    ) -> int:
        """Запрашивает API и возвращает mainCount. Возвращает -1 при ошибке."""
        url = self._build_url(rooms_number, min_price, max_price)
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Requested-With": "XMLHttpRequest",
            "X-Source": "client-browser",
            "Referer": "https://www.avito.ru/moskva/kvartiry/prodam",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        for attempt in range(1, MAX_RETRIES + 1):
            response = await context.request.get(url, headers=headers)
            if response.status == 403:
                self.logger.warning("HTTP 403 — IP заблокирован")
                return -1
            if response.status in (429, 503):
                wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 10)
                self.logger.warning(
                    f"HTTP {response.status}, попытка {attempt}/{MAX_RETRIES}, ждём {wait:.0f}с"
                )
                await asyncio.sleep(wait)
                continue
            if not response.ok:
                self.logger.error(f"HTTP {response.status} при получении счётчика")
                return -1
            data = await response.json()
            return int(data.get("mainCount") or 0)
        self.logger.error(f"Все {MAX_RETRIES} попытки исчерпаны")
        return -1

    async def _find_next_price_chunk(
        self, context, rooms_number: int, current_min: int, absolute_max: int
    ) -> dict[str, int]:
        total_count = await self._fetch_count(
            context, rooms_number, current_min, absolute_max
        )
        await asyncio.sleep(random.uniform(1.5, 3.0))

        if total_count < 0:
            return {"current_max": current_min, "listings_count": 0}

        if total_count <= MAX_LISTINGS:
            return {"current_max": absolute_max, "listings_count": total_count}

        low = current_min
        high = absolute_max
        best_max_for_chunk: int = current_min
        last_count: int = total_count

        first_guess_ratio = MAX_LISTINGS / total_count
        mid = current_min + int((absolute_max - current_min) * first_guess_ratio)
        mid = (mid // PRICE_STEP) * PRICE_STEP

        while low <= high:
            if mid < low or mid > high:
                mid = (low + high) // 2
            mid = max(low, (mid // PRICE_STEP) * PRICE_STEP)

            count = await self._fetch_count(context, rooms_number, current_min, mid)
            await asyncio.sleep(random.uniform(1.5, 3.0))

            self.logger.info(f"Поиск: {current_min}-{mid} (найдено {count})")

            if count < 0:
                self.logger.error(
                    "Ошибка получения счётчика — прерывание бинарного поиска"
                )
                break

            if count > MAX_LISTINGS:
                high = mid - PRICE_STEP
            else:
                best_max_for_chunk = mid
                last_count = count
                low = mid + PRICE_STEP
                if count >= IDEAL_MIN or (high - low) < PRICE_STEP:
                    break
            mid = -1

        return {"current_max": best_max_for_chunk, "listings_count": last_count}

    async def load_price_ranges(self, context, rooms_number: int):
        current_min = 0

        async with DatabaseManager() as db:
            while current_min < ABSOLUTE_MAX_PRICE:
                cached_max = await db.get_max_price(rooms_number, current_min)

                if cached_max is not None:
                    current_max = cached_max
                    self.logger.info(
                        f"Взято из БД (пропуск поиска): {current_min} - {current_max}"
                    )
                else:
                    self.logger.info(f"Поиск чанка для min_price={current_min}...")
                    next_chunk = await self._find_next_price_chunk(
                        context, rooms_number, current_min, ABSOLUTE_MAX_PRICE
                    )
                    current_max: int = next_chunk["current_max"]
                    listings_count: int = next_chunk["listings_count"]

                    if current_max <= current_min:
                        self.logger.warning(
                            f"Не удалось найти чанк от {current_min}, завершение комнаты"
                        )
                        break

                    await db.save_chunk(
                        rooms_number, current_min, current_max, listings_count
                    )
                    self.logger.info(
                        f"Новый чанк сохранён: {current_min} - {current_max} "
                        f"({listings_count} объявл.)"
                    )

                current_min = current_max + 1

    async def run(self, rooms: Optional[int] = None, redo: bool = False):
        self.logger.info("[AvitoRange] Запуск")

        if redo:
            async with DatabaseManager() as db:
                deleted = await db.delete_chunks(rooms)
                label = f"комнат={rooms}" if rooms is not None else "все"
                self.logger.info(f"Удалено {deleted} чанков ({label})")

        rooms_to_scrape = [rooms] if rooms is not None else list(range(0, 7))

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("[AvitoRange] Запуск браузера")
            context = await get_browser_context(p, self.user_data_dir, channel="chrome")
            page = context.pages[0] if context.pages else await context.new_page()
            base_page = BasePage(page)

            await page.goto("https://www.avito.ru/moskva/kvartiry/prodam")
            await base_page.wait_for_human_captcha()
            await base_page.human_scroll()
            await asyncio.sleep(random.uniform(2.0, 4.0))

            for rooms_number in rooms_to_scrape:
                self.logger.info(f"[AvitoRange] Обработка {rooms_number} комн.")
                await self.load_price_ranges(context, rooms_number)

            self.logger.info("[AvitoRange] Скрипт завершил работу")
            print("Готово. Скрипт ничего больше не делает.")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")
            await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сбор ценовых диапазонов с Avito")
    parser.add_argument(
        "--rooms",
        type=int,
        default=None,
        help="Кол-во комнат (0–6). Без аргумента — все.",
    )
    parser.add_argument(
        "--redo",
        action="store_true",
        help="Удалить существующие чанки и собрать заново.",
    )
    args = parser.parse_args()

    setup_logging()
    scraper = AvitoRangeScraper()
    asyncio.run(scraper.run(rooms=args.rooms, redo=args.redo))
