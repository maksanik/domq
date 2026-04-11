import asyncio
import logging
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from cian.browser import get_browser_context
from cian.pages.filter_page import CianFilterPage
from db.db_manager import DatabaseManager
from config import setup_logging, USER_DATA_DIR, DB_PATH


class RangeScraper:
    """Оркестратор сбора диапазонов цен (чанков)."""

    def __init__(self, user_data_dir: str, db_path: str):
        self.user_data_dir = user_data_dir
        self.db_path = db_path
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _find_next_price_chunk(
        self, cian_page: CianFilterPage, current_min: int, absolute_max: int
    ) -> dict[str, int]:
        MAX_LISTINGS = 1000
        IDEAL_MIN = 800
        PRICE_STEP = 10000

        await cian_page.select_price_range(current_min, absolute_max)
        total_count = await cian_page.get_listings_count()

        if total_count <= MAX_LISTINGS:
            return {"current_max": absolute_max, "listings_count": total_count}

        low = current_min
        high = absolute_max
        best_max_for_chunk = current_min
        last_count = total_count

        first_guess_ratio = MAX_LISTINGS / total_count
        mid = current_min + int((absolute_max - current_min) * first_guess_ratio)
        mid = (mid // PRICE_STEP) * PRICE_STEP

        while low <= high:
            if mid < low or mid > high:
                mid = (low + high) // 2
            mid = max(low, (mid // PRICE_STEP) * PRICE_STEP)

            await cian_page.select_price_range(current_min, mid)
            count = await cian_page.get_listings_count()

            self.logger.info(f"Поиск: {current_min}-{mid} (найдено {count})")

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

    async def load_price_ranges(self, cian_page: CianFilterPage, rooms_number: int):
        ABSOLUTE_MAX_PRICE = 500_000_000
        current_min = 0

        success = await cian_page.select_room_count(rooms_number)
        if not success:
            self.logger.error(
                f"Фильтр комнат не применен для {rooms_number} комн. Выход."
            )
            return

        async with DatabaseManager(self.db_path) as db:
            while current_min < ABSOLUTE_MAX_PRICE:
                cached_max = await db.get_max_price(rooms_number, current_min)

                if cached_max is not None:
                    current_max = cached_max
                    self.logger.info(
                        f"Взято из БД (пропуск поиска): {current_min} - {current_max}"
                    )
                else:
                    self.logger.info(
                        f"Поиск чанка в браузере для min_price: {current_min}..."
                    )
                    next_chunk = await self._find_next_price_chunk(
                        cian_page, current_min, ABSOLUTE_MAX_PRICE
                    )
                    current_max = next_chunk.get("current_max")
                    listings_count = next_chunk.get("listings_count")

                    await db.save_chunk(
                        rooms_number, current_min, current_max, listings_count
                    )
                    self.logger.info(
                        f"Новый чанк сохранен в БД: {current_min} - {current_max}"
                    )

                current_min = current_max + 1

    async def run(self):
        self.logger.info("Запуск Range Scraper")

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("Запуск браузера")
            context = await get_browser_context(p, self.user_data_dir)

            page = context.pages[0] if context.pages else await context.new_page()
            cian_page = CianFilterPage(page)

            await cian_page.open()
            await cian_page.wait_for_human_captcha()
            await cian_page.human_scroll()
            await cian_page.random_sleep()

            for rooms_number in range(1, 7):
                await cian_page.scroll_to_top()
                await self.load_price_ranges(cian_page, rooms_number)

            self.logger.info("Скрипт завершил работу")
            print("Готово. Скрипт ничего больше не делает.")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")
            await context.close()


if __name__ == "__main__":
    # Настраиваем логирование один раз на старте
    setup_logging()

    # Инициализируем и запускаем скрапер
    scraper = RangeScraper(user_data_dir=USER_DATA_DIR, db_path=DB_PATH)
    asyncio.run(scraper.run())
