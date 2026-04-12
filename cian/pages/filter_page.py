import asyncio
import re
import sys
from playwright.async_api import Page, TimeoutError
from cian.pages.base_page import BasePage


class CianFilterPage(BasePage):
    """Класс для взаимодействия со страницей фильтров Циана."""

    def __init__(self, page: Page):
        super().__init__(page)

        self.url = "https://www.cian.ru/kupit-kvartiru/"

        # Локаторы
        self.room_btn = self.page.locator('div[data-testid="roomType"] button')
        self.room_dropdown = self.page.locator(
            'div[data-testid="roomType"] div[data-testid="DropdownSelect"]'
        )
        self.price_dropdown = self.page.locator('div[data-testid="DropdownPrice"]')
        self.price_btn = self.page.locator('div[data-testid="price"] button')
        self.min_input = self.price_dropdown.locator(
            'input[data-name="MaskedInput"]'
        ).nth(0)
        self.max_input = self.price_dropdown.locator(
            'input[data-name="MaskedInput"]'
        ).nth(1)
        self.preloader = self.page.locator('div[data-testid="PagePreloader"]')
        self.summary_header = self.page.locator(
            'div[data-testid="SummaryHeader"] h5'
        ).first

        self.modifier = "Meta" if sys.platform == "darwin" else "Control"

    async def open(self):
        self.logger.info(f"Переход на {self.url}")
        await self.page.goto(self.url)

    async def select_room_count(self, count: int) -> bool:
        if count < 1 or count > 6:
            self.logger.error("Введён некорректный count в select_room_count")
            raise Exception

        await self.scroll_to_top()

        target_label = f"{count}-комнатная"
        self.logger.info(f"Установка фильтра строго на: {target_label}")

        try:
            if not await self.room_dropdown.is_visible():
                await self.room_btn.click(timeout=5000)
                await self.random_sleep(self.logger, 0.6, 1.2)

            options = self.page.locator('div[data-name="SelectOption"]')
            options_count = await options.count()

            for i in range(options_count):
                option = options.nth(i)
                text = await option.inner_text()
                checkbox = option.locator('input[type="checkbox"]')
                is_selected = await checkbox.is_checked()
                is_target = target_label in text

                if is_target and not is_selected:
                    await option.click()
                    self.logger.info(f"Включено: {text.strip()}")
                    await self.random_sleep(self.logger, 0.3, 0.5)
                elif not is_target and is_selected:
                    await option.click()
                    self.logger.info(f"Выключено: {text.strip()}")
                    await self.random_sleep(self.logger, 0.3, 0.5)

            if await self.room_dropdown.is_visible():
                await self.scroll_to_top()
                await self.room_btn.click()

            return True

        except Exception as e:
            self.logger.error(f"Ошибка при настройке фильтра комнат: {e}")
            return False

    async def select_price_range(self, min_price: int, max_price: int):
        try:
            self.logger.info(f"Установка цены от {min_price} до {max_price}")
            if not await self.price_dropdown.is_visible():
                await self.price_btn.click()
                await self.random_sleep(self.logger, 0.5, 1.0)

            for _ in range(5):
                current_val = (await self.min_input.input_value()).replace(" ", "")
                if current_val == str(min_price):
                    break
                await self.min_input.click()
                await self.min_input.press(f"{self.modifier}+A")
                await self.min_input.press("Backspace")
                await self.min_input.type(str(min_price), delay=50)
                await self.random_sleep(self.logger, 0.3, 0.7)

            for _ in range(5):
                current_val = (await self.max_input.input_value()).replace(" ", "")
                if current_val == str(max_price):
                    break
                await self.max_input.click()
                await self.max_input.press(f"{self.modifier}+A")
                await self.max_input.press("Backspace")
                await self.max_input.type(str(max_price), delay=50)
                await self.max_input.press("Enter")

            await self.random_sleep(self.logger, 1.5, 2)
        except TimeoutError:
            self.logger.error("Ошибка при установке фильтра цены (Таймаут)")

    async def get_listings_count(self) -> int:
        self.logger.info("Ожидание обновления данных...")
        try:
            if await self.preloader.is_visible():
                await self.preloader.wait_for(state="hidden", timeout=8000)
        except Exception:
            pass

        await asyncio.sleep(1)

        try:
            await self.summary_header.wait_for(state="visible", timeout=5000)
            text = await self.summary_header.text_content()
            if not text:
                return 0
            digits = re.sub(r"\D", "", text)
            number = int(digits) if digits else 0
            self.logger.info(f"Найдено объявлений: {number}")
            return number
        except TimeoutError:
            self.logger.warning("Не удалось найти заголовок с количеством объявлений")
            return 0
