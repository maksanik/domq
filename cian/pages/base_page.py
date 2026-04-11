import asyncio
import random
import logging
from playwright.async_api import Page


class BasePage:
    """Базовый класс для работы со страницами браузера."""

    def __init__(self, page: Page):
        self.page = page
        self.logger = logging.getLogger(self.__class__.__name__)

    async def random_sleep(self, a: float = 0.8, b: float = 2.0):
        t = random.uniform(a, b)
        self.logger.info(f"Сон {t:.2f} сек.")
        await asyncio.sleep(t)

    async def human_scroll(self, distance: int = 1200, step: int = 40):
        self.logger.info("Старт плавного скролла")
        steps = distance // step
        for _ in range(steps):
            delta = random.randint(int(step * 0.8), int(step * 1.2))
            await self.page.mouse.wheel(0, delta)
            await asyncio.sleep(random.uniform(0.01, 0.05))
        await asyncio.sleep(random.uniform(0.2, 0.5))
        for _ in range(steps):
            delta = random.randint(int(step * 0.8), int(step * 1.2))
            await self.page.mouse.wheel(0, -delta)
            await asyncio.sleep(random.uniform(0.01, 0.05))
        self.logger.info("Скролл завершен")

    async def scroll_to_top(self, step_min: int = 150, step_max: int = 300):
        self.logger.info("Старт прокрутки к началу страницы")
        while True:
            current_scroll = await self.page.evaluate("window.scrollY")
            if current_scroll <= 0:
                break
            current_step = random.randint(step_min, step_max)
            if current_scroll < current_step:
                current_step = current_scroll
            await self.page.mouse.wheel(0, -current_step)
            await asyncio.sleep(random.uniform(0.03, 0.1))
            new_scroll = await self.page.evaluate("window.scrollY")
            if new_scroll == current_scroll:
                break
        self.logger.info("Страница прокручена до верха")

    async def wait_for_human_captcha(self):
        self.logger.info("Ожидание решения капчи")
        print("Если появилась капча — реши её в браузере и нажми Enter в консоли...")
        await asyncio.to_thread(input)
        self.logger.info("Ожидание капчи завершено")
