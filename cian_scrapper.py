import asyncio
import random
import logging
import re
import sys
from playwright.async_api import async_playwright, TimeoutError, Page
from playwright_stealth import Stealth

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


class BasePage:
    """Базовый класс для работы со страницами браузера."""
    
    def __init__(self, page: Page):
        self.page = page
        self.logger = logging.getLogger(self.__class__.__name__)

    async def random_sleep(self, a: float = 0.8, b: float = 2.0):
        """Случайная пауза для имитации человека."""
        t = random.uniform(a, b)
        self.logger.info(f"Сон {t:.2f} сек.")
        await asyncio.sleep(t)

    async def human_scroll(self, distance: int = 1200, step: int = 40):
        """Плавный скролл вниз и обратно вверх."""
        self.logger.info("Старт плавного скролла")
        steps = distance // step

        # Скролл вниз
        for _ in range(steps):
            delta = random.randint(int(step * 0.8), int(step * 1.2))
            await self.page.mouse.wheel(0, delta)
            await asyncio.sleep(random.uniform(0.01, 0.05))

        await asyncio.sleep(random.uniform(0.2, 0.5))

        # Скролл вверх
        for _ in range(steps):
            delta = random.randint(int(step * 0.8), int(step * 1.2))
            await self.page.mouse.wheel(0, -delta)
            await asyncio.sleep(random.uniform(0.01, 0.05))

        self.logger.info("Скролл завершен")

    async def wait_for_human_captcha(self):
        """Остановка для ручного решения капчи."""
        self.logger.info("Ожидание решения капчи")
        print("Если появилась капча — реши её в браузере и нажми Enter в консоли...")
        await asyncio.to_thread(input)
        self.logger.info("Ожидание капчи завершено (подтверждено пользователем)")


class CianFilterPage(BasePage):
    """Класс для взаимодействия со страницей фильтров Циана."""
    
    def __init__(self, page: Page):
        super().__init__(page)
        self.url = "https://www.cian.ru/kupit-kvartiru/"
        
        # Локаторы
        self.room_btn = self.page.locator('div[data-testid="roomType"] button')
        self.one_room_option = self.page.locator('div[data-name="SelectOption"]:has-text("1-комнатная")')
        self.two_rooms_option = self.page.locator('div[data-name="SelectOption"]:has-text("2-комнатная")')
        self.three_rooms_option = self.page.locator('div[data-name="SelectOption"]:has-text("3-комнатная")')
        
        self.price_dropdown = self.page.locator('div[data-testid="DropdownPrice"]')
        self.price_btn = self.page.locator('div[data-testid="price"] button')
        self.min_input = self.price_dropdown.locator('input[data-name="MaskedInput"]').nth(0)
        self.max_input = self.price_dropdown.locator('input[data-name="MaskedInput"]').nth(1)
        
        self.preloader = self.page.locator('div[data-testid="PagePreloader"]')
        
        self.summary_header = self.page.locator('div[data-testid="SummaryHeader"] h5').first

        # Клавиша модификатор (Cmd для Mac, Ctrl для Windows/Linux)
        self.modifier = "Meta" if sys.platform == "darwin" else "Control"

    async def open(self):
        """Открытие страницы."""
        self.logger.info(f"Переход на {self.url}")
        await self.page.goto(self.url)

    async def select_one_room(self) -> bool:
        """Выбор 1-комнатной квартиры через современные локаторы."""
        self.logger.info("Выбор фильтра '1-комнатная'")
        try:
            await self.room_btn.click(timeout=5000)
            await self.random_sleep(0.5, 1.0)

            await self.one_room_option.click(timeout=5000)
            await self.random_sleep(0.5, 1.0)

            self.logger.info("1-комнатная квартира выбрана")
            return True
        except TimeoutError:
            self.logger.error("Не удалось открыть фильтр комнат (Таймаут)")
            return False

    async def select_price_range(self, min_price: int, max_price: int):
        """Ввод цены с защитой от зацикливания и надежной очисткой инпутов."""
        try:
            self.logger.info(f"Установка цены от {min_price} до {max_price}")
            
            # Если дропдаун скрыт, открываем его
            if not await self.price_dropdown.is_visible():
                await self.price_btn.click()
                await self.random_sleep(0.5, 1.0)

            # Ввод минимальной цены (с защитой от бесконечного цикла)
            for _ in range(5):
                current_val = (await self.min_input.input_value()).replace(" ", "")
                if current_val == str(min_price):
                    break
                    
                await self.min_input.click()
                await self.min_input.press(f"{self.modifier}+A")
                await self.min_input.press("Backspace")
                await self.min_input.type(str(min_price), delay=50)
                await self.random_sleep(0.3, 0.7)
            else:
                self.logger.warning("Не удалось корректно ввести min_price за 5 попыток.")

            # Ввод максимальной цены
            for _ in range(5):
                current_val = (await self.max_input.input_value()).replace(" ", "")
                if current_val == str(max_price):
                    break
                
                await self.max_input.click()
                await self.max_input.press(f"{self.modifier}+A")
                await self.max_input.press("Backspace")
                await self.max_input.type(str(max_price), delay=50)
                await self.max_input.press("Enter") # Применяем фильтр
            else:
                self.logger.warning("Не удалось корректно ввести max_price за 5 попыток.")
                
            await self.random_sleep(1.5, 2)
            self.logger.info("Цена установлена")
        except TimeoutError:
            self.logger.error("Ошибка при установке фильтра цены (Таймаут)")

    async def get_listings_count(self) -> int:
        """Ожидание загрузки данных и парсинг количества объявлений."""
        self.logger.info("Ожидание обновления данных...")
        try:
            if await self.preloader.is_visible():
                await self.preloader.wait_for(state="hidden", timeout=8000)
        except Exception:
            pass # Игнорируем, если прелоадера не было

        # Даем React-у время отрендерить новые цифры
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


class CianScraper:
    """Оркестратор скрипта. Управляет браузером и бизнес-логикой."""
    
    def __init__(self, user_data_dir: str = "cian_profile"):
        self.user_data_dir = user_data_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _adjust_price_ranges(self, cian_page: CianFilterPage) -> list[tuple[int]]:
        """Бизнес-логика: классический бинарный поиск по диапазонам цен."""
        ABSOLUTE_MAX_PRICE = 150_000_000
        MAX_LISTINGS = 1000
        IDEAL_MIN = 800
        
        chunks = []
        current_min = 0

        while current_min < ABSOLUTE_MAX_PRICE:
            low = current_min
            high = ABSOLUTE_MAX_PRICE
            best_max_for_chunk = current_min

            # Внутренний бинарный поиск для нахождения идеального current_max
            while low <= high:
                mid = (low + high) // 2
                
                # ВАЖНО: Cian может не принимать слишком мелкие шаги, 
                # можно округлять mid до 1000, если нужно
                
                await cian_page.select_price_range(current_min, mid)
                count = await cian_page.get_listings_count()
                
                self.logger.info(f"Проверка диапазона {current_min} - {mid}: найдено {count}")

                if count > MAX_LISTINGS:
                    # Слишком много объявлений, нужно уменьшить правую границу
                    high = mid - 1
                else:
                    # Подходит, но пробуем расширить диапазон (жадный поиск)
                    best_max_for_chunk = mid
                    low = mid + 1
                    
                    # Если мы уже попали в "золотую середину", можно не продолжать поиск
                    if count >= IDEAL_MIN:
                        break
            
            # Если бинарный поиск не смог сдвинуться (например, на одной цене 2000 квартир)
            if best_max_for_chunk == current_min and count > MAX_LISTINGS:
                self.logger.warning(f"Невозможно разбить диапазон: на цене {current_min} более {count} объявлений.")
                # Силой расширяем на 1, чтобы избежать вечного цикла
                best_max_for_chunk = current_min + 1

            chunks.append((current_min, best_max_for_chunk))
            self.logger.info(f"Добавлен чанк: {current_min} - {best_max_for_chunk}")

            # Следующий чанк начинается со следующего рубля
            current_min = best_max_for_chunk + 1
            
            # Ограничение из вашего кода (для тестов)
            if len(chunks) >= 3:
                break
                
        return chunks

    async def run(self):
        """Запуск полного цикла работы парсера."""
        self.logger.info("Запуск скрипта")

        async with Stealth().use_async(async_playwright()) as p:
            self.logger.info("Запуск браузера")
            context = await p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
                viewport={"width": 1366, "height": 768}
            )

            page = context.pages[0] if context.pages else await context.new_page()
            cian_page = CianFilterPage(page)

            # --- Сценарий выполнения ---
            await cian_page.open()
            await cian_page.wait_for_human_captcha()
            
            await cian_page.human_scroll()
            await cian_page.random_sleep()

            success = await cian_page.select_one_room()
            if not success:
                self.logger.error("Критическая ошибка: фильтр комнат не применен. Выход.")
                await context.close()
                return

            # Запуск алгоритма подбора цены
            await self._adjust_price_ranges(cian_page)

            # Завершение
            self.logger.info("Скрипт завершил работу")
            print("Готово. Скрипт ничего больше не делает.")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")

            self.logger.info("Закрытие браузера")
            await context.close()


if __name__ == "__main__":
    scraper = CianScraper()
    asyncio.run(scraper.run())