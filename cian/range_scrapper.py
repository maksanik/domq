import asyncio
import random
import logging
import re
import sys
from playwright.async_api import async_playwright, TimeoutError, Page
from playwright_stealth import Stealth
import aiosqlite
from typing import Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("cian/bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


class ChunkDatabase:
    """Класс для взаимодействия с SQLite БД для хранения чанков цен."""

    def __init__(self, db_path: str = "cian/cian_data.db"):
        self.db_path = db_path
        # Указываем IDE, что здесь будет либо подключение, либо None
        self.db: aiosqlite.Connection = None  # type: ignore
        self.logger = logging.getLogger(self.__class__.__name__)

    async def __aenter__(self):
        """Открывает соединение с БД при входе в блок async with и инициализирует таблицы."""
        self.db = await aiosqlite.connect(self.db_path)
        await self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрывает соединение с БД при выходе из блока async with."""
        if self.db:
            await self.db.close()

    async def _init_db(self):
        """Создает таблицу с уникальным индексом, если она не существует."""
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS price_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rooms_number INTEGER,
                min_price INTEGER,
                max_price INTEGER,
                listings_count INTEGER,
                UNIQUE(rooms_number, min_price)
            )
        """)
        await self.db.commit()

    async def get_max_price(self, rooms_number: int, min_price: int) -> Optional[int]:
        """Проверяет, существует ли уже чанк. Возвращает max_price или None."""
        async with self.db.execute(
            "SELECT max_price FROM price_chunks WHERE rooms_number = ? AND min_price = ?",
            (rooms_number, min_price),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def save_chunk(
        self, rooms_number: int, min_price: int, max_price: int, listings_count: int
    ):
        """Сохраняет найденный чанк в БД."""
        try:
            await self.db.execute(
                "INSERT INTO price_chunks (rooms_number, min_price, max_price, listings_count) VALUES (?, ?, ?, ?)",
                (rooms_number, min_price, max_price, listings_count),
            )
            await self.db.commit()
        except aiosqlite.IntegrityError:
            self.logger.warning(
                f"Попытка дублирования чанка в БД: {rooms_number} комн, {min_price}-{max_price}"
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

    async def scroll_to_top(self, step_min: int = 150, step_max: int = 300):
        """Плавная прокрутка до самого верха страницы."""
        self.logger.info("Старт прокрутки к началу страницы")

        while True:
            # Проверяем текущую позицию прокрутки
            current_scroll = await self.page.evaluate("window.scrollY")

            # Если мы уже в самом верху, выходим из цикла
            if current_scroll <= 0:
                break

            # Генерируем случайный шаг прокрутки
            # Если осталось мало до верха, уменьшаем шаг, чтобы не "проскочить" грубо
            current_step = random.randint(step_min, step_max)
            if current_scroll < current_step:
                current_step = current_scroll

            # Скроллим вверх (отрицательное значение для wheel)
            await self.page.mouse.wheel(0, -current_step)

            # Случайная пауза между движениями "колесика"
            await asyncio.sleep(random.uniform(0.03, 0.1))

            # Дополнительная проверка: если после скролла позиция не изменилась
            # (например, страница заблокирована или какой-то элемент мешает), выходим
            new_scroll = await self.page.evaluate("window.scrollY")
            if new_scroll == current_scroll:
                break

        self.logger.info("Страница прокручена до верха")

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

        # Клавиша модификатор (Cmd для Mac, Ctrl для Windows/Linux)
        self.modifier = "Meta" if sys.platform == "darwin" else "Control"

    async def open(self):
        """Открытие страницы."""
        self.logger.info(f"Переход на {self.url}")
        await self.page.goto(self.url)

    async def select_room_count(self, count: int) -> bool:
        """Выбор комнаты: включает нужную (1-6) и выключает все остальные (включая Студии и т.д.)."""
        if count < 1 or count > 6:
            self.logger.error("Введён некорректный count в select_room_count")
            raise Exception

        target_label = f"{count}-комнатная"
        self.logger.info(f"Установка фильтра строго на: {target_label}")

        try:
            # 1. Открываем выпадающий список, если он еще не открыт
            if not await self.room_dropdown.is_visible():
                await self.room_btn.click(timeout=5000)
                await self.random_sleep(0.6, 1.2)

            # 2. Получаем все доступные опции в меню
            # Используем локатор для всех элементов списка
            options = self.page.locator('div[data-name="SelectOption"]')
            options_count = await options.count()

            for i in range(options_count):
                option = options.nth(i)
                text = await option.inner_text()

                # Проверяем состояние через скрытый input (type="checkbox")
                # Даже если он aria-hidden, Playwright может проверить его свойство checked
                checkbox = option.locator('input[type="checkbox"]')
                is_selected = await checkbox.is_checked()

                is_target = target_label in text

                # Логика:
                # Если это целевая комнатность и она НЕ выбрана -> Кликаем (включаем)
                # Если это ДРУГАЯ комнатность и она ВЫБРАНА -> Кликаем (выключаем)
                if is_target:
                    if not is_selected:
                        await option.click()
                        self.logger.info(f"Включено: {text.strip()}")
                        await self.random_sleep(0.3, 0.5)
                else:
                    if is_selected:
                        await option.click()
                        self.logger.info(f"Выключено: {text.strip()}")
                        await self.random_sleep(0.3, 0.5)

            # 3. Закрываем выпадающее меню (клик в заголовок или по кнопке еще раз),
            # чтобы применить фильтр, если это требуется интерфейсом
            if await self.room_dropdown.is_visible():
                await self.room_btn.click()

            return True

        except Exception as e:
            self.logger.error(f"Ошибка при настройке фильтра комнат: {e}")
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
                self.logger.warning(
                    "Не удалось корректно ввести min_price за 5 попыток."
                )

            # Ввод максимальной цены
            for _ in range(5):
                current_val = (await self.max_input.input_value()).replace(" ", "")
                if current_val == str(max_price):
                    break

                await self.max_input.click()
                await self.max_input.press(f"{self.modifier}+A")
                await self.max_input.press("Backspace")
                await self.max_input.type(str(max_price), delay=50)
                await self.max_input.press("Enter")  # Применяем фильтр
            else:
                self.logger.warning(
                    "Не удалось корректно ввести max_price за 5 попыток."
                )

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
            pass  # Игнорируем, если прелоадера не было

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

    def __init__(
        self, user_data_dir: str = "cian_profile", db_path: str = "cian/cian_data.db"
    ):
        self.user_data_dir = user_data_dir
        self.db_path = db_path  # Передаем путь к базе
        self.logger = logging.getLogger(self.__class__.__name__)

    async def _find_next_price_chunk(
        self, cian_page: "CianFilterPage", current_min: int, absolute_max: int
    ) -> dict[str, int]:
        MAX_LISTINGS = 1000
        IDEAL_MIN = 800
        PRICE_STEP = 10000  # Минимальный порог изменения цены (не ищем по 1 рублю)

        # 1. Сначала проверяем весь остаток
        await cian_page.select_price_range(current_min, absolute_max)
        total_count = await cian_page.get_listings_count()

        if total_count <= MAX_LISTINGS:
            return {"current_max": absolute_max, "listings_count": total_count}

        # 2. Если не влезло, готовим бинарный поиск
        low = current_min
        high = absolute_max
        best_max_for_chunk = current_min
        last_count = total_count

        # Хитрый ход: первым "mid" берем не середину, а пропорциональную точку
        # Например: если 1234 квартиры, а лимит 1000, проверим цену на уровне 80% диапазона
        first_guess_ratio = MAX_LISTINGS / total_count
        mid = current_min + int((absolute_max - current_min) * first_guess_ratio)
        # Округляем до шага цены
        mid = (mid // PRICE_STEP) * PRICE_STEP

        while low <= high:
            # В первую итерацию mid уже посчитан пропорционально,
            # в последующие — обычное деление пополам
            if mid < low or mid > high:
                mid = (low + high) // 2

            # Округляем mid до ближайшего шага, чтобы не частить
            mid = max(low, (mid // PRICE_STEP) * PRICE_STEP)

            await cian_page.select_price_range(current_min, mid)
            count = await cian_page.get_listings_count()

            self.logger.info(f"Поиск: {current_min}-{mid} (найдено {count})")

            if count > MAX_LISTINGS:
                high = mid - PRICE_STEP  # Уходим ниже с учетом шага
            else:
                best_max_for_chunk = mid
                last_count = count
                low = mid + PRICE_STEP  # Идем выше

                # Если мы попали в "золотую середину" или диапазон поиска схлопнулся
                if count >= IDEAL_MIN or (high - low) < PRICE_STEP:
                    break

            # Сбрасываем пропорциональный mid, чтобы дальше работал обычный бинарный поиск
            mid = -1

        return {"current_max": best_max_for_chunk, "listings_count": last_count}

    async def load_price_ranges(self, cian_page: "CianFilterPage", rooms_number: int):
        """
        Собирает все чанки цен, последовательно вызывая поиск границ.
        Сначала проверяет наличие чанка в базе данных.
        """
        ABSOLUTE_MAX_PRICE = 500_000_000
        chunks = []
        current_min = 0

        success = await cian_page.select_room_count(rooms_number)
        if not success:
            self.logger.error(
                f"Критическая ошибка: фильтр комнат не применен для {rooms_number} комнатных квартир. Выход."
            )

        # Открываем соединение с БД на время всего цикла сбора
        async with ChunkDatabase(self.db_path) as db:
            while current_min < ABSOLUTE_MAX_PRICE:
                # 1. Сначала спрашиваем базу данных
                cached_max = await db.get_max_price(rooms_number, current_min)

                if cached_max is not None:
                    # Если данные есть в БД - берем их, пропускаем парсинг
                    current_max = cached_max
                    self.logger.info(
                        f"Взято из БД (пропуск поиска): {current_min} - {current_max}"
                    )
                else:
                    # 2. Если данных нет - выполняем долгий сетевой запрос
                    self.logger.info(
                        f"Поиск чанка в браузере для min_price: {current_min}..."
                    )
                    next_chunk = await self._find_next_price_chunk(
                        cian_page, current_min, ABSOLUTE_MAX_PRICE
                    )
                    current_max = next_chunk.get("current_max")
                    listings_count = next_chunk.get("listings_count")

                    # 3. И сразу сохраняем результат в БД
                    await db.save_chunk(
                        rooms_number, current_min, current_max, listings_count
                    )
                    self.logger.info(
                        f"Новый чанк найден и сохранен в БД: {current_min} - {current_max}"
                    )

                chunks.append((current_min, current_max))

                # Следующий чанк начинается со следующего рубля
                current_min = current_max + 1

                # Ограничение для тестов (можно убрать в продакшене)
                # if len(chunks) >= 1:
                #     break

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
                viewport={"width": 1366, "height": 768},
            )

            page = context.pages[0] if context.pages else await context.new_page()
            cian_page = CianFilterPage(page)

            # --- Сценарий выполнения ---
            await cian_page.open()
            await cian_page.wait_for_human_captcha()

            await cian_page.human_scroll()
            await cian_page.random_sleep()

            for rooms_number in range(1, 7):
                await cian_page.scroll_to_top()

                await self.load_price_ranges(cian_page, rooms_number)

            # Завершение
            self.logger.info("Скрипт завершил работу")
            print("Готово. Скрипт ничего больше не делает.")
            await asyncio.to_thread(input, "Нажми Enter для закрытия браузера...")

            self.logger.info("Закрытие браузера")
            await context.close()


if __name__ == "__main__":
    scraper = CianScraper()
    asyncio.run(scraper.run())
