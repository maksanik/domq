import asyncio
import random
import logging
from playwright.async_api import async_playwright, TimeoutError, Page

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


async def random_sleep(a=0.8, b=2.0):
    t = random.uniform(a, b)
    logger.info(f"Sleep {t:.2f} sec")
    await asyncio.sleep(t)


async def human_scroll(page, distance=1200, step=40):
    """Плавный скролл вниз и обратно вверх"""
    logger.info("Start human scroll")
    steps = distance // step

    # Скролл вниз
    for i in range(steps):
        delta = random.randint(int(step * 0.8), int(step * 1.2))
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(0.01, 0.05))
        logger.debug(f"Scroll down step {i + 1}/{steps}")

    # Лёгкая пауза наверху
    await asyncio.sleep(random.uniform(0.2, 0.5))

    # Скролл вверх
    for i in range(steps):
        delta = random.randint(int(step * 0.8), int(step * 1.2))
        await page.mouse.wheel(0, -delta)
        await asyncio.sleep(random.uniform(0.01, 0.05))
        logger.debug(f"Scroll up step {i + 1}/{steps}")

    logger.info("Finish human scroll")


async def wait_for_human_captcha():
    logger.info("Waiting for captcha solving")
    print("Если появилась капча — реши и нажми Enter...")
    await asyncio.to_thread(input)
    logger.info("Captcha solved (user confirmed)")


async def select_one_room(page):
    """Выбор 1-комнатной квартиры"""
    logger.info("Selecting 1-room apartment filter")

    try:
        logger.info("Open rooms filter")
        # Новый селектор для кнопки 'Комнатность'
        await page.click('div[data-testid="roomType"] button')
        await random_sleep()

        logger.info("Click 1-room option")
        # Опция 1-комнатной квартиры, оставляем прежний селектор, если структура списка не изменилась
        await page.click('div[data-name="SelectOption"]:has-text("1-комнатная")')
        await random_sleep()

        logger.info("1-room apartment selected")

    except TimeoutError:
        logger.error("Failed to select rooms filter")


async def select_price_range(page: Page, min_price: int, max_price: int):
    """Выбор цены"""

    try:
        logger.info("Open price filter")
        is_dropdowned = await page.query_selector_all(
            'div[data-testid="DropdownPrice"]'
        )

        if not is_dropdowned:
            await page.click('div[data-testid="price"] button')
            await random_sleep()

        logger.info(f"Selecting price from {min_price} to {max_price}")

        inputs = await page.query_selector_all(
            'div[data-testid="DropdownPrice"] input[data-name="MaskedInput"]'
        )
        min_input = inputs[0]
        max_input = inputs[1]

        while (await min_input.input_value()).replace(" ", "") != str(min_price):
            await min_input.click()
            await min_input.fill("")
            await min_input.type(str(min_price), delay=50)
            await random_sleep()

        await max_input.click()
        await max_input.fill("")
        await max_input.type(str(max_price), delay=50)
        await max_input.press("Enter")

        await random_sleep()
        logger.info("Price selected")

    except TimeoutError:
        logger.error("Failed to select price filter")


async def _get_listings_count_number(page: Page) -> int:
    """Выборка текущего количества листингов"""
    import re

    logger.info("Selecting current listings count")

    try:
        elements = await page.query_selector_all('div[data-testid="SummaryHeader"] h5')
        if not elements:
            logger.warning("Элементы не найдены")
            return 0

        text = await elements[0].text_content()  # <- важно await!
        number = int(re.sub(r"\D", "", text))  # type: ignore # удаляем все, кроме цифр

        logger.info(f"Найдено объявлений: {number}")
        return number

    except TimeoutError:
        logger.error("Failed to select rooms filter")
        return 0


async def select_current_listings_count(page: Page) -> int:
    listings_count = 0

    for i in range(10):
        if not await is_preloader_present(page):
            listings_count = await _get_listings_count_number(page)
            break
        else:
            logger.info(f"Waiting page to load {i + 1} times")
        await random_sleep()

    return listings_count


async def is_preloader_present(page: Page) -> bool:
    try:
        await page.wait_for_selector('div[data-testid="PagePreloader"]', timeout=1000)
        return True
    except Exception:
        return False


async def main():
    logger.info("Script started")

    async with async_playwright() as p:
        logger.info("Launching browser")
        context = await p.chromium.launch_persistent_context(
            user_data_dir="cian_profile",
            headless=False,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
        )

        logger.info("Opening new page")
        page = await context.new_page()

        logger.info("Go to CIAN website")
        await page.goto("https://www.cian.ru/kupit-kvartiru/")

        await wait_for_human_captcha()

        logger.info("Start scrolling")
        await human_scroll(page)
        await random_sleep()

        logger.info("Selecting apartment filter")
        await select_one_room(page)

        min_price = 10000000

        for _ in range(3):
            max_price = 100000000

            await select_price_range(page, min_price, max_price)

            listings_count = await select_current_listings_count(page)

            while listings_count and (listings_count > 1000 or listings_count < 800):
                step = max_price - ((max_price + min_price) // 2)

                if listings_count > 1000:
                    max_price = max_price - step
                else:
                    max_price = max_price + step

                await select_price_range(page, min_price, max_price)

                listings_count = await select_current_listings_count(page)

            min_price = max_price

        logger.info("Script finished")
        print("Готово. Скрипт ничего больше не делает.")
        await asyncio.to_thread(input, "Нажми Enter для закрытия...")

        logger.info("Closing browser")
        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
