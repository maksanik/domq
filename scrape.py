import argparse
import asyncio

from avito.scrapper import AvitoScraper
from cian.pagination_scrapper import PaginationScraper
from config import AVITO_USER_DATA_DIR, USER_DATA_DIR, setup_logging
from db.db_manager import DatabaseManager


async def main(rescrape: bool = False):
    async with DatabaseManager() as db:
        if rescrape:
            count = await db.reset_all_chunks()
            print(f"--rescrape: сброшено {count} чанков")
        chunks = await db.get_unscraped_chunks()

    print(f"Чанков для скрапинга: {len(chunks)}")

    captcha_event = asyncio.Event()

    async def wait_and_signal():
        print("Открыты два браузера: Cian и Avito.")
        print("Реши капчу в обоих окнах, затем нажми Enter...")
        await asyncio.to_thread(input)
        captcha_event.set()

    cian = PaginationScraper(USER_DATA_DIR)
    avito = AvitoScraper(AVITO_USER_DATA_DIR)

    await asyncio.gather(
        cian.run(captcha_event=captcha_event, chunks=chunks),
        avito.run(captcha_event=captcha_event, chunks=chunks),
        wait_and_signal(),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Запуск Cian + Avito скраперов параллельно"
    )
    parser.add_argument(
        "--rescrape",
        action="store_true",
        help="Сбросить все чанки в 'не скрапировано' перед стартом",
    )
    args = parser.parse_args()

    setup_logging()
    asyncio.run(main(rescrape=args.rescrape))
