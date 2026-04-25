import asyncio
import logging

from config import setup_logging
from db.db_manager import DatabaseManager


async def main():
    setup_logging()
    logger = logging.getLogger("purge_listings")
    logger.info("Очистка listings_raw и зависимых таблиц...")
    async with DatabaseManager() as db:
        counts = await db.purge_all_listings()
    for table, n in counts.items():
        logger.info(f"  {table}: удалено {n}")
    logger.info("Готово.")


asyncio.run(main())
