import aiosqlite
import logging
from typing import Optional


class DatabaseManager:
    """Класс для взаимодействия с SQLite БД."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: aiosqlite.Connection = None  # type: ignore
        self.logger = logging.getLogger(self.__class__.__name__)

    async def __aenter__(self):
        import os

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        self.db = await aiosqlite.connect(self.db_path)
        await self._init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            await self.db.close()

    async def _init_db(self):
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
        # В будущем здесь добавится CREATE TABLE listings ...
        await self.db.commit()

    async def get_max_price(self, rooms_number: int, min_price: int) -> Optional[int]:
        async with self.db.execute(
            "SELECT max_price FROM price_chunks WHERE rooms_number = ? AND min_price = ?",
            (rooms_number, min_price),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def save_chunk(
        self, rooms_number: int, min_price: int, max_price: int, listings_count: int
    ):
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
