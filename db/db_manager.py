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
                scraped INTEGER DEFAULT 0,
                UNIQUE(rooms_number, min_price)
            )
        """)
        # Миграция: добавляем столбец scraped если таблица уже существовала без него
        try:
            await self.db.execute(
                "ALTER TABLE price_chunks ADD COLUMN scraped INTEGER DEFAULT 0"
            )
        except Exception:
            pass

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY,
                rooms_count INTEGER,
                price INTEGER,
                total_area REAL,
                floor_number INTEGER,
                floors_count INTEGER,
                address TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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

    async def save_listing(self, listing: dict):
        try:
            await self.db.execute(
                """INSERT OR IGNORE INTO listings
                   (id, rooms_count, price, total_area, floor_number, floors_count, address)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    listing["id"],
                    listing.get("rooms_count"),
                    listing.get("price"),
                    listing.get("total_area"),
                    listing.get("floor_number"),
                    listing.get("floors_count"),
                    listing.get("address"),
                ),
            )
            await self.db.commit()
        except Exception as e:
            self.logger.error(f"Ошибка сохранения объявления {listing.get('id')}: {e}")

    async def mark_chunk_scraped(self, rooms_number: int, min_price: int):
        await self.db.execute(
            "UPDATE price_chunks SET scraped = 1 WHERE rooms_number = ? AND min_price = ?",
            (rooms_number, min_price),
        )
        await self.db.commit()

    async def get_unscraped_chunks(self) -> list[dict]:
        async with self.db.execute(
            """SELECT rooms_number, min_price, max_price, listings_count
               FROM price_chunks
               WHERE scraped = 0 OR scraped IS NULL
               ORDER BY rooms_number, min_price"""
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "rooms_number": r[0],
                    "min_price": r[1],
                    "max_price": r[2],
                    "listings_count": r[3],
                }
                for r in rows
            ]
