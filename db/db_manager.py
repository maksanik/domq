import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert

from db.models import ListingRaw, PriceChunk
from db.session import AsyncSessionLocal


class DatabaseManager:
    """Async context manager для работы с PostgreSQL через SQLAlchemy AsyncSession."""

    def __init__(self):
        self.session: AsyncSession = None  # type: ignore[assignment]
        self.logger = logging.getLogger(self.__class__.__name__)

    async def __aenter__(self) -> "DatabaseManager":
        self.session = AsyncSessionLocal()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type:
                await self.session.rollback()
            await self.session.close()

    # ------------------------------------------------------------------
    # price_chunks
    # ------------------------------------------------------------------

    async def get_max_price(self, rooms_number: int, min_price: int) -> Optional[int]:
        result = await self.session.execute(
            select(PriceChunk.max_price).where(
                PriceChunk.rooms_number == rooms_number,
                PriceChunk.min_price == min_price,
            )
        )
        return result.scalar_one_or_none()

    async def save_chunk(
        self, rooms_number: int, min_price: int, max_price: int, listings_count: int
    ):
        try:
            stmt = (
                insert(PriceChunk)
                .values(
                    rooms_number=rooms_number,
                    min_price=min_price,
                    max_price=max_price,
                    listings_count=listings_count,
                )
                .on_conflict_do_nothing(index_elements=["rooms_number", "min_price"])
            )
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            self.logger.error(f"Ошибка сохранения чанка: {e}")

    async def mark_chunk_scraped(self, rooms_number: int, min_price: int):
        await self.session.execute(
            update(PriceChunk)
            .where(
                PriceChunk.rooms_number == rooms_number,
                PriceChunk.min_price == min_price,
            )
            .values(scraped_at=datetime.now(timezone.utc))
        )
        await self.session.commit()

    async def reset_all_chunks(self) -> int:
        """Сбрасывает scraped_at в NULL для всех чанков. Возвращает количество затронутых строк."""
        result = await self.session.execute(
            update(PriceChunk).values(scraped_at=None).returning(PriceChunk.id)
        )
        count = len(result.all())
        await self.session.commit()
        return count

    async def get_unscraped_chunks(self) -> list[dict]:
        stale_threshold = datetime.now(timezone.utc) - timedelta(days=1)
        result = await self.session.execute(
            select(
                PriceChunk.rooms_number,
                PriceChunk.min_price,
                PriceChunk.max_price,
                PriceChunk.listings_count,
            )
            .where(
                (PriceChunk.scraped_at == None)  # noqa: E711
                | (PriceChunk.scraped_at < stale_threshold)
            )
            .order_by(PriceChunk.rooms_number, PriceChunk.min_price)
        )
        return [row._asdict() for row in result.all()]

    async def deactivate_unseen_listings(self, since: datetime) -> int:
        """Помечает is_active=False все объявления, не обновлявшиеся с момента since.

        Вызывается после завершения полного прогона: любое объявление, которое
        не появилось ни в одном чанке, считается снятым с публикации.
        Возвращает количество деактивированных записей.
        """
        result = await self.session.execute(
            update(ListingRaw)
            .where(ListingRaw.is_active == True, ListingRaw.parsed_at < since)  # noqa: E712
            .values(is_active=False, normalized_at=None)
            .returning(ListingRaw.id)
        )
        deactivated = len(result.all())
        await self.session.commit()
        return deactivated

    # ------------------------------------------------------------------
    # listings_raw
    # ------------------------------------------------------------------

    async def purge_all_listings(self) -> dict:
        """Полная очистка listings_raw и всех зависимых таблиц."""
        from db.models import (
            Listing,
            DealAnalysis,
            ListingSnapshot,
            PriceStat,
            LiquidityStat,
            PriceHistory,
        )

        counts = {}
        for model in [
            DealAnalysis,
            ListingSnapshot,
            Listing,
            PriceStat,
            LiquidityStat,
            PriceHistory,
            ListingRaw,
        ]:
            result = await self.session.execute(delete(model).returning(model.id))
            counts[model.__tablename__] = len(result.all())
        await self.session.commit()
        return counts

    async def save_raw_listing(self, listing: dict):
        """Сохраняет сырое объявление. При конфликте (source, external_id) — пропускает."""
        try:
            stmt = (
                insert(ListingRaw)
                .values(
                    source=listing.get("source"),
                    external_id=listing.get("external_id"),
                    url=listing.get("url"),
                    title=listing.get("title"),
                    description=listing.get("description"),
                    price=listing.get("price"),
                    area_total=listing.get("area_total"),
                    area_kitchen=listing.get("area_kitchen"),
                    rooms=listing.get("rooms"),
                    floor=listing.get("floor"),
                    floors_total=listing.get("floors_total"),
                    latitude=listing.get("latitude"),
                    longitude=listing.get("longitude"),
                    address_text=listing.get("address_text"),
                    year_built=listing.get("year_built"),
                    material_type=listing.get("material_type"),
                    images_count=listing.get("images_count"),
                    has_photos=listing.get("has_photos"),
                    thumbnail_url=listing.get("thumbnail_url"),
                    photos_json=listing.get("photos_json"),
                    created_at=listing.get("created_at"),
                )
                .on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={
                        "price": listing.get("price"),
                        "title": listing.get("title"),
                        "images_count": listing.get("images_count"),
                        "has_photos": listing.get("has_photos"),
                        "thumbnail_url": listing.get("thumbnail_url"),
                        "photos_json": listing.get("photos_json"),
                        "parsed_at": datetime.now(timezone.utc),
                        "is_active": True,
                        "normalized_at": None,
                    },
                )
            )
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            self.logger.error(
                f"Ошибка сохранения объявления {listing.get('external_id')}: {e}"
            )
