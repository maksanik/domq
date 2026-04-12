from datetime import date as date_type
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Скрапинг: технические таблицы
# ---------------------------------------------------------------------------


class PriceChunk(Base):
    """Ценовые диапазоны для обхода лимита 1000 объявлений Cian API."""

    __tablename__ = "price_chunks"
    __table_args__ = (UniqueConstraint("rooms_number", "min_price"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rooms_number: Mapped[int] = mapped_column(Integer, nullable=False)
    min_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    listings_count: Mapped[int] = mapped_column(Integer, nullable=False)
    scraped_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ListingRaw(Base):
    """Сырые данные объявлений — всё как пришло из источника."""

    __tablename__ = "listings_raw"
    __table_args__ = (UniqueConstraint("source", "external_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    external_id: Mapped[str] = mapped_column(String(50), nullable=False)
    url: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    area_total: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    area_kitchen: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    rooms: Mapped[Optional[int]] = mapped_column(Integer)
    floor: Mapped[Optional[int]] = mapped_column(Integer)
    floors_total: Mapped[Optional[int]] = mapped_column(Integer)
    latitude: Mapped[Optional[float]] = mapped_column(Numeric(10, 7))
    longitude: Mapped[Optional[float]] = mapped_column(Numeric(10, 7))
    address_text: Mapped[Optional[str]] = mapped_column(Text)
    # Денормализованные данные здания — нужны ETL для заполнения таблицы buildings
    year_built: Mapped[Optional[int]] = mapped_column(Integer)
    material_type: Mapped[Optional[str]] = mapped_column(String(50))
    images_count: Mapped[Optional[int]] = mapped_column(Integer)
    has_photos: Mapped[Optional[bool]] = mapped_column(Boolean)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    parsed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    normalized_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


# ---------------------------------------------------------------------------
# Нормализованная структура
# ---------------------------------------------------------------------------


class H3Cell(Base):
    """Гексагональные ячейки Uber H3 (resolution 9, ~174м)."""

    __tablename__ = "h3_cells"

    h3_index: Mapped[str] = mapped_column(String(15), primary_key=True)
    resolution: Mapped[int] = mapped_column(Integer, nullable=False)

    buildings: Mapped[list["Building"]] = relationship(back_populates="h3_cell")
    price_stats: Mapped[list["PriceStat"]] = relationship(back_populates="h3_cell")
    liquidity_stats: Mapped[list["LiquidityStat"]] = relationship(
        back_populates="h3_cell"
    )
    price_history: Mapped[list["PriceHistory"]] = relationship(back_populates="h3_cell")


class Building(Base):
    """Жилые здания. Дедупликация по H3 resolution 11 (~27м, уровень дома)."""

    __tablename__ = "buildings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    address: Mapped[Optional[str]] = mapped_column(Text)
    latitude: Mapped[Optional[float]] = mapped_column(Numeric(10, 7))
    longitude: Mapped[Optional[float]] = mapped_column(Numeric(10, 7))
    # H3 resolution 9 — для аналитики по районам
    h3_index: Mapped[Optional[str]] = mapped_column(
        String(15), ForeignKey("h3_cells.h3_index")
    )
    # H3 resolution 11 — уникальный ключ дедупликации (~27м = уровень дома)
    h3_index_r11: Mapped[Optional[str]] = mapped_column(String(15), unique=True)
    year_built: Mapped[Optional[int]] = mapped_column(Integer)
    floors_total: Mapped[Optional[int]] = mapped_column(Integer)
    material_type: Mapped[Optional[str]] = mapped_column(String(50))

    h3_cell: Mapped[Optional["H3Cell"]] = relationship(back_populates="buildings")
    flats: Mapped[list["Flat"]] = relationship(back_populates="building")


class Flat(Base):
    """Квартиры. Дедупликация по (building_id, rooms, floor) с допуском по площади."""

    __tablename__ = "flats"
    __table_args__ = (
        Index("ix_flats_building_rooms_floor", "building_id", "rooms", "floor"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    building_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("buildings.id"), nullable=False
    )
    area_total: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    area_kitchen: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    rooms: Mapped[Optional[int]] = mapped_column(Integer)
    floor: Mapped[Optional[int]] = mapped_column(Integer)

    building: Mapped["Building"] = relationship(back_populates="flats")
    listings: Mapped[list["Listing"]] = relationship(back_populates="flat")


class Listing(Base):
    """Активные объявления — нормализованная запись с историей присутствия."""

    __tablename__ = "listings"
    __table_args__ = (UniqueConstraint("raw_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raw_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("listings_raw.id"), nullable=False
    )
    flat_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("flats.id"), nullable=False
    )
    price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    raw: Mapped["ListingRaw"] = relationship()
    flat: Mapped["Flat"] = relationship(back_populates="listings")
    snapshots: Mapped[list["ListingSnapshot"]] = relationship(back_populates="listing")
    deal_analysis: Mapped[Optional["DealAnalysis"]] = relationship(
        back_populates="listing"
    )


class ListingSnapshot(Base):
    """История состояния объявления: цена и факт присутствия при каждом проходе."""

    __tablename__ = "listing_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("listings.id"), nullable=False
    )
    price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    is_online: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="False — объявление пропало в этом проходе"
    )
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    listing: Mapped["Listing"] = relationship(back_populates="snapshots")


# ---------------------------------------------------------------------------
# Аналитика
# ---------------------------------------------------------------------------


class PriceStat(Base):
    """Медианная и средняя цена м² по H3-соте и количеству комнат."""

    __tablename__ = "price_stats"
    __table_args__ = (UniqueConstraint("h3_index", "rooms"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    h3_index: Mapped[str] = mapped_column(
        String(15), ForeignKey("h3_cells.h3_index"), nullable=False
    )
    rooms: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    median_price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    listings_count: Mapped[int] = mapped_column(Integer, nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    h3_cell: Mapped["H3Cell"] = relationship(back_populates="price_stats")


class LiquidityStat(Base):
    """Средний и медианный срок продажи по H3-соте и количеству комнат."""

    __tablename__ = "liquidity_stats"
    __table_args__ = (UniqueConstraint("h3_index", "rooms"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    h3_index: Mapped[str] = mapped_column(
        String(15), ForeignKey("h3_cells.h3_index"), nullable=False
    )
    rooms: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_days_on_market: Mapped[Optional[int]] = mapped_column(Integer)
    median_days: Mapped[Optional[int]] = mapped_column(Integer)
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    h3_cell: Mapped["H3Cell"] = relationship(back_populates="liquidity_stats")


class PriceHistory(Base):
    """Ежедневная медианная цена м² — для графиков динамики цен."""

    __tablename__ = "price_history"
    __table_args__ = (UniqueConstraint("h3_index", "rooms", "date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    h3_index: Mapped[str] = mapped_column(
        String(15), ForeignKey("h3_cells.h3_index"), nullable=False
    )
    rooms: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[date_type] = mapped_column(Date, nullable=False)
    median_price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))

    h3_cell: Mapped["H3Cell"] = relationship(back_populates="price_history")


class PricePrediction(Base):
    """Результаты предиктивной оценки стоимости квартиры."""

    __tablename__ = "price_predictions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("listings.id"),
        nullable=True,
        comment="Для пост-фактум верификации точности прогноза",
    )
    input_params: Mapped[dict] = mapped_column(JSONB, nullable=False)
    predicted_price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class DealAnalysis(Base):
    """Оценка выгодности объявления относительно медианы соты."""

    __tablename__ = "deal_analysis"
    __table_args__ = (UniqueConstraint("listing_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("listings.id"), nullable=False
    )
    median_price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    actual_price_per_m2: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    discount_percent: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    is_hot_deal: Mapped[bool] = mapped_column(Boolean, nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    listing: Mapped["Listing"] = relationship(back_populates="deal_analysis")
