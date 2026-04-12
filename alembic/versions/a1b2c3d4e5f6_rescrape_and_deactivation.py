"""rescrape_and_deactivation

Revision ID: a1b2c3d4e5f6
Revises: 706d58a068c7
Create Date: 2026-04-12 20:00:00.000000

Changes:
- price_chunks.scraped (bool) → scraped_at (timestamptz, nullable)
  Позволяет переобходить чанки раз в неделю вместо однократного флага.
- listings_raw.is_active (bool, default true)
  Помечается False, если объявление не появилось ни в одном чанке за прогон.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "706d58a068c7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # price_chunks: заменяем bool-флаг на timestamp
    op.add_column(
        "price_chunks",
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Переносим существующие scraped=True → текущее время (условная дата),
    # scraped=False остаются NULL (будут переобработаны)
    op.execute("UPDATE price_chunks SET scraped_at = now() WHERE scraped = TRUE")
    op.drop_column("price_chunks", "scraped")

    # listings_raw: добавляем флаг активности
    op.add_column(
        "listings_raw",
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("TRUE"),
        ),
    )


def downgrade() -> None:
    op.drop_column("listings_raw", "is_active")

    op.add_column(
        "price_chunks",
        sa.Column(
            "scraped",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
    )
    op.execute("UPDATE price_chunks SET scraped = TRUE WHERE scraped_at IS NOT NULL")
    op.drop_column("price_chunks", "scraped_at")
