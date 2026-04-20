"""widen_discount_percent

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-20 00:00:00.000000

Widen deal_analysis.discount_percent from NUMERIC(5,2) to NUMERIC(7,2) so that
extreme outlier listings (price >> or << median) don't overflow the column.
"""

from alembic import op
import sqlalchemy as sa

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "deal_analysis",
        "discount_percent",
        type_=sa.Numeric(precision=7, scale=2),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "deal_analysis",
        "discount_percent",
        type_=sa.Numeric(precision=5, scale=2),
        existing_nullable=True,
    )
