"""add_photos_to_listings_raw

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-04-25 00:00:00.000000

Add thumbnail_url (first photo preview) and photos_json (all photo URLs as JSONB)
to listings_raw so the frontend can display real images instead of placeholders.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("listings_raw", sa.Column("thumbnail_url", sa.Text(), nullable=True))
    op.add_column("listings_raw", sa.Column("photos_json", JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("listings_raw", "photos_json")
    op.drop_column("listings_raw", "thumbnail_url")
