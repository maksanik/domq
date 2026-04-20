"""fix_deactivated_normalized_at

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-19 00:00:00.000000

One-time data fix: rows that were deactivated before the code fix that resets
normalized_at on deactivation still have a stale normalized_at value, so the
ETL would never re-process them to propagate is_active = false to listings.
This migration clears normalized_at for all such rows so the ETL picks them up.
"""

from alembic import op

revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE listings_raw
        SET normalized_at = NULL
        WHERE is_active = FALSE
          AND normalized_at IS NOT NULL
    """)


def downgrade() -> None:
    pass
