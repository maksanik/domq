"""add knn_predicted_price to price_predictions

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-04-27
"""

from alembic import op
import sqlalchemy as sa

revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "price_predictions",
        sa.Column(
            "knn_predicted_price",
            sa.Numeric(precision=15, scale=2),
            nullable=True,
            comment="Прогноз методом KNN (k=10), для сравнения с медианной H3",
        ),
    )


def downgrade() -> None:
    op.drop_column("price_predictions", "knn_predicted_price")
