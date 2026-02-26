"""initial product table

Revision ID: 20260226_0001
Revises:
Create Date: 2026-02-26 00:00:01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260226_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "product",
        sa.Column("sku", sa.String(), nullable=False),
        sa.Column("product_url", sa.String(), nullable=False),
        sa.Column("brand", sa.String(), nullable=True),
        sa.Column("product_name", sa.String(), nullable=True),
        sa.Column("price_amount", sa.Float(), nullable=True),
        sa.Column("currency", sa.String(), nullable=True),
        sa.Column("availability_status", sa.String(), nullable=True),
        sa.Column("ingredients_list", sa.String(), nullable=True),
        sa.Column("image_url", sa.String(), nullable=True),
        sa.Column("source_site", sa.String(), nullable=True),
        sa.Column("source_product_id", sa.String(), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("first_scraped_at", sa.DateTime(), nullable=False),
        sa.Column("last_scraped_at", sa.DateTime(), nullable=False),
        sa.Column("version_hash", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("sku"),
    )

    op.create_index(op.f("ix_product_brand"), "product", ["brand"], unique=False)
    op.create_index(op.f("ix_product_product_name"), "product", ["product_name"], unique=False)
    op.create_index(op.f("ix_product_product_url"), "product", ["product_url"], unique=True)
    op.create_index(op.f("ix_product_source_product_id"), "product", ["source_product_id"], unique=False)
    op.create_index(op.f("ix_product_source_site"), "product", ["source_site"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_product_source_site"), table_name="product")
    op.drop_index(op.f("ix_product_source_product_id"), table_name="product")
    op.drop_index(op.f("ix_product_product_url"), table_name="product")
    op.drop_index(op.f("ix_product_product_name"), table_name="product")
    op.drop_index(op.f("ix_product_brand"), table_name="product")
    op.drop_table("product")

