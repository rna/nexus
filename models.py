import os
from datetime import datetime, timezone
import hashlib
import json
from typing import Optional

from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession, create_async_engine

class Product(SQLModel, table=True):
    # Core product identifiers
    sku: str = Field(primary_key=True)
    product_url: str = Field(index=True, unique=True)
    
    # Scraped data
    brand: Optional[str] = Field(index=True)
    product_name: Optional[str] = Field(index=True)
    price_amount: Optional[float] = None
    currency: Optional[str] = None
    availability_status: Optional[str] = None
    ingredients_list: Optional[str] = None
    image_url: Optional[str] = None
    
    # Metadata for tracking and idempotency
    first_scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version_hash: str # SHA-256 hash of the product data dictionary

# --- Database Engine ---

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/nexus")
ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

engine = create_async_engine(ASYNC_DATABASE_URL, echo=False)

async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

# --- Idempotent Upsert Logic ---

def generate_version_hash(data: dict) -> str:
    """Creates a SHA-256 hash of a dictionary to track data versions."""
    # Sort the dictionary to ensure consistent hash generation
    dhash = hashlib.sha256()
    encoded = json.dumps(data, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()

async def upsert_products(session: AsyncSession, products_data: list[dict]):
    """
    Asynchronously and idempotently inserts or updates a batch of products.
    - Only updates if the `version_hash` has changed.
    - Sets `first_scraped_at` on creation.
    - Updates `last_scraped_at` on every upsert.
    """
    if not products_data:
        return

    # Prepare values for insertion
    values_to_insert = []
    for p_data in products_data:
        values_to_insert.append({
            **p_data,
            "version_hash": generate_version_hash(p_data),
            "last_scraped_at": datetime.now(timezone.utc),
        })

    # Create the UPSERT statement
    stmt = insert(Product).values(values_to_insert)

    # Define what to do on conflict (when SKU already exists)
    # These columns will be updated ONLY if the new `version_hash` is different
    # from the existing one.
    update_stmt = stmt.on_conflict_do_update(
        index_elements=['sku'],
        set_={
            "brand": stmt.excluded.brand,
            "product_name": stmt.excluded.product_name,
            "price_amount": stmt.excluded.price_amount,
            "currency": stmt.excluded.currency,
            "availability_status": stmt.excluded.availability_status,
            "ingredients_list": stmt.excluded.ingredients_list,
            "image_url": stmt.excluded.image_url,
            "product_url": stmt.excluded.product_url,
            "last_scraped_at": stmt.excluded.last_scraped_at,
            "version_hash": stmt.excluded.version_hash,
        },
        where=(Product.version_hash != stmt.excluded.version_hash)
    )
    
    await session.execute(update_stmt)
    await session.commit()
