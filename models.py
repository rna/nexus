from typing import Optional
from datetime import datetime

from sqlmodel import Field, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession, AsyncEngine, create_async_engine
from sqlalchemy.dialects.postgresql import insert
import os

class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str = Field(index=True, unique=True)
    brand: str
    product_name: str
    price_amount: float
    currency: str
    availability_status: str
    ingredients_list: Optional[str] = None
    image_url: Optional[str] = None
    product_url: str = Field(unique=True)
    scraped_at: datetime = Field(default_factory=datetime.utcnow, nullable=False)

# The database URL must be updated to use the asyncpg driver
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/nexus")
ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

engine = create_async_engine(ASYNC_DATABASE_URL, echo=False)

async def create_db_and_tables():
    async with engine.begin() as conn:
        # await conn.run_sync(SQLModel.metadata.drop_all) # Use for testing if you need to drop tables
        await conn.run_sync(SQLModel.metadata.create_all)

async def upsert_product(session: AsyncSession, product_data: dict):
    """
    Asynchronously inserts or updates a product in the database based on the SKU.
    """
    # The dialect-specific insert statement needs to be handled carefully in an async context
    # A simple merge is often easier and more database-agnostic with async sessions
    
    # Check if product exists
    result = await session.get(Product, product_data.get("sku"))
    if result:
        # It's not straightforward to get the existing product with a unique constraint other than PK
        # For this implementation, we will assume SKU is the primary key for simplicity in async
        # A more robust solution might involve a select and then an update.
        # However, for the UPSERT pattern, `on_conflict_do_update` is ideal but requires raw execute.
        
        stmt = insert(Product).values(**product_data)
        update_dict = {c.name: c for c in stmt.excluded if c.name not in ["sku"]}
        final_stmt = stmt.on_conflict_do_update(index_elements=['sku'], set_=update_dict)
        
        await session.execute(final_stmt)
    else:
        # Insert new product
        new_product = Product(**product_data)
        session.add(new_product)
        
    await session.commit()
