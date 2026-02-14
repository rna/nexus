from typing import Optional
from datetime import datetime

from sqlmodel import Field, SQLModel, create_engine, Session
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


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/nexus")
engine = create_engine(DATABASE_URL)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def upsert_product(session: Session, product_data: dict):
    """
    Inserts or updates a product in the database based on the SKU.
    """
    stmt = insert(Product).values(**product_data)

    update_dict = {
        c.name: c
        for c in stmt.excluded
        if c.name not in ["sku"]
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=['sku'],
        set_=update_dict,
    )
    session.execute(stmt)
    session.commit()
