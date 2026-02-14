import asyncio
import json
import os
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from models import upsert_product, engine, create_db_and_tables
from sqlmodel.ext.asyncio.session import AsyncSession
from tasks import (
    get_url_for_processing, mark_url_as_done, push_to_dlq,
    get_cache, set_cache, r, SEEN_URLS_SET, push_urls_to_queue
)
from fetchers.base import Fetcher
from fetchers.playwright_fetcher import PlaywrightFetcher
import extraction
from logger import get_logger

logger = get_logger(__name__)

# --- Configuration ---
PROXY_URL = os.environ.get("PROXY_URL")

async def process_url(fetcher: Fetcher, url: str) -> Optional[Dict[str, Any]]:
    """
    Orchestrates the fetching and extraction of a single product page.
    Returns the extracted product data dict on success, None on failure.
    """
    page = None
    try:
        logger.info(f"Fetching URL: {url}")
        fetch_result = await fetcher.fetch(url)

        if not fetch_result:
            logger.error(f"Failed to fetch {url} after all retries.")
            return None

        page = fetch_result.page_source # The Playwright page object
        
        site_config = get_site_config(url)
        site_selectors = site_config.get("selectors") if site_config else None

        product_data_json = await extraction.extract_from_json_ld(page)
        
        if product_data_json:
            logger.info(f"Extracted data from JSON-LD for {url}")
            return normalize_json_ld_data(product_data_json, url)
        
        if site_selectors:
            logger.info(f"Falling back to CSS selectors for {url}")
            css_extracted_data = await extraction.extract_with_css(page, site_selectors)
            if css_extracted_data:
                return normalize_css_data(css_extracted_data, url)

        logger.warning(f"No data extracted for {url}.")
        return {} # Done, but no data

    finally:
        if page:
            await page.close()

# --- Helper Functions ---

def get_site_config(url: str) -> Optional[Dict[str, Any]]:
    """Loads a site-specific configuration file."""
    try:
        domain = urlparse(url).netloc.replace("www.", "")
        config_path = os.path.join("sites", f"{domain}.json")
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error loading configuration for {url}: {e}", exc_info=True)
        return None

def normalize_json_ld_data(data: dict, url: str) -> dict:
    """Normalizes data from JSON-LD to the product schema."""
    offers = data.get("offers", [{}])[0] if isinstance(data.get("offers"), list) else data.get("offers", {})
    return {
        "sku": data.get("sku"), "brand": data.get("brand", {}).get("name"), "product_name": data.get("name"),
        "price_amount": float(offers.get("price", 0.0)), "currency": offers.get("priceCurrency"),
        "availability_status": "InStock" if "InStock" in offers.get("availability", "") else "OutOfStock",
        "ingredients_list": data.get("description"), "image_url": data.get("image"), "product_url": url,
    }

def normalize_css_data(data: dict, url: str) -> dict:
    """Normalizes data from CSS selectors to the product schema."""
    return {
        "sku": data.get("sku"), "brand": data.get("brand"), "product_name": data.get("product_name"),
        "price_amount": float(data.get("price_amount", "0.0").replace("$", "").strip()), "currency": "USD",
        "availability_status": data.get("availability_status"), "ingredients_list": data.get("ingredients_list"),
        "image_url": data.get("image_url"), "product_url": url,
    }

# --- Main Application Logic ---

async def init_db():
    """Initializes the database and tables with retry logic."""
    logger.info("Initializing database...")
    for i in range(5):
        try:
            await create_db_and_tables()
            logger.info("Database initialized successfully.")
            return
        except Exception as e:
            logger.error(f"Database connection failed (attempt {i+1}/5): {e}", exc_info=True)
            await asyncio.sleep(5)
    logger.critical("Could not connect to the database. Exiting.")
    exit(1)

async def main():
    """Main function to set up and run the scraper worker."""
    await init_db()
    async with async_playwright() as p:
        fetcher = PlaywrightFetcher(p, PROXY_URL)
        await fetcher.launch()

        logger.info("Worker started. Waiting for URLs from Redis queue...")
        while True:
            url = get_url_for_processing()
            if not url:
                await asyncio.sleep(10)
                continue
            
            product_data = get_cache(url)
            if product_data:
                logger.info(f"Cache hit for {url}. Processing cached data.")
            else:
                logger.info(f"Cache miss for {url}. Processing URL...")
                product_data = await process_url(fetcher, url)

            if product_data is not None:
                if product_data:
                    if not get_cache(url): # Don't cache a cache hit
                        set_cache(url, product_data)
                    async with AsyncSession(engine) as session:
                        await upsert_product(session, product_data)
                    logger.info(f"Successfully processed and saved {product_data.get('product_name')}")
                mark_url_as_done(url)
            else:
                mark_url_as_done(url)
                push_to_dlq(url)
        
        await fetcher.close()

if __name__ == "__main__":
    if r.scard(SEEN_URLS_SET) == 0:
         push_urls_to_queue(["https://www.sephora.com/product/the-ordinary-deciem-niacinamide-10-zinc-1-P427417"])
    
    asyncio.run(main())
