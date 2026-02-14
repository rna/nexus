import asyncio
import json
import os
import random
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Error, Page
from asyncio import TimeoutError

from models import upsert_product, engine, create_db_and_tables
from sqlmodel.ext.asyncio.session import AsyncSession
from tasks import (
    get_url_for_processing, mark_url_as_done, push_to_dlq,
    get_cache, set_cache, r, QUEUE_NAME, PROCESSING_QUEUE_NAME
)
from browser_manager import BrowserManager
import extraction
from logger import get_logger

logger = get_logger(__name__)

# --- Configuration ---
PROXY_URL = os.environ.get("PROXY_URL")

async def scrape_product(browser_manager: BrowserManager, url: str) -> Optional[Dict[str, Any]]:
    """
    Orchestrates the scraping of a single product page.
    Returns the extracted product data dict on success, None on failure.
    """
    page = None
    site_config = get_site_config(url)
    site_selectors = site_config.get("selectors") if site_config else None

    for attempt in range(5): # 5 retries
        try:
            page = await browser_manager.get_page()

            await asyncio.sleep(random.uniform(1, 3))
            response = await page.goto(url, wait_until="domcontentloaded")

            await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            await asyncio.sleep(random.uniform(0.5, 1.5))

            await handle_cookie_banner(page)
            await human_like_scroll(page)

            if response.status in [403, 429, 503]:
                raise Exception(f"Received status {response.status}")

            await asyncio.sleep(random.uniform(1, 2))

            product_data_json = await extraction.extract_from_json_ld(page)
            product_to_upsert = None

            if product_data_json:
                logger.info(f"Extracted data from JSON-LD for {url}")
                product_to_upsert = normalize_json_ld_data(product_data_json, url)
            elif site_selectors:
                logger.info(f"Falling back to CSS selectors for {url}")
                css_extracted_data = await extraction.extract_with_css(page, site_selectors)
                if css_extracted_data:
                    product_to_upsert = normalize_css_data(css_extracted_data, url)

            if not product_to_upsert:
                logger.warning(f"No data extracted for {url}. Skipping.")
                return {} # Return empty dict to signify "done, but no data"

            if not all([product_to_upsert.get("sku"), product_to_upsert.get("product_name"), product_to_upsert.get("price_amount") is not None]):
                logger.warning(f"Incomplete data for {url}. Skipping.")
                return {} # Also "done"

            return product_to_upsert # Success, return data
        except (TimeoutError, Error) as e:
            logger.warning(f"Network/Proxy error on attempt {attempt + 1} for {url}: {e}")
            logger.info("Forcing proxy rotation.")
            await browser_manager.new_context()
            await asyncio.sleep(random.uniform(0.5, 1.5) + 2 ** attempt)
        except Exception as e:
            logger.error(f"An unexpected error occurred on attempt {attempt + 1} for {url}: {e}", exc_info=True)
            await asyncio.sleep(2 ** attempt)
        finally:
            if page:
                await page.close()
    
    logger.error(f"Failed to scrape {url} after all retries.")
    return None # Failure

# --- Helper Functions ---

async def handle_cookie_banner(page: Page):
    """Looks for and accepts common cookie consent banners."""
    cookie_selectors = [
        'button:has-text("Accept")', 'button:has-text("Agree")', 'button:has-text("OK")',
        '[id*="consent"] button:has-text("Accept")', '[class*="cookie"] button:has-text("Accept")',
    ]
    for selector in cookie_selectors:
        try:
            await page.locator(selector).first.click(timeout=2000)
            logger.info("Clicked cookie banner.")
            return
        except (TimeoutError, Error):
            pass

async def human_like_scroll(page: Page):
    """Scrolls down the page in a human-like manner."""
    logger.info("Scrolling page...")
    total_height = await page.evaluate("document.body.scrollHeight")
    for i in range(1, total_height, random.randint(300, 600)):
        await page.mouse.wheel(0, i)
        await asyncio.sleep(random.uniform(0.3, 1.0))
        total_height = await page.evaluate("document.body.scrollHeight") # Recalculate for lazy loading

def get_site_config(url: str) -> Optional[Dict[str, Any]]:
    """Loads a site-specific configuration file."""
    try:
        domain = urlparse(url).netloc.replace("www.", "")
        config_path = os.path.join("sites", f"{domain}.json")
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info(f"No configuration file found for {domain}")
        return None
    except Exception as e:
        logger.error(f"Error loading configuration for {url}: {e}", exc_info=True)
        return None

def normalize_json_ld_data(data: dict, url: str) -> dict:
    """Normalizes data from JSON-LD to the product schema."""
    offers = data.get("offers", [{}])[0] if isinstance(data.get("offers"), list) else data.get("offers", {})
    return {
        "sku": data.get("sku"),
        "brand": data.get("brand", {}).get("name"),
        "product_name": data.get("name"),
        "price_amount": float(offers.get("price", 0.0)),
        "currency": offers.get("priceCurrency"),
        "availability_status": "InStock" if "InStock" in offers.get("availability", "") else "OutOfStock",
        "ingredients_list": data.get("description"),
        "image_url": data.get("image"),
        "product_url": url,
    }

def normalize_css_data(data: dict, url: str) -> dict:
    """Normalizes data from CSS selectors to the product schema."""
    return {
        "sku": data.get("sku"),
        "brand": data.get("brand"),
        "product_name": data.get("product_name"),
        "price_amount": float(data.get("price_amount", "0.0").replace("$", "").strip()),
        "currency": "USD",
        "availability_status": data.get("availability_status"),
        "ingredients_list": data.get("ingredients_list"),
        "image_url": data.get("image_url"),
        "product_url": url,
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
        browser_manager = BrowserManager(p, PROXY_URL)
        await browser_manager.launch_browser()

        logger.info("Worker started. Waiting for URLs from Redis queue...")
        while True:
            url = get_url_for_processing()
            if not url:
                await asyncio.sleep(10)
                continue

            logger.info(f"Processing URL: {url}")
            
            # 1. Check cache first
            cached_data = get_cache(url)
            if cached_data:
                logger.info(f"Cache hit for {url}. Processing cached data.")
                product_data = cached_data
            else:
                # 2. If no cache, scrape the product
                logger.info(f"Cache miss for {url}. Scraping page.")
                product_data = await scrape_product(browser_manager, url)

            # 3. Process the result
            if product_data is not None:
                if product_data: # If data is not an empty dict
                    # Cache the new result
                    set_cache(url, product_data)
                    # Upsert to DB
                    async with AsyncSession(engine) as session:
                        await upsert_product(session, product_data)
                    logger.info(f"Successfully processed and saved {product_data.get('product_name')}")
                
                # Mark as done whether data was found or not (empty dict means skip)
                mark_url_as_done(url)
            else:
                # Scrape failed after all retries, move from processing to DLQ
                mark_url_as_done(url) # Remove from processing
                push_to_dlq(url)      # Add to DLQ

if __name__ == "__main__":
    from tasks import push_url_to_queue
    if r.llen(QUEUE_NAME) == 0 and r.llen(PROCESSING_QUEUE_NAME) == 0:
         push_url_to_queue("https://www.sephora.com/product/the-ordinary-deciem-niacinamide-10-zinc-1-P427417")
    
    asyncio.run(main())
