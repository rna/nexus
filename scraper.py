import asyncio
import json
import random
import os
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright, Error
from playwright_stealth import stealth_async
from asyncio import TimeoutError

from models import upsert_product, engine, create_db_and_tables
from sqlmodel import Session
from user_agents import get_random_user_agent
from fingerprints import get_fingerprint, get_override_script

# --- Configuration ---
PROXY_URL = os.environ.get("PROXY_URL")
REQUEST_COUNT_PER_PROXY_SESSION = random.randint(5, 10)


class Scraper:
    def __init__(self, playwright: Playwright):
        self.playwright = playwright
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.request_count = 0
        self.fingerprint: Optional[dict] = None

    async def setup_browser(self):
        """Initializes the browser and a new browser context with stealth settings."""
        self.browser = await self.playwright.chromium.launch(headless=True)
        await self.new_context()

    async def new_context(self):
        """Creates a new browser context with randomized settings and proxy."""
        if self.context:
            await self.context.close()

        self.fingerprint = get_fingerprint()
        override_script = get_override_script(self.fingerprint)

        user_agent = get_random_user_agent()
        viewport = {
            "width": random.randint(1280, 1920),
            "height": random.randint(720, 1080),
        }

        context_options = {
            "user_agent": user_agent,
            "viewport": viewport,
            "java_script_enabled": True,
        }

        if PROXY_URL:
            context_options["proxy"] = {"server": PROXY_URL}

        self.context = await self.browser.new_context(**context_options)
        await self.context.add_init_script(override_script)
        self.context.set_default_navigation_timeout(60 * 1000) # 60 seconds

        # Disable loading of images, css, and fonts
        await self.context.route("**/*", self._intercept_requests)

    async def _intercept_requests(self, route):
        """Intercepts requests to block resource-heavy content."""
        if route.request.resource_type in ["image", "stylesheet", "font"]:
            await route.abort()
        else:
            await route.continue_()

    async def get_page(self) -> Page:
        """
        Gets a new page from the current context. Handles proxy rotation.
        """
        if self.request_count >= REQUEST_COUNT_PER_PROXY_SESSION:
            print("Rotating proxy session...")
            await self.new_context()
            self.request_count = 0

        self.request_count += 1
        page = await self.context.new_page()
        await stealth_async(page)
        return page

    async def handle_cookie_banner(self, page: Page):
        """Looks for and accepts common cookie consent banners."""
        cookie_selectors = [
            'button:has-text("Accept")',
            'button:has-text("Agree")',
            'button:has-text("OK")',
            '[id*="consent"] button:has-text("Accept")',
            '[class*="cookie"] button:has-text("Accept")',
        ]
        for selector in cookie_selectors:
            try:
                banner_button = page.locator(selector).first
                await banner_button.click(timeout=2000)
                print("Clicked cookie banner.")
                return
            except (TimeoutError, Error):
                pass # Button not found or not clickable, try next selector

    async def human_like_scroll(self, page: Page):
        """Scrolls down the page in a human-like manner."""
        print("Scrolling page...")
        total_height = await page.evaluate("document.body.scrollHeight")
        viewport_height = page.viewport_size['height']
        current_scroll = 0
        
        while current_scroll < total_height:
            scroll_increment = random.randint(int(viewport_height * 0.4), int(viewport_height * 0.8))
            await page.mouse.wheel(0, scroll_increment)
            await asyncio.sleep(random.uniform(0.3, 1.0))
            current_scroll += scroll_increment
            # It's good practice to re-check total height in case of lazy loading
            total_height = await page.evaluate("document.body.scrollHeight")

    def get_site_config(self, url: str) -> Optional[Dict[str, Any]]:
        """Loads a site-specific configuration file from the 'sites' directory."""
        try:
            domain = urlparse(url).netloc
            # Remove 'www.' if it exists
            if domain.startswith("www."):
                domain = domain[4:]
            
            config_path = os.path.join("sites", f"{domain}.json")
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"No configuration file found for {domain}")
            return None
        except Exception as e:
            print(f"Error loading configuration for {url}: {e}")
            return None

    async def extract_from_json_ld(self, page: Page) -> Optional[Dict[str, Any]]:
        """Extracts product data from JSON-LD scripts."""
        try:
            json_ld_element = await page.query_selector('script[type="application/ld+json"]')
            if not json_ld_element:
                return None

            json_ld_text = await json_ld_element.inner_text()
            data = json.loads(json_ld_text)

            # Look for product information in the JSON-LD data
            if data.get("@type") == "Product":
                return data
            
            # Some sites nest it
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'Product':
                        return item

        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Could not parse JSON-LD: {e}")
        return None

    async def extract_with_css(self, page: Page, selectors: Dict[str, str]) -> Dict[str, Any]:
        """Fallback to extract data using CSS selectors."""
        product_data = {}
        for key, selector in selectors.items():
            try:
                element = await page.query_selector(selector)
                if element:
                    product_data[key] = await element.inner_text()
            except (TimeoutError, Error) as e:
                print(f"Could not extract {key} using selector '{selector}': {e}")
        return product_data

    async def scrape_product(self, url: str):
        """
        Scrapes a single product page.
        """
        page = None
        site_config = self.get_site_config(url)
        site_selectors = site_config.get("selectors") if site_config else None

        for attempt in range(5): # 5 retries
            try:
                page = await self.get_page()

                # Human-like delay before navigating
                await asyncio.sleep(random.uniform(1, 3))

                response = await page.goto(url, wait_until="domcontentloaded")

                # Human-like mouse movement and delay after loading
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                await asyncio.sleep(random.uniform(0.5, 1.5))

                # Handle cookie banners
                await self.handle_cookie_banner(page)

                # Scroll the page to load all content
                await self.human_like_scroll(page)

                if response.status in [403, 429, 503]:
                    raise Exception(f"Received status {response.status}")

                # Human-like delay before extraction
                await asyncio.sleep(random.uniform(1, 2))

                product_data_json = await self.extract_from_json_ld(page)
                product_to_upsert = None

                if product_data_json:
                    print(f"Extracted data from JSON-LD for {url}")
                    product_to_upsert = {
                        "sku": product_data_json.get("sku", f"SKU_MISSING_{hash(url)}"),
                        "brand": product_data_json.get("brand", {}).get("name", "Unknown"),
                        "product_name": product_data_json.get("name", "Unknown Product"),
                        "price_amount": float(product_data_json.get("offers", {}).get("price", 0.0)),
                        "currency": product_data_json.get("offers", {}).get("priceCurrency", "USD"),
                        "availability_status": "InStock" if "InStock" in product_data_json.get("offers", {}).get("availability", "") else "OutOfStock",
                        "ingredients_list": product_data_json.get("description"),
                        "image_url": product_data_json.get("image"),
                        "product_url": url,
                    }
                elif site_selectors:
                    print(f"Falling back to CSS selectors for {url}")
                    css_extracted_data = await self.extract_with_css(page, site_selectors)
                    if css_extracted_data:
                        product_to_upsert = {
                            "sku": css_extracted_data.get("sku", f"SKU_MISSING_{hash(url)}_CSS"),
                            "brand": css_extracted_data.get("brand", "Unknown"),
                            "product_name": css_extracted_data.get("product_name", "Unknown Product (CSS)"),
                            "price_amount": float(css_extracted_data.get("price_amount", "0.0").replace("$", "")), # Basic cleaning
                            "currency": "USD", # Assuming USD for now
                            "availability_status": css_extracted_data.get("availability_status", "Unknown"),
                            "ingredients_list": css_extracted_data.get("ingredients_list"),
                            "image_url": css_extracted_data.get("image_url"),
                            "product_url": url,
                        }

                if not product_to_upsert:
                    print(f"No data extracted for {url}. Skipping.")
                    return

                # Clean and validate data before upserting
                if not all([product_to_upsert.get("sku"), product_to_upsert.get("product_name"), product_to_upsert.get("price_amount") is not None]):
                    print(f"Incomplete data for {url}. Skipping.")
                    return
                
                with Session(engine) as session:
                    upsert_product(session, product_to_upsert)
                
                print(f"Successfully scraped and saved {product_to_upsert.get('product_name')}")
                return
            except (TimeoutError, Error) as e:
                print(f"Network/Proxy error on attempt {attempt + 1} for {url}: {e}")
                print("Forcing proxy rotation.")
                await self.new_context()
                await asyncio.sleep(random.uniform(0.5, 1.5) + 2 ** attempt) # Exponential backoff with random jitter
            except Exception as e:
                print(f"An unexpected error occurred on attempt {attempt + 1} for {url}: {e}")
                await asyncio.sleep(2 ** attempt) # Exponential backoff
            finally:
                if page:
                    await page.close()
        
        print(f"Failed to scrape {url} after multiple retries.")


def init_db():
    """Initializes the database and tables with retry logic."""
    print("Initializing database...")
    for i in range(5):
        try:
            create_db_and_tables()
            print("Database initialized successfully.")
            return
        except Exception as e:
            print(f"Database connection failed (attempt {i+1}/5): {e}")
            time.sleep(5)
    print("Could not connect to the database. Exiting.")
    exit(1)


async def main():
    """Main function to run the scraper."""
    init_db()

    async with async_playwright() as p:
        scraper = Scraper(p)
        await scraper.setup_browser()

        # Example usage:
        # Replace with URLs from your Redis queue
        test_urls = [
             "https://www.sephora.com/product/the-ordinary-deciem-niacinamide-10-zinc-1-P427417"
        ]

        tasks = [scraper.scrape_product(url) for url in test_urls]
        await asyncio.gather(*tasks)

        if scraper.browser:
            await scraper.browser.close()

if __name__ == "__main__":
    # This is for standalone running. In production, you'd call this from your worker.
    asyncio.run(main())
