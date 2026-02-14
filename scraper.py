import asyncio
import json
import random
import os
import time
from typing import Optional, Dict, Any

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright, Error
from playwright_stealth import stealth_async
from asyncio import TimeoutError

from models import upsert_product, engine, create_db_and_tables
from sqlmodel import Session
from user_agents import get_random_user_agent

# --- Configuration ---
PROXY_URL = os.environ.get("PROXY_URL")
REQUEST_COUNT_PER_PROXY_SESSION = random.randint(5, 10)


class Scraper:
    def __init__(self, playwright: Playwright):
        self.playwright = playwright
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.request_count = 0

    async def setup_browser(self):
        """Initializes the browser and a new browser context with stealth settings."""
        self.browser = await self.playwright.chromium.launch(headless=True)
        await self.new_context()

    async def new_context(self):
        """Creates a new browser context with randomized settings and proxy."""
        if self.context:
            await self.context.close()

        user_agent = get_random_user_agent()
        viewport = {
            "width": random.randint(1280, 1920),
            "height": random.randint(720, 1080),
        }

        context_options = {
            "user_agent": user_agent,
            "viewport": viewport,
        }

        if PROXY_URL:
            context_options["proxy"] = {"server": PROXY_URL}

        self.context = await self.browser.new_context(**context_options)
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

    def extract_with_css(self, page: Page, selectors: Dict[str, str]) -> Dict[str, Any]:
        """Fallback to extract data using CSS selectors."""
        # This is a placeholder. You will need to implement the actual extraction
        # logic based on the selectors for a specific site.
        # For example:
        # product_data = {
        #     "product_name": await page.locator(selectors["product_name"]).inner_text(),
        #     "brand": await page.locator(selectors["brand"]).inner_text(),
        #     ...
        # }
        # return product_data
        raise NotImplementedError("CSS extraction must be implemented for each site.")

    async def scrape_product(self, url: str, site_selectors: Optional[Dict[str, str]] = None):
        """
        Scrapes a single product page.
        """
        page = None
        for attempt in range(5): # 5 retries
            try:
                page = await self.get_page()

                # Human-like delay before navigating
                await asyncio.sleep(random.uniform(1, 3))

                response = await page.goto(url, wait_until="domcontentloaded")

                # Human-like mouse movement and delay after loading
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                await asyncio.sleep(random.uniform(0.5, 1.5))

                if response.status in [403, 429, 503]:
                    raise Exception(f"Received status {response.status}")

                # Human-like delay before extraction
                await asyncio.sleep(random.uniform(1, 2))

                product_data_json = await self.extract_from_json_ld(page)

                if product_data_json:
                    print(f"Extracted data from JSON-LD for {url}")
                    # Normalize JSON-LD data to your DB schema
                    # This is a simplified example
                    product = {
                        "sku": product_data_json.get("sku"),
                        "brand": product_data_json.get("brand", {}).get("name"),
                        "product_name": product_data_json.get("name"),
                        "price_amount": float(product_data_json.get("offers", {}).get("price")),
                        "currency": product_data_json.get("offers", {}).get("priceCurrency"),
                        "availability_status": "InStock" if "InStock" in product_data_json.get("offers", {}).get("availability", "") else "OutOfStock",
                        "ingredients_list": ", ".join(product_data_json.get("description", "").split(",")), # Example, adjust as needed
                        "image_url": product_data_json.get("image"),
                        "product_url": url,
                    }
                elif site_selectors:
                    print(f"Falling back to CSS selectors for {url}")
                    # product = await self.extract_with_css(page, site_selectors)
                    # For now, we'll just log that we need to implement it
                    print("CSS selector logic not implemented yet.")
                    return
                else:
                    print(f"No extraction method found for {url}")
                    return

                # Clean and validate data before upserting
                if not all([product["sku"], product["product_name"], product["price_amount"]]):
                    print(f"Incomplete data for {url}. Skipping.")
                    return
                
                with Session(engine) as session:
                    upsert_product(session, product)
                
                print(f"Successfully scraped and saved {product['product_name']}")
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
