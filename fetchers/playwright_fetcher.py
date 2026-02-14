import asyncio
import random
from typing import Optional

from playwright.async_api import Playwright, Browser, BrowserContext, Page, Error
from asyncio import TimeoutError
from playwright_stealth import stealth_async

from user_agents import get_random_user_agent
from fingerprints import get_fingerprint, get_override_script
from logger import get_logger
from .base import Fetcher, FetchResult

logger = get_logger(__name__)

class PlaywrightFetcher(Fetcher):
    def __init__(self, playwright: Playwright, proxy_url: Optional[str] = None):
        self.playwright = playwright
        self.proxy_url = proxy_url
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.request_count = 0
        self.fingerprint: Optional[dict] = None
        self.request_count_per_proxy_session = random.randint(5, 10)

    async def launch(self):
        """Launches the Playwright browser."""
        logger.info("Launching Playwright browser...")
        self.browser = await self.playwright.chromium.launch(headless=True)
        await self.new_context()

    async def new_context(self):
        """Creates a new, clean browser context with stealth settings."""
        if self.context:
            await self.context.close()

        logger.info("Creating new browser context with new fingerprint and proxy session...")
        self.fingerprint = get_fingerprint()
        override_script = get_override_script(self.fingerprint)

        user_agent = get_random_user_agent()
        viewport = { "width": random.randint(1280, 1920), "height": random.randint(720, 1080) }

        context_options = {
            "user_agent": user_agent,
            "viewport": viewport,
            "java_script_enabled": True,
        }

        if self.proxy_url:
            context_options["proxy"] = {"server": self.proxy_url}

        self.context = await self.browser.new_context(**context_options)
        await self.context.add_init_script(override_script)
        self.context.set_default_navigation_timeout(60 * 1000)
        await self.context.route("**/*", self._intercept_requests)

    async def fetch(self, url: str) -> Optional[FetchResult]:
        """
        Fetches a URL using Playwright, with retries and human-like interaction.
        """
        page = None
        for attempt in range(5):
            try:
                page = await self._get_page()

                await asyncio.sleep(random.uniform(1, 3))
                response = await page.goto(url, wait_until="domcontentloaded")

                await self._simulate_human_activity(page)

                if response.status in [403, 429, 503]:
                    raise Exception(f"Received status code {response.status} indicating a block.")

                content = await page.content()
                return FetchResult(status_code=response.status, html_content=content, page_source=page)

            except (TimeoutError, Error) as e:
                logger.warning(f"Playwright/Network error on attempt {attempt + 1} for {url}: {e}")
                await self.new_context() # Force a new context/fingerprint/proxy
                await asyncio.sleep(random.uniform(1, 2) + 2 ** attempt)
            except Exception as e:
                logger.error(f"An unexpected error in fetch() on attempt {attempt + 1} for {url}: {e}", exc_info=True)
                await asyncio.sleep(2 ** attempt)
            finally:
                # We do not close the page here, the orchestrator who called fetch() is responsible
                # for closing it via the page_source object in the FetchResult.
                pass
        
        return None

    async def close(self):
        """Closes the browser."""
        if self.browser:
            await self.browser.close()

    async def _get_page(self) -> Page:
        """Gets a page, handling context rotation."""
        if self.request_count >= self.request_count_per_proxy_session:
            await self.new_context()
            self.request_count = 0

        self.request_count += 1
        page = await self.context.new_page()
        await stealth_async(page)
        return page

    @staticmethod
    async def _intercept_requests(route):
        """Blocks non-essential resources."""
        if route.request.resource_type in ["image", "stylesheet", "font"]:
            await route.abort()
        else:
            await route.continue_()

    @staticmethod
    async def _simulate_human_activity(page: Page):
        """Simulates human-like interactions on the page."""
        # 1. Cookie banner
        cookie_selectors = [
            'button:has-text("Accept")', 'button:has-text("Agree")',
            '[id*="consent"] button:has-text("Accept")', '[class*="cookie"] button:has-text("Accept")',
        ]
        for selector in cookie_selectors:
            try:
                await page.locator(selector).first.click(timeout=2000)
                logger.info("Clicked cookie banner.")
                break # Exit after first success
            except (TimeoutError, Error):
                pass
        
        # 2. Random mouse movement
        await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        # 3. Human-like scroll
        total_height = await page.evaluate("document.body.scrollHeight")
        for i in range(1, total_height, random.randint(300, 600)):
            await page.mouse.wheel(0, i)
            await asyncio.sleep(random.uniform(0.3, 1.0))
            if i > 2000: # Don't scroll forever on infinite-scroll pages
                break
