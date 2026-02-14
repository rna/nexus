import asyncio
import random
from typing import Optional

from playwright.async_api import Playwright, Browser, BrowserContext, Page
from playwright_stealth import stealth_async

from user_agents import get_random_user_agent
from fingerprints import get_fingerprint, get_override_script

class BrowserManager:
    def __init__(self, playwright: Playwright, proxy_url: Optional[str] = None):
        self.playwright = playwright
        self.proxy_url = proxy_url
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.request_count = 0
        self.fingerprint: Optional[dict] = None
        self.request_count_per_proxy_session = random.randint(5, 10)

    async def launch_browser(self):
        """Launches the browser."""
        self.browser = await self.playwright.chromium.launch(headless=True)
        await self.new_context()

    async def new_context(self):
        """Creates a new, clean browser context with stealth settings."""
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

        if self.proxy_url:
            context_options["proxy"] = {"server": self.proxy_url}

        self.context = await self.browser.new_context(**context_options)
        await self.context.add_init_script(override_script)
        self.context.set_default_navigation_timeout(60 * 1000)

        await self.context.route("**/*", self._intercept_requests)

    async def get_page(self) -> Page:
        """Gets a page, handling context rotation."""
        if self.request_count >= self.request_count_per_proxy_session:
            print("Rotating browser context and proxy session...")
            await self.new_context()
            self.request_count = 0

        self.request_count += 1
        page = await self.context.new_page()
        await stealth_async(page)
        return page

    async def close(self):
        """Closes the browser."""
        if self.browser:
            await self.browser.close()

    @staticmethod
    async def _intercept_requests(route):
        """Blocks non-essential resources."""
        if route.request.resource_type in ["image", "stylesheet", "font"]:
            await route.abort()
        else:
            await route.continue_()
