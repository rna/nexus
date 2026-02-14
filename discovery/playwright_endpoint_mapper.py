import asyncio
import json
from playwright.async_api import async_playwright
from logger import get_logger

logger = get_logger(__name__)

# --- Configuration ---
# The URL of a product page on the target site
TARGET_URL = "https://www.sephora.com/product/the-ordinary-deciem-niacinamide-10-zinc-1-P427417"

# Keywords to look for in the API request URLs
API_KEYWORDS = ["/api/", "/v2/", "graphql"]

# --- Main Discovery Logic ---

def handle_request(request):
    """
    This function is called for every request the page makes.
    It checks if the request URL matches our API keywords.
    """
    if any(keyword in request.url for keyword in API_KEYWORDS):
        logger.info(f"Potential API Endpoint Found: {request.method} {request.url}")
        
        # Log headers that might be important for authentication or session management
        headers = request.headers
        interesting_headers = {
            k: v for k, v in headers.items() 
            if k.lower() in ["authorization", "x-api-key", "x-csrf-token", "user-agent"]
        }
        if interesting_headers:
            logger.info(f"  Interesting Headers: {json.dumps(interesting_headers, indent=2)}")

async def main():
    """
    Launches a browser, navigates to a target URL, and listens for API requests.
    """
    logger.info(f"Starting API endpoint discovery for: {TARGET_URL}")
    logger.info("Please interact with the page (scroll, click variants, etc.) to trigger API calls.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False) # Must be non-headless to interact
        page = await browser.new_page()
        
        # Register the request listener
        page.on("request", handle_request)
        
        # Navigate to the page
        await page.goto(TARGET_URL, wait_until="networkidle")
        
        # Keep the browser open for manual interaction and inspection
        logger.info("Browser is open. Press Ctrl+C in the terminal to close.")
        try:
            # This loop keeps the script alive while you interact with the browser
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Closing browser...")
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
