import httpx
from typing import Optional, Dict

from logger import get_logger
from .proxy_manager import ProxyManager
from .block_detector import detect_block, BlockType

logger = get_logger(__name__)

# --- Default Headers ---
# This should be populated based on the discovery phase for a specific site
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

class ApiScraper:
    """
    An HTTP client for scraping APIs, integrated with proxy management and block detection.
    """
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager

    async def get(self, url: str, headers: Optional[Dict] = None) -> Optional[Dict]:
        """
        Performs a GET request to an API endpoint with proxy rotation and block detection.
        Returns the JSON response as a dictionary on success, None on failure.
        """
        request_headers = DEFAULT_HEADERS.copy()
        if headers:
            request_headers.update(headers)
        
        proxy_url = self.proxy_manager.get_proxy()
        if not proxy_url:
            logger.error("No available proxies to make a request.")
            return None
        
        proxies = {"http://": proxy_url, "https://": proxy_url}

        try:
            async with httpx.AsyncClient(proxies=proxies, timeout=15) as client:
                response = await client.get(url, headers=request_headers)

            # --- Block Detection ---
            block_type = detect_block(response.text, response.status_code)
            
            if block_type != BlockType.NOT_BLOCKED:
                logger.warning(f"Block detected for proxy {proxy_url} on {url}. Type: {block_type.name}")
                self.proxy_manager.record_failure(proxy_url)
                return None # Signal failure

            # --- Success ---
            self.proxy_manager.record_success(proxy_url)
            logger.info(f"Successful API request to {url} with proxy {proxy_url}")
            return response.json()

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status Error requesting {url} with proxy {proxy_url}: {e.response.status_code}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except httpx.RequestError as e:
            logger.error(f"Request Error with proxy {proxy_url} for {url}: {e}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except Exception as e:
            logger.critical(f"An unexpected error occurred in ApiScraper: {e}", exc_info=True)
            self.proxy_manager.record_failure(proxy_url) # Penalize proxy for unexpected errors too
            return None
