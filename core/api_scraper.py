import asyncio
import httpx
import os
from typing import Optional, Dict, Any
from urllib.parse import parse_qs, urlparse

from logger import get_logger
from .proxy_manager import DIRECT_PROXY_SENTINEL, ProxyManager
from .block_detector import detect_block, BlockType

try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - exercised in container when installed
    curl_requests = None

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
        self.http_backend = os.getenv("HTTP_CLIENT_BACKEND", "auto").lower()
        self.curl_impersonate = os.getenv("CURL_CFFI_IMPERSONATE", "chrome124")
        self._curl_session = None
        self._nykaa_warmup_done = False

    @staticmethod
    def _client_kwargs_for_proxy(proxy_url: str) -> dict:
        """
        Build kwargs compatible with modern httpx versions.
        `proxy` is supported in current releases; `proxies` support can be restored if needed.
        """
        if proxy_url == DIRECT_PROXY_SENTINEL:
            return {"timeout": 15}
        return {"proxy": proxy_url, "timeout": 15}

    @staticmethod
    def _is_nykaa_url(url: str) -> bool:
        return "nykaa.com" in (url or "")

    @classmethod
    def _nykaa_referer_for_api_url(cls, url: str) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        product_id = query.get("product_id", [None])[0] or query.get("productId", [None])[0]
        if product_id:
            return f"https://www.nykaa.com/p/{product_id}"
        return "https://www.nykaa.com/"

    def _build_request_headers(self, url: str, headers: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        request_headers = DEFAULT_HEADERS.copy()
        if self._is_nykaa_url(url):
            request_headers.update(
                {
                    "Referer": self._nykaa_referer_for_api_url(url),
                    "Origin": "https://www.nykaa.com",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Dest": "empty",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
            )
        if headers:
            request_headers.update(headers)
        return request_headers

    def _should_use_curl_backend(self, url: str) -> bool:
        if self.http_backend == "curl_cffi":
            return True
        if self.http_backend == "httpx":
            return False
        # auto: prefer browser-fingerprint backend for Nykaa if available
        return self._is_nykaa_url(url) and curl_requests is not None

    def _ensure_curl_session(self):
        if curl_requests is None:
            raise RuntimeError("curl_cffi is not installed; cannot use browser-fingerprint backend.")
        if self._curl_session is None:
            self._curl_session = curl_requests.Session(impersonate=self.curl_impersonate)
        return self._curl_session

    def _curl_client_request(self, url: str, proxy_url: str, headers: Dict[str, Any]):
        session = self._ensure_curl_session()

        # Warm up cookies/challenge on first Nykaa request using the same session.
        if self._is_nykaa_url(url) and not self._nykaa_warmup_done:
            warmup_kwargs = {"headers": {"User-Agent": headers.get("User-Agent", DEFAULT_HEADERS["User-Agent"])}}
            if proxy_url != DIRECT_PROXY_SENTINEL:
                warmup_kwargs["proxy"] = proxy_url
            try:
                warmup_response = session.get("https://www.nykaa.com/", timeout=20, **warmup_kwargs)
                logger.info(
                    "curl_cffi Nykaa warmup status=%s impersonate=%s",
                    getattr(warmup_response, "status_code", "unknown"),
                    self.curl_impersonate,
                )
            except Exception as exc:
                logger.warning("curl_cffi Nykaa warmup failed: %s", exc)
            finally:
                self._nykaa_warmup_done = True

        request_kwargs = {"headers": headers, "timeout": 20}
        if proxy_url != DIRECT_PROXY_SENTINEL:
            request_kwargs["proxy"] = proxy_url
        return session.get(url, **request_kwargs)

    async def _curl_get(self, url: str, proxy_url: str, headers: Dict[str, Any]):
        return await asyncio.to_thread(self._curl_client_request, url, proxy_url, headers)

    async def _httpx_get(self, url: str, proxy_url: str, headers: Dict[str, Any]):
        async with httpx.AsyncClient(**self._client_kwargs_for_proxy(proxy_url)) as client:
            return await client.get(url, headers=headers)

    async def _perform_request(self, url: str, proxy_url: str, headers: Dict[str, Any], *, use_curl: bool):
        if use_curl:
            return await self._curl_get(url, proxy_url, headers)
        return await self._httpx_get(url, proxy_url, headers)

    async def get(self, url: str, headers: Optional[Dict] = None) -> Optional[Dict]:
        """
        Performs a GET request to an API endpoint with proxy rotation and block detection.
        Returns the JSON response as a dictionary on success, None on failure.
        """
        request_headers = self._build_request_headers(url, headers)
        
        proxy_url = self.proxy_manager.get_proxy()
        if not proxy_url:
            logger.error("No available proxies to make a request.")
            return None
        
        try:
            use_curl = self._should_use_curl_backend(url)
            used_curl_backend = use_curl
            response = await self._perform_request(url, proxy_url, request_headers, use_curl=use_curl)

            # --- Block Detection ---
            block_type = detect_block(response.text, response.status_code)
            
            if block_type != BlockType.NOT_BLOCKED:
                if (
                    block_type in {BlockType.IP_BAN, BlockType.FORBIDDEN_CONTENT, BlockType.UNEXPECTED_FORMAT}
                    and not use_curl
                    and self.http_backend == "auto"
                    and self._is_nykaa_url(url)
                    and curl_requests is not None
                ):
                    logger.info("Retrying Nykaa request with curl_cffi browser fingerprint backend after block detection.")
                    used_curl_backend = True
                    response = await self._perform_request(url, proxy_url, request_headers, use_curl=True)
                    block_type = detect_block(response.text, response.status_code)

                if block_type != BlockType.NOT_BLOCKED:
                    logger.warning(f"Block detected for proxy {proxy_url} on {url}. Type: {block_type.name}")
                    self.proxy_manager.record_failure(proxy_url)
                    return None # Signal failure

            if hasattr(response, "raise_for_status"):
                response.raise_for_status()

            # --- Success ---
            self.proxy_manager.record_success(proxy_url)
            if proxy_url == DIRECT_PROXY_SENTINEL:
                logger.info(
                    "Successful API request to %s with direct egress (no proxy)%s",
                    url,
                    " via curl_cffi" if used_curl_backend else "",
                )
            else:
                logger.info(f"Successful API request to {url} with proxy {proxy_url}")
            return response.json()

        except RuntimeError as e:
            logger.error(f"Client backend error for {url} with proxy {proxy_url}: {e}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status Error requesting {url} with proxy {proxy_url}: {e.response.status_code}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except httpx.RequestError as e:
            logger.error(f"Request Error with proxy {proxy_url} for {url}: {e}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except ValueError as e:
            logger.error(f"Invalid JSON response for {url} with proxy {proxy_url}: {e}")
            self.proxy_manager.record_failure(proxy_url)
            return None
        except Exception as e:
            logger.critical(f"An unexpected error occurred in ApiScraper: {e}", exc_info=True)
            self.proxy_manager.record_failure(proxy_url) # Penalize proxy for unexpected errors too
            return None
