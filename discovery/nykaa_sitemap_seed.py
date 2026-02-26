import asyncio
import os
from collections import deque

import httpx

from core.nykaa import iter_sitemap_locs, build_product_details_api_url, extract_product_id_from_url
from core.proxy_manager import ProxyManager
from logger import get_logger
from tasks import push_urls_to_queue


logger = get_logger(__name__)

ROOT_SITEMAP_URL = os.getenv("NYKAA_SITEMAP_URL", "https://www.nykaa.com/sitemap.xml")
NYKAA_APP_VERSION = os.getenv("NYKAA_APP_VERSION", "8.6.6")
MAX_SITEMAP_FILES = int(os.getenv("NYKAA_SITEMAP_MAX_FILES", "500"))
MAX_PRODUCTS = int(os.getenv("NYKAA_SITEMAP_MAX_PRODUCTS", "0"))  # 0 = no cap
REQUEST_TIMEOUT_SECONDS = float(os.getenv("NYKAA_SITEMAP_TIMEOUT_SECONDS", "30"))


async def fetch_text(url: str, proxy_manager: ProxyManager) -> str | None:
    proxy_url = proxy_manager.get_proxy()
    if not proxy_url:
        logger.error("No proxy available for sitemap request.")
        return None

    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        proxy_manager.record_success(proxy_url)
        return response.text
    except Exception as exc:
        logger.warning("Failed sitemap request for %s via %s: %s", url, proxy_url, exc)
        proxy_manager.record_failure(proxy_url)
        return None


async def main() -> None:
    proxy_manager = ProxyManager()

    seen_sitemaps: set[str] = set()
    sitemap_queue = deque([ROOT_SITEMAP_URL])
    product_api_urls: list[str] = []
    processed_sitemaps = 0

    while sitemap_queue and processed_sitemaps < MAX_SITEMAP_FILES:
        sitemap_url = sitemap_queue.popleft()
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        processed_sitemaps += 1

        logger.info("Fetching sitemap %s (%s/%s)", sitemap_url, processed_sitemaps, MAX_SITEMAP_FILES)
        xml_text = await fetch_text(sitemap_url, proxy_manager)
        if not xml_text:
            continue

        for loc in iter_sitemap_locs(xml_text):
            if loc.endswith(".xml"):
                if loc not in seen_sitemaps:
                    sitemap_queue.append(loc)
                continue

            product_id = extract_product_id_from_url(loc)
            if product_id:
                product_api_urls.append(build_product_details_api_url(product_id, app_version=NYKAA_APP_VERSION))
                if MAX_PRODUCTS and len(product_api_urls) >= MAX_PRODUCTS:
                    break

        if MAX_PRODUCTS and len(product_api_urls) >= MAX_PRODUCTS:
            logger.info("Reached NYKAA_SITEMAP_MAX_PRODUCTS=%s, stopping discovery.", MAX_PRODUCTS)
            break

    pushed = push_urls_to_queue(product_api_urls)
    logger.info(
        "Nykaa sitemap seeding complete. sitemaps=%s discovered_products=%s queued_new=%s",
        processed_sitemaps,
        len(product_api_urls),
        pushed,
    )


if __name__ == "__main__":
    asyncio.run(main())
