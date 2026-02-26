import re
import xml.etree.ElementTree as ET
from typing import Iterable, Iterator, Optional
from urllib.parse import urlencode


NYKAA_HOST = "www.nykaa.com"
NYKAA_PRODUCT_DETAILS_ENDPOINT = f"https://{NYKAA_HOST}/app-api/index.php/products/details"
NYKAA_PRODUCT_ID_PATTERN = re.compile(r"/p/(\d+)(?:[/?#]|$)")


def extract_product_id_from_url(url: str) -> Optional[str]:
    match = NYKAA_PRODUCT_ID_PATTERN.search(url or "")
    return match.group(1) if match else None


def is_nykaa_product_page_url(url: str) -> bool:
    if not url:
        return False
    return "nykaa.com" in url and extract_product_id_from_url(url) is not None


def build_product_details_api_url(product_id: str | int, app_version: str = "8.6.6") -> str:
    params = {"app_version": str(app_version), "product_id": str(product_id)}
    return f"{NYKAA_PRODUCT_DETAILS_ENDPOINT}?{urlencode(params)}"


def iter_sitemap_locs(xml_text: str) -> Iterator[str]:
    """
    Yields every <loc> URL from XML sitemap or sitemap index documents.
    Handles default XML namespaces.
    """
    root = ET.fromstring(xml_text)
    for elem in root.iter():
        if elem.tag.endswith("loc") and elem.text:
            yield elem.text.strip()


def iter_product_api_urls_from_sitemap_locs(locs: Iterable[str], app_version: str = "8.6.6") -> Iterator[str]:
    for loc in locs:
        product_id = extract_product_id_from_url(loc)
        if product_id:
            yield build_product_details_api_url(product_id, app_version=app_version)
