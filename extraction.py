import json
from typing import Optional, Dict, Any

from playwright.async_api import Page, Error
from asyncio import TimeoutError
from logger import get_logger

logger = get_logger(__name__)

async def extract_from_json_ld(page: Page) -> Optional[Dict[str, Any]]:
    """Extracts product data from JSON-LD scripts."""
    try:
        json_ld_element = await page.query_selector('script[type="application/ld+json"]')
        if not json_ld_element:
            return None

        json_ld_text = await json_ld_element.inner_text()
        data = json.loads(json_ld_text)

        if data.get("@type") == "Product":
            return data
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get('@type') == 'Product':
                    return item

    except (json.JSONDecodeError, AttributeError, Error) as e:
        logger.warning(f"Could not parse JSON-LD: {e}")
    return None

async def extract_with_css(page: Page, selectors: Dict[str, str]) -> Dict[str, Any]:
    """Fallback to extract data using CSS selectors."""
    product_data = {}
    for key, selector in selectors.items():
        try:
            element = await page.query_selector(selector)
            if element:
                product_data[key] = await element.inner_text()
        except (TimeoutError, Error) as e:
            logger.warning(f"Could not extract {key} using selector '{selector}': {e}")
    return product_data
