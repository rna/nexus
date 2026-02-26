from typing import Dict, Any, Optional
from urllib.parse import parse_qs, urlparse
from logger import get_logger

logger = get_logger(__name__)

def normalize_product_data(raw_data: Dict[str, Any], product_url: str) -> Optional[Dict[str, Any]]:
    """
    Transforms raw API data from a specific site into the standardized Product schema.
    
    This function is a "router" that calls the appropriate site-specific normalizer.
    This is where you would add logic to handle different API response structures.
    """
    domain = "default" # In a real system, you'd determine this from the URL or a task parameter
    
    if "sephora.com" in product_url:
        domain = "sephora"
    elif "nykaa.com" in product_url:
        domain = "nykaa"
    # Add other domains here, e.g., "nykaa", "ulta"
    
    normalizer_func = NORMALIZERS.get(domain)
    
    if not normalizer_func:
        logger.error(f"No normalizer found for URL: {product_url}")
        return None
        
    try:
        return normalizer_func(raw_data, product_url)
    except Exception as e:
        logger.error(f"Failed to normalize data for {product_url}: {e}", exc_info=True)
        return None

# --- Site-Specific Normalization Functions ---

def _normalize_sephora(data: Dict[str, Any], url: str) -> Dict[str, Any]:
    """
    Normalizer for a hypothetical Sephora product API response.
    This needs to be adapted to the *actual* API structure found during discovery.
    """
    return {
        "sku": data.get("sku"),
        "product_url": url,
        "brand": data.get("brand", {}).get("displayName"),
        "product_name": data.get("displayName"),
        "price_amount": data.get("currentSku", {}).get("listPrice"),
        "currency": "USD", # Assuming
        "availability_status": "InStock" if data.get("currentSku", {}).get("isSellable") else "OutOfStock",
        "image_url": data.get("primaryProductImage", {}).get("url"),
        # Ingredients and other fields might require digging deeper into the JSON
        "ingredients_list": data.get("currentSku", {}).get("ingredientDesc"),
        "source_site": "sephora",
        "source_product_id": str(data.get("sku")) if data.get("sku") else None,
        "raw_payload": data,
    }


def _normalize_nykaa(data: Dict[str, Any], url: str) -> Dict[str, Any]:
    """
    Normalizer for Nykaa's product-details API envelope.
    Known shape (subject to change): {"status": "success", "response": {...product fields...}}
    """
    payload = data.get("response") if isinstance(data, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_product_id = query_params.get("product_id", [None])[0] or query_params.get("productId", [None])[0]

    product_id = (
        payload.get("id")
        or payload.get("product_id")
        or payload.get("productId")
        or query_product_id
    )
    product_id_str = str(product_id) if product_id is not None else None

    page_url = payload.get("url") or payload.get("product_url") or payload.get("share_url")
    if isinstance(page_url, str) and page_url:
        if page_url.startswith("/"):
            page_url = f"https://www.nykaa.com{page_url}"
        elif not page_url.startswith("http"):
            page_url = f"https://www.nykaa.com/{page_url.lstrip('/')}"
        if product_id_str and "/p/" not in page_url:
            page_url = page_url.rstrip("/") + f"/p/{product_id_str}"
    elif product_id_str:
        page_url = f"https://www.nykaa.com/p/{product_id_str}"
    else:
        page_url = url

    brand_field = payload.get("brand")
    brand = payload.get("brand_name")
    if not brand and isinstance(brand_field, dict):
        brand = brand_field.get("name") or brand_field.get("displayName")
    elif not brand and isinstance(brand_field, str):
        brand = brand_field

    price_amount = (
        payload.get("final_price")
        or payload.get("discounted_price")
        or payload.get("offer_price")
        or payload.get("price")
        or payload.get("mrp")
    )

    image_url = payload.get("image_url") or payload.get("img_url")
    if not image_url:
        image_urls = payload.get("image_urls") or payload.get("images") or payload.get("carousel")
        if isinstance(image_urls, list) and image_urls:
            first_image = image_urls[0]
            if isinstance(first_image, dict):
                image_url = first_image.get("url") or first_image.get("image")
            else:
                image_url = first_image

    saleable = payload.get("is_saleable")
    in_stock = payload.get("in_stock", payload.get("is_in_stock"))
    if saleable is False or in_stock is False:
        availability_status = "OutOfStock"
    elif saleable is True or in_stock is True:
        availability_status = "InStock"
    else:
        availability_status = payload.get("availability") or payload.get("stock_status")

    ingredients = (
        payload.get("ingredients")
        or payload.get("ingredient_info")
        or payload.get("ingredientDesc")
        or payload.get("key_ingredients")
        or payload.get("description")
    )

    sku = payload.get("sku") or payload.get("sku_code")
    if not sku and product_id_str:
        sku = f"nykaa-{product_id_str}"

    return {
        "sku": sku,
        "product_url": page_url,
        "brand": brand or payload.get("manufacturer"),
        "product_name": payload.get("name") or payload.get("title") or payload.get("product_name"),
        "price_amount": price_amount,
        "currency": payload.get("currency") or "INR",
        "availability_status": availability_status,
        "image_url": image_url,
        "ingredients_list": ingredients,
        "source_site": "nykaa",
        "source_product_id": product_id_str,
        "raw_payload": data,
    }

def _normalize_default(data: Dict[str, Any], url: str) -> Dict[str, Any]:
    """A generic pass-through normalizer."""
    logger.warning(f"Using default normalizer for {url}. Data mapping may be incomplete.")
    return {
        "sku": data.get("sku"),
        "product_url": url,
        "brand": data.get("brand"),
        "product_name": data.get("name"),
        "price_amount": data.get("price"),
        "currency": data.get("currency"),
        "availability_status": data.get("availability"),
        "image_url": data.get("imageUrl"),
        "ingredients_list": data.get("ingredients"),
        "source_site": None,
        "source_product_id": str(data.get("id")) if data.get("id") else None,
        "raw_payload": data,
    }

# --- Router ---
# Maps a domain key to the correct normalization function
NORMALIZERS = {
    "nykaa": _normalize_nykaa,
    "sephora": _normalize_sephora,
    "default": _normalize_default,
}
