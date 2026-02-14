from typing import Dict, Any, Optional
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
    }

# --- Router ---
# Maps a domain key to the correct normalization function
NORMALIZERS = {
    "sephora": _normalize_sephora,
    "default": _normalize_default,
}
