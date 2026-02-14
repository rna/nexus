from enum import Enum, auto

class BlockType(Enum):
    """Enumeration for different types of blocking."""
    NOT_BLOCKED = auto()
    CAPTCHA = auto()
    RATE_LIMIT = auto()
    IP_BAN = auto()
    FORBIDDEN_CONTENT = auto() # 403 Forbidden
    UNEXPECTED_FORMAT = auto() # e.g., HTML page instead of JSON

def detect_block(response_text: str, status_code: int) -> BlockType:
    """
    Analyzes an HTTP response to detect if and how we are being blocked.
    """
    lower_text = response_text.lower()

    if status_code == 429:
        return BlockType.RATE_LIMIT
    
    if status_code == 403:
        # 403 can be a temporary IP ban or a permanent content block
        # Simple check for now, can be improved with more pattern matching
        if "access denied" in lower_text or "forbidden" in lower_text:
             return BlockType.IP_BAN # Assume it's a ban for now
        return BlockType.FORBIDDEN_CONTENT

    if "captcha" in lower_text or "are you a robot" in lower_text:
        return BlockType.CAPTCHA

    # If we expect JSON but get HTML, it's a form of block (often a login/challenge page)
    if "<!doctype html>" in lower_text or "<html" in lower_text:
        # This check assumes we are expecting API responses, not HTML pages
        return BlockType.UNEXPECTED_FORMAT
        
    return BlockType.NOT_BLOCKED
