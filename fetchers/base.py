from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

@dataclass
class FetchResult:
    """A standard dataclass for returning results from a fetcher."""
    status_code: int
    html_content: str
    page_source: object = None # To hold the playwright page object for extraction

class Fetcher(ABC):
    """Abstract base class for all fetchers."""

    @abstractmethod
    async def launch(self):
        """Initializes the fetcher (e.g., launches a browser)."""
        pass

    @abstract_method
    async def fetch(self, url: str) -> Optional[FetchResult]:
        """
        Fetches the content of a given URL.
        Returns a FetchResult on success, None on failure after retries.
        """
        pass

    @abstract_method
    async def close(self):
        """Closes the fetcher and releases resources."""
        pass
