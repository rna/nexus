import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from logger import get_logger

logger = get_logger(__name__)

@dataclass
class Proxy:
    """A class to hold proxy information and its health status."""
    url: str
    health_score: int = 100
    failure_count: int = 0
    success_count: int = 0
    last_used: datetime = field(default_factory=datetime.utcnow)
    is_cooling_down: bool = False
    cooldown_until: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        return (self.success_count / total) * 100 if total > 0 else 100.0

class ProxyManager:
    """Manages a pool of proxies, their health, and rotation."""
    def __init__(self, cooldown_period_seconds: int = 300, health_threshold: int = 50):
        self.proxies: list[Proxy] = self._load_proxies()
        self.cooldown_period = timedelta(seconds=cooldown_period_seconds)
        self.health_threshold = health_threshold

    def _load_proxies(self) -> list[Proxy]:
        """Loads proxies from the PROXY_URLS environment variable."""
        proxy_urls_str = os.getenv("PROXY_URLS")
        if not proxy_urls_str:
            logger.critical("No PROXY_URLS environment variable found. Proxy Manager cannot operate.")
            return []
        
        urls = [url.strip() for url in proxy_urls_str.split(',')]
        logger.info(f"Loaded {len(urls)} proxies.")
        return [Proxy(url=url) for url in urls]

    def get_proxy(self) -> Optional[str]:
        """
        Selects the best available proxy from the pool.
        - Filters out proxies that are cooling down.
        - Sorts by the highest health score.
        """
        if not self.proxies:
            return None

        self._check_cooldowns()
        
        available_proxies = [p for p in self.proxies if not p.is_cooling_down]
        
        if not available_proxies:
            logger.warning("No available proxies. All are cooling down.")
            return None
            
        # Simple strategy: pick a random one from the top-tier proxies
        # A more complex strategy could involve round-robin or weighted selection.
        available_proxies.sort(key=lambda p: p.health_score, reverse=True)
        top_tier_cutoff = max(1, len(available_proxies) // 2)
        best_proxies = available_proxies[:top_tier_cutoff]
        
        selected_proxy = random.choice(best_proxies)
        selected_proxy.last_used = datetime.utcnow()
        return selected_proxy.url

    def record_success(self, proxy_url: str):
        """Records a successful request for a proxy, improving its health."""
        proxy = self._find_proxy(proxy_url)
        if proxy:
            proxy.success_count += 1
            # Slowly regenerate health
            if proxy.health_score < 100:
                proxy.health_score += 1

    def record_failure(self, proxy_url: str):
        """Records a failed request, reducing health and potentially starting a cooldown."""
        proxy = self._find_proxy(proxy_url)
        if proxy:
            proxy.failure_count += 1
            proxy.health_score -= 10 # Penalize heavily for a failure

            if proxy.health_score < self.health_threshold:
                self._start_cooldown(proxy)

    def _find_proxy(self, proxy_url: str) -> Optional[Proxy]:
        """Finds a Proxy object by its URL."""
        for proxy in self.proxies:
            if proxy.url == proxy_url:
                return proxy
        return None

    def _start_cooldown(self, proxy: Proxy):
        """Puts a proxy into a cooldown state."""
        proxy.is_cooling_down = True
        proxy.cooldown_until = datetime.utcnow() + self.cooldown_period
        logger.warning(f"Proxy {proxy.url} is cooling down for {self.cooldown_period.seconds} seconds due to poor health (score: {proxy.health_score}).")

    def _check_cooldowns(self):
        """Checks for and reactivates proxies whose cooldown period has ended."""
        now = datetime.utcnow()
        for proxy in self.proxies:
            if proxy.is_cooling_down and proxy.cooldown_until and now > proxy.cooldown_until:
                proxy.is_cooling_down = False
                proxy.cooldown_until = None
                proxy.health_score = self.health_threshold # Reset to baseline health
                logger.info(f"Proxy {proxy.url} is now active after cooldown.")

    def get_stats(self) -> list:
        """Returns statistics for all proxies in the pool."""
        return sorted([
            {
                "url": p.url,
                "health_score": p.health_score,
                "success_rate": p.success_rate,
                "is_cooling_down": p.is_cooling_down
            } for p in self.proxies], key=lambda x: x["health_score"], reverse=True)
