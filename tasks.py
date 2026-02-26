import os
from typing import Iterable, Optional

from logger import get_logger

try:
    import redis
except ImportError:  # pragma: no cover - enables unit tests without deps installed locally
    redis = None


logger = get_logger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

SCRAPING_QUEUE = os.getenv("SCRAPING_QUEUE", "scraping_queue")
PROCESSING_QUEUE = os.getenv("PROCESSING_QUEUE", "scraping_processing")
DLQ_QUEUE = os.getenv("DLQ_QUEUE", "scraping_dlq")
SEEN_URLS_SET = os.getenv("SEEN_URLS_SET", "scraping_seen")
DONE_URLS_SET = os.getenv("DONE_URLS_SET", "scraping_done")


def _build_redis_client():
    if redis is None:
        logger.warning("redis package is not installed; tasks queue client is unavailable until dependencies are installed.")
        return None
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


r = _build_redis_client()


def _require_redis_client():
    if r is None:
        raise RuntimeError("Redis client is unavailable. Install dependencies and configure REDIS_URL.")
    return r


def push_urls_to_queue(urls: Iterable[str]) -> int:
    """
    Deduplicates URLs using a Redis set and enqueues only unseen URLs.
    Returns the number of URLs newly pushed to the queue.
    """
    client = _require_redis_client()
    pushed = 0
    for raw_url in urls:
        url = (raw_url or "").strip()
        if not url:
            continue
        if client.sadd(SEEN_URLS_SET, url):
            client.rpush(SCRAPING_QUEUE, url)
            pushed += 1
    if pushed:
        logger.info("Queued %s new URLs.", pushed)
    return pushed


def get_url_for_processing() -> Optional[str]:
    """
    Moves one URL from the pending queue to the processing queue atomically.
    This prevents task loss if the worker crashes mid-processing.
    """
    client = _require_redis_client()
    return client.rpoplpush(SCRAPING_QUEUE, PROCESSING_QUEUE)


def mark_url_as_done(url: str) -> None:
    client = _require_redis_client()
    client.lrem(PROCESSING_QUEUE, 1, url)
    client.sadd(DONE_URLS_SET, url)


def push_to_dlq(url: str) -> None:
    client = _require_redis_client()
    client.lrem(PROCESSING_QUEUE, 1, url)
    client.lpush(DLQ_QUEUE, url)


def requeue_inflight_urls(limit: Optional[int] = None) -> int:
    """
    Requeues URLs left in the processing queue (e.g., after a crash) back to the pending queue.
    """
    client = _require_redis_client()
    moved = 0
    while True:
        if limit is not None and moved >= limit:
            break
        url = client.rpoplpush(PROCESSING_QUEUE, SCRAPING_QUEUE)
        if not url:
            break
        moved += 1
    if moved:
        logger.warning("Requeued %s in-flight URLs from processing back to pending.", moved)
    return moved
