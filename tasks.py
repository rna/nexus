import redis
import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "scraping_queue"
DLQ_NAME = "dead_letter_queue"

r = redis.from_url(REDIS_URL, decode_responses=True)


def push_url_to_queue(url: str):
    """Pushes a URL to the Redis queue."""
    r.lpush(QUEUE_NAME, url)


def pop_url_from_queue() -> str | None:
    """Pops a URL from the Redis queue. Returns None if the queue is empty."""
    return r.rpop(QUEUE_NAME)


def push_to_dlq(url: str):
    """Pushes a failed URL to the Dead Letter Queue."""
    r.lpush(DLQ_NAME, url)
