import redis
import os
import time

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "scraping_queue"
PROCESSING_QUEUE_NAME = "processing_queue"
DLQ_NAME = "dead_letter_queue"

# Add a timestamp to the URL to track how long it's been processing
# The URL will be stored as a string: f"{url}::{timestamp}"
PROCESSING_TIMEOUT = 300 # 5 minutes

r = redis.from_url(REDIS_URL, decode_responses=True)


def push_url_to_queue(url: str):
    """Pushes a URL to the main scraping queue."""
    r.lpush(QUEUE_NAME, url)

def get_url_for_processing() -> str | None:
    """
    Atomically moves a URL from the main queue to the processing queue.
    Returns the URL, or None if the queue is empty.
    """
    # LMOVE is the modern version of RPOPLPUSH
    # It moves from the right of QUEUE_NAME (oldest) to the left of PROCESSING_QUEUE_NAME
    url = r.lmove(QUEUE_NAME, PROCESSING_QUEUE_NAME, "RIGHT", "LEFT")
    if url:
        return url
    return None

def mark_url_as_done(url: str):
    """
    Removes a successfully processed URL from the processing queue.
    LREM count=1 means remove only the first occurrence.
    """
    r.lrem(PROCESSING_QUEUE_NAME, 1, url)

def requeue_stale_tasks():
    """
    Finds tasks in the processing queue that have timed out and
    moves them back to the main queue for reprocessing.
    This should be run periodically by a separate monitor process.
    """
    # This is a simplified implementation. A real-world version would
    # store timestamps with the URLs in the processing queue.
    # For now, we'll assume any task in the processing queue on startup
    # of the monitor is stale. A more robust implementation is a future step.
    
    stale_tasks = r.lrange(PROCESSING_QUEUE_NAME, 0, -1)
    for task in stale_tasks:
        # In this simple model, we just move them all back.
        r.lmove(PROCESSING_QUEUE_NAME, QUEUE_NAME, "RIGHT", "RIGHT")
        print(f"Requeued stale task: {task}")


def push_to_dlq(url: str):
    """Pushes a failed URL to the Dead Letter Queue."""
    r.lpush(DLQ_NAME, url)
