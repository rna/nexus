import asyncio
import os
from typing import Optional

from core.api_scraper import ApiScraper
from core.proxy_manager import ProxyManager
from core.rate_controller import AdaptiveRateController
from core.normalizer import normalize_product_data
from models import create_db_and_tables, upsert_products, engine
from sqlmodel.ext.asyncio.session import AsyncSession
from tasks import get_url_for_processing, mark_url_as_done, push_to_dlq, requeue_inflight_urls
from logger import get_logger

logger = get_logger(__name__)
POLL_INTERVAL_SECONDS = int(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "5"))

async def process_batch(api_scraper: ApiScraper, urls: list[str]):
    """
    Processes a batch of URLs concurrently.
    """
    tasks = [process_single_url(api_scraper, url) for url in urls]
    results = await asyncio.gather(*tasks)
    
    successful_products = [res for res in results if res]
    if successful_products:
        async with AsyncSession(engine) as session:
            await upsert_products(session, successful_products)
        logger.info(f"Successfully saved {len(successful_products)} products to the database.")

async def process_single_url(api_scraper: ApiScraper, url: str) -> Optional[dict]:
    """
    Scrapes and normalizes a single URL.
    """
    raw_data = await api_scraper.get(url)
    if not raw_data:
        # Failures are handled by the ApiScraper and ProxyManager
        # We can add more logic here to requeue, etc. if needed
        return None
        
    normalized_data = normalize_product_data(raw_data, url)
    if not normalized_data:
        return None

    return normalized_data


async def main():
    """Main function to set up and run the scraper worker."""
    logger.info("Starting worker process...")
    
    await create_db_and_tables()
    
    recovered = requeue_inflight_urls()
    if recovered:
        logger.info("Recovered %s URLs left in processing queue from a previous run.", recovered)

    proxy_manager = ProxyManager()
    rate_controller = AdaptiveRateController()
    api_scraper = ApiScraper(proxy_manager)
    
    # Start the adaptive rate controller as a background task
    asyncio.create_task(rate_controller.adjust_rate())

    logger.info("Worker started. Waiting for URLs from Redis queue...")
    
    # This is a simplified worker. A production one might use a batching strategy.
    while True:
        await rate_controller.acquire()
        try:
            url = get_url_for_processing()
            if not url:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            logger.info(f"Processing URL: {url}")
            
            # For simplicity, we process one-by-one with concurrency control.
            # A batching approach would be more efficient.
            normalized_data = await process_single_url(api_scraper, url)
            
            if normalized_data:
                async with AsyncSession(engine) as session:
                    await upsert_products(session, [normalized_data])
                rate_controller.record_success()
                mark_url_as_done(url)
                logger.info(f"Successfully processed and saved {normalized_data.get('product_name')}")
            else:
                rate_controller.record_failure()
                logger.error(f"Failed to process {url}. Moving to DLQ.")
                push_to_dlq(url)

        finally:
            rate_controller.release()


if __name__ == "__main__":
    asyncio.run(main())
