import importlib
import os
import unittest
from uuid import uuid4


class WorkerPipelineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if os.getenv("RUN_INTEGRATION_TESTS") != "1":
            self.skipTest("Set RUN_INTEGRATION_TESTS=1 to run integration tests.")

        try:
            import redis  # noqa: F401
            from sqlalchemy import delete, select  # noqa: F401
        except ModuleNotFoundError as exc:
            self.skipTest(f"Dependency missing: {exc}")

        self._env_patch = {
            "SCRAPING_QUEUE": f"it_scraping_queue_{uuid4().hex}",
            "PROCESSING_QUEUE": f"it_processing_queue_{uuid4().hex}",
            "DLQ_QUEUE": f"it_dlq_queue_{uuid4().hex}",
            "SEEN_URLS_SET": f"it_seen_urls_{uuid4().hex}",
            "DONE_URLS_SET": f"it_done_urls_{uuid4().hex}",
        }
        self._old_env = {k: os.environ.get(k) for k in self._env_patch}
        os.environ.update(self._env_patch)

        import tasks
        import models
        from workers import worker

        self.tasks = importlib.reload(tasks)
        self.models = models
        self.worker = worker

        await self.models.create_db_and_tables()

    async def asyncTearDown(self):
        if getattr(self, "tasks", None):
            client = self.tasks.r
            if client is not None:
                client.delete(
                    self.tasks.SCRAPING_QUEUE,
                    self.tasks.PROCESSING_QUEUE,
                    self.tasks.DLQ_QUEUE,
                    self.tasks.SEEN_URLS_SET,
                    self.tasks.DONE_URLS_SET,
                )

        for key, value in getattr(self, "_old_env", {}).items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    async def test_process_next_queue_item_persists_nykaa_product(self):
        from sqlalchemy import delete, select
        from sqlmodel.ext.asyncio.session import AsyncSession

        api_url = "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&product_id=688908"
        raw_payload = {
            "status": "success",
            "response": {
                "id": 688908,
                "name": "Nykaa Test Product",
                "url": "/nykaa-test-product/p/688908",
                "brand_name": "Nykaa",
                "final_price": 999,
                "in_stock": True,
                "image_url": "https://images.nykaa.com/test.jpg",
                "ingredients": "Water, Aloe",
            },
        }

        class FakeApiScraper:
            async def get(self, url: str):
                self.last_url = url
                return raw_payload

        fake_scraper = FakeApiScraper()
        rate_controller = self.worker.AdaptiveRateController(initial_concurrency=1)

        # Clean any leftovers for deterministic assertions.
        async with AsyncSession(self.models.engine) as session:
            await session.execute(delete(self.models.Product).where(self.models.Product.sku == "nykaa-688908"))
            await session.commit()

        pushed = self.tasks.push_urls_to_queue([api_url])
        self.assertEqual(pushed, 1)

        result = await self.worker.process_next_queue_item(
            fake_scraper,
            rate_controller,
            get_next_url=self.tasks.get_url_for_processing,
            mark_done_fn=self.tasks.mark_url_as_done,
            push_dlq_fn=self.tasks.push_to_dlq,
            db_engine=self.models.engine,
            poll_when_empty=False,
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["url"], api_url)
        self.assertEqual(getattr(fake_scraper, "last_url", None), api_url)

        async with AsyncSession(self.models.engine) as session:
            row = (
                await session.execute(
                    select(self.models.Product).where(self.models.Product.sku == "nykaa-688908")
                )
            ).scalar_one_or_none()

        self.assertIsNotNone(row)
        self.assertEqual(row.product_name, "Nykaa Test Product")
        self.assertEqual(row.price_amount, 999)
        self.assertEqual(row.source_site, "nykaa")
        self.assertEqual(row.source_product_id, "688908")
        self.assertEqual(row.raw_payload, raw_payload)

        self.assertTrue(self.tasks.r.sismember(self.tasks.DONE_URLS_SET, api_url))
        self.assertEqual(self.tasks.r.llen(self.tasks.DLQ_QUEUE), 0)


if __name__ == "__main__":
    unittest.main()
