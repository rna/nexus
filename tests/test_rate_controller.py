import asyncio
import unittest

from core.rate_controller import AdaptiveRateController


class AdaptiveRateControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_acquire_blocks_until_release_at_limit(self):
        rc = AdaptiveRateController(initial_concurrency=1)

        await rc.acquire()
        second_started = asyncio.Event()
        second_acquired = asyncio.Event()

        async def take_second_slot():
            second_started.set()
            await rc.acquire()
            second_acquired.set()

        task = asyncio.create_task(take_second_slot())
        await second_started.wait()
        await asyncio.sleep(0)  # Let the task attempt to acquire.
        self.assertFalse(second_acquired.is_set())

        await rc.release()
        await asyncio.wait_for(second_acquired.wait(), timeout=1)
        await rc.release()
        await task

    async def test_increase_concurrency_wakes_waiters(self):
        rc = AdaptiveRateController(initial_concurrency=1)
        await rc.acquire()

        waiter_ready = asyncio.Event()
        waiter_acquired = asyncio.Event()

        async def waiter():
            waiter_ready.set()
            await rc.acquire()
            waiter_acquired.set()

        task = asyncio.create_task(waiter())
        await waiter_ready.wait()
        await asyncio.sleep(0)
        self.assertFalse(waiter_acquired.is_set())

        await rc._change_concurrency(1)
        await asyncio.wait_for(waiter_acquired.wait(), timeout=1)

        await rc.release()
        await rc.release()
        await task

    async def test_failure_rate_calculation(self):
        rc = AdaptiveRateController(initial_concurrency=2)
        rc.record_success()
        rc.record_failure()
        rc.record_failure()
        self.assertAlmostEqual(rc.get_failure_rate(), (2 / 3) * 100)


if __name__ == "__main__":
    unittest.main()

