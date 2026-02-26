import asyncio
from collections import deque
from datetime import datetime, timedelta
from logger import get_logger

logger = get_logger(__name__)

INITIAL_CONCURRENCY = 5
MAX_CONCURRENCY = 50
MIN_CONCURRENCY = 1

class AdaptiveRateController:
    """
    Manages request concurrency and delays adaptively based on success/failure rates.
    """
    def __init__(self, initial_concurrency: int = INITIAL_CONCURRENCY):
        self.concurrency = max(MIN_CONCURRENCY, min(initial_concurrency, MAX_CONCURRENCY))
        self._in_flight = 0
        self._capacity_condition = asyncio.Condition()
        
        # Track recent request outcomes (1 for success, 0 for failure)
        self.history_window = 60 # seconds
        self.request_history = deque()
        
        self.last_adjustment_time = datetime.utcnow()
        self.adjustment_interval = timedelta(seconds=10)

    async def acquire(self):
        """Acquire a slot from the adaptive limiter."""
        async with self._capacity_condition:
            await self._capacity_condition.wait_for(lambda: self._in_flight < self.concurrency)
            self._in_flight += 1

    async def release(self):
        """Release a slot back to the adaptive limiter."""
        async with self._capacity_condition:
            if self._in_flight > 0:
                self._in_flight -= 1
            self._capacity_condition.notify_all()

    def record_success(self):
        self._add_history_event(1)

    def record_failure(self):
        self._add_history_event(0)

    def _add_history_event(self, outcome: int):
        """Adds a new event to the history, with a timestamp."""
        self.request_history.append((datetime.utcnow(), outcome))
        self._prune_history()

    def _prune_history(self):
        """Removes events from history that are older than the window."""
        now = datetime.utcnow()
        while self.request_history and self.request_history[0][0] < now - timedelta(seconds=self.history_window):
            self.request_history.popleft()

    def get_failure_rate(self) -> float:
        """Calculates the failure rate over the current history window."""
        if not self.request_history:
            return 0.0
        
        failures = sum(1 for _, outcome in self.request_history if outcome == 0)
        return (failures / len(self.request_history)) * 100

    async def adjust_rate(self):
        """
        Periodically adjusts the concurrency level based on the failure rate.
        This should be run as a background task.
        """
        while True:
            await asyncio.sleep(self.adjustment_interval.total_seconds())
            
            failure_rate = self.get_failure_rate()
            
            if failure_rate > 10.0 and self.concurrency > MIN_CONCURRENCY:
                # High failure rate: slow down
                await self._change_concurrency(-1)
                logger.warning(f"High failure rate ({failure_rate:.2f}%). Reducing concurrency to {self.concurrency}.")

            elif failure_rate < 2.0 and self.concurrency < MAX_CONCURRENCY:
                # Very low failure rate: speed up
                await self._change_concurrency(1)
                logger.info(f"Low failure rate ({failure_rate:.2f}%). Increasing concurrency to {self.concurrency}.")
            else:
                logger.info(f"Stable failure rate ({failure_rate:.2f}%). Concurrency remains at {self.concurrency}.")

    async def _change_concurrency(self, delta: int):
        """Atomically changes the concurrency level and wakes blocked waiters."""
        new_concurrency = self.concurrency + delta
        new_concurrency = max(MIN_CONCURRENCY, min(new_concurrency, MAX_CONCURRENCY))
        if new_concurrency == self.concurrency:
            return

        async with self._capacity_condition:
            self.concurrency = new_concurrency
            self._capacity_condition.notify_all()
