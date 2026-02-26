import os
import unittest

from discovery.nykaa_batch_runner import (
    QueueMetrics,
    RunStats,
    apply_queue_namespace,
    build_queue_namespace_env,
    get_queue_metrics,
    maybe_apply_seed_aliases,
    should_seed,
)


class _FakeRedis:
    def __init__(self, lengths=None, sets=None):
        self.lengths = lengths or {}
        self.sets = sets or {}

    def llen(self, key):
        return self.lengths.get(key, 0)

    def scard(self, key):
        return self.sets.get(key, 0)


class _FakeTasksModule:
    SCRAPING_QUEUE = "q_pending"
    PROCESSING_QUEUE = "q_processing"
    DLQ_QUEUE = "q_dlq"
    SEEN_URLS_SET = "s_seen"
    DONE_URLS_SET = "s_done"

    def __init__(self, client):
        self.r = client


class NykaaBatchRunnerHelperTests(unittest.TestCase):
    def test_build_queue_namespace_env(self):
        env_map = build_queue_namespace_env("nykaa run 1k")
        self.assertEqual(env_map["SCRAPING_QUEUE"], "nykaa_run_1k_scraping_queue")
        self.assertEqual(env_map["PROCESSING_QUEUE"], "nykaa_run_1k_scraping_processing")
        self.assertEqual(env_map["DLQ_QUEUE"], "nykaa_run_1k_scraping_dlq")
        self.assertEqual(env_map["SEEN_URLS_SET"], "nykaa_run_1k_scraping_seen")
        self.assertEqual(env_map["DONE_URLS_SET"], "nykaa_run_1k_scraping_done")

    def test_apply_queue_namespace_updates_environment(self):
        keys = ["SCRAPING_QUEUE", "PROCESSING_QUEUE", "DLQ_QUEUE", "SEEN_URLS_SET", "DONE_URLS_SET"]
        old = {k: os.environ.get(k) for k in keys}
        try:
            overrides = apply_queue_namespace("resume_run")
            self.assertEqual(os.environ["SCRAPING_QUEUE"], "resume_run_scraping_queue")
            self.assertEqual(overrides["DONE_URLS_SET"], "resume_run_scraping_done")
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_get_queue_metrics_reads_lengths_and_sets(self):
        fake_client = _FakeRedis(
            lengths={"q_pending": 12, "q_processing": 3, "q_dlq": 1},
            sets={"s_seen": 30, "s_done": 18},
        )
        fake_tasks = _FakeTasksModule(fake_client)
        metrics = get_queue_metrics(fake_tasks)
        self.assertEqual(metrics, QueueMetrics(pending=12, processing=3, dlq=1, seen=30, done=18))

    def test_should_seed_behavior(self):
        empty = QueueMetrics(pending=0, processing=0, dlq=0, seen=0, done=0)
        non_empty = QueueMetrics(pending=1, processing=0, dlq=0, seen=1, done=0)
        self.assertTrue(should_seed(enabled=True, only_if_queue_empty=True, queue_metrics=empty))
        self.assertFalse(should_seed(enabled=True, only_if_queue_empty=True, queue_metrics=non_empty))
        self.assertTrue(should_seed(enabled=True, only_if_queue_empty=False, queue_metrics=non_empty))
        self.assertFalse(should_seed(enabled=False, only_if_queue_empty=False, queue_metrics=empty))

    def test_run_stats_record_counts(self):
        stats = RunStats()
        stats.record("success")
        stats.record("failed")
        stats.record("empty")
        self.assertEqual(stats.attempts, 2)
        self.assertEqual(stats.successes, 1)
        self.assertEqual(stats.failures, 1)
        self.assertEqual(stats.empty_polls, 1)

    def test_seed_aliases_only_fill_missing_target_env(self):
        keys = [
            "NYKAA_RUN_SEED_MAX_PRODUCTS",
            "NYKAA_RUN_SEED_MAX_FILES",
            "NYKAA_SITEMAP_MAX_PRODUCTS",
            "NYKAA_SITEMAP_MAX_FILES",
        ]
        old = {k: os.environ.get(k) for k in keys}
        try:
            os.environ["NYKAA_RUN_SEED_MAX_PRODUCTS"] = "1000"
            os.environ["NYKAA_RUN_SEED_MAX_FILES"] = "10"
            os.environ.pop("NYKAA_SITEMAP_MAX_PRODUCTS", None)
            os.environ["NYKAA_SITEMAP_MAX_FILES"] = "500"

            maybe_apply_seed_aliases()

            self.assertEqual(os.environ["NYKAA_SITEMAP_MAX_PRODUCTS"], "1000")
            self.assertEqual(os.environ["NYKAA_SITEMAP_MAX_FILES"], "500")
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
