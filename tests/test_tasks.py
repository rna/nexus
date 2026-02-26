import importlib
import unittest

import tasks


class FakeRedis:
    def __init__(self):
        self.sets = {}
        self.lists = {}

    def sadd(self, key, value):
        s = self.sets.setdefault(key, set())
        before = len(s)
        s.add(value)
        return 1 if len(s) != before else 0

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    def rpoplpush(self, src, dst):
        src_list = self.lists.setdefault(src, [])
        if not src_list:
            return None
        value = src_list.pop()
        self.lists.setdefault(dst, []).insert(0, value)
        return value

    def lrem(self, key, count, value):
        if count != 1:
            raise NotImplementedError("FakeRedis only supports count=1 for tests")
        items = self.lists.setdefault(key, [])
        try:
            idx = items.index(value)
        except ValueError:
            return 0
        items.pop(idx)
        return 1


class TasksTests(unittest.TestCase):
    def setUp(self):
        importlib.reload(tasks)
        self.fake = FakeRedis()
        tasks.r = self.fake

    def test_push_urls_to_queue_deduplicates(self):
        pushed = tasks.push_urls_to_queue(["a", "b", "a", "", " "])
        self.assertEqual(pushed, 2)
        self.assertEqual(self.fake.smembers(tasks.SEEN_URLS_SET), {"a", "b"})
        self.assertEqual(self.fake.lists[tasks.SCRAPING_QUEUE], ["a", "b"])

    def test_processing_and_done_flow(self):
        tasks.push_urls_to_queue(["u1"])
        url = tasks.get_url_for_processing()
        self.assertEqual(url, "u1")
        self.assertEqual(self.fake.lists[tasks.SCRAPING_QUEUE], [])
        self.assertEqual(self.fake.lists[tasks.PROCESSING_QUEUE], ["u1"])

        tasks.mark_url_as_done("u1")
        self.assertEqual(self.fake.lists[tasks.PROCESSING_QUEUE], [])
        self.assertIn("u1", self.fake.smembers(tasks.DONE_URLS_SET))

    def test_dlq_flow_removes_from_processing(self):
        tasks.push_urls_to_queue(["u2"])
        tasks.get_url_for_processing()
        tasks.push_to_dlq("u2")
        self.assertEqual(self.fake.lists[tasks.PROCESSING_QUEUE], [])
        self.assertEqual(self.fake.lists[tasks.DLQ_QUEUE], ["u2"])

    def test_requeue_inflight_urls(self):
        self.fake.lists[tasks.PROCESSING_QUEUE] = ["u3", "u4"]
        moved = tasks.requeue_inflight_urls()
        self.assertEqual(moved, 2)
        self.assertEqual(self.fake.lists[tasks.PROCESSING_QUEUE], [])
        self.assertCountEqual(self.fake.lists[tasks.SCRAPING_QUEUE], ["u3", "u4"])


if __name__ == "__main__":
    unittest.main()
