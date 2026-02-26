import unittest
from unittest import mock

from core.proxy_manager import ProxyManager


class ProxyManagerTests(unittest.TestCase):
    def test_raises_without_proxies(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError):
                ProxyManager()

    def test_allows_explicit_direct_egress_mode(self):
        with mock.patch.dict("os.environ", {"ALLOW_DIRECT_EGRESS": "1"}, clear=True):
            pm = ProxyManager()
            self.assertEqual(pm.get_proxy(), "direct://")

    def test_loads_and_rotates_proxies(self):
        env = {"PROXY_URLS": "http://user:pass@host1:1111,http://user:pass@host2:2222"}
        with mock.patch.dict("os.environ", env, clear=True):
            pm = ProxyManager()
            selected = pm.get_proxy()
            self.assertIn(selected, {p.url for p in pm.proxies})

    def test_failure_can_trigger_cooldown(self):
        env = {"PROXY_URLS": "http://user:pass@host1:1111"}
        with mock.patch.dict("os.environ", env, clear=True):
            pm = ProxyManager(cooldown_period_seconds=1, health_threshold=95)
            proxy_url = pm.proxies[0].url
            pm.record_failure(proxy_url)
            self.assertTrue(pm.proxies[0].is_cooling_down)
            self.assertIsNone(pm.get_proxy())

    def test_direct_egress_not_penalized(self):
        with mock.patch.dict("os.environ", {"ALLOW_DIRECT_EGRESS": "1"}, clear=True):
            pm = ProxyManager()
            proxy_url = pm.get_proxy()
            pm.record_failure(proxy_url)
            self.assertEqual(pm.proxies[0].health_score, 100)
            self.assertFalse(pm.proxies[0].is_cooling_down)


if __name__ == "__main__":
    unittest.main()
