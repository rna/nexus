import unittest
from unittest import mock

from core.proxy_manager import ProxyManager


class ProxyManagerTests(unittest.TestCase):
    def test_raises_without_proxies(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError):
                ProxyManager()

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


if __name__ == "__main__":
    unittest.main()

