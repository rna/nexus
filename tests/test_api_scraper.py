import os
import unittest
from unittest import mock

try:
    from core.api_scraper import ApiScraper
    from core.proxy_manager import DIRECT_PROXY_SENTINEL
except ModuleNotFoundError:
    ApiScraper = None
    DIRECT_PROXY_SENTINEL = "direct://"


class DummyProxyManager:
    def __init__(self, proxy_url=DIRECT_PROXY_SENTINEL):
        self.proxy_url = proxy_url
        self.successes = 0
        self.failures = 0

    def get_proxy(self):
        return self.proxy_url

    def record_success(self, proxy_url):
        self.successes += 1

    def record_failure(self, proxy_url):
        self.failures += 1


class FakeResponse:
    def __init__(self, status_code=200, text='{"ok": true}', json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = {"ok": True} if json_data is None else json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_data


class ApiScraperTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if ApiScraper is None:
            self.skipTest("ApiScraper dependencies not installed locally.")
        self.proxy_manager = DummyProxyManager()

    def test_builds_nykaa_headers_with_referer(self):
        with mock.patch.dict(os.environ, {"HTTP_CLIENT_BACKEND": "httpx"}, clear=False):
            scraper = ApiScraper(self.proxy_manager)
        url = "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&product_id=688908"
        headers = scraper._build_request_headers(url, None)
        self.assertIn("Referer", headers)
        self.assertEqual(headers["Origin"], "https://www.nykaa.com")
        self.assertIn("/p/688908", headers["Referer"])
        self.assertEqual(headers["Sec-Fetch-Site"], "same-origin")

    def test_auto_backend_prefers_curl_for_nykaa_when_available(self):
        with mock.patch("core.api_scraper.curl_requests", object()):
            with mock.patch.dict(os.environ, {"HTTP_CLIENT_BACKEND": "auto"}, clear=False):
                scraper = ApiScraper(self.proxy_manager)
            self.assertTrue(
                scraper._should_use_curl_backend(
                    "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&product_id=688908"
                )
            )
            self.assertFalse(scraper._should_use_curl_backend("https://api.example.com/items/1"))

    async def test_get_retries_with_curl_after_block_in_auto_mode(self):
        with mock.patch("core.api_scraper.curl_requests", object()):
            with mock.patch.dict(os.environ, {"HTTP_CLIENT_BACKEND": "auto"}, clear=False):
                scraper = ApiScraper(self.proxy_manager)

        url = "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&product_id=688908"
        responses = [
            FakeResponse(status_code=403, text="<html>Access Denied</html>"),
            FakeResponse(status_code=200, text='{"ok": true}', json_data={"status": "success"}),
        ]
        perform_calls = []

        async def fake_perform_request(url_arg, proxy_url_arg, headers_arg, *, use_curl):
            perform_calls.append(use_curl)
            return responses.pop(0)

        with mock.patch.object(scraper, "_should_use_curl_backend", return_value=False):
            with mock.patch.object(scraper, "_perform_request", side_effect=fake_perform_request):
                result = await scraper.get(url)

        self.assertEqual(result, {"status": "success"})
        self.assertEqual(perform_calls, [False, True])
        self.assertEqual(self.proxy_manager.successes, 1)
        self.assertEqual(self.proxy_manager.failures, 0)

    async def test_get_returns_none_when_no_proxy_available(self):
        scraper = ApiScraper(DummyProxyManager(proxy_url=None))
        result = await scraper.get("https://example.com/api")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
