import unittest

from core.nykaa import (
    build_product_details_api_url,
    extract_product_id_from_url,
    is_nykaa_product_page_url,
    iter_product_api_urls_from_sitemap_locs,
    iter_sitemap_locs,
)
from core.normalizer import normalize_product_data


class NykaaHelpersTests(unittest.TestCase):
    def test_extract_product_id_from_url(self):
        self.assertEqual(
            extract_product_id_from_url("https://www.nykaa.com/some-product/p/688908"),
            "688908",
        )
        self.assertEqual(
            extract_product_id_from_url("https://www.nykaa.com/some-product/p/688908?pps=2"),
            "688908",
        )
        self.assertIsNone(extract_product_id_from_url("https://www.nykaa.com/brands"))

    def test_build_product_details_api_url(self):
        url = build_product_details_api_url(688908, app_version="9.0.0")
        self.assertIn("/products/details", url)
        self.assertIn("productId=688908", url)
        self.assertIn("app_version=9.0.0", url)

    def test_iter_sitemap_locs_and_product_api_urls(self):
        xml_text = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://www.nykaa.com/a/p/111</loc></url>
          <url><loc>https://www.nykaa.com/b/p/222</loc></url>
          <url><loc>https://www.nykaa.com/brands</loc></url>
        </urlset>
        """
        locs = list(iter_sitemap_locs(xml_text))
        self.assertEqual(len(locs), 3)
        api_urls = list(iter_product_api_urls_from_sitemap_locs(locs, app_version="8.6.6"))
        self.assertEqual(len(api_urls), 2)
        self.assertTrue(all("products/details" in url for url in api_urls))

    def test_is_nykaa_product_page_url(self):
        self.assertTrue(is_nykaa_product_page_url("https://www.nykaa.com/abc/p/123"))
        self.assertFalse(is_nykaa_product_page_url("https://www.nykaa.com/brands"))


class NykaaNormalizerTests(unittest.TestCase):
    def test_normalizes_nykaa_product_details_envelope(self):
        raw = {
            "status": "success",
            "response": {
                "id": 688908,
                "name": "Dot & Key Watermelon Cooling Sunscreen SPF 50+",
                "url": "/dot-key-watermelon-cooling-sunscreen-spf-50/p/688908",
                "brand_name": "Dot & Key",
                "final_price": 399,
                "in_stock": True,
                "image_url": "https://images.nykaa.com/example.jpg",
                "ingredients": "Aloe, Watermelon",
            },
        }
        api_url = "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&productId=688908"
        normalized = normalize_product_data(raw, api_url)
        assert normalized is not None
        self.assertEqual(normalized["sku"], "nykaa-688908")
        self.assertEqual(normalized["source_site"], "nykaa")
        self.assertEqual(normalized["source_product_id"], "688908")
        self.assertEqual(normalized["brand"], "Dot & Key")
        self.assertEqual(normalized["price_amount"], 399)
        self.assertEqual(normalized["currency"], "INR")
        self.assertEqual(normalized["availability_status"], "InStock")
        self.assertIn("/p/688908", normalized["product_url"])
        self.assertEqual(normalized["raw_payload"], raw)

    def test_falls_back_to_query_product_id_if_payload_missing_id(self):
        raw = {"status": "success", "response": {"name": "Product"}}
        api_url = "https://www.nykaa.com/app-api/index.php/products/details?app_version=8.6.6&productId=12345"
        normalized = normalize_product_data(raw, api_url)
        assert normalized is not None
        self.assertEqual(normalized["sku"], "nykaa-12345")
        self.assertEqual(normalized["source_product_id"], "12345")


if __name__ == "__main__":
    unittest.main()

