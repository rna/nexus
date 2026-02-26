import unittest

from core.block_detector import BlockType, detect_block


class BlockDetectorTests(unittest.TestCase):
    def test_detects_rate_limit(self):
        self.assertEqual(detect_block("{}", 429), BlockType.RATE_LIMIT)

    def test_detects_captcha_marker(self):
        self.assertEqual(detect_block("Please solve CAPTCHA", 200), BlockType.CAPTCHA)

    def test_detects_html_when_json_expected(self):
        self.assertEqual(detect_block("<html><body>Oops</body></html>", 200), BlockType.UNEXPECTED_FORMAT)

    def test_not_blocked_for_normal_json(self):
        self.assertEqual(detect_block('{"ok": true}', 200), BlockType.NOT_BLOCKED)

    def test_not_blocked_for_json_with_embedded_html_snippet(self):
        body = '{"status":"success","response":{"description":"<html-safe>Rich text</html-safe>"}}'
        self.assertEqual(detect_block(body, 200), BlockType.NOT_BLOCKED)


if __name__ == "__main__":
    unittest.main()
