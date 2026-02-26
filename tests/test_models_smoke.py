import unittest


class ModelsSmokeTests(unittest.TestCase):
    def test_models_module_imports(self):
        try:
            import models  # noqa: F401
        except ModuleNotFoundError as exc:
            self.skipTest(f"Dependency missing locally: {exc}")


if __name__ == "__main__":
    unittest.main()

