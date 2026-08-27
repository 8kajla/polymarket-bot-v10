import unittest

from config import COPY_NOTIONAL_FRACTION, WS_PRIORITY_COPY, BOOK_MAX_AGE_SECONDS


class ConfigTests(unittest.TestCase):
    def test_safe_defaults(self):
        self.assertAlmostEqual(COPY_NOTIONAL_FRACTION, 0.10)
        self.assertTrue(WS_PRIORITY_COPY)
        self.assertGreater(BOOK_MAX_AGE_SECONDS, 0)


if __name__ == "__main__":
    unittest.main()
