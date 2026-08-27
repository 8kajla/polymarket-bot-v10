import unittest
from unittest.mock import patch

from feeds import market_book


class MarketBookTests(unittest.TestCase):
    def setUp(self):
        with market_book.BOOK_LOCK:
            market_book.BOOKS.clear()
            market_book.SUBSCRIBED.clear()
            market_book.PENDING_SUBSCRIPTIONS.clear()
            market_book.BOOK_STATUS.update({
                "fallbacks": 0, "local_hits": 0, "snapshots": 0,
                "deltas": 0, "last_lookup_source": "NONE",
                "last_lookup_age_ms": None,
            })

    def test_snapshot_and_price_change(self):
        ok = market_book._apply_book({
            "asset_id": "A",
            "timestamp": "1770000000000",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.50", "size": "5"}, {"price": "0.51", "size": "8"}],
        })
        self.assertTrue(ok)
        book, source, age = market_book.get_local_book("A")
        self.assertEqual(source, "WS")
        self.assertEqual(book["asks"][0]["price"], "0.5")

        applied = market_book._apply_price_change({
            "timestamp": "1770000001000",
            "price_changes": [{"asset_id": "A", "price": "0.50", "size": "0", "side": "SELL"},
                               {"asset_id": "A", "price": "0.49", "size": "7", "side": "SELL"}],
        })
        self.assertEqual(applied, 2)
        book, source, age = market_book.get_local_book("A")
        self.assertEqual(source, "WS")
        self.assertEqual(book["asks"][0]["price"], "0.49")
        self.assertEqual(book["asks"][0]["size"], "7")

    def test_rest_fallback_is_used_when_local_book_missing(self):
        rest = {"bids": [{"price": "0.40", "size": "10"}], "asks": [{"price": "0.50", "size": "10"}]}
        with patch.object(market_book, "fetch_book_rest", return_value=rest):
            got = market_book.fetch_book("MISSING")
        self.assertEqual(got, rest)
        self.assertEqual(market_book.BOOK_STATUS["last_lookup_source"], "REST_FALLBACK")
        self.assertEqual(market_book.BOOK_STATUS["fallbacks"], 1)


if __name__ == "__main__":
    unittest.main()
