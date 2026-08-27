import unittest
import time

from feeds import race


class FeedRaceTests(unittest.TestCase):
    def setUp(self):
        with race.LOCK:
            race.PENDING.clear()
            for k in race.STATS:
                race.STATS[k] = 0 if k not in ("last_struct_ms", "last_rtds_ms", "median_advantage_ms") else None

    def test_struct_first_is_recorded(self):
        row = {"hash": "0xabc", "side": "BUY", "size": 1, "price": 0.5,
               "asset": "A", "conditionId": "C", "outcome": "YES", "timestamp": time.time()}
        t = time.time()
        race.record("struct", row, t)
        race.record("rtds", row, t + 0.5)
        snap = race.snapshot()
        self.assertEqual(snap["matched"], 1)
        self.assertEqual(snap["struct_first"], 1)
        self.assertEqual(snap["struct_first_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
