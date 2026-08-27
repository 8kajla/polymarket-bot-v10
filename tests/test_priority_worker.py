import time
import unittest
from unittest.mock import patch

from trading import priority


class PriorityWorkerTests(unittest.TestCase):
    def tearDown(self):
        priority.stop()
        priority.READY.clear()
        while not priority.LIVE_WS_QUEUE.empty():
            try:
                priority.LIVE_WS_QUEUE.get_nowait()
            except Exception:
                break

    def test_live_queue_is_processed_without_polling(self):
        state = {"cursor_ts": time.time(), "cursor_id": "", "duplicates_ignored": 0}
        trade = {"side": "BUY", "timestamp": time.time(), "_ws_received_at": time.time()}
        calls = []
        with patch.object(priority, "is_new", return_value=True), \
             patch.object(priority, "process_trade", side_effect=lambda *args: calls.append(args) or True), \
             patch.object(priority, "advance_cursor"):
            priority.start(state)
            priority.set_ready()
            priority.LIVE_WS_QUEUE.put_nowait(trade)
            deadline = time.time() + 2
            while time.time() < deadline and not calls:
                time.sleep(0.01)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][3], "ws")


if __name__ == "__main__":
    unittest.main()
