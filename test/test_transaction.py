# test_transaction.py
import threading
import time
import unittest
import requests

import utils as tu

ORDER_BASE = tu.order_base_url()


class TestOrderTransactionLocks(unittest.TestCase):
    def setUp(self):
        # Skip if order service doesn't expose /test/lock (e.g. old image)
        r = requests.get(f"{ORDER_BASE}/test/lock/__probe__", timeout=5)
        if r.status_code == 404:
            self.skipTest(
                "Order service returned 404 for /test/lock. "
                "Rebuild the order-service image: docker compose up -d --build"
            )

    def _lock_state(self, key: str) -> dict:
        """GET /test/lock/<key> on order service."""
        r = requests.get(f"{ORDER_BASE}/test/lock/{key}")
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def _hold_lock(self, key: str, ms: int):
        """POST /test/hold_lock/<key>/<ms> on order service."""
        return requests.post(f"{ORDER_BASE}/test/hold_lock/{key}/{ms}")

    def _fail_after_lock(self, key: str):
        """POST /test/fail_after_lock/<key> on order service."""
        return requests.post(f"{ORDER_BASE}/test/fail_after_lock/{key}")

    def test_exclusive_lock_visible_during_transaction_and_released_after(self):
        """
        Starts a transactional request that acquires an exclusive lock and sleeps.
        While running, /test/lock should show exclusive_owner != None.
        After completion, /test/lock should show exclusive_owner == None and shared_count == 0.
        """
        key = f"locktest-{int(time.time() * 1000)}"
        hold_ms = 400

        result_holder = {}

        def run_hold():
            resp = self._hold_lock(key, hold_ms)
            result_holder["status_code"] = resp.status_code
            result_holder["text"] = resp.text

        t = threading.Thread(target=run_hold, daemon=True)
        t.start()

        # Give the request time to start and acquire the lock
        time.sleep(0.10)

        state_during = self._lock_state(key)
        self.assertIsNotNone(
            state_during.get("exclusive_owner"),
            f"Expected exclusive_owner during txn, got: {state_during}",
        )

        # Wait for completion
        t.join(timeout=3.0)
        self.assertIn("status_code", result_holder, "Hold-lock request did not finish in time")
        self.assertTrue(200 <= result_holder["status_code"] < 300, result_holder.get("text", ""))

        state_after = self._lock_state(key)
        self.assertIsNone(
            state_after.get("exclusive_owner"),
            f"Expected no exclusive_owner after txn, got: {state_after}",
        )
        self.assertEqual(
            state_after.get("shared_count", 0),
            0,
            f"Expected shared_count=0 after txn, got: {state_after}",
        )

    def test_locks_released_on_failure(self):
        """
        Transaction should release locks even if endpoint aborts.
        /test/fail_after_lock acquires lock then aborts(400).
        After response, lock must be gone.
        """
        key = f"failtest-{int(time.time() * 1000)}"

        r = self._fail_after_lock(key)
        self.assertTrue(400 <= r.status_code < 500, r.text)

        state_after = self._lock_state(key)
        self.assertIsNone(state_after.get("exclusive_owner"), state_after)
        self.assertEqual(state_after.get("shared_count", 0), 0, state_after)


if __name__ == "__main__":
    unittest.main()