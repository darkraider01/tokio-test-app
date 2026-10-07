import unittest
from unittest.mock import patch

from analyze import analyze
from load import run_tier


class FakeClient:
    def put_object(self, bucket, key, payload):
        return {"status": 200}


class ExperimentTests(unittest.TestCase):
    def test_independent_segments_pair_polls_by_worker_and_timestamp(self):
        events = [
            {"event": "PollEndEvent", "timestamp_ns": 200, "worker_id": 0},
            {"event": "PollStartEvent", "timestamp_ns": 100, "worker_id": 0,
             "spawn_loc": "storage", "local_queue": 3},
            {"event": "PollStartEvent", "timestamp_ns": 110, "worker_id": 1,
             "spawn_loc": "network", "local_queue": 0},
            {"event": "PollEndEvent", "timestamp_ns": 140, "worker_id": 1},
        ]
        result = analyze(events)
        self.assertEqual(result["paired_polls"], 2)
        self.assertEqual(result["spawn_locations"]["storage"]["total_wall_ms"], .0001)
        self.assertEqual(result["spawn_locations"]["network"]["total_wall_ms"], .00003)
        self.assertEqual(result["local_queue_max"], 3)

    def test_partial_tier_polls_are_reported_without_fabricating_durations(self):
        events = [
            {"event": "ClockSyncEvent", "timestamp_ns": 10, "realtime_ns": 1010},
            {"event": "PollStartEvent", "timestamp_ns": 20, "worker_id": 0,
             "spawn_loc": "storage", "local_queue": 0},
            {"event": "PollEndEvent", "timestamp_ns": 40, "worker_id": 0},
        ]
        result = analyze(events, 1030, 1050)
        self.assertEqual(result["paired_polls"], 0)
        self.assertEqual(result["unmatched_poll_ends"], 1)
        self.assertIsNone(result["poll_wall_ms"])

    def test_open_loop_attempts_and_client_shedding_account_for_arrivals(self):
        result = run_tier(FakeClient(), "bucket", b"x", "r100", .05, rate=100, max_active=1)
        self.assertEqual(result["planned_arrivals"], 5)
        self.assertEqual(result["attempted"] + result["client_shed"], 5)
        self.assertEqual(result["completed_ok"] + result["failed"], result["attempted"])
        self.assertEqual(len(result["requests"]), 5)
        for request in result["requests"]:
            if not request.get("client_shed"):
                self.assertGreaterEqual(request["scheduled_to_completion_ms"],
                                        request["attempt_to_completion_ms"])

    def test_full_client_cap_sheds_arrivals_without_counting_server_failures(self):
        with patch("load.threading.BoundedSemaphore") as semaphore:
            semaphore.return_value.acquire.return_value = False
            result = run_tier(FakeClient(), "bucket", b"x", "r100", .05, rate=100)
        self.assertEqual(result["client_shed"], 5)
        self.assertEqual(result["attempted"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertIsNone(result["latency_ms"])

    def test_client_errors_preserve_attempt_latency_and_failure_counts(self):
        class FailingClient:
            def put_object(self, bucket, key, payload):
                raise OSError("connection refused")

        result = run_tier(FailingClient(), "bucket", b"x", "r100", .05, rate=100)
        self.assertEqual(result["attempted"], 5)
        self.assertEqual(result["failed"], 5)
        self.assertEqual(result["completed_ok"], 0)
        self.assertEqual(result["client_shed"], 0)
        self.assertTrue(all("attempt_to_completion_ms" in r for r in result["requests"]))


if __name__ == "__main__":
    unittest.main()
