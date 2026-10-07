import unittest
from unittest.mock import patch

from analyze import analyze
from load import run_tier
from stage_metrics import read_histograms, summarize_tier
from syscalls import read_syscalls
from request_spans import read_spans


class FakeClient:
    def put_object(self, bucket, key, payload):
        return {"status": 200}


class ExperimentTests(unittest.TestCase):
    def test_span_events_do_not_overwrite_request_attributes_or_span_name(self):
        spans, incomplete = read_spans('Span #0\n\tName : storage\n\tTraceId : abc\n'
                                      '\tSpanId : def\n\tParentSpanId : None (root span)\n'
                                      '\tStart time : 2026-10-08 00:00:01\n'
                                      '\tEnd time : 2026-10-08 00:00:02\n\tAttributes:\n'
                                      '\t\t -> object: String(Owned("c8/1.bin"))\n'
                                      '\tEvents:\n\tName : event\n\tAttributes:\n'
                                      '\t\t -> object: String(Owned("other"))')
        self.assertEqual(spans[0]["Name"], "storage")
        self.assertEqual(spans[0]["attributes"]["object"], "c8/1.bin")
        self.assertEqual(spans[0]["wall_ms"], 1000)
        self.assertEqual(incomplete, 0)

    def test_resumed_syscall_preserves_entry_time_and_file_path(self):
        calls, unmatched = read_syscalls("7 1.000000 fsync(3</volume/object>) <unfinished ...>\n"
                                        "7 1.500000 <... fsync resumed>) = 0 <0.500000>")
        self.assertEqual(calls[0]["start_realtime_ns"], 1_000_000_000)
        self.assertEqual(calls[0]["end_realtime_ns"], 1_500_000_000)
        self.assertEqual(calls[0]["wall_ms"], 500)
        self.assertIn("/volume/object", calls[0]["text"])
        self.assertEqual(unmatched, {"unmatched_returns": 0, "unfinished_calls": 0})

    def test_cumulative_stage_exports_exclude_pre_tier_observations(self):
        text = """Metrics
Metric #0
Name : rustfs_internal_stage_duration_ms
Temporality : Cumulative
EndTime : 2026-10-08 00:00:01.000000
DataPoint #0
Count : 5
Sum : 10.0
-> stage: erasure_encode_cpu
Metrics
Metric #0
Name : rustfs_internal_stage_duration_ms
Temporality : Cumulative
EndTime : 2026-10-08 00:00:03.000000
DataPoint #0
Count : 9
Sum : 22.0
-> stage: erasure_encode_cpu
"""
        points = read_histograms(text)
        result = summarize_tier(points, points[0]["end_realtime_ns"] + 1,
                                points[1]["end_realtime_ns"] - 1)
        self.assertEqual(result[0]["observations"], 4)
        self.assertEqual(result[0]["total_wall_ms"], 12)
        self.assertEqual(result[0]["mean_wall_ms"], 3)
        self.assertTrue(result[0]["baseline_present"])

    def test_histogram_reset_is_rejected_instead_of_reporting_negative_duration(self):
        points = [{"metric": "stage", "attributes": {"stage": "write"},
                   "temporality": "Cumulative", "end_realtime_ns": 1,
                   "count": 5, "sum": 10},
                  {"metric": "stage", "attributes": {"stage": "write"},
                   "temporality": "Cumulative", "end_realtime_ns": 3,
                   "count": 2, "sum": 4}]
        with self.assertRaisesRegex(ValueError, "reset"):
            summarize_tier(points, 2, 3)

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
