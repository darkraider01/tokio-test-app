import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analyze import analyze
from fs_probe import (HEADER, KIND_JOB_END, KIND_JOB_START, KIND_JOIN_READY,
                      KIND_SEND_OK, KIND_SUBMIT, RECORD, analyze_run,
                      build_poll_index,
                      containing_poll, group_jobs, job_intervals, op_hash,
                      pair_polls, read_probe, reconstruct_wait, step_hash,
                      summarize_jobs)
from load import run_tier
from stage_metrics import read_histograms, summarize_tier
from syscalls import read_syscalls
from request_spans import read_spans


def _probe_dump(records, *, total_seen=None, capacity=64, magic=b"RFSPRB01", version=1,
                flushed_monotonic_ns=1000, flushed_realtime_ns=2000):
    """Build a probe dump exactly like the Rust flush writes one."""
    body = b"".join(RECORD.pack(r["kind"], 0, 0, r["step"], r["tid"], 0,
                                r["id"], r["ts"], r["a"]) for r in records)
    header = HEADER.pack(magic, version, RECORD.size, capacity,
                         len(records) if total_seen is None else total_seen,
                         flushed_monotonic_ns, flushed_realtime_ns, 1, 0, 4242)
    return header + body


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


class FsProbeTests(unittest.TestCase):
    """Round-trip, reconstruction, and provenance tests for `fs_probe.py`."""

    @staticmethod
    def _record(kind, ts, *, tid=10, task_id=0, step=0, a=0):
        return {"kind": kind, "name": "", "step": step, "tid": tid,
                "id": task_id, "ts": ts, "a": a}

    def test_op_and_step_hashes_match_the_rust_probe(self):
        # Vectors generated by the probe's own Rust `op_hash`/`step_hash`
        # (identical bodies to `.repro/tokio-blocking-probe/src/fs_probe.rs`).
        self.assertEqual(op_hash("acceptance", "c8/259.bin"), 12264410275275888176)
        self.assertEqual(op_hash("", ""), 12638123428881205758)
        self.assertEqual(op_hash("a", "b"), 16582468340193742689)
        # The experiment's real join key (bucket/object as the probe hashes it).
        self.assertEqual(op_hash("tokio-experiment", "c8/259.bin"), 12392989869598242771)
        self.assertEqual(op_hash("tokio-experiment", "warmup/0.bin"), 7598290272417279984)
        self.assertEqual(step_hash("mkdir"), 2883839448)
        self.assertEqual(step_hash("make_dir_all"), 2398025995)
        self.assertEqual(step_hash("rename"), 2180167635)
        self.assertEqual(step_hash("rename_no_owner"), 3779027443)

    def test_probe_dump_round_trip_preserves_header_and_records(self):
        records = [
            self._record(1, 100, tid=7, task_id=42, step=5, a=99),
            self._record(2, 150, tid=8, task_id=42),
            self._record(3, 250, tid=8, task_id=42),
        ]
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        # A dump keeps exactly min(total_seen, capacity) records; here all
        # survive (3 <= 3) while 997 records beyond capacity were never stored.
        path.write_bytes(_probe_dump(records, total_seen=1000, capacity=3))
        header, parsed = read_probe(path)
        self.assertEqual(header["capacity"], 3)
        self.assertEqual(header["total_seen"], 1000)
        self.assertEqual(header["dropped_records"], 997)
        self.assertEqual(header["records"], 3)
        self.assertEqual(header["flushed_monotonic_ns"], 1000)
        self.assertEqual(header["flushed_realtime_ns"], 2000)
        self.assertEqual(header["pid"], 4242)
        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0]["kind"], 1)
        self.assertEqual(parsed[0]["name"], "submit")
        self.assertEqual(parsed[0]["step"], 5)
        self.assertEqual(parsed[0]["tid"], 7)
        self.assertEqual(parsed[0]["id"], 42)
        self.assertEqual(parsed[0]["ts"], 100)
        self.assertEqual(parsed[0]["a"], 99)

    def test_probe_dump_with_bad_magic_is_rejected(self):
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump([self._record(1, 1)], magic=b"NOTAPROB"))
        with self.assertRaisesRegex(ValueError, "magic"):
            read_probe(path)

    def test_probe_dump_with_partial_trailing_record_is_rejected(self):
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        raw = _probe_dump([self._record(1, 1)]) + b"\x00\x01\x02"
        path.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "partial"):
            read_probe(path)

    def test_probe_dump_record_count_must_match_header(self):
        # total_seen below the written count means a truncated/corrupt dump.
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump([self._record(1, 1), self._record(2, 2)], total_seen=1))
        with self.assertRaisesRegex(ValueError, "record count"):
            read_probe(path)

    def test_records_are_grouped_per_job_with_join_events_attached(self):
        records = [
            self._record(1, 100, task_id=1, step=11),
            self._record(2, 150, task_id=1),
            self._record(3, 250, task_id=1),
            self._record(4, 300, task_id=1),
            self._record(5, 400, task_id=1),
            self._record(1, 110, task_id=2, step=22),
            self._record(5, 500, task_id=999),  # join without a blocking job
        ]
        jobs, counters = group_jobs(records)
        self.assertEqual(set(jobs), {1, 2})
        self.assertEqual(counters["non_blocking_join_ready"], 1)
        self.assertEqual(counters["duplicate_boundaries"], 0)
        self.assertIn("join_ready", jobs[1])
        self.assertNotIn("join_ready", jobs[2])
        self.assertEqual(jobs[1]["submit"]["step"], 11)
        self.assertEqual(jobs[2]["submit"]["step"], 22)

    def test_duplicate_boundary_is_counted_not_silently_merged(self):
        records = [self._record(1, 100, task_id=1), self._record(1, 101, task_id=1)]
        jobs, counters = group_jobs(records)
        self.assertEqual(counters["duplicate_boundaries"], 1)
        # The earliest record wins; the later duplicate is not merged in.
        self.assertEqual(jobs[1]["submit"]["ts"], 100)

    def test_missing_boundaries_yield_none_intervals_not_zeros(self):
        records = [self._record(1, 100, task_id=1, step=7)]
        jobs, _ = group_jobs(records)
        summary = job_intervals(jobs[1], None)
        self.assertEqual(summary["missing"],
                         ["job_start", "job_end", "job_complete", "join_ready"])
        for field in ("submit_to_start_ms", "start_to_end_ms", "end_to_complete_ms",
                      "completion_to_poll_start_proxy_ms", "poll_start_to_join_ready_ms",
                      "total_submit_to_join_ready_ms"):
            self.assertIsNone(summary[field], field)
        self.assertEqual(summary["task_id"], 1)
        self.assertEqual(summary["step_hash"], 7)
        self.assertEqual(summary["op_hash"], 0)

    def test_full_job_intervals_use_only_observed_boundaries(self):
        records = [
            self._record(1, 1_000_000, task_id=1),
            self._record(2, 3_000_000, tid=20, task_id=1),
            self._record(3, 13_000_000, tid=20, task_id=1),
            self._record(4, 15_000_000, tid=20, task_id=1),
            self._record(5, 45_000_000, tid=30, task_id=1),
        ]
        polls = [{"tid": 30, "worker_id": 1, "start": 40_000_000, "end": 60_000_000,
                  "spawn_loc": None, "task_id": 1, "local_queue": 0}]
        jobs, _ = group_jobs(records)
        summary = job_intervals(jobs[1], build_poll_index(polls))
        self.assertEqual(summary["missing"], [])
        self.assertEqual(summary["submit_to_start_ms"], 2.0)
        self.assertEqual(summary["start_to_end_ms"], 10.0)
        self.assertEqual(summary["end_to_complete_ms"], 2.0)
        self.assertEqual(summary["completion_to_poll_start_proxy_ms"], 25.0)
        self.assertEqual(summary["poll_start_to_join_ready_ms"], 5.0)
        self.assertEqual(summary["total_submit_to_join_ready_ms"], 44.0)
        self.assertEqual(summary["job_tid"], 20)
        self.assertEqual(summary["resume_poll_start_ns"], 40_000_000)

    def test_containing_poll_requires_same_thread_and_interval(self):
        polls = [
            {"tid": 10, "worker_id": 0, "start": 100, "end": 200, "spawn_loc": None,
             "task_id": 1, "local_queue": 0},
            {"tid": 10, "worker_id": 0, "start": 300, "end": 400, "spawn_loc": None,
             "task_id": 2, "local_queue": 0},
            {"tid": 11, "worker_id": 1, "start": 150, "end": 250, "spawn_loc": None,
             "task_id": 3, "local_queue": 0},
        ]
        index = build_poll_index(polls)
        # Inside the first interval on the right thread.
        self.assertEqual(containing_poll(index, 150, 10)["task_id"], 1)
        # A thread id never seen returns None even when another thread covers it.
        self.assertIsNone(containing_poll(index, 150, 12))
        # Each thread only resolves to its own intervals.
        self.assertEqual(containing_poll(index, 220, 11)["task_id"], 3)
        # Gap between intervals on the same thread.
        self.assertIsNone(containing_poll(index, 250, 10))
        # Before every interval.
        self.assertIsNone(containing_poll(index, 50, 10))

    def test_wait_reconstruction_splits_send_and_resume_poll(self):
        op = op_hash("acceptance", "c8/259.bin")
        wait_begin = self._record(12, 1_000_000, tid=30, a=op)
        wait_end = self._record(13, 41_590_000, tid=30, a=op)
        send = self._record(14, 41_570_000, tid=20, a=op)
        polls = [{"tid": 30, "worker_id": 1, "start": 41_575_000, "end": 50_000_000,
                  "spawn_loc": None, "task_id": 34625, "local_queue": 0}]
        result = reconstruct_wait([wait_begin, wait_end, send],
                                  build_poll_index(polls), wait_begin, wait_end, op)
        self.assertEqual(result["missing"], [])
        self.assertEqual(result["send_kind"], "send_ok")
        self.assertEqual(result["wait_total_ms"], 40.59)
        self.assertEqual(result["wait_begin_to_send_ms"], 40.57)
        self.assertEqual(result["send_to_resume_poll_ms"], 0.005)
        self.assertEqual(result["resume_poll_to_wait_end_ms"], 0.015)
        self.assertEqual(result["resume_poll_task"], 34625)

    def test_wait_reconstruction_reports_missing_send_and_poll(self):
        op = op_hash("acceptance", "c8/259.bin")
        wait_begin = self._record(12, 1_000, tid=30, a=op)
        wait_end = self._record(13, 2_000, tid=30, a=op)
        result = reconstruct_wait([wait_begin, wait_end], None, wait_begin, wait_end, op)
        self.assertEqual(sorted(result["missing"]), ["resume_poll", "send"])
        self.assertIsNone(result["send_ts"])
        self.assertIsNone(result["wait_begin_to_send_ms"])
        self.assertIsNone(result["send_to_resume_poll_ms"])
        self.assertIsNone(result["resume_poll_to_wait_end_ms"])
        self.assertEqual(result["wait_total_ms"], 0.001)

    def test_wait_reconstruction_ignores_sends_outside_the_window(self):
        op = op_hash("acceptance", "c8/259.bin")
        other = op + 1 if op < 2**64 - 1 else 1
        wait_begin = self._record(12, 1_000, tid=30, a=op)
        wait_end = self._record(13, 2_000, tid=30, a=op)
        records = [
            self._record(14, 500, tid=20, a=op),     # before the wait
            self._record(14, 3_000, tid=20, a=op),   # after the wait
            self._record(14, 1_500, tid=20, a=other)  # different operation
        ]
        result = reconstruct_wait(records, None, wait_begin, wait_end, op)
        self.assertEqual(result["missing"], ["send", "resume_poll"])
        self.assertIsNone(result["send_ts"])

    def test_summarize_jobs_counts_complete_and_incomplete_jobs(self):
        complete = [self._record(1, 100, task_id=1), self._record(2, 200, task_id=1),
                    self._record(3, 300, task_id=1), self._record(4, 400, task_id=1),
                    self._record(5, 500, task_id=1)]
        incomplete = [self._record(1, 110, task_id=2)]
        jobs, _ = group_jobs(complete + incomplete)
        result = summarize_jobs(jobs)
        self.assertEqual(result["jobs"], 2)
        self.assertEqual(result["complete_jobs"], 1)
        self.assertEqual(result["incomplete_jobs"], 1)
        # The incomplete job contributes nothing to the interval statistics.
        self.assertEqual(result["intervals"]["submit_to_start_ms"]["observations"], 1)
        self.assertEqual(result["intervals"]["submit_to_start_ms"]["ms"]["p50"], 0.0001)
        self.assertEqual(result["intervals"]["start_to_end_ms"]["observations"], 1)
        self.assertEqual(result["intervals"]["completion_to_poll_start_proxy_ms"]["observations"], 0)
        self.assertIsNone(result["intervals"]["completion_to_poll_start_proxy_ms"]["ms"])

    def test_records_beyond_capacity_are_reported_as_dropped(self):
        # Recording stops at capacity: the dump keeps exactly `capacity` records
        # in claim order and total_seen counts everything never stored.
        records = [self._record(1, ts, task_id=ts) for ts in range(100, 104)]
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump(records, total_seen=104, capacity=4))
        header, parsed = read_probe(path)
        self.assertEqual(header["records"], 4)
        self.assertEqual(header["dropped_records"], 100)
        self.assertEqual([r["ts"] for r in parsed], [100, 101, 102, 103])

    def test_unknown_record_kind_is_kept_with_a_placeholder_name(self):
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump([self._record(77, 1)]))
        _, parsed = read_probe(path)
        self.assertEqual(parsed[0]["kind"], 77)
        self.assertEqual(parsed[0]["name"], "kind_77")

    def test_pair_polls_tags_workers_and_counts_unmatched_events(self):
        workers = {0: 10, 1: 30}
        events = [
            {"event": "PollStartEvent", "worker_id": 1, "task_id": 7, "timestamp_ns": 100},
            {"event": "PollEndEvent", "worker_id": 1, "timestamp_ns": 200},
            {"event": "PollEndEvent", "worker_id": 0, "timestamp_ns": 300},   # no start
            {"event": "PollStartEvent", "worker_id": 0, "task_id": 8, "timestamp_ns": 400},
            {"event": "PollStartEvent", "worker_id": 0, "task_id": 9, "timestamp_ns": 500},
            {"event": "ClockSyncEvent", "timestamp_ns": 600},                 # not a poll
        ]
        polls, counters = pair_polls(events, workers)
        # The paired poll carries the worker's OS thread id, not its id number.
        self.assertEqual(len(polls), 1)
        self.assertEqual(polls[0]["tid"], 30)
        self.assertEqual(polls[0]["task_id"], 7)
        self.assertEqual(counters["unmatched_poll_ends"], 1)
        self.assertEqual(counters["overwritten_poll_starts"], 1)
        self.assertEqual(counters["unmatched_poll_starts"], 1)
        # A poll whose worker never parked has no thread identity: refuse to guess.
        with self.assertRaisesRegex(ValueError, "without a thread identity"):
            pair_polls([{"event": "PollStartEvent", "worker_id": 5, "timestamp_ns": 1},
                        {"event": "PollEndEvent", "worker_id": 5, "timestamp_ns": 2}],
                       workers)

    def test_analyze_run_reconstructs_a_full_run_directory(self):
        """End-to-end: dump + Dial9 trace + tiers -> joined diagnostic record."""
        base = Path(self.enterContext(tempfile.TemporaryDirectory()))
        run = base / "run-1"
        (run / "telemetry" / "rustfs-tokio").mkdir(parents=True)
        attempt = 1_000_000_000
        offset = 1_791_000_000_000_000_000  # realtime - monotonic on this host
        bucket, key = "tokio-experiment", "c8/259.bin"
        op, step = op_hash(bucket, key), step_hash("mkdir")
        records = [
            self._record(10, attempt + 1_000_000, tid=30, a=op),
            self._record(1, attempt + 2_000_000, tid=30, task_id=42, step=step, a=op),
            self._record(2, attempt + 5_000_000, tid=10, task_id=42),
            self._record(3, attempt + 25_000_000, tid=10, task_id=42),
            self._record(4, attempt + 26_000_000, tid=10, task_id=42),
            self._record(12, attempt + 30_000_000, tid=30, a=op),
            self._record(14, attempt + 55_000_000, tid=10, a=op),
            self._record(5, attempt + 57_000_000, tid=30, task_id=42),
            self._record(13, attempt + 60_000_000, tid=30, a=op),
            self._record(11, attempt + 62_000_000, tid=30, a=op),
        ]
        (run / "fs-probe.bin").write_bytes(_probe_dump(
            records, flushed_monotonic_ns=attempt + 70_000_000,
            flushed_realtime_ns=attempt + 70_000_000 + offset))
        events = [
            {"event": "ClockSyncEvent", "realtime_ns": attempt + 10_000_000 + offset,
             "timestamp_ns": attempt + 10_000_000},
            {"event": "WorkerParkEvent", "worker_id": 1, "tid": 30,
             "timestamp_ns": attempt},
            {"event": "PollStartEvent", "worker_id": 1, "task_id": 34625,
             "timestamp_ns": attempt + 56_000_000},
            {"event": "PollEndEvent", "worker_id": 1, "task_id": 34625,
             "timestamp_ns": attempt + 61_000_000},
        ]
        (run / "telemetry" / "rustfs-tokio" / "trace.0.jsonl").write_text(
            "\n".join(json.dumps(e) for e in events) + "\n")
        (run / "tiers.json").write_text(json.dumps([{
            "tier": "c8", "bucket": bucket, "mode": "closed_loop",
            "planned_arrivals": 1,
            "requests": [{"key": key, "attempted_ns": attempt,
                          "attempted_realtime_ns": attempt + offset,
                          "attempt_to_completion_ms": 99.69, "status": 200}]}]))
        (base / "manifest.json").write_text(
            json.dumps({"fs_probe_enabled": True, "binary_sha256": "abc"}))

        result = analyze_run(run)
        self.assertEqual(result["manifest"]["fs_probe_enabled"], True)
        self.assertEqual(result["clock"]["offset_delta_ns"], 0)
        self.assertEqual(result["dial9"]["workers"], {1: 30})
        self.assertEqual(result["polls"]["paired"], 1)
        self.assertEqual(result["record_counts"]["submit"], 1)
        self.assertEqual(result["step_hashes"], {"mkdir": step})

        tier = result["tiers"][0]
        self.assertEqual(tier["missing"], [])
        self.assertEqual(tier["op_hash"], op)
        self.assertEqual(len(tier["waits"]), 1)
        wait = tier["waits"][0]
        self.assertEqual(wait["missing"], [])
        self.assertEqual(wait["send_kind"], "send_ok")
        self.assertEqual(wait["wait_total_ms"], 30.0)
        self.assertEqual(wait["wait_begin_to_send_ms"], 25.0)
        self.assertEqual(wait["send_to_resume_poll_ms"], 1.0)
        self.assertEqual(wait["resume_poll_to_wait_end_ms"], 4.0)
        self.assertEqual(wait["resume_poll_task"], 34625)

        jobs = tier["jobs_for_operation"]
        self.assertEqual(jobs["jobs"], 1)
        self.assertEqual(jobs["complete_jobs"], 1)
        self.assertEqual(jobs["intervals"]["submit_to_start_ms"]["ms"]["p50"], 3.0)
        self.assertEqual(jobs["intervals"]["start_to_end_ms"]["ms"]["p50"], 20.0)
        self.assertEqual(jobs["intervals"]["end_to_complete_ms"]["ms"]["p50"], 1.0)
        self.assertEqual(jobs["intervals"]["completion_to_poll_start_proxy_ms"]["ms"]["p50"], 30.0)
        self.assertEqual(jobs["intervals"]["poll_start_to_join_ready_ms"]["ms"]["p50"], 1.0)
        self.assertEqual(tier["job_steps"], {"mkdir": step})

        timeline = tier["operation_records"]
        self.assertEqual(timeline[0]["name"], "op_begin")
        self.assertEqual(timeline[0]["offset_ms"], 1.0)
        self.assertEqual(timeline[-1]["name"], "op_end")
        self.assertEqual(timeline[-1]["offset_ms"], 62.0)
        # The whole diagnostic must be JSON-serializable (it is the report).
        json.dumps(result)


if __name__ == "__main__":
    unittest.main()
