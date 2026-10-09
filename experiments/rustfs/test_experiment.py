import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analyze import analyze
from fs_probe import (HEADER, HEADER_V2_EXT, KIND_JOB_END, KIND_JOB_START,
                      KIND_JOIN_READY, KIND_OP_BEGIN, KIND_OP_END,
                      KIND_SEND_OK, KIND_SUB, KIND_SUBMIT, KIND_WAIT_BEGIN,
                      KIND_WAIT_END, RECORD, analyze_run, build_poll_index,
                      classify_pool_tids, containing_poll, group_jobs,
                      job_calls, job_intervals, job_stages, op_hash,
                      operation_job_rows, pair_polls, read_probe,
                      reconstruct_wait, step_hash, summarize_jobs)
from load import run_tier
from stage_metrics import read_histograms, summarize_tier
from fs_trace import (analyze as fs_trace_analyze, decompose_wrapper,
                      derive_syscall_mapping, group_clusters, merge_intervals,
                      parse_settings_file, parse_stats_file, parse_trace,
                      per_tag_totals, state_segments, syscall_windows_tid,
                      to_ns, union_regions, union_state_totals, union_summary,
                      validate_clock, zone_of)
from syscalls import read_syscalls
from request_spans import read_spans


def _probe_dump(records, *, total_seen=None, capacity=64, magic=b"RFSPRB01", version=2,
                flushed_monotonic_ns=1000, flushed_realtime_ns=2000,
                stored=None, rejected_capacity=None, rejected_closed=0):
    """Build a probe dump exactly like the Rust flush writes one.

    Version 2 (the current flush): the unchanged 64-byte header plus the
    `stored, rejected_capacity, rejected_closed` extension before the
    records.  Version 1 (older captures / compatibility tests): header
    only, counters derived by the reader as before.  Pass explicit
    `stored` / `rejected_capacity` only to build corrupt dumps for
    validation tests.
    """
    body = b"".join(RECORD.pack(r["kind"], 0, 0, r["step"], r["tid"], 0,
                                r["id"], r["ts"], r["a"]) for r in records)
    total = len(records) if total_seen is None else total_seen
    header = HEADER.pack(magic, version, RECORD.size, capacity, total,
                         flushed_monotonic_ns, flushed_realtime_ns, 1, 0, 4242)
    if version == 2:
        if stored is None:
            stored = min(total, capacity)
        if rejected_capacity is None:
            rejected_capacity = total - stored
        header += HEADER_V2_EXT.pack(stored, rejected_capacity,
                                     rejected_closed)
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

    def test_commit_path_and_sub_tag_hashes_match_the_rust_probe(self):
        # Same vectors as `fs_probe::step_hash_vectors` in the probe's
        # `src/fs_probe.rs`, verified by `cargo test --lib step_hashes_match`.
        for tag, expected in [
            ("dest_meta_read", 3173220776),
            ("staged_meta_write", 1136281453),
            ("src_dir_sync", 3702088569),
            ("rename_data_dir", 3947226104),
            ("rename_meta", 64001953),
            ("dst_dir_fsync", 379138918),
            ("ancestor_fsync", 2435288110),
            ("sub_scan", 1830077803),
            ("sub_prep_open", 2700932782),
            ("sub_prep_write", 3442534505),
            ("sub_fdatasync", 3828332939),
            ("sub_fsync_files", 2779959459),
            ("sub_dir_open", 605670494),
            ("sub_dir_sync", 1793101661),
            ("sub_rename", 3645352792),
            ("sub_scan_end", 2660144945),
            ("sub_prep_open_end", 1884947536),
            ("sub_prep_write_end", 1276132143),
            ("sub_fdatasync_end", 2941781905),
            ("sub_fsync_files_end", 3871831177),
            ("sub_dir_open_end", 169304928),
            ("sub_dir_sync_end", 1067621507),
            ("sub_rename_end", 3128992490),
        ]:
            self.assertEqual(step_hash(tag), expected, tag)

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

    def test_version2_dump_round_trips_explicit_counters(self):
        records = [
            self._record(1, 100, tid=7, task_id=42, step=5, a=99),
            self._record(2, 150, tid=8, task_id=42),
            self._record(3, 250, tid=8, task_id=42),
        ]
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump(records, total_seen=1000, capacity=3,
                                     rejected_closed=7))
        header, parsed = read_probe(path)
        self.assertEqual(header["version"], 2)
        self.assertEqual(header["records"], 3)
        self.assertEqual(header["stored_records"], 3)
        self.assertEqual(header["rejected_capacity"], 997)
        self.assertEqual(header["dropped_records"], 997)
        self.assertEqual(header["rejected_closed"], 7)
        self.assertEqual(header["total_seen"], 1000)
        self.assertEqual(len(parsed), 3)

    def test_version1_dump_stays_readable_with_missing_closed_counter(self):
        # Backward compatibility: captures produced before the format-2
        # extension keep their layout and meanings; the closed-rejection
        # counter did not exist and stays missing (None), never zero.
        records = [self._record(1, 100, tid=7), self._record(2, 200, tid=7)]
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        path.write_bytes(_probe_dump(records, version=1))
        header, parsed = read_probe(path)
        self.assertEqual(header["version"], 1)
        self.assertEqual(header["records"], 2)
        self.assertEqual(header["stored_records"], 2)
        self.assertEqual(header["rejected_capacity"], 0)
        self.assertIsNone(header["rejected_closed"])
        self.assertEqual([r["ts"] for r in parsed], [100, 200])

    def test_version2_dump_with_inconsistent_counters_is_rejected(self):
        records = [self._record(1, 100)]
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "fs-probe.bin"
        # stored > capacity cannot happen through the flush; reject it.
        path.write_bytes(_probe_dump(records, total_seen=10, capacity=4,
                                     stored=5))
        with self.assertRaises(ValueError):
            read_probe(path)
        # stored != min(total_seen, capacity) is likewise corruption.
        path.write_bytes(_probe_dump(records, total_seen=9, capacity=64,
                                     stored=2))
        with self.assertRaises(ValueError):
            read_probe(path)

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

    def test_quorum_send_fields_decode_when_recorded_and_stay_null_when_absent(self):
        op = op_hash("acceptance", "c8/259.bin")
        wait_begin = self._record(12, 1_000, tid=30, a=op)
        wait_end = self._record(13, 4_000, tid=30, a=op)
        # New probe encoding: step=results_seen, id=write_quorum,
        # reserved2=disk_count (the fanout size).
        send = dict(self._record(14, 3_000, tid=20, a=op, task_id=3))
        send["step"] = 4
        send["reserved2"] = 4
        polls = [{"tid": 30, "worker_id": 1, "start": 3_100, "end": 5_000,
                  "spawn_loc": None, "task_id": 7, "local_queue": 0}]
        result = reconstruct_wait([wait_begin, wait_end, send],
                                  build_poll_index(polls), wait_begin, wait_end, op)
        self.assertEqual(result["results_seen"], 4)
        self.assertEqual(result["write_quorum"], 3)
        self.assertEqual(result["disk_count"], 4)
        # Pre-extension captures record all three fields as 0 -> reported as
        # null, never as a fabricated zero quorum.
        legacy = dict(self._record(14, 3_000, tid=20, a=op))
        legacy["reserved2"] = 0
        result = reconstruct_wait([wait_begin, wait_end, legacy],
                                  build_poll_index(polls), wait_begin, wait_end, op)
        self.assertIsNone(result["results_seen"])
        self.assertIsNone(result["write_quorum"])
        self.assertIsNone(result["disk_count"])

    def test_job_cpu_and_offcpu_come_from_the_thread_clock(self):
        # job_start/job_end carry CLOCK_THREAD_CPUTIME_ID in `a`.
        start = self._record(2, 1_000_000, tid=30, task_id=42, a=5_000_000)
        end = self._record(3, 3_000_000, tid=30, task_id=42, a=6_500_000)
        summary = job_intervals({"job_start": start, "job_end": end}, None)
        self.assertEqual(summary["start_to_end_ms"], 2.0)
        self.assertEqual(summary["closure_cpu_ms"], 1.5)
        self.assertEqual(summary["closure_offcpu_ms"], 0.5)
        # Captures before the extension carry a=0: nulls, never zeros.
        legacy = job_intervals({"job_start": self._record(2, 1_000_000, tid=30, task_id=42),
                                "job_end": self._record(3, 3_000_000, tid=30, task_id=42)},
                               None)
        self.assertIsNone(legacy["closure_cpu_ms"])
        self.assertIsNone(legacy["closure_offcpu_ms"])
        self.assertEqual(legacy["start_to_end_ms"], 2.0)
        # A CPU clock running backwards across boundaries would be nonsense:
        # it stays null instead of producing a negative cpu figure.
        backwards = job_intervals({"job_start": self._record(2, 1_000_000, tid=30, task_id=42, a=9_000_000),
                                   "job_end": self._record(3, 3_000_000, tid=30, task_id=42, a=8_000_000)},
                                  None)
        self.assertIsNone(backwards["closure_cpu_ms"])
        self.assertIsNone(backwards["closure_offcpu_ms"])
        # One endpoint missing: cpu stays null while wall time still reports.
        half = job_intervals({"job_start": start}, None)
        self.assertIsNone(half["closure_cpu_ms"])
        self.assertIsNone(half["start_to_end_ms"])

    def test_sub_markers_group_into_named_stages(self):
        start = self._record(KIND_JOB_START, 1_000, tid=30, task_id=42)
        markers = [self._record(KIND_SUB, 2_500, tid=30, task_id=42,
                                step=step_hash("sub_dir_open")),
                   self._record(KIND_SUB, 3_000, tid=30, task_id=42,
                                step=step_hash("sub_dir_sync"))]
        end = self._record(KIND_JOB_END, 10_000, tid=30, task_id=42)
        # A marker for a job that never appeared is counted, not silently kept.
        orphan = self._record(KIND_SUB, 2_600, tid=30, task_id=99,
                              step=step_hash("sub_scan"))
        jobs, counters = group_jobs([end, orphan] + markers + [start])
        self.assertEqual(counters["orphan_sub_markers"], 1)
        self.assertEqual(len(jobs[42].get("subs", [])), 2)
        stages = job_stages(jobs[42])
        # Marker names the stage starting at its timestamp; the lead before
        # the first marker is its own segment; the last stage runs to job_end.
        self.assertEqual(stages[0], {"tag": "lead", "from_start_ms": 0.0,
                                     "dur_ms": 0.0015})
        self.assertEqual(stages[1]["tag"], "sub_dir_open")
        self.assertEqual(stages[1]["dur_ms"], 0.0005)
        self.assertEqual(stages[2]["tag"], "sub_dir_sync")
        self.assertEqual(stages[2]["dur_ms"], 0.007)
        # Without markers (or without a complete closure) stages are null.
        self.assertIsNone(job_stages({"job_start": start, "job_end": end}))
        self.assertIsNone(job_stages({"job_start": start}))

    def test_job_calls_pair_delimited_calls_and_skip_unpaired(self):
        # Job A: a delimited call whose end marker is last — residue runs to
        # job_end. Job B: a start marker whose `?`-failure skipped its end
        # marker, plus a stray end with no pending start.
        start_a = self._record(KIND_JOB_START, 1_000, tid=30, task_id=42)
        end_a = self._record(KIND_JOB_END, 95_000, tid=30, task_id=42)
        markers_a = [self._record(KIND_SUB, 2_000, tid=30, task_id=42,
                                  step=step_hash("sub_dir_sync")),
                     self._record(KIND_SUB, 92_000, tid=30, task_id=42,
                                  step=step_hash("sub_dir_sync_end"))]
        start_b = self._record(KIND_JOB_START, 1_000, tid=31, task_id=43)
        end_b = self._record(KIND_JOB_END, 9_000, tid=31, task_id=43)
        markers_b = [self._record(KIND_SUB, 2_000, tid=31, task_id=43,
                                  step=step_hash("sub_dir_open")),
                     self._record(KIND_SUB, 5_000, tid=31, task_id=43,
                                  step=step_hash("sub_prep_open_end"))]
        jobs, _ = group_jobs([end_a, end_b] + markers_a + markers_b + [start_a, start_b])
        calls = job_calls(jobs[42])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["tag"], "sub_dir_sync")
        self.assertEqual(calls[0]["from_start_ms"], 0.001)
        self.assertEqual(calls[0]["dur_ms"], 0.09)
        self.assertEqual(calls[0]["post_call_ms"], 0.003)
        # Unpaired start or stray end: no row and no guessed duration.
        self.assertIsNone(job_calls(jobs[43]))
        # Captures without end markers (v2) report None, never a wrapper guess.
        self.assertIsNone(job_calls({"job_start": start_a, "job_end": end_a}))

    def test_pool_classification_uses_tagged_worker_and_count_evidence(self):
        records, job_id = [], 0
        for tid, count, step, op_value in [(10, 60, step_hash("mkdir"), 1),
                                           (20, 60, 0, 1),
                                           (50, 3, step_hash("ancestor_fsync"), 1)]:
            for _ in range(count):
                job_id += 1
                records += [self._record(KIND_SUBMIT, job_id * 10, tid=tid,
                                         task_id=job_id, step=step, a=op_value),
                            self._record(KIND_JOB_START, job_id * 10 + 1, tid=tid,
                                         task_id=job_id),
                            self._record(KIND_JOB_END, job_id * 10 + 2, tid=tid,
                                         task_id=job_id)]
        # A tid carrying tags from both pools: conflicting evidence is
        # reported as ambiguous, never silently resolved to one pool.
        for step in (step_hash("mkdir"), step_hash("mkdir"),
                     step_hash("ancestor_fsync")):
            job_id += 1
            records += [self._record(KIND_SUBMIT, job_id * 10, tid=60,
                                     task_id=job_id, step=step, a=2),
                        self._record(KIND_JOB_START, job_id * 10 + 1, tid=60,
                                     task_id=job_id),
                        self._record(KIND_JOB_END, job_id * 10 + 2, tid=60,
                                     task_id=job_id)]
        # Worker run-loop: a single OP_NONE job with no job_end.
        job_id += 1
        records += [self._record(KIND_SUBMIT, 1_000_000, tid=30, task_id=job_id),
                    self._record(KIND_JOB_START, 1_000_001, tid=30, task_id=job_id)]
        # A thread with only a few untagged jobs: not enough evidence.
        for _ in range(3):
            job_id += 1
            records += [self._record(KIND_SUBMIT, job_id * 10, tid=40,
                                     task_id=job_id, a=2),
                        self._record(KIND_JOB_START, job_id * 10 + 1, tid=40,
                                     task_id=job_id),
                        self._record(KIND_JOB_END, job_id * 10 + 2, tid=40,
                                     task_id=job_id)]
        jobs, _ = group_jobs(records)
        pools = classify_pool_tids(jobs)
        # tid 50 proves the primary rule: few jobs, but fsync-pool tags only.
        self.assertEqual(pools, {10: "main", 20: "fsync", 30: "worker_loop",
                                 40: "unknown", 50: "fsync", 60: "ambiguous"})
        # Operation rows attach the pool and tag names to each job.
        rows = operation_job_rows(jobs, 0, pools)
        self.assertEqual(rows[0]["pool"], "main")
        self.assertEqual(rows[0]["step_tag"], "mkdir")
        fsync_row = next(row for row in rows if row["pool"] == "fsync")
        self.assertEqual(fsync_row["step_tag"], "untagged")
        loop_row = next(row for row in rows if row["pool"] == "worker_loop")
        self.assertIn("job_end", loop_row["missing"])
        self.assertIsNone(loop_row["wall_ms"])

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
            # step=results_seen mirrors the quorum encoding: it must not be
            # mistaken for a tag hash in run-level step_hashes.
            self._record(14, attempt + 55_000_000, tid=10, a=op, step=4),
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
        self.assertEqual(wait["results_seen"], 4)
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


class FsTraceTests(unittest.TestCase):
    """ftrace parsing, scheduler-state semantics, wrapper decomposition."""

    TRACE_SAMPLE = """# tracer: nop
#
# entries-in-buffer/entries-written: 9/9   #P:12
           bash-100 [000] d..2. 1000.000001: sched_switch: prev_comm=bash prev_pid=100 prev_prio=120 prev_state=S ==> next_comm=rustfs-fsync next_pid=200 next_prio=120
 notify-rs inoti-101 [001] d..2. 1000.000002: sched_switch: prev_comm=notify-rs inoti prev_pid=101 prev_prio=120 prev_state=R ==> next_comm=other next_pid=300 next_prio=120
   rustfs-fsync-200 [002] d..2. 1000.000010: sys_fsync(fd: 0x5)
   rustfs-fsync-200 [002] d..2. 1000.000020: sched_switch: prev_comm=rustfs-fsync prev_pid=200 prev_prio=120 prev_state=D ==> next_comm=swapper/2 next_pid=0 next_prio=120
   kworker/u8-50 [003] d..2. 1000.000030: sched_waking: comm=rustfs-fsync pid=200 prio=120 target_cpu=002
   swapper/2-0 [004] d..2. 1000.000040: sched_wakeup: comm=rustfs-fsync pid=200 prio=120 target_cpu=002
   rustfs-fsync-200 [002] d..2. 1000.000050: sys_fsync -> 0x0
   rustfs-fsync-200 [002] d..2. 1000.000060: sched_switch: prev_comm=rustfs-fsync prev_pid=200 prev_prio=120 prev_state=R+ ==> next_comm=other next_pid=300 next_prio=120
   other-300 [005] d..2. 1000.000070: sched_switch: prev_comm=other prev_pid=300 prev_prio=120 prev_state=S ==> next_comm=rustfs-fsync next_pid=200 next_prio=120
 garbage-line
"""

    @staticmethod
    def _trace_file(directory, text):
        path = Path(directory) / "trace.raw"
        path.write_text(text)
        return path

    def test_to_ns_pads_microsecond_fraction(self):
        # ftrace writes 6 fractional digits on this host: 1000.000001 s is
        # 1000 s + 1 us, not 1000 s + 1 ns.
        self.assertEqual(to_ns("1000", "000001"), 1000 * 10**9 + 1000)
        self.assertEqual(to_ns("1000", "000001000"), 1000 * 10**9 + 1000)

    def test_parse_trace_attributes_events_from_content_not_prefix(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(
            self._trace_file(tmp, self.TRACE_SAMPLE))
        # The line-prefix task is prev (sched_switch) or waker (waking):
        # both sides must be attributed from the event content, and the
        # two wake events keep their distinct kinds (waking = wakeup
        # processing start, wakeup = made runnable).
        self.assertEqual(
            timelines[200],
            [("in", 1000 * 10**9 + 1000, None),
             ("out", 1000 * 10**9 + 20000, "D"),
             ("waking", 1000 * 10**9 + 30000, None),
             ("wakeup", 1000 * 10**9 + 40000, None),
             ("out", 1000 * 10**9 + 60000, "R+"),
             ("in", 1000 * 10**9 + 70000, None)])
        self.assertEqual(timelines[100], [("out", 1000 * 10**9 + 1000, "S")])
        # Foreign comm with spaces still parses; next_pid=300 gets the "in".
        self.assertEqual(timelines[300][0],
                         ("in", 1000 * 10**9 + 2000, None))
        self.assertEqual(
            syscalls[200],
            [(1000 * 10**9 + 10000, "fsync", "enter"),
             (1000 * 10**9 + 50000, "fsync", "exit")])
        self.assertEqual(stats["bad_lines"], 1)
        self.assertEqual(stats["frac_digits"], [6])
        self.assertEqual(stats["header_entries"], (9, 9))
        self.assertEqual(stats["first_ts"], 1000 * 10**9 + 1000)
        self.assertEqual(stats["last_ts"], 1000 * 10**9 + 70000)
        self.assertEqual(stats["rustfs_comms"], ["rustfs-fsync"])
        self.assertEqual(stats["event_counts"]["sched_switch"], 5)
        self.assertEqual(stats["event_counts"]["sys_fsync_enter"], 1)

    def test_state_segments_preempt_r_plus_is_runnable(self):
        # prev_state=R+ (preempted while TASK_RUNNING) must read as
        # runnable-but-not-scheduled, never as a blocked state.
        rows = state_segments([("in", 100, None), ("out", 200, "R+"),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("running", 100, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_block_ends_at_wakeup(self):
        rows = state_segments([("out", 100, "D"), ("wakeup", 200, None),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_waking_to_wakeup_is_its_own_category(self):
        # sched_waking starts wakeup processing; sched_wakeup makes the
        # task runnable.  The interval between them is neither runnable
        # nor blocked/D time.
        rows = state_segments([("out", 100, "D"), ("waking", 150, None),
                               ("wakeup", 200, None), ("in", 300, None)],
                              0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("wakeup_transition", 150, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_wakeup_without_observed_waking(self):
        # The made-runnable boundary is sched_wakeup even when the
        # initiation event was lost: the block ends there, no unknown
        # span is invented (the lost start cannot be located).
        rows = state_segments([("out", 100, "D"), ("wakeup", 200, None),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_waking_without_wakeup_is_incomplete(self):
        # sched_waking seen but no sched_wakeup before the switch-in:
        # the made-runnable instant is missing -> unknown, not runnable.
        rows = state_segments([("out", 100, "D"), ("waking", 150, None),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("unknown_wake_incomplete", 150, 300),
                                ("running", 300, 400)])
        # Same at the window edge: the transition never completed.
        rows = state_segments([("out", 100, "D"), ("waking", 150, None)],
                              0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("unknown_wake_incomplete", 150, 400)])

    def test_state_segments_duplicate_and_spurious_wake_events(self):
        # Duplicate waking/wakeup pairs collapse to one transition; wake
        # events for an already scheduled or already runnable task are
        # spurious and must not move the state.
        rows = state_segments([("out", 100, "D"), ("waking", 150, None),
                               ("waking", 160, None), ("wakeup", 200, None),
                               ("wakeup", 210, None), ("in", 300, None)],
                              0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("wakeup_transition", 150, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])
        # Waking while still running (spurious preemption kick): ignored.
        rows = state_segments([("in", 100, None), ("waking", 150, None),
                               ("out", 200, "R+"), ("in", 300, None)],
                              0, 400)
        self.assertEqual(rows, [("running", 100, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])
        # Wakeup while already runnable (duplicate): ignored.
        rows = state_segments([("out", 100, "D"), ("wakeup", 150, None),
                               ("wakeup", 160, None), ("in", 200, None)],
                              0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("runnable", 150, 200),
                                ("running", 200, 400)])

    def test_state_segments_switch_out_during_transition_is_unknown(self):
        # A switch-out requires the task to have run: reaching one from a
        # wakeup transition means an entire in+wakeup sequence is missing.
        rows = state_segments([("out", 100, "D"), ("waking", 150, None),
                               ("out", 200, "R+"), ("in", 300, None)],
                              0, 400)
        self.assertEqual(rows, [("blocked:D", 100, 150),
                                ("unknown_lost_in", 150, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_runnable_wake_from_window_start(self):
        # Window opens mid-block (no switch-out observed): the observed
        # waking/wakeup pair still pins the runnable start exactly.
        rows = state_segments([("waking", 150, None), ("wakeup", 200, None),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("wakeup_transition", 150, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_plain_r_switch_out_is_runnable(self):
        # prev_state=R (runnable switch-out without preemption) is also
        # runnable-but-not-scheduled, like R+.
        rows = state_segments([("in", 100, None), ("out", 200, "R"),
                               ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("running", 100, 200),
                                ("runnable", 200, 300),
                                ("running", 300, 400)])

    def test_state_segments_reports_unknown_edges(self):
        # A switch-in with no observed wake must not be reported as blocked.
        rows = state_segments([("out", 100, "D"), ("in", 300, None)], 0, 400)
        self.assertEqual(rows, [("unknown_no_wake", 100, 300),
                                ("running", 300, 400)])
        # A switch-out with no observed switch-in marks the prior chunk.
        rows = state_segments([("out", 100, "D"), ("out", 200, "D"),
                               ("in", 250, None)], 0, 300)
        self.assertEqual(rows, [("unknown_lost_in", 100, 200),
                                ("unknown_no_wake", 200, 250),
                                ("running", 250, 300)])
        # Window edge before the first event clips the running segment.
        rows = state_segments([("in", 100, None), ("out", 200, "R+"),
                               ("in", 300, None)], 150, 400)
        self.assertEqual(rows[0], ("running", 150, 200))

    def test_decompose_wrapper_tiles_window_exactly(self):
        timeline = [("in", 10, None), ("out", 50, "D"),
                    ("wakeup", 70, None), ("in", 90, None)]
        events = [(20, "fsync", "enter"), (80, "fsync", "exit")]
        rows, windows, unknown, recon = decompose_wrapper(timeline, events,
                                                          0, 100)
        self.assertEqual(recon, 0)
        self.assertEqual(unknown, 10)  # leading span before the first event
        self.assertEqual(windows, [(20, 80, "fsync", False)])
        self.assertEqual(rows, [
            ("pre-entry", "unknown", 0, 10),
            ("pre-entry", "running", 10, 20),
            ("syscall", "running", 20, 50),
            ("syscall", "blocked:D", 50, 70),
            ("syscall", "runnable", 70, 80),
            ("post-exit", "runnable", 80, 90),
            ("post-exit", "running", 90, 100),
        ])

    def test_zone_without_syscall_windows(self):
        self.assertEqual(zone_of(0, 10, [], 0, 100), "no-traced-syscall")

    def test_syscall_windows_open_window_and_pairing(self):
        events = [(10, "fsync", "enter"), (50, "fsync", "exit"),
                  (90, "fdatasync", "enter")]
        windows = syscall_windows_tid(events, 0, 100)
        self.assertEqual(windows, [(10, 50, "fsync", False),
                                   (90, None, "fdatasync", True)])
        # Events outside the wrapper (+/- 2 us slack) are not borrowed; the
        # test window sits farther than the slack from every event.
        self.assertEqual(syscall_windows_tid(events, 3000, 4000), [])

    def test_derive_syscall_mapping_votes_from_capture(self):
        rows = [
            ("sub_dir_sync", 1, 0, 10, [(5, 8, "fsync", False)]),
            ("sub_dir_sync", 2, 0, 10, []),           # empty: no vote
            ("sub_fsync_files", 3, 0, 10, [(5, 8, "fdatasync", False)]),
            ("sub_fdatasync", 4, 0, 10, [(5, 8, "fdatasync", False)]),
            ("sub_rename", 5, 0, 10, []),              # not a sync candidate
        ]
        mapping, votes = derive_syscall_mapping(rows)
        self.assertEqual(mapping, {"sub_dir_sync": "fsync",
                                   "sub_fsync_files": "fdatasync",
                                   "sub_fdatasync": "fdatasync"})
        self.assertEqual(votes["sub_dir_sync"]["empty_windows"], 1)
        self.assertNotIn("sub_rename", votes)

    @staticmethod
    def _long(tag, tid, dur_ns, enter_ns, exit_ns, w0_ns, run="run-1"):
        return {"run": run, "tag": tag, "tid": tid, "dur_ms": dur_ns / 1e6,
                "enter_ns": enter_ns, "exit_ns": exit_ns,
                "w0_ns": w0_ns, "w1_ns": w0_ns + dur_ns}

    def test_group_clusters_split_by_enter_gap_and_exit_groups(self):
        # Two members entering within 50 ms of the first exit form one
        # cluster; a wrapper entering a second later forms a new cluster
        # (single-member clusters are dropped).  Exits more than 5 ms apart
        # split into release groups.
        wrappers = [
            self._long("sub_dir_sync", 1, 100_000_000, 1_000, 2_000_000, 990),
            self._long("sub_dir_sync", 2, 20_000_000, 40_000_000,
                       60_000_000, 39_999_000),
            self._long("sub_scan", 3, 5_000_000, 1_500_000_000,
                       1_505_000_000, 1_499_999_000),
        ]
        clusters = group_clusters(wrappers)
        self.assertEqual(len(clusters), 1)
        c = clusters[0]
        self.assertEqual(c["n_members"], 2)
        self.assertAlmostEqual(c["enter_spread_ms"], 39.999, places=3)
        self.assertAlmostEqual(c["exit_spread_ms"], 58.0, places=3)
        self.assertEqual(c["exit_groups"],
                         [{"n": 1, "spread_ms": 0.0},
                          {"n": 1, "spread_ms": 0.0}])

    def test_group_clusters_tight_exit_release(self):
        wrappers = [
            self._long("sub_dir_sync", 1, 100_000_000, 1_000, 101_000, 990),
            self._long("sub_dir_sync", 2, 102_000_000, 2_000, 103_000, 1_990),
        ]
        c = group_clusters(wrappers)[0]
        self.assertEqual(c["n_members"], 2)
        self.assertAlmostEqual(c["enter_spread_ms"], 0.001, places=6)
        self.assertAlmostEqual(c["exit_spread_ms"], 0.002, places=6)
        self.assertEqual(c["exit_groups"], [{"n": 2, "spread_ms": 0.002}])

    def test_parse_stats_and_settings_files(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        stats = tmp / "trace.stats"
        stats.write_text(
            " SMP: FTrace Dump triggered at wall time\n"
            "== /sys/kernel/tracing/per_cpu/cpu0/trace:\n"
            "entries: 10\noverrun: 0\ncommit overrun: 0\nbytes: 600\n"
            "oldest event ts: 100.000000\nnow ts: 200.000000\n"
            "dropped events: 0\nread events: 0\n"
            "== /sys/kernel/tracing/per_cpu/cpu1/trace:\n"
            "entries: 5\noverrun: 2\ncommit overrun: 0\nbytes: 300\n"
            "oldest event ts: 101.000000\nnow ts: 201.000000\n"
            "dropped events: 1\nread events: 0\n")
        parsed = parse_stats_file(stats)
        self.assertEqual(parsed["cpus"], 2)
        self.assertEqual(parsed["entries_total"], 15)
        self.assertEqual(parsed["overrun_total"], 2)
        self.assertEqual(parsed["dropped_total"], 1)
        self.assertEqual(parsed["bytes_total"], 900)

        settings = tmp / "trace.settings"
        settings.write_text(
            "trace_clock=mono\nbuffer_size_kb=16387\noverwrite=0\n"
            "sched/sched_switch enable=1 filter=prev_comm ~ \"rustfs*\" || "
            "next_comm ~ \"rustfs*\"\n"
            "syscalls/sys_enter_fsync enable=1 filter=none\n")
        parsed = parse_settings_file(settings)
        self.assertEqual(parsed["trace_clock"], "mono")
        self.assertEqual(parsed["overwrite"], "0")
        self.assertEqual(len(parsed["events"]), 2)

    # --- non-overlapping accounting (nested/overlapping wrappers) ---

    def test_union_nested_child_inside_parent_counts_shared_time_once(self):
        # A 10 ms child inside a 30 ms parent is one 30 ms region, not 40 ms.
        ms = 1_000_000
        wrappers = [self._long("sub_scan", 1, 30 * ms, None, None, 0),
                    self._long("sub_fdatasync", 1, 10 * ms, None, None,
                               10 * ms)]
        regions = union_regions(wrappers)
        self.assertEqual(regions, {("run-1", 1): [(0, 30 * ms)]})
        summary = union_summary({}, regions)
        self.assertEqual(summary["thread_time_ms"], 30.0)
        self.assertEqual(summary["regions"], 1)
        # Per-tag totals keep both observations and are marked overlapping.
        tags = per_tag_totals(wrappers)
        self.assertTrue(tags["sub_scan"]["overlapping_across_tags"])
        self.assertEqual(tags["sub_scan"]["duration_sum_ms"], 30.0)
        self.assertEqual(tags["sub_fdatasync"]["duration_sum_ms"], 10.0)
        self.assertEqual(
            sum(t["duration_sum_ms"] for t in tags.values()), 40.0)

    def test_union_partial_overlap_merges_shared_time(self):
        # [0,20] and [15,40] share [15,20]: union is 40 ms, not 45 ms.
        ms = 1_000_000
        wrappers = [self._long("sub_dir_sync", 1, 20 * ms, None, None, 0),
                    self._long("sub_scan", 1, 25 * ms, None, None, 15 * ms)]
        regions = union_regions(wrappers)
        self.assertEqual(regions, {("run-1", 1): [(0, 40 * ms)]})
        self.assertEqual(union_summary({}, regions)["thread_time_ms"], 40.0)

    def test_union_disjoint_intervals_on_one_tid_stay_separate(self):
        ms = 1_000_000
        wrappers = [self._long("sub_dir_sync", 1, 10 * ms, None, None, 0),
                    self._long("sub_dir_sync", 1, 10 * ms, None, None,
                               20 * ms)]
        regions = union_regions(wrappers)
        self.assertEqual(regions, {("run-1", 1): [(0, 10 * ms),
                                                  (20 * ms, 30 * ms)]})
        summary = union_summary({}, regions)
        self.assertEqual(summary["thread_time_ms"], 20.0)
        self.assertEqual(summary["regions"], 2)

    def test_union_never_merges_identical_windows_across_threads(self):
        # Simultaneous waits on two threads are separate thread-time.
        ms = 1_000_000
        wrappers = [self._long("sub_dir_sync", 1, 100 * ms, None, None, 0),
                    self._long("sub_dir_sync", 2, 100 * ms, None, None, 0)]
        regions = union_regions(wrappers)
        self.assertEqual(regions, {("run-1", 1): [(0, 100 * ms)],
                                   ("run-1", 2): [(0, 100 * ms)]})
        self.assertEqual(union_summary({}, regions)["thread_time_ms"], 200.0)

    def test_union_never_merges_across_repetitions(self):
        # The same TID with the same window in two reps stays two regions.
        wrappers = [self._long("sub_dir_sync", 5, 100, None, None, 0,
                               run="run-1"),
                    self._long("sub_dir_sync", 5, 100, None, None, 50,
                               run="run-2")]
        regions = union_regions(wrappers)
        self.assertEqual(regions, {("run-1", 5): [(0, 100)],
                                   ("run-2", 5): [(50, 150)]})
        # Overlapping timestamp ranges in different reps are never fused.
        self.assertEqual(union_summary({}, regions)["regions"], 2)

    def test_union_state_totals_report_unknown_portions(self):
        regions = {("run-1", 7): [(0, 100)]}
        # No timeline at all: the whole union region is unknown, not zero.
        self.assertEqual(union_state_totals({}, regions), {"unknown": 100})
        # A timeline starting mid-region: the leading hole stays unknown
        # and the observed states are derived from the timeline itself.
        timeline = [("out", 40, "D"), ("wakeup", 60, None), ("in", 70, None)]
        totals = union_state_totals({7: timeline}, regions)
        self.assertEqual(totals, {"unknown": 40, "blocked:D": 20,
                                  "runnable": 10, "running": 30})
        self.assertEqual(sum(totals.values()), 100)

    def test_union_mixed_unknown_and_known_states_across_regions(self):
        wrappers = [self._long("sub_dir_sync", 7, 50, None, None, 0),
                    self._long("sub_dir_sync", 8, 50, None, None, 0)]
        regions = union_regions(wrappers)
        timeline = {7: [("in", 10, None), ("out", 40, "D")]}  # tid 8 unseen
        totals = union_state_totals(timeline, regions)
        self.assertEqual(totals, {"unknown": 60, "running": 30,
                                  "blocked:D": 10})
        self.assertEqual(sum(totals.values()), 100)

    # --- clock validation ---

    REAL_CLOCK_LINE = ("local global counter uptime perf [mono] mono_raw "
                       "boot tai x86-tsc")

    @staticmethod
    def _alignment(total=27700, matched=27700, min_offset=-0.301,
                   per_run=None):
        return {
            "paired_sync_calls": total,
            "matched_with_syscall_enter": matched,
            "match_rate": (matched / total if total else None),
            "enter_offset_us": {"min": min_offset if matched else None,
                                "p50": 0.58, "max": 1588.559},
            "per_run": per_run if per_run is not None else {
                "run-1": {"paired_sync_calls": total // 2,
                          "matched_with_syscall_enter": matched // 2,
                          "match_rate": (matched / total if total else None),
                          "enter_offset_us": {"min": min_offset,
                                              "max": 100.0}},
                "run-2": {"paired_sync_calls": total - total // 2,
                          "matched_with_syscall_enter": matched - matched // 2,
                          "match_rate": (matched / total if total else None),
                          "enter_offset_us": {"min": min_offset,
                                              "max": 100.0}},
            },
        }

    def test_clock_valid_mono_and_clock_monotonic_validated(self):
        validation = validate_clock(self.REAL_CLOCK_LINE, {1}, [6],
                                    self._alignment())
        self.assertEqual(validation["trace_clock_selected"], "mono")
        self.assertEqual(validation["probe_clock_ids"], [1])
        self.assertEqual(validation["probe_clock"], "CLOCK_MONOTONIC")
        self.assertEqual(validation["timestamp_resolution_us"], 1.0)
        self.assertEqual(validation["compatibility"]["status"], "validated")
        self.assertEqual(validation["alignment"]["status"], "validated")
        self.assertEqual(validation["direct_subtraction"]["status"],
                         "validated")
        self.assertEqual(validation["alignment"]["tolerance_us"], 1.0)
        self.assertIn("quantization", validation["alignment"]["tolerance_basis"])

    def test_clock_different_selected_trace_clock_fails(self):
        validation = validate_clock(
            "local [global] counter uptime perf mono", {1}, [6],
            self._alignment())
        self.assertEqual(validation["trace_clock_selected"], "global")
        self.assertEqual(validation["compatibility"]["status"], "failed")
        self.assertEqual(validation["direct_subtraction"]["status"], "failed")
        # The alignment evidence may be fine on its own; the domains differ.
        self.assertEqual(validation["alignment"]["status"], "validated")

    def test_clock_different_probe_clock_id_fails(self):
        # The probe build stamps its clock into the dump header; a capture
        # whose header says another clock must not validate.
        validation = validate_clock(self.REAL_CLOCK_LINE, {7}, [6],
                                    self._alignment())
        self.assertEqual(validation["compatibility"]["status"], "failed")
        self.assertIsNone(validation["probe_clock"])
        self.assertEqual(validation["direct_subtraction"]["status"], "failed")

    def test_clock_missing_or_malformed_metadata_is_insufficient(self):
        evidence = self._alignment()
        cases = [
            # (trace_clock line, probe ids, digits,
            #  (compatibility, alignment, direct_subtraction))
            (None, {1}, [6],
             ("insufficient_evidence", "validated", "insufficient_evidence")),
            ("local global counter uptime", {1}, [6],
             ("insufficient_evidence", "validated", "insufficient_evidence")),
            ("[mono] mono_raw", None, [6],
             ("insufficient_evidence", "validated", "insufficient_evidence")),
            # Timestamp precision unknown: the tolerance cannot be justified
            # even though the clock metadata itself is fine.
            (self.REAL_CLOCK_LINE, {1}, [],
             ("validated", "insufficient_evidence", "insufficient_evidence")),
        ]
        for line, ids, digits, expected in cases:
            validation = validate_clock(line, ids, digits, evidence)
            statuses = (validation["compatibility"]["status"],
                        validation["alignment"]["status"],
                        validation["direct_subtraction"]["status"])
            self.assertEqual(statuses, expected, f"{line!r}/{digits}: {statuses}")
            self.assertNotEqual(validation["direct_subtraction"]["status"],
                                "validated")
            self.assertTrue(validation["direct_subtraction"]["reason"])

    def test_clock_no_matched_syscall_entries_is_not_success(self):
        # Expected wrappers but zero matches: insufficient evidence.
        validation = validate_clock(self.REAL_CLOCK_LINE, {1}, [6],
                                    self._alignment(total=100, matched=0,
                                                    per_run={}))
        self.assertEqual(validation["alignment"]["status"],
                         "insufficient_evidence")
        self.assertEqual(validation["direct_subtraction"]["status"],
                         "insufficient_evidence")
        # No samples at all: also insufficient, never validated.
        validation = validate_clock(self.REAL_CLOCK_LINE, {1}, [6],
                                    self._alignment(total=0, matched=0,
                                                    per_run={}))
        self.assertEqual(validation["alignment"]["status"],
                         "insufficient_evidence")
        # Enough samples but a failing match rate: failed.
        validation = validate_clock(
            self.REAL_CLOCK_LINE, {1}, [6],
            self._alignment(total=1000, matched=900,
                            per_run={"run-1": {"paired_sync_calls": 500,
                                               "matched_with_syscall_enter": 450,
                                               "match_rate": 0.9,
                                               "enter_offset_us": {"min": -0.2,
                                                                   "max": 10}}}))
        self.assertEqual(validation["alignment"]["status"], "failed")
        self.assertEqual(validation["direct_subtraction"]["status"], "failed")

    def test_clock_alignment_outside_documented_tolerance_fails(self):
        # An enter 5 us before its start marker is beyond the 1 us
        # quantization tolerance: alignment (and only alignment) fails.
        validation = validate_clock(self.REAL_CLOCK_LINE, {1}, [6],
                                    self._alignment(min_offset=-5.0))
        self.assertEqual(validation["compatibility"]["status"], "validated")
        self.assertEqual(validation["alignment"]["status"], "failed")
        self.assertIn("beyond the 1 us timestamp tolerance",
                      validation["alignment"]["reason"])
        self.assertEqual(validation["direct_subtraction"]["status"], "failed")

    def test_clock_current_capture_validates_from_actual_metadata(self):
        # The real capture must pass from its own settings file, dump
        # headers, and measured offsets — no test-specific bypass.
        root = Path(__file__).resolve().parents[2]
        run_dir = root / ".repro" / "rustfs-fstrace-main"
        results_path = (Path(__file__).resolve().parent / "results" /
                        "fs-trace-diagnostic.json")
        if not run_dir.is_dir() or not results_path.is_file():
            self.skipTest("raw capture or saved results not present")
        settings = parse_settings_file(run_dir / "trace.settings")
        clock_ids = set()
        for dump in sorted(run_dir.glob("run-*/fs-probe.bin")):
            header, _records = read_probe(dump)
            clock_ids.add(header["clock_id"])
        saved = json.loads(results_path.read_text())
        saved_alignment = saved["quality"]["alignment"]
        evidence = {key: saved_alignment[key] for key in
                    ("paired_sync_calls", "matched_with_syscall_enter",
                     "match_rate", "enter_offset_us")
                    if key in saved_alignment}
        if "per_run" in saved_alignment:
            evidence["per_run"] = saved_alignment["per_run"]
        validation = validate_clock(
            settings.get("trace_clock"), clock_ids,
            saved["quality"]["clock"]["timestamp_fraction_digits"], evidence)
        for question in ("compatibility", "alignment", "direct_subtraction"):
            self.assertEqual(validation[question]["status"], "validated",
                             f"{question}: {validation[question]['reason']}")

    # --- request/job identity through fs_trace ---

    @staticmethod
    def _record(kind, ts, *, tid=10, task_id=0, step=0, a=0):
        return {"kind": kind, "name": "", "step": step, "tid": tid,
                "id": task_id, "ts": ts, "a": a}

    T0 = 1_000 * 10**9
    MS = 1_000_000

    @classmethod
    def _build_fs_trace_fixture(cls, run_dir, probe_dump_version=2, diag=False):
        """A complete minimal capture dir: probe dumps carrying job and
        operation identity (nested wrappers, a commit wait with a send, a
        job without operation context) plus a trace whose sched/syscall
        events align with every sync wrapper."""
        T, MS, rec = cls.T0, cls.MS, cls._record
        op1, op2 = 0x1111222233334444, 0x5555666677778888
        records = []
        # Operation 1 / job 41 on executor tid 700: nested wrapper chain
        # (sub_scan > sub_fsync_files > sub_fdatasync), commit wait with a
        # send carrying a quorum snapshot.
        records.append(rec(KIND_OP_BEGIN, T - 5 * MS, tid=30, a=op1))
        records.append(rec(KIND_SUBMIT, T - 1 * MS, tid=30, task_id=41,
                           step=step_hash("src_dir_sync"), a=op1))
        records.append(rec(KIND_JOB_START, T, tid=700, task_id=41))
        for tag, at in (("sub_scan", 5), ("sub_fsync_files", 10),
                        ("sub_fdatasync", 15), ("sub_fdatasync_end", 75),
                        ("sub_fsync_files_end", 80), ("sub_scan_end", 85)):
            records.append(rec(KIND_SUB, T + at * MS, tid=700, task_id=41,
                               step=step_hash(tag)))
        records.append(rec(KIND_JOB_END, T + 90 * MS, tid=700, task_id=41))
        records.append(rec(KIND_WAIT_BEGIN, T + 20 * MS, tid=30, a=op1))
        records.append(rec(KIND_SEND_OK, T + 50 * MS, tid=30, a=op1,
                           step=4, task_id=3))  # results_seen / write_quorum
        records.append(rec(KIND_WAIT_END, T + 190 * MS, tid=30, a=op1))
        # Operation 2 / job 42 on the SAME executor tid: two long
        # sub_dir_sync wrappers plus 30 short ones (repeated tag, distinct
        # occurrences).
        records.append(rec(KIND_SUBMIT, T + 94 * MS, tid=31, task_id=42,
                           step=step_hash("dst_dir_fsync"), a=op2))
        records.append(rec(KIND_JOB_START, T + 95 * MS, tid=700, task_id=42))
        pairs = [(100, 160), (165, 230)] + [(240 + 4 * i, 241 + 4 * i)
                                            for i in range(30)]
        for begin, end in pairs:
            records.append(rec(KIND_SUB, T + begin * MS, tid=700, task_id=42,
                               step=step_hash("sub_dir_sync")))
            records.append(rec(KIND_SUB, T + end * MS, tid=700, task_id=42,
                               step=step_hash("sub_dir_sync_end")))
        records.append(rec(KIND_JOB_END, T + 400 * MS, tid=700, task_id=42))
        # Job 43 on tid 701 with NO operation context (OP_NONE).
        records.append(rec(KIND_SUBMIT, T + 295 * MS, tid=32, task_id=43,
                           step=step_hash("rename")))
        records.append(rec(KIND_JOB_START, T + 305 * MS, tid=701, task_id=43))
        records.append(rec(KIND_SUB, T + 310 * MS, tid=701, task_id=43,
                           step=step_hash("sub_rename")))
        records.append(rec(KIND_SUB, T + 370 * MS, tid=701, task_id=43,
                           step=step_hash("sub_rename_end")))
        records.append(rec(KIND_JOB_END, T + 380 * MS, tid=701, task_id=43))
        records.append(rec(KIND_OP_END, T + 410 * MS, tid=30, a=op1))
        (run_dir / "run-1").mkdir(parents=True)
        (run_dir / "run-1" / "fs-probe.bin").write_bytes(
            _probe_dump(records, capacity=1024, version=probe_dump_version))

        events = []
        def sw(ns, prev, ptid, state, nxt, ntid):
            events.append((ns, prev, ptid,
                           f"sched_switch: prev_comm={prev} prev_pid={ptid} "
                           f"prev_prio=120 prev_state={state} ==> "
                           f"next_comm={nxt} next_pid={ntid} next_prio=120"))
        sw(T - 10 * MS, "other", 99, "S", "rustfs-fsync", 700)
        events.append((T + 16 * MS, "rustfs-fsync", 700,
                       "sys_fdatasync(fd: 0x7)"))
        sw(T + 30 * MS, "rustfs-fsync", 700, "D", "swapper/0", 0)
        # sched_waking starts wakeup processing while the task is still
        # sleeping; sched_wakeup makes it runnable; only then the
        # switch-in.  (Timestamps in ns: 49.5 / 49.75 / 50 ms.)
        events.append((T + 50 * MS - 500_000, "kworker", 50,
                       "sched_waking: comm=rustfs-fsync pid=700 prio=120 "
                       "target_cpu=000"))
        events.append((T + 50 * MS - 250_000, "swapper/0", 0,
                       "sched_wakeup: comm=rustfs-fsync pid=700 prio=120 "
                       "target_cpu=000"))
        sw(T + 50 * MS, "swapper/0", 0, "S", "rustfs-fsync", 700)
        events.append((T + 74 * MS, "rustfs-fsync", 700,
                       "sys_fdatasync -> 0x0"))
        sw(T + 96 * MS, "rustfs-fsync", 700, "R+", "other", 99)
        sw(T + 98 * MS, "other", 99, "S", "rustfs-fsync", 700)
        events.append((T + 101 * MS, "rustfs-fsync", 700,
                       "sys_fsync(fd: 0x3)"))
        sw(T + 110 * MS, "rustfs-fsync", 700, "D", "swapper/0", 0)
        events.append((T + 130 * MS - 500_000, "kworker", 50,
                       "sched_waking: comm=rustfs-fsync pid=700 prio=120 "
                       "target_cpu=000"))
        events.append((T + 130 * MS - 250_000, "swapper/0", 0,
                       "sched_wakeup: comm=rustfs-fsync pid=700 prio=120 "
                       "target_cpu=000"))
        sw(T + 130 * MS, "swapper/0", 0, "S", "rustfs-fsync", 700)
        events.append((T + 159 * MS, "rustfs-fsync", 700, "sys_fsync -> 0x0"))
        events.append((T + 166 * MS, "rustfs-fsync", 700,
                       "sys_fsync(fd: 0x3)"))
        events.append((T + 229 * MS, "rustfs-fsync", 700, "sys_fsync -> 0x0"))
        for begin, _end in pairs[2:]:  # short wrappers still need their enters
            events.append((T + begin * MS + 100_000, "rustfs-fsync", 700,
                           "sys_fsync(fd: 0x3)"))
            events.append((T + begin * MS + 500_000, "rustfs-fsync", 700,
                           "sys_fsync -> 0x0"))
        sw(T + 306 * MS, "other", 98, "S", "rustfs-worker", 701)
        sw(T + 330 * MS, "rustfs-worker", 701, "D", "swapper/1", 1)
        events.append((T + 365 * MS, "kworker", 51,
                       "sched_waking: comm=rustfs-worker pid=701 prio=120 "
                       "target_cpu=001"))
        events.append((T + 365 * MS + 500_000, "swapper/1", 1,
                       "sched_wakeup: comm=rustfs-worker pid=701 prio=120 "
                       "target_cpu=001"))
        sw(T + 366 * MS, "swapper/1", 1, "S", "rustfs-worker", 701)
        sw(T + 450 * MS, "other", 97, "S", "swapper/0", 0)
        if diag:
            # Diagnostic fs/block/writeback events (the predeclared set),
            # placed to exercise every kernel_wait evidence class:
            # a same-task writeback wait inside the first blocked:D
            # segment, a completion 100 us before that segment's wake
            # edge, a block issue and a transaction commit later inside
            # the same long wrapper window, and one event outside every
            # long wrapper (negative attribution preserved).
            events.append((T + 35 * MS, "rustfs-fsync", 700,
                           "folio_wait_writeback: bdi nvme0n1p3: "
                           "ino=12345 index=7"))
            events.append((T + 49_400_000, "kworker", 50,
                           "block_rq_complete: 259,0 RM () 6175136 + 32 "
                           "be,0,4 [0]"))
            events.append((T + 60 * MS, "kworker/6:1H", 135,
                           "block_rq_issue: 259,0 RM 16384 () 6175136 + 32 "
                           "be,0,4 [kworker/6:1H]"))
            events.append((T + 78 * MS, "kworker", 51,
                           "btrfs_transaction_commit: "
                           "6320451e-6e11-4cbb-83ce-db232864bb96: "
                           "root=5(FS_TREE) gen=4242"))
            events.append((T + 290 * MS, "kworker", 52,
                           "btrfs_finish_ordered_extent: "
                           "6320451e-6e11-4cbb-83ce-db232864bb96: "
                           "root=5(FS_TREE) ino=999 start=0 len=4096 "
                           "uptodate=1"))
        events.sort(key=lambda event: event[0])  # stable within a timestamp
        lines = ["# tracer: nop",
                 f"# entries-in-buffer/entries-written: "
                 f"{len(events)}/{len(events)}   #P:8"]
        for ns, comm, tid, rest in events:
            sec, rem = divmod(ns, 10**9)
            lines.append(f"    {comm}-{tid} [000] d..2. "
                         f"{sec}.{rem // 1000:06d}: {rest}")
        (run_dir / "trace.raw").write_text("\n".join(lines) + "\n")
        settings_lines = [
            "trace_clock=local global counter uptime perf [mono] mono_raw "
            "boot tai x86-tsc\n",
            "buffer_size_kb=16387\noverwrite=0\n",
            'sched/sched_switch enable=1 filter=prev_comm ~ "rustfs*" || '
            'next_comm ~ "rustfs*"\n',
            "syscalls/sys_enter_fsync enable=1 filter=none\n",
        ]
        if diag:
            settings_lines += [
                "block/block_bio_queue enable=1 filter=dev == 271581184\n",
                "block/block_rq_issue enable=1 filter=dev == 271581184\n",
                "block/block_rq_complete enable=1 filter=dev == 271581184\n",
                "btrfs/btrfs_transaction_commit enable=1 filter=none\n",
            ]
        (run_dir / "trace.settings").write_text("".join(settings_lines))
        (run_dir / "trace.stats").write_text(
            "== /sys/kernel/tracing/per_cpu/cpu0/trace:\n"
            f"entries: {len(events)}\noverrun: 0\ncommit overrun: 0\n"
            "bytes: 4096\noldest event ts: 1000.000000\n"
            "now ts: 1000.450000\ndropped events: 0\nread events: 0\n")
        (run_dir / "trace.error_log").write_text("")
        (run_dir / "manifest.json").write_text(
            json.dumps({"repetitions": 1}))

    def _analyze_fixture(self, timelines=10):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self._build_fs_trace_fixture(tmp)
        return fs_trace_analyze(tmp, timelines=timelines), tmp

    def _analyze_diag_fixture(self, timelines=10):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self._build_fs_trace_fixture(tmp, diag=True)
        return fs_trace_analyze(tmp, timelines=timelines), tmp

    def test_fixture_without_diagnostic_events_has_no_kernel_wait(self):
        # A capture without the diagnostic event set must reproduce the
        # pre-extension output shape exactly: no kernel_wait section and
        # the original "no block/journal events captured" limitation.
        result, _ = self._analyze_fixture(timelines=0)
        self.assertNotIn("kernel_wait", result)
        self.assertTrue(any("no block/journal events captured" in s
                            for s in result["limitations"]))

    def test_kernel_wait_evidence_classes_from_diagnostic_events(self):
        result, tmp = self._analyze_diag_fixture(timelines=1)
        kw = result["kernel_wait"]
        self.assertEqual(kw["status"], "analyzed")
        self.assertEqual(kw["event_totals"], {
            "folio_wait_writeback": 1, "block_rq_complete": 1,
            "block_rq_issue": 1, "btrfs_transaction_commit": 1,
            "btrfs_finish_ordered_extent": 1,
        })
        # The btrfs commit outside every long wrapper stays unattributed
        # rather than being pulled into some wrapper's evidence.
        self.assertEqual(kw["outside_long_wrappers"], 1)
        self.assertEqual(len(kw["wrappers"]), 3)
        # Block filters were collected from trace.settings, not assumed.
        self.assertEqual(
            kw["capture_filter"]["block_event_filter_lines"],
            ["block/block_bio_queue enable=1 filter=dev == 271581184",
             "block/block_rq_issue enable=1 filter=dev == 271581184",
             "block/block_rq_complete enable=1 filter=dev == 271581184"])
        by_tag = {w["tag"]: w for w in kw["wrappers"]}
        scan = by_tag["sub_scan"]
        self.assertEqual(scan["events_in_window"], 4)
        self.assertEqual(scan["event_counts"], {
            "folio_wait_writeback": 1, "block_rq_complete": 1,
            "block_rq_issue": 1, "btrfs_transaction_commit": 1,
        })
        levels = {e["level"] for e in scan["evidence"]}
        self.assertEqual(levels, {"demonstrable_dependency", "supported_causal"})
        dep = next(e for e in scan["evidence"]
                   if e["level"] == "demonstrable_dependency")
        self.assertEqual(dep["event"], "folio_wait_writeback")
        self.assertEqual(dep["identity"]["bdi"], "nvme0n1p3")
        self.assertEqual(dep["identity"]["ino"], "12345")
        caus = next(e for e in scan["evidence"]
                    if e["level"] == "supported_causal")
        self.assertEqual(caus["event"], "block_rq_complete")
        self.assertEqual(caus["distance_us"], 100.0)
        self.assertEqual(caus["segment_dur_ms"], 19.5)
        # The wake edge carries the observed waker comm (fixture prefix).
        self.assertEqual(caus["waker_comm"], "kworker")
        # Phase structure: one blocked segment, isolated completion.
        phase = scan["phase_summary"]
        self.assertEqual(phase["blocked_segments"], 1)
        self.assertEqual(phase["blocked_total_ms"], 19.5)
        top = phase["top_segments"][0]
        self.assertEqual(top["start_offset_ms"], 25.0)
        self.assertEqual(top["rq_completions_during"], 1)
        self.assertEqual(top["waker_comm"], "kworker")
        self.assertEqual(phase["wakes_by_comm"], {"kworker": 1})
        prox = scan["proximity_summary"]
        self.assertEqual(prox["wake_edges"], 1)
        self.assertEqual(prox["dense_completion_edges"], 0)
        self.assertEqual(prox["isolated_completion_edges"], 1)
        self.assertEqual(prox["nearest_completion_us"]["p50"], 100.0)
        # The transaction commit inside the window is reported as a fact.
        self.assertEqual(
            [(f["event"], f["offset_ms"]) for f in scan["fs_events_in_window"]],
            [("btrfs_transaction_commit", 73.0)])
        # The block issue/completion pair identifies the shared device.
        self.assertEqual(scan["shared_device_events"]["devices"], ["259,0"])
        self.assertEqual(scan["shared_device_events"]["level"],
                         "shared_device_temporal")
        # A long wrapper with no diagnostic events keeps the negative.
        negatives = [w for w in kw["wrappers"]
                     if w["no_diagnostic_events_in_window"]]
        self.assertEqual(len(negatives), 1)
        self.assertEqual(negatives[0]["tag"], "sub_dir_sync")
        self.assertEqual(negatives[0]["event_counts"], {})
        # The stale limitation is swapped for the correlation caveats.
        joined = " ".join(result["limitations"])
        self.assertNotIn("no block/journal events captured", joined)
        self.assertIn("evidence classes separate temporal overlap", joined)
        # The rendered representative timeline includes the events.
        texts = sorted((tmp / "timelines").glob("*.txt"))
        self.assertEqual(len(texts), 1)
        body = texts[0].read_text()
        self.assertIn("fs/block/writeback events in window: 4", body)
        self.assertIn("folio_wait_writeback", body)
        self.assertIn("*", body.split("folio_wait_writeback")[0].splitlines()[-1])

    def test_kernel_wait_withheld_when_clock_crossing_unvalidated(self):
        # Diagnostic events are trace-side only; without validated
        # cross-clock subtraction the per-wrapper correlation is withheld
        # (totals stay, wrappers stay empty).
        from fs_trace import kernel_wait_section
        section = kernel_wait_section(
            [{"ts": 5, "event": "block_rq_issue", "tid": 1, "cpu": 0,
              "comm": "k", "fields": "259,0 R 1 + 1"}],
            {}, [{"long_wrappers": []}], {}, {}, {"events": []}, False)
        self.assertEqual(section["status"],
                         "withheld_cross_clock_not_validated")
        self.assertEqual(section["wrappers"], [])
        self.assertEqual(section["event_totals"], {"block_rq_issue": 1})

    def test_kernel_wait_dense_segment_proximity_is_not_causal(self):
        # Isolation rule: with >= 2 completions inside a blocked segment,
        # wake-edge proximity cannot single out a cause — reported as
        # dense temporal overlap (proximity_summary), never as
        # supported_causal.  A segment with exactly one completion IS
        # classified, with its observed waker comm.
        from fs_trace import kernel_wait_section
        MS = 10**6
        b0 = 1_000 * 10**9
        b1 = 2_000 * 10**9

        def rec(base, off_us, sector):
            return {"ts": base + off_us * 1000,
                    "event": "block_rq_complete", "tid": 999, "cpu": 0,
                    "comm": "irq/123",
                    "fields": f"259,0 W {sector} + 32 be,0,4 [0]"}

        diag = [
            rec(b0, 4400, 6175136), rec(b0, 4600, 6176672),
            rec(b0, 4999, 6177696),          # dense segment: 3 completions
            rec(b1, 4999, 6178720),          # isolated segment: 1 completion
        ]
        wrappers = [
            {"w0_ns": b0, "w1_ns": b0 + 10 * MS, "tid": 700,
             "tag": "sub_scan", "dur_ms": 10.0,
             "identity": {"job_task_id": 41}},
            {"w0_ns": b1, "w1_ns": b1 + 10 * MS, "tid": 701,
             "tag": "sub_dir_sync", "dur_ms": 10.0,
             "identity": {"job_task_id": 42}},
        ]
        timeline = {
            700: [("out", b0 + 2 * MS, "D"), ("waking", b0 + 5 * MS, None)],
            701: [("out", b1 + 2 * MS, "D"), ("waking", b1 + 5 * MS, None)],
        }
        wakes = {700: [(b0 + 5 * MS, "kworker/u48:0")],
                 701: [(b1 + 5 * MS, "kworker/u48:7")]}
        section = kernel_wait_section(
            diag, wakes, [{"run": "run-1", "long_wrappers": wrappers}],
            timeline, {}, {"events": []}, True)
        dense, isolated = section["wrappers"]
        # Dense segment: proximity is temporal only, no causal entry.
        self.assertEqual(dense["evidence"], [])
        prox = dense["proximity_summary"]
        self.assertEqual(prox["dense_completion_edges"], 1)
        self.assertEqual(prox["isolated_completion_edges"], 0)
        self.assertEqual(prox["nearest_completion_us"]["max"], 1.0)
        top = dense["phase_summary"]["top_segments"][0]
        self.assertEqual(top["rq_completions_during"], 3)
        self.assertEqual(top["waker_comm"], "kworker/u48:0")
        # Isolated segment: classified with waker attribution.
        self.assertEqual(len(isolated["evidence"]), 1)
        caus = isolated["evidence"][0]
        self.assertEqual(caus["level"], "supported_causal")
        self.assertEqual(caus["distance_us"], 1.0)
        self.assertEqual(caus["waker_comm"], "kworker/u48:7")
        self.assertEqual(
            isolated["proximity_summary"]["isolated_completion_edges"], 1)
        self.assertEqual(isolated["shared_device_events"]["devices"],
                         ["259,0"])

    def test_parse_trace_collects_diagnostic_events(self):
        sample = "\n".join([
            "# tracer: nop",
            "# entries-in-buffer/entries-written: 5/5   #P:8",
            " kworker/6:1H-135 [001] d..2. 1000.000100: block_rq_issue: "
            "259,0 RM 16384 () 6175136 + 32 be,0,4 [kworker/6:1H]",
            " rustfs-fsync-700 [000] d..2. 1000.000200: "
            "folio_wait_writeback: bdi nvme0n1p3: ino=12345 index=7",
            " kworker-51 [002] d..2. 1000.000300: btrfs_transaction_commit: "
            "6320451e-6e11-4cbb-83ce-db232864bb96: root=5(FS_TREE) gen=4242",
            " rustfs-fsync-700 [000] d..2. 1000.000400: "
            "sys_fsync(fd: 0x3)",
            " kworker/u8-50 [003] d..2. 1000.000450: sched_waking: "
            "comm=rustfs-fsync pid=700 prio=120 target_cpu=000",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        self.assertEqual(stats["bad_lines"], 0)
        self.assertEqual(len(stats["diag_events"]), 3)
        self.assertEqual(stats["diag_events"][0]["event"], "block_rq_issue")
        self.assertEqual(stats["diag_events"][0]["tid"], 135)
        self.assertEqual(stats["diag_events"][1]["tid"], 700)
        self.assertEqual(
            stats["event_counts"]["btrfs_transaction_commit"], 1)
        # Syscall lines stay syscall lines, not diagnostic events.
        self.assertEqual(syscalls[700],
                         [(1000 * 10**9 + 400 * 1000, "fsync", "enter")])
        self.assertEqual(stats["event_counts"]["sys_fsync_enter"], 1)
        # Wake lines record the waker comm (task current on the CPU);
        # LINE_RE splits "comm-tid", so comm is "kworker/u8" of tid 50.
        self.assertEqual(stats["wakes"][700],
                         [(1000 * 10**9 + 450 * 1000, "kworker/u8")])

    def test_parse_settings_keeps_diag_event_lines_as_events(self):
        from fs_trace import parse_settings_file
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        p = tmp / "trace.settings"
        p.write_text(
            "trace_clock=mono\nbuffer_size_kb=16387\n"
            "syscalls/sys_enter_fsync enable=1 filter=none\n"
            "block/block_rq_issue enable=1 filter=dev == 271581184\n"
            "btrfs/btrfs_tree_lock enable=1 filter=none\n")
        parsed = parse_settings_file(p)
        self.assertEqual(parsed["buffer_size_kb"], "16387")
        self.assertIn(
            "block/block_rq_issue enable=1 filter=dev == 271581184",
            parsed["events"])
        self.assertIn("btrfs/btrfs_tree_lock enable=1 filter=none",
                      parsed["events"])
        # The event line's "=" must not leak into the key=value section.
        self.assertNotIn("block/block_rq_issue enable", parsed)

    def test_fixture_identity_preserves_job_and_operation(self):
        result, _ = self._analyze_fixture(timelines=0)
        wrappers = result["runs"][0]["long_wrappers"]
        by_tag = {}
        for w in wrappers:
            by_tag.setdefault(w["tag"], []).append(w)
        # Two jobs on the SAME executor TID keep distinct identities.
        scan = by_tag["sub_scan"][0]
        dirs = sorted(by_tag["sub_dir_sync"], key=lambda w: w["dur_ms"])
        self.assertEqual(scan["identity"]["executor_tid"], 700)
        self.assertEqual(dirs[0]["identity"]["executor_tid"], 700)
        self.assertEqual(scan["identity"]["job_task_id"], 41)
        self.assertEqual(dirs[0]["identity"]["job_task_id"], 42)
        # Two operations retain their own job associations.
        self.assertEqual(scan["identity"]["op_hash"], 0x1111222233334444)
        self.assertEqual(dirs[0]["identity"]["op_hash"], 0x5555666677778888)
        self.assertEqual(scan["identity"]["step_tag"], "src_dir_sync")
        self.assertEqual(dirs[0]["identity"]["step_tag"], "dst_dir_fsync")
        self.assertEqual(scan["identity"]["submit_tid"], 30)
        self.assertEqual(dirs[0]["identity"]["submit_tid"], 31)
        # Repeated wrapper tags within one job stay distinguishable.
        self.assertEqual([d["identity"]["occurrence"] for d in dirs], [0, 1])
        self.assertTrue(all(d["identity"]["occurrences_for_tag"] == 32
                            for d in dirs))
        # Missing operation context stays explicit, never inferred from tid.
        rename = by_tag["sub_rename"][0]
        self.assertIsNone(rename["identity"]["op_hash"])
        self.assertEqual(rename["identity"]["job_task_id"], 43)
        self.assertEqual(rename["operation_link"]["association"], "missing")
        self.assertIsNone(rename["operation_link"]["overlaps_commit_wait"])

    def test_fixture_operation_link_commit_wait_and_quorum(self):
        result, _ = self._analyze_fixture(timelines=0)
        scan = next(w for w in result["runs"][0]["long_wrappers"]
                    if w["tag"] == "sub_scan")
        link = scan["operation_link"]
        self.assertEqual(link["association"], "job_submit_record")
        self.assertEqual(link["op_hash"], 0x1111222233334444)
        self.assertTrue(link["overlaps_commit_wait"])
        self.assertAlmostEqual(link["commit_wait"]["begin_offset_ms"], 15.0)
        self.assertAlmostEqual(link["commit_wait"]["end_offset_ms"], 185.0)
        self.assertEqual(link["send"]["kind"], "send_ok")
        self.assertAlmostEqual(link["send"]["offset_ms"], 45.0)
        self.assertEqual(link["send"]["results_seen"], 4)
        self.assertEqual(link["send"]["write_quorum"], 3)
        # Association and overlap are not a response-critical claim.
        self.assertEqual(link["required_before_response"], "not_established")
        # Operation 2 has no wait records: overlap stays unknown, not False.
        dirs = next(w for w in result["runs"][0]["long_wrappers"]
                    if w["tag"] == "sub_dir_sync")
        self.assertEqual(dirs["operation_link"]["op_hash"], 0x5555666677778888)
        self.assertIsNone(dirs["operation_link"]["overlaps_commit_wait"])
        self.assertIsNone(dirs["operation_link"]["commit_wait"])

    def test_fixture_overlap_annotation_and_accounting(self):
        result, _ = self._analyze_fixture(timelines=0)
        accounting = result["accounting"]
        # The count is a count of marker-delimited observations.
        self.assertEqual(accounting["wrapper_observations"]["run-1"], 6)
        self.assertEqual(accounting["wrapper_observations"]["total"], 6)
        self.assertIn("not independent",
                      accounting["wrapper_observations"]["note"])
        self.assertIn("double-counted",
                      accounting["definitions"]["per_tag_totals"])
        # Nested observations are counted and reported per run/tag pair.
        self.assertEqual(accounting["nesting_and_overlap"]["nested"], {
            "run-1:sub_scan>sub_fsync_files": 1,
            "run-1:sub_scan>sub_fdatasync": 1,
            "run-1:sub_fsync_files>sub_fdatasync": 1,
        })
        self.assertEqual(accounting["nesting_and_overlap"]["partial_overlaps"],
                         {})
        # Per-tag sums overlap (395 ms over 6 observations)...
        tags = accounting["per_tag"]
        self.assertTrue(all(t["overlapping_across_tags"] for t in tags.values()))
        self.assertEqual(tags["sub_dir_sync"]["observations"], 2)
        self.assertEqual(tags["sub_dir_sync"]["duration_sum_ms"], 125.0)
        self.assertEqual(sum(t["duration_sum_ms"] for t in tags.values()),
                         395.0)
        # ...the non-overlapping union does not (265 ms thread-time).
        non_over = accounting["non_overlapping"]
        wrapper_union = non_over["by_run"]["run-1"]["wrapper_regions"]
        self.assertEqual(wrapper_union["thread_time_ms"], 265.0)
        self.assertEqual(wrapper_union["tids"], 2)
        self.assertEqual(wrapper_union["regions"], 4)
        # State totals come from the timeline over the union regions and
        # tile them exactly (an accounting consistency check).
        self.assertEqual(sum(wrapper_union["states_ms"].values()), 265.0)
        self.assertGreater(wrapper_union["states_ms"]["blocked:D"], 0)
        syscall_union = non_over["by_run"]["run-1"]["syscall_regions"]
        self.assertEqual(syscall_union["thread_time_ms"], 179.0)
        self.assertEqual(sum(syscall_union["states_ms"].values()), 179.0)
        # Per-wrapper decompositions are preserved alongside.
        scan = next(w for w in result["runs"][0]["long_wrappers"]
                    if w["tag"] == "sub_scan")
        self.assertEqual(scan["overlap"]["same_tid_contains"][0]["tag"],
                         "sub_fsync_files")
        self.assertEqual(len(scan["overlap"]["same_tid_contains"]), 2)
        self.assertEqual(scan["dur_ms"], 80.0)
        self.assertEqual(result["cross_clock_analysis"]["status"], "reported")

    def test_fixture_timelines_do_not_overwrite_same_tag_same_tid(self):
        result, run_dir = self._analyze_fixture(timelines=10)
        written = result["timelines"]
        self.assertEqual(len(written), 6)
        self.assertEqual(len(set(written)), 6)
        on_disk = sorted(str(p.relative_to(run_dir))
                         for p in (run_dir / "timelines").glob("*.txt"))
        self.assertEqual(on_disk, sorted(written))
        # The two long sub_dir_sync wrappers share tag and TID but must get
        # separate files, distinguished by job and occurrence.
        dir_files = [f for f in written if "-sub_dir_sync-" in f]
        self.assertEqual(len(dir_files), 2)
        self.assertTrue(any("job42-occ0" in f for f in dir_files), dir_files)
        self.assertTrue(any("job42-occ1" in f for f in dir_files), dir_files)
        sample = next((run_dir / f).read_text() for f in dir_files
                      if "job42-occ0" in f)
        self.assertIn("identity: run=run-1 job_task_id=42", sample)
        self.assertIn("commit-wait:", sample)
        self.assertIn("overlap (same tid):", sample)

    def test_fixture_clock_validated_from_fixture_metadata(self):
        result, _ = self._analyze_fixture(timelines=0)
        clock = result["quality"]["clock"]
        self.assertEqual(clock["trace_clock_selected"], "mono")
        self.assertEqual(clock["probe_clock"], "CLOCK_MONOTONIC")
        self.assertEqual(clock["compatibility"]["status"], "validated")
        self.assertEqual(clock["direct_subtraction"]["status"], "validated")
        alignment = result["quality"]["alignment"]
        self.assertEqual(alignment["status"], "validated")
        self.assertEqual(alignment["paired_sync_calls"], 34)
        self.assertEqual(alignment["match_rate"], 1.0)
        self.assertEqual(alignment["tolerance_us"], 1.0)
        self.assertNotIn("direct_subtraction_validated", clock)


    def test_parse_trace_orders_equal_timestamps_causally(self):
        # ftrace prints 6 fraction digits (1 us) here, so a whole
        # sleep -> wake -> run cycle can share one timestamp.  The
        # parser orders equal-time events by scheduler admissibility
        # over the recorded line order: here the recorded order is
        # already causal (out, waking, wakeup, in) and is kept —
        # and must not be re-sorted.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 6/6   #P:8",
            "   bash-100 [000] d..2. 1000.000020: sched_switch: "
            "prev_comm=bash prev_pid=100 prev_prio=120 prev_state=D ==> "
            "next_comm=swapper/0 next_pid=0 next_prio=120",
            " rustfs-fsync-700 [000] d..2. 1000.000020: sched_switch: "
            "prev_comm=rustfs-fsync prev_pid=700 prev_prio=120 "
            "prev_state=D ==> next_comm=swapper/0 next_pid=0 next_prio=120",
            "  kworker/u8-50 [001] d..2. 1000.000025: sched_waking: "
            "comm=rustfs-fsync pid=700 prio=120 target_cpu=000",
            "  kworker/u8-50 [001] d..2. 1000.000025: sched_wakeup: "
            "comm=rustfs-fsync pid=700 prio=120 target_cpu=000",
            " swapper/0-0 [002] d..2. 1000.000025: sched_switch: "
            "prev_comm=swapper/0 prev_pid=0 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs-fsync next_pid=700 next_prio=120",
            " rustfs-fsync-700 [000] d..2. 1000.000200: sched_switch: "
            "prev_comm=rustfs-fsync prev_pid=700 prev_prio=120 "
            "prev_state=S ==> next_comm=swapper/0 next_pid=0 next_prio=120",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        self.assertEqual(timelines[700],
                         [("out", t + 20_000, "D"),
                          ("waking", t + 25_000, None),
                          ("wakeup", t + 25_000, None),
                          ("in", t + 25_000, None),
                          ("out", t + 200_000, "S")])
        self.assertEqual(stats["bad_lines"], 0)
        # One tie each for pid 700 (wakes + switch-in at 25 us) and pid 0
        # (the two switch-ins at 20 us); neither needed a reorder, none
        # was ambiguous, and none lacked an admissible order.
        self.assertEqual(stats["equal_ts_ties"], 2)
        self.assertEqual(stats["equal_ts_causal_repairs"], 0)
        self.assertEqual(stats["equal_ts_ambiguous"], 0)
        self.assertEqual(stats["equal_ts_unresolved"], 0)
        # Zero-length segments (transition and runnable inside one
        # timestamp) must not leak into the state accounting.
        rows = state_segments(timelines[700], t, t + 300_000)
        self.assertEqual(rows, [("blocked:D", t + 20_000, t + 25_000),
                                ("running", t + 25_000, t + 200_000),
                                ("blocked:S", t + 200_000, t + 300_000)])

    def test_parse_trace_keeps_in_then_out_tie_in_recorded_order(self):
        # A task can switch in and back out inside one printed
        # microsecond.  Both switch lines are recorded on the same CPU,
        # so their order is observable.  Forcing a causal rank that
        # always puts the switch-out first would rewrite this tie to
        # out -> in, end the tie group in state "running", and report
        # the whole interval after the tie as running time.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 5/5   #P:8",
            " rustfs-700 [000] d..2. 1000.000010: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=D ==> "
            "next_comm=swapper/0 next_pid=0 next_prio=120",
            "  kworker/u8-50 [001] d..3. 1000.000020: sched_waking: "
            "comm=rustfs pid=700 prio=120 target_cpu=002",
            "  kworker/u8-50 [001] d..3. 1000.000020: sched_wakeup: "
            "comm=rustfs pid=700 prio=120 target_cpu=002",
            " swapper/2-0 [002] d..2. 1000.000025: sched_switch: "
            "prev_comm=swapper/2 prev_pid=0 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            " rustfs-700 [002] d..2. 1000.000025: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=D ==> "
            "next_comm=kworker/u8-9 next_pid=99 next_prio=120",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        # Recorded order (in, out) is kept: it is the observable one.
        self.assertEqual(timelines[700],
                         [("out", t + 10_000, "D"),
                          ("waking", t + 20_000, None),
                          ("wakeup", t + 20_000, None),
                          ("in", t + 25_000, None),
                          ("out", t + 25_000, "D")])
        self.assertEqual(stats["equal_ts_ties"], 2)  # wakes@20, in/out@25
        self.assertEqual(stats["equal_ts_causal_repairs"], 0)
        self.assertEqual(stats["equal_ts_ambiguous"], 0)
        self.assertEqual(stats["equal_ts_unresolved"], 0)
        rows = state_segments(timelines[700], t, t + 100_000)
        # The tie ends in the post-switch-out state: [25 us, window end)
        # is blocked time, never invented running time, and the runnable
        # window between the wake and the switch-in is preserved.
        self.assertEqual(rows, [("blocked:D", t + 10_000, t + 20_000),
                                ("runnable", t + 20_000, t + 25_000),
                                ("blocked:D", t + 25_000, t + 100_000)])

    def test_parse_trace_repairs_cross_cpu_in_before_wake(self):
        # Across CPUs the trace file only shows ring-buffer merge order:
        # a switch-in can be printed before the wake it needs at the
        # same microsecond.  That recorded order is causally impossible
        # (a blocked task cannot run first) and must be repaired —
        # feeding it to the state machine as-is would relabel the whole
        # blocked span as unknown_no_wake.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 4/4   #P:8",
            " rustfs-700 [000] d..2. 1000.000010: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=D ==> "
            "next_comm=swapper/0 next_pid=0 next_prio=120",
            " swapper/0-0 [000] d..2. 1000.000020: sched_switch: "
            "prev_comm=swapper/0 prev_pid=0 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            "  kworker/u8-50 [001] d..3. 1000.000020: sched_waking: "
            "comm=rustfs pid=700 prio=120 target_cpu=000",
            "  kworker/u8-50 [001] d..3. 1000.000020: sched_wakeup: "
            "comm=rustfs pid=700 prio=120 target_cpu=000",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        # Recorded (in, waking, wakeup) -> causally repaired to
        # (waking, wakeup, in); the two wake events share a CPU, so the
        # choice among them is observable and not marked ambiguous.
        self.assertEqual(timelines[700],
                         [("out", t + 10_000, "D"),
                          ("waking", t + 20_000, None),
                          ("wakeup", t + 20_000, None),
                          ("in", t + 20_000, None)])
        self.assertEqual(stats["equal_ts_ties"], 1)
        self.assertEqual(stats["equal_ts_causal_repairs"], 1)
        self.assertEqual(stats["equal_ts_ambiguous"], 0)
        self.assertEqual(stats["equal_ts_unresolved"], 0)
        rows = state_segments(timelines[700], t, t + 100_000)
        # Blocked time survives the tie; no unknown_* appears.
        self.assertEqual(rows, [("blocked:D", t + 10_000, t + 20_000),
                                ("running", t + 20_000, t + 100_000)])

    def test_parse_trace_marks_cross_cpu_ties_ambiguous(self):
        # A wake event and a switch-out of the same task at one
        # timestamp come from different CPUs: both physical orders are
        # possible within the microsecond (futile wake while running vs.
        # immediate wake after blocking), so the recorded line order is
        # NOT an observation.  The causal default (switch-out before
        # wake initiation) picks, and the tie is marked ambiguous.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 4/4   #P:8",
            "   foo-1 [000] d..2. 1000.000010: sched_switch: "
            "prev_comm=foo prev_pid=1 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            "  kworker/u8-50 [001] d..3. 1000.000020: sched_waking: "
            "comm=rustfs pid=700 prio=120 target_cpu=000",
            " rustfs-700 [000] d..2. 1000.000020: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=S ==> "
            "next_comm=swapper/0 next_pid=0 next_prio=120",
            "  kworker/u8-50 [001] d..3. 1000.000030: sched_wakeup: "
            "comm=rustfs pid=700 prio=120 target_cpu=000",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        # Recorded (waking, out) -> causal default gives (out, waking):
        # the cross-CPU choice was not observable and is counted as such.
        self.assertEqual(timelines[700],
                         [("in", t + 10_000, None),
                          ("out", t + 20_000, "S"),
                          ("waking", t + 20_000, None),
                          ("wakeup", t + 30_000, None)])
        self.assertEqual(stats["equal_ts_ties"], 1)
        self.assertEqual(stats["equal_ts_causal_repairs"], 1)
        self.assertEqual(stats["equal_ts_ambiguous"], 1)
        self.assertEqual(stats["equal_ts_unresolved"], 0)
        rows = state_segments(timelines[700], t, t + 100_000)
        self.assertEqual(rows,
                         [("running", t + 10_000, t + 20_000),
                          ("wakeup_transition", t + 20_000, t + 30_000),
                          ("runnable", t + 30_000, t + 100_000)])

    def test_parse_trace_keeps_recorded_order_when_unresolvable(self):
        # Two switch-outs for one pid inside one microsecond: from the
        # second one onward nothing is admissible (a task cannot switch
        # out while it is not running).  The recorded order is kept and
        # flagged instead of being reordered into a fabricated story.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 3/3   #P:8",
            "   foo-1 [000] d..2. 1000.000010: sched_switch: "
            "prev_comm=foo prev_pid=1 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            " rustfs-700 [000] d..2. 1000.000020: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=S ==> "
            "next_comm=swapper/0 next_pid=0 next_prio=120",
            " rustfs-700 [001] d..2. 1000.000020: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=D ==> "
            "next_comm=foo next_pid=5 next_prio=120",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        self.assertEqual(timelines[700],
                         [("in", t + 10_000, None),
                          ("out", t + 20_000, "S"),
                          ("out", t + 20_000, "D")])
        self.assertEqual(stats["equal_ts_ties"], 1)
        self.assertEqual(stats["equal_ts_causal_repairs"], 0)
        self.assertEqual(stats["equal_ts_ambiguous"], 0)
        self.assertEqual(stats["equal_ts_unresolved"], 1)
        rows = state_segments(timelines[700], t, t + 100_000)
        self.assertEqual(rows, [("running", t + 10_000, t + 20_000),
                                ("blocked:D", t + 20_000, t + 100_000)])

    def test_parse_trace_mixed_cpu_tie_preserves_each_cpu_sequence(self):
        # Mixed-CPU tie: CPU 0 recorded in -> out(D) inside one
        # microsecond; CPU 1 a waking.  Each per-CPU buffer is read in
        # order, so CPU 0's in -> out sequence is observable and a hard
        # constraint.  Picking the admissible switch-out over CPU 0's
        # earlier recorded switch-in (because the simulated state
        # entering the tie is "running") would reverse that sequence
        # and end the tie in "running", reporting the whole following
        # interval as running time.  When causality and the recorded
        # sequences conflict, the uncertainty must be reported instead
        # of reordering observable events.
        sample = "\n".join([
            "# tracer: nop",
            "#",
            "# entries-in-buffer/entries-written: 4/4   #P:8",
            "   foo-1 [000] d..2. 1000.000010: sched_switch: "
            "prev_comm=foo prev_pid=1 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            "  kworker/u8-50 [001] d..3. 1000.000025: sched_waking: "
            "comm=rustfs pid=700 prio=120 target_cpu=000",
            " swapper/9-0 [000] d..2. 1000.000025: sched_switch: "
            "prev_comm=swapper/9 prev_pid=9 prev_prio=120 prev_state=S ==> "
            "next_comm=rustfs next_pid=700 next_prio=120",
            " rustfs-700 [000] d..2. 1000.000025: sched_switch: "
            "prev_comm=rustfs prev_pid=700 prev_prio=120 prev_state=D ==> "
            "next_comm=foo next_pid=5 next_prio=120",
        ]) + "\n"
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        timelines, syscalls, stats = parse_trace(self._trace_file(tmp, sample))
        t = 1000 * 10**9
        # CPU 0's recorded in -> out(D) survives untouched: only CPU 1's
        # lone waking is placed by causality, the rest keeps recorded
        # order.
        self.assertEqual(timelines[700],
                         [("in", t + 10_000, None),
                          ("waking", t + 25_000, None),
                          ("in", t + 25_000, None),
                          ("out", t + 25_000, "D")])
        self.assertEqual(stats["equal_ts_ties"], 1)
        self.assertEqual(stats["equal_ts_causal_repairs"], 0)
        self.assertEqual(stats["equal_ts_ambiguous"], 0)
        # The conflict (switch-in not admissible while "running", with
        # the switch-out behind it on the same CPU) is reported...
        self.assertEqual(stats["equal_ts_unresolved"], 1)
        rows = state_segments(timelines[700], t, t + 100_000)
        # ...and [25 us, window end) is the recorded post-out blocked
        # state — never invented running time.
        self.assertEqual(rows, [("running", t + 10_000, t + 25_000),
                                ("blocked:D", t + 25_000, t + 100_000)])

    def test_provenance_lists_every_consumed_input_with_hashes(self):
        result, tmp = self._analyze_fixture(timelines=0)
        prov = result["provenance"]
        self.assertEqual(result["schema"], "fs-trace-diagnostic/v3")
        roles = [e["role"] for e in prov["inputs"]]
        self.assertEqual(roles, ["ftrace raw capture", "trace settings",
                                 "trace stats", "trace error log",
                                 "capture manifest", "probe dump"])
        for entry in prov["inputs"]:
            self.assertTrue(entry["present"], entry)
            path = Path(entry["path"])
            self.assertTrue(path.is_file())
            # Recorded hash is the file's actual hash, computed by reading
            # it during this analysis.
            self.assertEqual(entry["sha256"],
                             hashlib.sha256(path.read_bytes()).hexdigest())
        params = prov["parameters"]
        self.assertEqual(params["long_wrapper_threshold_ms"], 50.0)
        self.assertEqual(params["timelines_per_run"], 0)
        self.assertEqual(params["selection_counts"]["long_wrappers_total"],
                         result["accounting"]["wrapper_observations"]["total"])
        self.assertEqual(
            params["selection_counts"]["paired_calls_by_run"],
            {r["run"]: r["paired_calls"] for r in result["runs"]})
        self.assertIn("compatible_trace_clocks",
                      params["clock_validation_thresholds"])
        # The trace block keeps the same hash as the inputs entry.
        trace_entry = prov["inputs"][0]
        self.assertEqual(prov["trace"]["sha256"], trace_entry["sha256"])

    def test_changing_an_input_changes_its_recorded_hash(self):
        result, tmp = self._analyze_fixture(timelines=0)
        settings = tmp / "trace.settings"
        dump = tmp / "run-1" / "fs-probe.bin"

        def hashes(res):
            return {e["path"]: e["sha256"] for e in res["provenance"]["inputs"]}

        before = hashes(result)
        # Settings: change one recorded value (still parseable).
        settings.write_text(settings.read_text().replace(
            "buffer_size_kb=16387", "buffer_size_kb=8192"))
        # Probe dump: flip a bit in a byte the reader ignores (a record's
        # reserved field), so the dump stays valid but its bytes change.
        raw = bytearray(dump.read_bytes())
        raw[HEADER.size + HEADER_V2_EXT.size + 1] ^= 0x01
        dump.write_bytes(bytes(raw))
        after = hashes(fs_trace_analyze(tmp, timelines=0))
        self.assertNotEqual(before[str(settings)], after[str(settings)])
        self.assertNotEqual(before[str(dump)], after[str(dump)])
        self.assertEqual(after[str(dump)],
                         hashlib.sha256(dump.read_bytes()).hexdigest())
        # An input that did not change keeps its hash across regeneration.
        self.assertEqual(before[str(tmp / "trace.raw")],
                         after[str(tmp / "trace.raw")])

    def test_provenance_distinguishes_declared_from_verified_binary_sha(self):
        result, tmp = self._analyze_fixture(timelines=0)
        binary = result["provenance"]["binary"]
        # Fixture manifest declares no binary: declared-only, nothing hashed.
        self.assertIsNone(binary["declared_path"])
        self.assertIsNone(binary["file_sha256"])
        self.assertIn("no binary path", binary["verification"])
        # With a declared path the analysis hashes that file itself and
        # keeps the independently computed digest separate from the
        # manifest's claim.
        blob = tmp / "fake-rustfs-binary"
        blob.write_bytes(b"not-a-real-binary")
        declared = hashlib.sha256(b"other-bytes").hexdigest()
        (tmp / "manifest.json").write_text(json.dumps(
            {"repetitions": 1, "binary": str(blob),
             "binary_sha256": declared}))
        result2 = fs_trace_analyze(tmp, timelines=0)
        binary = result2["provenance"]["binary"]
        self.assertEqual(binary["declared_sha256"], declared)
        self.assertEqual(binary["file_sha256"],
                         hashlib.sha256(b"not-a-real-binary").hexdigest())
        self.assertFalse(binary["matches_declared"])
        self.assertIn("independently hashed", binary["verification"])
        # The manifest block stays the declared view of the capture.
        self.assertEqual(result2["provenance"]["manifest"]["binary_sha256"],
                         declared)

    def test_version2_dump_reports_closed_rejections(self):
        result, _ = self._analyze_fixture(timelines=0)
        self.assertEqual(result["runs"][0]["probe_rejected_closed"], 0)
        self.assertEqual(
            result["quality"]["loss"]["probe_rejected_after_close"],
            {"run-1": 0})
        self.assertFalse(any("format version 1" in note
                             for note in result["limitations"]))

    def test_version1_dumps_append_probe_generation_limitation(self):
        # Older captures (format version 1) must still analyze, keep the
        # missing counter missing, and carry the evidence-preservation
        # note about the probe generation that produced them.
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self._build_fs_trace_fixture(tmp, probe_dump_version=1)
        result = fs_trace_analyze(tmp, timelines=0)
        _, records = read_probe(tmp / "run-1" / "fs-probe.bin")
        self.assertEqual(result["runs"][0]["probe_records"], len(records))
        self.assertIsNone(result["runs"][0]["probe_rejected_closed"])
        self.assertIsNone(
            result["quality"]["loss"]["probe_rejected_after_close"]["run-1"])
        notes = [note for note in result["limitations"]
                 if "format version 1" in note]
        self.assertEqual(len(notes), 1, result["limitations"])
        self.assertIn("do not prove memory safety", notes[0])
        self.assertIn("remains unresolved", notes[0])


class FtraceScriptTests(unittest.TestCase):
    """Fake-tracefs tests for ftrace.sh's instance-isolation contract.

    The fixture is a temp directory laid out like tracefs.  These tests
    prove the script's own control flow — ownership markers, fail-closed
    arming, stop-before-read collection, explicit cleanup, and that the
    default tracer is never written — against that fake tree.  They do
    NOT prove kernel tracefs behaviour; instance support on a real host
    remains to be validated (README separates the two).
    """

    SCRIPT = Path(__file__).resolve().parent / "ftrace.sh"
    INSTANCE = "rustfs-fstrace"
    DEFAULT_FILES = ("tracing_on", "trace_clock", "buffer_size_kb",
                     "events/enable", "options/overwrite", "current_tracer",
                     "error_log", "trace")
    EVENT_DIRS = ("sched/sched_switch", "sched/sched_waking",
                  "sched/sched_wakeup", "syscalls/sys_enter_fsync",
                  "syscalls/sys_exit_fsync", "syscalls/sys_enter_fdatasync",
                  "syscalls/sys_exit_fdatasync",
                  "btrfs/btrfs_transaction_commit",
                  "btrfs/btrfs_finish_ordered_extent",
                  "btrfs/btrfs_reserve_ticket", "btrfs/btrfs_tree_lock",
                  "writeback/folio_wait_writeback",
                  "block/block_bio_queue", "block/block_rq_issue",
                  "block/block_rq_complete")
    EMPTY_TRACE = ("# tracer: nop\n#\n"
                   "# entries-in-buffer/entries-written: 0/0   #P:8\n")
    TWO_ENTRY_TRACE = (
        "# tracer: nop\n#\n"
        "# entries-in-buffer/entries-written: 2/2   #P:8\n"
        "  default-a [000] d..2. 100.000001: sched_switch: prev_comm=default-a"
        " prev_pid=1 prev_prio=120 prev_state=S ==> next_comm=default-b"
        " next_pid=2 next_prio=120\n"
        "  default-b [001] d..2. 100.000002: sched_switch: prev_comm=default-b"
        " prev_pid=2 prev_prio=120 prev_state=S ==> next_comm=default-a"
        " next_pid=1 next_prio=120\n")

    def _write(self, path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def _make_tracefs(self, tmp, *, instance=True, owned=True,
                      instance_trace=None, instance_tracing_on="0\n"):
        """Build a fake tracefs tree plus its ownership state dir.

        The top-level (default) tracer files carry sentinel values that
        must survive every command unchanged.  The instance tree stands
        in for what the kernel populates when `mkdir instances/<name>`
        succeeds.
        """
        root = Path(tmp) / "tracefs"
        state = Path(tmp) / "state"
        self._write(root / "tracing_on", "1\n")
        self._write(root / "trace_clock", "global\n")
        self._write(root / "buffer_size_kb", "64\n")
        self._write(root / "events/enable", "1\n")
        self._write(root / "options/overwrite", "1\n")
        self._write(root / "current_tracer", "nop\n")
        self._write(root / "error_log", "default-log\n")
        self._write(root / "trace", self.TWO_ENTRY_TRACE)
        self._write(root / "per_cpu/cpu0/stats",
                    "== default:\nentries: 2\noverrun: 0\nbytes: 4096\n")
        (root / "instances").mkdir(parents=True, exist_ok=True)
        if instance:
            inst = root / "instances" / self.INSTANCE
            self._write(inst / "tracing_on", instance_tracing_on)
            self._write(inst / "trace_clock", "global\n")
            self._write(inst / "buffer_size_kb", "64\n")
            self._write(inst / "events/enable", "0\n")
            self._write(inst / "options/overwrite", "1\n")
            self._write(inst / "current_tracer", "nop\n")
            self._write(inst / "error_log", "")
            self._write(inst / "trace",
                        self.EMPTY_TRACE if instance_trace is None
                        else instance_trace)
            for e in self.EVENT_DIRS:
                self._write(inst / f"events/{e}/enable", "0\n")
                self._write(inst / f"events/{e}/filter", "\n")
            self._write(inst / "per_cpu/cpu0/stats",
                        "== instance:\nentries: 0\noverrun: 0\nbytes: 4096\n")
            if owned:
                self._write(state / f"instance.{self.INSTANCE}",
                            f"instance={self.INSTANCE}\n"
                            f"tracefs_root={root}\n"
                            "created_at=2026-01-01T00:00:00+00:00\n"
                            "created_by=tester\n"
                            "created_by_pid=1\n")
        return root, state

    def _run(self, *args, root, state, xtrace=False):
        env = dict(os.environ)
        env["TRACEFS"] = str(root)
        env["FTRACE_STATE_DIR"] = str(state)
        cmd = ["bash"] + (["-x"] if xtrace else []) + [str(self.SCRIPT)]
        return subprocess.run(cmd + list(args), capture_output=True,
                              text=True, env=env)

    def _default_snapshot(self, root):
        return {name: (root / name).read_text() for name in self.DEFAULT_FILES}

    def test_arm_configures_only_the_owned_instance(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        before = self._default_snapshot(root)
        proc = self._run("arm", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("fixture mode", proc.stderr)  # non-root, fake tree
        inst = root / "instances" / self.INSTANCE
        self.assertEqual((inst / "tracing_on").read_text().strip(), "1")
        self.assertEqual((inst / "trace_clock").read_text().strip(), "mono")
        self.assertEqual((inst / "options/overwrite").read_text().strip(), "0")
        self.assertEqual((inst / "buffer_size_kb").read_text().strip(), "16384")
        self.assertEqual(
            (inst / "events/sched/sched_switch/filter").read_text().strip(),
            'prev_comm ~ "rustfs*" || next_comm ~ "rustfs*"')
        for e in ("sched/sched_switch", "sched/sched_waking",
                  "sched/sched_wakeup", "syscalls/sys_enter_fsync",
                  "syscalls/sys_exit_fdatasync",
                  "btrfs/btrfs_transaction_commit",
                  "btrfs/btrfs_tree_lock",
                  "writeback/folio_wait_writeback",
                  "block/block_rq_issue"):
            self.assertEqual((inst / f"events/{e}/enable").read_text().strip(),
                             "1", e)
        # Block events carry the device filter before they are enabled.
        for e in ("block/block_bio_queue", "block/block_rq_issue",
                  "block/block_rq_complete"):
            self.assertEqual(
                (inst / f"events/{e}/filter").read_text().strip(),
                "dev == 271581184", e)
        # The default tracer is untouched: every sentinel survives.
        self.assertEqual(self._default_snapshot(root), before)
        # Ownership marker recorded, and status names the instance.
        marker = state / f"instance.{self.INSTANCE}"
        self.assertTrue(marker.is_file())
        self.assertIn(f"tracefs_root={root}", marker.read_text())
        self.assertIn(f"instance={self.INSTANCE}", proc.stdout)

    def test_arm_partial_failure_leaves_instance_disabled(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        # Remove the whole event directory: the enable write can then only
        # fail (in a real tracefs a rejected write fails the same way).
        shutil.rmtree(root / "instances" / self.INSTANCE /
                      "events/sched/sched_waking")
        before = self._default_snapshot(root)
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        inst = root / "instances" / self.INSTANCE
        # Fail-closed: the partially-configured instance is not recording.
        self.assertEqual((inst / "tracing_on").read_text().strip(), "0")
        # And the default tracer still was not written.
        self.assertEqual(self._default_snapshot(root), before)

    def test_arm_refuses_instance_without_ownership_marker(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp, owned=False)
        inst = root / "instances" / self.INSTANCE
        self._write(inst / "tracing_on", "1\n")  # "someone else's" tracer
        before = self._default_snapshot(root)
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ownership marker", proc.stderr)
        # Refused before any write: not taken over, not disabled, no marker.
        self.assertEqual((inst / "tracing_on").read_text().strip(), "1")
        self.assertFalse((state / f"instance.{self.INSTANCE}").exists())
        self.assertEqual(self._default_snapshot(root), before)

    def test_arm_without_instance_support_fails_without_fallback(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp, instance=False)
        before = self._default_snapshot(root)
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("refusing to fall back to the default tracer",
                      proc.stderr)
        # The default tracer's buffer and controls were never written.
        self.assertEqual(self._default_snapshot(root), before)

    def test_arm_refuses_rearm_with_uncollected_data(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        # Simulate a recording that was never collected.
        self._write(inst / "trace", self.TWO_ENTRY_TRACE)
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("uncollected data", proc.stderr)
        # --force is the explicit way to discard it.
        proc = self._run("arm", "--force", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # The uncollected buffer was discarded as explicitly ordered.
        self.assertEqual((inst / "trace").read_text().strip(), "")

    def test_collect_stops_recording_before_reading_snapshot(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        # Simulate recorded events (the ring is non-empty now).
        self._write(inst / "trace", self.TWO_ENTRY_TRACE)
        before = self._default_snapshot(root)
        out = tmp / "capture"
        proc = self._run("collect", str(out), "trace",
                         root=root, state=state, xtrace=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # xtrace order: the `echo 0` that freezes the ring precedes the
        # `cat` that reads it.
        lines = [l for l in proc.stderr.splitlines() if l.startswith("+ ")]
        stop = lines.index("+ echo 0")
        read = next(i for i, l in enumerate(lines) if l.startswith("+ cat"))
        self.assertLess(stop, read, proc.stderr)
        self.assertEqual((inst / "tracing_on").read_text().strip(), "0")
        # Snapshot content equals what was in the ring before collection.
        self.assertEqual((out / "trace.raw").read_text(),
                         self.TWO_ENTRY_TRACE)
        settings = (out / "trace.settings").read_text()
        self.assertIn(f"instance={self.INSTANCE}", settings)
        self.assertIn(f"instance_dir={inst}", settings)
        self.assertIn(f"tracefs={root}", settings)
        self.assertIn("tracing_on=0", settings)
        self.assertTrue((out / "trace.stats").is_file())
        # Ring cleared only after everything was captured...
        self.assertEqual((inst / "trace").read_text().strip(), "")
        # ...and the default tracer still untouched.
        self.assertEqual(self._default_snapshot(root), before)

    def test_collect_refuses_to_overwrite_an_existing_capture(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        out = tmp / "capture"
        out.mkdir()
        (out / "trace.raw").write_text("previous capture\n")
        proc = self._run("collect", str(out), "trace", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("refusing to overwrite", proc.stderr)
        self.assertEqual((out / "trace.raw").read_text(),
                         "previous capture\n")

    def test_off_is_repeatable_and_only_touches_the_instance(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        before = self._default_snapshot(root)
        for _ in range(2):  # safe and repeatable
            proc = self._run("off", root=root, state=state)
            self.assertEqual(proc.returncode, 0, proc.stderr)
        inst = root / "instances" / self.INSTANCE
        self.assertEqual((inst / "tracing_on").read_text().strip(), "0")
        self.assertEqual((inst / "events/enable").read_text().strip(), "0")
        self.assertEqual(self._default_snapshot(root), before)
        # status reports ownership and instance identity.
        proc = self._run("status", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(f"instance={self.INSTANCE}", proc.stdout)
        self.assertIn("owned=yes", proc.stdout)

    def test_status_on_absent_or_unowned_instance(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp, instance=False)
        proc = self._run("status", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("not present", proc.stdout)
        # off with no instance is a no-op, not an error.
        proc = self._run("off", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # Present but unowned: refuse to read it.
        root2, state2 = self._make_tracefs(tmp / "b", owned=False)
        proc = self._run("status", root=root2, state=state2)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ownership marker", proc.stderr)

    def test_destroy_is_explicit_and_guards_experiment_data(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        # Unowned instance: never removed.
        root, state = self._make_tracefs(tmp / "a", owned=False)
        proc = self._run("destroy", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertTrue((root / "instances" / self.INSTANCE).is_dir())
        # Owned instance holding data: refused without --force...
        root, state = self._make_tracefs(tmp / "b")
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        self._write(inst / "trace", self.TWO_ENTRY_TRACE)
        proc = self._run("destroy", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("uncollected data", proc.stderr)
        self.assertTrue(inst.is_dir())
        # ...explicit with --force, then repeatable.
        proc = self._run("destroy", "--force", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(inst.exists())
        self.assertFalse((state / f"instance.{self.INSTANCE}").exists())
        proc = self._run("destroy", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("nothing to remove", proc.stdout)

    def test_every_command_refuses_marker_from_another_tracefs_root(self):
        # Ownership validation is centralized: a marker recorded for a
        # different tracefs root grants nothing — no command may
        # disable, read, re-arm, or remove the instance through it.
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        marker = state / f"instance.{self.INSTANCE}"
        marker.write_text(marker.read_text().replace(
            f"tracefs_root={root}", "tracefs_root=/other/tracefs"))
        before = self._default_snapshot(root)
        # off must not disable an instance owned under another root.
        proc = self._run("off", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different tracefs root", proc.stderr)
        self.assertEqual((inst / "tracing_on").read_text().strip(), "1")
        # collect writes no capture.
        out = tmp / "capture"
        proc = self._run("collect", str(out), "trace", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different tracefs root", proc.stderr)
        self.assertFalse((out / "trace.raw").exists())
        # destroy does not remove the instance...
        proc = self._run("destroy", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different tracefs root", proc.stderr)
        self.assertTrue(inst.is_dir())
        # ...status does not claim ownership, re-arm does not reconfigure.
        proc = self._run("status", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different tracefs root", proc.stderr)
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different tracefs root", proc.stderr)
        # The mismatched marker is not ours to delete either.
        self.assertTrue(marker.is_file())
        self.assertEqual(self._default_snapshot(root), before)

    def test_foreign_root_marker_survives_and_local_stale_marker_is_cleaned(self):
        # clear_stale_marker may only remove markers recorded for THIS
        # tracefs root: a same-named instance elsewhere keeps its state.
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp, instance=False)
        state.mkdir(parents=True, exist_ok=True)
        foreign = state / f"instance.{self.INSTANCE}"
        foreign.write_text(f"instance={self.INSTANCE}\n"
                           "tracefs_root=/other/tracefs\n")
        proc = self._run("off", root=root, state=state)  # absent here
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(foreign.is_file())  # not our root: untouched
        # A stale marker FOR this root (instance gone) is still cleaned.
        foreign.write_text(f"instance={self.INSTANCE}\n"
                           f"tracefs_root={root}\n")
        proc = self._run("off", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(foreign.exists())

    def test_unknown_buffer_status_blocks_arm_and_destroy_without_force(self):
        # "Could not read" must never mean "no data": an unreadable or
        # unfamiliar trace file blocks the ring clear and the removal
        # unless --force is explicit.
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        # Unfamiliar content: non-whitespace, but no recognized header.
        self._write(inst / "trace", "not a trace header at all\nmore data\n")
        proc = self._run("arm", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("cannot establish", proc.stderr)
        # The refused arm did not clear the unestablished buffer.
        self.assertIn("not a trace header", (inst / "trace").read_text())
        proc = self._run("destroy", root=root, state=state)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("cannot establish", proc.stderr)
        self.assertTrue(inst.is_dir())
        # --force is the explicit override for both.
        proc = self._run("arm", "--force", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual((inst / "trace").read_text().strip(), "")
        # An unreadable trace file is also "unknown", not "empty".
        if os.geteuid() != 0:  # chmod cannot out-read root
            os.chmod(inst / "trace", 0o000)
            try:
                proc = self._run("arm", root=root, state=state)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("cannot establish", proc.stderr)
            finally:
                os.chmod(inst / "trace", 0o644)

    def test_cleared_buffer_reads_as_empty_and_rearms_without_force(self):
        # The collect -> arm cycle must work: a ring cleared by this
        # script reads back as empty (real tracefs: a 0/0 header;
        # fixture: a bare newline), never as "unknown".
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        root, state = self._make_tracefs(tmp)
        self.assertEqual(self._run("arm", root=root, state=state).returncode, 0)
        inst = root / "instances" / self.INSTANCE
        self._write(inst / "trace", self.TWO_ENTRY_TRACE)
        out = tmp / "capture"
        self.assertEqual(
            self._run("collect", str(out), "trace", root=root,
                      state=state).returncode, 0)
        self.assertEqual((out / "trace.raw").read_text(),
                         self.TWO_ENTRY_TRACE)
        # Re-arm of the cleared ring needs no --force.
        proc = self._run("arm", root=root, state=state)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
