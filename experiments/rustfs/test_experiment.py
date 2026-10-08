import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from analyze import analyze
from fs_probe import (HEADER, KIND_JOB_END, KIND_JOB_START, KIND_JOIN_READY,
                      KIND_SEND_OK, KIND_SUB, KIND_SUBMIT, RECORD, analyze_run,
                      build_poll_index, classify_pool_tids,
                      containing_poll, group_jobs, job_calls, job_intervals,
                      job_stages, op_hash, operation_job_rows, pair_polls,
                      read_probe, reconstruct_wait, step_hash, summarize_jobs)
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


if __name__ == "__main__":
    unittest.main()
