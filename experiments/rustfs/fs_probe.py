"""Reconstruct blocking-pool job boundaries and the commit-channel wait.

Analyzes one or more probe run directories: `fs-probe.bin` (records from the
patched Tokio), the Dial9 trace (poll containment and clock sync), and
`tiers.json` (client attempts); with several directories the output is
`{"runs": [...]}`. All probe and Dial9 timestamps are raw `CLOCK_MONOTONIC`
nanoseconds; the flush header's `(monotonic, realtime)` pair maps them to the
OTLP/client realtime clock. Missing boundaries are reported as `null` with an
explicit `missing` list, never as zero.
"""

import argparse
import bisect
import hashlib
import json
import struct
from pathlib import Path

from load import percentiles

MAGIC = b"RFSPRB01"
HEADER = struct.Struct("<8sIIQQQQIIQ")
RECORD = struct.Struct("<BBHIIIQQQ")

KIND_SUBMIT = 1
KIND_JOB_START = 2
KIND_JOB_END = 3
KIND_JOB_COMPLETE = 4
KIND_JOIN_READY = 5
KIND_OP_BEGIN = 10
KIND_OP_END = 11
KIND_WAIT_BEGIN = 12
KIND_WAIT_END = 13
KIND_SEND_OK = 14
KIND_SEND_ERR = 15

KIND_NAMES = {
    KIND_SUBMIT: "submit",
    KIND_JOB_START: "job_start",
    KIND_JOB_END: "job_end",
    KIND_JOB_COMPLETE: "job_complete",
    KIND_JOIN_READY: "join_ready",
    KIND_OP_BEGIN: "op_begin",
    KIND_OP_END: "op_end",
    KIND_WAIT_BEGIN: "wait_begin",
    KIND_WAIT_END: "wait_end",
    KIND_SEND_OK: "send_ok",
    KIND_SEND_ERR: "send_err",
}

OP_NONE = 0

# Tag names must match the `scope_step` call sites in the probe build.
STEP_TAGS = ("mkdir", "make_dir_all", "rename", "rename_no_owner")


def fnv1a(data, basis, prime, bits):
    mask = (1 << bits) - 1
    hash_value = basis
    for byte in data:
        hash_value = ((hash_value ^ byte) * prime) & mask
    return hash_value


def op_hash(bucket, object_key):
    """Mirror of the Rust probe's operation hash (FNV-1a 64, 0 -> 1)."""
    value = fnv1a(bucket.encode(), 0xcbf29ce484222325, 0x100000001b3, 64)
    value = fnv1a(b"/", value, 0x100000001b3, 64)
    value = fnv1a(object_key.encode(), value, 0x100000001b3, 64)
    return value if value else 1


def step_hash(tag):
    """Mirror of the Rust probe's step tag hash (FNV-1a 32, 0 -> 1)."""
    value = fnv1a(tag.encode(), 0x811c9dc5, 0x01000193, 32)
    return value if value else 1


def read_probe(path):
    raw = path.read_bytes()
    if len(raw) < HEADER.size:
        raise ValueError("probe dump smaller than its header")
    (magic, version, record_size, capacity, total, flushed_monotonic_ns,
     flushed_realtime_ns, clock_id, reserved, pid) = HEADER.unpack_from(raw)
    if magic != MAGIC:
        raise ValueError("probe dump magic mismatch")
    if version != 1 or record_size != RECORD.size:
        raise ValueError(f"unsupported probe dump format version={version} record_size={record_size}")
    body = raw[HEADER.size:]
    if len(body) % record_size:
        raise ValueError("probe dump has a partial trailing record")
    count = len(body) // record_size
    if count != min(total, capacity):
        raise ValueError(f"probe dump record count {count} != min(total_seen {total}, capacity {capacity})")
    records = []
    for offset in range(0, len(body), record_size):
        kind, _reserved, _pad, step, tid, _reserved2, task_id, ts, a = RECORD.unpack_from(body, offset)
        records.append({"kind": kind, "name": KIND_NAMES.get(kind, f"kind_{kind}"),
                        "step": step, "tid": tid, "id": task_id, "ts": ts, "a": a})
    header = {"version": version, "record_size": record_size, "capacity": capacity,
              "total_seen": total, "dropped_records": max(0, total - capacity),
              "flushed_monotonic_ns": flushed_monotonic_ns,
              "flushed_realtime_ns": flushed_realtime_ns,
              "clock_id": clock_id, "reserved": reserved, "pid": pid,
              "records": count}
    return header, records


def group_jobs(records):
    """Group blocking-job records by task id; keep join_ready only for jobs.

    Returns `(jobs, counters)`. Each job maps boundary name to its record;
    absent boundaries stay absent.
    """
    jobs = {}
    counters = {"duplicate_boundaries": 0, "non_blocking_join_ready": 0}
    for record in sorted(records, key=lambda r: r["ts"]):
        kind = record["kind"]
        if kind == KIND_JOIN_READY:
            continue
        if kind not in (KIND_SUBMIT, KIND_JOB_START, KIND_JOB_END, KIND_JOB_COMPLETE):
            continue
        job = jobs.setdefault(record["id"], {})
        name = KIND_NAMES[kind]
        if name in job:
            counters["duplicate_boundaries"] += 1
            continue
        job[name] = record
    for record in sorted(records, key=lambda r: r["ts"]):
        if record["kind"] != KIND_JOIN_READY:
            continue
        job = jobs.get(record["id"])
        if job is None:
            counters["non_blocking_join_ready"] += 1
        elif "join_ready" not in job:
            job["join_ready"] = record
    return jobs, counters


def worker_thread_ids(events):
    """Map Dial9 worker ids to OS thread ids; reject identity changes."""
    workers = {}
    for event in events:
        if event.get("event") == "WorkerParkEvent":
            worker = event["worker_id"]
            if worker in workers and workers[worker] != event["tid"]:
                raise ValueError("Worker thread identity changed mid-run")
            workers[worker] = event["tid"]
    return workers


def pair_polls(events, workers):
    """Pair poll events into intervals tagged with the worker's thread id."""
    tid_of = {worker: tid for worker, tid in workers.items()}
    pending = {}
    polls = []
    counters = {"unmatched_poll_ends": 0, "overwritten_poll_starts": 0}
    for event in sorted((e for e in events if "timestamp_ns" in e),
                        key=lambda e: e["timestamp_ns"]):
        kind = event.get("event")
        worker = event.get("worker_id")
        if kind == "PollStartEvent":
            if worker in pending:
                counters["overwritten_poll_starts"] += 1
            pending[worker] = event
        elif kind == "PollEndEvent":
            begin = pending.pop(worker, None)
            if begin is None:
                counters["unmatched_poll_ends"] += 1
                continue
            if worker not in tid_of:
                raise ValueError(f"Poll on worker {worker} without a thread identity")
            polls.append({"tid": tid_of[worker], "worker_id": worker,
                          "start": begin["timestamp_ns"], "end": event["timestamp_ns"],
                          "spawn_loc": begin.get("spawn_loc"),
                          "task_id": begin.get("task_id"),
                          "local_queue": begin.get("local_queue")})
    counters["unmatched_poll_starts"] = len(pending)
    return polls, counters


def build_poll_index(polls):
    """tid -> (starts, polls) with polls sorted by start for bisecting."""
    grouped = {}
    for poll in polls:
        grouped.setdefault(poll["tid"], []).append(poll)
    index = {}
    for tid, items in grouped.items():
        items.sort(key=lambda p: p["start"])
        index[tid] = ([p["start"] for p in items], items)
    return index


def containing_poll(index, ts, tid):
    """Poll on `tid` whose interval contains `ts`, else None."""
    if index is None:
        return None
    entry = index.get(tid)
    if entry is None:
        return None
    starts, items = entry
    position = bisect.bisect_right(starts, ts) - 1
    if position < 0:
        return None
    poll = items[position]
    if poll["end"] >= ts:
        return poll
    return None


def _millis(delta_ns):
    return None if delta_ns is None else delta_ns / 1e6


def job_intervals(job, poll_index):
    """Observed intervals of one blocking job; None means not observed."""
    submit = job.get("submit")
    start = job.get("job_start")
    end = job.get("job_end")
    complete = job.get("job_complete")
    join = job.get("join_ready")
    missing = [name for name in ("submit", "job_start", "job_end",
                                 "job_complete", "join_ready") if name not in job]
    resume_poll = containing_poll(poll_index, join["ts"], join["tid"]) if join else None
    resume_start = resume_poll["start"] if resume_poll else None
    intervals = {
        "submit_to_start_ms": _millis(start["ts"] - submit["ts"]) if submit and start else None,
        "start_to_end_ms": _millis(end["ts"] - start["ts"]) if start and end else None,
        "end_to_complete_ms": _millis(complete["ts"] - end["ts"]) if end and complete else None,
        "runnable_to_resume_ms": _millis(resume_start - complete["ts"]) if complete and resume_start else None,
        "resume_to_join_ready_ms": _millis(join["ts"] - resume_start) if join and resume_start else None,
        "total_submit_to_join_ready_ms": _millis(join["ts"] - submit["ts"]) if submit and join else None,
    }
    step = submit["step"] if submit else (start["step"] if start else 0)
    return {"task_id": submit["id"] if submit else (start["id"] if start else None),
            "step_hash": step,
            "op_hash": submit["a"] if submit else OP_NONE,
            "submit_tid": submit["tid"] if submit else None,
            "job_tid": start["tid"] if start else (end["tid"] if end else None),
            "join_tid": join["tid"] if join else None,
            "resume_poll_start_ns": resume_start,
            "resume_poll_task": resume_poll["task_id"] if resume_poll else None,
            "resume_poll_spawn_loc": resume_poll["spawn_loc"] if resume_poll else None,
            "missing": missing,
            **intervals}


def summarize_jobs(jobs, poll_index=None):
    summaries = [job_intervals(job, poll_index) for job in jobs.values()]
    complete = [s for s in summaries if not s["missing"]]
    fields = ("submit_to_start_ms", "start_to_end_ms", "end_to_complete_ms",
              "runnable_to_resume_ms", "resume_to_join_ready_ms",
              "total_submit_to_join_ready_ms")
    stats = {}
    for field in fields:
        values = [s[field] for s in summaries if s[field] is not None]
        stats[field] = {"observations": len(values), "ms": percentiles(values)}
    return {"jobs": len(summaries), "complete_jobs": len(complete),
            "incomplete_jobs": len(summaries) - len(complete),
            "intervals": stats}


def reconstruct_wait(records, poll_index, wait_begin, wait_end, op):
    """Decompose the commit-channel wait observed for one operation."""
    sends = [r for r in records
             if r["kind"] in (KIND_SEND_OK, KIND_SEND_ERR)
             and r["a"] == op and wait_begin["ts"] <= r["ts"] <= wait_end["ts"]]
    first_send = min(sends, key=lambda r: r["ts"]) if sends else None
    resume_poll = containing_poll(poll_index, wait_end["ts"], wait_end["tid"])
    resume_start = resume_poll["start"] if resume_poll else None
    missing = []
    if first_send is None:
        missing.append("send")
    if resume_poll is None:
        missing.append("resume_poll")
    return {
        "send_kind": None if first_send is None else KIND_NAMES[first_send["kind"]],
        "send_ts": None if first_send is None else first_send["ts"],
        "wait_total_ms": _millis(wait_end["ts"] - wait_begin["ts"]),
        "wait_begin_to_send_ms": _millis(first_send["ts"] - wait_begin["ts"]) if first_send else None,
        "send_to_resume_poll_ms": _millis(resume_start - first_send["ts"]) if first_send and resume_start else None,
        "resume_poll_to_wait_end_ms": _millis(wait_end["ts"] - resume_start) if resume_start else None,
        "resume_poll_start_ns": resume_start,
        "resume_poll_task": resume_poll["task_id"] if resume_poll else None,
        "wait_begin_tid": wait_begin["tid"],
        "wait_end_tid": wait_end["tid"],
        "missing": missing,
    }


def timeline_offsets(records_of_interest, attempt_mono_ns, extra=None):
    """ms offsets from the client attempt for a set of records."""
    entries = [{"name": r["name"], "offset_ms": (r["ts"] - attempt_mono_ns) / 1e6,
                "tid": r["tid"], "step_hash": r["step"], "op_hash": r["a"],
                "task_id": r["id"]}
               for r in records_of_interest]
    for name, ts, tid in extra or []:
        entries.append({"name": name, "offset_ms": (ts - attempt_mono_ns) / 1e6, "tid": tid})
    return sorted(entries, key=lambda e: e["offset_ms"])


def step_names(step_hashes):
    """Map observed step hashes back to their tag names where known."""
    known = {step_hash(tag): tag for tag in STEP_TAGS}
    return {known.get(value, "unknown"): value for value in sorted(set(step_hashes))}


def read_dial9(root):
    """Read every converted Dial9 trace under `root/telemetry`."""
    events = []
    traces = sorted(root.glob("telemetry/**/trace.*.jsonl"))
    for trace in traces:
        with trace.open() as stream:
            events.extend(json.loads(line) for line in stream if line.strip())
    return events, traces


def analyze_run(root):
    """One probe run directory -> the full diagnostic record."""
    manifest_path = root.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    header, records = read_probe(root / "fs-probe.bin")
    tiers = json.loads((root / "tiers.json").read_text())
    events, traces = read_dial9(root)
    workers = worker_thread_ids(events)
    polls, poll_counters = pair_polls(events, workers)
    poll_index = build_poll_index(polls)

    clock_sync = next((e for e in events if e.get("event") == "ClockSyncEvent"), None)
    probe_offset = header["flushed_realtime_ns"] - header["flushed_monotonic_ns"]
    dial9_offset = (clock_sync["realtime_ns"] - clock_sync["timestamp_ns"]
                    if clock_sync else None)

    jobs, job_counters = group_jobs(records)
    op_begins = {r["a"]: r for r in records if r["kind"] == KIND_OP_BEGIN}
    kind_counts = {}
    for record in records:
        kind_counts[record["name"]] = kind_counts.get(record["name"], 0) + 1

    result = {
        "manifest": manifest,
        "probe_dump": {**{k: header[k] for k in
                          ("version", "record_size", "capacity", "total_seen",
                           "dropped_records", "records", "clock_id", "pid",
                           "flushed_monotonic_ns", "flushed_realtime_ns")},
                       "sha256": hashlib.sha256(
                           (root / "fs-probe.bin").read_bytes()).hexdigest()},
        "record_counts": kind_counts,
        "clock": {"probe_realtime_minus_monotonic_ns": probe_offset,
                  "dial9_realtime_minus_monotonic_ns": dial9_offset,
                  "offset_delta_ns": None if dial9_offset is None
                  else probe_offset - dial9_offset},
        "jobs": summarize_jobs(jobs, poll_index),
        "job_counters": job_counters,
        "polls": {"paired": len(polls), **poll_counters},
        "step_hashes": step_names(r["step"] for r in records if r["step"]),
        "tiers": [],
        "limitations": [
            "Blocking-job boundaries are observed wall time on one host, not CPU time",
            "T5 completion is recorded after task.run() stores the output and wakes the joiner; it is an upper bound on when the result became available",
            "Poll containment resolves the resume point by OS thread id; a record outside every Dial9 poll is reported as missing",
            "Ring-buffer wrap drops the oldest records; dropped_records reports how many",
            "Probe records only appear for operations whose context reached a probe site; untagged work is reported as OP_NONE",
        ],
    }

    # Tier summaries plus the representative request's reconstruction.
    for tier in tiers:
        attempts = [r for r in tier["requests"] if not r.get("client_shed")]
        if not attempts:
            result["tiers"].append({"tier": tier["tier"],
                                    "load": {k: v for k, v in tier.items() if k != "requests"},
                                    "representative_request": None,
                                    "missing": ["attempts"]})
            continue
        slow = max(attempts, key=lambda r: r["attempt_to_completion_ms"])
        bucket = tier.get("bucket", "tokio-experiment")
        key = slow["key"]
        op = op_hash(bucket, key)
        op_records = [r for r in records if r["a"] == op and r["kind"] in (
            KIND_OP_BEGIN, KIND_OP_END, KIND_WAIT_BEGIN, KIND_WAIT_END,
            KIND_SEND_OK, KIND_SEND_ERR)]
        attempt_mono = slow["attempted_ns"]
        missing = []
        if not op_begins.get(op):
            missing.append("op_begin")
        if not any(r["kind"] == KIND_OP_END for r in op_records):
            missing.append("op_end")
        wait_pairs = []
        pending_begin = None
        for record in sorted((r for r in records if r["a"] == op
                              and r["kind"] in (KIND_WAIT_BEGIN, KIND_WAIT_END)),
                             key=lambda r: r["ts"]):
            if record["kind"] == KIND_WAIT_BEGIN:
                pending_begin = record
            elif pending_begin is not None:
                wait_pairs.append(reconstruct_wait(records, poll_index,
                                                   pending_begin, record, op))
                pending_begin = None
        if pending_begin is not None:
            missing.append("wait_end")
        if not wait_pairs:
            missing.append("wait_begin")
        op_jobs = {task_id: job for task_id, job in jobs.items()
                   if job.get("submit", {}).get("a") == op}
        result["tiers"].append({
            "tier": tier["tier"],
            "load": {k: v for k, v in tier.items() if k != "requests"},
            "representative_request": slow,
            "op_hash": op,
            "bucket": bucket,
            "missing": missing,
            "operation_records": timeline_offsets(op_records, attempt_mono),
            "waits": wait_pairs,
            "jobs_for_operation": summarize_jobs(op_jobs, poll_index),
            "job_steps": step_names(job.get("submit", {}).get("step", 0)
                                    for job in op_jobs.values()
                                    if job.get("submit", {}).get("step")),
        })
    result["dial9"] = {
        "events": len(events),
        "trace_sha256": {str(t.relative_to(root)): hashlib.sha256(t.read_bytes()).hexdigest()
                         for t in traces},
        "workers": workers,
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path, nargs="+",
                        help="probe run directory (contains fs-probe.bin, tiers.json, telemetry/)")
    parser.add_argument("--output", type=Path,
                        help="write JSON here instead of stdout")
    args = parser.parse_args()
    runs = [analyze_run(root) for root in args.run_directory]
    payload = runs[0] if len(runs) == 1 else {"runs": runs}
    text = json.dumps(payload, indent=2)
    if args.output:
        args.output.write_text(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    main()
