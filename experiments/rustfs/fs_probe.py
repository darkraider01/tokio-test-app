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
# Format version 2 appends this 24-byte extension between the version-1
# header and the records (see fs_probe.rs flush_to_env): stored records,
# capacity refusals, and pushes refused after recording closed. Version 1
# dumps have no extension — the closed-rejection counter simply did not
# exist and is reported as missing (None), never as zero.
HEADER_V2_EXT = struct.Struct("<QQQ")
RECORD = struct.Struct("<BBHIIIQQQ")

KIND_SUBMIT = 1
KIND_JOB_START = 2
KIND_JOB_END = 3
KIND_JOB_COMPLETE = 4
KIND_JOIN_READY = 5
# Inner-boundary marker inside a running blocking closure (`id` = job id).
KIND_SUB = 6
KIND_OP_BEGIN = 10
KIND_OP_END = 11
KIND_WAIT_BEGIN = 12
KIND_WAIT_END = 13
KIND_SEND_OK = 14
KIND_SEND_ERR = 15
KIND_DISK_COMPLETE = 16
KIND_QUORUM_RESULT = 17

KIND_NAMES = {
    KIND_SUBMIT: "submit",
    KIND_JOB_START: "job_start",
    KIND_JOB_END: "job_end",
    KIND_JOB_COMPLETE: "job_complete",
    KIND_JOIN_READY: "join_ready",
    KIND_SUB: "sub",
    KIND_OP_BEGIN: "op_begin",
    KIND_OP_END: "op_end",
    KIND_WAIT_BEGIN: "wait_begin",
    KIND_WAIT_END: "wait_end",
    KIND_SEND_OK: "send_ok",
    KIND_SEND_ERR: "send_err",
    KIND_DISK_COMPLETE: "disk_complete",
    KIND_QUORUM_RESULT: "quorum_result",
}

OP_NONE = 0
DISK_NONE = 0xFFFF_FFFF

# Tag names must match the `scope_step` call sites in the probe build
# (fs.rs, os.rs, and commit.rs). Frozen: changing a name changes its hash.
STEP_TAGS = ("mkdir", "make_dir_all", "rename", "rename_no_owner",
             "dest_meta_read", "staged_meta_write", "src_dir_sync",
             "rename_data_dir", "rename_meta", "dst_dir_fsync",
             "ancestor_fsync")

# Inner-boundary markers must match the `sub_step` call sites (os.rs). Frozen
# the same way; `sub` records exist only when the server ran with
# RUSTFS_FS_PROBE_SUB set. Each wrapped call `X` is delimited by a start
# marker `X` and an explicit end marker `X_end`; an error path that
# `?`-propagates before the end marker simply has no paired end (see
# `job_calls`).
SUB_TAGS = ("sub_scan", "sub_prep_open", "sub_prep_write", "sub_fdatasync",
            "sub_fsync_files", "sub_dir_open", "sub_dir_sync", "sub_rename",
            "sub_scan_end", "sub_prep_open_end", "sub_prep_write_end",
            "sub_fdatasync_end", "sub_fsync_files_end", "sub_dir_open_end",
            "sub_dir_sync_end", "sub_rename_end")

# Call-site -> pool map for the tags above (pool = which Tokio runtime runs
# the spawned closure; the two pools are disjoint thread sets):
# - MAIN_POOL_TAGS: spawn sites are `tokio::fs` wrappers or
#   `run_blocking_namespace_operation` -> the main runtime's blocking pool.
# - FSYNC_POOL_TAGS: call chain reaches `fsync_spawn_blocking` -> the
#   dedicated FSYNC_RUNTIME (os.rs).
MAIN_POOL_TAGS = frozenset(("mkdir", "make_dir_all", "rename", "rename_no_owner",
                            "dest_meta_read", "staged_meta_write",
                            "rename_data_dir", "rename_meta"))
FSYNC_POOL_TAGS = frozenset(("src_dir_sync", "dst_dir_fsync", "ancestor_fsync"))


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
    """Read a probe dump (format version 1 or 2) into header + records.

    Version 2 carries explicit counters (`stored_records`,
    `rejected_capacity`, `rejected_closed`) after the unchanged 64-byte
    version-1 header; version 1 dumps keep their original layout and
    meanings (`stored`/`rejected_capacity` are derived from
    `min/max(total_seen, capacity)` as always, `rejected_closed` did not
    exist and stays `None`). Record count and counter consistency are
    validated; a corrupt or truncated dump raises ValueError.
    """
    raw = path.read_bytes()
    if len(raw) < HEADER.size:
        raise ValueError("probe dump smaller than its header")
    (magic, version, record_size, capacity, total, flushed_monotonic_ns,
     flushed_realtime_ns, clock_id, reserved, pid) = HEADER.unpack_from(raw)
    if magic != MAGIC:
        raise ValueError("probe dump magic mismatch")
    if version not in (1, 2, 3):
        raise ValueError(
            f"unsupported probe dump format version={version} record_size={record_size}")
    if record_size != RECORD.size:
        raise ValueError(
            f"unsupported probe dump format version={version} record_size={record_size}")
    if version == 1:
        body = raw[HEADER.size:]
        stored = min(total, capacity)
        rejected_capacity = max(0, total - capacity)
        rejected_closed = None  # absent in version 1: preserved as missing
    else:
        if len(raw) < HEADER.size + HEADER_V2_EXT.size:
            raise ValueError("probe dump smaller than its version-2 header")
        stored, rejected_capacity, rejected_closed = HEADER_V2_EXT.unpack_from(
            raw, HEADER.size)
        body = raw[HEADER.size + HEADER_V2_EXT.size:]
        if stored > capacity:
            raise ValueError(
                f"probe dump stored {stored} exceeds capacity {capacity}")
        if stored != min(total, capacity) or rejected_capacity != total - stored:
            raise ValueError(
                "probe dump counters inconsistent: "
                f"stored {stored} rejected_capacity {rejected_capacity} "
                f"total_seen {total} capacity {capacity}")
    if len(body) % record_size:
        raise ValueError("probe dump has a partial trailing record")
    count = len(body) // record_size
    if count != stored:
        raise ValueError(f"probe dump record count {count} != stored {stored} (min(total_seen {total}, capacity {capacity}))")
    records = []
    for offset in range(0, len(body), record_size):
        kind, _reserved, _pad, step, tid, reserved2, task_id, ts, a = RECORD.unpack_from(body, offset)
        disk_index = None
        attempt = None
        status = None
        quorum_satisfied = None
        success_before = None
        success_after = None
        results_seen = None
        write_quorum = None
        if version >= 3:
            if kind == KIND_SUBMIT:
                if reserved2 != DISK_NONE:
                    disk_index = reserved2 & 0xFFFF
                    attempt = (reserved2 >> 16) & 0xFFFF
            elif kind == KIND_DISK_COMPLETE:
                disk_index = step & 0xFFFF
                attempt = (step >> 16) & 0xFFFF
                status = {0: "ok", 1: "err", 2: "panic"}.get(reserved2, f"status_{reserved2}")
            elif kind == KIND_QUORUM_RESULT:
                if step != DISK_NONE:
                    disk_index = step & 0xFFFF
                    attempt = (step >> 16) & 0xFFFF
                status = {0: "ok", 1: "err", 2: "panic", 3: "join_error"}.get(reserved2 & 0xFF, f"status_{reserved2 & 0xFF}")
                quorum_satisfied = bool((reserved2 >> 8) & 0xFF)
                success_before = (reserved2 >> 16) & 0xFF
                success_after = (reserved2 >> 24) & 0xFF
                results_seen = task_id & 0xFFFF_FFFF
                write_quorum = task_id >> 32

        rec = {"kind": kind, "name": KIND_NAMES.get(kind, f"kind_{kind}"),
               "step": step, "tid": tid, "id": task_id, "ts": ts, "a": a,
               "reserved2": reserved2, "disk_index": disk_index, "attempt": attempt}
        if status is not None:
            rec["status"] = status
        if quorum_satisfied is not None:
            rec["quorum_satisfied"] = quorum_satisfied
        if success_before is not None:
            rec["success_before"] = success_before
        if success_after is not None:
            rec["success_after"] = success_after
        if results_seen is not None:
            rec["results_seen"] = results_seen
        if write_quorum is not None:
            rec["write_quorum"] = write_quorum
        records.append(rec)
    header = {"version": version, "record_size": record_size, "capacity": capacity,
              "total_seen": total, "dropped_records": rejected_capacity,
              "stored_records": stored, "rejected_capacity": rejected_capacity,
              "rejected_closed": rejected_closed,
              "flushed_monotonic_ns": flushed_monotonic_ns,
              "flushed_realtime_ns": flushed_realtime_ns,
              "clock_id": clock_id, "reserved": reserved, "pid": pid,
              "records": count}
    return header, records


def group_jobs(records):
    """Group blocking-job records by task id; keep join_ready only for jobs.

    Returns `(jobs, counters)`. Each job maps boundary name to its record;
    absent boundaries stay absent. Inner-boundary markers (`sub`) attach to
    their job as a list under `subs`.
    """
    jobs = {}
    counters = {"duplicate_boundaries": 0, "non_blocking_join_ready": 0,
                "orphan_sub_markers": 0}
    for record in sorted(records, key=lambda r: r["ts"]):
        kind = record["kind"]
        if kind == KIND_JOIN_READY:
            continue
        if kind == KIND_SUB:
            job = jobs.get(record["id"])
            if job is None:
                counters["orphan_sub_markers"] += 1
                continue
            job.setdefault("subs", []).append(record)
            continue
        if kind not in (KIND_SUBMIT, KIND_JOB_START, KIND_JOB_END, KIND_JOB_COMPLETE):
            continue
        job = jobs.setdefault(record["id"], {})
        name = KIND_NAMES[kind]
        if name in job:
            counters["duplicate_boundaries"] += 1
            continue
        job[name] = record
        if kind == KIND_SUBMIT:
            job["disk_index"] = record.get("disk_index")
            job["attempt"] = record.get("attempt")
            job["op_hash"] = record.get("a")
            job["step_hash"] = record.get("step")
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
    """Observed intervals of one blocking job; None means not observed.

    `completion_to_poll_start_proxy_ms` is a diagnostic proxy, not measured
    scheduling latency: `job_complete` records after `task.run()` stored the
    result and woke the joiner, so the joiner may resume before the marker,
    and the Dial9 poll containing `join_ready` need not be the wake-triggered
    resume (the task may have been polled for another reason).
    """
    submit = job.get("submit")
    start = job.get("job_start")
    end = job.get("job_end")
    complete = job.get("job_complete")
    join = job.get("join_ready")
    missing = [name for name in ("submit", "job_start", "job_end",
                                 "job_complete", "join_ready") if name not in job]
    resume_poll = containing_poll(poll_index, join["ts"], join["tid"]) if join else None
    resume_start = resume_poll["start"] if resume_poll else None
    # job_start/job_end carry the executor thread's CPU clock (ns) in `a`.
    # Captures written before that extension carry 0 -> reported as null, not 0.
    cpu_ns = None
    if start and end and start["a"] and end["a"] and end["a"] >= start["a"]:
        cpu_ns = end["a"] - start["a"]
    wall_ns = (end["ts"] - start["ts"]) if start and end else None
    intervals = {
        "submit_to_start_ms": _millis(start["ts"] - submit["ts"]) if submit and start else None,
        "start_to_end_ms": _millis(wall_ns),
        "closure_cpu_ms": _millis(cpu_ns),
        "closure_offcpu_ms": _millis(wall_ns - cpu_ns) if wall_ns is not None and cpu_ns is not None else None,
        "end_to_complete_ms": _millis(complete["ts"] - end["ts"]) if end and complete else None,
        "completion_to_poll_start_proxy_ms": _millis(resume_start - complete["ts"]) if complete and resume_start else None,
        "poll_start_to_join_ready_ms": _millis(join["ts"] - resume_start) if join and resume_start else None,
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
    fields = ("submit_to_start_ms", "start_to_end_ms", "closure_cpu_ms",
              "closure_offcpu_ms", "end_to_complete_ms",
              "completion_to_poll_start_proxy_ms", "poll_start_to_join_ready_ms",
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
        # Quorum dependency recorded at the send: `step` = results the join loop
        # had processed, `id` = write_quorum, `reserved2` = fanout disk_count.
        # Captures written before that extension carry 0 -> reported as null.
        "results_seen": (first_send or {}).get("step") or None,
        "write_quorum": (first_send or {}).get("id") or None,
        "disk_count": (first_send or {}).get("reserved2", 0) or None,
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


def reconstruct_acknowledgements(records, op, jobs=None):
    """Reconstruct per-disk commit paths, acknowledgement counts, and quorum triggering."""
    op_records = [r for r in records if r.get("a") == op]
    sends = [r for r in op_records if r["kind"] in (KIND_SEND_OK, KIND_SEND_ERR)]
    first_send = min(sends, key=lambda r: r["ts"]) if sends else None
    send_ts = first_send["ts"] if first_send else None
    send_kind = KIND_NAMES[first_send["kind"]] if first_send else None

    completes = [r for r in op_records if r["kind"] == KIND_DISK_COMPLETE]
    quorum_results = [r for r in op_records if r["kind"] == KIND_QUORUM_RESULT]

    tag_names = {step_hash(tag): tag for tag in STEP_TAGS}
    disk_jobs = {}
    if jobs is not None:
        for job_id, job in jobs.items():
            if job.get("op_hash") == op and job.get("disk_index") is not None:
                key = (job["disk_index"], job.get("attempt", 0))
                disk_jobs.setdefault(key, []).append(job)

    all_keys = set()
    for r in completes + quorum_results:
        if r.get("disk_index") is not None:
            all_keys.add((r["disk_index"], r.get("attempt", 0)))
    for key in disk_jobs:
        all_keys.add(key)

    disks = []
    quorum_trigger_key = None

    for key in sorted(all_keys):
        d_idx, att = key
        comp = next((r for r in completes if r.get("disk_index") == d_idx and r.get("attempt", 0) == att), None)
        cons = next((r for r in quorum_results if r.get("disk_index") == d_idx and r.get("attempt", 0) == att), None)

        associated_jobs = []
        for j in sorted(disk_jobs.get(key, []), key=lambda j: j.get("submit", {}).get("ts", 0)):
            sub = j.get("submit")
            start = j.get("job_start")
            end = j.get("job_end")
            tag = tag_names.get(j.get("step_hash"), "unknown")
            dur = _millis(end["ts"] - start["ts"]) if start and end else None
            associated_jobs.append({
                "job_id": (sub or {}).get("id") or (start or {}).get("id"),
                "step_tag": tag,
                "submit_ts": sub["ts"] if sub else None,
                "start_ts": start["ts"] if start else None,
                "end_ts": end["ts"] if end else None,
                "dur_ms": dur,
                "is_prerequisite": (comp is not None and end is not None and end["ts"] <= comp["ts"])
            })

        prod_ts = comp["ts"] if comp else None
        prod_status = comp.get("status") if comp else None
        cons_ts = cons["ts"] if cons else None
        cons_status = cons.get("status") if cons else None
        success_before = cons.get("success_before") if cons else None
        success_after = cons.get("success_after") if cons else None
        quorum_satisfied = cons.get("quorum_satisfied", False) if cons else False

        if quorum_satisfied:
            quorum_trigger_key = key
            classification = "quorum_triggering_acknowledgement"
        elif cons_status == "ok":
            if send_ts is not None and cons_ts is not None and cons_ts <= send_ts:
                classification = "counted_before_quorum"
            else:
                classification = "post_quorum_tail"
        elif cons_status in ("err", "panic", "join_error"):
            classification = "failed_result"
        elif comp is not None:
            if send_ts is not None and prod_ts is not None and prod_ts <= send_ts:
                classification = "completed_before_quorum_unconsumed"
            else:
                classification = "post_quorum_tail"
        else:
            classification = "unestablished"

        missing = []
        if comp is None:
            missing.append("prod_boundary")
        if cons is None:
            missing.append("cons_boundary")

        disks.append({
            "disk_index": d_idx,
            "attempt": att,
            "classification": classification,
            "prod_ts": prod_ts,
            "prod_status": prod_status,
            "cons_ts": cons_ts,
            "cons_status": cons_status,
            "success_before": success_before,
            "success_after": success_after,
            "quorum_satisfied": quorum_satisfied,
            "missing": missing,
            "jobs": associated_jobs,
        })

    return {
        "op_hash": op,
        "send_kind": send_kind,
        "send_ts": send_ts,
        "quorum_trigger_disk": quorum_trigger_key[0] if quorum_trigger_key else None,
        "quorum_trigger_attempt": quorum_trigger_key[1] if quorum_trigger_key else None,
        "disks": disks,
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


def sub_names(step_hashes):
    """Map observed inner-boundary hashes back to their marker names."""
    known = {step_hash(tag): tag for tag in SUB_TAGS}
    return {known.get(value, "unknown"): value for value in sorted(set(step_hashes))}


def job_stages(job):
    """Sub-boundary segments of one closure, or None when markers are absent.

    A marker names the stage that starts at its timestamp; the stage runs to
    the next marker or to `job_end`. Time from `job_start` to the first marker
    is reported as `lead`. Markers exist only for servers built and run with
    `RUSTFS_FS_PROBE_SUB` set.
    """
    start = job.get("job_start")
    end = job.get("job_end")
    markers = sorted(job.get("subs") or (), key=lambda r: r["ts"])
    if not start or not end or not markers:
        return None
    known = {step_hash(tag): tag for tag in SUB_TAGS}
    stages = []
    lead = markers[0]["ts"] - start["ts"]
    if lead > 0:
        stages.append({"tag": "lead", "from_start_ms": 0.0,
                       "dur_ms": _millis(lead)})
    for index, marker in enumerate(markers):
        if marker["ts"] >= end["ts"]:
            break
        finish = end["ts"] if index + 1 == len(markers) else markers[index + 1]["ts"]
        finish = max(finish, marker["ts"])
        stages.append({
            "tag": known.get(marker["step"], "unknown"),
            "from_start_ms": (marker["ts"] - start["ts"]) / 1e6,
            "dur_ms": _millis(finish - marker["ts"]),
        })
    return stages or None


def job_calls(job):
    """Paired call-wrapper intervals of one closure, or None when absent.

    Pairs each start marker `X` with its explicit end marker `X_end`
    (emitted on every success path that reaches the marker; the `-v2`
    captures predate end markers and return None here). `dur_ms` is the
    start-marker-to-end-marker interval: the wrapped call plus any code
    between the markers, on one thread — a call-wrapper interval, not a
    kernel entry/exit measurement. `post_call_ms` is the residue from the
    end marker to the next marker or `job_end` (trailing cleanup or
    descheduling after the call returned). A call that failed via `?`
    before its end marker was recorded has no row rather than an estimated
    duration.
    """
    start = job.get("job_start")
    end = job.get("job_end")
    markers = sorted(job.get("subs") or (), key=lambda r: r["ts"])
    if not start or not end or not markers:
        return None
    known = {step_hash(tag): tag for tag in SUB_TAGS}
    pending = {}
    calls = []
    for index, marker in enumerate(markers):
        if marker["ts"] >= end["ts"]:
            break
        name = known.get(marker["step"], "unknown")
        if name.endswith("_end"):
            begin = pending.pop(name[:-4], None)
            if begin is None:
                continue
            finish = end["ts"] if index + 1 == len(markers) else markers[index + 1]["ts"]
            finish = max(finish, marker["ts"])
            calls.append({
                "tag": name[:-4],
                "from_start_ms": (begin - start["ts"]) / 1e6,
                "dur_ms": _millis(marker["ts"] - begin),
                "post_call_ms": _millis(finish - marker["ts"]),
            })
        else:
            pending.setdefault(name, marker["ts"])
    return calls or None


def classify_pool_tids(jobs):
    """Classify each executor tid's blocking pool: main | fsync | worker_loop | ambiguous | unknown.

    Primary rule is the call-site name of the tags the tid executed: every
    tag maps to exactly one spawn site (see `MAIN_POOL_TAGS` /
    `FSYNC_POOL_TAGS`) and the pools are disjoint runtimes, so one proven
    site fixes the tid. A tid that executed tags from *both* sets is
    reported as **ambiguous** — the evidence conflicts (e.g. an unknown
    tagging bug), and the classifier refuses to guess; no such tid was
    observed in the current captures, and operation rows for one would be
    labeled `ambiguous` rather than silently resolved. Fallbacks cover
    captures from before those tags existed (v2: only mkdir/rename-family
    tags existed and fsync sites were untagged):

    - a tid whose only job is an OP_NONE job spanning the capture (no
      `job_end`, or wall > 1 s) is labeled **worker_loop**: its *shape*
      matches the main- and fsync-runtime worker run-loops, but the label
      describes the shape, not a verified thread identity;
    - a never-tagged tid with >= 50 jobs is labeled **fsync**: fsync-pool
      call sites were untagged then, and with ~13 % of all jobs tagged the
      probability of a main-pool thread drawing zero tags over 50 jobs is
      < 1e-5 — a probability argument, not a proof;
    - everything else is **unknown** (too little evidence either way).
    """
    known = {step_hash(tag): tag for tag in STEP_TAGS}
    per_tid = {}
    for job in jobs.values():
        start = job.get("job_start")
        if start is None:
            continue
        tid = start["tid"]
        info = per_tid.setdefault(tid, {"jobs": 0, "main": False, "fsync": False,
                                        "worker_loop": True})
        info["jobs"] += 1
        submit = job.get("submit")
        end = job.get("job_end")
        tag = known.get(submit["step"]) if submit else None
        if tag is not None:
            info["main"] = info["main"] or tag in MAIN_POOL_TAGS
            info["fsync"] = info["fsync"] or tag in FSYNC_POOL_TAGS
            info["worker_loop"] = False
        elif submit is None or submit["a"] != OP_NONE:
            info["worker_loop"] = False
        elif end is not None and end["ts"] - start["ts"] <= 1_000_000_000:
            info["worker_loop"] = False
    pools = {}
    for tid, info in per_tid.items():
        if info["main"] and info["fsync"]:
            pools[tid] = "ambiguous"
        elif info["main"]:
            pools[tid] = "main"
        elif info["fsync"]:
            pools[tid] = "fsync"
        elif info["worker_loop"] and info["jobs"] == 1:
            pools[tid] = "worker_loop"
        elif info["jobs"] >= 50:
            pools[tid] = "fsync"
        else:
            pools[tid] = "unknown"
    return pools


def operation_job_rows(op_jobs, attempt_mono_ns, pools=None, poll_index=None):
    """Per-job detail rows for one operation's timeline."""
    known = {step_hash(tag): tag for tag in STEP_TAGS}
    ordered = sorted(op_jobs.values(),
                     key=lambda job: (job.get("submit") or job.get("job_start")
                                      or {}).get("ts", 0))
    rows = []
    for job in ordered:
        summary = job_intervals(job, poll_index)
        submit = job.get("submit")
        start = job.get("job_start")
        end = job.get("job_end")
        rows.append({
            "task_id": summary["task_id"],
            "step_hash": summary["step_hash"],
            "step_tag": (known.get(summary["step_hash"], "unknown")
                         if summary["step_hash"] else "untagged"),
            "submit_offset_ms": ((submit["ts"] - attempt_mono_ns) / 1e6
                                 if submit else None),
            "job_start_offset_ms": ((start["ts"] - attempt_mono_ns) / 1e6
                                    if start else None),
            "job_end_offset_ms": ((end["ts"] - attempt_mono_ns) / 1e6
                                  if end else None),
            "wall_ms": summary["start_to_end_ms"],
            "cpu_ms": summary["closure_cpu_ms"],
            "offcpu_ms": summary["closure_offcpu_ms"],
            "submit_tid": summary["submit_tid"],
            "job_tid": summary["job_tid"],
            "pool": (pools or {}).get(summary["job_tid"]),
            "stages": job_stages(job),
            "calls": job_calls(job),
            "missing": summary["missing"],
        })
    return rows


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
    pools = classify_pool_tids(jobs)
    pool_counts = {}
    for pool in pools.values():
        pool_counts[pool] = pool_counts.get(pool, 0) + 1
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
        "blocking_pools": {"counts": pool_counts,
                           "tid_pools": {str(tid): pool for tid, pool in sorted(pools.items())}},
        "polls": {"paired": len(polls), **poll_counters},
        "step_hashes": step_names(r["step"] for r in records
                                  if r["kind"] == KIND_SUBMIT and r["step"]),
        "sub_hashes": sub_names(r["step"] for r in records
                                if r["kind"] == KIND_SUB and r["step"]),
        "tiers": [],
        "limitations": [
            "Blocking-job boundaries are observed wall time on one host, not CPU time",
            "Blocking-job start-to-end is wall time inside the closure: job_start/job_end carry the thread CPU clock so captures with that extension report cpu and offcpu separately, but off-CPU time still mixes kernel waits, lock waits, and OS descheduling",
            "Inner-boundary stage records (sub) exist only for servers built and run with RUSTFS_FS_PROBE_SUB set; without them per-job stages are null",
            "Executor pool classification (main/fsync/worker_loop/ambiguous) is a documented heuristic over tag call-sites, job counts, and OP_NONE worker-loop shape: a worker_loop label describes a job shape rather than a verified thread identity, and the >= 50-job fallback is a probability argument, not a runtime fact",
            "T3' completion is recorded after task.run() stores the output and wakes the joiner; completion_to_poll_start_proxy_ms and the poll containing join_ready are diagnostic proxies, not directly measured scheduling latency",
            "Poll containment resolves the poll by OS thread id; a record outside every Dial9 poll is reported as missing",
            "Recording stops at ring capacity; total_seen beyond capacity is reported as dropped_records",
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
            "operation_jobs": operation_job_rows(op_jobs, attempt_mono, pools,
                                                 poll_index),
            "job_steps": step_names(job.get("submit", {}).get("step", 0)
                                    for job in op_jobs.values()
                                    if job.get("submit", {}).get("step")),
            "acknowledgements": reconstruct_acknowledgements(records, op, jobs),
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
