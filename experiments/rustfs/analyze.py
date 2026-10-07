"""Summarize Dial9 poll wall time without attributing it to CPU or disk stages."""

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

from load import percentiles


def analyze(events, start_realtime_ns=None, end_realtime_ns=None):
    timed = sorted((e for e in events if "timestamp_ns" in e),
                   key=lambda e: e["timestamp_ns"])
    sync = next((e for e in timed if e.get("event") == "ClockSyncEvent"), None)
    if (start_realtime_ns is not None or end_realtime_ns is not None) and sync is None:
        raise ValueError("A ClockSyncEvent is required to select wall-clock tier boundaries")
    offset = sync["realtime_ns"] - sync["timestamp_ns"] if sync else 0
    counts = Counter()
    pending = {}
    durations = []
    by_location = defaultdict(list)
    queues = []
    unmatched_ends = 0
    overwritten_starts = 0
    timestamps = []
    for event in timed:
        ts = event["timestamp_ns"]
        epoch = ts + offset
        if start_realtime_ns is not None and epoch < start_realtime_ns:
            continue
        if end_realtime_ns is not None and epoch >= end_realtime_ns:
            continue
        kind = event.get("event")
        counts[kind] += 1
        timestamps.append(ts)
        worker = event.get("worker_id")
        if kind == "PollStartEvent":
            overwritten_starts += worker in pending
            pending[worker] = event
            queues.append(event["local_queue"])
        elif kind == "PollEndEvent":
            begin = pending.pop(worker, None)
            if begin is None:
                unmatched_ends += 1
                continue
            elapsed = (ts - begin["timestamp_ns"]) / 1e6
            durations.append(elapsed)
            by_location[begin["spawn_loc"]].append(elapsed)
    return {
        "event_counts": dict(counts),
        "span_ms": (timestamps[-1] - timestamps[0]) / 1e6 if timestamps else None,
        "poll_wall_ms": percentiles(durations), "paired_polls": len(durations),
        "unmatched_poll_ends": unmatched_ends, "unmatched_poll_starts": len(pending),
        "overwritten_poll_starts": overwritten_starts,
        "polls_ge_30_ms": sum(d >= 30 for d in durations),
        "local_queue_max": max(queues, default=None),
        "polls_with_local_queue": sum(q > 0 for q in queues),
        "spawn_locations": {loc: {"polls": len(ds), "total_wall_ms": sum(ds),
                                   "mean_wall_ms": sum(ds) / len(ds), "wall_ms": percentiles(ds)}
                            for loc, ds in sorted(by_location.items())},
        "limitations": ["Poll durations are whole-task wall time, not CPU or stage time",
                        "Queue depth does not measure request queue wait",
                        "No driver-turn or kernel-readiness timing is reconstructed",
                        "Tier clock mapping uses the first ClockSyncEvent; clock drift is uncorrected"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--start-realtime-ns", type=int)
    parser.add_argument("--end-realtime-ns", type=int)
    args = parser.parse_args()
    with args.trace.open() as stream:
        events = [json.loads(line) for line in stream if line.strip()]
    result = analyze(events, args.start_realtime_ns, args.end_realtime_ns)
    result["jsonl_sha256"] = hashlib.sha256(args.trace.read_bytes()).hexdigest()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
