"""Pair strace entries and returns; durations include tracing and scheduling effects."""

import re
import argparse
import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

from load import percentiles


def read_syscalls(text):
    pending = {}
    calls = []
    unmatched_returns = 0
    for line in text.splitlines():
        row = re.match(r"^(\d+)\s+(\d+\.\d+)\s+(.*)$", line)
        if not row:
            continue
        tid, timestamp, body = row.groups()
        timestamp = int(Decimal(timestamp) * 1_000_000_000)
        resumed = re.match(r"<\.\.\. (\w+) resumed>", body)
        if resumed:
            begin = pending.pop(tid, None)
            if begin is None or begin["syscall"] != resumed[1]:
                unmatched_returns += 1
                continue
            body = begin["text"] + body[resumed.end():]
        else:
            name = re.match(r"(\w+)\(", body)
            if not name:
                continue
            begin = {"tid": int(tid), "syscall": name[1], "start_realtime_ns": timestamp}
        if body.endswith("<unfinished ...>"):
            begin["text"] = body.removesuffix("<unfinished ...>")
            pending[tid] = begin
            continue
        duration = re.search(r"<([\d.]+)>$", body)
        if duration:
            elapsed_ns = int(Decimal(duration[1]) * 1_000_000_000)
            calls.append({"tid": begin["tid"], "syscall": begin["syscall"],
                          "start_realtime_ns": begin["start_realtime_ns"],
                          "end_realtime_ns": begin["start_realtime_ns"] + elapsed_ns,
                          "wall_ms": elapsed_ns / 1e6, "text": body})
    return calls, {"unmatched_returns": unmatched_returns, "unfinished_calls": len(pending)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    root = parser.parse_args().run_directory
    trace = root / "syscalls.txt"
    calls, unmatched = read_syscalls(trace.read_text())
    names = {}
    for path in sorted(root.glob("threads*.json")):
        names.update(json.loads(path.read_text()))
    workers = {}
    for path in sorted(root.rglob("trace.*.jsonl")):
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event["event"] == "WorkerParkEvent":
                    if event["worker_id"] in workers and workers[event["worker_id"]] != event["tid"]:
                        raise ValueError("Worker thread identity changed; time-dependent classification is required")
                    workers[event["worker_id"]] = event["tid"]
    if not workers:
        raise ValueError("Dial9 worker identities are required to classify runtime syscalls")
    result = {"manifest": json.loads((root.parent / "manifest.json").read_text()),
              "worker_tids": workers, "thread_names": names, "pairing": unmatched,
              "trace_sha256": hashlib.sha256(trace.read_bytes()).hexdigest(), "tiers": [],
              "limitations": ["ptrace strongly perturbs latency; not a performance baseline",
                              "Syscall wall time includes descheduling and tracing; not pure device or off-CPU time",
                              "Futex intervals are not necessarily blocking-pool queue wait",
                              "Request-linked directory syncs do not cover the full request pipeline"]}

    def summarize(items):
        groups = {}
        for call in items:
            groups.setdefault(call["syscall"], []).append(call["wall_ms"])
        return {name: {"calls": len(ds), "wall_ms": percentiles(ds)}
                for name, ds in sorted(groups.items())}

    for tier in json.loads((root / "tiers.json").read_text()):
        window = [c for c in calls if c["start_realtime_ns"] >= tier["start_realtime_ns"]
                  and c["end_realtime_ns"] <= tier["end_realtime_ns"]]
        storage = [c for c in window if c["syscall"] in ("fsync", "fdatasync")]
        own = [c for c in window if c["tid"] in workers.values()]
        attempts = [r for r in tier["requests"] if not r.get("client_shed")]
        slow = max(attempts, key=lambda r: r["attempt_to_completion_ms"], default=None)
        linked, driver = [], []
        if slow:
            start = slow["attempted_realtime_ns"]
            end = start + int(slow["attempt_to_completion_ms"] * 1e6)
            prefix = "/tokio-experiment/" + slow["key"]
            linked = [dict(c, start_after_attempt_ms=(c["start_realtime_ns"] - start) / 1e6)
                      for c in calls if c["syscall"] in ("fsync", "fdatasync")
                      and (prefix + ">" in c["text"] or prefix + "/" in c["text"])
                      and c["start_realtime_ns"] >= start and c["end_realtime_ns"] <= end]
            driver = [c for c in own if c["syscall"] == "epoll_wait"
                      and start <= c["start_realtime_ns"] <= end]
        result["tiers"].append({"tier": tier["tier"],
                               "load": {k: v for k, v in tier.items() if k != "requests"},
                               "syscalls": summarize(window), "runtime_worker_syscalls": summarize(own),
                               "storage_calls_by_thread_name": dict(Counter(
                                   names.get(str(c["tid"]), "unknown") for c in storage)),
                               "storage_calls_on_runtime_workers": sum(c["tid"] in workers.values() for c in storage),
                               "representative_slow_request": slow, "request_linked_syncs": linked,
                               "representative_runtime_epoll_entries": len(driver)})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
