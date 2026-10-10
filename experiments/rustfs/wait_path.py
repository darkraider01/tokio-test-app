"""Join switch-out kernel stacks to request-linked long probe wrappers.

Stacks are taken by a sched_switch stacktrace trigger when prev_state & 2.
They identify the switch-out call path, not the eventual releasing entity.
Only one preceding blocked segment within 50 us may receive a stack; an
ambiguous match stays unassigned. The raw trace and ordinary analyzer JSON
remain separate inputs with their own hashes.
"""

import argparse
import json
from pathlib import Path

import fs_probe
import fs_trace


def match_stack(stack, segments, tolerance_ns=50_000):
    candidates = [(s, e) for state, s, e in segments
                  if state == "blocked:D" and s <= stack["ts"] <= e
                  and stack["ts"] - s <= tolerance_ns]
    return candidates[0] if len(candidates) == 1 else None


def analyze(run_dir, diagnostic_path):
    diagnostic = json.loads(diagnostic_path.read_text())
    if diagnostic["quality"]["clock"]["direct_subtraction"]["status"] != "validated":
        raise ValueError("wait-path correlation requires validated clock alignment")
    loss = diagnostic["quality"]["loss"]
    if loss["overrun_total"] or loss["dropped_total"] or not loss["entries_written_equal"]:
        raise ValueError("wait-path correlation requires a trace without reported loss")
    trace = run_dir / "trace.raw"
    timelines, _, stats = fs_trace.parse_trace(trace)
    stacks = stats.get("stack_traces", [])
    if stats["bad_lines"]:
        raise ValueError("unparsed trace lines must be resolved before stack correlation")
    by_tid = {}
    for stack in stacks:
        by_tid.setdefault(stack["tid"], []).append(stack)
    rows = []
    for run in diagnostic["runs"]:
        tiers = json.loads((run_dir / run["run"] / "tiers.json").read_text())
        attempts = {fs_probe.op_hash(tier["bucket"], request["key"]): request
                    for tier in tiers for request in tier["requests"]
                    if not request.get("client_shed")}
        for wrapper in run["long_wrappers"]:
            identity = wrapper["identity"]
            if identity["op_hash"] is None:
                continue
            tid, start, end = wrapper["tid"], wrapper["w0_ns"], wrapper["w1_ns"]
            # Keep full state-segment boundaries: clipping to wrapper start
            # could otherwise turn an older switch-out into a fresh one.
            segments = fs_trace.state_segments(timelines.get(tid, ()), 0, end)
            matches = []
            for stack in by_tid.get(tid, ()):
                if not start <= stack["ts"] <= end:
                    continue
                segment = match_stack(stack, segments)
                if segment is not None:
                    s, e = segment
                    matches.append({"switch_out_ns": s, "stack_ts_ns": stack["ts"],
                                    "stack_delay_us": (stack["ts"] - s) / 1000,
                                    "blocked_until_waking_ms": (e - s) / 1e6,
                                    "frames": stack["frames"]})
            matches.sort(key=lambda m: -m["blocked_until_waking_ms"])
            rows.append({"identity": identity, "dur_ms": wrapper["dur_ms"],
                         "operation_link": wrapper["operation_link"],
                         "client_attempt": attempts.get(identity["op_hash"]),
                         "switch_out_stacks": matches,
                         "missing": [] if matches else ["switch_out_stack"]})
    rows.sort(key=lambda r: -max((m["blocked_until_waking_ms"]
                                 for m in r["switch_out_stacks"]), default=0))
    return {"schema": "rustfs-wait-path/v1",
            "inputs": {str(p): fs_trace.sha256_file(p)
                       for p in [trace, diagnostic_path]
                       + sorted(run_dir.glob("run-*/tiers.json"))
                       + [p for p in [run_dir / "measurement.json",
                                      run_dir / "sched-switch-trigger",
                                      run_dir / "sched-switch-format",
                                      run_dir / "dirty-pages-format"] if p.exists()]},
            "stack_events": len(stacks), "trace_bad_lines": stats["bad_lines"],
            "match_tolerance_us": 50, "request_linked_wrappers": rows,
            "limitations": [
                "A switch-out stack names the encountered wait path, not its releasing entity",
                "Stack capture has a requested event limit; uncaptured stacks stay absent",
                "Only request-linked wrappers >= the ordinary analyzer threshold are selected",
                "Nested wrappers may share a stack; rows are observations, not additive time",
                "The final blocked segment can end at the wrapper boundary if wake is unobserved",
                "Operation hashes link unique workload object keys; they are not disk identifiers",
                "Response-criticality remains unestablished without the disk acknowledgement dependency",
            ]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("diagnostic", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.run_dir, args.diagnostic)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
