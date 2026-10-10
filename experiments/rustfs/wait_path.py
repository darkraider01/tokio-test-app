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


def verify_inputs(run_dir, diagnostic):
    """Verify that input files in run_dir match the diagnostic's recorded inputs.

    Validates trace.raw, probe dumps (fs-probe.bin), run membership, and client
    tier inputs (tiers.json). Relocation of identical bytes is permitted if hashes
    match recorded inputs. Legacy diagnostics lacking sufficient provenance are
    rejected with a clear error.
    """
    provenance = diagnostic.get("provenance")
    if not isinstance(provenance, dict) or "inputs" not in provenance or "trace" not in provenance:
        raise ValueError("legacy diagnostic lacks input provenance required for validated stack correlation")

    trace = run_dir / "trace.raw"
    if not trace.is_file():
        raise ValueError(f"trace.raw missing from {run_dir}")
    trace_hash = fs_trace.sha256_file(trace)
    recorded_trace = provenance.get("trace", {}).get("sha256")
    if not recorded_trace:
        for inp in provenance.get("inputs", []):
            if inp.get("role") == "ftrace raw capture" or inp.get("path", "").endswith("trace.raw"):
                recorded_trace = inp.get("sha256")
                break
    if not recorded_trace:
        raise ValueError("diagnostic provenance lacks recorded trace.raw SHA-256")
    if trace_hash != recorded_trace:
        raise ValueError(
            f"trace.raw SHA-256 {trace_hash} does not match diagnostic recorded input {recorded_trace}"
        )

    runs = diagnostic.get("runs")
    if not runs:
        raise ValueError("diagnostic contains no runs")

    verified_inputs = [
        {
            "role": "ftrace raw capture",
            "path": str(trace),
            "sha256": trace_hash,
            "matches_diagnostic": True,
            "status": "verified_match",
        }
    ]

    for run_entry in runs:
        run_name = run_entry.get("run")
        if not run_name:
            raise ValueError("diagnostic run entry missing 'run' identity")
        run_subdir = run_dir / run_name
        if not run_subdir.is_dir():
            raise ValueError(f"required run {run_name} missing from {run_dir}")

        probe_bin = run_subdir / "fs-probe.bin"
        if not probe_bin.is_file():
            raise ValueError(f"fs-probe.bin missing for {run_name} in {run_dir}")
        recorded_probe_hash = None
        for inp in provenance.get("inputs", []):
            p = inp.get("path", "")
            if p.endswith(f"{run_name}/fs-probe.bin") or (len(runs) == 1 and (inp.get("role") == "probe dump" or p.endswith("fs-probe.bin"))):
                recorded_probe_hash = inp.get("sha256")
                break
        if recorded_probe_hash is None:
            recorded_probe_hash = run_entry.get("probe_dump", {}).get("sha256")
        if not recorded_probe_hash:
            raise ValueError(f"diagnostic provenance lacks recorded fs-probe.bin SHA-256 for {run_name}")

        probe_hash = fs_trace.sha256_file(probe_bin)
        if probe_hash != recorded_probe_hash:
            raise ValueError(
                f"fs-probe.bin SHA-256 for {run_name} ({probe_hash}) does not match diagnostic input ({recorded_probe_hash})"
            )
        verified_inputs.append({
            "role": f"probe dump ({run_name})",
            "path": str(probe_bin),
            "sha256": probe_hash,
            "matches_diagnostic": True,
            "status": "verified_match",
        })

        tiers_path = run_subdir / "tiers.json"
        if not tiers_path.is_file():
            raise ValueError(f"client tier input {run_name}/tiers.json missing from {run_dir}")
        recorded_tiers_hash = None
        for inp in provenance.get("inputs", []):
            p = inp.get("path", "")
            if p.endswith(f"{run_name}/tiers.json") or (len(runs) == 1 and (inp.get("role") in ("client tiers", "tiers", "client_tiers") or p.endswith("tiers.json"))):
                recorded_tiers_hash = inp.get("sha256")
                break
        if recorded_tiers_hash is None:
            recorded_tiers_hash = (
                provenance.get("tier_hashes", {}).get(run_name)
                or run_entry.get("tiers_sha256")
                or diagnostic.get("tier_hashes", {}).get(run_name)
            )
        if not recorded_tiers_hash:
            raise ValueError(f"diagnostic provenance lacks recorded tiers.json SHA-256 for {run_name}")

        tiers_hash = fs_trace.sha256_file(tiers_path)
        if tiers_hash != recorded_tiers_hash:
            raise ValueError(
                f"client tier input {run_name}/tiers.json SHA-256 ({tiers_hash}) does not match diagnostic recorded input ({recorded_tiers_hash})"
            )
        verified_inputs.append({
            "role": f"client tiers ({run_name})",
            "path": str(tiers_path),
            "sha256": tiers_hash,
            "matches_diagnostic": True,
            "status": "verified_match",
        })

    missing_optional = []
    for opt_name in ["manifest.json", "measurement.json", "sched-switch-trigger", "sched-switch-format", "dirty-pages-format"]:
        opt_path = run_dir / opt_name
        if opt_path.is_file():
            opt_hash = fs_trace.sha256_file(opt_path)
            recorded_opt_hash = None
            for inp in provenance.get("inputs", []):
                p = inp.get("path", "")
                if p.endswith(opt_name) or inp.get("role") == opt_name:
                    recorded_opt_hash = inp.get("sha256")
                    break
            if recorded_opt_hash is not None and opt_hash != recorded_opt_hash:
                raise ValueError(
                    f"optional file {opt_name} SHA-256 ({opt_hash}) does not match diagnostic ({recorded_opt_hash})"
                )
            matches_diagnostic = (recorded_opt_hash is not None and opt_hash == recorded_opt_hash)
            verified_inputs.append({
                "role": opt_name,
                "path": str(opt_path),
                "sha256": opt_hash,
                "matches_diagnostic": matches_diagnostic,
                "status": "verified_match" if matches_diagnostic else "newly_hashed_unrecorded",
            })
        else:
            missing_optional.append(opt_name)

    return verified_inputs, missing_optional


def analyze(run_dir, diagnostic_path):
    diagnostic = json.loads(diagnostic_path.read_text())
    verified_inputs, missing_optional = verify_inputs(run_dir, diagnostic)
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
        probe_bin = run_dir / run["run"] / "fs-probe.bin"
        if probe_bin.exists():
            _, probe_records = fs_probe.read_probe(probe_bin)
            probe_jobs, _ = fs_probe.group_jobs(probe_records)
        else:
            probe_records = []
            probe_jobs = {}
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
            ack = fs_probe.reconstruct_acknowledgements(probe_records, identity["op_hash"], probe_jobs)
            rows.append({"identity": identity, "dur_ms": wrapper["dur_ms"],
                         "operation_link": wrapper["operation_link"],
                         "client_attempt": attempts.get(identity["op_hash"]),
                         "switch_out_stacks": matches,
                         "acknowledgements": ack,
                         "missing": [] if matches else ["switch_out_stack"]})
    rows.sort(key=lambda r: -max((m["blocked_until_waking_ms"]
                                 for m in r["switch_out_stacks"]), default=0))
    return {"schema": "rustfs-wait-path/v1",
            "provenance": {
                "diagnostic_path": str(diagnostic_path),
                "diagnostic_sha256": fs_trace.sha256_file(diagnostic_path),
                "run_dir": str(run_dir),
                "original_diagnostic_validation": {
                    "clock": diagnostic["quality"]["clock"],
                    "loss": diagnostic["quality"]["loss"],
                },
                "input_verification": {
                    "status": "verified",
                    "verified_matches": [i for i in verified_inputs if i["matches_diagnostic"]],
                    "newly_hashed_auxiliary_inputs": [i for i in verified_inputs if not i["matches_diagnostic"]],
                    "verified_inputs": verified_inputs,
                    "missing_optional_inputs": missing_optional,
                },
            },
            "inputs": {str(p): fs_trace.sha256_file(p)
                       for p in [trace, diagnostic_path]
                       + sorted(run_dir.glob("run-*/tiers.json"))
                       + sorted(run_dir.glob("run-*/fs-probe.bin"))
                       + [p for p in [run_dir / "manifest.json",
                                      run_dir / "measurement.json",
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
                "Input verification confirms file byte hashes match diagnostic records; it does not eliminate cross-clock perturbation from tracing triggers",
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
