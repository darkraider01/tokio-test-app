"""Inspect existing request spans from the pinned OpenTelemetry stdout exporter."""

import argparse
import datetime
import hashlib
import json
import re
from pathlib import Path


def read_spans(text):
    spans = []
    span = None
    attributes = False
    tail = False
    for line in text.splitlines():
        value = line.strip()
        if value.startswith("Span #"):
            span = {"attributes": {}}
            spans.append(span)
            attributes, tail = False, False
        elif span is not None:
            if line.startswith(("\tEvents:", "\tLinks:")):
                tail = True
            if tail:
                continue
            if line.startswith("\tAttributes:"):
                attributes = True
            elif attributes and value.startswith("->"):
                key, raw = value[2:].split(":", 1)
                raw = raw.strip()
                string = re.fullmatch(r'String\((?:Owned|Static)\((".*")\)\)', raw)
                integer = re.fullmatch(r"(?:I64|U64)\((-?\d+)\)", raw)
                decoded = json.loads(string[1]) if string else int(integer[1]) if integer else raw
                span["attributes"][key.strip()] = decoded
            elif line.startswith("\t") and not line.startswith("\t\t") and ":" in value:
                key, raw = value.split(":", 1)
                span[key.strip()] = raw.strip()
    required = ("Name", "TraceId", "SpanId", "ParentSpanId", "Start time", "End time")
    valid = [s for s in spans if all(k in s for k in required)]
    for span in valid:
        start = datetime.datetime.fromisoformat(span["Start time"])
        end = datetime.datetime.fromisoformat(span["End time"])
        span["wall_ms"] = (end - start).total_seconds() * 1000
    return valid, len(spans) - len(valid)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=Path)
    root = parser.parse_args().run_directory
    log = root / "rustfs.log"
    spans, incomplete = read_spans(log.read_text())
    tiers = json.loads((root / "tiers.json").read_text())
    result = {"manifest": json.loads((root.parent / "manifest.json").read_text()),
              "stdout_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
              "exported_spans": len(spans), "incomplete_stdout_spans": incomplete, "tiers": [],
              "limitations": ["Span lifetimes and busy/idle counters are elapsed time, not CPU time",
                              "Debug logging and stdout export perturb this diagnostic run",
                              "Parent links describe exported spans, not a complete critical path",
                              "Separate trace IDs are preserved; object matching does not repair context propagation"]}
    keep = {"code.file.path", "code.line.number", "busy_ns", "idle_ns", "object",
            "bucket", "request_id", "target", "thread.name", "thread.id"}
    for tier in tiers:
        attempts = [r for r in tier["requests"] if not r.get("client_shed")]
        slow = max(attempts, key=lambda r: r["attempt_to_completion_ms"])
        matches = [s for s in spans if s["attributes"].get("object") == slow["key"]]
        storage = [s for s in matches if s["Name"] == "put_object_with_old_current_size"]
        if not storage:
            raise ValueError(f"No storage span for request {slow['key']}")
        selected = max(storage, key=lambda s: s["wall_ms"])
        trace = [s for s in spans if s["TraceId"] == selected["TraceId"]]
        ids = {s["SpanId"] for s in trace}
        missing = {s["ParentSpanId"] for s in trace
                   if re.fullmatch(r"[0-9a-f]{16}", s.get("ParentSpanId", ""))
                   and s["ParentSpanId"] not in ids}
        compact = [{**s, "attributes": {k: v for k, v in s["attributes"].items() if k in keep}}
                   for s in sorted(trace, key=lambda s: s["Start time"])]
        result["tiers"].append({"tier": tier["tier"], "load": {k: v for k, v in tier.items() if k != "requests"},
                               "representative_request": slow, "trace_id": selected["TraceId"],
                               "http_request_id_match": any(
                                   s["attributes"].get("request_id") == slow.get("server_request_id")
                                   for s in trace if slow.get("server_request_id") is not None),
                               "other_object_linked_trace_ids": sorted({s["TraceId"] for s in matches} - {selected["TraceId"]}),
                               "missing_parent_span_ids": sorted(missing), "spans": compact})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
