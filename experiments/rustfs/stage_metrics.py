"""Capture existing OTLP exports and read the pinned stdout histogram format."""

import datetime
import hashlib
import json
import re
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer


@contextmanager
def metrics_receiver(directory):
    directory.mkdir()
    batches = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            size = int(self.headers.get("Content-Length", 0))
            if self.path not in ("/v1/metrics", "/v1/traces") or not 0 < size <= 16 * 1024 * 1024:
                self.send_error(400)
                return
            body = self.rfile.read(size)
            if len(body) != size:
                self.send_error(400)
                return
            filename = f"batch-{len(batches)}.bin"
            temporary = directory / f"{filename}.tmp"
            temporary.write_bytes(body)
            temporary.replace(directory / filename)
            batches.append({"file": filename, "received_realtime_ns": time.time_ns(),
                            "path": self.path,
                            "content_encoding": self.headers.get("Content-Encoding"),
                            "sha256": hashlib.sha256(body).hexdigest()})
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/metrics"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        temporary = directory / "index.json.tmp"
        temporary.write_text(json.dumps(batches, indent=2) + "\n")
        temporary.replace(directory / "index.json")


def read_histograms(text):
    points = []
    metric = None
    point = None
    metadata = {}
    for line in text.splitlines():
        line = line.strip()
        if line == "Metrics" or line.startswith("Metric #"):
            metric, point, metadata = None, None, {}
        elif line.startswith("Name") and ":" in line:
            metric = line.split(":", 1)[1].strip()
        elif line.startswith("Temporality"):
            metadata["temporality"] = line.split(":", 1)[1].strip()
        elif line.startswith("EndTime"):
            timestamp = datetime.datetime.fromisoformat(line.split(":", 1)[1].strip())
            metadata["end_realtime_ns"] = int(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp() * 1e9)
        elif line.startswith("DataPoint #"):
            point = {"metric": metric, "attributes": {}, **metadata}
            points.append(point)
        elif point is not None:
            match = re.fullmatch(r"(Count|Sum|Min|Max)\s*:\s*(\S+)", line)
            if match:
                key, value = match.groups()
                point[key.lower()] = int(value) if key == "Count" else float(value)
            elif line.startswith("->") and ":" in line:
                key, value = line[2:].split(":", 1)
                point["attributes"][key.strip()] = value.strip()
    return [p for p in points if p.get("metric") in
            ("rustfs_internal_stage_duration_ms", "rustfs_s3_put_object_stage_duration_ms")
            and "count" in p and "sum" in p and "stage" in p["attributes"]]


def summarize_tier(points, start_ns, end_ns):
    groups = {}
    for point in points:
        key = (point["metric"], tuple(sorted(point["attributes"].items())))
        groups.setdefault(key, []).append(point)
    stages = []
    for (metric, attributes), samples in sorted(groups.items()):
        samples.sort(key=lambda p: p["end_realtime_ns"])
        before = [p for p in samples if p["end_realtime_ns"] <= start_ns]
        after = [p for p in samples if p["end_realtime_ns"] >= end_ns]
        if not after:
            continue
        first, last = (before[-1] if before else None), after[0]
        if last.get("temporality") != "Cumulative":
            raise ValueError("Stage analysis requires cumulative histograms")
        if first and first.get("temporality") != "Cumulative":
            raise ValueError("Stage analysis requires cumulative histograms")
        count = last["count"] - (first["count"] if first else 0)
        total = last["sum"] - (first["sum"] if first else 0)
        if count < 0 or total < -1e-6:
            raise ValueError("Histogram reset within a tier")
        if count:
            stages.append({"metric": metric, "attributes": dict(attributes),
                           "observations": count, "total_wall_ms": total,
                           "mean_wall_ms": total / count,
                           "baseline_present": first is not None,
                           "export_start_ns": first["end_realtime_ns"] if first else None,
                           "export_end_ns": last["end_realtime_ns"]})
    return stages
