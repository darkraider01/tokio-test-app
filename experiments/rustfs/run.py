"""Run bounded, repeated PUT sweeps against a local RustFS binary."""

import argparse
import hashlib
import json
import os
import platform
import signal
import socket
import subprocess
import tempfile
import time
import tomllib
from contextlib import nullcontext
from pathlib import Path

from load import S3Client, run_tier
from stage_metrics import metrics_receiver, read_histograms, summarize_tier


def write_json(path, value):
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def read_thread_names(pid):
    names = {}
    for thread in Path(f"/proc/{pid}/task").iterdir():
        try:
            names[thread.name] = (thread / "comm").read_text().strip()
        except FileNotFoundError:
            continue
    return names


def main():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=root / ".repro/rustfs/target/release/rustfs")
    parser.add_argument("--rustfs-source", type=Path, default=root / ".repro/rustfs")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("release", "dev"), default="release")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--rates", type=float, nargs="*", default=[10, 25, 50])
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--max-active", type=int, default=64)
    parser.add_argument("--no-telemetry", action="store_true")
    parser.add_argument("--stage-metrics", action="store_true")
    parser.add_argument("--request-traces", action="store_true")
    parser.add_argument("--fs-probe", action="store_true",
                        help="collect blocking-pool probe records (requires the probe build; "
                             "sets RUSTFS_FS_PROBE_OUT and verifies run-N/fs-probe.bin after shutdown")
    parser.add_argument("--strace", type=Path)
    parser.add_argument("--converter", type=Path, default=root / ".repro/dial9/target/debug/examples/trace_to_jsonl")
    args = parser.parse_args()
    if args.stage_metrics and args.request_traces:
        parser.error("Collect stdout metrics and spans separately to avoid interleaved exporter output")
    if (args.repetitions < 1 or not 0 < args.duration <= 30 or args.workers < 1
            or not 1 <= args.max_active <= 256
            or any(not 1 <= c <= 256 for c in args.concurrency)
            or any(not 0 < r <= 1000 for r in args.rates)):
        parser.error("Use positive parameters, duration <=30s, concurrency <=256 and rates <=1000/s")
    args.binary = args.binary.resolve()
    if not args.binary.is_file():
        parser.error(f"Binary not found: {args.binary}; build it first")
    if args.strace is not None:
        args.strace = args.strace.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    lock = tomllib.loads((args.rustfs_source / "Cargo.lock").read_text())
    names = {"tokio", "dial9", "dial9-tokio-telemetry", "dial9-trace-format"}
    payload = os.urandom(1024 * 1024)
    manifest = {
        "rustfs_commit": subprocess.check_output(
            ["git", "-C", str(args.rustfs_source), "rev-parse", "HEAD"], text=True).strip(),
        "rustfs_working_tree": subprocess.check_output(
            ["git", "-C", str(args.rustfs_source), "status", "--porcelain"], text=True),
        "dependencies": [p for p in lock["package"] if p["name"] in names],
        "binary": str(args.binary),
        "declared_build_profile": args.profile, "host": platform.platform(),
        "workers": args.workers, "payload_bytes": len(payload),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "repetitions": args.repetitions, "generation_seconds_per_tier": args.duration,
        "telemetry_enabled": not args.no_telemetry,
        "stage_metrics_enabled": args.stage_metrics,
        "request_traces_enabled": args.request_traces,
        "fs_probe_enabled": args.fs_probe,
        "temporary_volume_parent": str(args.output.resolve()),
        "limitations": ["HTTP 200 is not a read-back integrity check",
                        "New connection and client signing costs are included in attempt latency",
                        "Declared profile must be verified against the binary build command"],
    }
    with args.binary.open("rb") as binary:
        manifest["binary_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
    if args.strace:
        manifest["strace_version"] = subprocess.check_output([str(args.strace), "-V"], text=True)
        manifest["limitations"].append("ptrace syscall tracing changes timing; diagnostic run only")
    if not args.no_telemetry:
        manifest["decoder_commit"] = subprocess.check_output(
            ["git", "-C", str(root / ".repro/dial9"), "rev-parse", "HEAD"], text=True).strip()
    write_json(args.output / "manifest.json", manifest)

    for repetition in range(args.repetitions):
        out = args.output / f"run-{repetition + 1}"
        out.mkdir()
        with (tempfile.TemporaryDirectory(prefix="volumes-", dir=out.resolve()) as data,
              metrics_receiver(out / "otlp") if args.stage_metrics or args.request_traces else nullcontext(None) as metric_endpoint):
            volumes = [Path(data) / f"vol{i}" for i in range(4)]
            for volume in volumes:
                volume.mkdir()
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            env = os.environ.copy()
            env.update(RUSTFS_VOLUMES=" ".join(map(str, volumes)),
                       RUSTFS_ADDRESS=f"127.0.0.1:{port}", RUSTFS_ACCESS_KEY="rustfsadmin",
                       RUSTFS_SECRET_KEY="rustfsadmin", RUSTFS_UNSAFE_BYPASS_DISK_CHECK="true",
                       RUSTFS_CONSOLE_ENABLE="false", RUSTFS_RUNTIME_WORKER_THREADS=str(args.workers),
                       RUSTFS_RUNTIME_DIAL9_ENABLED=str(not args.no_telemetry).lower(),
                       RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=str((out / "telemetry").resolve()))
            if args.stage_metrics:
                env.update(RUSTFS_OBS_ENDPOINT="", RUSTFS_OBS_METRIC_ENDPOINT=metric_endpoint,
                           RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED="true",
                           RUSTFS_OBS_METRICS_EXPORT_ENABLED="true", RUSTFS_OBS_USE_STDOUT="true",
                           RUSTFS_OBS_METER_INTERVAL="1", RUSTFS_OBS_TRACES_EXPORT_ENABLED="false",
                           RUSTFS_OBS_LOGS_EXPORT_ENABLED="false", RUSTFS_OBS_PROFILING_EXPORT_ENABLED="false")
            if args.request_traces:
                env.update(RUSTFS_OBS_ENDPOINT="", RUSTFS_OBS_METRIC_ENDPOINT="",
                           RUSTFS_OBS_TRACE_ENDPOINT=metric_endpoint.removesuffix("metrics") + "traces",
                           RUSTFS_OBS_TRACES_EXPORT_ENABLED="true", RUSTFS_OBS_SAMPLE_RATIO="1.0",
                           RUSTFS_OBS_METRICS_EXPORT_ENABLED="false", RUSTFS_OBS_USE_STDOUT="true",
                           RUSTFS_OBS_LOGS_EXPORT_ENABLED="false", RUSTFS_OBS_PROFILING_EXPORT_ENABLED="false",
                           RUSTFS_OBS_LOGGER_LEVEL="info,rustfs_ecstore=debug",
                           RUST_LOG="info,rustfs_ecstore=debug",
                           OTEL_BSP_SCHEDULE_DELAY="1000", OTEL_BSP_MAX_QUEUE_SIZE="16384")
            if args.fs_probe:
                env.update(RUSTFS_FS_PROBE_OUT=str((out / "fs-probe.bin").resolve()))
            tiers = []
            with (out / "rustfs.log").open("w") as log:
                command = [str(args.binary)]
                if args.strace:
                    # -D keeps RustFS as our direct child so shutdown still targets the server.
                    command = [str(args.strace), "-D", "-f", "-ttt", "-T", "-yy",
                               "-o", str((out / "syscalls.txt").resolve()), "-e",
                               "trace=fdatasync,fsync,futex,epoll_wait,epoll_pwait", "--", *command]
                proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                try:
                    client = S3Client(port=port)
                    bucket = "tokio-experiment"
                    timeout = time.monotonic() + 60
                    while time.monotonic() < timeout:
                        if proc.poll() is not None:
                            raise RuntimeError(f"RustFS exited early; see {out / 'rustfs.log'}")
                        try:
                            if client.create_bucket(bucket):
                                break
                        except OSError:
                            pass
                        time.sleep(.2)
                    else:
                        raise RuntimeError("RustFS did not become ready within 60 seconds")
                    for i in range(5):
                        if client.put_object(bucket, f"warmup/{i}", payload)["status"] != 200:
                            raise RuntimeError("Warmup PUT failed")
                    if args.stage_metrics:
                        time.sleep(2)
                    if args.strace:
                        write_json(out / "threads-before.json", read_thread_names(proc.pid))
                    specifications = [(f"c{c}", c, None) for c in args.concurrency]
                    specifications += [(f"r{r:g}", None, r) for r in args.rates]
                    for name, concurrency, rate in specifications:
                        result = run_tier(client, bucket, payload, name, args.duration,
                                          concurrency, rate, args.max_active)
                        tiers.append(result)
                        write_json(out / "tiers.json", tiers)
                        print(json.dumps({k: v for k, v in result.items() if k != "requests"}), flush=True)
                        if args.stage_metrics:
                            time.sleep(2)
                        if args.strace:
                            write_json(out / f"threads-after-{name}.json", read_thread_names(proc.pid))
                finally:
                    if proc.poll() is None:
                        proc.send_signal(signal.SIGTERM)
                        try:
                            proc.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
                    manifest[f"run_{repetition + 1}_exit_code"] = proc.returncode
                    write_json(args.output / "manifest.json", manifest)
            if args.stage_metrics:
                points = read_histograms((out / "rustfs.log").read_text())
                summaries = [{"tier": t["tier"], "stages": summarize_tier(
                    points, t["start_realtime_ns"], t["end_realtime_ns"])} for t in tiers]
                if not points or any(not t["stages"] for t in summaries):
                    raise RuntimeError(f"Missing stage histogram coverage; see {out / 'rustfs.log'}")
                write_json(out / "stage-metrics.json", summaries)
            if args.request_traces and "Span #" not in (out / "rustfs.log").read_text():
                raise RuntimeError(f"No request spans exported; see {out / 'rustfs.log'}")
            if args.fs_probe and not (out / "fs-probe.bin").is_file():
                raise RuntimeError(f"No probe dump written at shutdown; see {out / 'rustfs.log'}")
            if not args.no_telemetry:
                traces = sorted((out / "telemetry").rglob("trace.*.bin"))
                if not traces:
                    raise RuntimeError(f"No Dial9 trace produced; see {out / 'rustfs.log'}")
                for trace in traces:
                    subprocess.run([str(args.converter), str(trace), str(trace.with_suffix(".jsonl"))], check=True)


if __name__ == "__main__":
    main()
