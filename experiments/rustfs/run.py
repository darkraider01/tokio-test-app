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
from pathlib import Path

from load import S3Client, run_tier


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
    parser.add_argument("--converter", type=Path, default=root / ".repro/dial9/target/debug/examples/trace_to_jsonl")
    args = parser.parse_args()
    if (args.repetitions < 1 or not 0 < args.duration <= 30 or args.workers < 1
            or not 1 <= args.max_active <= 256
            or any(not 1 <= c <= 256 for c in args.concurrency)
            or any(not 0 < r <= 1000 for r in args.rates)):
        parser.error("Use positive parameters, duration <=30s, concurrency <=256 and rates <=1000/s")
    args.binary = args.binary.resolve()
    if not args.binary.is_file():
        parser.error(f"Binary not found: {args.binary}; build it first")
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
        "temporary_volume_parent": str(args.output.resolve()),
        "limitations": ["HTTP 200 is not a read-back integrity check",
                        "New connection and client signing costs are included in attempt latency",
                        "Declared profile must be verified against the binary build command"],
    }
    with args.binary.open("rb") as binary:
        manifest["binary_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
    if not args.no_telemetry:
        manifest["decoder_commit"] = subprocess.check_output(
            ["git", "-C", str(root / ".repro/dial9"), "rev-parse", "HEAD"], text=True).strip()
    write_json(args.output / "manifest.json", manifest)

    for repetition in range(args.repetitions):
        out = args.output / f"run-{repetition + 1}"
        out.mkdir()
        with tempfile.TemporaryDirectory(prefix="volumes-", dir=out.resolve()) as data:
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
            tiers = []
            with (out / "rustfs.log").open("w") as log:
                proc = subprocess.Popen([str(args.binary)], env=env, stdout=log, stderr=subprocess.STDOUT)
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
                    specifications = [(f"c{c}", c, None) for c in args.concurrency]
                    specifications += [(f"r{r:g}", None, r) for r in args.rates]
                    for name, concurrency, rate in specifications:
                        result = run_tier(client, bucket, payload, name, args.duration,
                                          concurrency, rate, args.max_active)
                        tiers.append(result)
                        write_json(out / "tiers.json", tiers)
                        print(json.dumps({k: v for k, v in result.items() if k != "requests"}), flush=True)
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
            if not args.no_telemetry:
                traces = sorted((out / "telemetry").rglob("trace.*.bin"))
                if not traces:
                    raise RuntimeError(f"No Dial9 trace produced; see {out / 'rustfs.log'}")
                for trace in traces:
                    subprocess.run([str(args.converter), str(trace), str(trace.with_suffix(".jsonl"))], check=True)


if __name__ == "__main__":
    main()
