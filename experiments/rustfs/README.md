# Local RustFS PUT investigation

This harness tests a real S3 PUT path on one node with four directories on the
same filesystem. It does not reproduce a production deployment or measure
kernel readiness, driver turns, disk service time, or per-request CPU time.
Python 3.11 or newer is required. No additional Python packages are needed.

## Build the recorded revisions

Run from the repository root. Clone only when the destination does not exist;
inspect an existing checkout before changing it.

```sh
git clone https://github.com/rustfs/rustfs.git .repro/rustfs
git -C .repro/rustfs checkout 6b1554003ebf8f2037ffb7da9c9b906527e758da
cd .repro/rustfs
RUSTFLAGS="--cfg tokio_unstable" cargo build --release --locked -p rustfs --bin rustfs --features dial9 --jobs 2
cd ../..
```

RustFS's lockfile selects registry Tokio 1.53.2 and Dial9 0.5.3. The runtime
does not use this harness's patched Tokio or cloned Dial9 Git dependency.
Record any profile overrides instead of treating them as the default release
profile. The initial experiment used `target/debug/rustfs` and cannot establish
release capacity.

The existing Dial9 checkout supplies the decoder, independently of the server:

```sh
cargo build --manifest-path .repro/dial9/dial9/Cargo.toml --example trace_to_jsonl --features analysis
```

## Run repeated tiers

Choose a new output directory each time. Stop competing builds before measuring.
The runner creates and cleans up only its own temporary volume directories and
shuts down the process it launches. Logs, traces, request records, and provenance
remain in the chosen output directory. It binds RustFS to loopback and uses
throwaway local test credentials. The disk-check bypass is for these temporary
directories on a shared device.
Temporary volumes live under the output directory, so its filesystem determines
the storage backing. The historical run used `/tmp`; a later host check found
that path backed by tmpfs. Do not infer physical disk latency from that run.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py --output .repro/rustfs-release-run --repetitions 3 --duration 3 --rates 100 200 300
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py --output .repro/rustfs-release-control --repetitions 3 --duration 3 --rates 100 200 300 --no-telemetry
```

Closed-loop concurrency defaults to 1, 2, 4, and 8. Arrival-rate tiers default
to 10, 25, and 50 requests/s with a cap of 64 active client operations. Use
`--rates` to choose rates spanning the capacity observed in the release baseline;
debug-build capacity is not a suitable assumption. Rates are bounded at 1000/s
and generation windows at 30 seconds. All attempts have socket timeouts.

Each repetition starts a fresh server and performs five warmup PUTs. Request
latency includes signing, connection setup, and the response body. The client
also records scheduled-to-completion latency and scheduled-to-launch lateness.
It drops arrivals when the client cap is full; `client_shed` is not server
backpressure. Generation-window completions and completions divided by elapsed
time including drain are distinct fields. HTTP 200 is not an integrity check.

`run-N/tiers.json` includes raw request records and epoch boundaries for each
tier. It is saved after each completed tier. Process exit codes and binary,
payload, source, and dependency provenance are recorded in `manifest.json`.

## Analyze trace windows

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/analyze.py .repro/rustfs-release-run/run-1/telemetry/rustfs-tokio/trace.0.jsonl
```

Use `--start-realtime-ns` and `--end-realtime-ns` with tier boundaries from
`tiers.json` to inspect one phase. Analyze generation and drain separately when
needed. The first trace `ClockSyncEvent` maps epochs to the trace clock; clock
drift is not corrected. Boundary-crossing polls are reported as unmatched and
excluded from duration distributions. Decode and inspect every rotated segment;
a single segment's summary must not be presented as whole-run coverage.

The analyzer sorts timestamps and pairs polls per worker. Reported durations
are whole-task **wall time**. A spawn location identifies a future, not the
expensive inner operation. Queue depths are sampled at polls, not request wait
durations. Neither long polls nor queue depth alone establish driver starvation.

RustFS already exposes opt-in PUT stage metrics through its existing OTLP
exporter (`RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED`), including
`erasure_encode_cpu`. Collect those or profiling samples before attributing
polls to encoding or assigning a numerical disk-wait budget. Stage aggregates
do not establish a request-linked latency decomposition by themselves.

## Optimized follow-up results

[release-comparison.json](results/release-comparison.json) preserves all 42 tier
summaries, both manifests, and hashes of the raw request records. The build
used the pinned source, locked dependencies, Rust 1.99.0, and the default
optimized release profile with `--cfg tokio_unstable`. Both conditions used
the same binary SHA-256. Each had three repetitions with three-second
generation windows, two runtime workers, and five warmup requests per fresh
server. Four temporary volume directories shared `/home`'s NVMe-backed
filesystem; the old `/tmp` debug run is not directly comparable.

Closed-loop throughput below is successful completions divided by elapsed
time including drain. Values are the median across repetitions, followed by
the minimum and maximum. These are short local measurements, not capacity
guarantees.

| Concurrency | Telemetry on: PUTs/s (range) | Telemetry off: PUTs/s (range) |
| --- | --- | --- |
| 1 | 66.20 (63.47–66.97) | 63.29 (60.84–63.94) |
| 2 | 119.10 (113.29–121.52) | 113.10 (111.89–116.95) |
| 4 | 175.26 (170.60–179.35) | 153.40 (147.71–162.58) |
| 8 | 190.19 (189.30–192.31) | 181.37 (157.74–189.07) |

Arrival-rate results below show the range of each repetition's successful
attempt-latency p50, not a pooled percentile. Client-shed counts are also
ranges across repetitions; the client cap was 64 operations.

| Target PUTs/s | On: p50 range (ms) | Off: p50 range (ms) | On: client-shed | Off: client-shed |
| --- | --- | --- | --- | --- |
| 100 | 15.66–16.69 | 16.35–16.75 | 0 | 0 |
| 200 | 23.04–270.69 | 99.26–321.83 | 5–92 | 18–78 |
| 300 | 306.15–320.05 | 297.56–338.16 | 280–317 | 282–331 |

All 9,401 telemetry-on and 8,888 telemetry-off attempts returned HTTP 200;
the client shed 1,009 and 1,102 scheduled arrivals respectively. All six
servers exited with code 0. Conditions ran sequentially (on, then off), not
in randomized order. Host/cache variability, different random payloads,
short windows, and client work remain confounders. The comparison does not
establish a reliable instrumentation-overhead percentage.

[release-trace-windows.json](results/release-trace-windows.json) contains
all 21 telemetry-on tier windows, including drain. Each window uses the
first ClockSync offset. Partial boundary polls are excluded and counted;
no overwritten poll starts were found. Polls at least 30 ms occurred in
every c8, r200, and r300 window, and none in c1, c2, c4, or r100. That is
load-associated whole-task wall time, not proof of CPU saturation, encoding
cost, disk latency, or delayed driver discovery. Request-linked stage and
driver timing remain unmeasured.

## Existing PUT stage metrics

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py --output .repro/rustfs-stage-run --repetitions 3 --duration 3 --concurrency 1 8 --rates --stage-metrics
```

`--stage-metrics` enables RustFS's existing PUT metrics, one-second cumulative
exports, and the pinned OpenTelemetry stdout exporter. A loopback HTTP receiver
preserves the compressed OTLP bodies and hashes under `run-N/otlp/`; other OTLP
signals and profiling export are disabled. No RustFS code or dependencies change.
The receiver stays alive through server shutdown. `stage-metrics.json` subtracts
the latest pre-tier cumulative snapshot from the first post-tier snapshot.
Two idle seconds after warmup and each tier allow exports to bracket the load.
The export boundaries are saved separately from the load boundaries.

These are padded observation windows, not request-linked stage timelines.
Stages can overlap, run on several disks, and include background metadata
operations. Means are per stage observation, not per request. Missing baseline
snapshots are flagged; histogram resets or non-cumulative exports are rejected.
Percentiles and maxima cannot be recovered by subtracting cumulative summaries.
Metrics/stdout export adds diagnostic overhead, so these results are kept
separate from the earlier performance comparison.

[stage-comparison.json](results/stage-comparison.json) preserves three fresh
server repetitions and six tiers: all 2,352 attempted PUTs returned HTTP 200.
The following ranges are each repetition's mean elapsed time per observation:

| Stage | Concurrency 1 (ms) | Concurrency 8 (ms) |
| --- | --- | --- |
| `app_store_put` | 13.22–13.95 | 32.45–43.76 |
| `set_disk_rename_quorum_wait` | 7.45–7.64 | 16.31–21.21 |
| `set_disk_encode` | 3.71–3.97 | 8.25–11.66 |
| `set_disk_rename_file_fdatasync` | 2.47–2.51 | 3.69–4.80 |

The path counter reports `write_single_block_non_inline`. Source dispatch at
`set_disk/ops/object.rs:3794` selects `IntegrityEncodeMode::SingleBlock`, which
calls `encode_small_direct` (`erasure/coding/encode.rs:744`). That function reads
the body, encodes synchronously, writes, and shuts down writers. It bypasses
`encode_block` and its separate `erasure_encode_cpu` timer. No such timer was
exported in these runs; this is missing coverage, not zero encoding cost.
`set_disk_encode` includes the whole operation and cannot be called CPU time.

The rename/quorum timer wraps an asynchronous wait for the commit receiver
(`set_disk/core/io_primitives.rs:3476`). The file timer wraps `File::sync_data`
(`disk/os.rs:1775`). These observations prioritize storage commit/quorum and
sync operations for profiling. They do not isolate device service time,
blocking-pool queue delay, admission wait, or driver starvation. The current
release binary lacks the optional Pyroscope feature. A local strace diagnostic
was collected separately after installing the approved tool under `.repro/`.

## Separate syscall diagnostic

Fedora's `strace-7.2-1.fc44.x86_64` RPM was downloaded with `dnf download`
and unpacked under `.repro/strace-package/`. `rpm -Kv` verified its signature
and payload digest. No system package was installed; sudo required a password.
To repeat with an available strace executable:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py --output .repro/rustfs-strace-run --repetitions 1 --duration 3 --concurrency 1 8 --rates --stage-metrics --strace .repro/strace-package/usr/bin/strace
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/syscalls.py .repro/rustfs-strace-run/run-1
```

The runner uses strace `-D` so RustFS remains its direct child and cleanup still
targets the server. It traces file syncs, futex, and epoll calls, with entry
timestamps, elapsed syscall time, and resolved file descriptors. It records
thread names around each tier. The analyzer pairs unfinished/resumed syscalls
and uses Dial9's worker-to-TID mapping; thread names alone are insufficient
because Tokio blocking threads also use `rustfs-worker`. Changing worker TIDs
or missing identities are rejected rather than misclassified.

[syscall-diagnostic.json](results/syscall-diagnostic.json) preserves one
diagnostic repetition: 80 successful concurrency-one requests and 107 successful
concurrency-eight requests. Throughput fell to 26.37 and 33.61 PUTs/s under
tracing, versus roughly 64–67 and 163–219 PUTs/s in the untraced stage runs.
The traced timings are not estimates of normal overhead or production latency.
All syscall entry/return pairs matched, the server exited with code 0, and its
temporary volumes were removed.

Neither tier had fsync/fdatasync on the two identified runtime worker TIDs.
Most sync calls ran on `rustfs-fsync` threads; the remaining calls ran on other
threads named `rustfs-worker`, consistent with blocking-thread use. This rules
out direct file-sync syscall occupancy of those two workers in this diagnostic
window; it does not rule out synchronous CPU work, queueing, or driver delays.

The slowest c8 request, `c8/75.bin`, took 345.26 ms under tracing. Four directory
syncs with that exact object path began 309.64–326.82 ms after its attempt and
lasted 0.86–1.36 ms each. The runtime workers entered epoll 19 times during the
request. Those observations establish partial request-linked storage activity,
not a full critical path. Temporary file syncs cannot all be linked by object
path, parallel sync times cannot be summed, and futex waits cannot be labeled
blocking-pool queue delay. No kernel socket-readiness timestamp was captured.

Existing stage evidence prioritizes storage commit and quorum waits. Explaining
the remainder of a representative untraced request requires request-linked
stage/queue timing or CPU/off-CPU samples. The current results do not establish
encoding CPU saturation or driver starvation.

## Existing request-span diagnostic

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py --output .repro/rustfs-request-span-run --repetitions 1 --duration 3 --concurrency 8 --rates --request-traces
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/request_spans.py .repro/rustfs-request-span-run/run-1
```

This separate mode enables existing OTLP spans with full sampling, a one-second
batch schedule, and an explicit child-process `RUST_LOG` filter. The local
receiver preserves `/v1/traces` exports; metrics and other OTLP signals are
disabled. Metrics and spans are collected separately because the pinned stdout
exporters print multiline output that can interleave. A first attempt without
the explicit filter exported no spans; empty captures now fail explicitly.
No RustFS source or build changes were needed.

[request-span-diagnostic.json](results/request-span-diagnostic.json) preserves
one repetition: 507 successful PUTs, 167.42 PUTs/s including drain, and 36,146
exported spans. The parser found no incomplete stdout span records. This is a
debug-logging/export diagnostic, not a production capacity or overhead estimate.

The slowest request, `c8/259.bin`, took 99.69 ms. Its server request ID matches
the exported HTTP span. The selected trace has 54 spans with all exported
parent references resolved. Its timeline includes:

| Span | Elapsed time (ms) |
| --- | --- |
| HTTP request | 92.851 |
| Storage entry (`crates/ecstore/src/store/mod.rs`) | 92.248 |
| Erasure-set PUT (`set_disk/ops/object.rs`) | 85.989 |
| Longest directory-creation span (`disk/os.rs:4417`) | 19.830 |

These spans nest and overlap; adding their durations would double-count time.
The erasure-set span exported 10.642 ms as `busy_ns` and 75.347 ms as `idle_ns`.
The directory span exported 0.720 ms busy and 19.110 ms idle. These counters
measure span entry/exit wall time, not CPU consumption or kernel off-CPU time.
The directory wrapper awaits filesystem operations backed by `tokio::fs`.
Its idle interval can include filesystem work, blocking-thread queueing,
executor scheduling, and diagnostic overhead; this trace cannot separate them.

An additional metadata trace has the same object key and a different trace ID.
It is preserved as an object-matched candidate, without inventing a parent link
or asserting that it is part of the selected critical path. Resolving all
exported parent references does not prove complete instrumentation coverage.
This request shows that most observed server time lies within storage work,
with a substantial await interval in directory creation. Request-linked queue
and filesystem timing are still needed to explain that await. Kernel readiness
and driver discovery remain unmeasured, so driver starvation is not established.

## Preserved preliminary evidence

- `results/debug-original.json`: the original single-run load summaries.
- `results/debug-provenance.json`: source/dependency provenance, trace hash,
  hashes of the original scratch scripts, and known measurement omissions.
- `results/debug-trace-summary.json`: independently recomputed trace statistics.

The original binary and decoded trace were copied locally to
`.repro/rustfs-original-evidence/`. Large traces are intentionally untracked.
The old generator did not preserve raw request records or phase boundaries;
new measurements cannot retroactively supply them. Historical summaries are
not outputs of the revised harness.

## Tests

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s experiments/rustfs -v
```
