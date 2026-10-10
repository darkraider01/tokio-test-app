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

## Blocking-pool probe diagnostic

The span diagnostic showed a 40.6 ms await with zero polls of the request task
(the `commit_rx.await` boundary, called G2 below), and a ~19.8 ms directory-creation
await, but neither signal can separate blocking-thread dispatch delay, filesystem
execution, completion propagation, and runnable-to-resumption. This diagnostic
observes those boundaries directly with a narrowly scoped Tokio probe.

Provenance: `.repro/tokio-blocking-probe/` is a byte copy of the registry
`tokio-1.53.2` source (`~/.cargo/registry/src/*/tokio-1.53.2/`) plus the probe
module and five hook sites (`spawn_blocking_inner`, `BlockingTask::poll`,
the blocking-worker `run` loop, `JoinHandle::poll`, and `fs_probe` exports).
`.repro/rustfs-probe/` is a git worktree of the same pinned RustFS commit with
the probe's RustFS-side scopes and a `[patch.crates-io] tokio` entry appended to
its `Cargo.toml`. Neither the control checkout nor the control binary
(`.repro/rustfs/target/release/rustfs`, SHA-256
`dc577ce78a0dab71cef0072896c02487cd496fb8e6e3e35e5cfb81850cf01c9e`) is touched.

The probe is reproducible from this repository alone: the `.repro/` checkouts
are working copies, and [patches/](patches) preserves every diagnostic edit —
Tokio probe modules and hook sites, RustFS-side scopes, the `Cargo.toml` patch
entry, and the resulting `Cargo.lock` change. Four generations exist: the
unsuffixed patches reproduce the first `-v2` capture working copies (binary
`d2fd9f310ea91a50b6a00c4ff45ee80ea0827b56710a14b2f35bef415f22e882`, 2^19-record
ring, four step tags); the `-v2` patches add the commit-path step tags, the
`RUSTFS_FS_PROBE_SUB`-gated inner-boundary markers, per-job thread CPU
samples, quorum-send dependency fields, and the 2^20-record ring (binary
`fac3b2d1581bbe43d228d6ab6623e52b7481ef93bf28c1ca9d1837627eacb23e`); the
`-v3` patches add explicit `_end` markers for every sub-marked call on each
success path (so each call-wrapper interval is delimited instead of running
to closure end), pair them in the analyzer via `calls`, and extend the hash
vectors to all 27 tags (binary
`8fc0577be62b37ae80304c4c8760511f0248cec4870f534ff2647a9adcc65779`); the
`-v4` patches fix two probe-write issues found in review — ring writes use
raw-pointer arithmetic derived from `UnsafeCell::get()` instead of creating a
whole-buffer `&mut`, and a single admission gate turns `flush_to_env` into a
barrier over every probe writer (admission closes, admitted writers drain,
then the ring is read and written as **format version 2**: the unchanged
64-byte header plus explicit `stored`, `rejected_capacity`, and
`rejected_closed` counters). The `-v4` generation has **not** produced a
capture: no binary was rebuilt for it, so every existing capture keeps its
own hash and generation; its deterministic writer/flush concurrency tests
live in `fs-probe-ring-tests/` inside the Tokio `-v4` patch:

| Patch | Applies to | SHA-256 |
| :--- | :--- | :--- |
| `tokio-1.53.2-fs-probe.patch` | registry `tokio-1.53.2` source | `9a6d5341c8dd297a91110e3beb08e872f562f8846c7ae88a29f9bb24f0aa697b` |
| `rustfs-probe.patch` | RustFS `6b1554003ebf8f2037ffb7da9c9b906527e758da` | `673a787466ab7d8aaa0c556d1f43fb1e55ab1ef1b9930a77c12e80402b717534` |
| `tokio-1.53.2-fs-probe-v2.patch` | registry `tokio-1.53.2` source | `d8abefac86da16682e7e4f9a2545ba071ad3511803f7227de466d2f45e872443` |
| `rustfs-probe-v2.patch` | RustFS `6b1554003ebf8f2037ffb7da9c9b906527e758da` | `3a78be5322eed0c5f7f266af069ab059186020719909791defc9102ff1e35e9d` |
| `tokio-1.53.2-fs-probe-v3.patch` | registry `tokio-1.53.2` source | `342e86f00e6de3b40fac1460061315e1922b9c9ac55c2943948385b2a9750769` |
| `rustfs-probe-v3.patch` | RustFS `6b1554003ebf8f2037ffb7da9c9b906527e758da` | `c88196310af6b38649176948a5a3aa51e5d994405b79bbc7aeff9f27bdd353f3` |
| `tokio-1.53.2-fs-probe-v4.patch` | registry `tokio-1.53.2` source | `7bd0547894b9fc9bc94d9b2ae18b7f5c6b65b3ea4b598f8acd1fd7388eb66b84` |
| `rustfs-probe-v4.patch` | RustFS `6b1554003ebf8f2037ffb7da9c9b906527e758da` | `7845d7349205cb44f465c39dd853805db5c2dac0b6686aff98dd9e24b2e4de09` |

All eight were verified to apply to pristine sources (fresh registry copy and
fresh clone at the pinned revision); each `-v2`, `-v3`, and `-v4` patch was
additionally applied and its result compared byte-for-byte against the live
working copies.
To recreate the current probe working copies (fresh directories; `git apply`
works without a surrounding repository):

```sh
cp -a "$(echo ~/.cargo/registry/src/*/tokio-1.53.2)" .repro/tokio-blocking-probe
git -C .repro/tokio-blocking-probe apply --check -p1 \
  ../../experiments/rustfs/patches/tokio-1.53.2-fs-probe-v4.patch
git -C .repro/tokio-blocking-probe apply -p1 \
  ../../experiments/rustfs/patches/tokio-1.53.2-fs-probe-v4.patch
git -C .repro/rustfs worktree add --detach ../rustfs-probe \
  6b1554003ebf8f2037ffb7da9c9b906527e758da
git -C .repro/rustfs-probe apply --check \
  ../../experiments/rustfs/patches/rustfs-probe-v4.patch
git -C .repro/rustfs-probe apply \
  ../../experiments/rustfs/patches/rustfs-probe-v4.patch
```

(Substitute `-v3` to recreate the working copies that produced the
`8fc0577…` captures, or `-v2` for the `fac3b2d1…` captures.)

Build the probe binary (separate target directory, no `--locked` because the
patch entry rewrites `Cargo.lock`):

```sh
cd .repro/rustfs-probe
RUSTFLAGS="--cfg tokio_unstable --cfg rustfs_fs_probe -Aunexpected_cfgs" \
  cargo build --release --offline -p rustfs --bin rustfs --features dial9 --jobs 2
cd ../..
```

Run the probe condition and two controls (fresh output directories each time;
`--rustfs-source` points the manifest's provenance at the probe worktree for
runs of the probe binary):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-run-v2 \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe --repetitions 2 --duration 3 --concurrency 1 8 --rates
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-control --repetitions 2 --duration 3 \
  --concurrency 1 8 --rates
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-binary-off-v2 \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --repetitions 1 --duration 3 --concurrency 1 8 --rates
```

The probe condition writes `run-N/fs-probe.bin`; the runner fails if the dump is
absent after shutdown. The third run uses the probe binary with `RUSTFS_FS_PROBE_OUT`
unset, so the probe is compiled in but runtime-disabled — it bounds the compiled-in
overhead separately from record-writing overhead. Probe runs intentionally omit
`--request-traces`: Dial9 polls, the probe records, and the client tiers are
sufficient, and the object-hash join replaces the span join.

The stage/CPU/quorum-instrumented revision (`-v2` patches, binary
`fac3b2d1…`) and one bounded group-commit control were run as:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-run-v3 \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe --repetitions 2 --duration 3 --concurrency 1 8
RUSTFS_EXPERIMENTAL_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE=true \
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-groupcommit-run \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe --repetitions 1 --duration 3 --concurrency 1 8
```

Neither command passes `--rates`, so each also ran the r10/r25/r50 rate tiers
after c8; probe analysis reads the c1/c8 tiers for comparison. `run.py` sets
`RUSTFS_FS_PROBE_SUB=1` on every `--fs-probe` server, which enables the
inner-boundary stage markers; with the flag absent the analyzer reports
per-job stages as null.

The end-marker revision (`-v3` patches, binary `8fc0577b…`) was run the same
way:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-run-v4 \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe --repetitions 2 --duration 3 --concurrency 1 8
```

The first probe attempt (`.repro/rustfs-fsprobe-run`) predates the detached-commit
spawn propagation fix below: every `WAIT`/`SEND` record carried `op=0`, so its
waits could not be joined. It is superseded; `results/fs-probe-diagnostic.json`
is regenerated from the `-v2` runs only and stays tied to the first-generation
probe binary (`d2fd9f31…`). The stage/CPU/quorum revision produced
`results/fs-probe-v3-diagnostic.json` (binary `fac3b2d1…`, two repetitions) and
`results/fs-probe-groupcommit-diagnostic.json` (the bounded control above);
the end-marker revision produced `results/fs-probe-v4-diagnostic.json`
(binary `8fc0577b…`, two repetitions); each records the SHA-256 of its
`fs-probe.bin` and of the server binary.

Analyze one or more probe runs:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_probe.py \
  .repro/rustfs-fsprobe-run-v2/run-1 .repro/rustfs-fsprobe-run-v2/run-2 \
  --output experiments/rustfs/results/fs-probe-diagnostic.json
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_probe.py \
  .repro/rustfs-fsprobe-run-v3/run-1 .repro/rustfs-fsprobe-run-v3/run-2 \
  --output experiments/rustfs/results/fs-probe-v3-diagnostic.json
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_probe.py \
  .repro/rustfs-fsprobe-run-v4/run-1 .repro/rustfs-fsprobe-run-v4/run-2 \
  --output experiments/rustfs/results/fs-probe-v4-diagnostic.json
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_probe.py \
  .repro/rustfs-groupcommit-run/run-1 \
  --output experiments/rustfs/results/fs-probe-groupcommit-diagnostic.json
```

Probe design notes:

- Boundaries are observed, not inferred: `T0` submit (job pushed to the blocking
  pool), `T1` job start (closure begins on a blocking thread), `T2` job end,
  `T3'` completion (recorded after `task.run()` returns, so an upper bound on when
  the result became available), and `T5` join-ready (the awaiting task's poll
  observed `Ready`). `T4` runnability is inferred from the `SEND` record written
  immediately before the oneshot send; the resume point is the Dial9 poll whose
  interval contains the `T5` record on the same OS thread id. `T3'` records
  after the wake (the joiner can resume before the marker) and the containing
  poll can predate it or be an unrelated poll, so the derived job interval is
  named `completion_to_poll_start_proxy_ms` — a diagnostic proxy, not measured
  scheduling latency.
- The record layout is 40 bytes (`<BBHIIIQQQ`) behind a 64-byte header
  (`<8sIIQQQQIIQ`) carrying a `(monotonic, realtime)` flush pair, `total_seen`,
  and the pid. The `-v4` flush appends a 24-byte **format-version-2**
  extension — `<QQQ>` of `stored`, `rejected_capacity`, `rejected_closed` —
  after that header (records then start at offset 88); version-1 dumps (every
  capture in this repository) end the header at 64 bytes, and the reader
  reports `rejected_closed` as *missing* (`null`) for them rather than
  silently reusing an old field for a new meaning. A version-2 dump whose
  counters disagree (`stored > capacity`, `stored != min(total_seen,
  capacity)`, or `rejected_capacity != total_seen - stored`) is rejected as
  corrupt. The ring holds 2^20 records (~40 MiB) in the `-v2` revision
  (2^19 in the first generation); recording stops at
  capacity instead of wrapping (a wrap could race a descheduled writer's
  store), and `total_seen` beyond capacity is reported as `dropped_records`.
  Analysis never invents a value: a missing
  boundary is `null` with an explicit `missing` list.
- Probe timestamps are raw `CLOCK_MONOTONIC` nanoseconds via `libc`, the same
  clock Dial9 uses, and thread ids come from `SYS_gettid` like Dial9's events;
  probe↔realtime and Dial9↔realtime offsets are both reported and their delta is
  recorded per run.
- Operation context is an FNV-1a-64 hash of `bucket/object` carried in a Tokio
  task-local and propagated across the three commit-path spawns that must not
  lose it: the detached commit-owner task (`put_object`'s `tokio::spawn` around
  the commit closure — the first probe attempt recorded every wait with `op=0`
  until this spawn was wrapped), the rename tail-drain spawn, and the per-disk
  fanout task spawn. Step tags are FNV-1a-32 task-locals around blocking work:
  the `-v2` revision tags eleven commit-path sites (`mkdir`, `make_dir_all`,
  `rename`, `rename_no_owner`, `dest_meta_read`, `staged_meta_write`,
  `src_dir_sync`, `rename_data_dir`, `rename_meta`, `dst_dir_fsync`,
  `ancestor_fsync`; the first generation had the first four). Python mirrors
  of both hashes are pinned to vectors generated by the Rust implementations
  in `test_experiment.py` and cross-checked by a Rust unit test
  (`cargo test --lib --features full,test-util step_hashes_match` inside
  `.repro/tokio-blocking-probe`, with both probe cfgs in `RUSTFLAGS`).
- The `-v2` revision adds three more record encodings, all backward
  compatible (older dumps decode them as nulls):
  * `job_start`/`job_end` carry `CLOCK_THREAD_CPUTIME_ID` nanoseconds in the
    `a` field, so the analyzer splits closure wall time into `closure_cpu_ms`
    and `closure_offcpu_ms` (off-CPU still mixes kernel wait, lock wait, and
    descheduling; it does not separate them);
  * `kind=6` (`sub`) inner-boundary markers inside a running closure, emitted
    only when `RUSTFS_FS_PROBE_SUB` is set and `CURRENT_JOB != 0`. In the
    `-v2` patches a marker is a *start* marker: the analyzer's `stages` split
    the closure into `lead` plus named segments, each running to the next
    marker or closure end — call-wrapper intervals (call + trailing cleanup +
    any descheduling), not kernel entry/exit measurements of the wrapped
    call (e.g. `sub_dir_sync` starts before `sync_all()` and runs to closure
    end). The `-v3` patches (exercised by the `-v4` captures) add explicit
    `_end` markers on every success path, paired by the analyzer's `calls`
    helper into `dur_ms` (start marker → end marker) and `post_call_ms`
    (residue from the end marker to the next marker or closure end); a
    `?`-failure path that skips its `_end` marker yields no row rather than
    an estimated duration. Even with `_end` markers the pair still bounds a
    call-wrapper interval — code between the markers plus any descheduling
    of that thread — and kernel entry/exit tracing is still needed to claim
    exact syscall duration;
  * `SEND_OK`/`SEND_ERR` carry the quorum dependency snapshot: `step` =
    `results_seen`, `id` = `write_quorum`, `reserved2` = fanout `disk_count`.
- Blocking-pool membership is derived, not assumed: `classify_pool_tids`
  assigns each executor tid `main`, `fsync`, `worker_loop`, `ambiguous`, or
  `unknown` from the call-site names of the tags it executed (the two pools
  are disjoint runtimes; a tid carrying tags from both sets is reported as
  `ambiguous` rather than silently resolved), falling back to the worker-loop
  job shape and job counts for captures from before the pool-spanning tags
  existed; the fallbacks and their probability argument are documented at the
  function (a `worker_loop` label describes a job shape, not a verified
  thread identity) and surfaced as a limitation in the output.
- Behavior changes in the probe build, all confined to the diagnostic worktree:
  the `[patch.crates-io]` entry, the `rustfs_fs_probe`-gated scopes listed above,
  `propagate_op` wrappers at those three spawn sites (identity functions when
  the cfg is off), `SEND`/`WAIT` records around
  `commit_rx.await`, and `run_process` dropping the runtime before
  `flush_to_env()` (this drop is probe-build only). Race-freedom of the flush
  is **generation-dependent**, stated exactly:
  * `-v3` (every capture analyzed here) relied on that runtime drop/join
    order alone — writers were assumed joined before the dump — and wrote
    slots through a temporary whole-buffer `&mut`. No corruption was observed
    in those captures (record counts, header/counter consistency, cross-field
    spot checks all agree), but clean counts do not prove memory safety.
  * `-v4` makes the flush self-contained: `flush_to_env` first closes record
    admission (one atomic gate bit), then waits — with acquire/release
    ordering — until every already-admitted writer has released its slot,
    and only then reads the ring; a record attempt after close is refused and
    counted (`rejected_closed`), never raced. Slots are written through raw
    pointers derived directly from `UnsafeCell::get()` (no whole-buffer
    `&mut` aliasing), each slot claimed exactly once before its store. The
    drop-order fast path remains but is no longer load-bearing. These are
    the invariants exercised by the deterministic concurrency tests in
    `fs-probe-ring-tests/`; they have not yet been run in a live capture.
  `tokio::fs` is not replaced by custom `spawn_blocking` wrappers, and there
  are no locks or logging on the hot path: `-v4` writes behind one
  compare-and-exchange admission, a capacity check, and one fixed-size store
  (release-ordered guard drop); `-v3` used one relaxed atomic fetch-add, a
  capacity check, and one fixed-size store.
- The probe module compiles only when the `libc` feature is enabled alongside
  `rustfs_fs_probe`; Tokio's build-dependency units (which never enable `libc`)
  get the no-op stub, so build scripts record nothing.

## Kernel trace of the directory-sync wrappers

The probe alone bounds a call-wrapper interval but cannot say what the thread
did inside it: user-space code, on-CPU kernel execution, uninterruptible
sleep, and runnable-but-descheduled time all look the same off-CPU. This
diagnostic correlates the probe's `CLOCK_MONOTONIC` markers with the kernel's
own scheduler and syscall events for the same threads.

Method (smallest usable one on this host): raw ftrace through tracefs — no
tracing tool is installed, and `perf_event_paranoid=2` rules out perf/BPF
instrumentation. [ftrace.sh](ftrace.sh) arms a bounded capture: per-CPU
buffer 16387 KB, `overwrite=off` (an overflow stops capture visibly in the
stats instead of silently recycling), `trace_clock=mono` (CLOCK_MONOTONIC —
the probe's clock; whether the two timestamp streams may be subtracted is
then *validated* from metadata and matched enters by the analyzer, not
assumed), `sched_switch`/`sched_waking`/`sched_wakeup` filtered to the
rustfs comms,
and `sys_enter/exit_fsync` + `sys_enter/exit_fdatasync` unfiltered (analysis
restricts them to probe tids). Comm filters use the kernel filter's glob
`~ "rustfs*"` form: the regex-looking `rustfs.*` stores without error but
matches zero events on this host (verified), while `rustfs*` matches all
three comms (`rustfs`, `rustfs-worker`, `rustfs-fsync`; each ≤ 15 chars, so
`TASK_COMM_LEN` truncation cannot create a false match). tracefs control is
root-only on this host; the workload itself runs as the normal user.

Isolation: every command now operates only on a dedicated named tracefs
instance — `$TRACEFS/instances/rustfs-fstrace`
(`TRACE_INSTANCE`, `FTRACE_STATE_DIR` overridable) — whose ownership is
recorded in a marker file outside tracefs. The default tracer is never
cleared, configured, or disabled; a missing instance or missing ownership
marker fails loudly instead of falling back to it, a partially failed `arm`
leaves the owned instance with `tracing_on=0`, `collect` freezes the ring
before reading it and refuses to overwrite an existing capture file, `off`
is idempotent, and removing the instance is an explicit `destroy`
(refused while the ring holds uncollected data unless `--force`). The
collected `trace.settings` carries `instance=`/`instance_dir=` so every
future analysis can see which instance produced a capture.

Budget stated before running: one smoke repetition for tool validation, at
most two traced repetitions plus one probe-only repetition, and an honest
stop if no long wrapper appears. The first smoke iteration failed validation
(the sched filters were broken); the corrected smoke is the second
iteration — a tooling fix, disclosed rather than hidden. The traced run
reuses the v4 workload arguments and the v4 binary (`8fc0577b…`, no
rebuild; `--rates` with no values keeps the c1/c8 tiers only, as v4 did).
Five server runs total for this phase: 2 smoke, 2 traced, 1 probe-only.

Commands executed from the repository root (these produced the captures
analyzed below; `ftrace.sh` has since gained the instance isolation described
above, so the recorded capture is a **pre-isolation** one — `trace.settings`
says `tracefs=/sys/kernel/tracing` with no `instance=` line):

```sh
sudo experiments/rustfs/ftrace.sh arm
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fstrace-main \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 2 --duration 3 --concurrency 1 8 --rates
sudo experiments/rustfs/ftrace.sh collect .repro/rustfs-fstrace-main trace
sudo experiments/rustfs/ftrace.sh off
# probe-only perturbation reference, tracer off:
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fstrace-probeonly \
  --binary .repro/rustfs-probe/target/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 1 --duration 3 --concurrency 1 8 --rates
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_trace.py \
  .repro/rustfs-fstrace-main \
  --probe-reference .repro/rustfs-fstrace-probeonly \
  --output experiments/rustfs/results/fs-trace-diagnostic.json \
  --timelines 3
```

Fixture-tested vs. host-validated, stated separately:

* **Fixture-tested** (run by `test_experiment.py` against a fake tracefs
  tree, no root, run by anyone): `ftrace.sh`'s control flow — instance
  creation and ownership refusal, fail-closed partial `arm`, stop-before-read
  `collect`, refusal to overwrite captures, idempotent `off`, guarded
  `destroy`, and that the default tracer's files are never written. Fake-FS
  tests prove the script's logic only; they cannot prove kernel tracefs
  behaviour.
* **Host-validated on this host** (previous capture, pre-isolation script):
  filter semantics, `trace_clock=mono`, stop-on-full loss accounting, and the
  analyzer's clock/alignment validation against a real capture.
* **Host-validated (updated 2026-10-09, live isolated-instance workflow
  run):** instance creation under `$TRACEFS/instances/` with per-instance
  `trace_clock`/`options/overwrite`/`error_log` support, a smoke `collect`
  proving `instance=`/`instance_dir=`/`instance_owned=yes` metadata lands in
  `trace.settings` with `tracing_on=0` (stop-before-read), ownership
  refusals on all five commands for both a missing marker and a foreign
  marker (instance untouched, no files created), refusal of nonempty-buffer
  `destroy`/re-`arm` without `--force`, fail-closed partial `arm`
  (`TRACE_BUFFER_KB=not-a-number` → exit 1, `tracing_on=0`), repeatable
  `off`, `destroy` on an empty ring, and the default tracer byte-identical
  to its baseline before and after everything (`nop`, `tracing_on=0`,
  `buffer_size_kb=16387`, `overwrite=0`, zero enabled root events). No
  `--force` was ever used.

Optional instance cleanup (never required; the owned instance persists
between captures and `arm` reuses it):

```sh
sudo experiments/rustfs/ftrace.sh destroy          # refused if data uncollected
sudo experiments/rustfs/ftrace.sh destroy --force  # discards the owned ring
```

Semantics were pinned before instrumenting, each verified on this host
(kernel `6.19.10-300.fc44.x86_64`) rather than assumed:

- ftrace fractions print with 6 digits (µs) here; parsing right-pads to ns.
- The line-prefix `comm-tid` is the *previous* task for `sched_switch` and
  the *waker* for `sched_waking`/`sched_wakeup`; per-thread timelines are
  attributed from event content (`prev_pid`/`next_pid`/`pid=`), never from
  the prefix (foreign comms may contain spaces and are parsed from the
  right).
- `prev_state=R+` is the preempted-while-TASK_RUNNING spelling in this
  kernel's `events/sched/sched_switch/format` print fmt: runnable but not
  scheduled — never counted as blocked. `R` alone is a runnable switch-out
  without preemption; `S`, `D`, … are voluntary blocks of that state.
- `sched_waking` and `sched_wakeup` are **distinct events with distinct
  meaning** (verified against this kernel's
  `kernel/sched/core.c`: `try_to_wake_up` emits `trace_sched_waking` while
  wakeup processing is still in progress, and only `ttwu_do_wakeup` — after
  setting the task `TASK_RUNNING` — emits `trace_sched_wakeup`). The
  analyzer therefore models four scheduler states plus two unknowns:
  `blocked:*` ends at `sched_waking` (the wait is over the moment wakeup
  processing starts), the `sched_waking` → `sched_wakeup` interval is its
  own `wakeup_transition` category (neither blocked/D nor runnable — the
  task is not yet runnable when `sched_waking` fires), `runnable` starts at
  `sched_wakeup`, and `running` starts at the switch-in. A `sched_waking`
  never followed by a `sched_wakeup` (before the next event or the window
  edge) is `unknown_wake_incomplete`, a wake without an observed
  `sched_wakeup` still ends the block at the wakeup boundary when one is
  seen, `sched_wakeup_new` (first activation of a thread) is not captured
  and surfaces as `unknown_no_wake` on that first switch-in, and duplicate
  or spurious wake events for an already scheduled/runnable task are
  ignored. Equal-microsecond timestamps (1 µs trace precision) are
  ordered per thread by scheduler admissibility over the *recorded* line
  order, with each CPU's recorded sequence as a hard constraint: only
  the earliest unplaced event of each CPU may be placed next (one
  per-CPU buffer is read in order), so a recorded in → out pair inside
  one microsecond is never reversed — forcing `out` before `in` would
  invent running time for the whole interval after the tie. Recorded
  orders that are causally impossible (a cross-CPU switch-in printed
  before the wake it needs) are repaired; cross-CPU choices between
  different event kinds — where the file only shows ring-buffer merge
  order — fall back to the causal default (`out` < `waking` <
  `wakeup` < `in`) and are counted as genuinely ambiguous instead of
  presented as observed order; and when causality conflicts with the
  recorded sequences (no CPU's next event is admissible), the recorded
  order is kept and the uncertainty is counted rather than reordering
  observable events. All four counters are in `quality.trace_parse`
  (`equal_ts_ties`, `equal_ts_causal_repairs`, `equal_ts_ambiguous`,
  `equal_ts_unresolved`), so zero-length segments still cannot leak
  into the totals.
- Which syscall each sync tag wraps is derived from the capture by votes
  across all paired windows, not hard-coded: `sub_dir_sync` → `fsync`,
  `sub_fdatasync` and `sub_fsync_files` → `fdatasync` (the last despite its
  name — observed in every window of both captures).

Validation (everything is in the results JSON, with statuses and reasons —
never a bare success flag). Clock questions are answered from evidence:
compatibility is `validated` because the *selected* trace clock (the `[x]`
marker in `trace.settings`) is `mono` and every probe dump header reports
`clock_id=1` (both CLOCK_MONOTONIC, as written by `fs_probe.rs`); alignment
is `validated` from this capture: 27 700/27 700 paired sync wrappers contain
their own syscall-enter, worst per-run match rate 1.0, earliest enter
−0.301 µs within the 1 µs timestamp-quantization tolerance (offsets min
−0.3 µs, p50 +0.6 µs, max 1.6 ms — pre-entry user time inside the window,
not clock skew), against stated thresholds (≥30 samples, ≥0.95 match rate).
A missing/mismatched clock or failing offsets would instead read `failed` or
`insufficient_evidence` and withhold the cross-clock conclusions (zone×state
decompositions, clusters, union state totals) while retaining the raw
observations — no offset is invented and nothing is silently realigned. The
matching window alone proves nothing: it is a search slack, not clock
compatibility. The rest of the capture quality: the trace window covers
every probe marker; all probe tids appear in the sched stream (96/96 and
84/84); zero loss (1 534 228/1 534 228 entries, 0 overrun, 0 dropped, empty
`error_log`, 0 probe drops — a capture-quality finding, not proof of causal
attribution); every decomposition tiles its wrapper exactly (max
reconciliation 0.000 ms — an accounting consistency check, not independent
proof of the state classifications); unknown state spans total 0.000 ms in
the reported wrappers, and if present they are reported as `unknown*`,
never as zero.

Provenance (schema `fs-trace-diagnostic/v3`): `provenance.inputs` lists
every input this analysis consumed — `trace.raw`, `trace.settings`,
`trace.stats`, `trace.error_log`, the capture manifest, each
`run-*/fs-probe.bin`, and the probe-reference manifest and dumps — with
path, size, and the SHA-256 **computed by reading that file during the
run** (absent optional inputs stay `present: false` / `sha256: null`,
never invented); `provenance.parameters` records the long-wrapper
threshold, timeline count, and selection counts actually used;
`provenance.binary` keeps the manifest's *declared* `binary_sha256` apart
from the digest this analysis computed by hashing the file at the declared
path (and says `declared-only` when the file is not present, instead of
claiming a verification that did not happen).

Evidence-preservation limitation: the probe dumps analyzed here are
**format version 1**, produced by the `-v3` probe generation before the
writer-admission flush barrier and the raw-pointer ring-write fix existed.
No corruption was observed in them (record counts, counter consistency),
but clean counts do not prove memory safety; the corrected `-v4` generation
has passing deterministic tests but has never run a capture. The kernel
traces themselves still support syscall localization, subject to the
scheduler-state accounting corrected above — the underlying
filesystem/kernel wait cause inside those D-state intervals remains
unresolved either way.

Accounting: 118 marker-delimited wrapper observations ≥ 50 ms (73 run-1,
45 run-2) — a count of observations, **not** of independent syscalls or
waits. The captures nest: 12 run-1 `sub_fdatasync` observations sit inside
`sub_fsync_files` (both inside `sub_scan`), 4 do so in run-2, all within
one job on one tid; there are no partial overlaps. Summed durations
double-count that shared time. Per-tag totals (explicitly overlapping
across tags): `sub_dir_sync` 36 obs / 4 189.7 ms, `sub_rename` 18 /
1 774.9 ms, `sub_scan` 16 / 1 485.6 ms, `sub_fsync_files` 16 / 1 448.1 ms,
`sub_fdatasync` 16 / 1 447.9 ms, `sub_prep_write` 12 / 983.0 ms,
`sub_prep_open` 4 / 339.7 ms. Non-overlapping union per repetition and TID
(never merged across threads or repetitions; thread-time = per-thread wall
time inside the regions — neither client latency nor elapsed experiment
time): run-1 4 865.8 ms over 49 tids, run-2 3 907.1 ms over 36 tids =
8 772.9 ms, split 8 246.6 ms `blocked:D`, 495.9 ms `running` (scheduled
residency, not exact CPU execution — interrupts may run while the task is
current), 24.7 ms runnable, and 5.7 ms in the `sched_waking` →
`sched_wakeup` transition (neither blocked nor yet runnable — the state
machine above). The union of the traced syscall windows inside
those wrappers is 6 655.9 ms (6 136.7 ms `blocked:D`, 492.0 ms `running`,
21.9 ms runnable, 5.3 ms `sched_waking` → `sched_wakeup` transition); the
remaining ≈ 2.1 s of wrapper time is untraced kernel
operations (`sub_scan`, `sub_rename`, `sub_prep_*`). Per-wrapper
decompositions remain individual: 112/118 wrappers are >60% `blocked:D`,
7/118 are on-CPU-heavy (>30% `running` inside the syscall), one wrapper is
in both groups. The longest wrappers are one fsync almost end to end
(e.g. `sub_dir_sync` tid 2210377: 207.438 ms wrapper, 207.434 ms fsync,
enter 0.0005 ms after the start marker; job 49150, operation hash
2028158358628619772 step `ancestor_fsync`; its commit wait began 23.1 ms
before the wrapper and the `send_ok` closed it 207.8 ms after the start
with results_seen=3 / write_quorum=3 / disk_count=4). Cluster releases are
synchronized in sub-groups: run-1 exits in groups of 0.284 ms and 3.028 ms
spread; run-2 in groups of 0.101–4.765 ms with entries as tight as 0.000 ms.
Which filesystem event (if any) those releases associate with is **not**
established — no block/journal events were captured, and that deepening
needs a new stated budget. Association is not causation here or anywhere in
this section.

Identity: every wrapper row carries its repetition, blocking-job id,
operation hash, step tag, tag + occurrence + boundaries, and executor tid
(missing identities stay missing — never inferred from the tid). The
representative request-linked timelines additionally show commit-wait
begin/end, the send with its quorum snapshot, wrapper boundaries, syscall
entry/exit, scheduler-state intervals, and same-TID nesting. A job
*associated* with an operation or *overlapping* its commit wait is not
thereby response-critical (the JSON says `required_before_response:
not_established`), a 3-of-4 quorum count does not identify which disk
acknowledged, and `op_hash` is an FNV-1a hash that does not resolve to a
client object name here. D-state inside fsync/fdatasync locates blocked
time within the syscall; it does not identify journal, writeback, lock, or
device causes.

Perturbation reference: paired-call p50s are close between traced and
probe-only reps (`sub_dir_sync` 1.363 ms traced vs 1.297 ms probe-only,
Δ +0.066 ms), but the runs are short, the counts differ (2 reps vs 1), and
long-wrapper counts differ (36 vs 0) — a comparison, not a claim of
negligible tracing overhead.

Raw captures stay untracked: `.repro/rustfs-fstrace-*/` (including the
226 MB `trace.raw`, SHA-256 `e013ef440f6b9e20d4686bfd9ddf168942c63205683e0eef93d2baaf2726db4f`,
and the representative text timelines under `<capture>/timelines/`, named
per repetition, tag, tid, job, occurrence, and start timestamp so repeated
tags on one tid never overwrite each other); the compact results file is
`results/fs-trace-diagnostic.json`.

## Event-guided kernel-wait diagnostic (isolated instance, v4 probe)

This run validates the whole workflow on the current host and answers the
question the raw-ftrace section could not: *what is the thread waiting for*
inside the long directory-sync wrappers. All artifacts below are from this
phase and are deliberately **not committed** (`.repro/` is ignored; the
committed baseline `results/fs-trace-diagnostic.json` regenerates
byte-identically).

Host topology, verified before attributing anything to "disks": the four
RustFS volume directories are created under the output directory on **one
btrfs filesystem (subvolume `/home` of a single NVMe**, KIOXIA
KXG80ZNV512G, `/dev/nvme0n1p3`, disk `259,0`) — one physical device, never
four independent disks; `zram0` is swap only. Block events therefore carry
`dev 259,0` and are capture-filtered to it
(`BLOCK_DEV_FILTER='dev == 271581184'`, i.e. `259<<20`; a rejected filter
write fails the `arm` — verified with a bogus field). Stack sampling
(`/proc/<tid>/stack`) exists as a fallback facility but was not used: it is
root-only, gives point samples rather than continuous coverage, and races
with wakeup; the event set below was able to bound the wait instead.

### Build record for the `-v4` probe (compile-validated, then smoke-validated)

* Patches, byte-verified against the pristine pinned sources before
  building (tokio 1.53.2, rustfs `6b1554003ebf8f2037ffb7da9c9b906527e758da`):
  `patches/tokio-1.53.2-fs-probe-v4.patch` SHA-256
  `7bd0547894b9fc9bc94d9b2ae18b7f5c6b65b3ea4b598f8acd1fd7388eb66b84`,
  `patches/rustfs-probe-v4.patch` SHA-256
  `7845d7349205cb44f465c39dd853805db5c2dac0b6686aff98dd9e24b2e4de09`.
* Command (from `.repro/rustfs-probe`, log `.repro/rustfs-probe/build-v4.log`):

  ```sh
  CARGO_TARGET_DIR="$PWD/target-v4" \
  RUSTFLAGS="--cfg tokio_unstable --cfg rustfs_fs_probe -Aunexpected_cfgs" \
  cargo build --release --offline -p rustfs --bin rustfs --features dial9 --jobs 2
  ```

  `BUILD_EXIT=0` (83 m 50 s, rustc/cargo 1.99.0). New binary
  `.repro/rustfs-probe/target-v4/release/rustfs` SHA-256
  `e8c4f77375f7055825139900c408b5d1bdbc5e4d90e724db0f5671f7a3e95f3e`.
  The only copy of the v3 binary was preserved first
  (`.repro/preserved-binaries/…`, `8fc0577b…`) and still compares equal to
  `target/release/rustfs`.
* Warnings: 16 total — 15 **new** (missing-doc: 11 in `src/fs_probe.rs`,
  4 in `src/fs_probe_stub.rs`, secondary span `src/lib.rs:9:5` — all inside
  the probe modules the patch adds) vs 1 **pre-existing** (deprecated
  `Atomic::fetch_update` in `src/runtime/io/scheduled_io.rs:208`, a file the
  patch does not touch). No baseline build log exists, so classification is
  by patch-touched file path.
* The binary is called runtime-validated only after the smoke below
  succeeded (format-v2 dump parsed, counters consistent); compiling alone
  was never treated as validation.

### Predeclared diagnostic event set (why these eight)

Selected from host evidence (rate probes of 2–3 s in throwaway instances),
one question per event; the full table with per-event rationale lives in
[ftrace.sh](ftrace.sh) (`FS_BLOCK_EVENTS`) and the pre-run notes:

| event | question it answers |
|---|---|
| `btrfs/btrfs_transaction_commit` | transaction/journal coordination (generation, root incl. TREE_LOG) |
| `btrfs/btrfs_finish_ordered_extent` | ordered-data completion (ino/range/uptodate) |
| `btrfs/btrfs_reserve_ticket` | space-reservation wait (start_ns, flush mode, error) |
| `btrfs/btrfs_tree_lock` | lock coordination (diff_ns hold time, is_log_tree) |
| `writeback/folio_wait_writeback` | named waiter on writeback (`common_pid` = waiter, ino/index = object) |
| `block/block_bio_queue` | block arrival / queue side |
| `block/block_rq_issue` | request dispatch to the device |
| `block/block_rq_complete` | device completion / service end |

Excluded with reasons: ext4/jbd2 events (host filesystem is btrfs);
`block_rq_insert` (fires for ≈0.02 % of requests here — `bio_queue` is the
arrival marker); `btrfs_sync_file/_fs` (redundant with the captured
sys_enter/exit_fsync); `balance_dirty_pages` (dirty throttling acts on
write *producers*; **note: the flagship wait below is not explained by any
captured event, and an fsync thread is also a metadata-write producer, so
this is the first candidate to add if a further budget is approved — it was
not added here**); delayed-ref add/run (a sub-mechanism of commit).
Measured idle rates: `btrfs_tree_lock` ≈270/s, nvme `rq_issue`/`rq_complete`
≈900/s each, `bio_queue` ≈2.4k/s, `folio_wait_writeback` ≈5/s,
`finish_ordered_extent` ≈2/s, `transaction_commit` and `reserve_ticket` ≈0
when idle. Buffer raised to `TRACE_BUFFER_KB=32768` per CPU (18 GB RAM
host).

### Commands actually executed (this phase)

```sh
# smoke (1 rep × c8 × 1 s, fresh dir, new binary, no tracing):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-smoke-v4 \
  --binary .repro/rustfs-probe/target-v4/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 1 --duration 1 --concurrency 8 --rates
# arm the isolated instance with the diagnostic set (fail-closed):
echo '<password>' | sudo -S -p '' env TRACE_BUFFER_KB=32768 \
  experiments/rustfs/ftrace.sh arm
# traced diagnostic (2 reps × c1+c8 × 3 s):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fstrace-diag-v4 \
  --binary .repro/rustfs-probe/target-v4/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 2 --duration 3 --concurrency 1 8 --rates
echo '<password>' | sudo -S -p '' experiments/rustfs/ftrace.sh \
  collect .repro/rustfs-fstrace-diag-v4 trace
# probe-only reference (1 rep × c1+c8 × 3 s, tracer off):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fstrace-diagv4-probeonly \
  --binary .repro/rustfs-probe/target-v4/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 1 --duration 3 --concurrency 1 8 --rates
# analysis (writes the results JSON + per-wrapper timelines):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/fs_trace.py \
  .repro/rustfs-fstrace-diag-v4 \
  --probe-reference .repro/rustfs-fstrace-diagv4-probeonly \
  --output experiments/rustfs/results/fs-trace-diagnostic-v4.json \
  --timelines 3
```

Budget used vs. allowed: 1 smoke run (allowed 1 + at most one corrective
repeat; no corrective repeat was needed), 2 traced repetitions (allowed ≤2),
1 probe-only reference (allowed 1), no automatic extension. Server runs
total for this phase: 4. The capture was collected with
`entries-in-buffer = entries-written = 4 063 712` (zero overrun), empty
`trace.error_log`, probe `dropped_records=0` and `rejected_closed=0` in
both runs (`rejected_closed=0` is an observation that no post-close write
was rejected — it does **not** prove the admission barrier; a non-zero
value after gate close would be legitimate gate behaviour, not corruption).
Analyzer clock validation: compatibility/alignment/direct-subtraction all
`validated`, 4 063 712 lines parsed, 0 bad, unknown-state total 0 ms.

### Evidence rules for the `kernel_wait` section (fixed before reading results)

After a pilot window showed the device completes thousands of requests per
second (6–9k/s under c8), two rules keep the correlation honest — both are
encoded in `fs_trace.kernel_wait_section` and asserted by tests:

* **Isolation rule** (`isolated_temporal_candidate`): a completion class only provides
  an entry when at most one event of that class occurred *inside* the
  blocked segment, within 1 ms of the wake edge. This selects a candidate
  solely by temporal proximity and sparsity; it does not establish that the
  event released the wait, match a dependency to the blocked task, or exclude
  untraced causes. Under busy-device density any edge is within the threshold
  of *some* completion, so dense edges are reported as `proximity_summary`
  counts instead.
* **Waker ambiguity**: `waker_comm` is the task *current on that CPU* when
  the wake fired; for irq-context wakes it is an unrelated task (observed:
  `spotify`, `Compositor`, `ai.opencode.des`, `<idle>` appearing as
  wakers). Reported as an observed fact with this caveat, never as a
  causal attribution.

Levels reported per wrapper: `temporal_overlap` (counts only),
`shared_device_temporal` (device busy, not wait attribution),
`demonstrable_dependency` (the wrapper thread's own
`folio_wait_writeback` at/before its own switch-out — waiter and folio
named; establishes that the task entered that wait path, while whether it explains
the full blocked segment duration remains inferred), `isolated_temporal_candidate`
(isolation rule: candidate selected by temporal proximity and sparsity). None of them
establishes response-criticality, device latency from fsync duration, or a single
cause for the wait; those are listed under `limitations` in the results
JSON.

Main findings from `results/fs-trace-diagnostic-v4.json` (full detail,
including per-wrapper phase summaries, waker distributions, and
`fs_events_in_window`, in that file and the rendered
`.repro/rustfs-fstrace-diag-v4/timelines/`):

* **Long intervals reproduced** in both repetitions (406 + 88 wrappers ≥50
  ms; representative set: 6 wrappers, all `sub_dir_sync`/`sub_scan`).
* Flagship (run-1 `sub_dir_sync`, job 138, tid 3917276, one 275.842 ms
  fsync): **two giant back-to-back `blocked:D` segments — 69.709 ms and
  83.365 ms — fill the first 153 ms** (thread asleep 99.98 % of it), then a
  mostly-running phase (108.159 ms on-CPU: 11 575 sub-µs tree locks,
  2 235 bios submitted by the thread itself) ending with
  `btrfs_transaction_commit root=1 gen=7876` fired **by that thread 6 µs
  before the wrapper ends**. Zones: blocked:D 166.637 ms, running 108.159
  ms, runnable 0.632 ms, wakeup_transition 0.409 ms.
* The 83.365 ms segment's wake is preceded by an **isolated**
  `btrfs_finish_ordered_extent ino=4517992` at +153.116 ms (159 µs before
  the edge; itself preceded 24 µs earlier by
  `folio_wait_writeback bdi=btrfs-1 ino=4517992` from another thread),
  with `waker_comm=kworker/u48:0` — an isolated temporal candidate sequence
  compatible with writeback completion, but without an established producer/consumer
  dependency link to the blocked task. The 69.709 ms segment's wake-edge proximity is classified
  **dense/temporal only** (440 completions during it), and its
  `waker_comm=spotify` is an irq-attribution artifact.
* Across the six representative wrappers: 810 wake edges — 527 isolated
  (≈1 completion inside, classified or counted), 259 dense (temporal only),
  24 with no completion inside; wakers are dominated by unbound
  `kworker/u48:*` (writeback/ordered workqueue context) with peer
  `rustfs-worker`/`rustfs-fsync` and the irq-attribution artifacts.
* Run-1 vs run-2 variance (c8 p50 250 ms vs 48 ms under identical tracing)
  exceeds the traced-vs-probe-only p50 delta (`sub_dir_sync` +0.167 ms),
  so tracing perturbation cannot be separated from system-state variance
  at this repetition count — recorded as a limitation, not resolved.
* The two giant waits are **not explained by any captured event class**:
  not tree locks (hold p50 0.2 µs), not ordered-extent finishes (only one
  isolated candidate, at the *second* giant edge), not a transaction
  commit (the only in-window commit is at exit), not space reservation
  (`reserve_ticket` absent), not same-task folio waits (none with that
  tid). Unresolved; candidate hypotheses for a separately approved budget
  include `balance_dirty_pages` (dirty throttling during metadata generation)
  or bounded stack sampling of blocked threads (`/proc/<tid>/stack`), though
  neither a single event nor a stack sample alone establishes the full causal
  mechanism.

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

### Request-linked switch-out stacks (2026-10-10)

The reviewed evidence corrections were committed and pushed as `8c52d03`.
The next measurement used the existing v4 binary, one 1-second c8 smoke,
two traced repetitions of c1+c8 (3 seconds per tier), and one probe-only
reference. A separate, explicitly approved 3-second c8 repetition followed
because the first run exhausted its stack trigger before the long requests.
No binary rebuild or storage-behavior change was made.

The owned `rustfs-waitpath` instance used the existing event set plus
`writeback/balance_dirty_pages` and this sched-switch trigger:

```text
stacktrace:30000 if prev_state & 2 && prev_comm ~ "rustfs*"
```

The targeted repetition requested `stacktrace:100000` with the same filter.
Requested counts are not treated as exact hard ceilings: 30,321 and 101,080
stack events were recorded. The raw traces, event formats, installed trigger,
executed scripts, logs, and supplemental measurement metadata are preserved
under `.repro/rustfs-waitpath-main/` and `.repro/rustfs-waitpath-targeted/`.
Trace collection stopped recording, removed the trigger, and disabled the
owned instance; it was then destroyed without force. The default instance
was not used by these commands.

```sh
# Reproduction commands; this session generated ordinary JSON under /tmp
# first, then installed it at the result paths shown here:
python3 experiments/rustfs/fs_trace.py .repro/rustfs-waitpath-main \
  --timelines 3 --probe-reference .repro/rustfs-waitpath-probeonly \
  --output experiments/rustfs/results/fs-trace-waitpath-diagnostic.json
python3 experiments/rustfs/wait_path.py .repro/rustfs-waitpath-main \
  experiments/rustfs/results/fs-trace-waitpath-diagnostic.json \
  --output experiments/rustfs/results/wait-path-stacks.json
python3 experiments/rustfs/wait_path.py .repro/rustfs-waitpath-targeted \
  experiments/rustfs/results/fs-trace-waitpath-targeted.json \
  --output experiments/rustfs/results/wait-path-targeted-stacks.json
```

The main trace had 150 long-wrapper observations, 142 operation-linked;
none of those selected wrappers had a captured stack. Stack recording ended
at monotonic 31825.211249 s, before the first linked long wrapper at
31826.421623215 s. This missing coverage is retained, not filled in.

The targeted run succeeded on all 542 PUTs and yielded six operation-linked
directory-sync wrappers of 77.091–78.653 ms. All six have matched switch-out
stacks. Five contain approximately 40 ms D segments whose stacks include
`wait_for_commit` under `btrfs_wait_for_commit` or `btrfs_commit_transaction`.
Some also show approximately 24 ms in `wait_current_trans` through
`start_transaction` and `btrfs_attach_transaction_barrier`.
[Linux v6.19 transaction source](https://github.com/torvalds/linux/blob/v6.19/fs/btrfs/transaction.c)
shows these paths waiting for transaction progress/completion. This localizes
the wait path for these samples; it does not explain why the transaction
needed that time or identify the transaction object/releasing work.

For `c8/247.bin` (122.860 ms client attempt), jobs 36366/36367/36370 have
77.961–78.653 ms ancestor-fsync wrappers, with 40.063–40.078 ms transaction
wait segments. All finish before SEND_OK. Association and ordering do not
identify which disk acknowledgements were required by the 3-of-4 quorum.
In contrast, job 36368 for `c8/258.bin` has a 78.633 ms wrapper but SEND_OK
occurs 26.138 ms after its start: approximately 52.496 ms lies after send
and cannot have blocked that response. Its client attempt was 65.331 ms.

Both diagnostic traces report zero probe drops, post-close rejections,
kernel trace overruns/drops, and parser errors, with validated clock
alignment. No balance_dirty_pages event was recorded. That is a negative
observation in these short traced windows, not proof that throttling can
never contribute. Stack-trigger overhead perturbs timings. A stack is joined
only to one D segment beginning at most 50 microseconds before it; unmatched
stacks remain unassigned. Scheduled residency and nested observations retain
the ordinary analyzer's limitations. The response-critical disk dependency,
transaction identity, and reason for slow commit remain unresolved.

### Per-disk acknowledgement attribution and quorum reconstruction (2026-10-10)

The v5 generation establishes the acknowledgement contract and links per-disk
commit paths to their blocking jobs, measured waits, and client responses:

1. **Acknowledgement Contract**: Tracing `SetDisks::rename_data_owned_early_ack_with_fence`
   identified the coordinator join loop as the invariant owner. `results_seen` counts
   all disk task outcomes (including errors and panics); `success_count` increments
   strictly on successful disk mutations (`Ok(Ok(res))`). Quorum (`write_quorum = 3`)
   is satisfied when `success_count >= 3`, which emits `SEND_OK`. Subsequent disk
   completions are post-quorum tails.
2. **Identity & Record Format v3**: Format version 3 (40-byte fixed records) records:
   - `DISK` task-local context carrying `disk_index` (0-indexed logical fanout member)
     and `attempt` across all spawn boundaries and into blocking jobs.
   - `KIND_DISK_COMPLETE` (16): emitted on the worker thread when disk mutation returns.
   - `KIND_QUORUM_RESULT` (17): emitted by coordinator when popping each result from
     `tasks.join_next()`, capturing `status`, `success_before`, `success_after`,
     `quorum_satisfied`, `results_seen`, and `write_quorum`.
3. **Validation & Live Captures**:
   - Preserved `rustfs-v4` binary (`e8c4f773...`).
   - Rebuilt `rustfs-v5` (`9bda251b...`) with immutable patches `tokio-1.53.2-fs-probe-v5.patch`
     (`ccf4619e...`) and `rustfs-probe-v5.patch` (`1fc129b1...`).
   - One smoke repetition: `.repro/rustfs-fsprobe-smoke-v5` (c8, 1 s, 1 MiB, 189 PUTs ok,
     0 drops, format v3 validated, 212/212 quorum triggers identified).
   - Two diagnostic repetitions: `.repro/rustfs-waitpath-v5` (c8, 3 s, 1 MiB, 577 + 600 PUTs ok,
     0 drops, 0 capacity/closed rejections).
4. **Reproduction & Analysis Commands**:

```sh
# Smoke run (1 rep x c8 x 1 s):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-fsprobe-smoke-v5 \
  --binary .repro/rustfs-probe/target-v5/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 1 --duration 1 --concurrency 8 --rates

# Diagnostic capture (2 reps x c8 x 3 s):
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output .repro/rustfs-waitpath-v5 \
  --binary .repro/rustfs-probe/target-v5/release/rustfs \
  --rustfs-source .repro/rustfs-probe --fs-probe \
  --repetitions 2 --duration 3 --concurrency 8 --rates

# Full probe analysis:
python3 experiments/rustfs/fs_probe.py \
  .repro/rustfs-waitpath-v5/run-1 .repro/rustfs-waitpath-v5/run-2 \
  --output experiments/rustfs/results/fs-probe-v5-diagnostic.json
```

5. **Findings**:
   - `c8/221.bin` (109.23 ms client latency): Disk 3, Disk 1, and Disk 0 formed the
     observed prerequisite chain. Disk 1 had a 72.91 ms ancestor-fsync wrapper; Disk 0
     had a 71.88 ms ancestor-fsync wrapper that triggered quorum (success count 2 -> 3)
     and emitted SEND_OK at `9175340216478`. Disk 2 finished 0.084 ms after send as a
     post-quorum tail.
   - `c8/210.bin` (89.56 ms client latency): Disk 3, Disk 1, and Disk 0 completed before
     send; Disk 2 ran a 72.51 ms ancestor-fsync wrapper that finished +18.30 ms **after**
     SEND_OK, demonstrating a post-response tail that did not delay client response.
   - `c8/208.bin` (91.51 ms client latency): Disk 0 ran a 44.60 ms fsync wrapper that finished
     +16.60 ms **after** SEND_OK as a post-response tail.
   - Observed dependency distinguishes execution order from counterfactual necessity (under
     another schedule, Disk 2 could have substituted). Why the underlying Btrfs transaction
     takes ~40 ms to commit remains unresolved without kernel transaction tracing.
