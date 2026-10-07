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
