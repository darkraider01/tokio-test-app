# Tokio / Dial9 investigation synthesis

Date: 2026-10-11. This report summarizes the pinned implementations and recorded
experiments in this repository; it does not describe every release of Tokio or
Dial9. The [technical report](dial9_tokio_observability_gap.md) contains the
synthetic experiments and source references. The [RustFS experiment notes](experiments/rustfs/README.md)
contain capture provenance, quality checks, and reproduction commands.

## What we learned

Task scheduling latency alone cannot explain end-to-end request latency. The
synthetic cases expose runtime transitions missing from the tested telemetry.
The RustFS case establishes a different boundary: a filesystem prerequisite on
a disk whose result triggered quorum contained an observed Btrfs wait path.
Explaining that request required application dependency markers and kernel
stacks as well as runtime timing.

We now have one real-workload example alongside the controlled scheduler cases.
There is still work to do before choosing production instrumentation, extending
the findings to other workloads, or explaining why the Btrfs commit was slow.

## What the controlled cases establish

| Case | Observed result | Missing explanation |
| --- | --- | --- |
| CPU-saturated I/O | Client-write-to-Tokio-readiness observation was about 30 ms; the representative stock schedule latency and Dial9 wake-to-poll delay were 3 and 4 microseconds. | Both intervals start too late to describe the pre-scheduling wait. Client write timestamps are not server kernel-readiness timestamps. |
| Wake coalescing | Under verified preconditions, five task wakes produced one task-correlated worker selection and four suppressed decisions in all 30 benchmark runs. | External wake/unpark events do not reveal the internal suppression decision or its reason. |
| Local queue / LIFO stranding | The 40 ms benchmark variant had a stranded-task median of 40.019 ms; the separate 50 ms representative trace reported 50.017 ms through stock schedule latency. | Stock timing does not explain placement. The tested Dial9 trace lacked a matching wake for that task and did not reconstruct its long interval; the reason for the absent wake remains unknown. |
| Worker notification | Internal probes distinguish unpark request, dispatch stage, and worker resume. | The tested stock unpark callback marks resume, leaving no paired notification-request timestamp or causal identity. A dispatch-stage marker can describe a no-op rather than an actual notification. |

These are separate boundaries. A long poll, an absent driver turn, an intentional
suppressed wake, and a task stranded in a LIFO slot should not receive the same
diagnostic explanation. The internal research probes demonstrate those
distinctions; their locking and recording overhead makes them unsuitable as a
production design without further work.

## Same-request RustFS evidence

The joint v5 capture executed one 3-second c8 tier with 1 MiB PUTs: 560 client
requests, all HTTP 200, with no client shedding. Four logical volume members
share the local filesystem/device; they are not four independent physical disks.
The diagnostic reports validated clock alignment and zero reported trace/probe
loss. Stack coverage is partial: 34 of 49 request-linked wrappers at the 25 ms
analysis threshold have switch-out stacks.

For `c8/82.bin`, client latency was 85.971 ms. Offsets below are relative to the
client attempt and rounded; the result files retain nanosecond timestamps.

| Offset | Evidence |
| --- | --- |
| +44.613 ms | Disk 2 result consumed: success count 0 to 1. |
| +51.996 to +80.687 ms | Disk 0 `dst_dir_fsync` job 15327 executes for 28.692 ms. The pinned source awaits it before that disk's mutation return. |
| +53.167 ms | A 27.408 ms blocked segment begins with a sampled `wait_for_commit` stack under `btrfs_commit_transaction` and `btrfs_sync_file`. |
| +84.821 ms | Disk 3 result consumed: success count 1 to 2. Its prerequisite job also has a sampled transaction-wait path. |
| +84.913 / +84.917 ms | Disk 0 mutation returns, then its result is consumed: success count 2 to 3, triggering quorum. |
| +84.920 ms | Coordinator records `SEND_OK` immediately before the channel send call. |
| +85.206 / +85.213 ms | Disk 1 mutation returns and its result is consumed after quorum. |
| +85.971 ms | Client completes successfully. |

The filesystem jobs run concurrently. Their durations cannot be added to client
latency. Disk 1's result was not counted toward this response; that does not prove
its concurrent filesystem work had no indirect effect on the other disks.
All four mutation returns precede client completion in this example. The
separate `c8/80.bin` example contains work after both SEND and client completion.

The stack establishes the encountered path at switch-out, not continuous
residence in that function throughout the blocked interval. Mutation return is
before metrics and enclosing task return. `SEND_OK` alone does not establish
channel delivery; the independently recorded HTTP 200 establishes client success.
An observed counted disk need not be counterfactually indispensable under a
different schedule.

Evidence: [trace diagnostic](experiments/rustfs/results/fs-trace-waitpath-v5-joint.json),
[correlated stacks](experiments/rustfs/results/wait-path-v5-joint-stacks.json), and
[acknowledgement summary](experiments/rustfs/results/wait-path-v5-joint.json).
The earlier v4 stacks and earlier v5 acknowledgements are separate captures; the
joint capture is what closes that previous evidence gap.

## What each layer contributed

| Layer | What it supplied in this investigation | What it did not establish |
| --- | --- | --- |
| Stock Tokio hooks | Task lifecycle/poll boundaries, schedule latency, thread park/resume callbacks. | Driver-service delay before scheduling, wake suppression reasons, queue placement, application quorum semantics, or kernel wait paths. |
| Tested Dial9 pipeline | Wake, poll, and park/unpark telemetry; wake-to-poll analysis; OS scheduling fields where available. | The missing scheduler decisions above or the request-to-disk dependency graph. The RustFS findings are not a new three-way stock/Dial9 benchmark. |
| Custom Tokio probes | Internal driver/scheduler decisions in synthetic cases; blocking-job submission and closure boundaries in RustFS. | Why a kernel transaction progressed slowly or which application disk results were required. Completion-to-poll timing retains its documented proxy semantics. |
| RustFS probes | Request context, step tags, paired call wrappers, logical disk/attempt identity, mutation returns, consumed results, and quorum count transitions. | Kernel transaction identity, release dependency, or device service time. |
| Kernel tracing | Scheduled/runnable/blocked decomposition and sampled switch-out wait paths. | Application acknowledgement semantics or the releasing entity from stack/proximity evidence alone. |

## What to work on next

The evidence points to a few possible changes. None is an agreed API yet, and
the comparison applies to the pinned versions tested here.

- **Tokio:** driver-turn/service boundaries; wake-selection/suppression decisions;
  notification-request versus resume correlation; task placement/steal context;
  precise blocking-job lifecycle boundaries. These require runtime-owned facts
  that an external consumer cannot recover reliably from existing events.
- **Dial9:** consume any agreed runtime signals and present separate wait phases;
  support request-to-blocking-job/dependency correlation and explicit missing
  evidence. First investigate the absent wake in the pinned LIFO example rather
  than assuming its cause or prescribing a fix.
- **Application instrumentation:** express request context, fanout members,
  awaited operations, consumed results, and quorum/response boundaries. Tokio
  cannot infer RustFS's three-of-four acknowledgement policy from task timing.
- **Kernel/filesystem tooling:** identify encountered wait paths and, in a
  separate investigation, transaction identity and releasing dependencies.
  A Tokio hook cannot by itself explain Btrfs commit progress.

A useful next discussion is to choose one diagnostic question and its smallest
missing signal. No broad Tokio patch, Dial9 redesign, or storage optimization
follows automatically from these experiments.

## Limits and reproduction

This is one short local workload on one storage setup. Instrumentation perturbs
timing; short controls do not establish negligible overhead. RustFS driver
readiness was not measured. Kernel timestamps have quantization and equal-time
ordering limitations described by the analyzer. A call-wrapper duration is not
a syscall-exact interval, and scheduled residency is not exact thread CPU time.
Neither the transaction identity nor its releasing work nor the reason for the
27 ms wait is established. No performance fix was attempted.

Use the synthetic commands in the technical report and the RustFS README's
joint-capture analysis commands. Preserve raw inputs and compare their recorded
hashes before regeneration. Raw captures live in ignored `.repro/` directories:
a clone of this repository alone does not include them. Published result hashes
identify evidence but do not replace access to those inputs. A future live run
must use a fresh capture directory and its own bounded authorization.
