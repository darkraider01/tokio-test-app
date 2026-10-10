# Tokio / Dial9 Runtime Observability Investigation

## Environment

- **Date:** 2026-10-05
- **OS:** Linux 6.18.33.2-2 x86_64 (WSL2 / Ubuntu 24.04 LTS) and Windows 11
- **Rust Toolchain:** `rustc 1.97.1 (b8e5c0e76 2026-03-24)`
- **Tokio Base Commit:** [`b2636752450484955e7ad334bac678424d51bc4a`](https://github.com/tokio-rs/tokio/tree/b2636752450484955e7ad334bac678424d51bc4a)
- **Dial9 Base Commit:** [`33b2d780628b42251047909ff2b88fdb97e3c28b`](https://github.com/dial9-rs/dial9/tree/33b2d780628b42251047909ff2b88fdb97e3c28b)
- **tokio-probe Patch:** `patches/tokio-ground-truth.patch`
  - Git object hash: `c1280c9bf27a3b77fcf3de59714be3c2a137e3a8`
  - SHA-256: `480B966EC16BFF334B6F477DCA59930CAFE5AFE42444C29F1D74A28DF082EB77`

---

## Methodology

This investigation establishes a strict 3-way empirical comparison across three observation tiers under identical controlled workloads:

1. **`tokio-probe` (Internal Tokio Ground Truth):**
   A temporary, minimal set of research probes instrumented directly inside Tokio scheduler internals (`driver.rs`, `scheduled_io.rs`, `harness.rs`, `worker.rs`, `idle.rs`, `park.rs`). It captures monotonic timestamps expressed in nanoseconds (generated via `Instant::elapsed().as_nanos()`), recorded into a mutex-protected in-memory event vector (`Mutex<Vec<ProbeEvent>>`). **It is used strictly as ground truth to expose internal state transitions and causal control flow, not as a proposed production design.**
   - *Measurement Characteristics:* Timestamps use nanosecond units (`as_nanos()`), which does not imply guaranteed nanosecond hardware clock resolution. The global `Mutex<Vec<_>>` lock introduces minor overhead that can perturb microsecond-scale scheduler timings; microsecond figures should therefore be treated as experimental timing measurements rather than zero-overhead production telemetry. Macroscopic effects (such as the ~30 ms Case E driver starvation gap) are orders of magnitude larger than any probe instrumentation overhead.
   - *Dispatch Measurement:* `WorkerUnparkDispatchBegin` records dispatch-stage entry after the Parker state swap: immediately before notification for `PARKED_CONDVAR` / `PARKED_DRIVER`, or the no-op dispatch path for `EMPTY` / `NOTIFIED`. A dispatch-stage event alone does not imply a notification primitive was invoked.
2. **`stock recorder` (Maximum Stock Tokio Observability):**
   Uses only public and unstable Tokio hooks currently available in upstream Tokio (`tokio_unstable`): `on_task_spawn`, `on_before_task_poll`, `on_after_task_poll`, `on_task_terminate`, `on_thread_park`, `on_thread_unpark`, and `TaskMeta::schedule_latency()`.
3. **`Dial9` (Actual Current Telemetry Consumer):**
   The real Dial9 telemetry pipeline compiled with `features = ["analysis"]`. Spawns tasks via `dial9_tokio_telemetry::spawn`, captures raw segments via `CapturingProcessor`, and decodes wire-format events: `WakeEvent`, `WorkerParkEvent`, `WorkerUnparkEvent` (with Linux `schedstat` / `sched_wait_ns` support where available in the kernel), `PollStartEvent`, and `PollEndEvent`, followed by Dial9's `compute_wake_to_poll_delays()`.
4. **`tokio-test-app` (Controlled Workload Generator):**
   Generates targeted concurrency patterns to isolate causal transitions: parked worker I/O, saturated worker I/O driver starvation, wake coalescing bursts, and work stealing with local-queue head-of-line blocking.

---

## Causal Model

We decompose the runtime execution lifecycle into four distinct causal boundaries rather than conflating them into a generic "unpark delay":

```
  External World / Driver                    Scheduler Submission & Decision                  Worker Resumption & Execution
+------------------------------------+     +----------------------------------+             +-------------------------------+
| A. External I/O Stimulus           |     | B. Scheduler Wake Decision       |             | C. Worker Wake Latency        |
|    -> Tokio Driver Observation     |     |                                  |             |                               |
|                                    |     | Runnable work submitted          |             | Tokio requests worker unpark  |
| External stimulus (WRITE_BEGIN)    |     |      ↓                           |             |      ↓                        |
|      ↓                             |     | Task state -> NOTIFIED           |             | Atomic state swap (CONDVAR)   |
| OS kernel socket buffer            |     |      ↓                           |             |      ↓                        |
|      ↓                             |     | Idle::worker_to_notify()         |             | OS unblocks thread (schedstat)|
| Tokio driver.turn()                |     |      ↓                           |             |      ↓                        |
|      ↓                             |     | [Unpark worker OR COALESCE]      |             | Worker leaves park loop       |
| IO_READINESS observed              |     +----------------------------------+             +-------------------------------+
+------------------------------------+                     |                                                |
                                                           v                                                v
                                           +--------------------------------------------------------------------------------+
                                           | D. Task Placement & Work Stealing                                              |
                                           |                                                                                |
                                           | Local queue vs Remote injection -> Work stealing -> LIFO slot -> Task Polled   |
                                           +--------------------------------------------------------------------------------+
```

### Boundary A: Client Write $\to$ Tokio Driver Observation
- **Interval:** Client write to Tokio readiness observation (`WRITE_BEGIN`/`WRITE_DONE` on the sending thread $\to$ Tokio `driver.turn()` / `IoReadinessObserved`).
- **Methodological Note:** `WRITE_BEGIN` and `WRITE_DONE` are client user-space timestamps, not server kernel-readiness timestamps. The reported metric is the client-write-to-Tokio-readiness-observation interval. Driver starvation is the explanation supported by the controlled setup (workers running non-yielding compute neither park nor advance `core.tick` to trigger `event_interval` maintenance), without claiming measured kernel-buffer residence or conclusively excluding network transit. Comparing the parked-worker control ($\Delta_{\text{io\_driver}} \approx 0.101\text{ ms}$) against worker CPU saturation ($\Delta_{\text{io\_driver}} \approx 29.844\text{ ms}$) demonstrates a ~30 ms pre-scheduling blind spot where the runtime fails to service the driver while workers are occupied.
- **Core Question:** Can current Tokio hooks or Dial9 detect when an application is waiting for Tokio to service the I/O driver while worker threads are saturated by CPU-bound tasks?

### Boundary B: Runnable Work $\to$ Wake / Coalesce Decision
- **Interval:** Work becomes runnable (`Harness::wake_by_val` / `Handle::schedule_task`) $\to$ `Idle::worker_to_notify()` evaluates whether to unpark an idle worker or suppress the wake.
- **Core Question:** When multiple tasks are awakened in rapid succession, can an observer distinguish intentional wake suppression from execution delays?

### Boundary C: Worker Notification $\to$ Worker Resume
- **Interval:** `Unparker::unpark()` initiates notification $\to$ target worker thread unblocks from kernel park and resumes user space.
- **Internal Stages Dissected:**
  - `WorkerUnparkRequested`: State transition / notification decision (atomic state swap to NOTIFIED).
  - `WorkerUnparkDispatchBegin`: Records entry into the dispatch stage after the Parker state swap. For `PARKED_CONDVAR` / `PARKED_DRIVER`, this occurs immediately before invoking `condvar.notify_one()` / `mio::Waker::wake()`. For `EMPTY` / `NOTIFIED`, it records the no-op dispatch path (`none_empty` / `none_already_notified`).
  - `WorkerResumed`: Worker has unblocked, re-acquired execution context, and returned to user-space scheduling loop.
- **Core Question:** Can an observer measure the true latency of waking a worker thread and correlate the wakeup with the task or event that requested it?

### Boundary D: Task Placement / Work Stealing $\to$ Poll
- **Interval:** Task placed on local queue or injection queue $\to$ Worker polls task.
- **Core Question:** Does `TaskMeta::schedule_latency()` explain *why* a task waited (e.g. stranded behind a CPU-heavy sibling on a local queue while another worker was idle)?

---

## Probe Locations

Every ground-truth probe in `patches/tokio-ground-truth.patch` is anchored to Tokio commit [`b2636752450484955e7ad334bac678424d51bc4a`](https://github.com/tokio-rs/tokio/tree/b2636752450484955e7ad334bac678424d51bc4a):

| Probe Event | Source File | Function | Tokio Permalink | Measurement | Diagnostic Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`IoReadinessObserved`** | `tokio/src/runtime/io/driver.rs` | `Driver::turn()` | [`driver.rs#L210-L225`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/io/driver.rs#L210-L225) | Timestamp, Mio token, ready flags | True instant Tokio's driver discovers I/O readiness |
| **`ResourceWakeDispatched`**| `tokio/src/runtime/io/scheduled_io.rs` | `ScheduledIo::wake()` | [`scheduled_io.rs#L270-L290`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/io/scheduled_io.rs#L270-L290) | Timestamp, readiness mask | Internal resource waker invocation |
| **`TaskWakeByVal`** / **`ByRef`** | `tokio/src/runtime/task/harness.rs` | `Harness::wake_by_val()` | [`harness.rs#L85-L105`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/task/harness.rs#L85-L105) | Timestamp, task ID, `submitted` | Task state transition to NOTIFIED |
| **`TaskScheduled`** | `tokio/src/runtime/scheduler/multi_thread/worker.rs` | `Handle::schedule_task()` | [`worker.rs#L1385-L1410`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L1385-L1410) | Timestamp, task ID, `is_local` | Task placed into local run queue vs remote injection queue |
| **`SchedulerWakeDecision`** | `tokio/src/runtime/scheduler/multi_thread/idle.rs` | `Idle::worker_to_notify()` | [`idle.rs#L51-L82`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/idle.rs#L51-L82) | Timestamp, `task_id`, caller, target worker, searching/unparked counts | Decision to notify worker or coalesce/suppress wake, causally linked to scheduling task ID via thread-local binding |
| **`WorkerUnparkRequested`** | `tokio/src/runtime/scheduler/multi_thread/park.rs` | `Unparker::unpark()` | [`park.rs#L305-L315`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/park.rs#L305-L315) | Timestamp, target worker, previous atomic state | Start of worker unpark request |
| **`WorkerUnparkDispatchBegin`**| `tokio/src/runtime/scheduler/multi_thread/park.rs` | `Unparker::unpark()` | [`park.rs#L316-L330`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/park.rs#L316-L330) | Timestamp, target worker, dispatch mechanism (`condvar`/`mio_waker`/`none_empty`/`none_already_notified`) | Dispatch-stage entry after the Parker state swap: before notification for parked states; no-op path for `EMPTY` / `NOTIFIED` |
| **`WorkerParkWaitBegin`** / **`End`** | `tokio/src/runtime/scheduler/multi_thread/park.rs` | `Parker::park()` | [`park.rs#L160-L240`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/park.rs#L160-L240) | Timestamp, worker ID, park kind (`driver`/`condvar`), state after wake | Entry and exit of OS blocking syscall |
| **`WorkerResumed`** | `tokio/src/runtime/scheduler/multi_thread/worker.rs` | `Context::run()` | [`worker.rs#L535-L545`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L535-L545) | Timestamp, worker ID | Worker exits park routine and resumes active execution loop |
| **`WorkerPollStart`** / **`End`** | `tokio/src/runtime/scheduler/multi_thread/worker.rs` | `Context::run_task()` & `lifo_slot` | [`worker.rs#L705-L720`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L705-L720) & [`L800-L815`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L800-L815) | Timestamp, worker ID, task ID | Wraps task poll invocation including in LIFO slot |
| **`ExternalIoStimulus`** | `tokio-test-app/src/ground_truth_probes.rs` | `record_external_stimulus()` | (Application harness) | Monotonic timestamp, phase (`WRITE_BEGIN`/`WRITE_DONE`) | Timestamp immediately surrounding external socket write |

---

## Experimental Results

All experiments were executed on Linux in release mode (`cargo run --release`). Statistical distributions represent **30 independent runs** per scenario.

### Case D: External I/O Stimulus While Workers Parked

- **Workload:** An async reader task is registered on a TCP stream and enters `read()`. Workers park on the driver. An external thread initiates a TCP write after workers are fully settled.
- **Interval Definitions:**
  - $\Delta_{\text{io\_driver}} = T(\text{tokio\_io\_readiness}) - T(\text{external\_write\_begin})$
  - $\Delta_{\text{schedule}} = T(\text{task\_scheduled}) - T(\text{tokio\_io\_readiness})$
  - $\Delta_{\text{poll}} = T(\text{task\_polled}) - T(\text{task\_scheduled})$
  - $\Delta_{\text{end\_to\_end}} = T(\text{task\_polled}) - T(\text{external\_write\_begin})$

#### Distribution (N=30 Runs, 30 Valid)

| Interval | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **$\Delta_{\text{io\_driver}}$** (External Stimulus $\to$ Tokio Driver Observation) | 0.076 ms | **0.101 ms** | 0.140 ms | 0.169 ms |
| **$\Delta_{\text{schedule}}$** (Tokio Driver Observation $\to$ Task Scheduled) | 0.003 ms | **0.003 ms** | 0.004 ms | 0.005 ms |
| **$\Delta_{\text{poll}}$** (Task Scheduled $\to$ Worker Poll Start) | 0.018 ms | **0.024 ms** | 0.035 ms | 0.053 ms |
| **$\Delta_{\text{end\_to\_end}}$** (External Stimulus $\to$ Worker Poll Start) | 0.099 ms | **0.128 ms** | 0.176 ms | 0.205 ms |

#### Representative Trace

```text
[INTERNAL GROUND TRUTH]
+  20.346 ms  EXTERNAL_IO_STIMULUS phase=WRITE_BEGIN details=Off-thread write to TCP socket
+  20.453 ms  EXTERNAL_IO_STIMULUS phase=WRITE_DONE details=TCP packet sent
+  20.459 ms  IO_READINESS token=99685252920832 ready=0x3 total_events=1
+  20.459 ms  RESOURCE_WAKE ready=0x3
+  20.469 ms  TASK_WAKE_VAL task_id=23 submitted=true
+  20.471 ms  TASK_SCHEDULED task_id=23 is_local=true
+  20.475 ms  WORKER_PARK_WAIT_END worker=0 kind=driver state_after=no_notification
+  20.476 ms  WORKER_RESUMED worker=0
+  20.497 ms  WORKER_POLL_START worker=0 task_id=23

[STOCK TOKIO OBSERVABILITY]
+  20.476 ms  on_thread_unpark
+  20.497 ms  on_before_task_poll task_id=23 schedule_latency=0.026ms

[DIAL9 OBSERVABILITY]
+  DIAL9 WORKER_UNPARK: worker=0 tid=7606 sched_wait=None (unsupported/unsampled)
+  DIAL9 POLL_START: worker=0 task_id=23 loc=src/main.rs:589:25
+  DIAL9 COMPUTED WAKE-TO-POLL DELAY: 0.026ms
```

---

### Case E: External I/O Stimulus Under Scheduler / CPU Saturation

- **Workload & Synchronization:** An async reader task is registered on a TCP stream. All $N$ workers ($N=2$, canonical) are occupied with non-yielding CPU compute loops (spin loops) for 40 ms. Each compute task increments an atomic barrier (`compute_started.fetch_add(1, SeqCst)`); only after all $N$ workers are confirmed executing the spin loop does an external writer thread initiate a 10 ms delay and send a TCP packet at $t \approx 10.36\text{ ms}$.
- **Key Observation:** Because all workers are executing non-yielding compute loops, **no worker turns the I/O driver**. After the external write, Tokio does not service the driver and observe socket readiness until roughly 30 ms later while all runtime workers remain occupied.

#### Distribution (N=30 Runs, 30 Valid)

| Interval | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **$\Delta_{\text{io\_driver}}$ (Driver Service Delay: Stimulus $\to$ Observation)** | 29.787 ms | **29.844 ms** | 29.871 ms | 29.879 ms |
| **$\Delta_{\text{schedule}}$ (Tokio Observation $\to$ Task Scheduled)** | 0.001 ms | **0.002 ms** | 0.003 ms | 0.003 ms |
| **$\Delta_{\text{poll}}$ (Task Scheduled $\to$ Worker Poll Start)** | 0.004 ms | **0.007 ms** | 0.012 ms | 0.028 ms |
| **$\Delta_{\text{end\_to\_end}}$ (Total Physical Real Latency)** | 29.795 ms | **29.855 ms** | 29.881 ms | 29.889 ms |

#### The Critical Observability Inversion

```text
Timeline of Real Events vs Telemetry Views in Case E:

Time        Physical Reality                       Stock Tokio View                Dial9 View
---------   --------------------------------       ----------------------------    ----------------------------
t=10.36ms   External TCP Write Sent (WRITE_BEGIN)
            [External write unobserved by Tokio]   (completely invisible)          (completely invisible)
            ... 29.82 ms driver service delay ...  (completely invisible)          (completely invisible)
t=40.18ms   Compute ends; Driver::turn() called    (no event emitted)              (no event emitted)
t=40.19ms   Reader Task Scheduled                  set_scheduled_at(40.19ms)       WakeEvent captured
t=40.19ms   Reader Task Polled                     schedule_latency: 0.003 ms!     wake_to_poll: 0.004 ms!
```

- **Reported `TaskMeta::schedule_latency()`:** **$0.003\text{ ms (3 \mu s)}$**
- **Reported Dial9 `wake_to_poll_delay`:** **$0.004\text{ ms (4 \mu s)}$**
- **Actual Application-Experienced Delay:** **$29.830\text{ ms}$** (representative run) / **$29.855\text{ ms}$** (benchmark p50)

**Result:** Both stock Tokio and Dial9 report sub-5-microsecond schedule latency, hiding **over 99.9% of the real latency**. The delay occurred entirely before Tokio serviced the I/O driver, rendering the driver service starvation completely invisible to application telemetry.

---

### Adversarial Case 1: Wake Coalescing & Wake Suppression

- **Workload & Precondition Synchronization:**
  - Worker 0 is occupied in a controlled non-yielding CPU compute loop under an explicit atomic flag (`compute_stop`), preventing premature idle transitions.
  - 5 tasks waiting on `Notify` instances are polled and confirmed pending via an atomic registration counter (`std::future::poll_fn`) before notifications are dispatched.
  - Worker 1 is parked and available.
  - Once setup is confirmed, an off-runtime thread fires all 5 `Notify::notify_one()` calls in tight succession.
- **Causal Correlation Accounting:**
  - `Handle::schedule_task()` binds the current task ID to a thread-local during scheduling, propagating `task_id` directly into `Idle::worker_to_notify()`'s `SchedulerWakeDecision`.
  - This establishes an exact causal association between each scheduled task and the scheduler's resulting wake/suppress decision, without relying on temporal event adjacency.
- **Tokio Scheduler Logic:**
  - Task 1: `Idle::worker_to_notify()` evaluates `!state.notify_should_wakeup()`. Since `num_searching == 0`, it selects Worker 1. An unpark request is dispatched (`condvar.notify_one()`). Worker 1 enters `searching` state (`num_searching = 1`).
  - Tasks 2–5: When scheduled, `worker_to_notify()` evaluates `!state.notify_should_wakeup()`. Because `num_searching >= 1` and Worker 0 is busy, Tokio intentionally **suppresses worker unparks**, returning `None`.

#### Distribution & Precondition Validation (N=30 Runs)

- **Attempted Runs:** 30
- **Runs Matching Intended Scheduler Precondition:** 30 (100%)
- **Runs with Alternate Scheduler Topology:** 0 (0%)

| Metric (N=30 Valid Intended-Precondition Runs) | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **Task-Correlated Worker Wake Selections** | **1** | **1** | 1 | 1 |
| **Task-Correlated Wake Suppressions (Tasks 2–5 Coalesced)** | **4** | **4** | 4 | 4 |
| **Total `target=None` Scheduler Decisions in Window** | **5** | **5** | 5 | 5 |
| **Task 1 Sched $\to$ Poll Latency** | 0.051 ms | **0.060 ms** | 0.095 ms | 0.096 ms |
| **Coalesced Tasks Sched $\to$ Poll Latency** | 0.027 ms | **0.035 ms** | 0.064 ms | 0.072 ms |

*Diagnostic Note on Earlier `min=0` Observations:*
In earlier un-synchronized prototype runs, occasionally `Task-Correlated Worker Wake Selections` reported `min=0` and `Task-Correlated Suppressions` reported `min=0`. Root-cause investigation showed this was a workload setup race: the blocker task was on an open-ended timer (40ms) while setup slept. If OS scheduling pauses delayed the setup thread, the blocker finished early, leaving Worker 0 idle, or if Worker 1 was still exiting searching mode when Task 1 arrived (`num_searching > 0`), Tokio suppressed Task 1's unpark too. Establishing explicit synchronization (atomic spin control on Worker 0, verified `poll_fn` registration on all 5 waiting tasks, and causal task tracking) eliminated setup nondeterminism across all 30 benchmark runs.

#### Exact Causal Ground Truth Timeline

```text
+   0.179 ms  TASK_SCHEDULED task_id=41 is_local=false
+   0.179 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify_selected task_id=Some(41) target=Some(1) searching=1 unparked=2/2
+   0.180 ms  WORKER_UNPARK_REQUESTED target_worker=1 prev_state=PARKED_CONDVAR
+   0.180 ms  WORKER_UNPARK_DISPATCH_BEGIN target_worker=1 mechanism=condvar
+   0.181 ms  TASK_SCHEDULED task_id=42 is_local=false
+   0.181 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify task_id=Some(42) target=None searching=1 unparked=2/2  <-- TASK 42 SUPPRESSION
+   0.182 ms  TASK_SCHEDULED task_id=43 is_local=false
+   0.182 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify task_id=Some(43) target=None searching=1 unparked=2/2  <-- TASK 43 SUPPRESSION
+   0.183 ms  TASK_SCHEDULED task_id=44 is_local=false
+   0.183 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify task_id=Some(44) target=None searching=1 unparked=2/2  <-- TASK 44 SUPPRESSION
+   0.184 ms  TASK_SCHEDULED task_id=45 is_local=false
+   0.184 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify task_id=Some(45) target=None searching=1 unparked=2/2  <-- TASK 45 SUPPRESSION
+   0.211 ms  WORKER_RESUMED worker=1
+   0.237 ms  WORKER_POLL_START worker=1 task_id=41
+   0.245 ms  WORKER_POLL_START worker=1 task_id=42
+   0.248 ms  WORKER_POLL_START worker=1 task_id=43
+   0.251 ms  WORKER_POLL_START worker=1 task_id=44
+   0.254 ms  WORKER_POLL_START worker=1 task_id=45
```

- **Stock Tokio View:** Records 5 `on_task_spawn` calls and 1 `on_thread_unpark` call (which takes 0 arguments and provides no worker ID or reason).
- **Dial9 View:** Records 5 `WakeEvent`s and 1 `WorkerUnparkEvent`. Dial9 computes `wake_to_poll_delays`: `[0.058ms, 0.032ms, 0.032ms, 0.033ms, 0.035ms]`.
- **Finding:** Under confirmed scheduler preconditions, exactly 1 unpark request was dispatched, exactly 4 task-correlated wakes were suppressed (tasks 2–5 coalesced onto worker 1), and 5 total `target=None` scheduler decisions occurred in the measurement window (an additional decision occurs during worker maintenance). Neither stock Tokio nor Dial9 can see that the scheduler deliberately suppressed 4 worker wakes. To an external observer, there is a strict 1-to-many relationship between 1 worker unpark and 5 scheduled tasks. Dial9 cannot determine whether tasks 2–5 waited because workers were slow to resume, the OS runqueue was delayed, or Tokio intentionally coalesced them onto worker 1.

---

### Adversarial Case 2: Work Stealing & Local Queue Head-of-Line Blocking

- **Workload:** A parent task running on Worker 1 spawns 4 subtasks in succession, then immediately enters a 40–50ms non-yielding compute loop.
- **Tokio Scheduler Behavior:**
  - Subtasks 1, 2, 3 are placed into Worker 1's local run queue.
  - Subtask 4 is placed into Worker 1's LIFO slot (`lifo_slot`).
  - Worker 0 is unparked to help. Worker 0 steals Subtasks 1, 2, 3 and executes them within microseconds.
  - Subtask 4 remains stranded in Worker 1's LIFO slot behind the 40–50ms compute loop.
  - Worker 0 finishes running the stolen tasks, finds no more tasks to steal, and **parks on the driver**, becoming idle while Subtask 4 is still waiting!

#### Distribution (N=30 Runs, 30 Valid)

| Metric | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **Stolen Tasks Sched $\to$ Poll (Worker 0)** | 0.002 ms | **0.003 ms** | 0.004 ms | 0.005 ms |
| **Stranded Task Sched $\to$ Poll (Worker 1 LIFO slot)** | 40.017 ms | **40.019 ms** | 40.037 ms | 40.044 ms |

#### Representative Ground Truth Timeline

The representative run in `linux_run_output.txt` uses a 50 ms compute loop: Task 52 is scheduled at approximately 0.114 ms and polled at 50.131 ms, giving a stranded-task delay of approximately **50.017 ms**. The statistical table above comes from the separate 40 ms variant in `linux_benchmark_output.txt` (**N=30, p50 40.019 ms**).

```text
+   0.109 ms  TASK_SCHEDULED task_id=49 is_local=true  (Worker 1 queue)
+   0.111 ms  TASK_SCHEDULED task_id=50 is_local=true  (Worker 1 queue)
+   0.113 ms  TASK_SCHEDULED task_id=51 is_local=true  (Worker 1 queue)
+   0.114 ms  TASK_SCHEDULED task_id=52 is_local=true  (Worker 1 LIFO slot)
+   0.122 ms  WORKER_POLL_START worker=0 task_id=50   <-- STOLEN by Worker 0
+   0.127 ms  WORKER_POLL_END   worker=0 task_id=50
+   0.130 ms  WORKER_POLL_START worker=0 task_id=49   <-- STOLEN by Worker 0
+   0.130 ms  WORKER_POLL_END   worker=0 task_id=49
+   0.133 ms  WORKER_POLL_START worker=0 task_id=51   <-- STOLEN by Worker 0
+   0.134 ms  WORKER_POLL_END   worker=0 task_id=51
+   0.151 ms  WORKER_PARK_WAIT_BEGIN worker=0 kind=driver <-- Worker 0 goes idle!
... [Worker 1 computes for 50 ms while Worker 0 sleeps] ...
+  50.123 ms  WORKER_POLL_END   worker=1 task_id=48   (Parent compute finishes)
+  50.131 ms  WORKER_POLL_START worker=1 task_id=52   (Stranded task finally runs!)
```

- **Stock Tokio View:** Stock Tokio reports Task 52's approximately 50.017 ms schedule latency directly via `TaskMeta::schedule_latency()` (`on_before_task_poll task_id=52 schedule_latency=50.017ms`).
- **Dial9 View:** Dial9 records Task 52's eventual `PollStart`, but the current Dial9 trace contains no matching `WakeEvent` for Task 52, so `compute_wake_to_poll_delays()` does not produce the long stranded-task interval for this execution. The computed list is `["0.007ms"]`; the trace contains a wake for Task 48, not Task 52. The trace does not establish why Task 52's WakeEvent is absent.
- **OS Scheduling Signal:** `sched_wait` was unavailable/unsampled in this WSL2 run (`sched_wait=None`), so no conclusion about kernel runqueue delay is drawn from this field.
- **Finding:** Stock Tokio observes the long schedule latency but cannot explain the local/LIFO queue stranding. Dial9 observes Task 52 being polled but does not reconstruct its long delay; its current wake-to-poll analysis does not explain the stranding either. Neither view exposes queue placement (local run queue vs LIFO slot vs injection queue) or correlates Worker 0's idle period with Task 52 waiting on Worker 1.

---

### Case F: Controlled Synthetic TCP Service Under Forced Worker Saturation

- **Workload Classification & Context:**
  - This workload is a controlled generic synthetic TCP service modeling synchronous CPU processing (non-yielding compute on worker threads) during request ingestion. Because no pinned production application repository or upload call chain was verified to establish synchronous execution boundaries versus thread offloading, the workload is modeled and labeled strictly as a generic synthetic TCP service. It isolates the interaction between incoming TCP traffic, synchronous worker compute, and Tokio driver servicing.
- **Workload & Synchronization (Controlled Forced-Saturation Test):**
  - A probe TCP client pre-establishes a persistent stream to the server. The connection handler task (`target_tid`) executes its initial poll, waits on `socket.read_exact()`, registers its waker on `ScheduledIo`, and yields `Poll::Pending`.
  - Both runtime workers ($N=2$) are then occupied by non-yielding 40 ms compute tasks.
  - At $t \approx 10\text{ ms}$ into the 40 ms compute window, the probe client thread records `WRITE_BEGIN`, writes an 8-byte request packet, and records `WRITE_DONE`.
- **Measurement Language & External Arrival Timing:**
  - `WRITE_BEGIN` and `WRITE_DONE` are client user-space timestamps recording when `write_all` started and returned on the sending thread. They do not record the nanosecond the receiving socket buffer became readable inside the OS kernel network stack (which requires eBPF or kernel probes).
  - Driver starvation is the explanation supported by the controlled setup (workers running non-yielding compute neither park nor advance `core.tick` to trigger `event_interval` maintenance), without claiming measured kernel-buffer residence or conclusively excluding network transit.
  - `IoReadinessObserved` is an epoll readiness discovery event emitted when `Driver::turn()` returns ready sockets. Tokio currently provides no driver-turn lifecycle events (e.g. `DriverTurnBegin` / `DriverTurnEnd`).

#### Exact Causal Breakdown for Identified Handler Task (Target Task ID = 56)

```text
  Target Task ID:             56 (Identified Probe Request Handler Task)
  T(client_write_begin):      +  10.219 ms (client initiated write_all; user-space timestamp)
  T(client_write_done):       +  10.270 ms (client finished write_all; user-space timestamp)
  T(tokio_io_readiness):       +  40.086 ms (Driver::turn runs epoll_wait and discovers socket readiness)
  T(task_scheduled):           +  40.089 ms (task waker called, placed on worker local queue)
  T(task_polled):              +  40.093 ms (worker polls request handler task)
  --------------------------------------------------------------------------------------------------
  Δclient_write_to_readiness:   29.868 ms <=== INVISIBLE TO TOKIO & DIAL9 (Driver not turned)
  Δschedule (Ready -> Sched):    0.002 ms
  Δpoll (Sched -> Poll):         0.004 ms
  Δtotal_e2e (Write -> Poll):   29.874 ms

  Stock Tokio Schedule Latency (Task 56):          0.004 ms
  Dial9 Wake-to-Poll Delay (Task 56):              0.005 ms
```

- **Observability Inversion for Identified Task:**
  Both Stock Tokio (`TaskMeta::schedule_latency()`) and Dial9 (`compute_wake_to_poll_delays`) report sub-10-microsecond latency ($0.004\text{ ms}$ and $0.005\text{ ms}$), completely missing the preceding $29.868\text{ ms}$ delay during which Tokio's worker threads remained saturated and did not turn the driver.
- **Unambiguous Readiness Attribution:**
  The readiness event was correlated to Task 56 by validating that exactly one candidate token occurred between `WRITE_BEGIN` and the target task's `TaskWakeByVal` event, ensuring zero cross-connection contamination.

---

### Case G: Closed-Loop Concurrency Load Sweep

To investigate runtime behavior under increasing client load, we executed a closed-loop concurrency load sweep across four tiers with constant per-request compute ($5\text{ ms}$ per request) on a 2-worker runtime (`workers=2`).

#### Benchmark Distribution (N=5 Iterations Per Tier, Median Metrics)

| Concurrency Tier | Offered Load | Achieved Throughput | Planned / Iter | Attempted / Iter | Completed / Iter | Fail (total/rate) | Unattempted | Latency p50 | Latency p95 | Latency max | Stock p50 | Stock p95 | Dial9 p50 | Dial9 p95 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Tier 1 (Conc 1, 15ms pace)** | $49.4\text{ rps}$ | $49.4\text{ rps}$ | $20$ | $20$ | $20$ | $0$ | $0$ | $5.12\text{ ms}$ | $5.17\text{ ms}$ | $5.17\text{ ms}$ | $0.010\text{ ms}$ | $0.017\text{ ms}$ | $0.013\text{ ms}$ | $0.028\text{ ms}$ |
| **Tier 2 (Conc 2, 0ms pace)** | $197.2\text{ rps}$ | $197.2\text{ rps}$ | $60$ | $60$ | $60$ | $0$ | $0$ | $10.07\text{ ms}$ | $10.13\text{ ms}$ | $10.16\text{ ms}$ | $0.003\text{ ms}$ | $0.022\text{ ms}$ | $0.004\text{ ms}$ | $0.023\text{ ms}$ |
| **Tier 3 (Conc 4, 0ms pace)** | $380.0\text{ rps}$ | $380.0\text{ rps}$ | $160$ | $160$ | $160$ | $0$ | $0$ | $10.10\text{ ms}$ | $10.39\text{ ms}$ | $15.19\text{ ms}$ | $0.008\text{ ms}$ | $5.021\text{ ms}$ | $0.008\text{ ms}$ | $5.004\text{ ms}$ |
| **Tier 4 (Conc 8, 0ms pace)** | $385.4\text{ rps}$ | $385.4\text{ rps}$ | $384$ | $384$ | $384$ | $0$ | $0$ | $20.13\text{ ms}$ | $30.22\text{ ms}$ | $35.39\text{ ms}$ | $5.048\text{ ms}$ | $14.732\text{ ms}$ | $5.048\text{ ms}$ | $10.442\text{ ms}$ |

#### Methodological Findings from Closed-Loop Testing:
1. **Request Accounting & Failure Transparency:** Across all tiers, $100\%$ of attempted requests completed successfully ($0$ failures, $0$ unattempted). Failure counting reports total failure counts and fractional rates rather than truncating integer averages. Client thread joins propagate panics.
2. **Actual Response Validation:** The client verifies that the 8-byte response echoes the 32-bit request ID and returns a non-zero compute hash produced by `simulate_chunk_processing`. It does not perform a cryptographic digest or expected-result calculation.
3. **Client Latency Monotonicity:** End-to-end client round-trip latency increases monotonically from $5.12\text{ ms}$ (uncontended single client) to $20.13\text{ ms}$ (p50) and $35.39\text{ ms}$ (max) under $4\times$ oversubscription (Tier 4), as closed-loop queues develop.
4. **Task-Specific Filtering in Telemetry:** Stock Tokio and Dial9 metrics are strictly filtered for the identified connection handler tasks (`handler_task_ids`), completely eliminating contamination from unrelated runtime tasks.
5. **Visibility of Task Runqueue vs Invisibility of Driver Starvation:**
   - In Tier 4, Stock Tokio ($5.048\text{ ms}$ p50) and Dial9 ($5.048\text{ ms}$ p50) *do* observe queue delay when multiple tasks have already been woken and are queued in the runqueue behind active worker compute.
   - However, when a packet arrives while workers are active and epoll has not yet been polled, the preceding driver turn delay remains completely invisible.
6. **Why Per-Socket Driver Delay is Not Attributed in Concurrent Sweeps:**
   Tokio's internal `ScheduledIo` token is an opaque raw pointer address (`token.0`) not exported on `tokio::net::TcpStream`. In concurrent multi-stream traffic, attributing individual `IoReadinessObserved` events to specific client requests without per-socket resource probes is ambiguous. Rather than substituting unrelated readiness events, we report client round-trip latency and task schedule latency directly, without computing an unsupported "blind spot percentage".
7. **Original Test Suite Failure Root Cause:**
   The original test suite failure (`0.048 ms` vs `0.084 ms`) occurred in a test suite containing only a single test (`test_network_service_load_tiers`). It was not caused by concurrent tests. The root cause was that `run_network_service_load_tier` previously matched each client write to the first `IoReadinessObserved` event from *any* socket across the runtime, pairing an earlier connection establishment readiness event with a later send. Restricting readiness attribution to unambiguous single-token sequences and serializing instrumented sessions with a shared test lock eliminated both the attribution error and cross-test interference.

---

## Ground Truth vs Stock Tokio vs Dial9

| Event / Internal Fact | Internal Ground Truth (`tokio-probe`) | Stock Tokio (`tokio_unstable`) | Dial9 Telemetry | Status |
| :--- | :--- | :--- | :--- | :--- |
| **External I/O packet sent (`WRITE_BEGIN`)** | Exactly recorded (`ExternalIoStimulus`) | **Invisible** | **Invisible** | **REAL GAP** |
| **I/O driver readiness discovery** | Recorded on socket readiness (`IoReadinessObserved`); does not record turns with no ready sockets or full turn lifecycle | **Invisible** (only aggregate counters) | **Invisible** | **REAL GAP** |
| **Driver starvation under CPU saturation** | Directly measured ($\Delta_{\text{io\_driver}} \approx 30\text{ ms}$) | **Invisible** (reports $<10\ \mu\text{s}$) | **Invisible** (reports $<10\ \mu\text{s}$) | **REAL GAP** |
| **I/O waker dispatched** | `ResourceWakeDispatched` | **Invisible** | `WakeEvent` (if `WakeTraced`) | **Inferable** |
| **Task scheduled timestamp** | `TaskScheduled` (`is_local` recorded) | Stamped internally into `TaskMeta` (no public event) | Stamped into `TaskMeta` | **Inferable at poll** |
| **Task queue placement (Local vs Injected)** | `is_local: bool` | **Invisible** | **Invisible** | **REAL GAP** |
| **Scheduler wake decision (Notify vs Coalesce)**| `SchedulerWakeDecision` (`target`/`None`)| **Invisible** | **Invisible** | **REAL GAP** |
| **Worker unpark requested** | `WorkerUnparkRequested` | **Invisible** | **Invisible** | **REAL GAP** |
| **Worker unpark dispatch begins** | `WorkerUnparkDispatchBegin` (mechanism) | **Invisible** | **Invisible** | **REAL GAP** |
| **Tokio park wait returns (`Parker::park` exit)** | `WorkerParkWaitEnd` (return timestamp & state) | **Invisible** | Sampled Linux `schedstat` | **Partial / Ambiguous** |
| **Worker thread resumes execution** | `WorkerResumed` (worker ID recorded) | `on_thread_unpark()` (0 args) | `WorkerUnparkEvent` | **Ambiguous** |
| **Task poll start** | `WorkerPollStart` (worker + task ID) | `on_before_task_poll` | `PollStartEvent` | **Observable** |
| **Task schedule latency** | Nanosecond math ($T(\text{poll}) - T(\text{sched})$) | `TaskMeta::schedule_latency()` | `compute_wake_to_poll_delays` where matching wake/poll events exist; no long interval for Task 52 in this trace | **Observable in stock; conditional in Dial9 analysis** |
| **Work-stealing head-of-line blocking** | Worker idle vs stranded queue trace | High schedule latency, zero context | Task 52 poll observed; long delay not reconstructed | **REAL GAP** |

*Note on Park Return:* `WorkerParkWaitEnd` tells you when Tokio's park wait returned. The request $\to$ park-return interval contains notification mechanics, kernel scheduling, condvar/mio wake, and lock reacquisition. Dial9 has Linux `schedstat` support capable of exposing `sched_wait_ns` (isolating kernel runqueue wait), but in the WSL2 runs used here the field was unavailable/unsampled (`sched_wait=None`), so no empirical claim in this report relies on schedstat values. Because upstream Tokio exposes no matching `Unparker::unpark()` dispatch timestamp, request $\to$ resume latency cannot be directly reconstructed.

---

## Findings

### Finding 1: Wake suppression / coalescing is completely invisible externally
Tokio's scheduler intentionally coalesces wakeups: if an idle worker is already in the `searching` state, `Idle::worker_to_notify()` returns `None`. In our 30-run benchmark across 5 sequentially notified tasks under confirmed scheduler preconditions (30 attempted runs, 30 valid intended-precondition runs, 0 alternate topology runs), exactly 1 task-correlated worker wake selection occurred, exactly 4 task-correlated wakes were suppressed (tasks 2–5 coalesced onto worker 1), and 5 total `target=None` scheduler decisions occurred in the measurement window. The selection metric counts `SchedulerWakeDecision { target_worker: Some(...) }`, not Unparker dispatch events. Separately, the representative trace shows the corresponding unpark request and dispatch stage. External observers see 5 tasks awaken and 1 unpark event, with no mechanism to determine whether later tasks were intentionally batched onto the running worker or delayed by contention.

### Finding 2: Worker notification $\to$ resume latency cannot be directly measured
Upstream Tokio provides `on_thread_unpark()`, but this callback takes 0 arguments and fires *after* the thread has already resumed execution in user space. There is no timestamp for when the unpark was requested. Therefore, the interval $T(\text{resumed}) - T(\text{unpark\_requested})$ cannot be measured. Furthermore, an observer cannot correlate which task or I/O event caused the worker to resume.

### Finding 3: External I/O stimulus $\to$ Tokio driver service latency is invisible and can invert telemetry
When workers are occupied by non-yielding CPU-bound tasks, `Driver::turn()` is not called. In our Case E benchmark across 30 runs, external TCP traffic was sent, but Tokio did not service the driver for $\sim 29.84\text{ ms}$ (p50). Once the driver finally turned, the reader task was scheduled and polled within $7\ \mu\text{s}$ (p50). Both stock Tokio's `TaskMeta::schedule_latency()` ($0.003\text{ ms}$ in representative run) and Dial9's `wake_to_poll_delay` ($0.004\text{ ms}$ in representative run) reported sub-5-microsecond schedule latency, hiding $\sim 29.82\text{ ms}$ of unserved latency.

**Mechanism in Tokio Multi-Thread Scheduler (`event_interval` vs `global_queue_interval`):**
Source inspection of pinned Tokio (`tokio/src/runtime/scheduler/multi_thread/worker.rs`) clarifies the exact driver servicing mechanics:
1. **Periodic Driver Servicing via `event_interval` (`worker.rs:844`):**
   ```rust
   fn maintenance(&self, mut core: Box<Core>) -> Box<Core> {
       if core.tick % self.worker.handle.shared.config.event_interval == 0 {
           super::counters::inc_num_maintenance();
           core.stats.end_processing_scheduled_tasks();
           core = self.park_yield(core); // Turns I/O driver with 0 timeout
           core.maintenance(&self.worker);
           core.stats.start_processing_scheduled_tasks();
       }
       core
   }
   ```
   Tokio multi-thread workers do not run a dedicated I/O polling thread. Instead, the I/O driver is turned cooperatively when workers park waiting for work or during periodic worker maintenance ticks governed by `event_interval` (default 61 ticks).
2. **Distinction from `global_queue_interval` (`worker.rs:1132`):**
   `global_queue_interval` controls how often a worker checks the shared injection queue (`worker.handle.next_remote_task()`), *not* driver polling. Driver servicing is strictly governed by `event_interval` via `park_yield(core)`.
3. **The Root Cause of Starvation:**
   Worker tick progression (`tick(&mut self)` at `worker.rs:1126-1128`) only occurs when a task yields or completes its poll cycle. When worker threads execute non-yielding compute blocks (such as synchronous hashing or chunk processing), `core.tick` never increments, and control never returns to `maintenance()`. Consequently, `event_interval` driver servicing is starved completely until the compute tasks finish.

### Finding 4: Task queue placement causality is not represented by `schedule_latency`
When a task experiences high schedule latency, `TaskMeta::schedule_latency()` provides only a scalar duration. In the representative work-stealing run, Task 52 waited approximately $50.017\text{ ms}$ in Worker 1's LIFO slot behind a 50 ms compute loop while Worker 0 was idle and parked. The separate 40 ms benchmark variant reports a stranded-task p50 of $40.019\text{ ms}$ across 30 runs. Stock telemetry does not explain the queue placement; Dial9's current wake-to-poll analysis does not reconstruct Task 52's long interval. `sched_wait=None` supplies no evidence for or against kernel runqueue delay.

---

## Attempts to Disprove the Gaps

We systematically tested whether existing signals could reconstruct or infer the missing intervals:

1. **Can `TaskMeta::schedule_latency()` infer Boundary A (Driver Starvation)?**
   - **Tested:** Correlated `TaskMeta::schedule_latency()` against external write timestamps in Case E.
   - **Result:** Disproved. `schedule_latency()` measures only from `task.set_scheduled_at()` (which is called *after* `driver.turn()` returns). The entire driver service delay occurs before `set_scheduled_at()` is executed.
2. **Can `on_thread_park` / `on_thread_unpark` infer Boundary C (Worker Wake Latency)?**
   - **Tested:** Subtracted $T(\text{on\_thread\_park})$ from $T(\text{on\_thread\_unpark})$.
   - **Result:** Disproved. This difference measures the *total sleep duration* of the worker thread, not the notification latency. A worker sleeping for 5 seconds waiting for work will produce a 5000ms duration, completely conflating idle time with wake latency.
3. **Can Linux `schedstat` (`sched_wait_ns`) isolate thread wakeup delay?**
   - **Tested:** Dial9 has Linux `schedstat` support capable of exposing `sched_wait_ns` on worker unpark. In our WSL2 environment, this field was unavailable/unsampled (`sched_wait=None (unsupported/unsampled)`), so no empirical claims in this report rely on schedstat measurements.
   - **Theoretical Capability & Limitations:** Even where Linux `schedstat` is active, it only measures how long a runnable OS thread sat on the Linux kernel runqueue waiting for a CPU core. It cannot measure:
     - The time Tokio spent evaluating whether to wake a worker ($S_1$).
     - The time Tokio spent in `Unparker::unpark()` acquiring mutexes/condvars ($S_2$).
     - The time Tokio spent executing driver loop maintenance before polling ($S_4$).
     - Any platforms other than Linux.
4. **Can `io_driver_ready_count` detect driver starvation?**
   - **Tested:** Monitored `RuntimeMetrics::io_driver_ready_count`.
   - **Result:** Disproved. This is a monotonic counter of discovered events. It does not record event timestamps, queue delay, or when `poll()` was invoked relative to external arrival.

---

## Current Upstream Work & Survey

We reviewed Tokio issues and pull requests to verify if these gaps are being addressed:

- **PR [#8282](https://github.com/tokio-rs/tokio/pull/8282) (Russell Cohen, Merged):**
  Introduced `TaskMeta::schedule_latency()` and exposed it via `on_before_task_poll` and runtime metrics. This established Boundary D baseline measurement, but explicitly does not touch driver turn timing, wake coalescing decisions, or worker unpark dispatch.
- **PR [#7986](https://github.com/tokio-rs/tokio/pull/7986) (Merged):**
  Added aggregate task schedule latency histogram metric to `RuntimeMetrics`.
- **PR [#8043](https://github.com/tokio-rs/tokio/pull/8043) & [#8025](https://github.com/tokio-rs/tokio/pull/8025) (Russell Cohen, Merged):**
  Added `taskdump::trace_with` for customized task dumps, skipping redundant double-wakes.
- **PR [#7921](https://github.com/tokio-rs/tokio/pull/7921) (Merged):**
  Added `tokio::runtime::worker_index()` exposing current worker thread identity.
- **Upstream Gap Status:**
  No active PR or open issue in Tokio currently addresses:
  - Driver turn interval observability.
  - Exposing `Idle::worker_to_notify()` wake coalescing decisions.
  - Notification dispatch timestamps for `Unparker::unpark()`.
  - Distinguishing local queue vs remote injection queue task placement.

---

## Remaining Uncertainty

1. **External Arrival Ground Truth in Production:**
   In our experiment, `ExternalIoStimulus` provided physical stimulus timestamps using an in-process socket. In real production networks, packet arrival occurs at the NIC and kernel TCP stack. Hardware NIC timestamps or eBPF `sock:sock_data_ready` would be required to measure exact kernel socket readiness in production.
2. **`mio::Waker` Driver Cross-Wakes:**
   When a worker parked on the I/O driver (`mio::Poll::poll`) is unparked via `mio::Waker::wake()`, the OS unblocks epoll. The exact syscall duration of `mio::Waker::wake()` vs `pthread_cond_signal` exhibits small variance across Linux kernel versions.
3. **Overhead of Trace Points:**
   Ground-truth probes in `tokio-probe` write to an in-memory buffer. Any eventual upstream hook design must ensure zero cost when hooks are unset.

---

## Conclusion

We classify each causal boundary independently:

| Boundary | Classification | Verdict Summary |
| :--- | :--- | :--- |
| **Boundary A: External I/O Stimulus $\to$ Tokio Driver Observation** | **REAL GAP** | **Proven.** Under worker compute saturation, driver service delay can reach tens of milliseconds while `TaskMeta::schedule_latency` and Dial9 report microseconds. |
| **Boundary B: Runnable Work $\to$ Wake / Coalesce Decision** | **REAL GAP** | **Proven.** Wake suppression in `Idle::worker_to_notify()` is invisible externally. One task-correlated worker wake selection can correspond to multiple scheduled tasks, while the wake/coalesce decisions themselves are not exposed. |
| **Boundary C: Worker Notification $\to$ Worker Resume** | **REAL GAP** | **Proven.** `on_thread_unpark` fires after resumption with 0 arguments. The duration between unpark dispatch and worker loop resumption cannot be reconstructed from current external Tokio/Dial9 telemetry or attributed to a cause. |
| **Boundary D: Task Placement / Work Stealing $\to$ Poll** | **PARTIAL GAP** | **Partially Addressed.** `TaskMeta::schedule_latency()` measures the total delay, but does not expose queue placement (local vs injected vs LIFO) or work-stealing causality. |

### Addressing Russell Cohen's Real-Application Question: From Controlled Reproduction to Real RustFS Investigation

A central motivation from Russell Cohen was to determine whether this investigation explains real-world application slowdowns or only a synthetic, controlled reproduction. Russell specifically pointed to RustFS because it integrates Dial9 telemetry in production.

The preliminary local RustFS run reproduces a slowdown under load. Its traces expose long task polls and queue depths, but do not yet establish the request-level cause or reconstruct why the I/O driver was delayed.

---

## Preliminary Local RustFS Workload Investigation (Debug Build)

### 1. Primary Source Request Path & Hypotheses (Commit `6b1554003ebf8f2037ffb7da9c9b906527e758da`)

To establish whether driver starvation or task scheduling bottlenecks emerge organically, we inspected primary RustFS source at commit [`6b1554003ebf8f2037ffb7da9c9b906527e758da`](https://github.com/rustfs/rustfs/tree/6b1554003ebf8f2037ffb7da9c9b906527e758da) and traced the relevant source path for the standard S3 `PutObject` operation. Source inspection identifies possible work, not the measured duration of each operation:

1. **Network Ingress & Connection Spawning:**
   - [`rustfs/src/server/http.rs:1905`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/server/http.rs#L1905): `listener.accept().await` accepts the incoming client TCP socket.
   - [`rustfs/src/server/http.rs:2218-2224`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/server/http.rs#L2218-L2224): `process_connection()` is spawned as an async task on the Tokio runtime.
   - [`rustfs/src/server/http.rs:2488`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/server/http.rs#L2488): Hands the socket to Hyper via `TokioIo::new(socket)`.
2. **Request Routing & Admission Control:**
   - [`rustfs/src/app/object/put.rs:1230`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/app/object/put.rs#L1230): Hyper dispatches S3 `PUT` requests to `put_object()` $\to$ `put_object_core()`.
   - [`rustfs/src/app/object/put.rs:1517`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/app/object/put.rs#L1517): `ConcurrencyManager::admit_put_object()` applies semaphore admission control.
3. **Payload Streaming & Synchronous Checksumming:**
   - [`crates/rio/src/hash_reader.rs:191-230`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/rio/src/hash_reader.rs#L191-L230): `HashReader::from_stream()` wraps the HTTP body stream.
   - [`crates/rio/src/hash_reader.rs:531-545`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/rio/src/hash_reader.rs#L531-L545): `poll_read()` updates the SHA-256 hasher synchronously when that hasher is configured. This identifies an execution boundary, not the cost of hashing in the captured requests.
4. **Storage Pipeline Dispatch:**
   - [`rustfs/src/app/object/put.rs:1990`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/app/object/put.rs#L1990): Calls `spawn_traced_join(store.put_object_with_old_current_size(...))` defined at [`rustfs/src/storage/request_context.rs:266`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/storage/request_context.rs#L266).
   - Enters `SetDisks::put_object_with_old_current_size_inner()` at [`crates/ecstore/src/set_disk/ops/object.rs:3497`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/ecstore/src/set_disk/ops/object.rs#L3497).
5. **Inlined Reed-Solomon Erasure Coding (Key CPU Path Finding):**
   - [`crates/ecstore/src/erasure/coding/encode.rs:702-717`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/ecstore/src/erasure/coding/encode.rs#L702-L717): Under `RuntimeFlavor::MultiThread`, RustFS **deliberately executes Reed-Solomon matrix encoding directly on the Tokio worker thread without yielding** (`encode_once()`). RustFS avoids `spawn_blocking` or `block_in_place` here to prevent thread parking overhead.
   - [`crates/ecstore/src/erasure/coding/bitrot.rs:533-542`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/ecstore/src/erasure/coding/bitrot.rs#L533-L542): `BitrotWriter` computes `HighwayHash256` bitrot checksums per block synchronously on the worker thread.
6. **Disk I/O Offloading:**
   - [`crates/ecstore/src/disk/local.rs:3839`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/crates/ecstore/src/disk/local.rs#L3839): `open_write()` creates the file-backed writer, with backend-dependent paths and wrappers. The earlier citation to `object.rs:5001` identified a spawned commit task, not a vectored-write call. Neither disk service time nor blocking-pool queue wait was measured in this run.
7. **Runtime & Dial9 Telemetry Integration:**
   - [`rustfs/src/server/runtime.rs:207`](https://github.com/rustfs/rustfs/blob/6b1554003ebf8f2037ffb7da9c9b906527e758da/rustfs/src/server/runtime.rs#L207): Calls `rustfs_obs::dial9::build_traced_runtime()`, attaching `Dial9Handle` with unstable Tokio hooks (`tokio_unstable`).

**Hypothesis Formulation:**
- **Observable Symptom:** As concurrent PUT load increases, request latency degrades non-linearly.
- **Suspected Mechanism:** Because RustFS inlines Reed-Solomon EC computation and chunk hashing on worker threads, multiple concurrent operations may hold worker threads for extended intervals, delaying periodic I/O driver turns (`core.tick % 61 == 0`) and causing pre-readiness discovery delays on active sockets.
- **Evidence that would support it:** Dial9 worker poll durations regularly exceeding multiple milliseconds, accompanied by driver discovery delays and rising socket queueing.
- **Evidence that would contradict it:** Worker poll durations remaining small (<1 ms), while latency degradation is dominated by Tokio blocking pool disk I/O or client-side queueing.

---

### 2. Experimental Setup & Workload Parameters

- **Repository Revisions:**
  - RustFS Base Commit: [`6b1554003ebf8f2037ffb7da9c9b906527e758da`](https://github.com/rustfs/rustfs/tree/6b1554003ebf8f2037ffb7da9c9b906527e758da)
  - Runtime dependencies from RustFS Cargo.lock: registry Tokio **1.53.2**, Dial9 **0.5.3**, and dial9-tokio-telemetry **0.5.3**. Registry checksums are saved in [debug-provenance.json](experiments/rustfs/results/debug-provenance.json). The runtime was not built against the harness's cloned Git revisions.
  - Decoder checkout: `33b2d780628b42251047909ff2b88fdb97e3c28b`; this describes the separately built trace converter, not the RustFS runtime dependencies.
  - Original build command: `RUSTFLAGS="--cfg tokio_unstable" cargo build -p rustfs --bin rustfs --features dial9`. The binary was `target/debug/rustfs`, using the unoptimized dev profile. These numbers are not release performance or a production capacity limit.
- **Host Resources:**
  - CPU: AMD Ryzen 5 5600H (12 vCPUs, 3.3/4.2 GHz)
  - Memory: 18 GiB total (7.1 GiB available buffer cache)
  - Kernel: Linux 6.18.33
- **RustFS Configuration:**
  - Topology: single node with four local volume directories on the same host (`/tmp/rustfs-exp/vol{1..4}`). This is not four independently provisioned storage devices.
  - A host check during follow-up found `/tmp` backed by tmpfs. The original run did not capture mount provenance, and its timings cannot establish physical disk latency.
  - Runtime Workers: `RUSTFS_RUNTIME_WORKER_THREADS=2` (bounded worker count to isolate multi-threaded contention).
  - Telemetry: `RUSTFS_RUNTIME_DIAL9_ENABLED=true`, writing to `/tmp/rustfs-exp/telemetry`.
  - Flags: `RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true`, `RUSTFS_CONSOLE_ENABLE=false`.
- **Workload:**
  - Operation: Fixed 1 MiB (`1,048,576` bytes) S3 `PutObject` with AWS SigV4 authentication.
  - Warmup: 5 sequential requests.
  - Closed-loop Concurrency Tiers: 1, 2, 4, 8 workers (duration = 8.0s per tier).
  - Scheduled arrival-rate sweep: 10, 25, 50 requests/s for 6 seconds, with a client cap of 64 active threads. Requests arriving at a full cap were shed by the client.
  - One run per tier; tiers ran sequentially. Startup, warmup, and all tiers share one trace without saved phase timestamps. Instrumentation overhead and run-to-run variability were not measured.

---

### 3. Load Experiment Results

All **attempted** requests in the saved results returned HTTP 200. No read-back integrity check was performed. The old field named TTFB measures time through response-header parsing, not the exact first response byte. The original result JSON is preserved in [debug-original.json](experiments/rustfs/results/debug-original.json).

#### Closed-Loop Concurrency Sweep (Duration: 8.0s per tier)

| Tier | Concurrency | Completed Reqs | Achieved Ops/s | Throughput (MiB/s) | Total Latency p50 | Total Latency p95 | Total Latency Max | Write Time p50 | Response-header wait p50 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Tier 1 (Baseline)** | 1 | 97 | 12.05 ops/s | 12.05 MiB/s | **82.71 ms** | 85.29 ms | 87.38 ms | 0.33 ms | 81.73 ms |
| **Tier 2 (Worker Limit)**| 2 | 184 | 22.88 ops/s | 22.88 MiB/s | **86.74 ms** | 90.70 ms | 100.24 ms | 0.41 ms | 85.61 ms |
| **Tier 3 (2x Workers)** | 4 | 186 | 23.00 ops/s | 23.00 MiB/s | **167.42 ms** | 253.94 ms | 287.52 ms | 0.35 ms | 166.33 ms |
| **Tier 4 (4x Workers)** | 8 | 189 | 23.24 ops/s | 23.24 MiB/s | **344.72 ms** | 471.32 ms | 568.14 ms | 0.37 ms | 343.63 ms |

*Observation:* This debug run plateaued near 23 successful PUTs/s as concurrency rose. Median latency increased from 82.71 ms to 344.72 ms. The experiment does not establish whether CPU work, admission, storage, client behavior, or another resource caused that plateau. Throughput uses the full elapsed time including final request drain (8.04–8.13 seconds), not a strictly fixed 8-second completion window.

#### Scheduled Arrival Sweep with Client Shedding (6-second Generation Window)

| Tier | Target Rate | Scheduled | Attempted | Completed OK | Client-shed | Ops/s Including Drain | Latency p50 | Latency p95 | Latency Max |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Rate 10** | 10 req/s | 60 | 60 | 60 (100%) | 0 | 10.03 ops/s | **83.44 ms** | 86.44 ms | 115.46 ms |
| **Rate 25** | 25 req/s | 150 | 150 | 150 (100%) | 0 | 23.14 ops/s | **562.90 ms** | 1035.11 ms | 1180.23 ms |
| **Rate 50** | 50 req/s | 300 | 162 | 162 (100%) | 138 (client cap)| 23.19 ops/s | **2553.01 ms** | 2984.84 ms | 3037.07 ms |

*Observation:* Attempt latency increased with the configured arrival rate, but the 50 requests/s tier issued only 162 of 300 scheduled requests; the client shed 138. This is not measured server rejection or server backpressure. Reported throughput divides completions by generation plus drain (5.98, 6.48, and 6.99 seconds in the old generator). Latencies omit scheduled-to-launch delay and include client signing and connection setup. The old generator did not save per-request timings, so these omissions cannot be repaired retrospectively.

---

### 4. Whole-Run Dial9 Observations

Upon graceful server shutdown (SIGTERM), Dial9 flushed and sealed the binary trace segment:
- **Trace Artifact:** `/tmp/rustfs-exp/telemetry/rustfs-tokio/trace.0.bin` (6,629,197 bytes).
- **Decoded Events:** 572,625 events over 59,475.41 ms of live execution.
  - `PollStartEvent`: 188,024
  - `PollEndEvent`: 188,024
  - `TaskSpawnEvent`: 37,892
  - `TaskTerminateEvent`: 122,408
  - `WorkerParkEvent`: 14,925
  - `WorkerUnparkEvent`: 14,923
  - `RuntimeMetricsEvent`: 5,839
  - `ProcessResourceUsageEvent`: 585

#### Worker Poll Wall Time

- **Worker 0:** 92,875 polls, 44,482.53 ms inside task polls (**74.8% of the trace wall-clock span**), 7,461 parks.
- **Worker 1:** 95,149 polls, 45,135.97 ms inside task polls (**75.9% of the trace wall-clock span**), 7,464 parks.
- **Poll Duration Distribution:**
  - Median ($p50$): **0.068 ms** (68 µs)
  - 90th percentile ($p90$): **0.814 ms** (814 µs)
  - 95th percentile ($p95$): **2.054 ms**
  - 99th percentile ($p99$): **2.164 ms**
  - Maximum Poll Duration: **43.291 ms**
- **Count of Extended Worker Polls:**
  - Polls $\ge 1\text{ ms}$: **18,518** (9.8% of all task polls)
  - Polls $\ge 5\text{ ms}$: **1,055**
  - Polls $\ge 10\text{ ms}$: **1,035**
  - Polls $\ge 30\text{ ms}$: **1,035**

#### Source Attribution of Extended Polls

Dial9 records spawn locations for whole futures. These locations do not identify which inner operation occupied a poll. All statistics below combine startup, warmup, background work, and every load tier:

1. **`rustfs/src/storage/request_context.rs:266:5` (`spawn_traced_join`):**
   - Max poll: **43.291 ms**
   - Average poll: **1.579 ms**
   - Total polls: **48,927**
   - *Scope:* The storage future contains hashing, encoding, storage coordination, and other work. The trace cannot assign the 43.291 ms poll specifically to Reed-Solomon encoding. Its combined poll wall time is **77,248.85 ms** across the run; the mean per poll is not the total CPU cost per request.
2. **`rustfs/src/init.rs:89:5`:** Max poll = **41.875 ms** (startup storage volume scan).
3. **`crates/scanner/src/scanner_io/io_cache.rs:690:46`:** Max poll = **9.524 ms** (scanner metadata cache).
4. **`crates/ecstore/src/store/object.rs:3263:9`:** Max poll = **5.377 ms** (object metadata update).
5. **`rustfs/src/server/http.rs:2224:5`:** Max poll = **3.224 ms** (Hyper connection processor reading network body).

#### Local Runqueue Depth Accumulation

Dial9 records `local_queue` depth on every `PollStartEvent`:
- `local_queue == 0`: 36.8% of polls.
- `local_queue > 0`: **63.2% of polls**.
- Maximum observed local queue depth: **91 tasks**. Without tier boundaries and request/task links, this does not identify a particular tier's queue wait or prove a request-level head-of-line bottleneck.

The repository analyzer independently reproduces the poll counts and durations in [debug-trace-summary.json](experiments/rustfs/results/debug-trace-summary.json). It labels them as wall time and reports unmatched poll boundaries. Thread CPU-counter spans are separate evidence and cannot allocate CPU to a particular request or encoding stage.

---

### 5. Evidence Boundaries and Remaining Questions

| Question | What this run establishes | What remains unmeasured |
| :--- | :--- | :--- |
| Did real requests slow down under increased load? | Yes, in a local debug build with successful attempted PUTs and client-side shedding at the highest rate. | Release behavior, repeated measurements, and telemetry overhead. |
| Were long worker polls present? | Yes: whole-task wall-clock polls reached 43.291 ms. | CPU versus descheduling time for each poll, and the cost of individual inner operations. |
| Was the driver starved during a particular request? | Not established by this trace. Long polls make the hypothesis worth testing. | Overlapping occupancy of all workers, driver-turn intervals, and request-linked readiness observation. One occupied worker alone does not prevent the other worker from servicing I/O. |
| Was the baseline dominated by disk waits? | Not established. No numerical baseline decomposition is available. | Request-linked disk service time, blocking-pool queue wait, admission waits, and synchronous processing. The earlier 75 ms disk / 2–5 ms CPU split was unsupported and has been removed. |
| Why are wake-to-poll measurements unavailable? | The decoded trace contains no wake events; the inspected request helper uses standard `tokio::spawn` rather than Dial9's wake wrapper. | Wake-to-poll analysis is unavailable for this configuration. This does not imply that every possible Tokio schedule-latency measurement requires application rewrites. |

RustFS already has opt-in PUT stage metrics (`RUSTFS_OBS_PUT_STAGE_METRICS_ENABLED`), emitted through its existing metrics exporter. They were not collected in the original run. Separate encoding timers such as `erasure_encode_cpu` cover particular code paths, not every PUT. These aggregate stages help distinguish possible costs, but are not a request-linked causal timeline and must not be subtracted from request latency as independent percentiles.

### 6. Reproduction and Next Measurements

The reusable client, bounded runner, trace analyzer, and tests are now in [experiments/rustfs](experiments/rustfs/README.md). They use Python's standard library and the existing RustFS/Dial9 dependencies. New runs record each tier's wall-clock boundaries, each request's attempt and completion timestamps, launch lateness, client shedding, generation-window throughput, and throughput including drain. Repetitions use fresh temporary data directories. Large traces and binaries remain outside tracked results.

The [optimized follow-up](experiments/rustfs/README.md#optimized-follow-up-results) now includes three telemetry-on and three telemetry-off repetitions using the same release binary and pinned lockfile. Every tier has a three-second generation window, saved request records, and explicit drain accounting. Unlike the original `/tmp` run, temporary volumes shared the NVMe-backed `/home` filesystem. The different storage backing prevents a direct debug-versus-release comparison.

Telemetry-on closed-loop throughput at concurrency eight ranged from 189.30–192.31 PUTs/s; telemetry-off ranged from 157.74–189.07 PUTs/s. At 300 scheduled arrivals/s, successful attempt-latency p50 ranged from 306.15–320.05 ms with telemetry and 297.56–338.16 ms without it. Client shedding occurred in both conditions. All 18,289 attempted PUTs returned HTTP 200; read-back integrity was not checked. These short, sequential conditions do not establish an instrumentation-overhead percentage or production capacity.

The [21 per-tier trace windows](experiments/rustfs/results/release-trace-windows.json) include generation and drain and explicitly report partial poll boundaries. Polls of at least 30 ms occurred in every c8, r200, and r300 window, and none in c1, c2, c4, or r100. This supports an association between heavier local load and long whole-task wall time. To explain a representative slowdown, collect existing stage metrics or profiling evidence and correlate it with the request path. Driver starvation remains a separate hypothesis requiring relevant I/O timing and evidence that all workers were unavailable.

**Objective status: partially answered.** A real local RustFS request path and load-dependent slowdown were reproduced, including short repeated release measurements. The request-level cause, driver servicing delay, and instrumentation overhead remain unresolved. The controlled synthetic experiments remain the direct evidence for their deliberately forced conditions.

### 7. Existing Stage Metrics Follow-up

The [stage-metrics experiment](experiments/rustfs/README.md#existing-put-stage-metrics) collected three repetitions at concurrency one and eight with the same optimized binary and 1 MiB PUT workload. All 2,352 attempts returned HTTP 200. Cumulative export differences bracket each tier with idle padding; they are observation-window aggregates, not request-level traces.

Mean `app_store_put` elapsed time ranged from 13.22–13.95 ms at concurrency one to 32.45–43.76 ms at concurrency eight. Mean rename/quorum wait rose from 7.45–7.64 ms to 16.31–21.21 ms; per-file fdatasync timing rose from 2.47–2.51 ms to 3.69–4.80 ms. Nested stages and parallel disk observations must not be added into a latency budget. These results prioritize storage commit/quorum and sync operations for further profiling; they do not establish physical disk service time or exclude scheduler effects.

The emitted path label is `write_single_block_non_inline`. This dispatches to `encode_small_direct`, which synchronously encodes an owned block and also awaits reading, writes, and shutdown. It bypasses the `encode_block` timer cited earlier as a possible CPU path. No `erasure_encode_cpu` samples were exported, so encoding CPU cost remains unmeasured. The broader `set_disk_encode` mean increased from 3.71–3.97 ms to 8.25–11.66 ms and includes async work. The next useful measurement is storage syscall and off-CPU profiling, followed by request correlation; this evidence does not yet justify a driver-starvation claim.

### 8. Separate Syscall Diagnostic

The [strace diagnostic](experiments/rustfs/README.md#separate-syscall-diagnostic) traced file sync, futex, and epoll calls and correlated thread identities with Dial9. It strongly perturbed execution: c8 throughput dropped to 33.61 PUTs/s. Its timings must not be used as a normal performance baseline. No fsync/fdatasync calls ran on the two identified Tokio runtime workers during either tier; most ran on dedicated fsync threads. Other threads reused the runtime worker name, so names alone would have misclassified them.

For the traced c8 request `c8/75.bin`, attempt latency was 345.26 ms. Four directory syncs associated by exact object path began 309.64–326.82 ms after the attempt and each lasted 0.86–1.36 ms. The runtime workers entered epoll 19 times during that request. This is a partial timeline, not a latency decomposition: request-specific temporary-file syncs, queue waits, CPU work, and pre-driver readiness delay remain unmeasured. The untraced stage data prioritizes storage commit/quorum work, but neither these syscalls nor aggregate histograms explain the full critical path. No driver-starvation or encoding-CPU claim follows from this diagnostic.

### 9. Existing Request Spans: A Representative Slow PUT

The [request-span diagnostic](experiments/rustfs/README.md#existing-request-span-diagnostic) enabled RustFS's existing spans without changing its source or build. One repetition completed 507 PUTs successfully and exported 36,146 spans. Debug logging and export overhead make this a separate diagnostic condition.

Request `c8/259.bin` took 99.69 ms at the client. The matching server request ID identifies a 92.851 ms HTTP span. Its selected trace contains 54 spans with all exported parent references resolved; the erasure-set storage operation spans 85.989 ms. A nested directory-creation operation takes 19.830 ms, including an exported 19.110 ms span-idle interval. This wrapper awaits `tokio::fs` operations. The span counters are wall time around enter/exit, not CPU time or a measured blocking-pool queue delay; filesystem work, queueing, scheduling, and diagnostic overhead remain indistinguishable within the await.

An additional object-matched metadata trace has a separate trace ID and is preserved without inventing a causal parent link. The selected hierarchy locates most server elapsed time inside storage and identifies a substantial await interval, but does not fully explain that interval or establish complete request coverage. The next causal boundary to measure is request-linked filesystem/queue completion and task resumption; §10 measures it directly with the blocking-pool probe. Kernel socket readiness and driver discovery are still absent; the evidence does not establish driver starvation in this RustFS workload.

### 10. Blocking-Pool Probe: Decomposing the Remaining Delay

The [blocking-pool probe diagnostic](experiments/rustfs/README.md#blocking-pool-probe-diagnostic) instruments Tokio's own blocking-pool and `JoinHandle` boundaries in an isolated patched copy (byte copy of registry `tokio-1.53.2`, wired only into the `.repro/rustfs-probe` worktree; both patches are preserved and hash-pinned under `experiments/rustfs/patches/`; the control binary is untouched, SHA-256 `dc577ce7…`). It observes five boundaries directly — `T0` submit, `T1` job start, `T2` job end, `T3'` completion recorded after `task.run()`, and `T5` join-ready — plus `SEND`/`WAIT` records around `commit_rx.await`, and joins everything to the client request by an FNV hash of `bucket/object`. Two probe repetitions (concurrency 1 and 8, 1 MiB PUTs, 3 s tiers) recorded 458,882 and 471,759 records with zero drops; probe↔Dial9 clock offsets agree within 11–20 ns; 167,665+ Dial9 polls were paired with no unmatched boundaries. Both patch generations (the original two files and the stage/CPU/quorum follow-up revision) are preserved and hash-pinned under `experiments/rustfs/patches/`, and each follow-up patch was applied to pristine sources and compared byte-for-byte against the live working copies.

**Finding:** for the selected requests, almost all commit-channel waiting occurred before the quorum-send marker. Waiter resumption afterward was short. Correlated blocking jobs occupied most of that interval; their internal cause is unresolved in these captures and is attributed directly by the follow-up probe revision in §10.1.

For the slowest PUT of every tier, the `commit_rx.await` wait — the boundary the span diagnostic could only show as "an await with zero polls" — accounts for 65–86% of client latency, and within that wait:

| Representative PUT | client | wait | wait = begin→send | send→containing poll | poll→wait end |
| :--- | ---: | ---: | ---: | ---: | ---: |
| run-1 `c1/209.bin` | 19.5 ms | 12.720 ms | 12.717 ms | 0.69 µs | 1.82 µs |
| run-1 `c8/351.bin` | 135.4 ms | 115.111 ms | 115.108 ms | 0.94 µs | 2.12 µs |
| run-2 `c1/6.bin` | 29.5 ms | 22.578 ms | 22.575 ms | 0.94 µs | 2.16 µs |
| run-2 `c8/204.bin` | 87.5 ms | 74.851 ms | 74.849 ms | 0.46 µs | 1.37 µs |

Over 99.99% of each wait elapsed before the send marker: the operation waited for the quorum `SEND_OK` to be produced. After the send, the Dial9 poll containing the waiter's `wait_end` began within ~1 µs and the wait closed ~2 µs later — short by any reading, with the caveat that the poll is resolved by thread-id containment and can in principle have begun before the wake.

The wait window is occupied by the operation's own blocking jobs: 97.3–98.9% of each window is covered by `[submit..end]` segments of that PUT's jobs, with total dispatch overlap (`submit→start` inside the window) of only 0.26–0.83 ms and roughly four jobs in flight concurrently (452 ms of clipped job wall time inside the 115 ms run-1 c8 window). Per representative PUT, 56 blocking jobs join: 44 untagged commit-phase jobs (the `rename`/`rename_no_owner` step tags never fire on this path, so their call sites are unattributed) accumulate 455.4 ms of closure wall time — including a single 101.6 ms job — while the 12 tagged `mkdir`/`make_dir_all` jobs total 1.1 ms.

Across all 174,006 jobs in the two probe runs (job aggregates do not depend on the request join):

- **Blocking-pool dispatch (`T0→T1`)**: p50 8 µs, p95 44–50 µs, p99 0.20–0.21 ms, max 3.8 ms — dispatch delay is negligible at the median and sub-millisecond at p99.
- **Blocking-closure wall time (`T1→T2`)**: p50 20 µs, p95 3.5 ms, p99 6.1–6.2 ms, max 6.4–6.6 s — the heavy tail lives here, but this is wall time inside the closure: filesystem calls, CPU work, locks, and OS descheduling are not separated, so it is not yet a "filesystem execution" measurement.
- **Completion propagation (`T2→T3'`)**: p50 0.8 µs, p99 ~7 µs — small in these captures.
- **Completion→poll-start proxy (`T3'`→containing poll of `join_ready`)**: p50 89–101 µs, p95 1.1 ms, p99 2.8–2.9 ms, max 20–35 ms — a diagnostic proxy, not measured scheduling latency: `T3'` records after `task.run()` stored the result and woke the joiner (who can resume before the marker), and the poll containing `join_ready` need not be the wake-triggered resume.
- **Submit→join-ready total**: p50 0.26–0.27 ms, p95 4.3–4.5 ms, p99 7.1–7.5 ms.

Probe-on p50 latency (14.2–14.4 ms at c1, 32.0–32.8 ms at c8) matches the probe-binary-disabled condition (14.3 / 32.7 ms) and the control (15.3–15.6 / 36.6–42.4 ms); no obvious slowdown was observed in these short runs, but overhead remains unquantified — these are not an overhead benchmark.

Two probe defects are part of the record. First, the initial attempt (`.repro/rustfs-fsprobe-run`) lost the operation context at `put_object`'s detached commit-owner spawn, so 797/798 waits recorded `op=0` and could not be joined; the spawn is now wrapped in `propagate_op` like the tail-drain and fanout spawns, and [fs-probe-diagnostic.json](experiments/rustfs/results/fs-probe-diagnostic.json) is regenerated from the fixed `-v2` runs only, with that attempt's wait rows left as `missing: ["wait_begin"]`, never zero-filled. Second, the original ring wrapped with an atomic claim but a non-atomic store, which would race if a writer were descheduled across a wrap; these captures recorded 458,882/471,759 records against a 524,288-slot capacity with zero drops, so no wrap ever occurred and the race window never opened. Recording now stops at capacity and counts later records as dropped, so the analyzed dumps are unaffected.

### 10.1. Attribution of the Commit Wait: Stages, Thread CPU, Quorum Snapshot

A second, stage/CPU/quorum-instrumented probe revision answers that question on the same workload. It adds: eleven commit-path step tags (the original four plus `dest_meta_read`, `staged_meta_write`, `src_dir_sync`, `rename_data_dir`, `rename_meta`, `dst_dir_fsync`, `ancestor_fsync`); `RUSTFS_FS_PROBE_SUB`-gated inner-boundary *start* markers (`sub_scan`, `sub_prep_open`, `sub_prep_write`, `sub_fdatasync`, `sub_fsync_files`, `sub_dir_open`, `sub_dir_sync`, `sub_rename`) that delimit named call-wrapper intervals within a running closure (this capture generation records only start markers; an interval runs to the next marker or closure end — see the end-marker follow-up below); `CLOCK_THREAD_CPUTIME_ID` samples at job start/end (off-CPU = closure wall − thread CPU: kernel wait, lock wait, and descheduling combined); and a quorum snapshot in the `SEND_OK`/`SEND_ERR` record (`results_seen`, `write_quorum`, `disk_count`). Patches `tokio-1.53.2-fs-probe-v2.patch` (`d8abefac…`) and `rustfs-probe-v2.patch` (`3a78be53…`); server binary `fac3b2d1…`; two repetitions plus one bounded group-commit control, recording 477,085 / 458,028 / 622,241 records against a 2^20-slot ring with zero drops, zero duplicate boundaries, and zero orphan stage markers. Blocking-pool membership is derived per executor tid from the call-site names it executed (the two pools are disjoint runtimes): run-1 classified 45 main-pool, 33 fsync-pool, and 10 worker-loop threads, with no ambiguous tids.

**Representative slow PUT (run-1 c8, 56 jobs, every job now named).** Wait 172.5 ms = 84.1% of client latency 205.2 ms (run-2 c8: 107.2 ms = 74.1%); over 99.99% of the wait precedes `SEND_OK`; send→containing-poll 0.8 µs. The quorum record is the first direct measurement of the dependency structure: `results_seen=3, write_quorum=3, disk_count=4` — the response is produced by three of four disk acks, and the fourth disk's job chain may legitimately continue afterwards. Job groups (run-1 aggregates, complete closures):

| Job group | pool | n | wall p50 | wall p99 | max wall | stage intervals (representative op; call wrappers, see marker semantics above) |
| :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| `ancestor_fsync` (bucket dir, per disk) | fsync | 6,512 | 0.025 ms | 7.48 ms | **146.3 ms** | `sub_dir_sync` wrapper: start marker → closure end |
| `src_dir_sync` (object-dir files+dir) | fsync | 3,256 | 5.99 ms | 27.0 ms | 44.2 ms | `sub_fdatasync` wrapper 5.31 ms (marker precedes the open) + `sub_dir_sync` wrapper 2.16 ms |
| `staged_meta_write` (xl.meta) | main | 3,256 | 3.06 ms | 21.3 ms | 43.4 ms | `sub_prep_write` wrapper (write + optional fdatasync) dominates |
| `dst_dir_fsync` | fsync | 3,256 | 1.70 ms | 9.27 ms | 74.1 ms | `sub_dir_sync` wrapper dominates (2.63–3.93 ms) |
| `rename_data_dir` | main | 3,256 | 0.50 ms | 10.3 ms | 72.2 ms | `lead` (dir-guard prep) + `sub_rename` |
| `rename_meta` | main | 3,256 | 0.31 ms | 8.38 ms | 71.5 ms | `sub_rename` (renameat) dominates |
| `dest_meta_read` | main | 3,444 | 0.011 ms | 0.040 ms | 1.3 ms | µs-scale, as predicted |
| `mkdir` / `make_dir_all` | main | 6,528 / 3,256 | 0.033 / 0.069 ms | ~1.9 ms | 11.4 / 5.9 ms | negligible, as before |

**The dominant interval is a directory-sync call wrapper with very little thread CPU consumption.** The representative op's dominant `ancestor_fsync` job: wall 146.24 ms, thread CPU 0.20 ms, off-CPU 146.04 ms, stages `sub_dir_open` ≈ 0.01 ms then `sub_dir_sync` 146.23 ms. In this instrumentation generation `sub_dir_sync` is a *start* marker placed before `File::sync_all()`, and its stage runs to closure end: it is a call-wrapper interval — the call plus any trailing cleanup, and including any time the thread was runnable but descheduled — not a kernel entry/exit measurement of `fsync(2)`; kernel entry/exit tracing would be needed to claim exact syscall duration. What the interval does bound is application-side code: between the marker and closure end the only application code is the sync call and its immediate unwinding. Ten such jobs above 50 ms exist in run-1; all ten started within a single 0.39 ms window spanning five concurrent PUTs, and nine ended within ~0.2 ms of one another — a synchronized delay cluster consistent with a shared disturbance, but the data does not distinguish filesystem-level coordination from shared OS scheduling effects, and a thread CPU clock cannot separate runnable-but-descheduled time from kernel wait. Eight carried 0.14–0.32 ms of thread CPU (≥99.7% off-CPU); two outliers carried 38.6 and 80.6 ms of thread CPU alongside 65–108 ms off-CPU. Across all 79,062 complete closures in run-1, p50 wall 0.031 ms ≈ p50 thread CPU 0.030 ms — the median closure is near-pure CPU and the heavy tail is off-CPU. Tokio dispatch was separately bounded (`T0→T1` p50 8 µs in §10), and both group-commit code paths are compiled in but disabled by default in these runs.

**End-marker follow-up (v4, binary `8fc0577b…`, `-v3` patches).** A fresh capture adds explicit `_end` markers after each wrapped call, so every interval splits into start→end (the call wrapper) and end→closure-end (post-call residue). The capture is clean (509,709/502,365 records, zero drops, zero duplicate boundaries, zero orphan markers; 51,700 + 50,976 paired calls) and the long intervals reproduce. Run-1 contains one cluster of four `sub_dir_sync` wrappers of 328.9–349.0 ms whose start markers fall within 0.002 ms of each other; run-2 contains two clusters — four of 232.5–243.5 ms within 0.001 ms, and seven of 99.9–123.1 ms within 4.9 ms — and run-2's representative c8 op (client total 188.8 ms) spends 151.8 ms = 80.4% in the reconstructed commit wait, quorum 3/3-of-4. For every long wrapper the measured post-call residue is ≤0.013 ms: trailing cleanup after `sync_all()` returns contributes microseconds, so essentially the whole interval lies between the start and end markers, around the call. Thread CPU is bimodal *within* a single start-cluster — some wrappers consume 0.11–0.34 ms of closure thread CPU while others starting in the same window consume 40–175 ms — so simultaneous starts do not imply a uniform internal mechanism, and the earlier "scheduler delay excluded" reading would have been wrong for the high-CPU members. What `_end` markers still do not resolve: descheduling between the start marker and kernel entry, or between kernel return and the end marker, remains inside the wrapper — kernel entry/exit tracing is still required to split syscall from preemption. One run-1 closure carries a complete marker pair but no `job_end` (flush-boundary incomplete job); `calls` reports no row for it rather than estimating a duration.

**Post-`SEND_OK` tail.** 320/826 op keys in run-1 have at least one job ending after their send (636 jobs: 552 `ancestor_fsync`, 59 `dst_dir_fsync`) — the expected shape of a 3-of-4 quorum. The bulk end within ~1 ms of the send (median ≈ 0.5 ms); two reach ≈147 ms when the fourth disk's fsync lands inside the same stall; a handful of multi-second background stragglers are unrelated to response time (the send already fired). One of 851 waits has no send record (flush-boundary truncation) and is reported as `missing: ["send"]`, never zero-filled.

**Bounded controls.** (1) The existing strace capture (§ syscall diagnostic; control binary `dc577ce7…`, **1 MiB payloads per its saved manifest**, strace 7.2 with the `fdatasync,fsync,futex,epoll_*` filter, and the manifest itself flags `ptrace syscall tracing changes timing; diagnostic run only`): 5,232 paired `fsync`/`fdatasync` syscalls, with bucket-directory fsyncs recorded at p50 0.98 ms, p99 1.99 ms, max 4.97 ms. This is a separate, heavily traced capture; it cannot establish a normal baseline or a slowdown ratio for the probe runs, and none is claimed — it is descriptive context only (most directory syncs in this workload complete in ~1 ms under tracing). (2) One env-var run with `RUSTFS_EXPERIMENTAL_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE=true` (same instrumented binary, one repetition): `dst_dir_fsync` collapses from p50 1.70 ms / max 74 ms to p50 0.047 ms / max 1.6 ms — the batching machinery works and does not touch `ancestor_fsync`. That window contained no interval above 50 ms (`ancestor_fsync` max 9.4 ms), and its c8 throughput was 199 rps vs 119–130 rps in the stalled repetitions; the wait share (41.3%) is therefore not comparable to the stalled windows. The long intervals are episodic — one short start-cluster per multi-second tier — not a steady per-request cost.

**Revised conclusion:** a request-correlated directory-sync wrapper (bucket-directory fsync, and a tier below `fdatasync`/`fsync` on object metadata) contains a long interval — up to ~146 ms in the first instrumented capture and ~349 ms in the end-marker follow-up — with very little thread CPU consumption in its low-CPU members, and overlapping requests show synchronized delay clusters (start markers within 0.001–0.002 ms in two v4 clusters, and within 4.9 ms in the third). The response is produced at the 3-of-4 quorum while the fourth disk drains; internal batching (group-commit) is disabled in these runs, and Tokio dispatch was separately bounded as short. The wrapper interval is not a syscall-exact measurement, and its kernel and scheduling causes remain unresolved.

**What remains unresolved:** the kernel and scheduling causes of the long wrapper intervals — a thread CPU clock cannot separate journal commit vs ordered-data writeback vs device queueing inside the kernel from runnable-but-descheduled time, and no block-layer or kernel tracing (fsync entry/exit) was taken. The `_end` markers now bound the post-call residue at ≤0.013 ms, which localizes the time to the marker-delimited call region but does not split kernel execution from preemption inside that region. Also open: why thread CPU within one start-cluster is bimodal (0.11–0.34 ms vs 40–175 ms in the v4 clusters) when the wrappers start essentially simultaneously; whether the long-interval rate scales with concurrency or payload size (one workload; six short start-clusters observed across the two capture generations); and whether driver readiness contributes — no `Driver::turn` evidence exists for these runs. The capture-lifetime outliers (worker run-loops; 6.4–6.6 s aggregates in the first probe, 15.9 s in the follow-up) remain attributed to unparked worker threads spanning the capture — a job-shape classification, not a verified thread identity — and are excluded from request-correlated analysis; they are not device measurements. Per-job attribution inside the commit jobs is now complete for this workload, so the next measurement is on the kernel side (fsync/fdatasync duration via kernel or block-layer tracing on the 1 MiB workload), not more in-process instrumentation.

### 10.2. Kernel Trace Decomposition of the Long Wrappers (raw ftrace)

The open item of §10.1 — "kernel entry/exit tracing is still required to split syscall from preemption" — is now measured directly. Same 1 MiB workload, same c1/c8 tiers and v4 arguments, the same v4 binary (`8fc0577b…`), correlated raw ftrace (`trace_clock=mono`, sched + fsync/fdatasync events, no tracing tool installed) against the probe's `CLOCK_MONOTONIC` markers. The budget was stated before running (one smoke repetition, ≤2 traced repetitions, 1 probe-only repetition, honest stop if no long wrapper); five server runs happened in total: 2 smoke — the first smoke iteration failed validation because of broken sched filters and is disclosed as a tooling fix — 2 traced, 1 probe-only. Method, executed commands, and pinned semantics live in `experiments/rustfs/README.md` ("Kernel trace of the directory-sync wrappers"); the analyzer is `experiments/rustfs/fs_trace.py`, results with full provenance in `experiments/rustfs/results/fs-trace-diagnostic.json`.

**Observations (measured, with quality checks first):**

- Long wrappers reproduce under tracing: 118 marker-delimited wrapper observations ≥ 50 ms across the two traced reps (run-1: 73, run-2: 45), in clusters (run-1: five `sub_dir_sync` at 195.4–207.4 ms; run-2: five at 134.5–137.5 ms). The count is of observations, **not** of independent syscalls or waits: 12 run-1 `sub_fdatasync` observations nest inside `sub_fsync_files` (both inside `sub_scan`), 4 do so in run-2 — all within one job on one tid — so summed durations double-count shared thread time (verified from the captures; no partial overlaps exist in either rep).
- Clock validation (evidence-based, statuses in the results JSON): compatibility `validated` — the *selected* trace clock is `mono` and every probe dump header reports `clock_id=1`, both CLOCK_MONOTONIC; alignment `validated` — 27,700/27,700 paired sync wrappers contain their own syscall-enter, worst per-run match rate 1.0, offsets p50 +0.6 µs, min −0.301 µs within the 1 µs timestamp-quantization tolerance (max 1.6 ms — pre-entry user time inside the window, not clock skew), against stated ≥30-sample / ≥0.95-rate thresholds. `direct_subtraction` is `validated` with its reason; missing/mismatched metadata or out-of-tolerance offsets would yield `failed`/`insufficient_evidence` and withhold the cross-clock conclusions instead of printing a success flag. Trace window covers every probe marker; all probe tids (96/96, 84/84) appear in the sched stream.
- Loss: 1,534,228/1,534,228 entries, 0 overrun, 0 dropped, empty `error_log`, 0 dropped probe records — a capture-quality finding, not proof of causal attribution. Reconciliation: every wrapper decomposes to an exact tiling (max 0.000 ms) — an accounting consistency check, not independent proof of the state classifications — and the reported wrappers carry 0.000 ms unknown state time (including no `sched_waking` left incomplete by a missing `sched_wakeup`; `sched_wakeup_new` was not captured but none of these windows needed it); unknowns are reported as `unknown*`, never zeroed. Probe-side caveat: these dumps are format version 1 from the `-v3` probe generation, written before the `-v4` writer-admission flush barrier and raw-pointer ring-write fix (both preserved as patches with passing deterministic tests, but no capture has been run from that generation) — no corruption was observed (record counts, header/counter consistency, cross-field checks agree), yet clean counts do not prove memory safety.
- The wrapper ≈ the syscall: for the longest `sub_dir_sync` wrappers the fsync/fdatasync occupies all but ≤0.005 ms of the wrapper (flagship case: 207.438 ms wrapper, 207.434 ms fsync, enter 0.0005 ms after the start marker). Descheduling before kernel entry and after kernel return — the specific gap §10.1 could not close — is bounded at microseconds for these wrappers.
- Time accounting, non-overlapping (union per repetition and TID; never merged across threads or repetitions; thread-time, not client latency and not elapsed experiment time): run-1 4,865.8 ms over 49 tids, run-2 3,907.1 ms over 36 tids = 8,772.9 ms total — 8,246.6 ms `blocked:D`, 495.9 ms `running` (scheduled residency, not exact CPU execution; interrupts may run while the task is current), 24.7 ms runnable, and 5.7 ms in the `sched_waking` → `sched_wakeup` transition (neither blocked nor yet runnable: `sched_waking` fires while wakeup processing is still in progress, and only `ttwu_do_wakeup` — after setting the task `TASK_RUNNING` — emits `sched_wakeup`; verified against Linux v6.19 `kernel/sched/core.c`, distinct event kinds; equal-timestamp ties keep each CPU's recorded sequence as a hard constraint (never reversed), repair causally impossible orders, mark cross-CPU choices as ambiguous, and report conflicts with the recorded order as uncertainty instead of re-sorting). The union of traced syscall windows inside those wrappers is 6,655.9 ms (6,136.7 ms `blocked:D`, 492.0 ms `running`, 21.9 ms runnable, 5.3 ms `sched_waking` → `sched_wakeup` transition); the remaining ≈2.1 s of wrapper-region time is untraced kernel operations (`sub_scan`, `sub_rename`, `sub_prep_*`), plus ≤3 ms pre-entry/post-exit residue. Per-tag sums are reported separately and explicitly marked overlapping: `sub_dir_sync` 36 obs / 4,189.7 ms, `sub_rename` 18 / 1,774.9 ms, `sub_scan` 16 / 1,485.6 ms, `sub_fsync_files` 16 / 1,448.1 ms, `sub_fdatasync` 16 / 1,447.9 ms, `sub_prep_write` 12 / 983.0 ms, `sub_prep_open` 4 / 339.7 ms. Per-wrapper, 112/118 are more than 60% `blocked:D`; 7/118 are on-CPU-heavy (>30% `running` inside the syscall), one wrapper in both groups (6 are on-CPU-heavy without being D-dominant) — the v4 thread-CPU bimodality reappears as a kernel-state split, consistent with per-member mechanism differences inside one start cluster.
- A representative request-linked decomposition (run-1, `sub_dir_sync` tid 2210377, 207.438 ms): 122.4 ms `blocked:D` + 81.2 ms `running` (scheduled residency) + 2.4 ms runnable + 1.4 ms `sched_waking` → `sched_wakeup` transition within one fsync. Identity: blocking job 49150, operation hash 2028158358628619772, step `ancestor_fsync`, occurrence 1/1, executor tid 2210377; the commit wait began 23.1 ms before the wrapper and overlaps it, and the `send_ok` closed it at +207.8 ms with results_seen=3 / write_quorum=3 / disk_count=4 — association and overlap do **not** establish that this job was response-critical (JSON: `required_before_response: not_established`), and a 3-of-4 quorum count does not identify which disk acknowledged. The D-time is fragmented (hundreds of ~0.1 ms sleeps interleaved with ~0.1 ms runs) plus one continuous 73 ms D-block and one 10.8 ms D-block whose wake lands ~14 µs before fsync returns.
- Cluster timing: tight-group entries span 0.000–2.3 ms (run-2 A: four enters within 0.000 ms; run-1 dir_sync group: five within 2.286 ms) and releases are synchronized in sub-groups — exit spreads within groups of 0.012–0.284 ms for the dir_sync groups, 0.101–4.765 ms across the larger auto-grouped episodes.
- Perturbation reference (probe-only rep, tracer off): paired-call p50 differs by ≈+0.066 ms for `sub_dir_sync` between traced and untraced reps; long-wrapper counts differ (36 traced vs 0 probe-only; 2 reps vs 1). Short runs with unequal counts — a comparison, not a negligible-overhead result.

**Inferences (consistent with the data, not established):**

- The long wrappers are dominated by uninterruptible kernel sleep *inside* the fsync/fdatasync call; user-space code and pre/post-syscall descheduling are ruled out at microsecond scale for these wrappers.
- Simultaneous sub-group releases are consistent with waiters released by a shared kernel event (a journal/transaction completion is the natural candidate), but no block-layer or filesystem-internal events were captured, so no specific event is named and association ≠ causation.
- The on-CPU-heavy members show the same wrapper shape can also spend tens of milliseconds scheduled inside the kernel (`running` state while current — interrupted time may still be included) — consistent with v4's high-thread-CPU members, still without a per-member mechanism.

**Still unresolved:** which kernel wait the `D` state represents (journal commit, ordered-data writeback, lock contention, or device queue) — that requires btrfs/block-layer events on the same workload, i.e. a *new stated tracing budget* beyond the ≤2 traced + 1 probe-only reps used here; whether release sub-groups correlate with any specific filesystem event; the exact kernel path of the on-CPU-heavy members; and the scaling questions left open in §10.1 (concurrency, payload size, episodic rate). No RustFS behavior was changed in any of this work.

### 10.3. Event-Guided Kernel-Wait Diagnostic (isolated tracefs instance, `-v4` probe generation)

The §10.2 unresolved item — *which kernel wait the `D` state represents* — was attacked with a separately stated budget: a predeclared event set chosen from host rate-probe evidence, the live-validated isolated-instance workflow, a freshly built `-v4` probe generation (the first runtime-validated one), ≤2 traced repetitions plus 1 probe-only reference, and no automatic extension. Method, per-event rationale, executed commands, and the evidence rules live in `experiments/rustfs/README.md` ("Event-guided kernel-wait diagnostic"); results with full provenance in `experiments/rustfs/results/fs-trace-diagnostic-v4.json`. All artifacts of this phase are deliberately **uncommitted** for review; the committed §10.2 baseline (`fs-trace-diagnostic.json`) regenerates **byte-identically** from the analyzer changes.

**Observations (quality checks first):**

- **Build and smoke, recorded before any claim of runtime validity.** Patches byte-verified against pristine pinned sources (tokio 1.53.2, rustfs `6b155400`): `tokio-1.53.2-fs-probe-v4.patch` SHA-256 `7bd05478…`, `rustfs-probe-v4.patch` SHA-256 `7845d734…`; build `BUILD_EXIT=0` (83 m 50 s, rustc/cargo 1.99.0, offline); new binary `.repro/rustfs-probe/target-v4/release/rustfs` SHA-256 `e8c4f77375f7055825139900c408b5d1bdbc5e4d90e724db0f5671f7a3e95f3e`. Warnings: 16 = 15 new (missing-doc inside the patch's probe modules) + 1 pre-existing (deprecated `Atomic::fetch_update` in a file the patch does not touch); with no baseline log, classification is by patch-touched path. The only copy of the workload-v4 binary (`8fc0577b…`, format-version-1 dumps in §10.2) was preserved first and still compares equal. The smoke run (1 rep × c8 × 1 s, fresh dirs) then passed: format-version-2 dump (`version=2`, `rejected_closed=0` as an integer where version 1 would carry `None`), `records = stored_records = total_seen = 124,191`, `dropped_records=0`, `rejected_capacity=0`, file size exactly 88 + 124,191 × 40 bytes, manifest binary SHA matches `e8c4f773…`, client 173/173 OK — compile success alone was never called validation, and `rejected_closed=0` is an observation that no post-close write arrived, **not** proof of the admission barrier (a non-zero value after gate close would be legitimate gate behaviour, not corruption). No corrective repeat was needed.
- **Instance workflow live-validated on this host** (the §10.2 script's previously fixture-only claims): arm creates and owns the instance, sets `mono`/`overwrite=0`/filters (block filter `dev == 271581184` accepted; a bogus filter fails the write and the `arm` exits non-zero with `tracing_on=0` — fail-closed, verified); ownership refusals fire on all five commands for a missing marker and a foreign-root marker, leaving the instance untouched and creating no files; a nonempty buffer refuses `destroy` and re-`arm` without `--force` (never used); `collect` stops before reading (`tracing_on=0` in `trace.settings`, `instance=`/`instance_owned=yes` recorded) and its raw contains correlatable `python3-<tid> … sys_fsync(fd: 3)` lines; `off` is repeatable; `destroy` on an empty ring removes instance and marker while preserving collected artifacts; the **default tracer was byte-identical to baseline before and after everything** (`nop`, `tracing_on=0`, `buffer_size_kb=16387`, `overwrite=0`, zero enabled root events, `[mono]`).
- **Topology, verified before any disk language:** the four RustFS volume directories live on **one btrfs filesystem, subvolume `/home` of a single NVMe** (`/dev/nvme0n1p3`, KIOXIA KXG80ZNV512G, device `259,0` as printed in block events) — one physical device, never four independent disks; `zram0` is swap only. Block events were capture-filtered to `dev == 271581184` (259<<20) at arm time.
- **Capture quality:** 4,063,712/4,063,712 entries (zero overrun), empty `trace.error_log`, probe `dropped_records=0` and `rejected_closed=0` in both runs, 4,063,712 lines parsed with 0 bad, unknown-state total 0 ms; clock compatibility/alignment/direct-subtraction all `validated`. Equal-µs ties: 32,844 observed, 8,959 causally repaired, 4,761 marked ambiguous, 7,832 unresolved — surfaced as uncertainty, never silently re-sorted.
- **Predeclared event set** (8 events: `btrfs_transaction_commit`, `btrfs_finish_ordered_extent`, `btrfs_reserve_ticket`, `btrfs_tree_lock`, `folio_wait_writeback`, `block_bio_queue`, `block_rq_issue`, `block_rq_complete`) with per-event rationale and exclusions in `ftrace.sh`/README; buffer 32,768 KB per CPU. Measured totals in this capture: tree_lock 999,372; bio_queue 701,204; rq_issue 353,884; rq_complete 357,749; finish_ordered 415,848; folio_wait 45,217; transaction_commit 31; plus sched 558,185/295,538/295,538 and fsync/fdatasync enters 13,578/6,995. Excluded `balance_dirty_pages` with the predeclared note "add only if evidence points there" — the unresolved wait below leaves dirty-page throttling as a candidate hypothesis to evaluate.
- **Long intervals reproduced** under tracing: 494 wrapper observations ≥ 50 ms (run-1 406, run-2 88; `sub_dir_sync` alone 82). Client tiers: run-1 c1 p50 40.7 ms / c8 p50 250.2 ms (98 c8 attempts), run-2 c1 p50 17.9 ms / c8 p50 48.6 ms (458 attempts), probe-only c1 16.4 ms / c8 42.5 ms — a 5.2× c8 spread *under identical tracing*.
- **Non-overlapping accounting** (union per repetition and TID; thread-time, not client latency, not elapsed): wrapper regions 29,804.8 ms total (run-1 24,250.2 over 74 tids, run-2 5,554.6 over 39) = 27,534.9 ms `blocked:D`, 820.3 ms `running`, 1,426.9 ms runnable, 22.7 ms `sched_waking` → `sched_wakeup`; traced-syscall union 19,197.7 ms. Per-tag sums remain explicitly overlapping: `sub_dir_sync` 82 obs / 7,982.1 ms (7,158.8 ms `blocked:D`).
- **Flagship decomposition** (run-1 `sub_dir_sync`, job 138, tid 3917276, one 275.842 ms fsync, `association=missing`): zones `blocked:D` 166.637 ms / `running` 108.159 ms / runnable 0.632 ms / wakeup_transition 0.409 ms, splitting into two phases. **Phase A (+0.182 → +153.275): two giant back-to-back `blocked:D` segments of 69.709 ms and 83.365 ms** — the thread asleep 99.98 % of the first 153 ms. **Phase B (+157.678 → +275.836): mostly on-CPU** (11,575 `btrfs_tree_lock` acquisitions by this thread with hold p50 0.2 µs; 2,235 bios submitted by the thread itself; 91 micro-segments totalling 14.6 ms), ending with `btrfs_transaction_commit root=1(ROOT_TREE) gen=7876` **fired by this thread 6 µs before the wrapper ends** (window generations 7874/7875 → 7876 at the fsync's own commit).
- **Evidence at the giant edges, under the predeclared rules.** The 83.365 ms segment: one *isolated* `btrfs_finish_ordered_extent ino=4517992` at +153.116 ms — 159 µs before the wake edge, itself preceded 24 µs earlier by `folio_wait_writeback bdi=btrfs-1 ino=4517992` from another thread — with `waker_comm=kworker/u48:0`: classified `isolated_temporal_candidate` by the isolation rule (at most one event of that class inside the segment, within the 1 ms threshold). Selected by temporal proximity and sparsity; no dependency match to the blocked task was established, a single candidate among captured events does not exclude untraced causes, and waker_comm is execution context on the CPU, not necessarily the logical producer or releasing subsystem. The 69.709 ms segment: 440 completions during it → proximity is **dense and therefore vacuous** (at 6–9k completions/s any edge is always within 1 ms of some completion), reported only as `proximity_summary`, and its `waker_comm=spotify` is an irq-attribution artifact (the task current on the CPU when the wake fired), not a logical waker. Aggregate over the six representative wrappers: 810 wake edges = 527 isolated / 259 dense / 24 with no completion inside; wakers = 752 unbound `kworker/u48:*`, 43 peer `rustfs-worker`/`rustfs-fsync`, 15 irq-attribution artifacts (`<idle>`, `ai.opencode.des`, `spotify`, `Compositor`, `ThreadPoolForeg`). One `demonstrable_dependency` exists in the capture (run-2 `sub_scan`: the wrapper thread's own `folio_wait_writeback` at +1.080 ms at/before its own switch-out, establishing that the task encountered that wait path; whether it explains the full subsequent blocked interval remains inferred).
- **Probe-only comparison:** `sub_dir_sync` traced p50 1.462 ms vs probe-only 1.295 ms (Δ +0.167 ms), max Δ +161.9 ms — but the run-1 vs run-2 client spread under tracing (5.2×) exceeds the traced/untraced delta, so at 2+1 repetitions tracing perturbation cannot be separated from system-state variance.

**Inferences (consistent with the data, not established):**

- The §10.2 "`D`-dominant monolith" now resolves into a **wait phase and an own-work phase inside one fsync**: the first 153 ms is dominated by uninterruptible sleep containing an isolated completion candidate near the second edge, while the final ~108 ms of scheduled time is the thread performing its own metadata writeback and the transaction commit — refining "wrapper ≈ syscall" to "syscall ≈ wait-phase + own-commit-phase" for this flagship.
- The isolated finish/folio/wake chain at the second giant edge is an isolated temporal candidate sequence compatible with, but not proof of, writeback release (the recorded wake context was `kworker/u48:0`, but no producer/consumer dependency link to the blocked task was established, and waker attribution reflects CPU context at wake time).
- The unbound-kworker waker dominance (752/810) shows that wakeups occurred while kworker threads were active on CPU, which is compatible with, but not proof of, writeback-mediated release.

**Still unresolved:**

- **What the giant waits block on.** The 69.709/83.365 ms flagship segments (and counterparts in the other representatives: 189.910+53.702, 86.556, 72.723+39.035+22.738, 52.073 ms) are *not* explained by any captured event class: tree locks are sub-µs, `reserve_ticket` never fires, the only in-window transaction commit is at exit, no same-task folio wait exists in the flagship, and only one isolated finish candidate sits at one edge. Candidate hypotheses for a separately approved budget include `balance_dirty_pages` (dirty-page throttling is one possible wait mechanism for metadata producers when writeback is active, though not established by current events) or a bounded blocked-thread stack sample (`/proc/<tid>/stack`, root-only, point-in-time, subject to wakeup races). A future measurement would need to trace the task's actual wait path, link a completion directly to that wait, and distinguish dirty-page throttling, writeback, transaction coordination, lock contention, and device queueing; neither a single stack sample nor a nearby event alone establishes the full causal mechanism.
- **Response-criticality** remains `not_established` for every wrapper: the flagship job has `association=missing` (no operation context), and none of the four evidence levels addresses response ordering by design — a job's temporal association with a filesystem or block event never shows the event was required before the response, and no 3-of-4 quorum is interpreted as per-disk identity.
- **Device latency** is not derived from fsync duration: request pairing by (dev, sector, count) is incomplete under merges/splits (353,884 issues vs 357,749 completions), so no queue/service-time claims are made.
- Whether release sub-groups correlate with one specific filesystem event (candidates exist for only one giant edge); why run-1 was degraded at *both* tiers under identical tracing; and the §10.1 scaling questions. No RustFS behavior was changed; no performance fix was attempted here.

---

## Possible Future Upstream Directions

*(Conceptual considerations for future maintainer discussions; no concrete API proposal is made here.)*

1. **Driver Turn Lifecycle Hooks:**
   A hook or metric tracking when `Driver::turn()` begins and ends, and the interval between driver turns. This would allow telemetry tools to instantly detect when the runtime has failed to service I/O due to compute saturation.
2. **Worker Unpark Notification Event:**
   A hook invoked at `Unparker::unpark()` recording `(target_worker_id, reason, timestamp)`. Pairing this with the existing `on_thread_unpark()` would allow external tools to directly compute worker wakeup latency.
3. **Wake Decision Visibility:**
   A lightweight signal when `worker_to_notify` decides not to wake a worker because another worker is already searching, allowing telemetry systems to differentiate intentional scheduler coalescing from resource starvation.
4. **Queue Placement Metadata:**
   Exposing whether a task was placed on a worker-local queue, the LIFO slot, or the global injection queue within `TaskMeta`.

### Follow-up: request-linked Btrfs transaction wait paths

The separately bounded switch-out-stack measurement is documented in the
RustFS README, with results in `wait-path-targeted-stacks.json`. Six
request-linked directory-sync wrappers reproduced at 77–79 ms; five have
approximately 40 ms D segments in Btrfs `wait_for_commit`. Stacks also
identify transaction admission waits through `wait_current_trans`. This
establishes an encountered transaction wait path for these samples, rather
than inferring it from completion proximity. It does not establish why the
transaction progressed slowly or identify the transaction/releasing entity.
For one request, approximately 52.5 ms of its long wrapper occurs after
SEND_OK and cannot block the response. Other matched jobs finish before
send, but the required disk-acknowledgement dependencies remain unmeasured.
The earlier two-repetition trace lacked stack coverage of its long linked
wrappers because the trigger was exhausted; this failed coverage result is
preserved. No performance fix follows from these captures alone.

### Follow-up: Joint probe and kernel-stack capture of quorum-triggering fsyncs

A subsequent bounded joint capture (`.repro/rustfs-waitpath-v5-joint`) on `rustfs-v5`
closes the cross-capture evidence gap by combining the v5 per-disk acknowledgement probe
with kernel ftrace sched-switch stack sampling in the same live run. Full details are
documented in `experiments/rustfs/README.md`, with results in `fs-trace-waitpath-v5-joint.json`,
`wait-path-v5-joint-stacks.json`, and `wait-path-v5-joint.json`.

In this capture, all 560 client tier PUTs completed HTTP 200 without losses or dropped probe records (560 c8 requests in tiers.json, matching measurement.json; 584 probe send records including warmup). For a representative slow successful PUT (`c8/82.bin`, client latency 85.97 ms), Disk 0's `dst_dir_fsync` job was a source-established prerequisite that triggered write quorum (success count 2 -> 3). A 27.41 ms blocked segment began with a sampled `wait_for_commit` stack under `btrfs_commit_transaction` inside the same live capture. Disk 3 was also counted before quorum and its 27.42 ms blocked segment began with a matching `wait_for_commit` stack, while Disk 2 completed before the stall (+44.60 ms) and Disk 1 completed as a post-quorum tail after SEND_OK (+85.21 ms).

This establishes that an encountered Btrfs transaction wait path delayed the specific filesystem prerequisite on the disk that produced write quorum for that request. It establishes the encountered path at the switch-out, not continuous residence in that function throughout the sleep. It does not establish transaction identity, the releasing entity, or why the commit required 27 ms, nor does it prove all slow requests share this cause.
