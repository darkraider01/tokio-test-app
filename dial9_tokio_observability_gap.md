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
   A temporary, minimal set of research probes instrumented directly inside Tokio scheduler internals (`driver.rs`, `scheduled_io.rs`, `harness.rs`, `worker.rs`, `idle.rs`, `park.rs`). It records nano-precision monotonic timestamps into an in-memory ring buffer. **It is used strictly as ground truth to expose actual internal state transitions and is not a proposed production design.**
2. **`stock recorder` (Maximum Stock Tokio Observability):**
   Uses only public and unstable Tokio hooks currently available in upstream Tokio (`tokio_unstable`): `on_task_spawn`, `on_before_task_poll`, `on_after_task_poll`, `on_task_terminate`, `on_thread_park`, `on_thread_unpark`, and `TaskMeta::schedule_latency()`.
3. **`Dial9` (Actual Current Telemetry Consumer):**
   The real Dial9 telemetry pipeline compiled with `features = ["analysis"]`. Spawns tasks via `dial9_tokio_telemetry::spawn`, captures raw segments via `CapturingProcessor`, and decodes wire-format events: `WakeEvent`, `WorkerParkEvent`, `WorkerUnparkEvent` (with Linux `schedstat` / `sched_wait_ns`), `PollStartEvent`, and `PollEndEvent`, followed by Dial9's `compute_wake_to_poll_delays()`.
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

### Boundary A: External I/O Stimulus $\to$ Tokio Driver Observation
- **Interval:** External I/O stimulus initiated (`WRITE_BEGIN`/`WRITE_DONE`) $\to$ Tokio `driver.turn()` (`mio::Poll::poll`) returns readiness.
- **Methodological Note:** Our start timestamp is `WRITE_BEGIN`/`WRITE_DONE` on the sending thread, not the exact instant the receiving socket became kernel-readable in the OS network stack. (Proving the exact kernel-readiness timestamp would require kernel/eBPF instrumentation). However, comparing the parked-worker control ($\Delta_{\text{io\_driver}} \approx 0.103\text{ ms}$) against worker CPU saturation ($\Delta_{\text{io\_driver}} \approx 29.834\text{ ms}$) definitively demonstrates a ~30 ms pre-scheduling blind spot where the runtime fails to service the driver while workers are occupied.
- **Core Question:** Can current Tokio hooks or Dial9 detect when an application is waiting for Tokio to service the I/O driver while worker threads are saturated by CPU-bound tasks?

### Boundary B: Runnable Work $\to$ Wake / Coalesce Decision
- **Interval:** Work becomes runnable (`Harness::wake_by_val` / `Handle::schedule_task`) $\to$ `Idle::worker_to_notify()` evaluates whether to unpark an idle worker or suppress the wake.
- **Core Question:** When multiple tasks are awakened in rapid succession, can an observer distinguish intentional wake suppression from execution delays?

### Boundary C: Worker Notification $\to$ Worker Resume
- **Interval:** `Unparker::unpark()` initiates notification $\to$ target worker thread unblocks from kernel park and resumes user space.
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
| **`WorkerUnparkDispatched`**| `tokio/src/runtime/scheduler/multi_thread/park.rs` | `Unparker::unpark()` | [`park.rs#L316-L330`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/park.rs#L316-L330) | Timestamp, target worker, notification mechanism (`condvar`/`mio_waker`) | Completion of atomic swap & OS notification dispatch |
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

#### Distribution (N=30 Runs)

| Interval | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **$\Delta_{\text{io\_driver}}$** (External Stimulus $\to$ Tokio Driver Observation) | 0.081 ms | **0.103 ms** | 0.148 ms | 0.152 ms |
| **$\Delta_{\text{schedule}}$** (Tokio Driver Observation $\to$ Task Scheduled) | 0.002 ms | **0.003 ms** | 0.004 ms | 0.004 ms |
| **$\Delta_{\text{poll}}$** (Task Scheduled $\to$ Worker Poll Start) | 0.019 ms | **0.023 ms** | 0.034 ms | 0.047 ms |
| **$\Delta_{\text{end\_to\_end}}$** (External Stimulus $\to$ Worker Poll Start) | 0.103 ms | **0.130 ms** | 0.178 ms | 0.180 ms |

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
+  DIAL9 WORKER_UNPARK: worker=0 tid=7606 sched_wait=None
+  DIAL9 POLL_START: worker=0 task_id=23 loc=src/main.rs:589:25
+  DIAL9 COMPUTED WAKE-TO-POLL DELAY: 0.026ms
```

---

### Case E: External I/O Stimulus Under Scheduler / CPU Saturation

- **Workload:** An async reader task is registered on a TCP stream. All $N$ workers (tested with $N=2$ and $N=4$) are occupied with non-yielding CPU compute loops (spin loops) for 40–60ms. While all workers are 100% occupied, an external thread sends a TCP packet at $t \approx 15\text{ ms}$.
- **Key Observation:** Because all workers are executing compute loops, **no worker turns the I/O driver**. The TCP packet sits in the OS kernel receive buffer for ~30ms before Tokio discovers it.

#### Distribution (N=30 Runs)

| Interval | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **$\Delta_{\text{io\_driver}}$ (Driver Service Delay: Stimulus $\to$ Observation)** | 29.752 ms | **29.834 ms** | 29.867 ms | 29.871 ms |
| **$\Delta_{\text{schedule}}$ (Tokio Observation $\to$ Task Scheduled)** | 0.001 ms | **0.001 ms** | 0.002 ms | 0.002 ms |
| **$\Delta_{\text{poll}}$ (Task Scheduled $\to$ Worker Poll Start)** | 0.004 ms | **0.005 ms** | 0.012 ms | 0.030 ms |
| **$\Delta_{\text{end\_to\_end}}$ (Total Physical Real Latency)** | 29.758 ms | **29.841 ms** | 29.872 ms | 29.877 ms |

#### The Critical Observability Inversion

```text
Timeline of Real Events vs Telemetry Views in Case E:

Time        Physical Reality                       Stock Tokio View                Dial9 View
---------   --------------------------------       ----------------------------    ----------------------------
t=15.30ms   External TCP Write Sent (WRITE_BEGIN)
            [Packet in kernel socket buffer]       (completely invisible)          (completely invisible)
            ... 29.83 ms latency ...               (completely invisible)          (completely invisible)
t=60.13ms   Compute ends; Driver.turn() called     (no event emitted)              (no event emitted)
t=60.14ms   Reader Task Scheduled                  set_scheduled_at(60.14ms)       WakeEvent captured
t=60.14ms   Reader Task Polled                     schedule_latency: 0.007 ms!     wake_to_poll: 0.007 ms!
```

- **Reported `TaskMeta::schedule_latency()`:** **$0.007\text{ ms (7 \mu s)}$**
- **Reported Dial9 `wake_to_poll_delay`:** **$0.007\text{ ms (7 \mu s)}$**
- **Actual Application-Experienced Delay:** **$29.841\text{ ms}$**

**Result:** Both stock Tokio and Dial9 report sub-10-microsecond schedule latency, hiding **99.98% of the real latency**. The delay occurred entirely before Tokio serviced the I/O driver, rendering the driver service starvation completely invisible to the application telemetry.

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
| **Unpark Requests Dispatched** | **1** | **1** | 1 | 1 |
| **Task-Correlated Wake Suppressions (Tasks 2–5 Coalesced)** | **4** | **4** | 4 | 4 |
| **Total `target=None` Scheduler Decisions in Window** | **5** | **5** | 5 | 5 |
| **Task 1 Sched $\to$ Poll Latency** | 0.045 ms | **0.058 ms** | 0.082 ms | 0.108 ms |
| **Coalesced Tasks Sched $\to$ Poll Latency** | 0.010 ms | **0.035 ms** | 0.044 ms | 0.115 ms |

*Diagnostic Note on Earlier `min=0` Observations:*
In earlier un-synchronized prototype runs, occasionally `Unpark Requests Dispatched` reported `min=0` and `Task-Correlated Suppressions` reported `min=0`. Root-cause investigation showed this was a workload setup race: the blocker task was on an open-ended timer (40ms) while setup slept. If OS scheduling pauses delayed the setup thread, the blocker finished early, leaving Worker 0 idle, or if Worker 1 was still exiting searching mode when Task 1 arrived (`num_searching > 0`), Tokio suppressed Task 1's unpark too. Establishing explicit synchronization (atomic spin control on Worker 0, verified `poll_fn` registration on all 5 waiting tasks, and causal task tracking) eliminated setup nondeterminism across all 30 benchmark runs.

#### Exact Causal Ground Truth Timeline

```text
+   0.179 ms  TASK_SCHEDULED task_id=41 is_local=false
+   0.179 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify_selected task_id=Some(41) target=Some(1) searching=1 unparked=2/2
+   0.180 ms  WORKER_UNPARK_REQUESTED target_worker=1 prev_state=PARKED_CONDVAR
+   0.180 ms  WORKER_UNPARK_DISPATCHED target_worker=1 mechanism=condvar
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

#### Distribution (N=30 Runs)

| Metric | Min | Median (p50) | p95 | Max |
| :--- | :--- | :--- | :--- | :--- |
| **Stolen Tasks Sched $\to$ Poll (Worker 0)** | 0.002 ms | **0.002 ms** | 0.004 ms | 0.005 ms |
| **Stranded Task Sched $\to$ Poll (Worker 1 LIFO slot)** | 40.015 ms | **40.019 ms** | 40.036 ms | 41.201 ms |

#### Ground Truth Timeline

```text
+   0.117 ms  TASK_SCHEDULED task_id=49 is_local=true  (Worker 1 queue)
+   0.120 ms  TASK_SCHEDULED task_id=50 is_local=true  (Worker 1 queue)
+   0.121 ms  TASK_SCHEDULED task_id=51 is_local=true  (Worker 1 queue)
+   0.123 ms  TASK_SCHEDULED task_id=52 is_local=true  (Worker 1 LIFO slot)
+   0.129 ms  WORKER_POLL_START worker=0 task_id=49   <-- STOLEN by Worker 0
+   0.148 ms  WORKER_POLL_END   worker=0 task_id=49
+   0.152 ms  WORKER_POLL_START worker=0 task_id=50   <-- STOLEN by Worker 0
+   0.153 ms  WORKER_POLL_END   worker=0 task_id=50
+   0.154 ms  WORKER_POLL_START worker=0 task_id=51   <-- STOLEN by Worker 0
+   0.154 ms  WORKER_POLL_END   worker=0 task_id=51
+   0.158 ms  WORKER_PARK_WAIT_BEGIN worker=0 kind=driver <-- Worker 0 goes idle!
... [Worker 1 computes for 40 ms while Worker 0 sleeps] ...
+  40.134 ms  WORKER_POLL_END   worker=1 task_id=48   (Parent compute finishes)
+  40.148 ms  WORKER_POLL_START worker=1 task_id=52   (Stranded task finally runs!)
```

- **Stock Tokio View:** Reports `on_before_task_poll task_id=52 schedule_latency=40.019ms`.
- **Dial9 View:** Captures `POLL_START: worker=1 task_id=52` and computes `wake_to_poll_delay = 40.019ms`. Dial9 notes `sched_wait=None` (not an OS runqueue delay).
- **Finding:** Both stock Tokio and Dial9 accurately observe that Task 52 had a 40ms delay, but **neither can explain why**. The telemetry provides no visibility into queue placement (local run queue vs LIFO slot vs injection queue), nor the fact that Worker 0 was idle and parked while Task 52 sat stranded on Worker 1.

---

## Ground Truth vs Stock Tokio vs Dial9

| Event / Internal Fact | Internal Ground Truth (`tokio-probe`) | Stock Tokio (`tokio_unstable`) | Dial9 Telemetry | Status |
| :--- | :--- | :--- | :--- | :--- |
| **External I/O packet sent (`WRITE_BEGIN`)** | Exactly recorded (`ExternalIoStimulus`) | **Invisible** | **Invisible** | **REAL GAP** |
| **I/O driver turns (`Driver::turn`)** | Exactly recorded (`IoReadinessObserved`) | **Invisible** (only aggregate counters) | **Invisible** | **REAL GAP** |
| **Driver starvation under CPU saturation** | Directly measured ($\Delta_{\text{io\_driver}} \approx 30\text{ ms}$) | **Invisible** (reports $<10\ \mu\text{s}$) | **Invisible** (reports $<10\ \mu\text{s}$) | **REAL GAP** |
| **I/O waker dispatched** | `ResourceWakeDispatched` | **Invisible** | `WakeEvent` (if `WakeTraced`) | **Inferable** |
| **Task scheduled timestamp** | `TaskScheduled` (`is_local` recorded) | Stamped internally into `TaskMeta` (no public event) | Stamped into `TaskMeta` | **Inferable at poll** |
| **Task queue placement (Local vs Injected)** | `is_local: bool` | **Invisible** | **Invisible** | **REAL GAP** |
| **Scheduler wake decision (Notify vs Coalesce)**| `SchedulerWakeDecision` (`target`/`None`)| **Invisible** | **Invisible** | **REAL GAP** |
| **Worker unpark requested** | `WorkerUnparkRequested` | **Invisible** | **Invisible** | **REAL GAP** |
| **Worker unpark dispatched** | `WorkerUnparkDispatched` (mechanism) | **Invisible** | **Invisible** | **REAL GAP** |
| **Tokio park wait returns (`Parker::park` exit)** | `WorkerParkWaitEnd` (return timestamp & state) | **Invisible** | Sampled Linux `schedstat` | **Partial / Ambiguous** |
| **Worker thread resumes execution** | `WorkerResumed` (worker ID recorded) | `on_thread_unpark()` (0 args) | `WorkerUnparkEvent` | **Ambiguous** |
| **Task poll start** | `WorkerPollStart` (worker + task ID) | `on_before_task_poll` | `PollStartEvent` | **Observable** |
| **Task schedule latency** | Nano-precision math | `TaskMeta::schedule_latency()` | `compute_wake_to_poll_delays` | **Observable** |
| **Work-stealing head-of-line blocking** | Worker idle vs stranded queue trace | High schedule latency, zero context | High delay, zero context | **REAL GAP** |

*Note on Park Return:* `WorkerParkWaitEnd` tells you when Tokio's park wait returned. The request $\to$ park-return interval contains notification mechanics, kernel scheduling, condvar/mio wake, and lock reacquisition. Dial9's sampled `schedstat` (`sched_wait_ns`) is the signal that partially isolates Linux runqueue wait. Because upstream Tokio exposes no matching `Unparker::unpark()` dispatch timestamp, request $\to$ resume latency cannot be directly reconstructed.

---

## Findings

### Finding 1: Wake suppression / coalescing is completely invisible externally
Tokio's scheduler intentionally coalesces wakeups: if an idle worker is already in the `searching` state, `Idle::worker_to_notify()` returns `None`. In our 30-run benchmark across 5 sequentially notified tasks under confirmed scheduler preconditions (30 attempted runs, 30 valid intended-precondition runs, 0 alternate topology runs), exactly 1 unpark request was dispatched, exactly 4 task-correlated wakes were suppressed (tasks 2–5 coalesced onto worker 1), and 5 total `target=None` scheduler decisions occurred in the measurement window. External observers see 5 tasks awaken and 1 unpark event, with no mechanism to determine whether later tasks were intentionally batched onto the running worker or delayed by contention.

### Finding 2: Worker notification $\to$ resume latency cannot be directly measured
Upstream Tokio provides `on_thread_unpark()`, but this callback takes 0 arguments and fires *after* the thread has already resumed execution in user space. There is no timestamp for when the unpark was requested. Therefore, the interval $T(\text{resumed}) - T(\text{unpark\_requested})$ cannot be measured. Furthermore, an observer cannot correlate which task or I/O event caused the worker to resume.

### Finding 3: External I/O stimulus $\to$ Tokio driver service latency is invisible and can invert telemetry
When workers are occupied by CPU-intensive cooperative tasks, `Driver::turn()` is not called. In our Case E benchmark across 30 runs, external TCP traffic was sent, but Tokio did not service the driver for $\sim 29.83\text{ ms}$. Once the driver finally turned, the reader task was scheduled and polled within $6\ \mu\text{s}$. Both stock Tokio's `TaskMeta::schedule_latency()` and Dial9's `wake_to_poll_delay` reported $0.007\text{ ms}$, hiding $29.83\text{ ms}$ of unserved latency.

### Finding 4: Task queue placement causality is not represented by `schedule_latency`
When a task experiences high schedule latency, `TaskMeta::schedule_latency()` provides only a scalar duration. In our work-stealing benchmark, Subtask 52 waited $40.019\text{ ms}$ because it sat in Worker 1's LIFO slot behind a 40ms compute loop while Worker 0 was completely idle and parked. Current telemetry cannot distinguish whether this delay was caused by OS CPU starvation, thread unpark delays, or queue head-of-line blocking.

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
   - **Tested:** Dial9 samples `/proc/[pid]/task/[tid]/schedstat` on worker unpark.
   - **Result:** Partial. `schedstat` measures how long a runnable OS thread sat on the Linux kernel runqueue waiting for a CPU core. However, it cannot measure:
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
| **Boundary B: Runnable Work $\to$ Wake / Coalesce Decision** | **REAL GAP** | **Proven.** Wake suppression in `Idle::worker_to_notify()` is completely invisible. 1 unpark request frequently services $N$ scheduled tasks with zero telemetry visibility into the coalescing decision. |
| **Boundary C: Worker Notification $\to$ Worker Resume** | **REAL GAP** | **Proven.** `on_thread_unpark` fires after resumption with 0 arguments. The duration between unpark dispatch and worker loop resumption cannot be measured or attributed to a cause. |
| **Boundary D: Task Placement / Work Stealing $\to$ Poll** | **PARTIAL GAP** | **Partially Addressed.** `TaskMeta::schedule_latency()` measures the total delay, but does not expose queue placement (local vs injected vs LIFO) or work-stealing causality. |

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
