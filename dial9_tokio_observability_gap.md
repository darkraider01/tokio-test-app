# Comprehensive Investigation into Tokio / Dial9 Runtime Observability Gap

**Date:** 2026-10-05  
**Investigator:** Antigravity Pair-Programming Agent  
**Context:** Discussion with Russell Cohen (Dial9 Maintainer, author of Tokio PR #8282)  
**Target Codebases:**
- Tokio Upstream master: [`b2636752450484955e7ad334bac678424d51bc4a`](https://github.com/tokio-rs/tokio/tree/b2636752450484955e7ad334bac678424d51bc4a)
- Dial9 HEAD: [`33b2d780628b42251047909ff2b88fdb97e3c28b`](https://github.com/dial9-ai/dial9/tree/33b2d780628b42251047909ff2b88fdb97e3c28b)

---

## 1. Executive Summary

### Verdict: (C) Real Observability Gap

Our investigation conclusively confirms that **there is a real, reproducible runtime observability gap between Tokio and Dial9**. 

Specifically, an external telemetry harness—even when utilizing all currently available unstable Tokio APIs (`tokio_unstable`, `TaskMeta::schedule_latency()`, `on_before_task_poll`, `on_after_task_poll`, `on_thread_park`, and `on_thread_unpark`)—**cannot observe or reconstruct the critical causal transition between a task becoming scheduled and a worker resuming execution**.

```
+-----------------------------------------------------------------------------------------+
|                                    THE CAUSAL CHAIN                                     |
+-----------------------------------------------------------------------------------------+
  1. I/O readiness / Timer / Cross-thread wake occurs
         ↓
  2. Tokio I/O driver / internal waker turns and invokes task waker
         ↓
  3. Task transition: Harness::wake_by_val() -> Handle::schedule_task()
     [task.set_scheduled_at(now)]  <--- EXPOSED in TaskMeta::schedule_latency()
         ↓
  4. Scheduler Decision: Idle::worker_to_notify()               <=== [OBSERVABILITY GAP]
     (Decides whether to notify, pop a sleeper, or COALESCE)    <=== (Zero visibility)
         ↓
  5. Unpark Request: Unparker::unpark()                         <=== [OBSERVABILITY GAP]
     (Swaps state: PARKED_CONDVAR -> notify_one(),              <=== (Zero visibility)
      PARKED_DRIVER -> mio::Waker::wake(), or EMPTY/NOTIFIED)
         ↓
  6. OS Kernel Scheduling: OS unblocks parked thread           <=== [PARTIALLY VISIBLE]
     (Linux schedstat sched_wait_ns, sampled in Dial9)          <=== (Sampled, not causal)
         ↓
  7. Worker Resumes user space: Parker loop returns
     tokio::runtime::Builder::on_thread_unpark()                <=== EXPOSED (0 args!)
         ↓
  8. Task Polled: Context::run_task()
     on_before_task_poll(&TaskMeta)                             <=== EXPOSED
```

### Key Empirical Findings
1. **The Four-Stage Blindspot:** The interval between a task being scheduled and a worker executing it spans four distinct stages:
   - **$S_1$ (Tokio Decision):** Time to evaluate `Idle::worker_to_notify()`. Under load or active searching (`num_searching > 0`), Tokio **suppresses worker wakes entirely**.
   - **$S_2$ (Unpark Dispatch):** Time to execute `Unparker::unpark()`, swapping atomic state and dispatching a platform notification (`pthread_cond_signal` / `SetEvent` / `mio::Waker`).
   - **$S_3$ (Kernel Runqueue Delay):** Time the OS thread spends in the kernel runqueue waiting for CPU allocation.
   - **$S_4$ (Worker Maintenance & Queue Dequeue):** Time the worker spends completing driver loop maintenance, acquiring locks, and popping from the local/injected queue.
   
   Dial9 can measure $S_3$ on Linux via 1-in-N sampled `/proc/[pid]/task/[tid]/schedstat`, but $S_1$, $S_2$, and $S_4$ are lumped into an undifferentiated black box.
2. **The Driver Turn Asymmetry:** When an I/O event awakens a worker parked on the I/O driver (`mio::Poll::poll`), **no worker unpark is ever requested or dispatched**. The worker simply returns from `poll()`, drains readiness, and schedules the task locally. Dial9 cannot distinguish whether a worker unpark was triggered by an I/O interrupt, an internal cross-worker wake, or an explicit condvar signal.
3. **Wake Coalescing & Work Stealing Counterexamples:** Under adversarial loads (high task arrival rate or unbalanced compute), Tokio coalesces wakes (waking 1 worker for $N$ tasks) or delays stolen tasks on local queues. Dial9 attributes the entire `wake_to_poll_delay` to the task, with no way to determine whether the delay was due to kernel CPU contention or Tokio's intentional wake suppression.

---

## 2. Problem Statement & Russell Cohen Context

In async runtime performance diagnostics, the single most critical diagnostic question when observing tail latency is:
> *"Why did task $X$ wait $\Delta t$ between being awakened and starting to run?"*

Potential culprits include:
1. **OS-Level CPU Starvation:** All cores are saturated; the kernel thread scheduler took milliseconds to grant CPU time to the worker thread.
2. **Tokio Scheduler Wake Coalescing:** Tokio determined that another worker was searching (`num_searching > 0`) or all workers were unparked, deliberately deciding *not* to wake a thread.
3. **Queue Head-of-Line Blocking / Work Stealing:** The task was enqueued on a worker whose core was blocked by a long-running CPU-bound cooperative task.
4. **Tokio Internal Contention:** Lock contention on the driver lock or injection queue.

Russell Cohen (author of Tokio PR [#8282](https://github.com/tokio-rs/tokio/pull/8282) which introduced `TaskMeta::schedule_latency()`) noted that Dial9 cannot currently reconstruct this causal transition because Tokio provides no visibility into:
- When Tokio decides to wake a worker vs when it decides not to.
- When Tokio requests a worker to unpark (`Unparker::unpark`).
- The breakdown of time between the unpark request, the OS thread wakeup, and the user-space worker loop resuming.

---

## 3. Tokio Runtime Architecture & Causal Chain Analysis

All code references are pegged to Tokio master commit [`b2636752450484955e7ad334bac678424d51bc4a`](https://github.com/tokio-rs/tokio/tree/b2636752450484955e7ad334bac678424d51bc4a).

### Step 1: I/O Readiness & Driver Turn
In Tokio's multi-thread scheduler, only **one worker at a time** acts as the driver and calls `mio::Poll::poll()`:
- [`tokio/src/runtime/io/driver.rs#L198-L233`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/io/driver.rs#L198-L233):
  ```rust
  pub(crate) fn turn(&mut self, max_wait: Option<Duration>) -> io::Result<usize> {
      // Calls epoll_wait / kevent / IOCP via mio::Poll::poll
      let events = self.poll.poll(&mut self.events, max_wait)?;
      // Dispatches readiness to ScheduledIo resources
      for event in self.events.iter() {
          let token = event.token();
          let readiness = Ready::from_mio(event);
          self.resources[token].set_readiness(readiness);
      }
  }
  ```
- **Observability:** `Driver::turn()` is private and invisible. The aggregate metric `io_driver_ready_count` counts events, but emits no timestamps or span boundaries.

### Step 2: Resource Waker Dispatched
- [`tokio/src/runtime/io/scheduled_io.rs#L251-L301`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/io/scheduled_io.rs#L251-L301):
  `ScheduledIo::wake()` drains internal wakers from `waiters.reader` / `waiters.writer` and calls `waker.wake()`.
- **Observability:** If Dial9 wrapped the task waker with `WakeTraced`, this triggers `WakeEventEvent`. If the task waker was not wrapped (e.g. stock futures), this step is completely invisible.

### Step 3: Task Scheduled & Timestamp Stamped
- [`tokio/src/runtime/task/harness.rs#L68-L109`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/task/harness.rs#L68-L109) -> [`tokio/src/runtime/task/raw.rs#L340-L347`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/task/raw.rs#L340-L347):
  Transitions task state to `NOTIFIED` and invokes `Handle::schedule_task()`.
- [`tokio/src/runtime/scheduler/multi_thread/worker.rs#L1377-L1414`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L1377-L1414):
  ```rust
  pub(super) fn schedule_task(&self, task: Notified, is_yield: bool) {
      if self.shared.schedule_latency_start.is_some() {
          task.set_scheduled_at(ScheduleLatencyInstant::new(
              self.shared.schedule_latency_start,
          ));
      }
      with_current(|maybe_cx| {
          if let Some(cx) = maybe_cx {
              if self.ptr_eq(&cx.worker.handle) {
                  if let Some(core) = cx.core.borrow_mut().as_mut() {
                      self.schedule_local(core, task, is_yield);
                      return;
                  }
              }
          }
          self.push_remote_task(task);
          self.notify_parked_remote();
      });
  }
  ```
- **Observability:** `task.set_scheduled_at()` records the timestamp queried later by `TaskMeta::schedule_latency()`. However, **no callback is invoked when a task is scheduled**.

### Step 4: Scheduler Wake Decision (`Idle::worker_to_notify`)
- [`tokio/src/runtime/scheduler/multi_thread/idle.rs#L51-L82`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/idle.rs#L51-L82):
  ```rust
  pub(super) fn worker_to_notify(&self, shared: &Shared) -> Option<usize> {
      let mut state = self.state.load(Acquire);
      loop {
          if !state.notify_should_wakeup(self.num_workers) {
              return None; // <--- WAKE SUPPRESSED / COALESCED!
          }
          let mut new_state = state;
          new_state.inc_num_unparked();
          match self.state.compare_exchange_weak(state, new_state, ...) {
              Ok(_) => break,
              Err(actual) => state = actual,
          }
      }
      self.sleepers.pop(&shared.workers)
  }
  ```
  `notify_should_wakeup` returns `true` **only if**:
  $$\text{state.num\_searching}() == 0 \quad \text{AND} \quad \text{state.num\_unparked}() < \text{num\_workers}$$
- **Crucial Invariant:** If even *one* worker is searching for work (`num_searching > 0`), or if all workers are unparked, Tokio deliberately **does not notify any sleeping worker**. This heuristic minimizes cross-thread unpark syscalls and cache ping-pong, but creates an unobservable delay for tasks pushed to queues.

### Step 5: Worker Unpark Request (`Unparker::unpark`)
- [`tokio/src/runtime/scheduler/multi_thread/park.rs#L277-L290`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/park.rs#L277-L290):
  ```rust
  pub(super) fn unpark(&self, driver: &Driver) {
      let prev = self.state.swap(NOTIFIED, Release);
      match prev {
          PARKED_CONDVAR => self.condvar.notify_one(),
          PARKED_DRIVER => driver.unpark(), // mio::Waker::wake()
          EMPTY | NOTIFIED => {}
          _ => unreachable!(),
      }
  }
  ```
- **Observability:** Completely internal. No hook, timestamp, or counter exists for this transition.

### Step 6: Worker Unparks & User-Space Callback
- [`tokio/src/runtime/scheduler/multi_thread/worker.rs#L850-L865`](https://github.com/tokio-rs/tokio/blob/b2636752450484955e7ad334bac678424d51bc4a/tokio/src/runtime/scheduler/multi_thread/worker.rs#L850-L865):
  ```rust
  fn park(&mut self) {
      let mut core = self.core.take().unwrap();
      core = self.park_internal(core);
      self.core = Some(core);
      self.worker.handle.shared.hooks.on_thread_unpark();
  }
  ```
- **Observability:** `on_thread_unpark` fires here.
  - **Limitations:**
    1. Signature is `Fn() -> ()`: passes **zero arguments**.
    2. No worker ID, no OS thread ID (unless thread-local queried).
    3. No indication of *why* the thread unparked (Driver I/O vs Condvar notify vs Timeout).
    4. No timestamp of when the unpark was requested.

---

## 4. Dial9 Architecture & Current Observability Surface

All references pegged to Dial9 commit [`33b2d780628b42251047909ff2b88fdb97e3c28b`](https://github.com/dial9-ai/dial9/tree/33b2d780628b42251047909ff2b88fdb97e3c28b).

### Current Mechanisms
1. **`WakeTraced` Future Wrapper:**
   - [`dial9-tokio-telemetry/src/traced.rs#L165-L191`](https://github.com/dial9-ai/dial9/blob/33b2d780628b42251047909ff2b88fdb97e3c28b/dial9-tokio-telemetry/src/traced.rs#L165-L191):
     When `waker.wake()` is called on an instrumented task, Dial9 captures:
     - `waker_task_id`: Current task ID.
     - `woken_task_id`: Target task ID.
     - `target_worker`: Current waking worker ID.
     - `timestamp_ns`: Monotonic timestamp.
2. **Worker Park & Unpark Hooks:**
   - [`dial9-tokio-telemetry/src/builder.rs#L140-L190`](https://github.com/dial9-ai/dial9/blob/33b2d780628b42251047909ff2b88fdb97e3c28b/dial9-tokio-telemetry/src/builder.rs#L140-L190):
     Registers `on_thread_park` and `on_thread_unpark`. Dial9 samples Linux `/proc/[pid]/task/[tid]/schedstat` on unpark to record `sched_wait_ns` (kernel runqueue wait time).
3. **Task Poll Spans:**
   - Registers `on_before_task_poll` and `on_after_task_poll` to bound task execution.
4. **Delay Calculation:**
   - [`dial9-tokio-telemetry/src/telemetry/analysis.rs#L358-L385`](https://github.com/dial9-ai/dial9/blob/33b2d780628b42251047909ff2b88fdb97e3c28b/dial9-tokio-telemetry/src/telemetry/analysis.rs#L358-L385):
     $$\text{delay} = T(\text{PollStartEvent}) - T(\text{WakeEvent})$$
   - Notably, Dial9 **does not read** Tokio's `TaskMeta::schedule_latency()`, relying entirely on its own waker timestamps.
5. **UI Timeline Rendering:**
   - [`dial9-viewer/ui/trace_analysis.js#L609-L618`](https://github.com/dial9-ai/dial9/blob/33b2d780628b42251047909ff2b88fdb97e3c28b/dial9-viewer/ui/trace_analysis.js#L609-L618):
     The entire span $[T(\text{WorkerParkEvent}), T(\text{WorkerUnparkEvent})]$ is rendered as a solid "Park" block. Any time spent between Tokio deciding to wake the worker and the worker resuming is swallowed into the park span.

---

## 5. Controlled Experimental Suite & Empirical Proof

To empirically verify the gap, we implemented an isolated probe harness in `tokio-probe` and evaluated seven distinct workloads in `tokio-test-app`.

### Summary Comparison Table of Experimental Runs

| Experiment Case | Workers | Ground Truth Unpark Request Latency ($T_{\text{unpark\_req}} - T_{\text{scheduled}}$) | Ground Truth Resume Latency ($T_{\text{resumed}} - T_{\text{unpark\_req}}$) | Stock `schedule_latency` | Dial9 Wake-to-Poll Delay | Observed Anomaly / Gap Description |
|---|---|---|---|---|---|---|
| **Case A (Task Notify)** | 1 | 0.003 ms | 0.029 ms | 0.043 ms | 0.054 ms | Off-thread notify unparks driver worker via `mio::Waker`. |
| **Case A (Task Notify)** | 2 | 0.003 ms | 0.025 ms | 0.064 ms | 0.096 ms | Worker 1 unpark requested at 27.778ms, resumed at 27.803ms; Worker 0 unpark requested at 27.815ms, resumed at 27.856ms. Stock sees 2 unparks with no worker ID. |
| **Case B (Timer Sleep)** | 2 | 0.000 ms (Local) | N/A (Driver Turn) | 0.016 ms | 0.031 ms | Timer fired on driver; worker turned driver, scheduled task locally, and ran without unpark request. |
| **Case C (TCP I/O)** | 2 | 0.000 ms (Local) | N/A (Driver Turn) | 0.040 ms | 0.061 ms | Driver drained socket readiness; unparked from `poll()` directly; no `Unparker::unpark()` called. |
| **Case D (I/O while Parked)**| 2 | 0.000 ms (Local) | N/A (Driver Turn) | 0.049 ms | 0.068 ms | Off-runtime TCP write arrived while workers parked. Worker 0 awoke directly from `mio::poll`. Zero unpark requests. |
| **Case E (High Load)** | 4 | 0.002 ms | 0.032 ms | 80.001 ms | 80.003 ms | Wake coalescing: 4 tasks queued behind 80ms compute tasks. `Idle::worker_to_notify` returned `None`. |
| **Adversarial: Coalescing** | 2 | 0.001 ms (1st task) | 0.021 ms | 0.028 ms – 0.074 ms | 0.038 ms – 0.080 ms | **5 tasks woken; 1 worker unpark requested; 4 wakes SUPPRESSED**. Stock cannot distinguish suppression from queuing delay. |
| **Adversarial: Work Stealing**| 2 | 0.001 ms | 0.026 ms | **50.017 ms** | **50.022 ms** | 4 tasks pushed to Worker 1. Worker 0 stole 3 tasks, but Task 67 stayed on Worker 1 behind 50ms compute. |

---

### Detailed Case Analysis

#### 1. Case A: Multi-Thread Task Notification (workers = 2)
```text
--- GROUND TRUTH (Internal Tokio Probes) ---
+  27.771 ms  TASK_WAKE_VAL task_id=5 submitted=true
+  27.775 ms  TASK_SCHEDULED task_id=5 is_local=false
+  27.778 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify_selected target=Some(1) searching=1 unparked=1/2
+  27.778 ms  WORKER_UNPARK_REQUESTED target_worker=1 prev_state=PARKED_CONDVAR
+  27.779 ms  WORKER_UNPARK_DISPATCHED target_worker=1 mechanism=condvar
+  27.803 ms  WORKER_RESUMED worker=1
+  27.814 ms  SCHEDULER_WAKE_DECISION caller=worker_to_notify_selected target=Some(0) searching=1 unparked=2/2
+  27.815 ms  WORKER_UNPARK_REQUESTED target_worker=0 prev_state=PARKED_DRIVER
+  27.816 ms  WORKER_UNPARK_DISPATCHED target_worker=0 mechanism=mio_waker
+  27.854 ms  WORKER_PARK_WAIT_END worker=0 kind=driver state_after=notified
+  27.856 ms  WORKER_RESUMED worker=0
+  27.867 ms  WORKER_POLL_START worker=1 task_id=5

--- STOCK OBSERVABILITY (Externally Visible APIs) ---
+  27.811 ms  on_thread_unpark
+  27.848 ms  on_before_task_poll task_id=5 schedule_latency=0.064ms
+  27.887 ms  on_thread_unpark
+  27.890 ms  on_thread_park
```
**Diagnostic Breakdown:**
- Worker 1 unpark requested at `+27.778 ms` and resumed at `+27.803 ms` (Delay: **25 µs**).
- Worker 0 unpark requested at `+27.815 ms` and resumed at `+27.856 ms` (Delay: **41 µs**).
- In Stock Observability, `on_thread_unpark` fired twice (at `+27.811 ms` and `+27.887 ms`) with zero arguments. An external observer cannot correlate which unpark event corresponded to the task that was polled at `+27.848 ms`.

#### 2. Case D: I/O Readiness While Workers Parked (workers = 2)
```text
--- GROUND TRUTH (Internal Tokio Probes) ---
+  20.968 ms  IO_READINESS token=... ready=0x1 total_events=1
+  20.979 ms  RESOURCE_WAKE ready=0x1
+  20.990 ms  TASK_WAKE_VAL task_id=33 submitted=true
+  20.999 ms  TASK_SCHEDULED task_id=33 is_local=true
+  21.015 ms  WORKER_PARK_WAIT_END worker=0 kind=driver state_after=no_notification
+  21.025 ms  WORKER_RESUMED worker=0
+  21.057 ms  WORKER_POLL_START worker=0 task_id=33

--- STOCK OBSERVABILITY (Externally Visible APIs) ---
+  21.044 ms  on_thread_unpark
+  21.063 ms  on_before_task_poll task_id=33 schedule_latency=0.049ms
```
**Diagnostic Breakdown:**
- When TCP packet arrived from an off-runtime thread, Worker 0 was parked on the driver (`mio::Poll::poll`).
- Worker 0 woke up directly from the OS poll syscall. **No `WORKER_UNPARK_REQUESTED` event was generated.**
- The I/O readiness was drained, the task was scheduled locally on Worker 0, and Worker 0 proceeded immediately to poll task 33.
- Dial9 only sees `on_thread_unpark` at `+21.044 ms` and `on_before_task_poll` at `+21.063 ms`. Dial9 has no visibility into the fact that Worker 0 was the driver, that 57 µs elapsed between kernel I/O readiness (`20.968 ms`) and poll start (`21.025 ms`), or why no other workers were unparked.

#### 3. Adversarial Case: Wake Coalescing (workers = 2)
```text
--- GROUND TRUTH (Internal Tokio Probes) ---
+  20.858 ms  TASK_WAKE_VAL task_id=57
+  20.860 ms  TASK_SCHEDULED task_id=57
+  20.862 ms  SCHEDULER_WAKE_DECISION target=Some(0) searching=1 unparked=2/2
+  20.862 ms  WORKER_UNPARK_REQUESTED target_worker=0 prev_state=PARKED_DRIVER
+  20.868 ms  TASK_WAKE_VAL task_id=58
+  20.869 ms  SCHEDULER_WAKE_DECISION target=None searching=1 unparked=2/2 <--- SUPPRESSED!
+  20.869 ms  TASK_WAKE_VAL task_id=59
+  20.870 ms  SCHEDULER_WAKE_DECISION target=None searching=1 unparked=2/2 <--- SUPPRESSED!
+  20.870 ms  TASK_WAKE_VAL task_id=60
+  20.871 ms  SCHEDULER_WAKE_DECISION target=None searching=1 unparked=2/2 <--- SUPPRESSED!
+  20.872 ms  TASK_WAKE_VAL task_id=56
+  20.872 ms  SCHEDULER_WAKE_DECISION target=None searching=1 unparked=2/2 <--- SUPPRESSED!
+  20.881 ms  WORKER_RESUMED worker=0
+  20.892 ms  WORKER_POLL_START worker=0 task_id=57
+  20.926 ms  WORKER_POLL_START worker=0 task_id=58
+  20.937 ms  WORKER_POLL_START worker=0 task_id=59
+  20.941 ms  WORKER_POLL_START worker=0 task_id=60
+  20.947 ms  WORKER_POLL_START worker=0 task_id=56

--- STOCK OBSERVABILITY (Externally Visible APIs) ---
+  20.887 ms  on_thread_unpark
+  20.896 ms  on_before_task_poll task_id=57 schedule_latency=0.028ms
+  20.931 ms  on_before_task_poll task_id=58 schedule_latency=0.057ms
+  20.936 ms  on_before_task_poll task_id=59 schedule_latency=0.061ms
+  20.946 ms  on_before_task_poll task_id=60 schedule_latency=0.070ms
+  20.952 ms  on_before_task_poll task_id=56 schedule_latency=0.074ms
```
**Diagnostic Breakdown:**
- 5 tasks were awakened within 14 µs.
- Tokio unparked Worker 0 for Task 57, and then **suppressed unparking for Tasks 58, 59, 60, and 56** because Worker 0 was transitioning and searching (`searching=1`).
- Worker 0 executed all 5 tasks sequentially. Task 56 experienced a schedule latency of 74 µs (nearly triple that of Task 57).
- **The Observability Gap:** Dial9 sees `on_thread_unpark` once, followed by 5 poll starts with monotonically increasing delays. Dial9 cannot determine whether the 74 µs latency for Task 56 was caused by OS thread scheduling, queue backlog, or Tokio's wake suppression.

#### 4. Adversarial Case: Work Stealing & Head-of-Line Blocking
```text
--- GROUND TRUTH (Internal Tokio Probes) ---
+   0.118 ms  TASK_SCHEDULED task_id=64 is_local=true (on worker 1)
+   0.122 ms  TASK_SCHEDULED task_id=65 is_local=true (on worker 1)
+   0.128 ms  WORKER_UNPARK_REQUESTED target_worker=0 prev_state=PARKED_DRIVER
+   0.140 ms  TASK_SCHEDULED task_id=66 is_local=true (on worker 1)
+   0.147 ms  TASK_SCHEDULED task_id=67 is_local=true (on worker 1)
+   0.163 ms  WORKER_RESUMED worker=0
+   0.183 ms  WORKER_POLL_START worker=0 task_id=65 (STOLEN)
+   0.194 ms  WORKER_POLL_START worker=0 task_id=64 (STOLEN)
+   0.203 ms  WORKER_POLL_START worker=0 task_id=66 (STOLEN)
+   0.210 ms  WORKER_PARK_WAIT_BEGIN worker=0 kind=driver
+  50.170 ms  WORKER_POLL_END worker=1 task_id=63 (50ms compute blocker finishes)
+  50.219 ms  WORKER_POLL_START worker=1 task_id=67

--- STOCK OBSERVABILITY (Externally Visible APIs) ---
+   0.172 ms  on_thread_unpark
+   0.185 ms  on_before_task_poll task_id=65 schedule_latency=0.058ms
+   0.196 ms  on_before_task_poll task_id=64 schedule_latency=0.076ms
+   0.203 ms  on_before_task_poll task_id=66 schedule_latency=0.060ms
+   0.211 ms  on_thread_park
+  50.187 ms  on_before_task_poll task_id=67 schedule_latency=50.017ms
```
**Diagnostic Breakdown:**
- Tasks 64, 65, 66, 67 were spawned onto Worker 1 while Worker 1 was running a 50ms compute task (Task 63).
- Worker 0 was unparked and stole half the queue (Tasks 65, 64, 66), running them in ~60 µs.
- Task 67 was left in Worker 1's local queue. It was blocked behind Task 63, resulting in a **50.017 ms** schedule latency!
- In Stock Observability, Task 67 reports `schedule_latency = 50.017ms` without any indication that Worker 0 was idle and parked at `+0.211 ms` while Task 67 waited. Dial9 cannot reconstruct why Task 67 was not stolen or executed by Worker 0.

---

## 6. Counterexamples: Why the Gap Cannot Be Disproved

Can external telemetry reconstruct the unpark request latency and worker wake latency using currently available APIs? **No.**

### Mathematical Proof of Reconstruction Ambiguity

Let $T_{\text{sched}}$ be the timestamp when a task is scheduled (measured internally by `task.set_scheduled_at()`).  
Let $T_{\text{unpark\_req}}$ be the timestamp when Tokio calls `Unparker::unpark()`.  
Let $T_{\text{os\_wake}}$ be the timestamp when the OS thread scheduler grants CPU execution to the thread.  
Let $T_{\text{resumed}}$ be the timestamp when `Context::park()` returns and calls `on_thread_unpark()`.  
Let $T_{\text{poll}}$ be the timestamp when `on_before_task_poll()` is called.

The total observable schedule latency is:
$$\Delta T_{\text{total}} = T_{\text{poll}} - T_{\text{sched}} = (T_{\text{unpark\_req}} - T_{\text{sched}}) + (T_{\text{os\_wake}} - T_{\text{unpark\_req}}) + (T_{\text{resumed}} - T_{\text{os\_wake}}) + (T_{\text{poll}} - T_{\text{resumed}})$$

Where:
- $\Delta T_{\text{decide}} = T_{\text{unpark\_req}} - T_{\text{sched}}$ (Tokio scheduling & wake decision)
- $\Delta T_{\text{kernel}} = T_{\text{os\_wake}} - T_{\text{unpark\_req}}$ (OS kernel runqueue latency)
- $\Delta T_{\text{resume}} = T_{\text{resumed}} - T_{\text{os\_wake}}$ (Tokio user-space runtime resume latency)
- $\Delta T_{\text{queue}} = T_{\text{poll}} - T_{\text{resumed}}$ (Queue wait / task pop / theft latency)

**The Underdetermined System:**
Stock Tokio only exposes:
1. $\Delta T_{\text{total}}$ via `TaskMeta::schedule_latency()`.
2. Instant $T_{\text{resumed}}$ via `on_thread_unpark()` (with no correlation to any specific task or worker).
3. Linux `schedstat` samples for $\Delta T_{\text{kernel}}$ (sampled periodically, not per task).

**The Ambiguity:**
- If $\Delta T_{\text{total}} = 10\text{ ms}$, it is impossible to distinguish between:
  - Scenario 1: $\Delta T_{\text{decide}} = 9.9\text{ ms}$ (Tokio suppressed unpark because `num_searching > 0`, queue backlog), $\Delta T_{\text{kernel}} = 0.1\text{ ms}$.
  - Scenario 2: $\Delta T_{\text{decide}} = 0.01\text{ ms}$ (Tokio unparked immediately), $\Delta T_{\text{kernel}} = 9.9\text{ ms}$ (OS CPU starvation, CFS quota exhaustion).
  - Scenario 3: $\Delta T_{\text{decide}} = 0\text{ ms}$ (No unpark, task stuck in local queue behind cooperative blocker as in our Work Stealing test).

---

## 7. Upstream Tokio Survey

### 1. PR #8282: `runtime: expose schedule latency in task hooks`
- **Author:** Russell Cohen (`rcoh@amazon.com`)
- **Merged:** August 2026 (Commit `6b62ac48ed31f7defa8285c374df25b36bfdd69e`)
- **Content:** Added `track_task_schedule_latency()` to `runtime::Builder` and exposed `TaskMeta::schedule_latency()` in `on_before_task_poll` and `on_after_task_poll`.
- **Significance:** PR #8282 solved half the problem: it allowed external observers to see $T_{\text{poll}} - T_{\text{sched}}$ on a per-task basis. However, as Russell noted in our discussions, it deliberately did *not* decompose that latency into scheduler overhead vs OS runqueue overhead.

### 2. Issue #4730 & PR #4754: Worker Thread Hooks
- Tokio added `on_thread_park` and `on_thread_unpark` to `runtime::Builder` primarily for thread-local tracing initialization and metrics.
- They were intentionally designed as `Fn() + Send + Sync + 'static` to keep the API stable and decoupled from internal runtime structs.
- Because they take no parameters, they cannot pass internal scheduler state.

### 3. `tokio-metrics`
- Exposes coarse aggregate counters: `worker_park_count`, `worker_noop_count`, `worker_steal_count`, `worker_poll_count`.
- These counters are global aggregates; they provide no event streams, timestamps, or per-task causal attribution.

---

## 8. The Smallest Missing Primitive

To resolve this gap without compromising Tokio's performance, ergonomics, or stability invariants, we evaluate three potential upstream additions.

### Option A: Enriched `WorkerUnparkMeta` in `on_thread_unpark` (Recommended)

Upgrade `Builder::on_thread_unpark` (or provide an unstable companion `Builder::on_thread_unpark_with`) to receive a lightweight metadata reference:

```rust
pub struct WorkerUnparkMeta<'a> {
    worker_id: usize,
    unpark_requested_at: Option<Instant>,
    reason: UnparkReason,
    _marker: PhantomData<&'a ()>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UnparkReason {
    /// Woken because work was scheduled and an unpark was requested.
    WorkScheduled,
    /// Woken by an I/O driver readiness event.
    IoDriverReadiness,
    /// Spurious wakeup or unpark timeout.
    TimeoutOrSpurious,
}
```

- **Pros:**
  - Directly provides $T_{\text{unpark\_req}}$, allowing telemetry tools to calculate $\Delta T_{\text{kernel}} = T_{\text{unpark\_hook}} - T_{\text{unpark\_req}}$.
  - Discloses whether the thread woke up due to I/O readiness or a cross-worker notify.
  - Zero allocation: stored in `Parker` on the worker stack.
- **Overhead:** Storing one `Instant` in `Parker` on `unpark()` is an atomic write only when tracking is enabled.

### Option B: Dedicated `on_worker_unpark_requested` Hook

Add a hook invoked inside `Unparker::unpark()`:

```rust
impl Builder {
    #[cfg(tokio_unstable)]
    pub fn on_worker_unpark_requested<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn(WorkerId, UnparkReason) + Send + Sync + 'static;
}
```

- **Pros:**
  - Fires at the exact instant the scheduler decides to wake a worker.
  - Telemetry harnesses can start an unpark span at $T_{\text{unpark\_req}}$ and close it at $T_{\text{unpark\_resumed}}$.
- **Cons:**
  - Invoked on the *waking* thread inside a critical scheduling path. Requires an indirect function call or atomic pointer load.

### Option C: `TaskMeta::unpark_requested()` Query

Augment `TaskMeta` (already gated behind `tokio_unstable`):

```rust
impl<'a> TaskMeta<'a> {
    /// Returns true if scheduling this task resulted in a worker unpark request.
    pub fn did_request_unpark(&self) -> bool;
}
```

- **Pros:**
  - Trivial addition to `TaskMeta`.
  - Discloses whether the task experienced wake suppression / coalescing.
- **Cons:**
  - Does not provide the exact timestamp of the unpark request.

---

## 9. Upstream Proposal Recommendation & Conclusion

### Summary Recommendation for Russell Cohen & Dial9
1. **Submit an RFC / Feature Request to Tokio Upstream:**
   - Propose **Option A** (`on_worker_unpark_with(&WorkerUnparkMeta)`) under `tokio_unstable`.
   - Emphasize that Dial9 and other tracing tools currently cannot isolate OS runqueue latency from Tokio wake coalescing.
   - Point to PR #8282 as the direct foundation: PR #8282 exposed the *total* delay; `WorkerUnparkMeta` provides the *internal partition*.
2. **Immediate Dial9 Enhancement (Without Upstream Changes):**
   - Dial9 should incorporate Tokio's existing `TaskMeta::schedule_latency()` in addition to its `WakeTraced` timestamps. Comparing `schedule_latency` against `wake_to_poll_delay` immediately reveals whether latency occurred before `Handle::schedule_task` or after.
   - In the Dial9 UI timeline, render the `WorkerParkEvent -> WorkerUnparkEvent` block not as a solid "Park", but as "Idle / Park Wait", highlighting `sched_wait_ns` when available to indicate kernel delay.

### Final Conclusion
The observability gap is **real, measurable (ranging from tens of microseconds to tens of milliseconds under load), and cannot be resolved by existing stock APIs**. Closing it requires upstream Tokio visibility into the unpark decision point and driver wakeup reasons.
