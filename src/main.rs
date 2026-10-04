use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::ground_truth_probes::{self, ProbeEvent};
use tokio::sync::Notify;

use dial9_core::buffer::MemoryBuffer;
use dial9_core::pipeline::{ProcessError, SegmentData, SegmentProcessor};
use dial9_core::recorder::recorder;
use dial9_tokio_telemetry::telemetry::analysis::compute_wake_to_poll_delays;
use dial9_tokio_telemetry::telemetry::analysis_events::Dial9Event;
use dial9_tokio_telemetry::telemetry::{Dial9HandleTokioExt, TokioAttachOptions, TokioHooks};
use dial9_trace_format::decoder::Decoder;

#[derive(Clone, Debug)]
pub struct StockEvent {
    pub t_ns: u64,
    pub event_type: &'static str,
    pub worker_id: Option<usize>,
    pub task_id: Option<u64>,
    pub schedule_latency_ns: Option<u64>,
    pub details: String,
}

#[derive(Default)]
pub struct StockRecorder {
    pub base_time: Mutex<Option<Instant>>,
    pub events: Mutex<Vec<StockEvent>>,
}

impl StockRecorder {
    pub fn reset(&self) {
        let mut b = self.base_time.lock().unwrap();
        *b = Some(Instant::now());
        self.events.lock().unwrap().clear();
    }

    pub fn now_ns(&self) -> u64 {
        let b = self.base_time.lock().unwrap();
        match *b {
            Some(t) => t.elapsed().as_nanos() as u64,
            None => 0,
        }
    }

    pub fn record(
        &self,
        event_type: &'static str,
        worker_id: Option<usize>,
        task_id: Option<u64>,
        latency_ns: Option<u64>,
        details: String,
    ) {
        let t_ns = self.now_ns();
        let mut evs = self.events.lock().unwrap();
        evs.push(StockEvent {
            t_ns,
            event_type,
            worker_id,
            task_id,
            schedule_latency_ns: latency_ns,
            details,
        });
    }

    pub fn take_events(&self) -> Vec<StockEvent> {
        let mut evs = self.events.lock().unwrap();
        std::mem::take(&mut *evs)
    }
}

#[derive(Clone)]
struct CapturingProcessor {
    segments: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl SegmentProcessor for CapturingProcessor {
    fn name(&self) -> &'static str {
        "Capture"
    }

    fn process(
        &mut self,
        data: SegmentData,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<SegmentData, ProcessError>> + Send + 'static>,
    > {
        self.segments
            .lock()
            .unwrap()
            .push(data.payload().clone().into_vec());
        Box::pin(async move { Ok(data) })
    }
}

pub struct InstrumentedSession {
    pub runtime: Option<tokio::runtime::Runtime>,
    pub recorder: Option<dial9_core::recording::Recorder>,
    pub stock_rec: Arc<StockRecorder>,
    pub dial9_segments: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl InstrumentedSession {
    pub fn new(worker_threads: usize) -> Self {
        let stock_rec = Arc::new(StockRecorder::default());
        let dial9_segments = Arc::new(Mutex::new(Vec::new()));
        let proc = CapturingProcessor {
            segments: dial9_segments.clone(),
        };

        let recorder = recorder(MemoryBuffer::new(16 * 1024 * 1024).expect("memory buffer"))
            .processors(vec![Box::new(proc)])
            .build();

        let mut tokio_hooks = TokioHooks::default();
        let rec_p = stock_rec.clone();
        tokio_hooks.on_thread_park(move || {
            rec_p.record("on_thread_park", None, None, None, String::new());
        });
        let rec_u = stock_rec.clone();
        tokio_hooks.on_thread_unpark(move || {
            rec_u.record("on_thread_unpark", None, None, None, String::new());
        });
        let rec_s = stock_rec.clone();
        tokio_hooks.on_task_spawn(move |meta| {
            rec_s.record(
                "on_task_spawn",
                None,
                meta.id().to_string().parse::<u64>().ok(),
                None,
                format!("spawned_at={:?}", meta.spawned_at()),
            );
        });
        let rec_t = stock_rec.clone();
        tokio_hooks.on_task_terminate(move |meta| {
            rec_t.record(
                "on_task_terminate",
                None,
                meta.id().to_string().parse::<u64>().ok(),
                None,
                String::new(),
            );
        });
        let rec_bp = stock_rec.clone();
        tokio_hooks.on_before_task_poll(move |meta| {
            let lat = meta.schedule_latency().map(|d| d.as_nanos() as u64);
            rec_bp.record(
                "on_before_task_poll",
                None,
                meta.id().to_string().parse::<u64>().ok(),
                lat,
                String::new(),
            );
        });
        let rec_ap = stock_rec.clone();
        tokio_hooks.on_after_task_poll(move |meta| {
            rec_ap.record(
                "on_after_task_poll",
                None,
                meta.id().to_string().parse::<u64>().ok(),
                None,
                String::new(),
            );
        });

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(worker_threads)
            .enable_all()
            .track_task_schedule_latency();

        let options = TokioAttachOptions::builder()
            .tokio_hooks(tokio_hooks)
            .task_tracking_enabled(true)
            .build();

        let runtime = recorder
            .handle()
            .attach_tokio_runtime(builder, options)
            .expect("attach tokio runtime");

        Self {
            runtime: Some(runtime),
            recorder: Some(recorder),
            stock_rec,
            dial9_segments,
        }
    }

    pub fn finish(mut self) -> (Vec<ProbeEvent>, Vec<StockEvent>, Vec<Dial9Event>) {
        let gt_events = ground_truth_probes::take_events();
        let stock_events = self.stock_rec.take_events();

        // 1. Drop runtime to shut down workers and flush thread-local trace encoders
        drop(self.runtime.take());

        // 2. Shut down recorder to seal and flush segments through CapturingProcessor
        if let Some(rec) = self.recorder.take() {
            let _ = rec.graceful_shutdown(Duration::from_millis(500));
        }

        // 3. Decode Dial9 events
        let mut dial9_events = Vec::new();
        let segs = self.dial9_segments.lock().unwrap();
        for seg in segs.iter() {
            if let Some(mut dec) = Decoder::new(seg) {
                let _ = dec.for_each_event(|raw| {
                    if let Ok(ev) = raw.deserialize::<Dial9Event>() {
                        dial9_events.push(ev);
                    }
                });
            }
        }

        (gt_events, stock_events, dial9_events)
    }
}

fn print_three_view_timeline(
    title: &str,
    ground_truth: &[ProbeEvent],
    stock: &[StockEvent],
    dial9: &[Dial9Event],
) {
    println!("\n=======================================================");
    println!("EXPERIMENT: {}", title);
    println!("=======================================================");

    println!("\n--- [VIEW A] INTERNAL TOKIO GROUND TRUTH ---");
    for ev in ground_truth {
        let (t_ns, desc) = match ev {
            ProbeEvent::IoReadinessObserved {
                t_ns,
                token,
                ready,
                count,
            } => (
                *t_ns,
                format!(
                    "IO_READINESS token={} ready={:#x} total_events={}",
                    token, ready, count
                ),
            ),
            ProbeEvent::ResourceWakeDispatched { t_ns, ready } => {
                (*t_ns, format!("RESOURCE_WAKE ready={:#x}", ready))
            }
            ProbeEvent::TaskWakeByVal {
                t_ns,
                task_id,
                submitted,
            } => (
                *t_ns,
                format!("TASK_WAKE_VAL task_id={} submitted={}", task_id, submitted),
            ),
            ProbeEvent::TaskWakeByRef {
                t_ns,
                task_id,
                submitted,
            } => (
                *t_ns,
                format!("TASK_WAKE_REF task_id={} submitted={}", task_id, submitted),
            ),
            ProbeEvent::TaskScheduled {
                t_ns,
                task_id,
                is_local,
            } => (
                *t_ns,
                format!("TASK_SCHEDULED task_id={} is_local={}", task_id, is_local),
            ),
            ProbeEvent::SchedulerWakeDecision {
                t_ns,
                task_id,
                caller,
                target_worker,
                num_searching,
                num_unparked,
                total_workers,
            } => (
                *t_ns,
                format!(
                    "SCHEDULER_WAKE_DECISION caller={} task_id={:?} target={:?} searching={} unparked={}/{}",
                    caller, task_id, target_worker, num_searching, num_unparked, total_workers
                ),
            ),
            ProbeEvent::WorkerUnparkRequested {
                t_ns,
                target_worker,
                prev_state,
            } => (
                *t_ns,
                format!(
                    "WORKER_UNPARK_REQUESTED target_worker={} prev_state={}",
                    target_worker, prev_state
                ),
            ),
            ProbeEvent::WorkerUnparkDispatched {
                t_ns,
                target_worker,
                mechanism,
            } => (
                *t_ns,
                format!(
                    "WORKER_UNPARK_DISPATCHED target_worker={} mechanism={}",
                    target_worker, mechanism
                ),
            ),
            ProbeEvent::WorkerParkWaitBegin {
                t_ns,
                worker_id,
                kind,
            } => (
                *t_ns,
                format!("WORKER_PARK_WAIT_BEGIN worker={} kind={}", worker_id, kind),
            ),
            ProbeEvent::WorkerParkWaitEnd {
                t_ns,
                worker_id,
                kind,
                state_after,
            } => (
                *t_ns,
                format!(
                    "WORKER_PARK_WAIT_END worker={} kind={} state_after={}",
                    worker_id, kind, state_after
                ),
            ),
            ProbeEvent::WorkerResumed { t_ns, worker_id } => {
                (*t_ns, format!("WORKER_RESUMED worker={}", worker_id))
            }
            ProbeEvent::WorkerPollStart {
                t_ns,
                worker_id,
                task_id,
            } => (
                *t_ns,
                format!("WORKER_POLL_START worker={} task_id={}", worker_id, task_id),
            ),
            ProbeEvent::WorkerPollEnd {
                t_ns,
                worker_id,
                task_id,
            } => (
                *t_ns,
                format!("WORKER_POLL_END worker={} task_id={}", worker_id, task_id),
            ),
            ProbeEvent::ExternalIoStimulus {
                t_ns,
                phase,
                details,
            } => (
                *t_ns,
                format!("EXTERNAL_IO_STIMULUS phase={} details={}", phase, details),
            ),
        };
        println!("+{:>8.3} ms  {}", (t_ns as f64) / 1_000_000.0, desc);
    }

    println!("\n--- [VIEW B] STOCK TOKIO OBSERVABILITY ---");
    for ev in stock {
        let mut extra = String::new();
        if let Some(tid) = ev.task_id {
            extra.push_str(&format!(" task_id={}", tid));
        }
        if let Some(lat) = ev.schedule_latency_ns {
            extra.push_str(&format!(" schedule_latency={:.3}ms", (lat as f64) / 1_000_000.0));
        }
        if !ev.details.is_empty() {
            extra.push_str(&format!(" ({})", ev.details));
        }
        println!(
            "+{:>8.3} ms  {}{}",
            (ev.t_ns as f64) / 1_000_000.0,
            ev.event_type,
            extra
        );
    }

    println!("\n--- [VIEW C] ACTUAL DIAL9 OBSERVABILITY ---");
    let mut wake_events = Vec::new();
    let mut park_events = Vec::new();
    let mut unpark_events = Vec::new();
    let mut poll_starts = Vec::new();

    for ev in dial9 {
        match ev {
            Dial9Event::WakeEvent(w) => {
                wake_events.push(w);
                println!(
                    "+  DIAL9 WAKE_EVENT: waker_task={} -> woken_task={} (issuing_worker={})",
                    w.waker_task_id, w.woken_task_id, w.target_worker
                );
            }
            Dial9Event::WorkerParkEvent(p) => {
                park_events.push(p);
                println!(
                    "+  DIAL9 WORKER_PARK: worker={} tid={} cpu_time_ns={}",
                    p.worker_id, p.tid, p.cpu_time_ns
                );
            }
            Dial9Event::WorkerUnparkEvent(u) => {
                unpark_events.push(u);
                let sched_str = match u.sched_wait_ns {
                    Some(ns) => format!("sched_wait={:.3}ms", (ns as f64) / 1_000_000.0),
                    None => "sched_wait=None (unsupported/unsampled)".to_string(),
                };
                println!(
                    "+  DIAL9 WORKER_UNPARK: worker={} tid={} {}",
                    u.worker_id, u.tid, sched_str
                );
            }
            Dial9Event::PollStartEvent(p) => {
                poll_starts.push(p);
                println!(
                    "+  DIAL9 POLL_START: worker={} task_id={} loc={}",
                    p.worker_id, p.task_id, p.spawn_loc
                );
            }
            Dial9Event::PollEndEvent(p) => {
                println!("+  DIAL9 POLL_END: worker={}", p.worker_id);
            }
            _ => {}
        }
    }

    let delays = compute_wake_to_poll_delays(dial9);
    if !delays.is_empty() {
        println!(
            "+  DIAL9 COMPUTED WAKE-TO-POLL DELAYS: {:?}",
            delays
                .iter()
                .map(|ns| format!("{:.3}ms", (*ns as f64) / 1_000_000.0))
                .collect::<Vec<_>>()
        );
    }
}

// -------------------------------------------------------------
// Case A: Task-to-task notify
// -------------------------------------------------------------
fn test_case_a(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    session.runtime.as_ref().unwrap().block_on(async {
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();

        let task = dial9_tokio_telemetry::spawn(async move {
            notify2.notified().await;
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            notify.notify_one();
        });

        handle.join().unwrap();
        task.await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    print_three_view_timeline(
        &format!("CASE A: Task->Task Notify (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );
}

// -------------------------------------------------------------
// Case B: Timer Sleep
// -------------------------------------------------------------
fn test_case_b(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    session.runtime.as_ref().unwrap().block_on(async {
        let task = dial9_tokio_telemetry::spawn(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        task.await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    print_three_view_timeline(
        &format!("CASE B: Timer Sleep (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );
}

// -------------------------------------------------------------
// Case C: TCP Stream Readiness
// -------------------------------------------------------------
fn test_case_c(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = dial9_tokio_telemetry::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 16];
            let _ = socket.read(&mut buf).await.unwrap();
            let _ = socket.read(&mut buf).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let mut client = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        use tokio::io::AsyncWriteExt;
        client.write_all(b"hello").await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        client.write_all(b"world").await.unwrap();

        server_task.await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    print_three_view_timeline(
        &format!("CASE C: TCP I/O Readiness (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );
}

// -------------------------------------------------------------
// Case D: I/O While Workers Parked (with External Stimulus Timestamps)
// -------------------------------------------------------------
fn test_case_d(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    let mut ext_write_start = 0u64;
    let mut tokio_io_ready = 0u64;
    let mut task_sched = 0u64;
    let mut task_polled = 0u64;

    session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_conn, server_conn) = tokio::join!(
            TcpStream::connect(addr),
            async {
                let (server_conn, _) = listener.accept().await.unwrap();
                server_conn
            }
        );

        let mut client_conn = client_conn.unwrap();
        let std_stream = server_conn.into_std().unwrap();

        let read_task = dial9_tokio_telemetry::spawn(async move {
            let mut buf = [0u8; 16];
            client_conn.read(&mut buf).await.unwrap()
        });

        // Let all workers settle and park
        tokio::time::sleep(Duration::from_millis(100)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let mut sync_stream = std_stream;
            use std::io::Write;
            ground_truth_probes::record_external_stimulus("WRITE_BEGIN", "Off-thread write to TCP socket");
            sync_stream.write_all(b"ping").unwrap();
            ground_truth_probes::record_external_stimulus("WRITE_DONE", "TCP packet sent");
        });

        handle.join().unwrap();
        read_task.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();

    for ev in &gt_events {
        match ev {
            ProbeEvent::ExternalIoStimulus { t_ns, phase, .. } if *phase == "WRITE_BEGIN" => {
                ext_write_start = *t_ns;
            }
            ProbeEvent::IoReadinessObserved { t_ns, .. } if tokio_io_ready == 0 => {
                tokio_io_ready = *t_ns;
            }
            ProbeEvent::TaskScheduled { t_ns, .. } if task_sched == 0 => {
                task_sched = *t_ns;
            }
            ProbeEvent::WorkerPollStart { t_ns, .. } if task_polled == 0 => {
                task_polled = *t_ns;
            }
            _ => {}
        }
    }

    print_three_view_timeline(
        &format!("CASE D: I/O While Workers Parked (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );

    if ext_write_start > 0 && tokio_io_ready > 0 && task_sched > 0 && task_polled > 0 {
        let delta_driver = tokio_io_ready.saturating_sub(ext_write_start) as f64 / 1_000_000.0;
        let delta_sched = task_sched.saturating_sub(tokio_io_ready) as f64 / 1_000_000.0;
        let delta_poll = task_polled.saturating_sub(task_sched) as f64 / 1_000_000.0;
        let delta_e2e = task_polled.saturating_sub(ext_write_start) as f64 / 1_000_000.0;

        println!("\n--- [CASE D INTERVAL BREAKDOWN] ---");
        println!("  T(external_write):       +{:>8.3} ms", ext_write_start as f64 / 1_000_000.0);
        println!("  T(tokio_io_readiness):   +{:>8.3} ms", tokio_io_ready as f64 / 1_000_000.0);
        println!("  T(task_scheduled):       +{:>8.3} ms", task_sched as f64 / 1_000_000.0);
        println!("  T(task_polled):          +{:>8.3} ms", task_polled as f64 / 1_000_000.0);
        println!("  Δio_driver (Ext -> Tokio): {:>8.3} ms", delta_driver);
        println!("  Δschedule (Ready -> Sched):{:>8.3} ms", delta_sched);
        println!("  Δpoll (Sched -> Poll):     {:>8.3} ms", delta_poll);
        println!("  Δend_to_end (Ext -> Poll): {:>8.3} ms", delta_e2e);
    }
}

// -------------------------------------------------------------
// Case E: Reworked - I/O under Scheduler / CPU Saturation
// -------------------------------------------------------------
fn test_case_e(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    let mut ext_write_start = 0u64;
    let mut tokio_io_ready = 0u64;
    let mut task_sched = 0u64;
    let mut task_polled = 0u64;

    session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_conn, server_conn) = tokio::join!(
            TcpStream::connect(addr),
            async {
                let (server_conn, _) = listener.accept().await.unwrap();
                server_conn
            }
        );

        let mut client_conn = client_conn.unwrap();
        let std_stream = server_conn.into_std().unwrap();

        // Spawn reader task awaiting incoming TCP packet
        let read_task = dial9_tokio_telemetry::spawn(async move {
            let mut buf = [0u8; 16];
            client_conn.read(&mut buf).await.unwrap()
        });

        // Let reader poll once and register waker on driver
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Occupy all N workers with non-yielding CPU compute for 60ms
        let mut compute_handles = Vec::new();
        for _ in 0..workers {
            compute_handles.push(dial9_tokio_telemetry::spawn(async move {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(60) {
                    std::hint::spin_loop();
                }
            }));
        }

        // At t=15ms while all workers are 100% occupied with compute, send TCP data
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(15));
            let mut sync_stream = std_stream;
            use std::io::Write;
            ground_truth_probes::record_external_stimulus("WRITE_BEGIN", "Off-thread write while workers saturated");
            sync_stream.write_all(b"ping").unwrap();
            ground_truth_probes::record_external_stimulus("WRITE_DONE", "TCP packet sent");
        });

        handle.join().unwrap();
        for ch in compute_handles {
            ch.await.unwrap();
        }
        read_task.await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();

    let mut reader_task_id = None;
    for ev in &gt_events {
        match ev {
            ProbeEvent::ExternalIoStimulus { t_ns, phase, .. } if *phase == "WRITE_BEGIN" => {
                ext_write_start = *t_ns;
            }
            ProbeEvent::IoReadinessObserved { t_ns, .. } if tokio_io_ready == 0 => {
                tokio_io_ready = *t_ns;
            }
            ProbeEvent::TaskScheduled { t_ns, task_id, .. } if tokio_io_ready > 0 && task_sched == 0 => {
                task_sched = *t_ns;
                reader_task_id = Some(*task_id);
            }
            ProbeEvent::WorkerPollStart { t_ns, task_id, .. } if reader_task_id == Some(*task_id) && task_polled == 0 => {
                task_polled = *t_ns;
            }
            _ => {}
        }
    }

    print_three_view_timeline(
        &format!("CASE E: I/O Under Scheduler/CPU Saturation (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );

    if ext_write_start > 0 && tokio_io_ready > 0 && task_sched > 0 && task_polled > 0 {
        let delta_driver = tokio_io_ready.saturating_sub(ext_write_start) as f64 / 1_000_000.0;
        let delta_sched = task_sched.saturating_sub(tokio_io_ready) as f64 / 1_000_000.0;
        let delta_poll = task_polled.saturating_sub(task_sched) as f64 / 1_000_000.0;
        let delta_e2e = task_polled.saturating_sub(ext_write_start) as f64 / 1_000_000.0;

        println!("\n--- [CASE E CRITICAL DISCREPANCY ANALYSIS] ---");
        println!("  T(external_stimulus):       +{:>8.3} ms", ext_write_start as f64 / 1_000_000.0);
        println!("  T(tokio_io_readiness):       +{:>8.3} ms", tokio_io_ready as f64 / 1_000_000.0);
        println!("  T(task_scheduled):           +{:>8.3} ms", task_sched as f64 / 1_000_000.0);
        println!("  T(task_polled):              +{:>8.3} ms", task_polled as f64 / 1_000_000.0);
        println!("  --------------------------------------------------");
        println!("  Δio_driver (Driver Service Delay): {:>8.3} ms <=== INVISIBLE TO TOKIO & DIAL9!", delta_driver);
        println!("  Δschedule (Ready -> Sched):        {:>8.3} ms", delta_sched);
        println!("  Δpoll (Sched -> Poll):             {:>8.3} ms", delta_poll);
        println!("  Δend_to_end (Total Real Delay):    {:>8.3} ms", delta_e2e);
    }
}

// -------------------------------------------------------------
// Adversarial Case 1: Wake Coalescing & Wake Suppression
// -------------------------------------------------------------
fn test_adversarial_wake_coalescing(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    let compute_started = Arc::new(AtomicBool::new(false));
    let compute_stop = Arc::new(AtomicBool::new(false));
    let tasks_registered = Arc::new(AtomicUsize::new(0));

    let c_start = compute_started.clone();
    let c_stop = compute_stop.clone();

    session.runtime.as_ref().unwrap().block_on(async {
        let notifies: Vec<Arc<Notify>> = (0..5).map(|_| Arc::new(Notify::new())).collect();
        let mut notified_tasks = Vec::new();

        // 1 blocker task keeping worker 1 busy in controlled non-yielding compute until explicitly released
        let compute_handle = dial9_tokio_telemetry::spawn(async move {
            c_start.store(true, Ordering::SeqCst);
            while !c_stop.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }
        });

        for i in 0..5 {
            let n = notifies[i].clone();
            let reg = tasks_registered.clone();
            notified_tasks.push(dial9_tokio_telemetry::spawn(async move {
                let mut notified = std::pin::pin!(n.notified());
                std::future::poll_fn(|cx| {
                    let res = notified.as_mut().poll(cx);
                    if res.is_pending() {
                        reg.fetch_add(1, Ordering::SeqCst);
                    }
                    res
                }).await;
            }));
        }

        // Wait until blocker task is actively spinning AND all 5 tasks are confirmed pending on their Notify
        while !compute_started.load(Ordering::SeqCst) || tasks_registered.load(Ordering::SeqCst) < 5 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Allow worker 0 (which polled registration) to finish and park
        tokio::time::sleep(Duration::from_millis(30)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Signal task 0, then immediately signal tasks 1..5 in tight sequence from off-thread
        let n_clones = notifies.clone();
        let handle = std::thread::spawn(move || {
            for n in n_clones {
                n.notify_one();
            }
        });

        handle.join().unwrap();
        for t in notified_tasks {
            let _ = t.await;
        }

        // Release the compute worker now that all notified tasks completed
        compute_stop.store(true, Ordering::SeqCst);
        let _ = compute_handle.await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    print_three_view_timeline(
        &format!("ADVERSARIAL: Wake Coalescing (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );

    println!("\n--- [EXACT CAUSAL TASK -> SCHEDULER WAKE DECISION TRACE] ---");
    for ev in &gt_events {
        if let ProbeEvent::SchedulerWakeDecision { task_id, caller, target_worker, num_searching, num_unparked, total_workers, .. } = ev {
            match target_worker {
                Some(w) => println!("  Task {:?} schedule -> SELECTED worker {} (searching={}, unparked={}/{}, caller={})", task_id, w, num_searching, num_unparked, total_workers, caller),
                None => println!("  Task {:?} schedule -> NO worker selected / SUPPRESSED (searching={}, unparked={}/{}, caller={})", task_id, num_searching, num_unparked, total_workers, caller),
            }
        }
    }
}

// -------------------------------------------------------------
// Adversarial Case 2: Work Stealing & Local Queue Head-of-Line Blocking
// -------------------------------------------------------------
fn test_adversarial_work_stealing(workers: usize) {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    session.runtime.as_ref().unwrap().block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        dial9_tokio_telemetry::spawn(async {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(dial9_tokio_telemetry::spawn(async {
                    std::hint::spin_loop();
                }));
            }
            // 50ms compute loop
            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(50) {
                std::hint::spin_loop();
            }
            for h in handles {
                let _ = h.await;
            }
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        ground_truth_probes::disable();
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    print_three_view_timeline(
        &format!("ADVERSARIAL: Work Stealing (workers={})", workers),
        &gt_events,
        &stock_events,
        &dial9_events,
    );
}

// -------------------------------------------------------------
// Statistical Distribution Evaluation
// -------------------------------------------------------------
#[derive(Default, Debug)]
struct Stats {
    samples: Vec<f64>,
}

impl Stats {
    fn add(&mut self, val: f64) {
        self.samples.push(val);
    }

    fn summarize(&mut self) -> (f64, f64, f64, f64) {
        if self.samples.is_empty() {
            return (0.0, 0.0, 0.0, 0.0);
        }
        self.samples
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min = self.samples[0];
        let max = *self.samples.last().unwrap();
        let p50 = self.samples[(self.samples.len() as f64 * 0.50) as usize];
        let p95 = self.samples[((self.samples.len() as f64 * 0.95) as usize).min(self.samples.len() - 1)];
        (min, p50, p95, max)
    }
}

fn run_distribution_benchmarks(runs: usize) {
    println!("\n=======================================================");
    println!("RUNNING STATISTICAL BENCHMARKS ({} iterations per case)", runs);
    println!("=======================================================");

    // Case D Benchmark
    let mut d_delta_driver = Stats::default();
    let mut d_delta_sched = Stats::default();
    let mut d_delta_poll = Stats::default();
    let mut d_delta_e2e = Stats::default();

    for _ in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();
        let mut ext_write = 0u64;
        let mut io_ready = 0u64;
        let mut t_sched = 0u64;
        let mut t_poll = 0u64;

        session.runtime.as_ref().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (client_conn, server_conn) = tokio::join!(
                TcpStream::connect(addr),
                async { listener.accept().await.unwrap().0 }
            );
            let mut client = client_conn.unwrap();
            let std_stream = server_conn.into_std().unwrap();

            let read_task = tokio::spawn(async move {
                let mut buf = [0u8; 16];
                client.read(&mut buf).await.unwrap()
            });

            tokio::time::sleep(Duration::from_millis(30)).await;
            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            let handle = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                let mut s = std_stream;
                use std::io::Write;
                ground_truth_probes::record_external_stimulus("WRITE_BEGIN", "");
                s.write_all(b"x").unwrap();
            });

            handle.join().unwrap();
            read_task.await.unwrap();
            ground_truth_probes::disable();
        });

        let (gt, _, _) = session.finish();
        for ev in &gt {
            match ev {
                ProbeEvent::ExternalIoStimulus { t_ns, .. } => ext_write = *t_ns,
                ProbeEvent::IoReadinessObserved { t_ns, .. } if io_ready == 0 => io_ready = *t_ns,
                ProbeEvent::TaskScheduled { t_ns, .. } if t_sched == 0 => t_sched = *t_ns,
                ProbeEvent::WorkerPollStart { t_ns, .. } if t_poll == 0 => t_poll = *t_ns,
                _ => {}
            }
        }
        if ext_write > 0 && io_ready > 0 && t_sched > 0 && t_poll > 0 {
            d_delta_driver.add(io_ready.saturating_sub(ext_write) as f64 / 1_000_000.0);
            d_delta_sched.add(t_sched.saturating_sub(io_ready) as f64 / 1_000_000.0);
            d_delta_poll.add(t_poll.saturating_sub(t_sched) as f64 / 1_000_000.0);
            d_delta_e2e.add(t_poll.saturating_sub(ext_write) as f64 / 1_000_000.0);
        }
    }

    println!("\n--- [CASE D: I/O While Parked Distribution (N={})] ---", runs);
    let (d_min, d_p50, d_p95, d_max) = d_delta_driver.summarize();
    println!("  Δio_driver:  min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", d_min, d_p50, d_p95, d_max);
    let (s_min, s_p50, s_p95, s_max) = d_delta_sched.summarize();
    println!("  Δschedule:   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", s_min, s_p50, s_p95, s_max);
    let (p_min, p_p50, p_p95, p_max) = d_delta_poll.summarize();
    println!("  Δpoll:       min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", p_min, p_p50, p_p95, p_max);
    let (e_min, e_p50, e_p95, e_max) = d_delta_e2e.summarize();
    println!("  Δend_to_end: min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", e_min, e_p50, e_p95, e_max);

    // Case E Benchmark
    let mut e_delta_driver = Stats::default();
    let mut e_delta_sched = Stats::default();
    let mut e_delta_poll = Stats::default();
    let mut e_delta_e2e = Stats::default();

    for _ in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();
        let mut ext_write = 0u64;
        let mut io_ready = 0u64;
        let mut t_sched = 0u64;
        let mut t_poll = 0u64;

        session.runtime.as_ref().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (client_conn, server_conn) = tokio::join!(
                TcpStream::connect(addr),
                async { listener.accept().await.unwrap().0 }
            );
            let mut client = client_conn.unwrap();
            let std_stream = server_conn.into_std().unwrap();

            let read_task = tokio::spawn(async move {
                let mut buf = [0u8; 16];
                client.read(&mut buf).await.unwrap()
            });

            tokio::time::sleep(Duration::from_millis(20)).await;
            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            let mut compute = Vec::new();
            for _ in 0..2 {
                compute.push(tokio::spawn(async move {
                    let start = Instant::now();
                    while start.elapsed() < Duration::from_millis(40) {
                        std::hint::spin_loop();
                    }
                }));
            }

            let handle = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                let mut s = std_stream;
                use std::io::Write;
                ground_truth_probes::record_external_stimulus("WRITE_BEGIN", "");
                s.write_all(b"x").unwrap();
            });

            handle.join().unwrap();
            for c in compute {
                c.await.unwrap();
            }
            read_task.await.unwrap();
            ground_truth_probes::disable();
        });

        let (gt, _, _) = session.finish();
        let mut reader_task_id = None;
        for ev in &gt {
            match ev {
                ProbeEvent::ExternalIoStimulus { t_ns, .. } => ext_write = *t_ns,
                ProbeEvent::IoReadinessObserved { t_ns, .. } if io_ready == 0 => io_ready = *t_ns,
                ProbeEvent::TaskScheduled { t_ns, task_id, .. } if io_ready > 0 && t_sched == 0 => {
                    t_sched = *t_ns;
                    reader_task_id = Some(*task_id);
                }
                ProbeEvent::WorkerPollStart { t_ns, task_id, .. } if reader_task_id == Some(*task_id) && t_poll == 0 => {
                    t_poll = *t_ns;
                }
                _ => {}
            }
        }
        if ext_write > 0 && io_ready > 0 && t_sched > 0 && t_poll > 0 {
            e_delta_driver.add(io_ready.saturating_sub(ext_write) as f64 / 1_000_000.0);
            e_delta_sched.add(t_sched.saturating_sub(io_ready) as f64 / 1_000_000.0);
            e_delta_poll.add(t_poll.saturating_sub(t_sched) as f64 / 1_000_000.0);
            e_delta_e2e.add(t_poll.saturating_sub(ext_write) as f64 / 1_000_000.0);
        }
    }

    println!("\n--- [CASE E: Saturated Workers Distribution (N={})] ---", runs);
    let (d_min, d_p50, d_p95, d_max) = e_delta_driver.summarize();
    println!("  Δio_driver (Driver Service Delay): min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", d_min, d_p50, d_p95, d_max);
    let (s_min, s_p50, s_p95, s_max) = e_delta_sched.summarize();
    println!("  Δschedule (Ready -> Sched):        min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", s_min, s_p50, s_p95, s_max);
    let (p_min, p_p50, p_p95, p_max) = e_delta_poll.summarize();
    println!("  Δpoll (Sched -> Poll):             min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", p_min, p_p50, p_p95, p_max);
    let (e_min, e_p50, e_p95, e_max) = e_delta_e2e.summarize();
    println!("  Δend_to_end (Total Real Latency):   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", e_min, e_p50, e_p95, e_max);

    // Wake Coalescing Benchmark
    let mut coal_unparks = Stats::default();
    let mut coal_task_suppressed = Stats::default();
    let mut coal_total_suppressed = Stats::default();
    let mut coal_t1_delay = Stats::default();
    let mut coal_coalesced_delay = Stats::default();

    let mut valid_intended_runs = 0usize;
    let mut other_topology_runs = 0usize;
    let mut other_topology_details = Vec::new();

    for run_idx in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();

        let compute_started = Arc::new(AtomicBool::new(false));
        let compute_stop = Arc::new(AtomicBool::new(false));
        let tasks_registered = Arc::new(AtomicUsize::new(0));

        let c_start = compute_started.clone();
        let c_stop = compute_stop.clone();
        let notifies: Vec<Arc<Notify>> = (0..5).map(|_| Arc::new(Notify::new())).collect();
        let mut notified_tasks = Vec::new();

        session.runtime.as_ref().unwrap().block_on(async {
            let compute_handle = tokio::spawn(async move {
                c_start.store(true, Ordering::SeqCst);
                while !c_stop.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            });

            for i in 0..5 {
                let n = notifies[i].clone();
                let reg = tasks_registered.clone();
                notified_tasks.push(tokio::spawn(async move {
                    let mut notified = std::pin::pin!(n.notified());
                    std::future::poll_fn(|cx| {
                        let res = notified.as_mut().poll(cx);
                        if res.is_pending() {
                            reg.fetch_add(1, Ordering::SeqCst);
                        }
                        res
                    }).await;
                }));
            }

            while !compute_started.load(Ordering::SeqCst) || tasks_registered.load(Ordering::SeqCst) < 5 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // Allow the worker that polled tasks to finish and park
            tokio::time::sleep(Duration::from_millis(30)).await;

            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            let n_clones = notifies.clone();
            let handle = std::thread::spawn(move || {
                for n in n_clones {
                    n.notify_one();
                }
            });

            handle.join().unwrap();
            for t in notified_tasks {
                let _ = t.await;
            }
            compute_stop.store(true, Ordering::SeqCst);
            let _ = compute_handle.await;

            tokio::time::sleep(Duration::from_millis(20)).await;
            ground_truth_probes::disable();
        });

        let (gt, _, _) = session.finish();
        let mut scheduled_tasks = Vec::new();
        let mut task_sched_times = std::collections::HashMap::new();

        for ev in &gt {
            if let ProbeEvent::TaskScheduled { t_ns, task_id, .. } = ev {
                if !scheduled_tasks.contains(task_id) {
                    scheduled_tasks.push(*task_id);
                }
                task_sched_times.insert(*task_id, *t_ns);
            }
        }

        let mut task_unparks = 0usize;
        let mut task_suppressions = 0usize;

        for ev in &gt {
            if let ProbeEvent::SchedulerWakeDecision { task_id: Some(tid), target_worker, .. } = ev {
                if scheduled_tasks.contains(tid) {
                    if target_worker.is_some() {
                        task_unparks += 1;
                    } else {
                        task_suppressions += 1;
                    }
                }
            }
        }

        let total_none_decisions = gt.iter().filter(|ev| matches!(ev, ProbeEvent::SchedulerWakeDecision { target_worker: None, .. })).count();

        let mut t1_poll = 0u64;
        let mut other_delays = Vec::new();
        let first_task_id = scheduled_tasks.first().copied();

        for ev in &gt {
            if let ProbeEvent::WorkerPollStart { t_ns, task_id, .. } = ev {
                if Some(*task_id) == first_task_id && t1_poll == 0 {
                    t1_poll = *t_ns;
                } else if scheduled_tasks.contains(task_id) {
                    if let Some(s_time) = task_sched_times.get(task_id) {
                        other_delays.push(t_ns.saturating_sub(*s_time) as f64 / 1_000_000.0);
                    }
                }
            }
        }

        let t1_sched = first_task_id.and_then(|id| task_sched_times.get(&id).copied()).unwrap_or(0);

        let is_intended = task_unparks == 1 && task_suppressions == 4;
        if is_intended {
            valid_intended_runs += 1;
            coal_unparks.add(task_unparks as f64);
            coal_task_suppressed.add(task_suppressions as f64);
            coal_total_suppressed.add(total_none_decisions as f64);
            if t1_sched > 0 && t1_poll > 0 {
                coal_t1_delay.add(t1_poll.saturating_sub(t1_sched) as f64 / 1_000_000.0);
            }
            if !other_delays.is_empty() {
                let avg: f64 = other_delays.iter().sum::<f64>() / other_delays.len() as f64;
                coal_coalesced_delay.add(avg);
            }
        } else {
            other_topology_runs += 1;
            other_topology_details.push(format!(
                "run #{}: task_unparks={}, task_suppressions={}, total_none={}",
                run_idx + 1, task_unparks, task_suppressions, total_none_decisions
            ));
        }
    }

    println!("\n--- [ADVERSARIAL: Wake Coalescing Distribution (N={})] ---", runs);
    println!("  Attempted runs:                                {}", runs);
    println!("  Runs matching intended scheduler precondition: {}", valid_intended_runs);
    println!("  Runs with alternate scheduler topology:        {}", other_topology_runs);
    if other_topology_runs > 0 {
        println!("    Alternate topologies observed: {:?}", other_topology_details);
    }
    if valid_intended_runs > 0 {
        println!("\n  [Metrics across {} Valid Intended-Precondition Runs]:", valid_intended_runs);
        let (u_min, u_p50, u_p95, u_max) = coal_unparks.summarize();
        println!("  Unpark requests dispatched:      min={:.0}  p50={:.0}  p95={:.0}  max={:.0}", u_min, u_p50, u_p95, u_max);
        let (ts_min, ts_p50, ts_p95, ts_max) = coal_task_suppressed.summarize();
        println!("  Task-correlated wake suppressed: min={:.0}  p50={:.0}  p95={:.0}  max={:.0} (tasks 2-5 coalesced)", ts_min, ts_p50, ts_p95, ts_max);
        let (sup_min, sup_p50, sup_p95, sup_max) = coal_total_suppressed.summarize();
        println!("  Total target=None decisions:     min={:.0}  p50={:.0}  p95={:.0}  max={:.0}", sup_min, sup_p50, sup_p95, sup_max);
        let (t1_min, t1_p50, t1_p95, t1_max) = coal_t1_delay.summarize();
        println!("  Task 1 Sched->Poll Latency:      min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", t1_min, t1_p50, t1_p95, t1_max);
        let (co_min, co_p50, co_p95, co_max) = coal_coalesced_delay.summarize();
        println!("  Coalesced Tasks Sched->Poll:     min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", co_min, co_p50, co_p95, co_max);
    }

    // Work Stealing Benchmark
    let mut steal_stolen_delay = Stats::default();
    let mut steal_stranded_delay = Stats::default();

    for _ in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();

        session.runtime.as_ref().unwrap().block_on(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            tokio::spawn(async {
                let mut handles = Vec::new();
                for _ in 0..4 {
                    handles.push(tokio::spawn(async {
                        std::hint::spin_loop();
                    }));
                }
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(40) {
                    std::hint::spin_loop();
                }
                for h in handles {
                    let _ = h.await;
                }
            });

            tokio::time::sleep(Duration::from_millis(60)).await;
            ground_truth_probes::disable();
        });

        let (gt, _, _) = session.finish();
        let mut subtask_sched = Vec::new();
        let mut subtask_poll = std::collections::HashMap::new();

        for ev in &gt {
            match ev {
                ProbeEvent::TaskScheduled { t_ns, task_id, is_local } => {
                    // Subtasks spawned by the worker task are placed locally
                    if *is_local {
                        subtask_sched.push((*task_id, *t_ns));
                    }
                }
                ProbeEvent::WorkerPollStart { t_ns, task_id, .. } => {
                    subtask_poll.insert(*task_id, *t_ns);
                }
                _ => {}
            }
        }

        let mut delays: Vec<f64> = subtask_sched
            .iter()
            .filter_map(|(tid, st)| {
                subtask_poll.get(tid).map(|pt| pt.saturating_sub(*st) as f64 / 1_000_000.0)
            })
            .collect();
        delays.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if delays.len() >= 2 {
            // Shortest delays are stolen tasks polled by worker 0
            steal_stolen_delay.add(delays[0]);
            // Longest delay is the stranded task waiting behind the 40ms compute loop on worker 1
            steal_stranded_delay.add(*delays.last().unwrap());
        }
    }

    println!("\n--- [ADVERSARIAL: Work Stealing Distribution (N={})] ---", runs);
    let (st_min, st_p50, st_p95, st_max) = steal_stolen_delay.summarize();
    println!("  Stolen Tasks Sched->Poll:    min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", st_min, st_p50, st_p95, st_max);
    let (str_min, str_p50, str_p95, str_max) = steal_stranded_delay.summarize();
    println!("  Stranded Task Sched->Poll:   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms", str_min, str_p50, str_p95, str_max);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let is_benchmark = args.iter().any(|a| a == "--benchmark" || a == "-b");

    println!("===============================================================");
    println!("TOKIO / DIAL9 RUNTIME OBSERVABILITY GAP INVESTIGATION");
    println!("3-Way Empirical Observability Matrix (Ground Truth vs Stock vs Dial9)");
    println!("===============================================================");

    if is_benchmark {
        run_distribution_benchmarks(30);
        return;
    }

    println!("\n>>> RUNNING CASE A: Task->Task Notify <<<");
    test_case_a(1);
    test_case_a(2);

    println!("\n>>> RUNNING CASE B: Timer Sleep <<<");
    test_case_b(1);
    test_case_b(2);

    println!("\n>>> RUNNING CASE C: TCP I/O Readiness <<<");
    test_case_c(1);
    test_case_c(2);

    println!("\n>>> RUNNING CASE D: I/O Readiness While Workers Parked <<<");
    test_case_d(2);
    test_case_d(4);

    println!("\n>>> RUNNING CASE E: High Load with I/O Readiness <<<");
    test_case_e(2);
    test_case_e(4);

    println!("\n>>> RUNNING ADVERSARIAL CASES <<<");
    test_adversarial_wake_coalescing(2);
    test_adversarial_work_stealing(2);

    println!("\n===============================================================");
    println!("INVESTIGATION RUN COMPLETED");
    println!("Run with `--benchmark` to compute 30-run statistical distributions.");
    println!("===============================================================");
}
