use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
            ProbeEvent::WorkerUnparkDispatchBegin {
                t_ns,
                target_worker,
                mechanism,
            } => (
                *t_ns,
                format!(
                    "WORKER_UNPARK_DISPATCH_BEGIN target_worker={} mechanism={}",
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
            extra.push_str(&format!(
                " schedule_latency={:.3}ms",
                (lat as f64) / 1_000_000.0
            ));
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

    let reader_task_id = session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_conn, server_conn) = tokio::join!(TcpStream::connect(addr), async {
            let (server_conn, _) = listener.accept().await.unwrap();
            server_conn
        });

        let mut client_conn = client_conn.unwrap();
        let std_stream = server_conn.into_std().unwrap();

        let read_task = dial9_tokio_telemetry::spawn(async move {
            let mut buf = [0u8; 16];
            client_conn.read(&mut buf).await.unwrap()
        });
        let reader_id = read_task.id().to_string().parse::<u64>().unwrap();

        // Let all workers settle and park
        tokio::time::sleep(Duration::from_millis(100)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let mut sync_stream = std_stream;
            use std::io::Write;
            ground_truth_probes::record_external_stimulus(
                "WRITE_BEGIN",
                "Off-thread write to TCP socket",
            );
            sync_stream.write_all(b"ping").unwrap();
            ground_truth_probes::record_external_stimulus("WRITE_DONE", "TCP packet sent");
        });

        handle.join().unwrap();
        read_task.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        ground_truth_probes::disable();
        reader_id
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
            ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                if *task_id == reader_task_id && task_sched == 0 =>
            {
                task_sched = *t_ns;
            }
            ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                if *task_id == reader_task_id && task_polled == 0 =>
            {
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
        println!(
            "  T(external_write):       +{:>8.3} ms",
            ext_write_start as f64 / 1_000_000.0
        );
        println!(
            "  T(tokio_io_readiness):   +{:>8.3} ms",
            tokio_io_ready as f64 / 1_000_000.0
        );
        println!(
            "  T(task_scheduled):       +{:>8.3} ms",
            task_sched as f64 / 1_000_000.0
        );
        println!(
            "  T(task_polled):          +{:>8.3} ms",
            task_polled as f64 / 1_000_000.0
        );
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

    let reader_task_id = session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (client_conn, server_conn) = tokio::join!(TcpStream::connect(addr), async {
            let (server_conn, _) = listener.accept().await.unwrap();
            server_conn
        });

        let mut client_conn = client_conn.unwrap();
        let std_stream = server_conn.into_std().unwrap();

        // Spawn reader task awaiting incoming TCP packet
        let read_task = dial9_tokio_telemetry::spawn(async move {
            let mut buf = [0u8; 16];
            client_conn.read(&mut buf).await.unwrap()
        });
        let reader_id = read_task.id().to_string().parse::<u64>().unwrap();

        // Let reader poll once and register waker on driver
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Occupy all N workers with non-yielding CPU compute for 40ms
        let compute_started = Arc::new(AtomicUsize::new(0));
        let mut compute_handles = Vec::new();
        for _ in 0..workers {
            let c_started = compute_started.clone();
            compute_handles.push(dial9_tokio_telemetry::spawn(async move {
                c_started.fetch_add(1, Ordering::SeqCst);
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(40) {
                    std::hint::spin_loop();
                }
            }));
        }

        // Wait until all workers are confirmed actively executing the non-yielding loop,
        // then wait 10ms into the compute window before sending TCP data
        let c_started = compute_started.clone();
        let handle = std::thread::spawn(move || {
            while c_started.load(Ordering::SeqCst) < workers {
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(10));
            let mut sync_stream = std_stream;
            use std::io::Write;
            ground_truth_probes::record_external_stimulus(
                "WRITE_BEGIN",
                "Off-thread write while workers saturated",
            );
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
        reader_id
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
            ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                if *task_id == reader_task_id && task_sched == 0 =>
            {
                task_sched = *t_ns;
            }
            ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                if *task_id == reader_task_id && task_polled == 0 =>
            {
                task_polled = *t_ns;
            }
            _ => {}
        }
    }

    print_three_view_timeline(
        &format!(
            "CASE E: I/O Under Scheduler/CPU Saturation (workers={})",
            workers
        ),
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
        println!(
            "  T(external_stimulus):       +{:>8.3} ms",
            ext_write_start as f64 / 1_000_000.0
        );
        println!(
            "  T(tokio_io_readiness):       +{:>8.3} ms",
            tokio_io_ready as f64 / 1_000_000.0
        );
        println!(
            "  T(task_scheduled):           +{:>8.3} ms",
            task_sched as f64 / 1_000_000.0
        );
        println!(
            "  T(task_polled):              +{:>8.3} ms",
            task_polled as f64 / 1_000_000.0
        );
        println!("  --------------------------------------------------");
        println!(
            "  Δio_driver (Driver Service Delay): {:>8.3} ms <=== INVISIBLE TO TOKIO & DIAL9!",
            delta_driver
        );
        println!(
            "  Δschedule (Ready -> Sched):        {:>8.3} ms",
            delta_sched
        );
        println!(
            "  Δpoll (Sched -> Poll):             {:>8.3} ms",
            delta_poll
        );
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
                })
                .await;
            }));
        }

        // Wait until blocker task is actively spinning AND all 5 tasks are confirmed pending on their Notify
        while !compute_started.load(Ordering::SeqCst) || tasks_registered.load(Ordering::SeqCst) < 5
        {
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
        if let ProbeEvent::SchedulerWakeDecision {
            task_id,
            caller,
            target_worker,
            num_searching,
            num_unparked,
            total_workers,
            ..
        } = ev
        {
            match target_worker {
                Some(w) => println!(
                    "  Task {:?} schedule -> SELECTED worker {} (searching={}, unparked={}/{}, caller={})",
                    task_id, w, num_searching, num_unparked, total_workers, caller
                ),
                None => println!(
                    "  Task {:?} schedule -> NO worker selected / SUPPRESSED (searching={}, unparked={}/{}, caller={})",
                    task_id, num_searching, num_unparked, total_workers, caller
                ),
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
        let p95 =
            self.samples[((self.samples.len() as f64 * 0.95) as usize).min(self.samples.len() - 1)];
        (min, p50, p95, max)
    }

    fn summarize_with_p99(&mut self) -> (f64, f64, f64, f64, f64) {
        if self.samples.is_empty() {
            return (0.0, 0.0, 0.0, 0.0, 0.0);
        }
        self.samples
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min = self.samples[0];
        let max = *self.samples.last().unwrap();
        let p50 = self.samples[(self.samples.len() as f64 * 0.50) as usize];
        let p95 =
            self.samples[((self.samples.len() as f64 * 0.95) as usize).min(self.samples.len() - 1)];
        let p99 =
            self.samples[((self.samples.len() as f64 * 0.99) as usize).min(self.samples.len() - 1)];
        (min, p50, p95, p99, max)
    }
}

// -------------------------------------------------------------
// Realistic Network-Service Workload (RustFS Chunk Ingestion Model)
// -------------------------------------------------------------

/// Simulates synchronous CPU processing during object storage chunk ingestion
/// (e.g. AWS SigV4 payload SHA-256 calculation, CRC32C checksumming, and
/// Reed-Solomon erasure coding parity generation across shards) executed
/// synchronously on runtime worker threads without yielding.
fn simulate_chunk_processing(duration: Duration) -> u32 {
    let start = Instant::now();
    let mut hash = 0x811c9dc5u32;
    while start.elapsed() < duration {
        hash = hash.wrapping_mul(0x01000193) ^ 0x5a;
        std::hint::spin_loop();
    }
    hash
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
struct LoadTierMetrics {
    concurrency: usize,
    offered_load_rps: f64,
    achieved_rps: f64,
    attempted_requests: usize,
    completed_requests: usize,
    failed_requests: usize,
    lat_min_ms: f64,
    lat_p50_ms: f64,
    lat_p95_ms: f64,
    lat_p99_ms: f64,
    lat_max_ms: f64,
    handler_stock_sched_p50_ms: f64,
    handler_stock_sched_p95_ms: f64,
    handler_stock_sched_max_ms: f64,
    handler_dial9_delay_p50_ms: f64,
    handler_dial9_delay_p95_ms: f64,
    handler_dial9_delay_max_ms: f64,
}

fn run_network_service_load_tier(
    workers: usize,
    concurrency: usize,
    requests_per_client: usize,
    client_pacing: Duration,
    chunk_compute: Duration,
) -> LoadTierMetrics {
    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    let server_shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_signal = server_shutdown.clone();

    let handler_task_ids = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let h_ids = handler_task_ids.clone();

    let client_latencies = Arc::new(Mutex::new(Vec::new()));
    let attempted_count = Arc::new(AtomicUsize::new(0));
    let completed_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));

    let tier_start = Instant::now();

    session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let s_shutdown = shutdown_signal.clone();
        let accept_handle = dial9_tokio_telemetry::spawn(async move {
            while !s_shutdown.load(Ordering::Relaxed) {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let conn_shutdown = s_shutdown.clone();
                        let conn_task = dial9_tokio_telemetry::spawn(async move {
                            let mut socket = socket;
                            let _ = socket.set_nodelay(true);
                            let mut req_buf = [0u8; 8];
                            while !conn_shutdown.load(Ordering::Relaxed) {
                                match socket.read_exact(&mut req_buf).await {
                                    Ok(_) => {
                                        let req_id = u32::from_le_bytes([
                                            req_buf[0], req_buf[1], req_buf[2], req_buf[3],
                                        ]);
                                        let compute_us = u32::from_le_bytes([
                                            req_buf[4], req_buf[5], req_buf[6], req_buf[7],
                                        ]);
                                        let hash = simulate_chunk_processing(
                                            Duration::from_micros(compute_us as u64),
                                        );
                                        let mut resp = [0u8; 8];
                                        resp[0..4].copy_from_slice(&req_id.to_le_bytes());
                                        resp[4..8].copy_from_slice(&hash.to_le_bytes());
                                        if socket.write_all(&resp).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        });
                        if let Ok(tid) = conn_task.id().to_string().parse::<u64>() {
                            h_ids.lock().unwrap().insert(tid);
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(15)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let mut client_threads = Vec::new();

        for client_idx in 0..concurrency {
            let lat_sink = client_latencies.clone();
            let a_count = attempted_count.clone();
            let c_count = completed_count.clone();
            let f_count = failed_count.clone();

            client_threads.push(std::thread::spawn(move || {
                let stream_res = std::net::TcpStream::connect(addr);
                let mut stream = match stream_res {
                    Ok(s) => s,
                    Err(_) => {
                        f_count.fetch_add(requests_per_client, Ordering::SeqCst);
                        a_count.fetch_add(requests_per_client, Ordering::SeqCst);
                        return;
                    }
                };
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

                use std::io::{Read, Write};
                for req_i in 0..requests_per_client {
                    if !client_pacing.is_zero() && req_i > 0 {
                        std::thread::sleep(client_pacing);
                    }
                    a_count.fetch_add(1, Ordering::SeqCst);
                    let req_id = ((client_idx * 100_000) + req_i) as u32;
                    let compute_us = chunk_compute.as_micros() as u32;

                    let mut req_buf = [0u8; 8];
                    req_buf[0..4].copy_from_slice(&req_id.to_le_bytes());
                    req_buf[4..8].copy_from_slice(&compute_us.to_le_bytes());

                    let t_send = ground_truth_probes::now_ns();
                    if stream.write_all(&req_buf).is_err() {
                        f_count.fetch_add(1, Ordering::SeqCst);
                        break;
                    }

                    let mut resp_buf = [0u8; 8];
                    if stream.read_exact(&mut resp_buf).is_err() {
                        f_count.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    let t_recv = ground_truth_probes::now_ns();
                    let resp_id =
                        u32::from_le_bytes([resp_buf[0], resp_buf[1], resp_buf[2], resp_buf[3]]);
                    let resp_hash =
                        u32::from_le_bytes([resp_buf[4], resp_buf[5], resp_buf[6], resp_buf[7]]);
                    if resp_id != req_id || resp_hash == 0 {
                        f_count.fetch_add(1, Ordering::SeqCst);
                        break;
                    }

                    let lat_ms = t_recv.saturating_sub(t_send) as f64 / 1_000_000.0;
                    lat_sink.lock().unwrap().push(lat_ms);
                    c_count.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for ch in client_threads {
            let _ = ch.join();
        }

        ground_truth_probes::disable();
        shutdown_signal.store(true, Ordering::SeqCst);
        accept_handle.abort();
    });

    let tier_duration = tier_start.elapsed().as_secs_f64();
    let (_gt_events, stock_events, dial9_events) = session.finish();

    let attempted = attempted_count.load(Ordering::SeqCst);
    let completed = completed_count.load(Ordering::SeqCst);
    let failed = failed_count.load(Ordering::SeqCst);

    let mut lat_stats = Stats::default();
    for lat in client_latencies.lock().unwrap().iter() {
        lat_stats.add(*lat);
    }
    let (l_min, l_p50, l_p95, l_p99, l_max) = lat_stats.summarize_with_p99();

    // Filter Stock Tokio schedule latency strictly for the identified handler tasks
    let target_tids = handler_task_ids.lock().unwrap().clone();
    let mut stock_sched_stats = Stats::default();
    for ev in &stock_events {
        if let Some(tid) = ev.task_id {
            if target_tids.contains(&tid) {
                if let Some(lat_ns) = ev.schedule_latency_ns {
                    stock_sched_stats.add(lat_ns as f64 / 1_000_000.0);
                }
            }
        }
    }
    let (_, s_p50, s_p95, _, s_max) = stock_sched_stats.summarize_with_p99();

    // Filter Dial9 wake-to-poll delays strictly for the identified handler tasks
    let mut dial9_wakes_by_task: std::collections::HashMap<u64, Vec<u64>> =
        std::collections::HashMap::new();
    for e in &dial9_events {
        if let Dial9Event::WakeEvent(w) = e {
            if target_tids.contains(&w.woken_task_id) {
                dial9_wakes_by_task
                    .entry(w.woken_task_id)
                    .or_default()
                    .push(w.timestamp_ns);
            }
        }
    }
    for v in dial9_wakes_by_task.values_mut() {
        v.sort_unstable();
    }
    let mut dial9_handler_delays = Vec::new();
    for e in &dial9_events {
        if let Dial9Event::PollStartEvent(p) = e {
            if target_tids.contains(&p.task_id) {
                if let Some(wakes) = dial9_wakes_by_task.get(&p.task_id) {
                    let idx = wakes.partition_point(|&t| t <= p.timestamp_ns);
                    if idx > 0 {
                        let delay = p.timestamp_ns - wakes[idx - 1];
                        if delay > 0 && delay < 1_000_000_000 {
                            dial9_handler_delays.push(delay);
                        }
                    }
                }
            }
        }
    }
    let mut dial9_stats = Stats::default();
    for d_ns in dial9_handler_delays {
        dial9_stats.add(d_ns as f64 / 1_000_000.0);
    }
    let (_, d_p50, d_p95, _, d_max) = dial9_stats.summarize_with_p99();

    let achieved_rps = if tier_duration > 0.0 {
        completed as f64 / tier_duration
    } else {
        0.0
    };
    let offered_rps = if tier_duration > 0.0 {
        attempted as f64 / tier_duration
    } else {
        0.0
    };

    LoadTierMetrics {
        concurrency,
        offered_load_rps: offered_rps,
        achieved_rps,
        attempted_requests: attempted,
        completed_requests: completed,
        failed_requests: failed,
        lat_min_ms: l_min,
        lat_p50_ms: l_p50,
        lat_p95_ms: l_p95,
        lat_p99_ms: l_p99,
        lat_max_ms: l_max,
        handler_stock_sched_p50_ms: s_p50,
        handler_stock_sched_p95_ms: s_p95,
        handler_stock_sched_max_ms: s_max,
        handler_dial9_delay_p50_ms: d_p50,
        handler_dial9_delay_p95_ms: d_p95,
        handler_dial9_delay_max_ms: d_max,
    }
}

fn test_network_service_workload(workers: usize) {
    println!("\n=======================================================");
    println!(
        "CONTROLLED REPRODUCTION: S3 Chunk Ingestion Under Forced Worker Saturation (workers={})",
        workers
    );
    println!("=======================================================");

    let session = InstrumentedSession::new(workers);
    let stock_rec = session.stock_rec.clone();

    let mut ext_write_start = 0u64;
    let mut ext_write_done = 0u64;
    let mut tokio_io_ready = 0u64;
    let mut task_sched = 0u64;
    let mut task_polled = 0u64;

    let probe_handler_task_id = Arc::new(AtomicU64::new(0));
    let p_tid = probe_handler_task_id.clone();

    let shutdown_signal = Arc::new(AtomicBool::new(false));

    session.runtime.as_ref().unwrap().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_handle = dial9_tokio_telemetry::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                let mut socket = socket;
                let _ = socket.set_nodelay(true);
                let h_task = dial9_tokio_telemetry::spawn(async move {
                    let mut req_buf = [0u8; 8];
                    if socket.read_exact(&mut req_buf).await.is_ok() {
                        let req_id =
                            u32::from_le_bytes([req_buf[0], req_buf[1], req_buf[2], req_buf[3]]);
                        let compute_us =
                            u32::from_le_bytes([req_buf[4], req_buf[5], req_buf[6], req_buf[7]]);
                        let hash =
                            simulate_chunk_processing(Duration::from_micros(compute_us as u64));
                        let mut resp = [0u8; 8];
                        resp[0..4].copy_from_slice(&req_id.to_le_bytes());
                        resp[4..8].copy_from_slice(&hash.to_le_bytes());
                        let _ = socket.write_all(&resp).await;
                    }
                });
                if let Ok(tid) = h_task.id().to_string().parse::<u64>() {
                    p_tid.store(tid, Ordering::SeqCst);
                }
                let _ = h_task.await;
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut probe_stream = std::net::TcpStream::connect(addr).unwrap();
        probe_stream.set_nodelay(true).unwrap();

        // Wait for connection to be accepted and reader task to be parked on read_exact
        tokio::time::sleep(Duration::from_millis(30)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Occupy all N workers with non-yielding chunk compute for 40ms
        // (representing in-flight S3 chunk processing: hashing / erasure coding)
        let bg_compute_started = Arc::new(AtomicUsize::new(0));
        let mut compute_handles = Vec::new();
        for _ in 0..workers {
            let cs = bg_compute_started.clone();
            compute_handles.push(dial9_tokio_telemetry::spawn(async move {
                cs.fetch_add(1, Ordering::SeqCst);
                simulate_chunk_processing(Duration::from_millis(40));
            }));
        }

        let cs = bg_compute_started.clone();
        let target_handle = std::thread::spawn(move || {
            // Wait until both worker tasks have entered their chunk compute
            while cs.load(Ordering::SeqCst) < workers {
                std::thread::sleep(Duration::from_millis(1));
            }
            // Send request 10ms into the 40ms compute window
            std::thread::sleep(Duration::from_millis(10));

            let mut req = [0u8; 8];
            req[0..4].copy_from_slice(&(9999u32).to_le_bytes());
            req[4..8].copy_from_slice(&(10_000u32).to_le_bytes());

            use std::io::{Read, Write};
            ground_truth_probes::record_external_stimulus(
                "WRITE_BEGIN",
                "Off-thread request write under service load",
            );
            probe_stream.write_all(&req).unwrap();
            ground_truth_probes::record_external_stimulus("WRITE_DONE", "TCP request packet sent");

            let mut resp = [0u8; 8];
            probe_stream.read_exact(&mut resp).unwrap();
        });

        target_handle.join().unwrap();
        for ch in compute_handles {
            ch.await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        ground_truth_probes::disable();
        shutdown_signal.store(true, Ordering::SeqCst);
        let _ = accept_handle.await;
    });

    let (gt_events, stock_events, dial9_events) = session.finish();
    let target_tid = probe_handler_task_id.load(Ordering::SeqCst);

    for ev in &gt_events {
        match ev {
            ProbeEvent::ExternalIoStimulus { t_ns, phase, .. } if *phase == "WRITE_BEGIN" => {
                ext_write_start = *t_ns;
            }
            ProbeEvent::ExternalIoStimulus { t_ns, phase, .. } if *phase == "WRITE_DONE" => {
                ext_write_done = *t_ns;
            }
            ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                if target_tid > 0
                    && *task_id == target_tid
                    && task_sched == 0
                    && *t_ns >= ext_write_start =>
            {
                task_sched = *t_ns;
            }
            ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                if target_tid > 0
                    && *task_id == target_tid
                    && task_polled == 0
                    && *t_ns >= ext_write_start =>
            {
                task_polled = *t_ns;
            }
            _ => {}
        }
    }

    // Correlate the specific IoReadinessObserved event that woke target_tid
    let mut target_wake_idx = None;
    for (idx, ev) in gt_events.iter().enumerate() {
        if let ProbeEvent::TaskWakeByVal {
            t_ns,
            task_id,
            submitted,
        } = ev
        {
            if *task_id == target_tid && *submitted && *t_ns >= ext_write_start {
                target_wake_idx = Some(idx);
                break;
            }
        }
    }

    if let Some(w_idx) = target_wake_idx {
        for ev in gt_events[..w_idx].iter().rev() {
            if let ProbeEvent::IoReadinessObserved { t_ns, .. } = ev {
                if *t_ns >= ext_write_start {
                    tokio_io_ready = *t_ns;
                    break;
                }
            }
        }
    }

    print_three_view_timeline(
        &format!(
            "CONTROLLED REPRODUCTION: S3 Chunk Ingestion Under Forced Saturation (workers={}, task_id={})",
            workers, target_tid
        ),
        &gt_events,
        &stock_events,
        &dial9_events,
    );

    // Extract exact Stock Tokio schedule latency for target_tid
    let stock_target_lat = stock_events.iter().find_map(|ev| {
        if ev.task_id == Some(target_tid) {
            ev.schedule_latency_ns
        } else {
            None
        }
    });

    // Extract exact Dial9 delay for target_tid
    let mut d_wake = 0u64;
    let mut d_poll = 0u64;
    for ev in &dial9_events {
        match ev {
            Dial9Event::WakeEvent(w) if w.woken_task_id == target_tid && d_wake == 0 => {
                d_wake = w.timestamp_ns;
            }
            Dial9Event::PollStartEvent(p)
                if p.task_id == target_tid && d_wake > 0 && d_poll == 0 =>
            {
                d_poll = p.timestamp_ns;
            }
            _ => {}
        }
    }
    let dial9_target_delay = if d_poll > d_wake {
        Some(d_poll - d_wake)
    } else {
        None
    };

    if ext_write_start > 0 && tokio_io_ready > 0 && task_sched > 0 && task_polled > 0 {
        let delta_driver = tokio_io_ready.saturating_sub(ext_write_start) as f64 / 1_000_000.0;
        let delta_sched = task_sched.saturating_sub(tokio_io_ready) as f64 / 1_000_000.0;
        let delta_poll = task_polled.saturating_sub(task_sched) as f64 / 1_000_000.0;
        let delta_e2e = task_polled.saturating_sub(ext_write_start) as f64 / 1_000_000.0;

        println!("\n--- [CONTROLLED FORCED-SATURATION DISCREPANCY ANALYSIS] ---");
        println!("  Target Task ID:             {}", target_tid);
        println!(
            "  T(external_write_begin):    +{:>8.3} ms (client initiated write_all)",
            ext_write_start as f64 / 1_000_000.0
        );
        println!(
            "  T(external_write_done):     +{:>8.3} ms (client finished write_all)",
            ext_write_done as f64 / 1_000_000.0
        );
        println!(
            "  T(tokio_io_readiness):       +{:>8.3} ms (Driver::turn runs epoll_wait and observes socket readiness)",
            tokio_io_ready as f64 / 1_000_000.0
        );
        println!(
            "  T(task_scheduled):           +{:>8.3} ms (task waker called, placed on worker queue)",
            task_sched as f64 / 1_000_000.0
        );
        println!(
            "  T(task_polled):              +{:>8.3} ms (worker polls request handler task)",
            task_polled as f64 / 1_000_000.0
        );
        println!(
            "  --------------------------------------------------------------------------------------------------"
        );
        println!(
            "  Δdriver_observation (Write Init -> Driver Ready): {:>8.3} ms <=== INVISIBLE TO TOKIO & DIAL9!",
            delta_driver
        );
        println!(
            "  Δschedule (Ready -> Sched):                      {:>8.3} ms",
            delta_sched
        );
        println!(
            "  Δpoll (Sched -> Poll):                           {:>8.3} ms",
            delta_poll
        );
        println!(
            "  Δtotal_real (Write Init -> Task Poll):           {:>8.3} ms",
            delta_e2e
        );
        if let Some(s_lat) = stock_target_lat {
            println!(
                "  Stock Tokio Schedule Latency (Task {}):          {:>8.3} ms",
                target_tid,
                s_lat as f64 / 1_000_000.0
            );
        }
        if let Some(d_delay) = dial9_target_delay {
            println!(
                "  Dial9 Wake-to-Poll Delay (Task {}):              {:>8.3} ms",
                target_tid,
                d_delay as f64 / 1_000_000.0
            );
        }
    }

    // Run bounded load sweep (single overview run)
    run_network_service_load_sweep(workers, 1);
}

fn run_network_service_load_sweep(workers: usize, iterations: usize) {
    println!(
        "\n========================================================================================================================"
    );
    println!(
        "CLOSED-LOOP CONCURRENCY LOAD SWEEP: S3 Ingestion Model (workers={}, iterations={})",
        workers, iterations
    );
    println!(
        "========================================================================================================================"
    );

    // Constant chunk compute cost across all tiers: 5 ms per request
    let constant_compute = Duration::from_millis(5);

    let tiers = vec![
        (
            1,
            20,
            Duration::from_millis(15),
            "Tier 1: Concurrency 1 (Low-Load Baseline, 15ms pacing)",
        ),
        (
            2,
            30,
            Duration::ZERO,
            "Tier 2: Concurrency 2 (Balanced Capacity, 0ms pacing)",
        ),
        (
            4,
            40,
            Duration::ZERO,
            "Tier 3: Concurrency 4 (2x Oversubscription, 0ms pacing)",
        ),
        (
            8,
            48,
            Duration::ZERO,
            "Tier 4: Concurrency 8 (4x Oversubscription, 0ms pacing)",
        ),
    ];

    println!(
        "Conc  Offered(rps)  Achieved(rps)  Attempted  Completed  Failed  Lat p50(ms)  Lat p95(ms)  Lat max(ms)  Stock p50  Stock p95  Dial9 p50  Dial9 p95"
    );
    println!(
        "----------------------------------------------------------------------------------------------------------------------------------------------------"
    );

    for (conc, reqs_per_client, pacing, _desc) in tiers {
        let mut off_stats = Stats::default();
        let mut ach_stats = Stats::default();
        let mut lat_p50_stats = Stats::default();
        let mut lat_p95_stats = Stats::default();
        let mut lat_max_stats = Stats::default();
        let mut stock_p50_stats = Stats::default();
        let mut stock_p95_stats = Stats::default();
        let mut dial9_p50_stats = Stats::default();
        let mut dial9_p95_stats = Stats::default();
        let mut total_attempted = 0usize;
        let mut total_completed = 0usize;
        let mut total_failed = 0usize;

        for _ in 0..iterations {
            let m = run_network_service_load_tier(
                workers,
                conc,
                reqs_per_client,
                pacing,
                constant_compute,
            );
            off_stats.add(m.offered_load_rps);
            ach_stats.add(m.achieved_rps);
            lat_p50_stats.add(m.lat_p50_ms);
            lat_p95_stats.add(m.lat_p95_ms);
            lat_max_stats.add(m.lat_max_ms);
            stock_p50_stats.add(m.handler_stock_sched_p50_ms);
            stock_p95_stats.add(m.handler_stock_sched_p95_ms);
            dial9_p50_stats.add(m.handler_dial9_delay_p50_ms);
            dial9_p95_stats.add(m.handler_dial9_delay_p95_ms);
            total_attempted += m.attempted_requests;
            total_completed += m.completed_requests;
            total_failed += m.failed_requests;
        }

        let (_, off_p50, _, _) = off_stats.summarize();
        let (_, ach_p50, _, _) = ach_stats.summarize();
        let (_, l50_p50, _, _) = lat_p50_stats.summarize();
        let (_, l95_p50, _, _) = lat_p95_stats.summarize();
        let (_, lmax_p50, _, _) = lat_max_stats.summarize();
        let (_, s50_p50, _, _) = stock_p50_stats.summarize();
        let (_, s95_p50, _, _) = stock_p95_stats.summarize();
        let (_, d50_p50, _, _) = dial9_p50_stats.summarize();
        let (_, d95_p50, _, _) = dial9_p95_stats.summarize();

        println!(
            "{:>4}  {:>12.1}  {:>13.1}  {:>9}  {:>9}  {:>6}  {:>11.2}  {:>11.2}  {:>11.2}  {:>8.3}ms  {:>8.3}ms  {:>8.3}ms  {:>8.3}ms",
            conc,
            off_p50,
            ach_p50,
            total_attempted / iterations,
            total_completed / iterations,
            total_failed / iterations,
            l50_p50,
            l95_p50,
            lmax_p50,
            s50_p50,
            s95_p50,
            d50_p50,
            d95_p50,
        );
    }
    println!(
        "----------------------------------------------------------------------------------------------------------------------------------------------------"
    );
    println!("Closed-Loop Load Testing Methodology & Discrepancy Notes:");
    println!(
        "  - Client Latency measures full round-trip time: client write -> OS socket queuing -> driver turn -> task sched -> compute -> client read."
    );
    println!(
        "  - Stock Tokio & Dial9 latencies strictly measure the identified request handler tasks (excluding unrelated background runtime tasks)."
    );
    println!(
        "  - Socket-level driver delay is not attributed individually in concurrent sweeps because Tokio's internal `ScheduledIo` token is an unexported pointer."
    );
    println!(
        "  - Controlled single-request driver starvation is verified with task-level ground-truth attribution in the isolated trace above."
    );
}

fn run_distribution_benchmarks(runs: usize) {
    println!("\n=======================================================");
    println!(
        "RUNNING STATISTICAL BENCHMARKS ({} iterations per case)",
        runs
    );
    println!("=======================================================");

    // Case D Benchmark
    let mut d_delta_driver = Stats::default();
    let mut d_delta_sched = Stats::default();
    let mut d_delta_poll = Stats::default();
    let mut d_delta_e2e = Stats::default();
    let mut d_valid = 0usize;

    for _ in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();
        let mut ext_write = 0u64;
        let mut io_ready = 0u64;
        let mut t_sched = 0u64;
        let mut t_poll = 0u64;

        let reader_task_id = session.runtime.as_ref().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (client_conn, server_conn) = tokio::join!(TcpStream::connect(addr), async {
                listener.accept().await.unwrap().0
            });
            let mut client = client_conn.unwrap();
            let std_stream = server_conn.into_std().unwrap();

            let read_task = tokio::spawn(async move {
                let mut buf = [0u8; 16];
                client.read(&mut buf).await.unwrap()
            });
            let reader_id = read_task.id().to_string().parse::<u64>().unwrap();

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
            reader_id
        });

        let (gt, _, _) = session.finish();
        for ev in &gt {
            match ev {
                ProbeEvent::ExternalIoStimulus { t_ns, .. } => ext_write = *t_ns,
                ProbeEvent::IoReadinessObserved { t_ns, .. } if io_ready == 0 => io_ready = *t_ns,
                ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                    if *task_id == reader_task_id && t_sched == 0 =>
                {
                    t_sched = *t_ns
                }
                ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                    if *task_id == reader_task_id && t_poll == 0 =>
                {
                    t_poll = *t_ns
                }
                _ => {}
            }
        }
        if ext_write > 0 && io_ready > 0 && t_sched > 0 && t_poll > 0 {
            d_valid += 1;
            d_delta_driver.add(io_ready.saturating_sub(ext_write) as f64 / 1_000_000.0);
            d_delta_sched.add(t_sched.saturating_sub(io_ready) as f64 / 1_000_000.0);
            d_delta_poll.add(t_poll.saturating_sub(t_sched) as f64 / 1_000_000.0);
            d_delta_e2e.add(t_poll.saturating_sub(ext_write) as f64 / 1_000_000.0);
        }
    }

    println!(
        "\n--- [CASE D: I/O While Parked Distribution (N={}, valid={})] ---",
        runs, d_valid
    );
    let (d_min, d_p50, d_p95, d_max) = d_delta_driver.summarize();
    println!(
        "  Δio_driver:  min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        d_min, d_p50, d_p95, d_max
    );
    let (s_min, s_p50, s_p95, s_max) = d_delta_sched.summarize();
    println!(
        "  Δschedule:   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        s_min, s_p50, s_p95, s_max
    );
    let (p_min, p_p50, p_p95, p_max) = d_delta_poll.summarize();
    println!(
        "  Δpoll:       min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        p_min, p_p50, p_p95, p_max
    );
    let (e_min, e_p50, e_p95, e_max) = d_delta_e2e.summarize();
    println!(
        "  Δend_to_end: min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        e_min, e_p50, e_p95, e_max
    );

    // Case E Benchmark
    let mut e_delta_driver = Stats::default();
    let mut e_delta_sched = Stats::default();
    let mut e_delta_poll = Stats::default();
    let mut e_delta_e2e = Stats::default();
    let mut e_valid = 0usize;

    for _ in 0..runs {
        let session = InstrumentedSession::new(2);
        let stock_rec = session.stock_rec.clone();
        let mut ext_write = 0u64;
        let mut io_ready = 0u64;
        let mut t_sched = 0u64;
        let mut t_poll = 0u64;

        let reader_task_id = session.runtime.as_ref().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (client_conn, server_conn) = tokio::join!(TcpStream::connect(addr), async {
                listener.accept().await.unwrap().0
            });
            let mut client = client_conn.unwrap();
            let std_stream = server_conn.into_std().unwrap();

            let read_task = tokio::spawn(async move {
                let mut buf = [0u8; 16];
                client.read(&mut buf).await.unwrap()
            });
            let reader_id = read_task.id().to_string().parse::<u64>().unwrap();

            tokio::time::sleep(Duration::from_millis(20)).await;
            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            let compute_started = Arc::new(AtomicUsize::new(0));
            let mut compute = Vec::new();
            for _ in 0..2 {
                let c_started = compute_started.clone();
                compute.push(tokio::spawn(async move {
                    c_started.fetch_add(1, Ordering::SeqCst);
                    let start = Instant::now();
                    while start.elapsed() < Duration::from_millis(40) {
                        std::hint::spin_loop();
                    }
                }));
            }

            let c_started = compute_started.clone();
            let handle = std::thread::spawn(move || {
                while c_started.load(Ordering::SeqCst) < 2 {
                    std::thread::sleep(Duration::from_millis(1));
                }
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
            reader_id
        });

        let (gt, _, _) = session.finish();
        for ev in &gt {
            match ev {
                ProbeEvent::ExternalIoStimulus { t_ns, .. } => ext_write = *t_ns,
                ProbeEvent::IoReadinessObserved { t_ns, .. } if io_ready == 0 => io_ready = *t_ns,
                ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                    if *task_id == reader_task_id && t_sched == 0 =>
                {
                    t_sched = *t_ns;
                }
                ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                    if *task_id == reader_task_id && t_poll == 0 =>
                {
                    t_poll = *t_ns;
                }
                _ => {}
            }
        }
        if ext_write > 0 && io_ready > 0 && t_sched > 0 && t_poll > 0 {
            e_valid += 1;
            e_delta_driver.add(io_ready.saturating_sub(ext_write) as f64 / 1_000_000.0);
            e_delta_sched.add(t_sched.saturating_sub(io_ready) as f64 / 1_000_000.0);
            e_delta_poll.add(t_poll.saturating_sub(t_sched) as f64 / 1_000_000.0);
            e_delta_e2e.add(t_poll.saturating_sub(ext_write) as f64 / 1_000_000.0);
        }
    }

    println!(
        "\n--- [CASE E: Saturated Workers Distribution (N={}, valid={})] ---",
        runs, e_valid
    );
    let (d_min, d_p50, d_p95, d_max) = e_delta_driver.summarize();
    println!(
        "  Δio_driver (Driver Service Delay): min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        d_min, d_p50, d_p95, d_max
    );
    let (s_min, s_p50, s_p95, s_max) = e_delta_sched.summarize();
    println!(
        "  Δschedule (Ready -> Sched):        min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        s_min, s_p50, s_p95, s_max
    );
    let (p_min, p_p50, p_p95, p_max) = e_delta_poll.summarize();
    println!(
        "  Δpoll (Sched -> Poll):             min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        p_min, p_p50, p_p95, p_max
    );
    let (e_min, e_p50, e_p95, e_max) = e_delta_e2e.summarize();
    println!(
        "  Δend_to_end (Total Real Latency):   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        e_min, e_p50, e_p95, e_max
    );

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
                    })
                    .await;
                }));
            }

            while !compute_started.load(Ordering::SeqCst)
                || tasks_registered.load(Ordering::SeqCst) < 5
            {
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
            if let ProbeEvent::SchedulerWakeDecision {
                task_id: Some(tid),
                target_worker,
                ..
            } = ev
            {
                if scheduled_tasks.contains(tid) {
                    if target_worker.is_some() {
                        task_unparks += 1;
                    } else {
                        task_suppressions += 1;
                    }
                }
            }
        }

        let total_none_decisions = gt
            .iter()
            .filter(|ev| {
                matches!(
                    ev,
                    ProbeEvent::SchedulerWakeDecision {
                        target_worker: None,
                        ..
                    }
                )
            })
            .count();

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

        let t1_sched = first_task_id
            .and_then(|id| task_sched_times.get(&id).copied())
            .unwrap_or(0);

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
                run_idx + 1,
                task_unparks,
                task_suppressions,
                total_none_decisions
            ));
        }
    }

    println!(
        "\n--- [ADVERSARIAL: Wake Coalescing Distribution (N={})] ---",
        runs
    );
    println!("  Attempted runs:                                {}", runs);
    println!(
        "  Runs matching intended scheduler precondition: {}",
        valid_intended_runs
    );
    println!(
        "  Runs with alternate scheduler topology:        {}",
        other_topology_runs
    );
    if other_topology_runs > 0 {
        println!(
            "    Alternate topologies observed: {:?}",
            other_topology_details
        );
    }
    if valid_intended_runs > 0 {
        println!(
            "\n  [Metrics across {} Valid Intended-Precondition Runs]:",
            valid_intended_runs
        );
        let (u_min, u_p50, u_p95, u_max) = coal_unparks.summarize();
        println!(
            "  Task-correlated worker wake selections: min={:.0}  p50={:.0}  p95={:.0}  max={:.0}",
            u_min, u_p50, u_p95, u_max
        );
        let (ts_min, ts_p50, ts_p95, ts_max) = coal_task_suppressed.summarize();
        println!(
            "  Task-correlated wake suppressed: min={:.0}  p50={:.0}  p95={:.0}  max={:.0} (tasks 2-5 coalesced)",
            ts_min, ts_p50, ts_p95, ts_max
        );
        let (sup_min, sup_p50, sup_p95, sup_max) = coal_total_suppressed.summarize();
        println!(
            "  Total target=None decisions:     min={:.0}  p50={:.0}  p95={:.0}  max={:.0}",
            sup_min, sup_p50, sup_p95, sup_max
        );
        let (t1_min, t1_p50, t1_p95, t1_max) = coal_t1_delay.summarize();
        println!(
            "  Task 1 Sched->Poll Latency:      min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
            t1_min, t1_p50, t1_p95, t1_max
        );
        let (co_min, co_p50, co_p95, co_max) = coal_coalesced_delay.summarize();
        println!(
            "  Coalesced Tasks Sched->Poll:     min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
            co_min, co_p50, co_p95, co_max
        );
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
                ProbeEvent::TaskScheduled {
                    t_ns,
                    task_id,
                    is_local,
                } => {
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
                subtask_poll
                    .get(tid)
                    .map(|pt| pt.saturating_sub(*st) as f64 / 1_000_000.0)
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

    println!(
        "\n--- [ADVERSARIAL: Work Stealing Distribution (N={})] ---",
        runs
    );
    let (st_min, st_p50, st_p95, st_max) = steal_stolen_delay.summarize();
    println!(
        "  Stolen Tasks Sched->Poll:    min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        st_min, st_p50, st_p95, st_max
    );
    let (str_min, str_p50, str_p95, str_max) = steal_stranded_delay.summarize();
    println!(
        "  Stranded Task Sched->Poll:   min={:.3}ms  p50={:.3}ms  p95={:.3}ms  max={:.3}ms",
        str_min, str_p50, str_p95, str_max
    );

    // Closed-Loop Concurrency Load Sweep Benchmark (5 repeated iterations per tier)
    run_network_service_load_sweep(2, 5);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let is_benchmark = args.iter().any(|a| a == "--benchmark" || a == "-b");
    let is_service = args.iter().any(|a| a == "--service" || a == "-s");

    println!("===============================================================");
    println!("TOKIO / DIAL9 RUNTIME OBSERVABILITY GAP INVESTIGATION");
    println!("3-Way Empirical Observability Matrix (Ground Truth vs Stock vs Dial9)");
    println!("===============================================================");

    if is_service {
        test_network_service_workload(2);
        return;
    }

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

    println!("\n>>> RUNNING CONTROLLED NETWORK SERVICE WORKLOAD (RustFS Model) <<<");
    test_network_service_workload(2);

    println!("\n===============================================================");
    println!("INVESTIGATION RUN COMPLETED");
    println!("Run with `--benchmark` to compute statistical distributions.");
    println!("Run with `--service` to run the network-service workload.");
    println!("===============================================================");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_service_load_tiers() {
        let compute = Duration::from_millis(5);

        // Tier 1: Concurrency 1 baseline
        let baseline = run_network_service_load_tier(2, 1, 10, Duration::from_millis(10), compute);
        assert_eq!(
            baseline.failed_requests, 0,
            "baseline tier must complete with 0 failures"
        );
        assert_eq!(
            baseline.completed_requests, baseline.attempted_requests,
            "all attempted requests in baseline tier must complete"
        );

        // Tier 3: Concurrency 4 oversubscribed
        let oversubscribed = run_network_service_load_tier(2, 4, 10, Duration::ZERO, compute);
        assert_eq!(
            oversubscribed.failed_requests, 0,
            "oversubscribed tier must complete with 0 failures"
        );
        assert_eq!(
            oversubscribed.completed_requests, oversubscribed.attempted_requests,
            "all attempted requests in oversubscribed tier must complete"
        );

        // Monotonicity check under identical compute cost:
        // Closed-loop request latency increases with concurrency oversubscription
        assert!(
            oversubscribed.lat_p50_ms > baseline.lat_p50_ms,
            "closed-loop request latency p50 must increase under concurrency oversubscription (baseline={:.2}ms, oversubscribed={:.2}ms)",
            baseline.lat_p50_ms,
            oversubscribed.lat_p50_ms
        );
    }

    #[test]
    fn test_controlled_saturation_driver_delay() {
        let workers = 2;
        let session = InstrumentedSession::new(workers);
        let stock_rec = session.stock_rec.clone();

        let probe_handler_task_id = Arc::new(AtomicU64::new(0));
        let p_tid = probe_handler_task_id.clone();
        let shutdown_signal = Arc::new(AtomicBool::new(false));

        let mut ext_write_start = 0u64;
        let mut tokio_io_ready = 0u64;
        let mut task_sched = 0u64;
        let mut task_polled = 0u64;

        session.runtime.as_ref().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let accept_handle = dial9_tokio_telemetry::spawn(async move {
                if let Ok((socket, _)) = listener.accept().await {
                    let mut socket = socket;
                    let _ = socket.set_nodelay(true);
                    let h_task = dial9_tokio_telemetry::spawn(async move {
                        let mut req_buf = [0u8; 8];
                        if socket.read_exact(&mut req_buf).await.is_ok() {
                            let _ = socket.write_all(&req_buf).await;
                        }
                    });
                    if let Ok(tid) = h_task.id().to_string().parse::<u64>() {
                        p_tid.store(tid, Ordering::SeqCst);
                    }
                    let _ = h_task.await;
                }
            });

            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut probe_stream = std::net::TcpStream::connect(addr).unwrap();
            probe_stream.set_nodelay(true).unwrap();

            tokio::time::sleep(Duration::from_millis(30)).await;

            stock_rec.reset();
            ground_truth_probes::reset_base_time();
            ground_truth_probes::enable();

            let bg_started = Arc::new(AtomicUsize::new(0));
            let mut compute_handles = Vec::new();
            for _ in 0..workers {
                let cs = bg_started.clone();
                compute_handles.push(dial9_tokio_telemetry::spawn(async move {
                    cs.fetch_add(1, Ordering::SeqCst);
                    simulate_chunk_processing(Duration::from_millis(40));
                }));
            }

            let cs = bg_started.clone();
            let target_handle = std::thread::spawn(move || {
                while cs.load(Ordering::SeqCst) < workers {
                    std::thread::sleep(Duration::from_millis(1));
                }
                std::thread::sleep(Duration::from_millis(10));
                let req = [1u8; 8];
                use std::io::{Read, Write};
                ground_truth_probes::record_external_stimulus("WRITE_BEGIN", "Test probe write");
                probe_stream.write_all(&req).unwrap();
                let mut resp = [0u8; 8];
                probe_stream.read_exact(&mut resp).unwrap();
            });

            target_handle.join().unwrap();
            for ch in compute_handles {
                ch.await.unwrap();
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
            ground_truth_probes::disable();
            shutdown_signal.store(true, Ordering::SeqCst);
            let _ = accept_handle.await;
        });

        let (gt_events, stock_events, _) = session.finish();
        let target_tid = probe_handler_task_id.load(Ordering::SeqCst);
        assert!(target_tid > 0, "probe handler task id must be recorded");

        for ev in &gt_events {
            match ev {
                ProbeEvent::ExternalIoStimulus { t_ns, phase, .. } if *phase == "WRITE_BEGIN" => {
                    ext_write_start = *t_ns;
                }
                ProbeEvent::TaskScheduled { t_ns, task_id, .. }
                    if *task_id == target_tid && task_sched == 0 && *t_ns >= ext_write_start =>
                {
                    task_sched = *t_ns;
                }
                ProbeEvent::WorkerPollStart { t_ns, task_id, .. }
                    if *task_id == target_tid && task_polled == 0 && *t_ns >= ext_write_start =>
                {
                    task_polled = *t_ns;
                }
                _ => {}
            }
        }

        // Correlate the specific IoReadinessObserved event that woke target_tid
        let mut target_wake_idx = None;
        for (idx, ev) in gt_events.iter().enumerate() {
            if let ProbeEvent::TaskWakeByVal {
                t_ns,
                task_id,
                submitted,
            } = ev
            {
                if *task_id == target_tid && *submitted && *t_ns >= ext_write_start {
                    target_wake_idx = Some(idx);
                    break;
                }
            }
        }

        if let Some(w_idx) = target_wake_idx {
            for ev in gt_events[..w_idx].iter().rev() {
                if let ProbeEvent::IoReadinessObserved { t_ns, .. } = ev {
                    if *t_ns >= ext_write_start {
                        tokio_io_ready = *t_ns;
                        break;
                    }
                }
            }
        }

        assert!(ext_write_start > 0, "write start must be recorded");
        assert!(
            tokio_io_ready > ext_write_start,
            "driver readiness must occur after write"
        );
        let delta_driver_ms = (tokio_io_ready - ext_write_start) as f64 / 1_000_000.0;

        assert!(
            delta_driver_ms > 15.0,
            "driver delay must be > 15ms due to worker compute saturation (observed {:.2}ms)",
            delta_driver_ms
        );

        let stock_sched_ms = stock_events
            .iter()
            .find_map(|ev| {
                if ev.task_id == Some(target_tid) {
                    ev.schedule_latency_ns.map(|ns| ns as f64 / 1_000_000.0)
                } else {
                    None
                }
            })
            .unwrap_or(0.0);

        assert!(
            stock_sched_ms < 1.0,
            "stock schedule latency for newly woken task must be sub-millisecond (observed {:.3}ms)",
            stock_sched_ms
        );
        assert!(
            delta_driver_ms > stock_sched_ms * 10.0,
            "physical driver delay ({:.2}ms) must exceed stock schedule latency ({:.3}ms) by >10x",
            delta_driver_ms,
            stock_sched_ms
        );
    }
}
