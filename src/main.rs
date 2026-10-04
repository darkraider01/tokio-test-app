use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::ground_truth_probes::{self, ProbeEvent};
use tokio::sync::Notify;

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

    pub fn record(&self, event_type: &'static str, worker_id: Option<usize>, task_id: Option<u64>, latency_ns: Option<u64>, details: String) {
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

pub fn create_instrumented_runtime(worker_threads: usize, stock_rec: Arc<StockRecorder>) -> tokio::runtime::Runtime {
    let rec_park = stock_rec.clone();
    let rec_unpark = stock_rec.clone();
    let rec_spawn = stock_rec.clone();
    let rec_term = stock_rec.clone();
    let rec_before_poll = stock_rec.clone();
    let rec_after_poll = stock_rec.clone();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .track_task_schedule_latency()
        .on_thread_park(move || {
            rec_park.record("on_thread_park", None, None, None, String::new());
        })
        .on_thread_unpark(move || {
            rec_unpark.record("on_thread_unpark", None, None, None, String::new());
        })
        .on_task_spawn(move |meta| {
            rec_spawn.record("on_task_spawn", None, meta.id().to_string().parse::<u64>().ok(), None, format!("spawned_at={:?}", meta.spawned_at()));
        })
        .on_task_terminate(move |meta| {
            rec_term.record("on_task_terminate", None, meta.id().to_string().parse::<u64>().ok(), None, String::new());
        })
        .on_before_task_poll(move |meta| {
            let lat = meta.schedule_latency().map(|d| d.as_nanos() as u64);
            rec_before_poll.record("on_before_task_poll", None, meta.id().to_string().parse::<u64>().ok(), lat, String::new());
        })
        .on_after_task_poll(move |meta| {
            rec_after_poll.record("on_after_task_poll", None, meta.id().to_string().parse::<u64>().ok(), None, String::new());
        })
        .build()
        .expect("build runtime")
}

fn print_timeline(title: &str, ground_truth: &[ProbeEvent], stock: &[StockEvent]) {
    println!("\n=======================================================");
    println!("EXPERIMENT: {}", title);
    println!("=======================================================");

    println!("\n--- GROUND TRUTH (Internal Tokio Probes) ---");
    for ev in ground_truth {
        let (t_ns, desc) = match ev {
            ProbeEvent::IoReadinessObserved { t_ns, token, ready, count } => {
                (*t_ns, format!("IO_READINESS token={} ready={:#x} total_events={}", token, ready, count))
            }
            ProbeEvent::ResourceWakeDispatched { t_ns, ready } => {
                (*t_ns, format!("RESOURCE_WAKE ready={:#x}", ready))
            }
            ProbeEvent::TaskWakeByVal { t_ns, task_id, submitted } => {
                (*t_ns, format!("TASK_WAKE_VAL task_id={} submitted={}", task_id, submitted))
            }
            ProbeEvent::TaskWakeByRef { t_ns, task_id, submitted } => {
                (*t_ns, format!("TASK_WAKE_REF task_id={} submitted={}", task_id, submitted))
            }
            ProbeEvent::TaskScheduled { t_ns, task_id, is_local } => {
                (*t_ns, format!("TASK_SCHEDULED task_id={} is_local={}", task_id, is_local))
            }
            ProbeEvent::SchedulerWakeDecision { t_ns, caller, target_worker, num_searching, num_unparked, total_workers } => {
                (*t_ns, format!("SCHEDULER_WAKE_DECISION caller={} target={:?} searching={} unparked={}/{}", caller, target_worker, num_searching, num_unparked, total_workers))
            }
            ProbeEvent::WorkerUnparkRequested { t_ns, target_worker, prev_state } => {
                (*t_ns, format!("WORKER_UNPARK_REQUESTED target_worker={} prev_state={}", target_worker, prev_state))
            }
            ProbeEvent::WorkerUnparkDispatched { t_ns, target_worker, mechanism } => {
                (*t_ns, format!("WORKER_UNPARK_DISPATCHED target_worker={} mechanism={}", target_worker, mechanism))
            }
            ProbeEvent::WorkerParkWaitBegin { t_ns, worker_id, kind } => {
                (*t_ns, format!("WORKER_PARK_WAIT_BEGIN worker={} kind={}", worker_id, kind))
            }
            ProbeEvent::WorkerParkWaitEnd { t_ns, worker_id, kind, state_after } => {
                (*t_ns, format!("WORKER_PARK_WAIT_END worker={} kind={} state_after={}", worker_id, kind, state_after))
            }
            ProbeEvent::WorkerResumed { t_ns, worker_id } => {
                (*t_ns, format!("WORKER_RESUMED worker={}", worker_id))
            }
            ProbeEvent::WorkerPollStart { t_ns, worker_id, task_id } => {
                (*t_ns, format!("WORKER_POLL_START worker={} task_id={}", worker_id, task_id))
            }
            ProbeEvent::WorkerPollEnd { t_ns, worker_id, task_id } => {
                (*t_ns, format!("WORKER_POLL_END worker={} task_id={}", worker_id, task_id))
            }
        };
        println!("+{:>8.3} ms  {}", (t_ns as f64) / 1_000_000.0, desc);
    }

    println!("\n--- STOCK OBSERVABILITY (Externally Visible APIs) ---");
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
        println!("+{:>8.3} ms  {}{}", (ev.t_ns as f64) / 1_000_000.0, ev.event_type, extra);
    }
}

// -------------------------------------------------------------
// Case A: task -> task wake using Notify
// -------------------------------------------------------------
fn test_case_a(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        // Let workers settle and park
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();

        let target_task = tokio::spawn(async move {
            notify_clone.notified().await;
        });

        // Small wait to ensure target_task is pending and workers park
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Trigger notification
        notify.notify_one();

        let _ = target_task.await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("CASE A: Task->Task Notify (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Case B: timer -> task wake
// -------------------------------------------------------------
fn test_case_b(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let timer_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
        });

        let _ = timer_task.await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("CASE B: Timer Sleep (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Case C: TCP I/O readiness -> task
// -------------------------------------------------------------
fn test_case_c(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        let server_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 16];
            let n = socket.read(&mut buf).await.unwrap();
            n
        });

        // Give server task time to register read interest and park
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Off-thread client connects and writes
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            use std::io::Write;
            let mut stream = std::net::TcpStream::connect(addr).unwrap();
            std::thread::sleep(Duration::from_millis(10));
            stream.write_all(b"hello tokio").unwrap();
        });

        let n = server_task.await.unwrap();
        assert_eq!(n, 11);
        tokio::time::sleep(Duration::from_millis(10)).await;

        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("CASE C: TCP I/O Readiness (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Case D: I/O readiness arrives while workers are parked
// -------------------------------------------------------------
fn test_case_d(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
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

        let read_task = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            client_conn.read(&mut buf).await.unwrap()
        });

        // Wait 100ms so all workers are completely parked
        tokio::time::sleep(Duration::from_millis(100)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Send data from off-runtime thread
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let mut sync_stream = std_stream;
            use std::io::Write;
            sync_stream.write_all(b"ping").unwrap();
        });

        handle.join().unwrap();
        read_task.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("CASE D: I/O Readiness While Workers Parked (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Case E: High scheduler load / busy workers
// -------------------------------------------------------------
fn test_case_e(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Spawn busy compute tasks to saturate all worker threads for 80ms
        for _i in 0..workers {
            tokio::spawn(async move {
                let start = Instant::now();
                // Busy compute loop
                while start.elapsed() < Duration::from_millis(80) {
                    std::hint::spin_loop();
                }
            });
        }

        // Spawn I/O waiter task
        let io_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8];
            socket.read(&mut buf).await.unwrap()
        });

        // Trigger I/O readiness after 20ms while workers are busy
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            use std::io::Write;
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            s.write_all(b"workload").unwrap();
        });

        let _ = io_task.await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("CASE E: High Load with I/O Readiness (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Adversarial Case 1: Multiple tasks scheduled while workers busy (Coalescing)
// -------------------------------------------------------------
fn test_adversarial_coalescing(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // 1 worker busy
        tokio::spawn(async {
            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(60) {
                std::hint::spin_loop();
            }
        });

        let notify = Arc::new(Notify::new());

        // Spawn 5 waiting tasks
        for _ in 0..5 {
            let n = notify.clone();
            tokio::spawn(async move {
                n.notified().await;
            });
        }

        tokio::time::sleep(Duration::from_millis(15)).await;

        // Rapidly wake all 5 tasks from off-runtime thread
        let n = notify.clone();
        std::thread::spawn(move || {
            for _ in 0..5 {
                n.notify_one();
            }
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        ground_truth_probes::disable();
    });

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("ADVERSARIAL: Wake Coalescing (workers={})", workers), &gt_events, &stock_events);
}

// -------------------------------------------------------------
// Adversarial Case 2: Work Stealing
// -------------------------------------------------------------
fn test_adversarial_work_stealing(workers: usize) {
    let stock_rec = Arc::new(StockRecorder::default());
    let rt = create_instrumented_runtime(workers, stock_rec.clone());

    rt.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;

        stock_rec.reset();
        ground_truth_probes::reset_base_time();
        ground_truth_probes::enable();

        // Worker 0 will spawn multiple tasks into its local queue, then do a busy spin
        tokio::spawn(async {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(tokio::spawn(async {
                    // Small work
                    std::hint::spin_loop();
                }));
            }
            // Now worker 0 does a 50ms compute loop so it cannot process those 4 tasks immediately
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

    let gt_events = ground_truth_probes::take_events();
    let stock_events = stock_rec.take_events();
    print_timeline(&format!("ADVERSARIAL: Work Stealing (workers={})", workers), &gt_events, &stock_events);
}

fn main() {
    println!("===============================================================");
    println!("TOKIO / DIAL9 RUNTIME OBSERVABILITY GAP INVESTIGATION");
    println!("Controlled Reproductions & Ground Truth Comparisons");
    println!("===============================================================\n");

    println!("\n>>> RUNNING CASE A: Task->Task Notify <<<");
    test_case_a(1);
    test_case_a(2);
    test_case_a(4);

    println!("\n>>> RUNNING CASE B: Timer Sleep <<<");
    test_case_b(1);
    test_case_b(2);
    test_case_b(4);

    println!("\n>>> RUNNING CASE C: TCP I/O Readiness <<<");
    test_case_c(1);
    test_case_c(2);
    test_case_c(4);

    println!("\n>>> RUNNING CASE D: I/O Readiness While Workers Parked <<<");
    test_case_d(2);
    test_case_d(4);

    println!("\n>>> RUNNING CASE E: High Load with I/O Readiness <<<");
    test_case_e(2);
    test_case_e(4);

    println!("\n>>> RUNNING ADVERSARIAL CASES <<<");
    test_adversarial_coalescing(2);
    test_adversarial_work_stealing(2);

    println!("\n===============================================================");
    println!("INVESTIGATION COMPLETED");
    println!("===============================================================");
}
