# Tokio / Dial9 Observability Gap Investigation Suite

Controlled reproduction and experimental validation harness for investigating Tokio runtime observability gaps, evaluating internal runtime transitions against stock Tokio hooks and actual Dial9 telemetry.

## Documentation

The complete technical report, architectural trace, empirical timelines, distributions, and boundary classifications are documented in:
- [`dial9_tokio_observability_gap.md`](./dial9_tokio_observability_gap.md)

---

## Reproducing the Experiment

### Automated Setup (Recommended)

#### Linux / macOS:
```bash
./setup.sh
```

#### Windows (PowerShell):
```powershell
.\setup.ps1
```

The setup script automatically:
1. Clones Tokio at exact tested SHA `b2636752450484955e7ad334bac678424d51bc4a` into `../tokio-probe`.
2. Applies `patches/tokio-ground-truth.patch` cleanly.
3. Clones Dial9 at exact tested SHA `33b2d780628b42251047909ff2b88fdb97e3c28b` into `../dial9`.

---

### Manual Setup Instructions

If you prefer to perform the setup manually:

1. Clone Tokio and check out the exact commit:
   ```bash
   git clone https://github.com/tokio-rs/tokio.git ../tokio-probe
   cd ../tokio-probe
   git checkout b2636752450484955e7ad334bac678424d51bc4a
   git apply ../tokio-test-app/patches/tokio-ground-truth.patch
   cd ../tokio-test-app
   ```

2. Clone Dial9 at the tested commit:
   ```bash
   git clone https://github.com/dial9-ai/dial9.git ../dial9
   cd ../dial9
   git checkout 33b2d780628b42251047909ff2b88fdb97e3c28b
   cd ../tokio-test-app
   ```

---

## Running the Experiments

Ensure `--cfg tokio_unstable` is active (already configured in `.cargo/config.toml`).

### 1. Run Complete 3-Way Comparative Matrix
Runs all test cases (Case A through E, plus adversarial cases) displaying the 3 observation tiers side by side (Internal Ground Truth vs Stock Tokio vs Actual Dial9):

```bash
cargo run --release
```

### 2. Run 30-Iteration Statistical Benchmark
Runs 30 iterations per decisive scenario (Case D, Case E, Wake Coalescing, Work Stealing) and computes statistical distributions (`min`, `p50`, `p95`, `max`):

```bash
cargo run --release -- --benchmark
```

---

## Causal Boundaries Tested

- **Boundary A: I/O Readiness $\to$ Driver Observation:**
  External stimulus timestamping vs Tokio `Driver::turn()` discovery under worker CPU saturation.
- **Boundary B: Runnable Work $\to$ Wake / Coalesce Decision:**
  Multi-task wake bursts and intentional wake suppression via `Idle::worker_to_notify()`.
- **Boundary C: Worker Notification $\to$ Worker Resume:**
  `Unparker::unpark()` dispatch vs worker thread resumption.
- **Boundary D: Task Placement / Work Stealing $\to$ Poll:**
  Worker-local run queues and LIFO slots vs global injection and work-stealing delays.
