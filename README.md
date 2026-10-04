# Tokio / Dial9 Observability Gap Investigation Suite

Controlled reproduction and experimental validation harness for investigating Tokio runtime observability gaps, specifically focusing on the unpark request vs worker resume causal transition.

## Documentation

The complete technical report, architectural trace, empirical timelines, counterexamples, and upstream API proposals are documented in:
- [`dial9_tokio_observability_gap.md`](./dial9_tokio_observability_gap.md)

## Test Matrix

The suite evaluates seven distinct test scenarios comparing internal ground-truth probe timestamps with stock Tokio/Dial9 observability APIs:
- **Case A:** Task-to-task notification across worker counts (1, 2, 4)
- **Case B:** Timer sleep expiration and driver turn
- **Case C:** TCP stream I/O readiness
- **Case D:** External I/O readiness arriving while workers are parked
- **Case E:** High scheduler load and multi-task queue delays
- **Adversarial Scenario 1:** Scheduler wake coalescing and wake suppression
- **Adversarial Scenario 2:** Work stealing and local queue head-of-line blocking

## Running

Ensure `--cfg tokio_unstable` is enabled (configured in `.cargo/config.toml`):

```bash
cargo run
```
