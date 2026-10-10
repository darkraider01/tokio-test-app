#!/usr/bin/env bash
set -euo pipefail

# WORKLOAD REPRODUCIBILITY NOTE:
# The recorded capture in .repro/rustfs-waitpath-v5-joint was produced by running
# concurrency 8 only (--concurrency 8, duration 3s, payload 1 MiB), yielding exactly
# 560 client requests in run-1/tiers.json under the 'c8' tier (plus 5 warmup PUTs and
# background operations in the probe dump). This matches measurement.json ("tiers": ["c8"]).
# If reproducing the multi-tier template (c1 followed by c8), replace --concurrency 8 with:
#   --concurrency 1 8
# The default below matches the actual recorded capture (--concurrency 8).

REPO_ROOT="/home/brandybuck/Code/tokio-test-app"
cd "$REPO_ROOT"

export TRACE_INSTANCE="rustfs-v5-joint"
export TRACE_BUFFER_KB="32768"
OUT_DIR=".repro/rustfs-waitpath-v5-joint"
FORMATS_DIR="/tmp/rustfs-waitpath-formats-v5"
CONCURRENCY="${CONCURRENCY:-8}"
DURATION="${DURATION:-3}"

mkdir -p "$FORMATS_DIR"

INSTANCE_ACQUIRED=0

cleanup() {
  local exit_code=$?

  # Only clean up tracing if the instance was successfully acquired by this script
  if [[ "${INSTANCE_ACQUIRED:-0}" -eq 1 ]]; then
    # 1. Disarm stack trigger only on our verified owned instance
    sudo python3 -c '
from pathlib import Path
state_dir = Path("'"${FTRACE_STATE_DIR:-/run/ftrace-instance-state}"'")
marker = state_dir / ("instance.'"$TRACE_INSTANCE"'")
p = Path("/sys/kernel/tracing/instances/'"$TRACE_INSTANCE"'/events/sched/sched_switch/trigger")
if marker.exists() and p.exists():
    try:
        p.write_text("!stacktrace")
    except Exception:
        pass
' 2>/dev/null || true

    # 2. Turn off tracing on the owned instance (ftrace.sh enforces ownership marker)
    sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh off >/dev/null 2>&1 || true

    # 3. On failure, preserve available raw trace evidence before destroying instance
    if [[ $exit_code -ne 0 && ! -f "$OUT_DIR/trace.raw" ]]; then
      mkdir -p "$OUT_DIR"
      echo "joint_v5_capture.sh: failure exit ($exit_code); preserving failure trace snapshot to $OUT_DIR" >&2
      sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh collect "$OUT_DIR" trace >/dev/null 2>&1 || true
    fi

    # 4. Destroy owned instance using built-in ownership checks (safe after collect clears ring)
    sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh destroy >/dev/null 2>&1 || true
    INSTANCE_ACQUIRED=0
  fi

  # 5. Clean up temporary formats directory
  rm -rf "$FORMATS_DIR"

  # 6. Ensure non-root ownership of output directory
  if [[ -d "$OUT_DIR" ]]; then
    sudo chown -R "$(id -u):$(id -g)" "$OUT_DIR" 2>/dev/null || true
  fi

  exit "$exit_code"
}
trap cleanup EXIT

echo "=== 1. Arming ftrace instance $TRACE_INSTANCE ==="
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" TRACE_BUFFER_KB="$TRACE_BUFFER_KB" experiments/rustfs/ftrace.sh arm
INSTANCE_ACQUIRED=1

echo "=== 2. Configuring stack trigger and dirty pages on $TRACE_INSTANCE ==="
sudo python3 -c '
from pathlib import Path
import sys
instance_root = Path("/sys/kernel/tracing/instances/'"$TRACE_INSTANCE"'")
(instance_root / "tracing_on").write_text("0")
(instance_root / "events/sched/sched_switch/trigger").write_text("stacktrace:100000 if prev_state & 2 && prev_comm ~ \"rustfs*\"")
(instance_root / "events/writeback/balance_dirty_pages/enable").write_text("1")
(instance_root / "trace").write_text("")
formats_dir = Path("'"$FORMATS_DIR"'")
for name, p in [
    ("sched-switch-format", instance_root / "events/sched/sched_switch/format"),
    ("sched-switch-trigger", instance_root / "events/sched/sched_switch/trigger"),
    ("dirty-pages-format", instance_root / "events/writeback/balance_dirty_pages/format"),
]:
    (formats_dir / name).write_text(p.read_text())
(instance_root / "tracing_on").write_text("1")
'

echo "=== 3. Running v5 workload (concurrency: $CONCURRENCY, duration: ${DURATION}s) ==="
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output "$OUT_DIR" \
  --binary .repro/rustfs-probe/target-v5/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe \
  --repetitions 1 \
  --duration "$DURATION" \
  --concurrency $CONCURRENCY \
  --rates

echo "=== 4. Copying format and trigger files to $OUT_DIR ==="
cp "$FORMATS_DIR/sched-switch-format" "$OUT_DIR/"
cp "$FORMATS_DIR/sched-switch-trigger" "$OUT_DIR/"
cp "$FORMATS_DIR/dirty-pages-format" "$OUT_DIR/"
rm -rf "$FORMATS_DIR"

echo "=== 5. Writing measurement.json ==="
python3 -c '
import json
from pathlib import Path
out = Path("'"$OUT_DIR"'")
concurrencies = "'"$CONCURRENCY"'".split()
tiers = [f"c{c}" for c in concurrencies]
try:
    duration_s = float("'"$DURATION"'")
    if duration_s.is_integer():
        duration_s = int(duration_s)
except ValueError:
    duration_s = 3
meta = {
    "budget": {
        "repetitions": 1,
        "seconds": duration_s,
        "tiers": tiers
    },
    "authorization": "User authorized one bounded joint v5 probe + kernel-stack capture",
    "instance": "'"$TRACE_INSTANCE"'",
    "stack_trigger": "stacktrace:100000 if prev_state & 2 && prev_comm ~ \"rustfs*\"",
    "stack_trigger_limit_requested": 100000,
    "balance_dirty_pages_enabled": True,
    "workload_parameters": {
        "concurrency": "'"$CONCURRENCY"'",
        "duration_seconds": duration_s,
        "tiers": tiers
    },
    "workload_note": "Recorded run executed concurrency 8 (560 c8 attempts); multi-tier template uses --concurrency 1 8",
    "limitations": [
        "Switch-out stack tracing perturbs timing",
        "Only captured same-task wait paths are observed; release cause and disk acknowledgement dependencies remain unmeasured"
    ]
}
(out / "measurement.json").write_text(json.dumps(meta, indent=2) + "\n")
'

echo "=== 6. Collecting ftrace data to $OUT_DIR ==="
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh collect "$OUT_DIR" trace

echo "=== 7. Disarming stack trigger and turning off ftrace instance ==="
sudo python3 -c '
from pathlib import Path
state_dir = Path("'"${FTRACE_STATE_DIR:-/run/ftrace-instance-state}"'")
marker = state_dir / ("instance.'"$TRACE_INSTANCE"'")
p = Path("/sys/kernel/tracing/instances/'"$TRACE_INSTANCE"'/events/sched/sched_switch/trigger")
if marker.exists() and p.exists():
    try:
        p.write_text("!stacktrace")
    except Exception:
        pass
' 2>/dev/null || true
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh off

echo "=== 8. Destroying owned ftrace instance $TRACE_INSTANCE ==="
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh destroy
INSTANCE_ACQUIRED=0

echo "=== 9. Fixing permissions of collected files in $OUT_DIR ==="
sudo chown -R "$(id -u):$(id -g)" "$OUT_DIR"

echo "=== Capture completed successfully in $OUT_DIR ==="
