#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/brandybuck/Code/tokio-test-app"
cd "$REPO_ROOT"

export TRACE_INSTANCE="rustfs-v5-joint"
export TRACE_BUFFER_KB="32768"
OUT_DIR=".repro/rustfs-waitpath-v5-joint"
FORMATS_DIR="/tmp/rustfs-waitpath-formats-v5"

mkdir -p "$FORMATS_DIR"

trap 'sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh off >/dev/null 2>&1 || true' EXIT

echo "=== 1. Arming ftrace instance $TRACE_INSTANCE ==="
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" TRACE_BUFFER_KB="$TRACE_BUFFER_KB" experiments/rustfs/ftrace.sh arm

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

echo "=== 3. Running v5 workload (c8, 1 MiB, 3s, concurrency 8) ==="
PYTHONDONTWRITEBYTECODE=1 python3 experiments/rustfs/run.py \
  --output "$OUT_DIR" \
  --binary .repro/rustfs-probe/target-v5/release/rustfs \
  --rustfs-source .repro/rustfs-probe \
  --fs-probe \
  --repetitions 1 \
  --duration 3 \
  --concurrency 8 \
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
meta = {
    "budget": {
        "repetitions": 1,
        "seconds": 3,
        "tiers": ["c8"]
    },
    "authorization": "User authorized one bounded joint v5 probe + kernel-stack capture",
    "instance": "'"$TRACE_INSTANCE"'",
    "stack_trigger": "stacktrace:100000 if prev_state & 2 && prev_comm ~ \"rustfs*\"",
    "stack_trigger_limit_requested": 100000,
    "balance_dirty_pages_enabled": True,
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
p = Path("/sys/kernel/tracing/instances/'"$TRACE_INSTANCE"'/events/sched/sched_switch/trigger")
p.write_text("!stacktrace")
'
sudo env TRACE_INSTANCE="$TRACE_INSTANCE" experiments/rustfs/ftrace.sh off

echo "=== 8. Fixing permissions of collected files in $OUT_DIR ==="
sudo chown -R "$(id -u):$(id -g)" "$OUT_DIR"

echo "=== Capture completed successfully in $OUT_DIR ==="
