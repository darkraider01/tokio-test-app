#!/usr/bin/env bash
# Raw-ftrace control for the fsync/scheduler wrapper diagnostic.
#
# ISOLATION CONTRACT: every command operates ONLY on a dedicated named
# tracefs instance ("$TRACEFS/instances/$TRACE_INSTANCE", default
# rustfs-fstrace).  The default tracer (top-level tracefs) is never
# cleared, configured, or disabled here — if the owned instance cannot be
# used, this script fails loudly instead of falling back to it.
#
# Ownership of the instance is recorded in a marker file
# ($FTRACE_STATE_DIR/instance.<name>, default /run/ftrace-instance-state).
# An instance directory that exists without a marker is not touched or
# removed; a marker whose recorded tracefs root does not match is also
# refused.  Instance cleanup is an explicit `destroy` command.
#
# The workload itself runs as the normal user; only tracefs control
# requires root on this host (every file under /sys/kernel/tracing is
# root:root 640).  Setting TRACEFS to a path outside /sys/kernel selects
# fixture mode (used by the fake-tracefs tests); fixture mode never
# exercises real kernel tracefs behaviour — see README for which
# commands are fixture-tested vs. still host-validated.
#
#   sudo experiments/rustfs/ftrace.sh arm [--force]
#   sudo experiments/rustfs/ftrace.sh status
#   sudo experiments/rustfs/ftrace.sh collect <outdir> <label>
#   sudo experiments/rustfs/ftrace.sh off
#   sudo experiments/rustfs/ftrace.sh destroy [--force]
#
# Clock: trace_clock=mono is CLOCK_MONOTONIC, the same clock the probe
# records (fs_probe.rs now_ns()), so probe and trace timestamps share one
# time base and may be subtracted directly (alignment verified empirically
# by requiring syscall events to fall inside their marker windows).
set -Eeuo pipefail

T_ROOT="${TRACEFS:-/sys/kernel/tracing}"           # tracefs root (default instance)
TRACE_INSTANCE="${TRACE_INSTANCE:-rustfs-fstrace}"  # our dedicated instance name
T="$T_ROOT/instances/$TRACE_INSTANCE"              # the ONLY tracer this script writes
STATE_DIR="${FTRACE_STATE_DIR:-/run/ftrace-instance-state}"
MARKER="$STATE_DIR/instance.$TRACE_INSTANCE"

# NOTE: the kernel's ftrace filter `~` operator behaved glob-like in
# verification on this host: the regex form "rustfs.*" stored cleanly but
# matched zero events, while the glob form "rustfs*" matched all three
# rustfs comms (rustfs, rustfs-worker, rustfs-fsync).  The default below is
# the form that was verified to record events.
COMM_RE="${TRACE_COMM_RE:-rustfs*}"
BUFFER_KB="${TRACE_BUFFER_KB:-16384}"  # per-CPU; 12 CPUs -> 192 MiB total
SYSCALL_EVENTS=(sys_enter_fsync sys_exit_fsync sys_enter_fdatasync sys_exit_fdatasync)
SCHED_EVENTS=(sched_switch sched_waking sched_wakeup)

usage() {
  echo "usage: $0 arm [--force] | collect <outdir> <label> | off | status | destroy [--force]" >&2
  exit 2
}

require_root() {
  if [[ $(id -u) -eq 0 ]]; then
    return 0
  fi
  if [[ "$T_ROOT" != /sys/kernel/* ]]; then
    # Fixture mode: an operator-provided tracefs tree outside /sys/kernel
    # (fake-tracefs tests).  Never used for the real tracer.
    echo "ftrace.sh: fixture mode: TRACEFS=$T_ROOT is outside /sys/kernel/tracing; running as $(id -un) — this tests the script, not kernel tracefs" >&2
    return 0
  fi
  echo "this action needs root (tracefs is root-only on this host)" >&2
  exit 1
}

validate_instance_name() {
  # The instance name becomes a directory and a marker filename: no path
  # separators, no traversal.
  if [[ ! "$TRACE_INSTANCE" =~ ^[A-Za-z0-9_-]{1,64}$ ]]; then
    echo "ftrace.sh: invalid TRACE_INSTANCE '$TRACE_INSTANCE' (allowed: A-Z a-z 0-9 _ -)" >&2
    exit 2
  fi
}

clear_stale_marker() {
  # Our own state file only: drop it when the instance it names is gone.
  # A marker recorded for a different tracefs root is not ours to
  # delete.  Never touches any experiment data.
  if [[ -f "$MARKER" && ! -d "$T" ]] \
      && grep -qxF "tracefs_root=$T_ROOT" "$MARKER" 2>/dev/null; then
    rm -f "$MARKER"
  fi
}

require_owned() {
  # $1: "ok" (absent instance -> nothing to do, exit 0) or
  #     "error" (absent instance -> exit 1).
  #
  # The single ownership check for every command: the instance
  # directory must exist, a marker must exist, and the marker must
  # have been recorded for THIS tracefs root.  A marker naming a
  # different root grants nothing here — collect, off, status,
  # destroy and arm's re-arm path all refuse through this function,
  # so no command can disable, read, or remove an instance it does
  # not own at $T_ROOT.
  if [[ ! -d "$T" ]]; then
    if [[ "${1:-error}" == "ok" ]]; then
      echo "ftrace.sh: instance '$TRACE_INSTANCE' not present at $T; nothing to do"
      exit 0
    fi
    echo "ftrace.sh: instance '$TRACE_INSTANCE' not present at $T" >&2
    exit 1
  fi
  if [[ ! -f "$MARKER" ]]; then
    echo "ftrace.sh: instance '$TRACE_INSTANCE' exists at $T but has no ownership marker ($MARKER);" >&2
    echo "refusing to operate on an instance this script cannot claim ownership of" >&2
    exit 1
  fi
  if ! grep -qxF "tracefs_root=$T_ROOT" "$MARKER" 2>/dev/null; then
    echo "ftrace.sh: ownership marker $MARKER was recorded for a different tracefs root than $T_ROOT;" >&2
    echo "refusing to operate on instance '$TRACE_INSTANCE' at $T" >&2
    exit 1
  fi
}

ensure_instance_owned() {
  if [[ -d "$T" ]]; then
    # Re-arm of an existing instance runs the same validation as every
    # other command (marker exists AND names this tracefs root).
    require_owned error
    return 0
  fi
  # Create OUR instance; the default tracer is never used as a fallback.
  if ! mkdir -p "$T_ROOT/instances" 2>/dev/null; then
    echo "ftrace.sh: cannot create $T_ROOT/instances (no tracefs instance support here?);" >&2
    echo "refusing to fall back to the default tracer" >&2
    exit 1
  fi
  if ! mkdir "$T" 2>/dev/null; then
    echo "ftrace.sh: failed to create instance $T;" >&2
    echo "refusing to fall back to the default tracer" >&2
    exit 1
  fi
  mkdir -p "$STATE_DIR"
  {
    echo "instance=$TRACE_INSTANCE"
    echo "tracefs_root=$T_ROOT"
    echo "created_at=$(date --iso-8601=seconds)"
    echo "created_by=$(id -un)"
    echo "created_by_pid=$$"
  } > "$MARKER"
}

buffer_status() {
  # Prints exactly one of: empty | nonempty | unknown.
  #
  # The destructive guards (arm's ring clear, destroy's removal) act
  # only on a POSITIVE statement that the buffer is empty.  A missing,
  # unreadable, or unfamiliar trace file is "unknown" and blocks them
  # without --force — "could not read" must never mean "no data".
  #  * recognizable header "# entries-in-buffer/entries-written: N/M":
  #    N == 0 -> empty, N > 0 -> nonempty;
  #  * whitespace-only content -> empty (a ring cleared by this script
  #    reads back as nothing on real tracefs, and as a bare newline in
  #    the fixture);
  #  * anything else -> unknown.
  local content hdr n
  if [[ ! -e "$T/trace" ]]; then
    echo unknown
    return 0
  fi
  if ! content=$(head -n 5 "$T/trace" 2>/dev/null); then
    echo unknown
    return 0
  fi
  if ! grep -q '[^[:space:]]' <<<"$content"; then
    echo empty
    return 0
  fi
  hdr=$(grep -m1 'entries-in-buffer' <<<"$content" || true)
  if [[ -z "$hdr" ]]; then
    echo unknown
    return 0
  fi
  n="${hdr#*:}"
  n="${n# }"
  n="${n%%/*}"
  if [[ "$n" =~ ^[0-9]+$ ]]; then
    if (( n > 0 )); then
      echo nonempty
    else
      echo empty
    fi
  else
    echo unknown
  fi
}

print_status() {
  echo "instance=$TRACE_INSTANCE instance_dir=$T"
  echo "owned=$([[ -f "$MARKER" ]] && echo yes || echo no) marker=$MARKER"
  echo "tracing_on=$(cat "$T/tracing_on" 2>/dev/null || echo '?') trace_clock=$(cat "$T/trace_clock" 2>/dev/null || echo '?')"
  echo "buffer_size_kb=$(cat "$T/buffer_size_kb" 2>/dev/null || echo '?') overwrite=$(cat "$T/options/overwrite" 2>/dev/null || echo n/a)"
  for e in sched/sched_switch sched/sched_waking sched/sched_wakeup \
           syscalls/sys_enter_fsync syscalls/sys_exit_fsync \
           syscalls/sys_enter_fdatasync syscalls/sys_exit_fdatasync; do
    en=$(cat "$T/events/$e/enable" 2>/dev/null || echo "?")
    f=$(cat "$T/events/$e/filter" 2>/dev/null | head -1 || true)
    echo "  $e enable=$en filter=${f:-<none>}"
  done
}

arm() {
  require_root
  validate_instance_name
  clear_stale_marker
  ensure_instance_owned
  # Instance-support checks first (pure reads): if the kernel did not
  # populate the instance, refuse loudly BEFORE touching any state —
  # the default tracer is never used as a fallback.
  if [[ ! -e "$T/tracing_on" ]]; then
    echo "ftrace.sh: instance $T has no tracing_on file (the kernel did not populate it?);" >&2
    echo "refusing to fall back to the default tracer; validate instance support on this host (see README)" >&2
    exit 1
  fi
  local f
  for f in trace trace_clock buffer_size_kb events/enable options/overwrite; do
    if [[ ! -e "$T/$f" ]]; then
      echo "ftrace.sh: instance $T lacks required file '$f'; cannot arm here;" >&2
      echo "refusing to fall back to the default tracer; validate instance support on this host (see README)" >&2
      exit 1
    fi
  done
  # A re-arm without a prior collect would discard a capture in progress.
  # Only a positively established empty buffer may be cleared without
  # --force: an unreadable or unfamiliar trace file is "unknown" and
  # blocks the clear just like real data would.
  if [[ "${1:-}" != "--force" ]]; then
    case "$(buffer_status)" in
      nonempty)
        echo "ftrace.sh: refusing to re-arm instance '$TRACE_INSTANCE': its ring buffer still holds uncollected data;" >&2
        echo "run 'collect' first, or pass 'arm --force' to discard the buffer" >&2
        exit 1 ;;
      unknown)
        echo "ftrace.sh: cannot establish that instance '$TRACE_INSTANCE' is empty (unreadable or unfamiliar trace header at $T/trace);" >&2
        echo "refusing to clear data that was never accounted for — inspect it or run 'collect', or pass 'arm --force' to discard it" >&2
        exit 1 ;;
    esac
  fi
  # Fail-closed: with -E the ERR trap fires inside this function, so any
  # failure below leaves the OWNED instance not recording (and never
  # writes to the default tracer at all).
  trap 'echo 0 > "$T/tracing_on" 2>/dev/null || true' ERR
  echo 0 > "$T/tracing_on"
  echo > "$T/trace"          # clear ring buffers (data guard passed or --force)
  echo > "$T/error_log" 2>/dev/null || true  # drop stale filter-parse entries
  echo 0 > "$T/events/enable" # reset every event first
  echo mono > "$T/trace_clock"
  # Stop-on-full (overwrite off): a full buffer stops recording and the loss
  # shows up in per-CPU stats instead of silently discarding the oldest data.
  echo 0 > "$T/options/overwrite"
  echo "$BUFFER_KB" > "$T/buffer_size_kb"
  # Scheduler events: keep only threads whose comm matches the RustFS tree
  # (rustfs / rustfs-worker / rustfs-fsync, verified via /proc/<pid>/task).
  # A switch where either side is a target thread is kept, so target
  # switch-in/switch-out pairs survive even when the other side is foreign.
  # `~` patterns must use the verified glob form (see COMM_RE note above).
  echo "prev_comm ~ \"$COMM_RE\" || next_comm ~ \"$COMM_RE\"" \
    > "$T/events/sched/sched_switch/filter"
  echo "comm ~ \"$COMM_RE\"" > "$T/events/sched/sched_waking/filter"
  echo "comm ~ \"$COMM_RE\"" > "$T/events/sched/sched_wakeup/filter"
  for e in "${SCHED_EVENTS[@]}"; do
    echo 1 > "$T/events/sched/$e/enable"
  done
  # fsync/fdatasync entry/exit: syscall tracepoints carry no comm field, so
  # they are left unfiltered (low volume); analysis restricts to probe TIDs
  # and counts foreign TIDs explicitly.
  for e in "${SYSCALL_EVENTS[@]}"; do
    echo 1 > "$T/events/syscalls/$e/enable"
  done
  echo 1 > "$T/tracing_on"
  trap - ERR
  print_status
}

collect() {
  require_root
  local out="${1:?usage: collect <outdir> <label>}" label="${2:-trace}"
  validate_instance_name
  clear_stale_marker
  require_owned error
  mkdir -p "$out"
  if [[ -e "$out/$label.raw" ]]; then
    echo "ftrace.sh: refusing to overwrite existing experiment data $out/$label.raw;" >&2
    echo "use another label, or remove the file yourself if it is truly obsolete" >&2
    exit 1
  fi
  # Stop recording BEFORE reading the snapshot: the ring must be frozen.
  echo 0 > "$T/tracing_on"
  # Raw trace first: its header documents the timestamp format in effect.
  cat "$T/trace" > "$out/$label.raw"
  {
    echo "collected_at=$(date --iso-8601=seconds)"
    echo "instance=$TRACE_INSTANCE"
    echo "instance_dir=$T"
    echo "instance_owned=yes"
    echo "tracefs=$T_ROOT"
    echo "trace_clock=$(cat "$T/trace_clock" 2>/dev/null || echo '?')"
    echo "tracing_on=$(cat "$T/tracing_on" 2>/dev/null || echo '?')"
    echo "buffer_size_kb=$(cat "$T/buffer_size_kb" 2>/dev/null || echo '?')"
    echo "overwrite=$(cat "$T/options/overwrite" 2>/dev/null || echo n/a)"
    echo "current_tracer=$(cat "$T/current_tracer" 2>/dev/null || echo '?')"
    echo "kernel=$(uname -r)"
    echo "tracefs_tools=none (raw ftrace via shell)"
  } > "$out/$label.settings"
  for e in sched/sched_switch sched/sched_waking sched/sched_wakeup \
           syscalls/sys_enter_fsync syscalls/sys_exit_fsync \
           syscalls/sys_enter_fdatasync syscalls/sys_exit_fdatasync; do
    printf '%s enable=%s filter=%s\n' "$e" \
      "$(cat "$T/events/$e/enable" 2>/dev/null || echo '?')" \
      "$(cat "$T/events/$e/filter" 2>/dev/null | head -1 || true)" \
      >> "$out/$label.settings"
  done
  {
    for cpu in "$T"/per_cpu/cpu*; do
      echo "== $cpu"
      cat "$cpu/stats" 2>/dev/null || true
    done
  } > "$out/$label.stats"
  cat "$T/error_log" > "$out/$label.error_log" 2>/dev/null || : > "$out/$label.error_log"
  chmod -R a+rX "$out"
  # The snapshot is safely on disk: clear the owned instance's ring so the
  # next 'arm' sees an empty buffer (its data guard then only fires on
  # genuinely uncollected data).
  echo > "$T/trace"
}

off() {
  require_root
  validate_instance_name
  clear_stale_marker
  require_owned ok
  echo 0 > "$T/tracing_on"
  echo 0 > "$T/events/enable"
  print_status
}

status() {
  require_root
  validate_instance_name
  clear_stale_marker
  require_owned ok
  print_status
}

destroy() {
  require_root
  validate_instance_name
  clear_stale_marker
  if [[ ! -d "$T" ]]; then
    echo "ftrace.sh: instance '$TRACE_INSTANCE' not present; nothing to remove"
    exit 0
  fi
  # Marker + tracefs-root validation, identical to off/collect/status:
  # a marker recorded for a different root cannot authorize removal.
  require_owned error
  # Guard BEFORE mutating anything: removal needs a positively empty
  # buffer (or explicit --force); unknown status is not "empty".
  if [[ "${1:-}" != "--force" ]]; then
    case "$(buffer_status)" in
      nonempty)
        echo "ftrace.sh: refusing to remove instance '$TRACE_INSTANCE': its ring buffer still holds uncollected data;" >&2
        echo "run 'collect' first, or pass 'destroy --force' to discard it" >&2
        exit 1 ;;
      unknown)
        echo "ftrace.sh: cannot establish that instance '$TRACE_INSTANCE' is empty (unreadable or unfamiliar trace header at $T/trace);" >&2
        echo "refusing to remove data that was never accounted for — inspect it or run 'collect', or pass 'destroy --force' to discard it" >&2
        exit 1 ;;
    esac
  fi
  # Stop the owned instance before removing it.
  echo 0 > "$T/tracing_on" 2>/dev/null || true
  echo 0 > "$T/events/enable" 2>/dev/null || true
  if rmdir "$T" 2>/dev/null; then
    : # tracefs instance directory removed by the kernel
  elif [[ "$T_ROOT" == /sys/kernel/* ]]; then
    echo "ftrace.sh: failed to remove instance directory $T" >&2
    exit 1
  else
    # Fixture-only: a fake tracefs tree cannot emulate the kernel's rmdir
    # teardown (tracefs entries are synthetic and not unlinkable).
    rm -rf "$T"
  fi
  rm -f "$MARKER"
  echo "ftrace.sh: removed instance '$TRACE_INSTANCE'"
}

case "${1:-}" in
  arm) shift; arm "$@" ;;
  collect) shift; collect "$@" ;;
  off) off ;;
  status) status ;;
  destroy) shift; destroy "$@" ;;
  *) usage ;;
esac
