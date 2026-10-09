#!/usr/bin/env python3
"""Correlate a raw ftrace capture with the fs-probe marker dump.

Reads one experiment directory produced by ``run.py --fs-probe`` together
with the files ``ftrace.sh collect`` wrote into it (``trace.raw``,
``trace.settings``, ``trace.stats``, ``trace.error_log``), and decomposes
every paired call-wrapper interval into kernel-facing zones and scheduler
states.

Clocks
------
Whether probe markers and trace events may be subtracted directly is
decided by ``validate_clock`` from inspectable evidence, never assumed:

* compatibility — the *selected* trace clock (the ``[x]`` marker in
  ``trace.settings``) must be a known CLOCK_MONOTONIC spelling (``mono``)
  and every probe dump header must carry ``clock_id=1``
  (CLOCK_MONOTONIC, as written by fs_probe.rs).  Missing or malformed
  metadata yields ``insufficient_evidence``; a different clock yields
  ``failed`` — never a success flag;
* alignment — this capture's matched syscall-entry evidence must exist in
  sufficient samples, at a sufficient match rate, with the earliest enter
  no earlier than the documented timestamp tolerance (one quantization
  step) before its own wrapper start marker.

Each question and the overall ``direct_subtraction`` conclusion carry an
explicit status (``validated`` / ``failed`` / ``insufficient_evidence``)
with a reason.  A matching window or an exact arithmetic tiling alone
proves neither clock compatibility nor alignment; the matching window is
only a search slack.  No clock offset is invented and incompatible
timestamps are never realigned: an unvalidated capture keeps its raw
observations, while conclusions that require cross-clock subtraction
(zone/state decompositions, clusters, union state totals) are withheld.

The raw ftrace timestamp fraction is written with 6 digits (microseconds)
on this host; fractions are right-padded to nanoseconds during parsing,
and that observed precision is what justifies the alignment tolerance.

Trace line semantics (verified against this host's
``events/sched/sched_switch/format`` print fmt, kernel 6.19):
  * the line-prefix ``comm-tid`` is the *previous* task for sched_switch and
    the *waker* for sched_waking/sched_wakeup; per-thread timelines are
    therefore attributed from event content (prev_pid / next_pid / pid),
    never from the line prefix;
  * ``prev_state=R+`` means the task was preempted while TASK_RUNNING —
    runnable but not scheduled (``R+`` = "R" plus the TASK_REPORT_MAX bit);
    ``R`` alone is a runnable switch-out without preemption; any other letter
    (``S``, ``D``, ...) is a voluntary block of that task state;
  * syscall tracepoints print ``sys_fsync(args)`` on entry and
    ``sys_fsync -> ret`` on exit; they carry no comm field, so they are
    unfiltered in the capture and restricted to probe tids here.

State machine per thread (event semantics verified against Linux v6.19
``kernel/sched/core.c``: ``try_to_wake_up`` emits ``trace_sched_waking``
only after the task's state matched, i.e. at the *start* of wakeup
processing while the task is still sleeping; ``ttwu_do_wakeup`` sets
``TASK_RUNNING`` and only then emits ``trace_sched_wakeup``, i.e. when
the task is made runnable — so the two events are different boundaries
and are kept as different event kinds).  Per thread:

  running -> (switch-out) -> blocked:<letter>
  blocked -> (sched_waking) -> wakeup_transition   [still not runnable]
  wakeup_transition -> (sched_wakeup) -> runnable  [made runnable]
  runnable -> (switch-in) -> running

Spans whose cause cannot be observed are reported as ``unknown``,
``unknown_no_wake`` (switch-in without an observed wake),
``unknown_wake_incomplete`` (sched_waking seen but no sched_wakeup
before the next event or the window edge) or ``unknown_lost_in``
(switch-out without an observed switch-in) — never as zero, and never
silently counted as runnable or blocked time.

Usage:
    python3 fs_trace.py RUN_DIR [--output out.json] [--timelines N]
                        [--threshold-ms 50] [--probe-reference DIR]
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

import fs_probe

LINE_RE = re.compile(
    r"^\s*(?P<prefix>.*?)-(?P<tid>\d+)\s+\[(?P<cpu>\d+)\]\s+(?P<flags>\S+)\s+"
    r"(?P<sec>\d+)\.(?P<frac>\d+):\s+(?P<rest>.*)$"
)
SWITCH_RE = re.compile(
    r"sched_switch: prev_comm=(?P<prevc>.+?) prev_pid=(?P<prev>\d+) "
    r"prev_prio=(?P<prio>\d+) prev_state=(?P<state>\S+)\s+"
    r"==> next_comm=(?P<nextc>.+?) next_pid=(?P<next>\d+)"
)
# sched_wak(e|eup|ing): -> waking: / wakeup:  (tested against all three
# spellings this host emits; a bare "e|ing" alternation never matches
# "sched_wakeup:").  The two observed events are NOT interchangeable —
# see `state_segments` for what each one means; a bare "sched_wake:"
# spelling (never observed in these captures) is classified as the
# initiation event, matching the previous parser.
WAKE_RE = re.compile(r"sched_wak(?:ing|eup|e):\s+comm=(?P<comm>.+?) pid=(?P<pid>\d+)")
SYSCALL_RE = re.compile(r"^sys_(?P<name>fsync|fdatasync)(?P<rest>.*)$")

# Tags whose wrapped call is one of the traced syscalls.  Which syscall each
# tag actually performs is derived from the capture itself (see
# derive_syscall_mapping) rather than hard-coded; these are the candidates.
SYNC_TAG_CANDIDATES = ("sub_dir_sync", "sub_fsync_files", "sub_fdatasync")

# Search slack when pairing a wrapper with its syscall events: ftrace
# timestamps are quantized (1 us at 6 fraction digits on this host), so an
# enter may print up to ~2 us outside the nanosecond-precise marker window.
# This slack widens the *search* only — validate_clock applies the stricter
# one-quantization-step tolerance to the observed offsets, because a matching
# window by itself does not prove clock compatibility.
ENTRY_SLACK_NS = 2000  # +/- 2 us search slack

# Clock validation thresholds (see validate_clock).
COMPATIBLE_TRACE_CLOCKS = ("mono",)   # selected trace clock == CLOCK_MONOTONIC
PROBE_CLOCK_MONOTONIC_ID = 1          # libc CLOCK_MONOTONIC, as stored in the
                                      # probe dump header (fs_probe.rs)
ALIGN_MIN_SAMPLES = 30                # minimum matched syscall-entry samples
ALIGN_MIN_MATCH_RATE = 0.95           # per capture and per run

UNKNOWN_STATES = ("unknown", "unknown_no_wake", "unknown_lost_in",
                  "unknown_wake_incomplete")

# Timeline event kinds with a causal default rank for equal-timestamp
# ties (ftrace timestamps are quantized to 1 us here, so distinct events
# of one task can share a timestamp): a switch-out precedes a wakeup
# initiation of the same task at equal precision, sched_waking precedes
# sched_wakeup, and both precede the switch-in that runs the task.
#
# This default is NOT universally observable truth.  One per-CPU buffer
# is read in order, so events of the same CPU at the same timestamp have
# a real order; across CPUs the trace file only shows the ring-buffer
# merge order, which carries no causal meaning — and forcing the rank
# over a recorded same-CPU order can invent state (e.g. reordering a
# real switch-in -> switch-out pair to out -> in leaves the task
# "running" for the whole following interval).  order_ties therefore
# treats each CPU's recorded sequence as a hard constraint (only the
# earliest unplaced event of each CPU may be placed next), keeps the
# recorded order wherever it is observable, and consults this rank only
# for cross-CPU ties between different event kinds, counting those as
# genuinely ambiguous (quality.trace_parse.equal_ts_ambiguous).
EVENT_RANK = {"out": 0, "waking": 1, "wakeup": 2, "in": 3}


def _state_after(state, kind, extra):
    """Scheduler state after one timeline event (mirrors state_segments)."""
    if kind == "in":
        return "running"
    if kind == "out":
        return "runnable" if extra in ("R", "R+") else f"blocked:{extra}"
    if kind == "waking":
        # While blocked/unknown this starts the transition; while
        # running/runnable/transition it is spurious or a duplicate.
        if state.startswith("blocked") or state == "unknown":
            return "wakeup_transition"
        return state
    if kind == "wakeup":
        # Ends a block or transition; while running/runnable: spurious.
        if (state == "wakeup_transition" or state.startswith("blocked")
                or state == "unknown"):
            return "runnable"
        return state
    return state


def _tie_candidates(state, pending):
    """Event kinds that could legally occur next from ``state``.

    ``pending`` is the list of ``(kind, ts, extra, cpu)`` events of this
    equal-timestamp group not yet placed.  Admissibility follows the
    scheduler: a task cannot switch out unless it is running, and it
    cannot switch in before the wake that makes it runnable (while wake
    events of the same tie are still pending).  Duplicate switch-ins of
    an already-running task remain admissible: pid 0 is one idle thread
    per CPU sharing a single pid, and state_segments ignores a
    switch-in while the state is already running.
    """
    kinds = {e[0] for e in pending}
    if state == "unknown":
        # Window start: nothing observed yet, recorded order decides.
        return kinds
    if state == "running":
        cand = {k for k in kinds if k != "in"}
        if "out" not in kinds:
            cand.add("in")
        return cand
    if state.startswith("blocked") or state == "wakeup_transition":
        wakes = kinds & {"waking", "wakeup"}
        if wakes:
            return wakes       # the wake precedes the switch-in it enables
        return kinds & {"in"}
    if state == "runnable":
        return kinds - {"out"}
    return set()


def _order_tie_group(group, state):
    """Reorder one equal-timestamp group of a thread's events.

    ``group`` is the tie's events in recorded line order; ``state`` is
    the simulated scheduler state entering the tie.  Returns
    ``(ordered, flags)`` where flags has:

    * ``ambiguous`` — a choice between *different* event kinds had to be
      made among frontier events recorded on different CPUs.  The
      recorded line order for such a tie is the ring-buffer merge
      order, not an observation, so the causal default (EVENT_RANK)
      picks and the tie is explicitly marked rather than presented as
      observed order;
    * ``unresolved`` — causality and the recorded sequences conflict:
      no event at any CPU's frontier is admissible from the current
      state.  Observable sequences are never reordered to dodge the
      conflict, so the recorded order is kept for the remainder and
      reported as uncertainty instead (the state machine then
      classifies whatever it receives, ``unknown_*`` where applicable).

    Only the *frontier* — the earliest unplaced event of each CPU — may
    be placed at any step.  One per-CPU buffer is read in order, so each
    CPU's recorded sequence is a hard constraint: an admissible event
    later in its own CPU's sequence (or on another CPU) never leaps over
    an earlier recorded event of its CPU.  Causal lookahead still sees
    the whole remaining tie (a wake anywhere in the tie precedes the
    switch-in it enables), and a single admissible kind on the frontier
    is placed wherever it sits — that is a causal repair of the recorded
    order when the merge put it the other way (e.g. a cross-CPU
    switch-in printed before the wake it needs).
    """
    pending = list(group)
    placed = []
    flags = {"ambiguous": False, "unresolved": False}
    while pending:
        # Earliest unplaced event per CPU (file order preserves each
        # buffer's own sequence, so this is each CPU's next event).
        frontier_pos = {}
        for i, e in enumerate(pending):
            frontier_pos.setdefault(e[3], i)
        # Admissibility uses the whole remaining tie for its causal
        # lookahead rules; placement is restricted to the frontier.
        cand = _tie_candidates(state, pending)
        options = [i for i in frontier_pos.values()
                   if pending[i][0] in cand]
        if not options:
            # Every CPU's next event is inadmissible here: the
            # observable sequences conflict with the inferred state.
            # Report uncertainty — never reorder recorded events.
            flags["unresolved"] = True
            placed.extend(pending)
            break
        kinds = {pending[i][0] for i in options}
        if len(kinds) > 1 and len({pending[i][3] for i in options}) > 1:
            # Different kinds, different CPUs: recorded order is not
            # causal-observable here; fall back to the causal default.
            flags["ambiguous"] = True
            best = min(EVENT_RANK[k] for k in kinds)
            options = [i for i in options
                       if EVENT_RANK[pending[i][0]] == best]
        nxt = pending.pop(min(options))  # file-first among eligible
        placed.append(nxt)
        state = _state_after(state, nxt[0], nxt[2])
    return placed, flags


def order_ties(events, stats):
    """Order one thread's timeline events across equal timestamps.

    ``events`` are ``(kind, ts, extra, cpu)`` in recorded line order;
    returns ``[(kind, ts, extra), ...]`` ready for ``state_segments``.
    Timestamps never change — only the order inside an equal-timestamp
    group may, and only where the recorded order is not observable or
    not causally admissible (see _order_tie_group).

    Counters recorded into ``stats`` (surfaced in quality.trace_parse):

    * ``equal_ts_ties`` — groups with >= 2 events at one timestamp;
    * ``equal_ts_causal_repairs`` — recorded order contradicted scheduler
      causality and was reordered;
    * ``equal_ts_ambiguous`` — a cross-CPU frontier choice between
      different event kinds (genuinely ambiguous at 1 us precision;
      causal default used);
    * ``equal_ts_unresolved`` — causality and the recorded per-CPU
      sequences conflict (no frontier event admissible; recorded order
      kept, uncertainty reported rather than an observable sequence
      reordered).

    A group may contribute to more than one counter; groups are counted
    even for pids whose combined timeline is not consumed by state
    accounting (pid 0 = one idle thread per CPU).
    """
    # Stable by timestamp: the recorded line order is the base for ties.
    events.sort(key=lambda e: e[1])
    out = []
    state = "unknown"
    i, n = 0, len(events)
    while i < n:
        j = i + 1
        while j < n and events[j][1] == events[i][1]:
            j += 1
        group = events[i:j]
        if len(group) == 1:
            ordered = group
        else:
            stats["equal_ts_ties"] += 1
            ordered, flags = _order_tie_group(group, state)
            if [e[:3] for e in ordered] != [e[:3] for e in group]:
                stats["equal_ts_causal_repairs"] += 1
            if flags["ambiguous"]:
                stats["equal_ts_ambiguous"] += 1
            if flags["unresolved"]:
                stats["equal_ts_unresolved"] += 1
        for kind, ts, extra, _cpu in ordered:
            out.append((kind, ts, extra))
            state = _state_after(state, kind, extra)
        i = j
    return out


def to_ns(sec, frac):
    """ftrace timestamp -> ns; the fraction is right-padded to 9 digits."""
    return int(sec) * 10**9 + int(frac.ljust(9, "0")[:9])


def parse_trace(path):
    """Parse a raw ftrace dump into per-thread timelines and syscall lists.

    sched_switch lines are attributed from event content: prev_pid receives
    the switch-out (with its prev_state) and next_pid the switch-in, even
    when the other side is a foreign thread (the comm filter keeps such
    lines).  sched_waking/sched_wakeup ``pid=`` is the wakee, and the two
    are preserved as distinct timeline kinds (``waking`` = wakeup
    processing started, ``wakeup`` = task made runnable); a bare
    ``sched_wake:`` spelling would classify as ``waking``.

    Each event carries its recording CPU so that equal-timestamp ties can
    be ordered by :func:`order_ties`: same-CPU order is observable and
    kept, cross-CPU ties are causally repaired or explicitly marked
    ambiguous instead of being silently re-sorted.
    """
    timelines = {}
    syscalls = {}
    stats = {
        "lines": 0, "bad_lines": 0, "bad_samples": [],
        "frac_digits": set(), "first_ts": None, "last_ts": None,
        "event_counts": {}, "foreign_syscall_lines": 0,
        "rustfs_comms": set(), "header_entries": None,
        "equal_ts_ties": 0, "equal_ts_causal_repairs": 0,
        "equal_ts_ambiguous": 0, "equal_ts_unresolved": 0,
    }
    with path.open() as fh:
        for line in fh:
            if line.startswith("#"):
                hm = re.search(r"entries-in-buffer/entries-written:\s*(\d+)/(\d+)",
                               line)
                if hm:
                    stats["header_entries"] = (int(hm.group(1)), int(hm.group(2)))
                continue
            stats["lines"] += 1
            m = LINE_RE.match(line)
            if not m:
                stats["bad_lines"] += 1
                if len(stats["bad_samples"]) < 5:
                    stats["bad_samples"].append(line.rstrip()[:120])
                continue
            ts = to_ns(m.group("sec"), m.group("frac"))
            stats["frac_digits"].add(len(m.group("frac")))
            stats["first_ts"] = ts if stats["first_ts"] is None else min(
                stats["first_ts"], ts)
            stats["last_ts"] = ts if stats["last_ts"] is None else max(
                stats["last_ts"], ts)
            tid = int(m.group("tid"))
            if m.group("prefix").startswith("rustfs"):
                stats["rustfs_comms"].add(m.group("prefix"))
            rest = m["rest"]
            sm = SYSCALL_RE.match(rest)
            if sm:
                phase = "exit" if "->" in rest else "enter"
                key = f"sys_{sm.group('name')}_{phase}"
                stats["event_counts"][key] = stats["event_counts"].get(key, 0) + 1
                if not m.group("prefix").startswith("rustfs"):
                    stats["foreign_syscall_lines"] += 1
                syscalls.setdefault(tid, []).append((ts, sm.group("name"), phase))
                continue
            sw = SWITCH_RE.search(rest)
            if sw:
                stats["event_counts"]["sched_switch"] = \
                    stats["event_counts"].get("sched_switch", 0) + 1
                prev, nxt = int(sw.group("prev")), int(sw.group("next"))
                timelines.setdefault(prev, []).append(
                    ("out", ts, sw.group("state"), m.group("cpu")))
                timelines.setdefault(nxt, []).append(
                    ("in", ts, None, m.group("cpu")))
                continue
            wk = WAKE_RE.search(rest)
            if wk:
                kind = ("sched_wakeup" if rest.startswith("sched_wakeup")
                        else "sched_waking")
                stats["event_counts"][kind] = stats["event_counts"].get(kind, 0) + 1
                wakee = int(wk.group("pid"))
                timelines.setdefault(wakee, []).append(
                    ("wakeup" if kind == "sched_wakeup" else "waking",
                     ts, None, m.group("cpu")))
                continue
    # Equal-timestamp ties (1 us quantization): order per thread by
    # scheduler admissibility over the recorded line order — same-CPU
    # order is observable and never overridden; see order_ties for the
    # repair/ambiguity accounting written into stats.
    for tid in list(timelines):
        timelines[tid] = order_ties(timelines[tid], stats)
    for evs in syscalls.values():
        evs.sort()
    stats["frac_digits"] = sorted(stats["frac_digits"])
    stats["rustfs_comms"] = sorted(stats["rustfs_comms"])
    return timelines, syscalls, stats


def state_segments(timeline, t0, t1):
    """Clip one thread's state timeline to [t0, t1].

    Returns a list of ``(state, seg_start, seg_end)`` rows (ns).  States:

    * ``running`` — scheduled residency after a switch-in (not exact CPU
      execution: interrupts may run while the task is current);
    * ``blocked:<prev_state letter>`` — switched out into that task state;
    * ``runnable`` — made runnable (``sched_wakeup`` observed, or a
      pre-emptive ``R``/``R+`` switch-out) but not yet scheduled;
    * ``wakeup_transition`` — between an observed ``sched_waking`` (start
      of wakeup processing) and its ``sched_wakeup`` (made runnable); the
      task is neither running nor runnable yet, and this is deliberately
      NOT counted as either runnable time or blocked/D time;
    * ``unknown`` (window edge before any event), ``unknown_no_wake``
      (switch-in without an observed wake), ``unknown_wake_incomplete``
      (``sched_waking`` observed but no ``sched_wakeup`` before the next
      event or the window edge — the made-runnable instant is missing),
      and ``unknown_lost_in`` (switch-out without an observed switch-in).

    Unknowns are reported separately and never counted as zero.  Event
    kinds must come from :func:`parse_trace`, which preserves
    ``waking``/``wakeup`` distinctly and has already ordered
    equal-timestamp ties (:func:`order_ties`) — this machine never
    re-orders events itself; duplicate or spurious wake events
    (``waking`` while running/runnable, a second ``waking`` inside a
    transition, ``wakeup`` while already runnable/running) do not move
    the state.  A ``wakeup`` without an observed ``waking`` still ends
    the block: ``sched_wakeup`` is precisely the made-runnable
    boundary, and the unobserved initiation start cannot be located.
    """
    segs = []
    state = "unknown"
    seg_start = None

    def emit(end):
        if state != "unknown" or seg_start is not None:
            s, e = max(seg_start, t0), min(end, t1)
            if e > s:
                segs.append((state, s, e))

    for kind, ts, extra in timeline:
        if ts > t1:
            break
        if kind == "in":
            if state.startswith("blocked"):
                # Switch-in while still recorded as blocked: the wake
                # events were lost or not captured (e.g. a thread's first
                # activation via sched_wakeup_new, which this capture
                # does not record).
                state = "unknown_no_wake"
            elif state == "wakeup_transition":
                # sched_waking seen but no sched_wakeup before the task
                # already runs: the made-runnable instant is missing.
                state = "unknown_wake_incomplete"
            emit(ts)
            state, seg_start = "running", ts
        elif kind == "out":
            if state.startswith("blocked") or state in ("runnable",
                                                        "wakeup_transition"):
                # A switch-out requires the task to have run: for the
                # wakeup-transition state an entire in+wakeup sequence is
                # missing, not just the switch-in.
                state = "unknown_lost_in"
            emit(ts)
            if extra in ("R", "R+"):
                state, seg_start = "runnable", ts
            else:
                state, seg_start = f"blocked:{extra}", ts
        elif kind == "waking":
            # sched_waking = wakeup processing started; the task is still
            # sleeping, so this begins the transition, never runnable time.
            if state.startswith("blocked") or state == "unknown":
                emit(ts)
                state, seg_start = "wakeup_transition", ts
            # while running: spurious/stale initiation for a task that is
            # already scheduled — ignore.  while runnable: already made
            # runnable by an earlier wake — ignore.  while already in a
            # transition: duplicate — ignore.
        elif kind == "wakeup":
            # sched_wakeup = task made runnable (boundary used for the
            # runnable state, whether or not the initiation was observed).
            if (state == "wakeup_transition" or state.startswith("blocked")
                    or state == "unknown"):
                emit(ts)
                state, seg_start = "runnable", ts
            # while running/runnable: duplicate or spurious — ignore.
    if state == "wakeup_transition":
        # Window edge before sched_wakeup arrived: incomplete transition.
        state = "unknown_wake_incomplete"
    emit(t1)
    merged = []
    for st, s, e in segs:
        if merged and merged[-1][0] == st and merged[-1][2] == s:
            merged[-1] = (st, merged[-1][1], e)
        else:
            merged.append((st, s, e))
    return merged


def syscall_windows_tid(events, w0, w1):
    """Paired (enter, exit) windows of the traced syscalls inside [w0, w1].

    An enter without an observed exit yields a window open at the top
    (``exit=None``); such a window is attributed to the end of the wrapper
    and reported as ``open`` in results.
    """
    evs = [e for e in events if w0 - ENTRY_SLACK_NS <= e[0] <= w1 + ENTRY_SLACK_NS]
    windows, open_enter = [], None
    for ts, name, phase in evs:
        if phase == "enter":
            if open_enter is None:
                open_enter = (ts, name)
        elif open_enter is not None:
            windows.append((open_enter[0], ts, open_enter[1], False))
            open_enter = None
    if open_enter is not None:
        windows.append((open_enter[0], None, open_enter[1], True))
    return windows


def zone_of(a, b, windows, w0, w1):
    """Zone name for a piece fully inside or outside the syscall windows."""
    if not windows:
        return "no-traced-syscall"
    if any(s <= a and (e is None or e >= b) for s, e, _, _ in windows):
        return "syscall"
    starts = [s for s, _, _, _ in windows]
    ends = [e if e is not None else w1 for s, e, _, _ in windows]
    if b <= min(starts, default=w1):
        return "pre-entry"
    if a >= max(ends, default=w0):
        return "post-exit"
    return "between"


def decompose_wrapper(timeline, syscalls_for_tid, w0, w1):
    """Full zone x state decomposition of one wrapper.

    Returns ``(rows, windows, unknown_ns, recon_diff_ns)`` where rows are
    ``(zone, state, seg_start, seg_end)``; the rows tile [w0, w1] exactly
    (recon_diff_ns is the arithmetic check, in ns).
    """
    windows = syscall_windows_tid(syscalls_for_tid, w0, w1)
    bounds = {w0, w1}
    for s, e, _, _ in windows:
        bounds.add(max(s, w0))
        bounds.add(min(e if e is not None else w1, w1))
    bounds = sorted(bounds)
    segs = state_segments(timeline, w0, w1)
    rows = []
    for a, b in zip(bounds, bounds[1:]):
        zone = zone_of(a, b, windows, w0, w1)
        # Clip state segments into the piece and fill every hole (window
        # edges before the first event) with an explicit unknown row, so the
        # rows tile [w0, w1] exactly and unknowns are never silently lost.
        cursor = a
        clipped = sorted(((max(s, a), min(e, b), st) for st, s, e in segs
                          if s < b and e > a))
        for s, e, st in clipped:
            if s > cursor:
                rows.append((zone, "unknown", cursor, s))
            rows.append((zone, st, s, e))
            cursor = e
        if cursor < b:
            rows.append((zone, "unknown", cursor, b))
    covered = sum(e - s for _, _, s, e in rows)
    unknown = sum(e - s for _, st, s, e in rows if st in UNKNOWN_STATES)
    return rows, windows, unknown, covered - (w1 - w0)


def aggregate(rows):
    """(zone, state) -> ns across decomposition rows."""
    out = {}
    for zone, st, s, e in rows:
        out[f"{zone}|{st}"] = out.get(f"{zone}|{st}", 0) + (e - s)
    return out


# --- non-overlapping accounting ------------------------------------------------
#
# Marker-delimited wrapper observations may be nested (sub_fdatasync inside
# sub_fsync_files inside sub_scan on one thread) or partially overlapping, so
# summed durations double-count shared thread time.  The functions below give
# the two complementary views: per-tag totals that deliberately overlap, and a
# non-overlapping union that is only ever taken per repetition AND per thread.


def merge_intervals(intervals):
    """Merge overlapping or touching [start, end) intervals (ns).

    Callers must group by repetition and thread first: merging across TIDs
    or across repetitions would fuse independent waits into one interval.
    Degenerate (end <= start) intervals are dropped, not reported as time.
    """
    merged = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def union_regions(wrappers):
    """Non-overlapping union of wrapper windows, keyed (run, tid).

    Intervals are merged only within one key, so simultaneous waits on
    different threads stay separate thread-time and repetitions are never
    merged with each other.
    """
    grouped = {}
    for w in wrappers:
        grouped.setdefault((w["run"], w["tid"]),
                           []).append((w["w0_ns"], w["w1_ns"]))
    return {key: merge_intervals(iv) for key, iv in grouped.items()}


def union_state_totals(timelines, regions):
    """Scheduler-state totals over union regions, from the state timeline.

    The totals are derived by clipping each thread's own state timeline to
    the union regions (never by proportionally subtracting parent/child
    sums).  Regions are keyed (run, tid); state_segments only needs the
    thread's timeline, which is identical across runs for a given tid — the
    run in the key keeps repetitions separate regardless.  Holes before the
    first observed event are reported as ``unknown``, never as zero.
    """
    totals = {}
    for (run, tid), iv in regions.items():
        for start, end in iv:
            cursor = start
            for state, a, b in state_segments(timelines.get(tid, ()),
                                              start, end):
                if a > cursor:
                    totals["unknown"] = totals.get("unknown", 0) + (a - cursor)
                totals[state] = totals.get(state, 0) + (b - a)
                cursor = b
            if cursor < end:
                totals["unknown"] = totals.get("unknown", 0) + (end - cursor)
    return totals


def union_summary(timelines, regions):
    """Merge interval groups (keyed (run, tid)) and total their states."""
    merged = {key: merge_intervals(iv) for key, iv in regions.items()}
    thread_ns = sum(end - start for iv in merged.values()
                    for start, end in iv)
    return {
        "tids": len(merged),
        "regions": sum(len(iv) for iv in merged.values()),
        "thread_time_ms": round(thread_ns / 1e6, 6),
        "states_ms": {k: round(v / 1e6, 6) for k, v in
                      sorted(union_state_totals(timelines, merged).items())},
    }


def per_tag_totals(wrappers):
    """Per-tag observation counts and duration/state sums (overlapping).

    These totals overlap across tags and across nested wrappers: a
    sub_fdatasync observation inside a sub_fsync_files observation inside a
    sub_scan observation contributes to all three tags, so summed durations
    double-count shared thread time and are not independent syscalls or
    waits.  ``overlapping_across_tags`` marks that explicitly; use
    ``union_regions``/``union_summary`` for thread-time totals.
    """
    out = {}
    for w in wrappers:
        entry = out.setdefault(w["tag"], {
            "observations": 0, "observations_by_run": {},
            "duration_sum_ms": 0.0, "zones_states_ms": {},
            "overlapping_across_tags": True})
        entry["observations"] += 1
        entry["observations_by_run"][w["run"]] = \
            entry["observations_by_run"].get(w["run"], 0) + 1
        entry["duration_sum_ms"] += w["dur_ms"]
        zones = w.get("zones_states_ms")
        if zones:
            for key, value in zones.items():
                entry["zones_states_ms"][key] = \
                    entry["zones_states_ms"].get(key, 0.0) + value
    for entry in out.values():
        entry["duration_sum_ms"] = round(entry["duration_sum_ms"], 6)
        entry["zones_states_ms"] = {
            k: round(v, 6) for k, v in sorted(entry["zones_states_ms"].items())}
    return dict(sorted(out.items()))


def _interval_ref(w):
    return {"run": w["run"], "tag": w["tag"], "tid": w["tid"],
            "w0_ns": w["w0_ns"], "w1_ns": w["w1_ns"]}


def annotate_overlap(wrappers):
    """Record same-run, same-TID nesting/overlap relations per wrapper.

    Relations are checked only within one thread of one repetition:
    wrappers on different threads (or in different reps) are independent
    observations even when their timestamps coincide.
    """
    groups = {}
    for w in wrappers:
        groups.setdefault(w["tid"], []).append(w)
    for group in groups.values():
        for w in group:
            contained_in, contains, partial = [], [], []
            a0, a1 = w["w0_ns"], w["w1_ns"]
            for other in group:
                if other is w:
                    continue
                b0, b1 = other["w0_ns"], other["w1_ns"]
                if b0 <= a0 and a1 <= b1:
                    contained_in.append(_interval_ref(other))
                elif a0 <= b0 and b1 <= a1:
                    contains.append(_interval_ref(other))
                elif max(a0, b0) < min(a1, b1):
                    partial.append(_interval_ref(other))
            w["overlap"] = {
                "same_tid_contained_in": contained_in,
                "same_tid_contains": contains,
                "same_tid_partial_overlaps": partial,
            }
    return wrappers


def overlap_counts(wrappers):
    """Nesting/overlap pair counts keyed ``<run>:<outer>><inner>``."""
    groups = {}
    for w in wrappers:
        groups.setdefault((w["run"], w["tid"]), []).append(w)
    nested, partial = {}, {}
    for (run, _tid), group in groups.items():
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                a0, a1, b0, b1 = a["w0_ns"], a["w1_ns"], b["w0_ns"], b["w1_ns"]
                if b0 >= a0 and b1 <= a1:
                    key = f"{run}:{a['tag']}>{b['tag']}"
                    nested[key] = nested.get(key, 0) + 1
                elif a0 >= b0 and a1 <= b1:
                    key = f"{run}:{b['tag']}>{a['tag']}"
                    nested[key] = nested.get(key, 0) + 1
                elif max(a0, b0) < min(a1, b1):
                    key = f"{run}:{a['tag']}~{b['tag']}"
                    partial[key] = partial.get(key, 0) + 1
    return {"nested": dict(sorted(nested.items())),
            "partial_overlaps": dict(sorted(partial.items()))}


# --- operation / job identity --------------------------------------------------


def operation_context(records):
    """Commit-wait pairs and their first send, keyed by operation hash.

    Wait pairs are built chronologically exactly like fs_probe.analyze_run,
    and each pair is decomposed with fs_probe.reconstruct_wait.  Dial9 polls
    are not part of this analysis, so the reconstruction's resume-poll
    fields stay missing (they are dropped from the emitted send rows rather
    than reported as absent evidence about the send itself).
    """
    waits_by_op, sends_by_op = {}, {}
    for record in records:
        kind = record["kind"]
        if kind in (fs_probe.KIND_WAIT_BEGIN, fs_probe.KIND_WAIT_END):
            waits_by_op.setdefault(record["a"], []).append(record)
        elif kind in (fs_probe.KIND_SEND_OK, fs_probe.KIND_SEND_ERR):
            sends_by_op.setdefault(record["a"], []).append(record)
    context = {}
    for op, events in waits_by_op.items():
        if op == fs_probe.OP_NONE:
            continue
        pairs, pending = [], None
        for record in sorted(events, key=lambda r: r["ts"]):
            if record["kind"] == fs_probe.KIND_WAIT_BEGIN:
                pending = record
            elif pending is not None:
                pairs.append((pending, record))
                pending = None
        sends = sorted(sends_by_op.get(op, ()), key=lambda r: r["ts"])
        context[op] = [
            {"begin_ns": begin["ts"], "end_ns": end["ts"],
             "send": fs_probe.reconstruct_wait(sends, None, begin, end, op)}
            for begin, end in pairs]
    return context


def operation_link(op_context, op_hash, w0, w1):
    """Link one wrapper to its operation's commit wait and send records.

    Three distinct relations are kept apart: a job *associated* with an
    operation (via its submit record), a wrapper *overlapping* the commit
    wait in time, and a dependency *required before the response* — the
    last is never established here and is reported as such.  Missing
    identities stay missing (association "missing", overlap None).
    """
    if op_hash is None:
        return {"association": "missing", "op_hash": None,
                "commit_wait": None, "overlaps_commit_wait": None,
                "send": None,
                "required_before_response": "not_established"}
    pairs = op_context.get(op_hash) or []
    chosen = next((p for p in pairs
                   if p["begin_ns"] < w1 and p["end_ns"] > w0), None)
    wait = send = None
    if chosen is not None:
        wait = {"begin_ns": chosen["begin_ns"], "end_ns": chosen["end_ns"],
                "begin_offset_ms": (chosen["begin_ns"] - w0) / 1e6,
                "end_offset_ms": (chosen["end_ns"] - w0) / 1e6}
        reconstruction = chosen["send"]
        if reconstruction.get("send_kind") is not None:
            send = {
                "kind": reconstruction["send_kind"],
                "ts": reconstruction["send_ts"],
                "offset_ms": (reconstruction["send_ts"] - w0) / 1e6,
                "results_seen": reconstruction["results_seen"],
                "write_quorum": reconstruction["write_quorum"],
                "disk_count": reconstruction["disk_count"],
                "missing": [m for m in reconstruction["missing"]
                            if m != "resume_poll"],
            }
    return {
        "association": "job_submit_record",
        "op_hash": op_hash,
        "commit_wait": wait,
        "overlaps_commit_wait": (chosen is not None) if pairs else None,
        "send": send,
        "required_before_response": "not_established",
    }


# --- clock validation ----------------------------------------------------------


def parse_selected_clock(trace_clock_line):
    """The single ``[selected]`` marker of a trace_clock settings line.

    Returns None when the line is absent, empty, or carries anything other
    than exactly one marker (missing *and* malformed metadata are both
    "not evidence", never a default).
    """
    if not trace_clock_line:
        return None
    marks = re.findall(r"\[([a-z0-9_-]+)\]", trace_clock_line)
    return marks[0] if len(marks) == 1 else None


def timestamp_resolution_us(frac_digits):
    """Coarsest timestamp step (us) implied by the observed fraction digits."""
    if not frac_digits:
        return None
    return max(10 ** (6 - digits) for digits in frac_digits)


def validate_clock(trace_clock_line, probe_clock_ids, frac_digits, alignment):
    """Evidence-based validation of direct timestamp subtraction.

    Returns a dict whose ``compatibility`` (same clock domain?) and
    ``alignment`` (does this capture's matched-entry evidence hold within
    the documented tolerance?) each carry ``status`` (``validated`` /
    ``failed`` / ``insufficient_evidence``) and a ``reason``, plus the
    combined ``direct_subtraction`` status.  ``alignment`` also carries the
    tolerance, its justification, and the thresholds it was judged against.

    ``alignment`` evidence: ``paired_sync_calls`` (expected samples),
    ``matched_with_syscall_enter`` (matched samples), ``enter_offset_us``
    (observed offsets with min/p50/max) and optionally ``per_run`` (same
    fields keyed by repetition).  A matching window or an exact tiling
    alone is *not* treated as proof: compatibility comes from the clock
    metadata, alignment from the observed offsets against a tolerance of
    one timestamp quantization step.  No offset is invented and no
    timestamp is realigned.
    """
    selected = parse_selected_clock(trace_clock_line)
    ids = sorted(probe_clock_ids) if probe_clock_ids else None
    digits = sorted(frac_digits)
    resolution = timestamp_resolution_us(digits)

    # 1) Compatibility: do the metadata describe the same clock domain?
    if not trace_clock_line:
        compat_status = "insufficient_evidence"
        compat_reason = ("trace clock metadata missing "
                         "(trace.settings has no trace_clock line)")
    elif selected is None:
        compat_status = "insufficient_evidence"
        compat_reason = ("trace clock selection malformed in trace.settings "
                         "(expected exactly one [x] marker)")
    elif ids is None:
        compat_status = "insufficient_evidence"
        compat_reason = ("probe clock id unavailable "
                         "(no probe dump header was read)")
    elif selected not in COMPATIBLE_TRACE_CLOCKS:
        compat_status = "failed"
        compat_reason = (
            f"selected trace clock '{selected}' is outside the probe's "
            f"CLOCK_MONOTONIC domain (accepted: "
            f"{', '.join(COMPATIBLE_TRACE_CLOCKS)}); no offset is applied "
            "and cross-clock subtraction is not validated")
    elif ids != [PROBE_CLOCK_MONOTONIC_ID]:
        compat_status = "failed"
        compat_reason = (f"probe dump clock_id={ids} is not CLOCK_MONOTONIC "
                         f"({PROBE_CLOCK_MONOTONIC_ID}); the clock domains "
                         "differ")
    else:
        compat_status = "validated"
        compat_reason = (
            f"selected trace clock '{selected}' and probe dump clock_id=1 "
            "are both CLOCK_MONOTONIC (fs_probe.rs now_ns() -> "
            "clock_gettime): same clock domain")

    # 2) Alignment: does this capture provide empirical evidence?
    total = alignment.get("paired_sync_calls") or 0
    matched = alignment.get("matched_with_syscall_enter") or 0
    offsets = alignment.get("enter_offset_us") or {}
    if resolution is None:
        tolerance_basis = ("timestamp precision unknown (no parsed trace "
                           "timestamps); the tolerance cannot be justified")
    else:
        tolerance_basis = (
            f"one timestamp quantization step ({resolution:g} us at "
            f"{','.join(str(d) for d in digits)}-digit fractions): an enter "
            "may follow its own start marker by arbitrary user-space time, "
            "so only the negative direction is bounded; the "
            f"+-{ENTRY_SLACK_NS / 1000:g} us matching window is a search "
            "slack, not proof of clock compatibility")
    per_run = alignment.get("per_run") or {}
    if total == 0:
        align_status = "insufficient_evidence"
        align_reason = ("no paired sync wrappers were available to match "
                        "against (0 alignment samples)")
    elif resolution is None:
        align_status = "insufficient_evidence"
        align_reason = ("timestamp precision unknown (no parsed trace "
                        "timestamps); the tolerance cannot be justified")
    elif matched < ALIGN_MIN_SAMPLES:
        align_status = "insufficient_evidence"
        align_reason = (f"only {matched} matched syscall-entry samples "
                        f"(minimum {ALIGN_MIN_SAMPLES})")
    else:
        rates = [matched / total]
        rates += [e["match_rate"] for e in per_run.values()
                  if e.get("match_rate") is not None]
        worst_rate = min(rates)
        min_offset = offsets.get("min")
        failures = []
        if worst_rate < ALIGN_MIN_MATCH_RATE:
            failures.append(
                f"match rate {worst_rate:.4f} is below the required "
                f"{ALIGN_MIN_MATCH_RATE}")
        if min_offset is None:
            failures.append("matched samples carry no entry offsets")
        elif min_offset < -resolution:
            failures.append(
                f"earliest enter precedes its wrapper start by "
                f"{-min_offset:.3f} us, beyond the {resolution:g} us "
                "timestamp tolerance")
        if failures:
            align_status = "failed"
            align_reason = "; ".join(failures)
        else:
            align_status = "validated"
            align_reason = (
                f"{matched}/{total} paired sync wrappers contain their "
                f"expected syscall-enter; earliest enter "
                f"{min_offset:+.3f} us is within the {resolution:g} us "
                f"timestamp tolerance; worst per-run match rate "
                f"{worst_rate:.4f}")

    # 3) Combined conclusion.
    statuses = [compat_status, align_status]
    if "failed" in statuses:
        direct_status = "failed"
    elif "insufficient_evidence" in statuses:
        direct_status = "insufficient_evidence"
    else:
        direct_status = "validated"
    direct_reason = (f"compatibility {compat_status}: {compat_reason} | "
                     f"alignment {align_status}: {align_reason}")

    return {
        "trace_clock": trace_clock_line,
        "trace_clock_selected": selected,
        "probe_clock_ids": ids,
        "probe_clock": ("CLOCK_MONOTONIC"
                        if ids == [PROBE_CLOCK_MONOTONIC_ID] else None),
        "timestamp_fraction_digits": list(digits),
        "timestamp_resolution_us": resolution,
        "compatibility": {"status": compat_status, "reason": compat_reason},
        "alignment": {"status": align_status, "reason": align_reason,
                      "tolerance_us": resolution,
                      "tolerance_basis": tolerance_basis,
                      "min_samples_required": ALIGN_MIN_SAMPLES,
                      "min_match_rate_required": ALIGN_MIN_MATCH_RATE},
        "direct_subtraction": {"status": direct_status,
                               "reason": direct_reason},
    }


def derive_syscall_mapping(wrapper_rows):
    """Derive tag -> syscall from observed windows (empirical, per run).

    For each sync-candidate tag, count which syscall names actually appear
    inside its paired windows; the winner is the mapping.  Windows with no
    syscall at all are counted but do not vote.
    """
    votes = {}
    for tag, _tid, _w0, _w1, windows in wrapper_rows:
        if tag not in SYNC_TAG_CANDIDATES:
            continue
        names = {n for _s, _e, n, _open in windows}
        entry = votes.setdefault(tag, {"observed": {}, "empty_windows": 0})
        if not names:
            entry["empty_windows"] += 1
        for n in names:
            entry["observed"][n] = entry["observed"].get(n, 0) + 1
    mapping = {}
    for tag, entry in votes.items():
        if entry["observed"]:
            mapping[tag] = max(entry["observed"], key=entry["observed"].get)
    return mapping, votes


def group_clusters(wrappers, enter_gap_ns=50_000_000, exit_group_gap_ns=5_000_000):
    """Group long wrappers that overlap in time into delay clusters."""
    ordered = sorted(wrappers, key=lambda w: w["enter_ns"] or w["w0_ns"])
    clusters, cur = [], None
    for w in ordered:
        if cur is None or (w["enter_ns"] or w["w0_ns"]) > cur["max_exit"] + enter_gap_ns:
            if cur:
                clusters.append(cur)
            cur = {"members": [w], "max_exit": (w["exit_ns"] or w["w1_ns"])}
        else:
            cur["members"].append(w)
            cur["max_exit"] = max(cur["max_exit"], (w["exit_ns"] or w["w1_ns"]))
    if cur:
        clusters.append(cur)
    out = []
    for c in clusters:
        members = c["members"]
        if len(members) < 2:
            continue
        enters = sorted(w["enter_ns"] for w in members if w["enter_ns"])
        exits = sorted(w["exit_ns"] for w in members if w["exit_ns"])
        groups, g = [], []
        for x in exits:
            if g and x - g[-1] > exit_group_gap_ns:
                groups.append(g)
                g = []
            g.append(x)
        if g:
            groups.append(g)
        out.append({
            "n_members": len(members),
            "enter_spread_ms": (max(enters) - min(enters)) / 1e6 if len(enters) > 1 else 0.0,
            "exit_spread_ms": (max(exits) - min(exits)) / 1e6 if len(exits) > 1 else 0.0,
            "exit_groups": [
                {"n": len(g), "spread_ms": (max(g) - min(g)) / 1e6}
                for g in groups
            ],
            "members": [
                {"tag": w["tag"], "tid": w["tid"], "dur_ms": w["dur_ms"],
                 "enter_offset_ms": ((w["enter_ns"] - min(enters)) / 1e6
                                     if w["enter_ns"] else None),
                 "exit_offset_ms": ((w["exit_ns"] - min(exits)) / 1e6
                                    if w["exit_ns"] else None)}
                for w in sorted(members, key=lambda w: w["w0_ns"])
            ],
        })
    return out


def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def input_entry(role, path, *, digest=None, required=True):
    """One consumed input: inspectable path plus its SHA-256.

    ``digest`` reuses a hash computed while reading the file so no input
    is read twice for provenance.  Required inputs that are absent fail
    clearly; optional inputs (manifests) are recorded as missing
    (``present: false``, ``sha256: null``) instead of being invented.
    The hash is always computed by actually reading this file — never
    copied from a declaration elsewhere.
    """
    entry = {"role": role, "path": str(path)}
    if path.is_file():
        entry.update({
            "present": True,
            "bytes": path.stat().st_size,
            "sha256": digest if digest is not None else sha256_file(path),
        })
    else:
        if required:
            raise SystemExit(f"{path} not found (required input: {role})")
        entry.update({"present": False, "bytes": None, "sha256": None})
    return entry


def binary_provenance(manifest, cache):
    """Declared vs independently verified identity of the capture binary.

    ``declared_sha256`` comes from the capture manifest (written when the
    experiment ran); ``file_sha256`` exists only when this analysis
    actually read and hashed the file at the declared path — a missing
    file stays ``null`` with an explicit note, never a re-used claim.
    ``cache`` maps path -> digest so the same binary declared by both
    captures is hashed once.
    """
    declared_path = manifest.get("binary")
    declared_sha = manifest.get("binary_sha256")
    out = {
        "declared_path": declared_path,
        "declared_sha256": declared_sha,
        "declared_sha256_source": "capture manifest, written at capture time",
        "file_sha256": None,
        "file_bytes": None,
        "matches_declared": None,
        "verification": None,
    }
    if not declared_path:
        out["verification"] = ("no binary path declared in this manifest; "
                               "declared-only, nothing was hashed")
        return out
    path = Path(declared_path)
    if not path.is_file():
        out["verification"] = ("declared-only: no file at the declared path "
                               "during this analysis, so it was not hashed")
        return out
    key = str(path)
    if key not in cache:
        cache[key] = sha256_file(path)
    digest = cache[key]
    out.update({
        "file_sha256": digest,
        "file_bytes": path.stat().st_size,
        "matches_declared": (digest == declared_sha
                             if declared_sha is not None else None),
        "verification": ("independently hashed during this analysis by "
                         "reading the declared path; this verifies the file "
                         "on disk now, not that trace.raw was produced by "
                         "this exact binary"),
    })
    return out


def parse_stats_file(path):
    """Parse per-cpu ftrace stats into totals plus per-cpu rows."""
    cpus, cur = [], None
    for line in path.read_text().splitlines():
        if line.startswith("== "):
            cur = {}
            cpus.append(cur)
        elif ":" in line and cur is not None:
            k, v = line.split(":", 1)
            cur[k.strip()] = v.strip()
    def total(key):
        s = 0
        for c in cpus:
            try:
                s += int(c.get(key, "0"))
            except ValueError:
                pass
        return s
    return {
        "cpus": len(cpus),
        "entries_total": total("entries"),
        "overrun_total": total("overrun"),
        "dropped_total": total("dropped events"),
        "bytes_total": total("bytes"),
    }


def parse_settings_file(path):
    out = {}
    events = []
    for line in path.read_text().splitlines():
        if "=" in line and not line.startswith(("sched", "syscalls")):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
        elif line.strip():
            events.append(line.strip())
    out["events"] = events
    return out


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[idx]


def analyze(run_dir, threshold_ms=50.0, timelines=3, probe_reference=None):
    run_dir = Path(run_dir)
    trace_path = run_dir / "trace.raw"
    if not trace_path.exists():
        raise SystemExit(f"{trace_path} not found (was the trace collected?)")

    timeline, syscalls, tstats = parse_trace(trace_path)
    trace_sha = sha256_file(trace_path)  # hashed once; reused below
    settings = parse_settings_file(run_dir / "trace.settings")
    stats = parse_stats_file(run_dir / "trace.stats")
    error_log = (run_dir / "trace.error_log").read_text()

    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}

    # Every input consumed by this analysis, hashed as it is consumed, in a
    # deterministic order (see provenance.inputs at the bottom).
    inputs = [
        input_entry("ftrace raw capture", trace_path, digest=trace_sha),
        input_entry("trace settings", run_dir / "trace.settings"),
        input_entry("trace stats", run_dir / "trace.stats"),
        input_entry("trace error log", run_dir / "trace.error_log"),
        input_entry("capture manifest", manifest_path, required=False),
    ]

    runs = sorted(p.parent.name for p in run_dir.glob("run-*/fs-probe.bin"))
    known_steps = {fs_probe.step_hash(tag): tag for tag in fs_probe.STEP_TAGS}
    per_run, align_offsets = [], []
    align_total = align_matched = 0
    per_run_align = {}
    probe_clock_ids = set()
    op_contexts = {}
    unknown_total = 0
    recon_max = 0.0
    wrapper_rows_raw = []  # (tag, tid, w0, w1, windows) for mapping votes
    probe_tids = set()

    for run_name in runs:
        dump_path = run_dir / run_name / "fs-probe.bin"
        inputs.append(input_entry("probe dump", dump_path))
        header, records = fs_probe.read_probe(dump_path)
        jobs, counters = fs_probe.group_jobs(records)
        probe_tids.update(r["tid"] for r in records)
        probe_clock_ids.add(header.get("clock_id"))
        op_contexts[run_name] = operation_context(records)
        rows = []
        for task_id, job in jobs.items():
            calls = fs_probe.job_calls(job)
            if not calls or "job_start" not in job:
                continue
            start, tid = job["job_start"]["ts"], job["job_start"]["tid"]
            submit = job.get("submit")
            # Identity follows fs_probe's data model: the job key is the
            # blocking-job id (task id); the operation comes from the
            # submit record's `a` field.  OP_NONE means no operation
            # context and stays None (missing), never inferred from the
            # tid — one executor thread serves many jobs/operations.
            op_hash = (submit["a"] if submit is not None
                       and submit["a"] != fs_probe.OP_NONE else None)
            step = (submit["step"] if submit is not None
                    else (job["job_start"].get("step") or 0))
            step_tag = known_steps.get(step,
                                       "unknown" if step else "untagged")
            totals = {}
            for c in calls:
                totals[c["tag"]] = totals.get(c["tag"], 0) + 1
            seen = {}
            for c in calls:
                occurrence = seen.get(c["tag"], 0)
                seen[c["tag"]] = occurrence + 1
                w0 = start + int(c["from_start_ms"] * 1e6)
                w1 = w0 + int(c["dur_ms"] * 1e6)
                windows = syscall_windows_tid(syscalls.get(tid, ()), w0, w1)
                rows.append({
                    "tag": c["tag"], "tid": tid, "w0": w0, "w1": w1,
                    "dur_ms": c["dur_ms"], "windows": windows,
                    "identity": {
                        "run": run_name, "job_task_id": task_id,
                        "op_hash": op_hash, "step_tag": step_tag,
                        "tag": c["tag"], "occurrence": occurrence,
                        "occurrences_for_tag": totals[c["tag"]],
                        "executor_tid": tid,
                        "submit_tid": (submit["tid"]
                                       if submit is not None else None),
                    },
                })
                wrapper_rows_raw.append((c["tag"], tid, w0, w1, windows))
        per_run.append({
            "run": run_name,
            "probe_records": header["records"],
            "probe_dropped": header["dropped_records"],
            # None for format-version-1 dumps: the counter did not exist
            # then, and is preserved as missing rather than as zero.
            "probe_rejected_closed": header["rejected_closed"],
            "probe_counters": counters,
            "paired_calls": len(rows),
            "rows": rows,
        })

    mapping, votes = derive_syscall_mapping(wrapper_rows_raw)

    # Alignment pass: every paired call of a mapped sync tag, regardless of
    # duration, must contain its own syscall-enter event inside the window.
    # This is the empirical evidence for validate_clock, computed per run as
    # well as overall so one bad repetition cannot hide behind the total.
    for run in per_run:
        run_total = run_matched = 0
        run_offsets = []
        for r in run["rows"]:
            expected = mapping.get(r["tag"])
            if expected is None:
                continue
            align_total += 1
            run_total += 1
            enters = [s for s, e, n, o in r["windows"] if n == expected]
            if enters:
                align_matched += 1
                run_matched += 1
                offset = (enters[0] - r["w0"]) / 1000.0
                align_offsets.append(offset)
                run_offsets.append(offset)
        per_run_align[run["run"]] = {
            "paired_sync_calls": run_total,
            "matched_with_syscall_enter": run_matched,
            "match_rate": run_matched / run_total if run_total else None,
            "enter_offset_us": {"min": min(run_offsets) if run_offsets else None,
                                "max": max(run_offsets) if run_offsets else None},
        }

    alignment_evidence = {
        "paired_sync_calls": align_total,
        "matched_with_syscall_enter": align_matched,
        "match_rate": (align_matched / align_total if align_total else None),
        "enter_offset_us": {
            "min": min(align_offsets) if align_offsets else None,
            "p50": percentile(align_offsets, 0.5),
            "max": max(align_offsets) if align_offsets else None,
        },
        "per_run": per_run_align,
    }
    validation = validate_clock(
        settings.get("trace_clock"),
        sorted(probe_clock_ids) if probe_clock_ids else None,
        tstats["frac_digits"], alignment_evidence)
    cross_clock_ok = validation["direct_subtraction"]["status"] == "validated"

    sys_union = {}  # (run, tid) -> [(s, e)] traced syscall pieces of long wrappers
    for run in per_run:
        rows = run.pop("rows")
        wrappers = []
        for r in rows:
            if r["dur_ms"] < threshold_ms:
                continue
            w0, w1, tid = r["w0"], r["w1"], r["tid"]
            link = operation_link(op_contexts[run["run"]],
                                  r["identity"]["op_hash"], w0, w1)
            if not cross_clock_ok:
                # Raw probe-side observations are retained; every field that
                # needs trace<->probe subtraction is withheld, not guessed.
                wrappers.append({
                    "run": run["run"], "tag": r["tag"], "tid": tid,
                    "dur_ms": r["dur_ms"], "w0_ns": w0, "w1_ns": w1,
                    "enter_ns": None, "exit_ns": None,
                    "syscall_windows": None, "zones_states_ms": None,
                    "unknown_ms": None, "reconciliation_diff_ms": None,
                    "quality": ["cross_clock_analysis_withheld_not_validated"],
                    "identity": r["identity"], "operation_link": link,
                })
                continue
            segments, windows, unknown_ns, recon = decompose_wrapper(
                timeline.get(tid, []), syscalls.get(tid, ()), w0, w1)
            recon_max = max(recon_max, abs(recon))
            unknown_total += unknown_ns
            for s, e, _n, _o in windows:
                sys_union.setdefault((run["run"], tid), []).append(
                    (max(s, w0), min(e if e is not None else w1, w1)))
            expected = mapping.get(r["tag"])
            enters_all = [s for s, _e, _n, _o in windows]
            exits_all = [e for _s, e, _n, _o in windows if e is not None]
            quality = []
            if r["tag"] in mapping and not windows:
                quality.append("missing_expected_syscall")
            if any(o for _s, _e, _n, o in windows):
                quality.append("open_syscall_window")
            if expected is not None and any(
                    n != expected for _s, _e, n, _o in windows):
                quality.append("unexpected_syscall_in_window")
            if unknown_ns > 0:
                quality.append("unknown_state_present")
            wrappers.append({
                "run": run["run"], "tag": r["tag"], "tid": tid,
                "dur_ms": r["dur_ms"],
                "w0_ns": w0, "w1_ns": w1,
                "enter_ns": enters_all[0] if enters_all else None,
                "exit_ns": exits_all[-1] if exits_all else None,
                "syscall_windows": [
                    {"enter_offset_ms": (s - w0) / 1e6,
                     "dur_ms": ((e - s) / 1e6) if e else None,
                     "name": n, "open": o}
                    for s, e, n, o in windows],
                "zones_states_ms": {k: v / 1e6 for k, v in
                                    sorted(aggregate(segments).items())},
                "unknown_ms": unknown_ns / 1e6,
                "reconciliation_diff_ms": recon / 1e6,
                "quality": quality,
                "identity": r["identity"], "operation_link": link,
            })
        wrappers.sort(key=lambda w: -w["dur_ms"])
        annotate_overlap(wrappers)
        run["long_wrappers"] = wrappers
        run["long_wrapper_count"] = len(wrappers)
        run["clusters"] = group_clusters(wrappers) if cross_clock_ok else None

    trace_tids = set(timeline) | set(syscalls)
    coverage = []
    for run in per_run:
        run_tids = set()
        header, records = fs_probe.read_probe(
            run_dir / run["run"] / "fs-probe.bin")
        run_tids.update(r["tid"] for r in records)
        covered = (tstats["first_ts"] is not None
                   and min(r["ts"] for r in records) >= tstats["first_ts"]
                   and max(r["ts"] for r in records) <= tstats["last_ts"])
        coverage.append({
            "run": run["run"],
            "probe_tids": len(run_tids),
            "probe_tids_in_trace": len(run_tids & trace_tids),
            "trace_covers_probe_markers": covered,
        })

    # Non-overlapping accounting: union per repetition and TID only, with
    # scheduler states derived by clipping each thread's own timeline to the
    # union regions.  Per-tag sums stay separate and marked overlapping.
    all_wrappers = [w for run in per_run for w in run["long_wrappers"]]
    observations = {run["run"]: run["long_wrapper_count"]
                    for run in per_run}
    accounting = {
        "long_wrapper_threshold_ms": threshold_ms,
        "definitions": {
            "wrapper_observation":
                "one marker-delimited call-wrapper interval (start marker to "
                "end marker); observations may be nested inside another "
                "wrapper on the same thread, so the count is a count of "
                "observations, not of independent syscalls or waits",
            "per_tag_totals":
                "per-tag sums across observations; explicitly overlapping "
                "across tags and across nested wrappers (shared thread time "
                "is double-counted) — for composition inspection only",
            "non_overlapping_union":
                "union of wrapper regions merged only within one repetition "
                "and one executor TID; never merged across TIDs or across "
                "repetitions; the summed thread-time is per-thread wall time "
                "inside wrapper regions — neither client latency nor elapsed "
                "experiment time",
            "syscall_region_union":
                "union of the traced syscall windows belonging to long "
                "wrappers, per repetition and TID (a narrower coverage than "
                "the wrapper-region union)",
        },
        "wrapper_observations": {
            **observations,
            "total": sum(observations.values()),
            "note": "marker-delimited wrapper observations, not independent "
                    "syscalls or waits",
        },
        "per_tag": per_tag_totals(all_wrappers),
        "nesting_and_overlap": overlap_counts(all_wrappers),
        "cross_clock_analysis": ("reported" if cross_clock_ok else "withheld"),
    }
    if cross_clock_ok:
        wrap_regions = union_regions(all_wrappers)
        by_run = {}
        for run in per_run:
            name = run["run"]
            by_run[name] = {
                "wrapper_regions": union_summary(
                    timeline, {k: v for k, v in wrap_regions.items()
                               if k[0] == name}),
                "syscall_regions": union_summary(
                    timeline, {k: v for k, v in sys_union.items()
                               if k[0] == name}),
            }
        accounting["non_overlapping"] = {
            "by_run": by_run,
            "wrapper_thread_time_sum_ms": round(sum(
                by_run[n]["wrapper_regions"]["thread_time_ms"]
                for n in by_run), 6),
            "syscall_thread_time_sum_ms": round(sum(
                by_run[n]["syscall_regions"]["thread_time_ms"]
                for n in by_run), 6),
            "sum_note": "arithmetic sum of the per-repetition unions; "
                        "repetitions are reported separately and never "
                        "merged into one interval",
        }

    # Probe-reference inputs (manifest + comparison dumps) are consumed for
    # the perturbation comparison; hash them here so provenance.inputs lists
    # every input of this run in one deterministic order.
    ref_manifest = {}
    if probe_reference:
        ref_dir = Path(probe_reference)
        ref_manifest_path = ref_dir / "manifest.json"
        inputs.append(input_entry("probe-reference manifest",
                                  ref_manifest_path, required=False))
        if ref_manifest_path.is_file():
            ref_manifest = json.loads(ref_manifest_path.read_text())
        for p in sorted(ref_dir.glob("run-*/fs-probe.bin")):
            inputs.append(input_entry("probe-reference dump", p))
    binary_cache = {}

    result = {
        "schema": "fs-trace-diagnostic/v3",
        "generated_by": "experiments/rustfs/fs_trace.py",
        "provenance": {
            "run_dir": str(run_dir),
            "inputs": inputs,
            "parameters": {
                "long_wrapper_threshold_ms": threshold_ms,
                "timelines_per_run": timelines,
                "timeline_selection": (
                    "the N longest wrappers per repetition, ordered by "
                    "duration (0 disables text timelines)"),
                "probe_reference": (str(probe_reference)
                                    if probe_reference else None),
                "selection_counts": {
                    "long_wrappers_by_run": dict(observations),
                    "long_wrappers_total": sum(observations.values()),
                    "paired_calls_by_run": {
                        r["run"]: r["paired_calls"] for r in per_run},
                    "alignment_samples": len(align_offsets),
                },
                "clock_validation_thresholds": {
                    "compatible_trace_clocks": list(COMPATIBLE_TRACE_CLOCKS),
                    "min_alignment_samples": ALIGN_MIN_SAMPLES,
                    "min_match_rate": ALIGN_MIN_MATCH_RATE,
                    "note": "alignment tolerance is derived from the observed "
                            "timestamp precision and reported under "
                            "quality.alignment",
                },
            },
            # Declared capture metadata (as written at capture time), kept
            # separate from independently computed hashes below.
            "manifest": {k: manifest.get(k) for k in (
                "binary_sha256", "rustfs_commit", "repetitions",
                "generation_seconds_per_tier", "payload_bytes",
                "workers", "fs_probe_enabled") if k in manifest},
            "binary": binary_provenance(manifest, binary_cache),
            "probe_reference_inputs": (
                None if not probe_reference else {
                    "run_dir": str(Path(probe_reference)),
                    "manifest_present": bool(ref_manifest),
                    "declared": {k: ref_manifest.get(k) for k in (
                        "binary_sha256", "rustfs_commit", "repetitions",
                        "fs_probe_enabled") if k in ref_manifest},
                    "binary": binary_provenance(ref_manifest, binary_cache),
                }),
            "trace": {
                "path": str(trace_path),
                "sha256": trace_sha,
                "bytes": trace_path.stat().st_size,
                "settings": settings,
                "stats": stats,
                "error_log_bytes": len(error_log),
                "error_log_excerpt": error_log.splitlines()[:10],
            },
        },
        "quality": {
            "clock": {
                "trace_clock": validation["trace_clock"],
                "trace_clock_selected": validation["trace_clock_selected"],
                "probe_clock_ids": validation["probe_clock_ids"],
                "probe_clock": validation["probe_clock"],
                "probe_clock_source": (
                    "probe dump header clock_id written by fs_probe.rs "
                    "(now_ns -> clock_gettime(CLOCK_MONOTONIC), header "
                    "stores libc::CLOCK_MONOTONIC as u32)"
                    if validation["probe_clock"] else None),
                "timestamp_fraction_digits":
                    validation["timestamp_fraction_digits"],
                "timestamp_resolution_us":
                    validation["timestamp_resolution_us"],
                "compatibility": validation["compatibility"],
                "direct_subtraction": validation["direct_subtraction"],
            },
            "trace_parse": {
                "lines": tstats["lines"],
                "bad_lines": tstats["bad_lines"],
                "bad_line_samples": tstats["bad_samples"],
                "event_counts": tstats["event_counts"],
                "foreign_syscall_lines": tstats["foreign_syscall_lines"],
                "rustfs_comms": tstats["rustfs_comms"],
                "header_entries": tstats["header_entries"],
                "equal_ts_ties": tstats["equal_ts_ties"],
                "equal_ts_causal_repairs": tstats["equal_ts_causal_repairs"],
                "equal_ts_ambiguous": tstats["equal_ts_ambiguous"],
                "equal_ts_unresolved": tstats["equal_ts_unresolved"],
            },
            "loss": {
                "entries_written_equal": (
                    tstats["header_entries"] is not None
                    and tstats["header_entries"][0] == tstats["header_entries"][1]),
                "overrun_total": stats["overrun_total"],
                "dropped_total": stats["dropped_total"],
                "error_log_empty": not error_log.strip(),
                "probe_dropped_records": {
                    r["run"]: r["probe_dropped"] for r in per_run},
                "probe_rejected_after_close": {
                    r["run"]: r["probe_rejected_closed"] for r in per_run},
            },
            "coverage": coverage,
            "alignment": {
                **validation["alignment"],
                **alignment_evidence,
            },
            "unknown_state_ns_total": unknown_total if cross_clock_ok else None,
            "reconciliation_max_abs_diff_ms": (recon_max / 1e6
                                               if cross_clock_ok else None),
        },
        "cross_clock_analysis": {
            "status": "reported" if cross_clock_ok else "withheld",
            "withheld": [] if cross_clock_ok else [
                "zone x state decompositions, syscall enter/exit times, "
                "clusters, and union state totals require validated direct "
                "timestamp subtraction; raw probe marker durations and raw "
                "trace observations are retained",
            ],
        },
        "accounting": accounting,
        "syscall_mapping": {
            "derived_from_capture": mapping,
            "votes": votes,
        },
        "runs": per_run,
        "limitations": [
            "sched events are comm-filtered to rustfs* threads; foreign "
            "threads appear only as the other side of a rustfs switch",
            "syscall events are unfiltered in the capture; analysis restricts "
            "them to probe tids and counts foreign lines",
            "unknown_* state spans are reported as unknown, never as zero",
            "runnable time starts at sched_wakeup (task made runnable); the "
            "sched_waking -> sched_wakeup interval is the separate "
            "wakeup_transition state, counted as neither runnable nor blocked "
            "time, and a sched_waking with no observed sched_wakeup before "
            "the next event or window edge is unknown_wake_incomplete",
            "sched_wakeup_new (a thread's first activation) is not captured, "
            "so such an initial switch-in appears as unknown_no_wake",
            "equal-timestamp ties (1 us trace precision) within one thread "
            "are ordered by scheduler admissibility over the recorded line "
            "order: each CPU's recorded sequence is a hard constraint "
            "(only the earliest unplaced event of a CPU may be placed "
            "next, so observable order is never reversed); causally "
            "impossible recorded orders are repaired (counted in "
            "quality.trace_parse.equal_ts_causal_repairs); cross-CPU ties "
            "between different event kinds are genuinely ambiguous — they "
            "use the causal default out -> waking -> wakeup -> in and are "
            "counted as equal_ts_ambiguous rather than presented as "
            "observed order; equal_ts_unresolved counts ties where "
            "causality conflicts with the recorded per-CPU sequences "
            "(recorded order kept and uncertainty reported rather than "
            "reordering observable events)",
            "association of cluster release with a filesystem event is not "
            "established by these events alone (no block/journal events "
            "captured)",
            "wrapper observations may be nested or overlapping on one "
            "thread; per-tag and all-wrapper duration sums double-count "
            "shared time and are neither independent syscalls nor elapsed "
            "time — use accounting.non_overlapping for thread-time",
            "thread-time totals are per-thread wall time inside wrapper "
            "regions: neither client latency nor elapsed experiment time, "
            "and repetitions are never merged into one interval",
            "scheduler 'running' is scheduled residency: interrupt activity "
            "may occur while the task remains current, so it is not exact "
            "task CPU execution",
            "exact tiling (reconciliation) is an accounting consistency "
            "check, not independent proof that every state classification "
            "is correct",
            "zero recorded trace loss is a capture-quality finding, not "
            "proof of causal attribution",
            "kernel D-state inside fsync/fdatasync locates blocked time "
            "within the syscall; it does not identify journal, writeback, "
            "lock, or device causes",
            "op_hash is an FNV-1a hash of bucket+key and does not resolve "
            "to a client object name here; a job associated with an "
            "operation or overlapping its commit wait is not thereby "
            "response-critical, and a quorum snapshot (results_seen / "
            "write_quorum / disk_count) does not identify which disk or "
            "job supplied a required acknowledgement",
        ],
    }

    # Evidence-preservation note: format-version-1 dumps were produced by a
    # probe generation before the writer-admission flush barrier and the
    # raw-pointer ring-write fix existed.
    if any(r["probe_rejected_closed"] is None for r in per_run):
        result["limitations"].append(
            "these probe dumps are format version 1: produced before the "
            "writer-admission flush barrier and the raw-pointer ring-write "
            "fix existed; no corruption was observed in the checks "
            "performed (record counts, counter consistency), but clean "
            "record counts do not prove memory safety — the kernel traces "
            "still support syscall localization subject to this "
            "scheduler-state accounting, while the underlying "
            "filesystem/kernel wait cause remains unresolved")

    if probe_reference:
        ref = probe_summary(Path(probe_reference))
        traced = probe_summary(run_dir)
        result["probe_reference"] = ref
        result["probe_reference_comparison"] = compare_summaries(traced, ref)

    if timelines:
        if not cross_clock_ok:
            # Rendering mixes probe markers with trace events on one axis.
            result["timelines"] = []
        else:
            out_dir = run_dir / "timelines"
            out_dir.mkdir(exist_ok=True)
            # Derived output of earlier analyzer runs: remove stale text
            # timelines so renamed files cannot accumulate alongside.
            for stale in out_dir.glob("*.txt"):
                stale.unlink()
            written = []
            for run in per_run:
                for w in run["long_wrappers"][:timelines]:
                    ident = w["identity"]
                    # Unique per job and occurrence: two long wrappers with
                    # the same tag on the same TID must never overwrite.
                    p = out_dir / (
                        f"{ident['run']}-{w['tag']}-tid{w['tid']}"
                        f"-job{ident['job_task_id']}"
                        f"-occ{ident['occurrence']}-w{w['w0_ns']}.txt")
                    p.write_text(render_timeline(w,
                                                 timeline.get(w["tid"], [])))
                    written.append(str(p.relative_to(run_dir)))
            result["timelines"] = written
    return result


def probe_summary(run_dir):
    """Distribution summary of paired call wrappers in a probe-only run."""
    summary = {"run_dir": str(run_dir), "tags": {}}
    dirs = sorted(run_dir.glob("run-*/fs-probe.bin"))
    for dump in dirs:
        header, records = fs_probe.read_probe(dump)
        jobs, _ = fs_probe.group_jobs(records)
        for job in jobs.values():
            calls = fs_probe.job_calls(job)
            if not calls:
                continue
            for c in calls:
                entry = summary["tags"].setdefault(
                    c["tag"], {"durations_ms": [], "ge_50ms": 0})
                entry["durations_ms"].append(c["dur_ms"])
                if c["dur_ms"] >= 50:
                    entry["ge_50ms"] += 1
    for tag, entry in summary["tags"].items():
        ds = entry.pop("durations_ms")
        entry.update({
            "n": len(ds),
            "p50_ms": percentile(ds, 0.5),
            "p95_ms": percentile(ds, 0.95),
            "max_ms": max(ds),
        })
    return summary


def compare_summaries(traced, reference):
    """Traced vs probe-only paired-call durations (perturbation signal).

    Short runs on a shared machine: differences are reported, not judged;
    equal medians would not prove zero perturbation.
    """
    out = {}
    for tag in sorted(set(traced["tags"]) | set(reference["tags"])):
        t = traced["tags"].get(tag, {})
        r = reference["tags"].get(tag, {})
        out[tag] = {
            "traced": t,
            "probe_only": r,
            "p50_delta_ms": (round(t["p50_ms"] - r["p50_ms"], 6)
                             if t.get("p50_ms") is not None
                             and r.get("p50_ms") is not None else None),
            "max_delta_ms": (round(t["max_ms"] - r["max_ms"], 6)
                             if t.get("max_ms") is not None
                             and r.get("max_ms") is not None else None),
        }
    return out


def render_timeline(wrapper, timeline):
    """Readable text timeline for one wrapper (all segments, in order)."""
    w0, w1 = wrapper["w0_ns"], wrapper["w1_ns"]
    ident = wrapper.get("identity") or {}
    link = wrapper.get("operation_link") or {}
    lines = [
        f"# wrapper {wrapper['tag']} tid={wrapper['tid']} "
        f"dur={wrapper['dur_ms']:.3f} ms",
        f"# window [{w0}, {w1}] ns   (offsets below are ms from w0)",
        f"# identity: run={ident.get('run')} "
        f"job_task_id={ident.get('job_task_id')} "
        f"op_hash={ident.get('op_hash')} step_tag={ident.get('step_tag')} "
        f"tag={ident.get('tag')} "
        f"occurrence={(ident.get('occurrence') or 0) + 1}/"
        f"{ident.get('occurrences_for_tag')} "
        f"executor_tid={ident.get('executor_tid')} "
        f"submit_tid={ident.get('submit_tid')}",
        *_operation_lines(link, w0),
        *_overlap_lines(wrapper.get("overlap")),
        f"# quality: {wrapper['quality'] or ['ok']}",
    ]
    for k, v in wrapper["zones_states_ms"].items():
        lines.append(f"#   {k}: {v:.3f} ms")
    lines.append(f"# unknown: {wrapper['unknown_ms']:.3f} ms")
    rows = decompose_wrapper(timeline,
                             _syscalls_stub(wrapper), w0, w1)[0]
    lines.append(f"{'start':>10} {'end':>10} {'dur_ms':>9}  zone/state")
    for zone, st, s, e in rows:
        lines.append(f"{(s - w0) / 1e6:10.3f} {(e - w0) / 1e6:10.3f} "
                     f"{(e - s) / 1e6:9.3f}  {zone}/{st}")
    return "\n".join(lines) + "\n"


def _operation_lines(link, w0):
    """Identity/commit-wait/send header lines for one wrapper timeline."""
    if not link:
        return ["# operation: missing (no job submit record)"]
    if link.get("association") == "missing":
        return ["# operation: association=missing op_hash=None "
                "(no operation context for this job; identity preserved "
                "as missing, never inferred from the tid)"]
    lines = [
        f"# operation: association={link['association']} "
        f"op_hash={link['op_hash']} "
        f"required_before_response={link['required_before_response']} "
        "(an associated job overlapping its commit wait is not thereby "
        "response-critical; op_hash does not resolve to an object name here)",
    ]
    wait = link.get("commit_wait")
    if wait:
        lines.append(
            f"# commit-wait: begin {wait['begin_offset_ms']:+.3f} ms "
            f"end {wait['end_offset_ms']:+.3f} ms (from wrapper start); "
            f"overlaps_commit_wait={link['overlaps_commit_wait']}")
    else:
        lines.append("# commit-wait: missing (no wait records for this "
                     f"operation); overlaps_commit_wait="
                     f"{link['overlaps_commit_wait']}")
    send = link.get("send")
    if send:
        line = (f"# send: {send['kind']} at {send['offset_ms']:+.3f} ms "
                f"quorum_snapshot: results_seen={send['results_seen']} "
                f"write_quorum={send['write_quorum']} "
                f"disk_count={send['disk_count']}")
        if send["missing"]:
            line += f" missing={send['missing']}"
        lines.append(line)
    else:
        lines.append("# send: missing in the commit-wait window")
    return lines


def _overlap_lines(overlap):
    """Same-TID nesting header lines (cross-TID relations are not checked)."""
    if not overlap:
        return []
    def fmt(items):
        return ",".join(f"{i['tag']}@{i['w0_ns']}" for i in items) or "-"
    return [
        "# overlap (same tid): "
        f"contains=[{fmt(overlap['same_tid_contains'])}] "
        f"contained_in=[{fmt(overlap['same_tid_contained_in'])}] "
        f"partial=[{fmt(overlap['same_tid_partial_overlaps'])}]",
    ]


def _syscalls_stub(wrapper):
    # render_timeline is called with only the timeline; syscall rows are
    # already materialised in the wrapper (enter offsets/durations), so the
    # zone boundaries are re-derived from those windows instead of the raw
    # event list.
    evs = []
    for w in wrapper["syscall_windows"]:
        enter = wrapper["w0_ns"] + int(w["enter_offset_ms"] * 1e6)
        evs.append((enter, w["name"], "enter"))
        if not w["open"]:
            evs.append((enter + int(w["dur_ms"] * 1e6), w["name"], "exit"))
    return evs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path,
                        help="directory with trace.raw and run-*/ probe dumps")
    parser.add_argument("--output", type=Path,
                        help="write results JSON here (default: stdout)")
    parser.add_argument("--threshold-ms", type=float, default=50.0,
                        help="report wrappers at least this long (default 50)")
    parser.add_argument("--timelines", type=int, default=3,
                        help="text timelines per run for the longest wrappers "
                             "(0 disables; default 3)")
    parser.add_argument("--probe-reference", type=Path,
                        help="probe-only run dir for the perturbation comparison")
    args = parser.parse_args()
    result = analyze(args.run_dir, threshold_ms=args.threshold_ms,
                     timelines=args.timelines,
                     probe_reference=args.probe_reference)
    text = json.dumps(result, indent=2)
    if args.output:
        args.output.write_text(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    main()
