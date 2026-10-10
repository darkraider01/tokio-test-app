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
import bisect
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

# Predeclared diagnostic event set (the ftrace.sh FS_BLOCK_EVENTS array):
# filesystem/block/writeback events captured to locate the kernel wait
# inside the long directory-sync wrappers.  Lines are recorded into
# stats["diag_events"] (popped by analyze) so captures without these
# events reproduce byte-identical results.
DIAG_EVENTS = frozenset({
    "btrfs_transaction_commit", "btrfs_finish_ordered_extent",
    "btrfs_reserve_ticket", "btrfs_tree_lock", "folio_wait_writeback",
    "block_bio_queue", "block_rq_issue", "block_rq_complete",
})
DIAG_RE = re.compile(r"^(?P<event>[a-z][a-z0-9_]*):\s+(?P<fields>.+)$")

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
        # Diagnostic fs/block/writeback events, chronologically appended
        # (raw dumps are emitted in timestamp order across CPUs; analyze
        # pops this list so results of captures without these events stay
        # byte-identical).
        "diag_events": [],
        # sched_waking/sched_wakeup lines per wakee tid: (ts, comm-current-
        # on-cpu), popped by analyze alongside diag_events.
        "wakes": {},
    }
    pending_stack = None
    with path.open() as fh:
        for line in fh:
            if line.startswith(" => ") and pending_stack is not None:
                pending_stack["frames"].append(line.strip()[3:].strip())
                continue
            pending_stack = None
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
            if rest == "<stack trace>":
                pending_stack = {"tid": tid, "ts": ts,
                                 "cpu": int(m["cpu"]), "frames": []}
                stats.setdefault("stack_traces", []).append(pending_stack)
                stats["event_counts"]["kernel_stack"] = (
                    stats["event_counts"].get("kernel_stack", 0) + 1)
                continue
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
                stats["wakes"].setdefault(wakee, []).append(
                    (ts, m.group("prefix")))
                continue
            dm = DIAG_RE.match(rest)
            if dm and dm.group("event") in DIAG_EVENTS:
                stats["event_counts"][dm.group("event")] = \
                    stats["event_counts"].get(dm.group("event"), 0) + 1
                stats["diag_events"].append({
                    "ts": ts, "event": dm.group("event"), "tid": tid,
                    "cpu": int(m.group("cpu")), "comm": m.group("prefix"),
                    "fields": dm.group("fields"),
                })
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
        # Event lines look like "<subsys/event> enable=<0|1> filter=<f>";
        # match them generically (the diag fs/block events are neither
        # sched nor syscalls) so their "=" never leaks into the key=value
        # section.
        if re.match(r"^\S+ enable=\d+ filter=", line):
            events.append(line.strip())
        elif "=" in line and not line.startswith(("sched", "syscalls")):
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
    # Diagnostic fs/block/writeback events: popped out of stats so results
    # of captures without these events stay byte-identical.
    diag_events = tstats.pop("diag_events", [])
    wakes = tstats.pop("wakes", {})
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

    kernel_wait = kernel_wait_section(diag_events, wakes, per_run, timeline,
                                      syscalls, settings, cross_clock_ok)

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
        **({"kernel_wait": kernel_wait} if kernel_wait is not None else {}),
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

    if kernel_wait is not None:
        # This capture included the diagnostic fs/block/writeback events:
        # the "no block/journal events captured" note no longer describes
        # it, and the correlation caveats below apply instead.
        result["limitations"] = [
            s for s in result["limitations"]
            if "no block/journal events captured" not in s
        ] + [
            "diagnostic btrfs/writeback events are not comm-filtered: "
            "background activity on the same filesystem appears in their "
            "totals; block events are capture-filtered to the volume's "
            "device (kernel_wait.capture_filter)",
            "kernel_wait evidence classes separate temporal overlap, "
            "shared-device activity, a same-task writeback wait, and "
            "proximity of a wake edge to a completion event; only the "
            "same-task wait names its object, proximity is never proof, "
            "and none of them identifies a single cause for the wait or "
            "establishes response-criticality",
        ]

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
                                                 timeline.get(w["tid"], []),
                                                 diag_events))
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


# --- kernel-wait evidence (diagnostic fs/block/writeback events) ------------
#
# Predeclared rules for correlating the diagnostic event set
# (ftrace.sh FS_BLOCK_EVENTS) with the long wrappers.  Evidence levels are
# defined by what was OBSERVED, never by what it would imply about the
# response path:
#
#   temporal_overlap        an event timestamp falls inside the wrapper
#                           window (per-window event_counts); under dense
#                           device activity wake-edge proximity is also
#                           only temporal — counted in
#                           proximity_summary, never claimed as causal;
#   shared_device_temporal  a block event on the volume's device overlaps
#                           the window: the device was busy, not that this
#                           wait was for those requests (block events are
#                           not tagged with the calling task's I/O);
#   demonstrable_dependency the wrapper thread itself entered a writeback
#                           wait (folio_wait_writeback recorded with the
#                           wrapper's own tid) at/before entry into its
#                           own blocked segment — waiter and waited folio
#                           are named by the event; establishes that the
#                           task encountered that wait path; whether it
#                           explains the full blocked segment duration
#                           remains inferred;
#   isolated_temporal_candidate
#                           isolation rule: at most one event of that
#                           completion class inside the segment, within
#                           the temporal threshold of the wake edge.
#                           Selected by temporal proximity and sparsity;
#                           no dependency match to the blocked task was
#                           established, and a single candidate does not
#                           exclude untraced causes; waker comm is CPU
#                           context at wake, not necessarily the logical
#                           producer or releasing subsystem.
#
KERNEL_WAIT_WRAPPERS_PER_RUN = 3
KERNEL_WAIT_WAKE_EDGE_US = 1000.0
# folio_wait_writeback fires just before the thread switches itself out;
# allow that tracepoint-to-switch gap when matching a same-task wait to
# the blocked segment it precedes.
FOLIO_PRE_US = 200 * 1000
# A wake edge is attributed to a waker comm only within this distance of
# a sched_waking/sched_wakeup for the wrapper tid.
WAKER_MATCH_US = 2_000 * 1000
COMPLETION_EVENTS = ("btrfs_finish_ordered_extent", "block_rq_complete",
                     "btrfs_transaction_commit")
BLOCK_EVENTS = frozenset({"block_bio_queue", "block_rq_issue",
                          "block_rq_complete"})

_BLOCK_FIELDS_RE = re.compile(
    r"^(?P<dev>\d+,\d+)\s+(?P<rwbs>\S+).*?(?P<sector>\d+)\s+\+\s+(?P<count>\d+)")
_KV_PATTERNS = (
    ("ino", r"\bino=(\d+)"), ("index", r"\bindex=(\d+)"),
    ("root", r"\broot=(\d+)"), ("generation", r"\bgeneration=(\d+)"),
    ("gen", r"\bgen=(\d+)"), ("diff_ns", r"\bdiff_ns=(\d+)"),
    ("is_log_tree", r"\bis_log_tree=(-?\d+)"), ("bytes", r"\bbytes=(\d+)"),
    ("error", r"\berror=(-?\d+)"), ("uptodate", r"\buptodate=(\d+)"),
    ("start_ns", r"\bstart_ns=(\d+)"),
)
_FSID_RE = re.compile(
    r"^(?P<fsid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{12}):")
_FOLIO_BDI_RE = re.compile(r"^bdi (?P<bdi>.*?): ino=")


def diag_identity(event, fields):
    """Correlation identity extracted from a diagnostic event's fields.

    Only field values the event actually printed are extracted; the raw
    ``fields`` string is always preserved alongside, so an absent field
    stays missing rather than being guessed.
    """
    ident = {}
    if event in BLOCK_EVENTS:
        m = _BLOCK_FIELDS_RE.match(fields)
        if m:
            ident.update(m.groupdict())
        return ident
    fsid = _FSID_RE.match(fields)
    if fsid:
        ident["fsid"] = fsid.group("fsid")
    bdi = _FOLIO_BDI_RE.match(fields)
    if bdi:
        ident["bdi"] = bdi.group("bdi")
    for key, pat in _KV_PATTERNS:
        m = re.search(pat, fields)
        if m:
            ident[key] = m.group(1)
    return ident


def _merge_windows(windows):
    """Merge overlapping wrapper windows for an outside-any-window count."""
    merged = []
    for s, e in sorted(windows):
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


def kernel_wait_section(diag, wakes, per_run, timeline, syscalls, settings,
                        cross_clock_ok):
    """Evidence section for the diagnostic fs/block/writeback events.

    Returns None when the capture contains no diagnostic events, so
    captures without them reproduce byte-identical results.

    The correlation rules below were fixed after inspecting a
    rate-probe-style pilot window of this capture and observing that the
    volume device completes thousands of requests per second: under such
    density a wake edge is *always* within the temporal threshold of
    *some* completion, so raw proximity carries no information.  Two
    rules keep the classification honest:

    * the isolation rule — a completion class only supports an
      ``isolated_temporal_candidate`` entry when at most one event of that
      class occurred inside the segment (the candidate at the edge is then
      the only one of its kind); dense segments are reported through
      ``proximity_summary`` instead;
    * waker attribution — the ``comm`` recorded at a wake edge is the
      task *current on that CPU* when the wake fired, which for
      irq-context wakes is an unrelated task (observed: desktop apps
      "waking" fsync threads).  Waker comms are reported as observed
      facts with this ambiguity, never as a causal claim.
    """
    if not diag:
        return None
    diag = sorted(diag, key=lambda e: e["ts"])
    totals = {}
    by_name = {}
    for e in diag:
        totals[e["event"]] = totals.get(e["event"], 0) + 1
        by_name.setdefault(e["event"], []).append(e["ts"])
    block_filters = [line for line in settings.get("events", [])
                     if line.startswith("block/")]
    section = {
        "status": ("analyzed" if cross_clock_ok
                   else "withheld_cross_clock_not_validated"),
        "event_totals": totals,
        "capture_filter": {
            "block_event_filter_lines": block_filters,
            "note": ("block events were filtered to the volume's device at "
                     "capture time (the filter lines are collected from "
                     "trace.settings); btrfs/writeback events carry no comm "
                     "filter, so background activity on the same filesystem "
                     "appears in the totals"),
        },
        "correlation_rules": {
            "evidence_levels": {
                "temporal_overlap":
                    "event timestamp inside the wrapper window or a "
                    "blocked segment; reported as event_counts and "
                    "proximity_summary only, with no relationship claimed "
                    "(under dense device activity any wake edge is within "
                    "the threshold of some completion — such proximity is "
                    "counted as dense, not causal)",
                "shared_device_temporal":
                    "block event on the volume device overlapping the "
                    "window — device busy, not wait attribution",
                "demonstrable_dependency":
                    "the wrapper thread's own writeback-wait event "
                    "(folio_wait_writeback with this tid) inside its own "
                    "blocked segment, allowing folio_pre_segment_us for "
                    "the tracepoint that fires just before the "
                    "switch-out (waiter and folio named). Establishes that "
                    "the task entered that wait path; proximity to the "
                    "switch-out does not prove that this wait accounts for "
                    "the full following blocked segment duration, which "
                    "remains inferred",
                "isolated_temporal_candidate":
                    "isolation rule: at most one event of that completion "
                    "class inside the segment, within "
                    "wake_edge_threshold_us before the wake edge. "
                    "Selected by temporal proximity and sparsity; no "
                    "dependency match to the blocked task was established. "
                    "A single candidate among captured events does not "
                    "exclude untraced causes, and waker_comm is CPU execution "
                    "context at wake time, not proof of release",
            },
            "wake_edge_threshold_us": KERNEL_WAIT_WAKE_EDGE_US,
            "folio_pre_segment_us": FOLIO_PRE_US / 1000.0,
            "completion_events": list(COMPLETION_EVENTS),
            "waker_comm_semantics":
                "waker_comm is the comm of the task current on the CPU "
                "when the wake fired: for workqueue/process-context wakes "
                "that is the logical waker, for irq-context wakes it is "
                "an unrelated task that happened to be running (observed: "
                "desktop apps and <idle> as waker_comm) — an observed "
                "fact, not a causal attribution",
            "selection": (f"the {KERNEL_WAIT_WRAPPERS_PER_RUN} longest "
                          "wrappers per repetition, ordered by duration"),
            "no_response_criticality":
                "none of these levels establishes response-criticality: a "
                "job's temporal association with a filesystem or block "
                "event does not show that the event was required before "
                "the response",
        },
        "wrappers": [],
    }
    windows = [(w["w0_ns"], w["w1_ns"]) for run in per_run
               for w in run["long_wrappers"]]
    merged = _merge_windows(windows)
    starts = [m[0] for m in merged]
    outside = 0
    for e in diag:
        i = bisect.bisect_right(starts, e["ts"]) - 1
        if i < 0 or e["ts"] > merged[i][1]:
            outside += 1
    section["outside_long_wrappers"] = outside

    if cross_clock_ok:
        eps = int(KERNEL_WAIT_WAKE_EDGE_US * 1000)

        def count_in(name, a, b):
            arr = by_name.get(name)
            if not arr:
                return 0
            return (bisect.bisect_left(arr, b) - bisect.bisect_left(arr, a))

        def nearest_isolated(name, s, en):
            """Latest event of class ``name`` inside [s, en], else None."""
            arr = by_name.get(name)
            if not arr:
                return None
            i = bisect.bisect_left(arr, en) - 1
            if i < 0 or arr[i] < s:
                return None
            ts = arr[i]
            return ts if 0 <= en - ts <= eps else None

        for run in per_run:
            for w in run["long_wrappers"][:KERNEL_WAIT_WRAPPERS_PER_RUN]:
                w0, w1, tid = w["w0_ns"], w["w1_ns"], w["tid"]
                lo = bisect.bisect_left(diag, w0, key=lambda e: e["ts"])
                hi = bisect.bisect_right(diag, w1, key=lambda e: e["ts"])
                win = diag[lo:hi]
                rows = decompose_wrapper(timeline.get(tid, []),
                                         syscalls.get(tid, ()), w0, w1)[0]
                blocked = [(s, en) for _z, state, s, en in rows
                           if state.startswith("blocked")]
                tid_wakes = wakes.get(tid, [])

                def waker_at(edge):
                    best = None
                    best_d = None
                    for ts, comm in tid_wakes:
                        d = abs(ts - edge)
                        if d <= WAKER_MATCH_US and (best_d is None
                                                    or d < best_d):
                            best, best_d = comm, d
                    return best

                counts, evidence = {}, []
                claimed_folios = set()
                for e in win:
                    counts[e["event"]] = counts.get(e["event"], 0) + 1

                # Per-segment correlation.
                proximity = {"wake_edges": len(blocked),
                             "dense_completion_edges": 0,
                             "isolated_completion_edges": 0,
                             "no_completion_within_threshold_edges": 0,
                             "nearest_completion_us": None}
                nearest_dists = []
                wakes_by_comm = {}
                top_segments = []
                for s, en in blocked:
                    waker = waker_at(en)
                    if waker is not None:
                        wakes_by_comm[waker] = wakes_by_comm.get(waker, 0) + 1
                    top_segments.append({
                        "start_offset_ms": round((s - w0) / 1e6, 6),
                        "dur_ms": round((en - s) / 1e6, 6),
                        "rq_completions_during": count_in(
                            "block_rq_complete", s, en),
                        "waker_comm": waker,
                    })

                    # demonstrable_dependency: this thread's own folio
                    # wait, immediately before / during the switch-out.
                    fol = [e for e in win
                           if e["event"] == "folio_wait_writeback"
                           and e["tid"] == tid
                           and s - FOLIO_PRE_US <= e["ts"] < en
                           and e["ts"] not in claimed_folios]
                    if fol:
                        fe = max(fol, key=lambda e: e["ts"])
                        claimed_folios.add(fe["ts"])
                        evidence.append({
                            "level": "demonstrable_dependency",
                            "event": fe["event"],
                            "offset_ms": round((fe["ts"] - w0) / 1e6, 6),
                            "segment_start_offset_ms": round(
                                (s - w0) / 1e6, 6),
                            "identity": diag_identity(fe["event"],
                                                      fe["fields"]),
                            "basis": (
                                f"tid {tid} itself recorded this writeback "
                                "wait path at/before the switch-out into its own "
                                "blocked:D segment (naming bdi/ino/index). "
                                "This establishes that the task encountered the "
                                "folio wait path; proximity to the switch-out does "
                                "not prove that this wait path accounts for the "
                                "entire duration of the following blocked segment, "
                                "which remains inferred."),
                        })

                    # isolated_temporal_candidate: isolation rule per class.
                    rq_n = count_in("block_rq_complete", s, en)
                    near_rq = nearest_isolated("block_rq_complete", s, en)
                    if near_rq is not None:
                        nearest_dists.append(en - near_rq)
                    if rq_n >= 2:
                        proximity["dense_completion_edges"] += 1
                    elif rq_n == 1 and near_rq is not None:
                        proximity["isolated_completion_edges"] += 1
                    else:
                        proximity["no_completion_within_threshold_edges"] += 1
                    for cls in COMPLETION_EVENTS:
                        if count_in(cls, s, en) > 1:
                            continue  # dense for this class: vacuous
                        ts = nearest_isolated(cls, s, en)
                        if ts is None:
                            continue
                        evidence.append({
                            "level": "isolated_temporal_candidate",
                            "event": cls,
                            "offset_ms": round((ts - w0) / 1e6, 6),
                            "wake_edge_offset_ms": round((en - w0) / 1e6, 6),
                            "distance_us": round((en - ts) / 1000.0, 3),
                            "segment_dur_ms": round((en - s) / 1e6, 6),
                            "waker_comm": waker,
                            "identity": diag_identity(cls, next(
                                e["fields"] for e in win
                                if e["event"] == cls and e["ts"] == ts)),
                            "basis": (
                                "isolation rule: this is the only event "
                                "of its class inside the segment and occurred "
                                "within wake_edge_threshold_us of the wake. "
                                "Selected by temporal proximity and sparsity; "
                                "no dependency match to the blocked task was "
                                "established. A single candidate among captured "
                                "events does not exclude untraced causes, and "
                                "waker_comm is execution context on the CPU, not "
                                "necessarily the logical producer or releasing "
                                "subsystem."),
                        })
                if nearest_dists:
                    proximity["nearest_completion_us"] = {
                        "p50": round(percentile(nearest_dists, 0.5) / 1000.0,
                                     3),
                        "max": round(max(nearest_dists) / 1000.0, 3),
                    }
                proximity["note"] = (
                    "dense edges (>= 2 completions during the segment) "
                    "cannot single out a cause by proximity and are "
                    "reported here instead of as isolated_temporal_candidate")

                evidence.sort(key=lambda x: x["offset_ms"])
                top_segments.sort(key=lambda x: -x["dur_ms"])
                fs_events = [{
                    "event": e["event"],
                    "offset_ms": round((e["ts"] - w0) / 1e6, 6),
                    "tid": e["tid"],
                    "identity": diag_identity(e["event"], e["fields"]),
                } for e in win
                    if e["event"] in ("btrfs_transaction_commit",
                                      "btrfs_finish_ordered_extent")][:20]
                devs = sorted({d for d in (
                    diag_identity(e["event"], e["fields"]).get("dev")
                    for e in win if e["event"] in BLOCK_EVENTS) if d})
                section["wrappers"].append({
                    "run": run["run"], "tag": w["tag"], "tid": tid,
                    "dur_ms": w["dur_ms"], "w0_ns": w0, "w1_ns": w1,
                    "job_task_id": w["identity"]["job_task_id"],
                    "events_in_window": len(win),
                    "event_counts": counts,
                    "phase_summary": {
                        "blocked_segments": len(blocked),
                        "blocked_total_ms": round(
                            sum(en - s for s, en in blocked) / 1e6, 3),
                        "top_segments": top_segments[:5],
                        "wakes_by_comm": wakes_by_comm,
                    },
                    "proximity_summary": proximity,
                    "fs_events_in_window": fs_events,
                    "shared_device_events": (
                        {"devices": devs, "level": "shared_device_temporal",
                         "note": ("block events on these devices overlap the "
                                  "window: the device was busy, not that "
                                  "this wait was for these requests")}
                        if devs else None),
                    "evidence": evidence,
                    "no_diagnostic_events_in_window": not win,
                })
    return section


def render_timeline(wrapper, timeline, diag_events=None):
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
    if diag_events:
        win = [e for e in diag_events if w0 <= e["ts"] <= w1]
        lines.append(
            f"# fs/block/writeback events in window: {len(win)} "
            "(offset ms from w0; temporal unless noted in kernel_wait)")
        for e in win[:500]:
            same = "*" if e["tid"] == wrapper["tid"] else " "
            lines.append(f"{(e['ts'] - w0) / 1e6:10.3f}{same} "
                         f"{e['event']:<26} tid={e['tid']} {e['fields']}")
        if len(win) > 500:
            lines.append(f"# ... {len(win) - 500} more events not rendered")
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
