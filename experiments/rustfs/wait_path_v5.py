"""Reproducible generator for v5 per-disk acknowledgement and quorum reconstruction.

Reads existing v5 captures, reconstructs acknowledgements using fs_probe, joins client
object keys from tiers.json to operation hashes, records all consumed input hashes and
analysis parameters, selects representative operations using explicit deterministic criteria,
and emits the disk/job/acknowledgement table and methodological limitations.
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import fs_probe


DEFAULT_HISTORICAL_OPERATIONS = [
    {"run": "run-1", "key": "c8/221.bin", "criterion": "slow_successful_put_with_counted_long_wait"},
    {"run": "run-1", "key": "c8/210.bin", "criterion": "operation_with_long_post_send_tail"},
    {"run": "run-1", "key": "c8/208.bin", "criterion": "operation_with_post_send_tail_and_counted_waits"},
    {"run": "run-2", "key": "c8/37.bin", "criterion": "run2_slow_successful_put"},
]

DEFAULT_JOINT_OPERATIONS = [
    {"run": "run-1", "key": "c8/82.bin", "criterion": "slow_successful_put_with_counted_fsync_and_stack"},
    {"run": "run-1", "key": "c8/373.bin", "criterion": "slowest_put_with_stack_covered_fsync"},
    {"run": "run-1", "key": "c8/80.bin", "criterion": "counted_and_tail_fsyncs_with_stacks"},
]

CRITERIA_DEFINITIONS = {
    "slow_successful_put_with_counted_long_wait": (
        "Successful PUT (HTTP 200, unshed) with the highest client attempt-to-completion "
        "latency where a counted acknowledgement (counted_before_quorum or quorum_triggering) "
        "includes an ancestor_fsync >= 50 ms. Tie-breaking: earliest attempted_ns, then lexicographical key."
    ),
    "operation_with_long_post_send_tail": (
        "Successful PUT where a post-send or post-quorum disk tail contains an ancestor_fsync >= 50 ms "
        "completing after SEND_OK. Ranked by longest post-send tail duration, tie-breaking by earliest attempted_ns."
    ),
    "operation_with_post_send_tail_and_counted_waits": (
        "Successful PUT exhibiting both counted pre-quorum fsync waits and an observed post-send tail. "
        "Tie-breaking: highest client attempt-to-completion latency, then earliest attempted_ns."
    ),
    "run2_slow_successful_put": (
        "Successful PUT in run-2 with the highest client attempt-to-completion latency. "
        "Tie-breaking: earliest attempted_ns, then lexicographical key."
    ),
    "slow_successful_put_with_counted_fsync_and_stack": (
        "Manually selected successful PUT (HTTP 200, unshed) verified against the correlated stack artifact "
        "to have a validated quorum sequence where a counted acknowledgement (Disk 0 quorum trigger) "
        "contains an fsync wrapper with a sampled wait_for_commit stack."
    ),
    "slowest_put_with_stack_covered_fsync": (
        "Manually selected slowest successful PUT (HTTP 200, unshed) verified against the correlated stack artifact "
        "to have counted fsync wrappers with matching kernel switch-out stacks."
    ),
    "counted_and_tail_fsyncs_with_stacks": (
        "Manually selected successful PUT verified against the correlated stack artifact exhibiting both "
        "counted pre-quorum fsync waits with matching stacks and an observed post-send, post-client-completion tail."
    ),
}

LIMITATIONS = [
    "Disk tasks execute in parallel on separate storage paths",
    "Result-consumption order is an observed coordinator acknowledgement sequence, not a serial dependency chain across disks",
    "A counted acknowledgement is not necessarily counterfactually indispensable; another disk might have substituted under another schedule",
    "Parallel job durations across disks must not be summed into client latency",
    "KIND_DISK_COMPLETE is mutation return immediately following the disk mutation future, before stage metrics and enclosing task return",
    "The interval from mutation return to coordinator consumption includes any intervening task code plus result propagation through JoinSet/channels",
    "Prerequisite relationships are established only where the pinned source path (commit.rs) demonstrably awaits that job before producing the disk mutation result",
    "Jobs finishing after mutation return or lacking source-verified awaited paths are marked unestablished or contradictory",
    "A sampled switch-out stack establishes the encountered wait path at the switch-out, not continuous residence in that function throughout the sleep",
    "The transaction identity, releasing work, and reason for a long commit remain unresolved",
]


def sha256_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def find_repo_root(start_path):
    current = start_path.resolve()
    for parent in [current] + list(current.parents):
        if (parent / ".git").exists() or (parent / "experiments").exists():
            return parent
    return current


def collect_input_hashes(run_dir):
    repo_root = find_repo_root(run_dir)
    inputs = {}

    candidate_patches = [
        repo_root / "experiments/rustfs/patches/tokio-1.53.2-fs-probe-v5.patch",
        repo_root / "experiments/rustfs/patches/rustfs-probe-v5.patch",
        repo_root / "patches/tokio-1.53.2-fs-probe-v5.patch",
        repo_root / "patches/rustfs-probe-v5.patch",
    ]
    for p in candidate_patches:
        if p.is_file():
            rel = "patches/" + p.name
            inputs[rel] = sha256_file(p)

    candidate_binary = repo_root / ".repro/preserved-binaries/rustfs-v5"
    if candidate_binary.is_file():
        inputs["binary/rustfs-v5"] = sha256_file(candidate_binary)

    for fname in ["manifest.json", "trace.raw", "measurement.json"]:
        fpath = run_dir / fname
        if fpath.is_file():
            rel = str(fpath.relative_to(repo_root)) if fpath.is_relative_to(repo_root) else str(fpath)
            inputs[rel] = sha256_file(fpath)

    for run_subdir in sorted(run_dir.glob("run-*")):
        for fname in ["tiers.json", "fs-probe.bin"]:
            fpath = run_subdir / fname
            if fpath.is_file():
                rel = str(fpath.relative_to(repo_root)) if fpath.is_relative_to(repo_root) else str(fpath)
                inputs[rel] = sha256_file(fpath)

    return inputs


def select_automatic_operations(run_data):
    selected = []
    # 1. run-1 slow_successful_put_with_counted_long_wait
    # Successful PUT (HTTP 200, unshed) with the highest client attempt-to-completion
    # latency where a counted acknowledgement (counted_before_quorum or quorum_triggering)
    # includes an ancestor_fsync >= 50 ms. Tie-breaking: earliest attempted_ns, then lexicographical key.
    run1 = run_data.get("run-1")
    if run1:
        candidates = []
        for req in run1["attempts"].values():
            op = fs_probe.op_hash(req.get("bucket", "tokio-experiment"), req["key"])
            ack = fs_probe.reconstruct_acknowledgements(run1["records"], op, run1["jobs"])
            has_long_counted = any(
                d["classification"] in ("counted_before_quorum", "quorum_triggering_acknowledgement")
                and any(j["step_tag"] == "ancestor_fsync" and (j["dur_ms"] or 0) >= 50.0 for j in d["jobs"])
                for d in ack["disks"]
            )
            if has_long_counted:
                candidates.append({
                    "key": req["key"],
                    "latency_ms": req["attempt_to_completion_ms"],
                    "attempted_ns": req["attempted_ns"],
                })
        if candidates:
            candidates.sort(key=lambda c: (-c["latency_ms"], c["attempted_ns"], c["key"]))
            chosen_key = candidates[0]["key"]
            selected.append({"run": "run-1", "key": chosen_key, "criterion": "slow_successful_put_with_counted_long_wait"})

    # 2. run-1 operation_with_long_post_send_tail
    # Successful PUT where a post-send or post-quorum disk tail contains an ancestor_fsync >= 50 ms
    # completing after SEND_OK. Ranked by longest post-send tail duration, tie-breaking by earliest attempted_ns, then lexicographical key.
    if run1:
        candidates = []
        for req in run1["attempts"].values():
            op = fs_probe.op_hash(req.get("bucket", "tokio-experiment"), req["key"])
            ack = fs_probe.reconstruct_acknowledgements(run1["records"], op, run1["jobs"])
            send_ts = ack.get("send_ts")
            if not send_ts or ack.get("send_kind") != "send_ok":
                continue
            tail_durs = []
            for d in ack["disks"]:
                if d["classification"] in ("post_quorum", "post_send"):
                    for j in d["jobs"]:
                        if j["step_tag"] == "ancestor_fsync" and (j["dur_ms"] or 0) >= 50.0:
                            if j.get("end_ts") is not None and j["end_ts"] > send_ts:
                                tail_durs.append((j["end_ts"] - send_ts) / 1e6)
            if tail_durs:
                max_tail_ms = max(tail_durs)
                candidates.append({
                    "key": req["key"],
                    "post_send_tail_ms": max_tail_ms,
                    "attempted_ns": req["attempted_ns"],
                })
        if candidates:
            candidates.sort(key=lambda c: (-c["post_send_tail_ms"], c["attempted_ns"], c["key"]))
            chosen_key = candidates[0]["key"]
            selected.append({"run": "run-1", "key": chosen_key, "criterion": "operation_with_long_post_send_tail"})

    # 3. run-1 operation_with_post_send_tail_and_counted_waits
    # Successful PUT exhibiting both counted pre-quorum fsync waits and an observed post-send tail.
    # Tie-breaking: highest client attempt-to-completion latency, then earliest attempted_ns, then lexicographical key.
    if run1:
        candidates = []
        for req in run1["attempts"].values():
            if req["key"] in [s["key"] for s in selected]:
                continue
            op = fs_probe.op_hash(req.get("bucket", "tokio-experiment"), req["key"])
            ack = fs_probe.reconstruct_acknowledgements(run1["records"], op, run1["jobs"])
            send_ts = ack.get("send_ts")
            if not send_ts or ack.get("send_kind") != "send_ok":
                continue
            has_counted_fsync = any(
                d["classification"] in ("counted_before_quorum", "quorum_triggering_acknowledgement")
                and any(j["step_tag"] in ("ancestor_fsync", "dst_dir_fsync") and (j["dur_ms"] or 0) > 0 for j in d["jobs"])
                for d in ack["disks"]
            )
            has_observed_tail = any(
                d["classification"] in ("post_quorum", "post_send")
                and (
                    (d.get("mutation_return_ts") is not None and d["mutation_return_ts"] > send_ts)
                    or any(j.get("end_ts") is not None and j["end_ts"] > send_ts for j in d["jobs"])
                )
                for d in ack["disks"]
            )
            if has_counted_fsync and has_observed_tail:
                candidates.append({
                    "key": req["key"],
                    "latency_ms": req["attempt_to_completion_ms"],
                    "attempted_ns": req["attempted_ns"],
                })
        if candidates:
            candidates.sort(key=lambda c: (-c["latency_ms"], c["attempted_ns"], c["key"]))
            chosen_key = candidates[0]["key"]
            selected.append({"run": "run-1", "key": chosen_key, "criterion": "operation_with_post_send_tail_and_counted_waits"})

    # 4. run-2 slow successful put
    # Successful PUT in run-2 with the highest client attempt-to-completion latency.
    # Tie-breaking: earliest attempted_ns, then lexicographical key.
    run2 = run_data.get("run-2")
    if run2:
        candidates = [
            {
                "key": req["key"],
                "latency_ms": req["attempt_to_completion_ms"],
                "attempted_ns": req["attempted_ns"],
            }
            for req in run2["attempts"].values()
        ]
        if candidates:
            candidates.sort(key=lambda c: (-c["latency_ms"], c["attempted_ns"], c["key"]))
            chosen_key = candidates[0]["key"]
            selected.append({"run": "run-2", "key": chosen_key, "criterion": "run2_slow_successful_put"})

    return selected


BTRFS_WAIT_FRAMES = (
    "wait_for_commit",
    "btrfs_commit_transaction",
    "wait_log_commit",
    "btrfs_sync_log",
)


def normalize_rel_to_run_dir(p_str, run_dir, repo_root):
    """Normalize a path to be relative to run_dir while preserving run subdirectories."""
    p = Path(p_str)
    if p.is_absolute():
        try:
            return str(p.relative_to(run_dir.resolve()))
        except ValueError:
            pass
    abs_p = (repo_root / p).resolve()
    try:
        return str(abs_p.relative_to(run_dir.resolve()))
    except ValueError:
        pass
    parts = p.parts
    if run_dir.name in parts:
        idx = parts.index(run_dir.name)
        if idx + 1 < len(parts):
            return str(Path(*parts[idx + 1:]))
    return str(p)


def validate_capture_hashes(stacks_data, run_dir, input_hashes, selected_runs=None):
    """Validate that the stack artifact includes matching hashes for trace.raw
    and the selected run's probe dump and tiers.json using run-specific identities.
    """
    if not isinstance(stacks_data, dict):
        return False, "stacks_data_not_dict"

    run_dir = Path(run_dir)
    repo_root = find_repo_root(run_dir)

    declared_items = []
    if isinstance(stacks_data.get("inputs"), dict):
        for p, h in stacks_data["inputs"].items():
            declared_items.append((p, h))
    prov = stacks_data.get("provenance")
    if isinstance(prov, dict):
        if isinstance(prov.get("inputs"), dict):
            for p, h in prov["inputs"].items():
                declared_items.append((p, h))
        input_ver = prov.get("input_verification")
        if isinstance(input_ver, dict):
            for item in input_ver.get("verified_inputs", []) + input_ver.get("verified_matches", []):
                if isinstance(item, dict) and item.get("path") and item.get("sha256"):
                    declared_items.append((item["path"], item["sha256"]))

    if not declared_items:
        return False, "missing_provenance_or_inputs"

    declared_by_rel = {}
    for decl_path, decl_hash in declared_items:
        norm = normalize_rel_to_run_dir(decl_path, run_dir, repo_root)
        if norm in declared_by_rel:
            if declared_by_rel[norm] != decl_hash:
                return False, f"conflicting_declared_hashes_for_{norm}"
        else:
            declared_by_rel[norm] = decl_hash

    if not selected_runs:
        selected_runs = [d.name for d in sorted(run_dir.glob("run-*")) if d.is_dir()]

    required_files = []
    if (run_dir / "trace.raw").is_file():
        required_files.append("trace.raw")
    for r in selected_runs:
        if (run_dir / r / "fs-probe.bin").is_file():
            required_files.append(f"{r}/fs-probe.bin")
        if (run_dir / r / "tiers.json").is_file():
            required_files.append(f"{r}/tiers.json")

    for req in required_files:
        if req not in declared_by_rel:
            return False, f"missing_required_capture_hash_for_{req}"

    for norm, decl_hash in declared_by_rel.items():
        disk_p = run_dir / norm
        if disk_p.is_file():
            actual_h = sha256_file(disk_p)
            if decl_hash != actual_h:
                return False, f"capture_hash_mismatch_for_{norm}"
        elif (repo_root / norm).is_file():
            actual_h = sha256_file(repo_root / norm)
            if decl_hash != actual_h:
                return False, f"capture_hash_mismatch_for_{norm}"

    return True, "hashes_verified"


def validate_operation_stack_evidence(stacks_data, run_name, key, op_hash, ack, criterion=None):
    """Validate that stacks_data contains matching run/op/job identity, a counted
    source-established fsync prerequisite, and claimed Btrfs wait frames.
    """
    matched_wrappers = []
    counted_fsync_matches = []
    total_stacks = 0
    commit_stacks = 0
    total_wait_stacks = 0

    for w in stacks_data.get("request_linked_wrappers", []):
        if w.get("client_attempt", {}).get("key") != key:
            continue
        ident = w.get("identity", {})
        if ident.get("run") != run_name:
            continue
        if ident.get("op_hash") != op_hash and w.get("operation_link", {}).get("op_hash") != op_hash:
            continue
        jid = ident.get("job_task_id")
        matching_disk = None
        matching_job = None
        for d in ack.get("disks", []):
            for j in d.get("jobs", []):
                if j.get("job_id") == jid:
                    matching_disk = d
                    matching_job = j
                    break
            if matching_disk:
                break
        if not matching_disk or not matching_job:
            continue

        matched_wrappers.append(w)
        stacks = w.get("switch_out_stacks", [])
        total_stacks += len(stacks)

        cur_tx = 0
        cur_wait = 0
        for s in stacks:
            frames = s.get("frames", [])
            has_tx = any("wait_for_commit" in f or "btrfs_commit_transaction" in f for f in frames)
            has_wait = has_tx or any("wait_log_commit" in f or "btrfs_sync_log" in f for f in frames)
            if has_tx:
                cur_tx += 1
            if has_wait:
                cur_wait += 1

        commit_stacks += cur_tx
        total_wait_stacks += cur_wait

        is_counted = matching_disk.get("classification") in (
            "counted_before_quorum",
            "quorum_triggering",
            "quorum_triggering_acknowledgement",
        )
        is_prereq = (
            matching_job.get("prerequisite") == "established"
            and matching_job.get("completed_before_mutation_return") is True
        ) or matching_job.get("is_prerequisite") is True
        is_fsync = (
            "fsync" in matching_job.get("step_tag", "")
            or matching_job.get("step_tag", "").endswith("sync")
            or ident.get("step_tag", "").endswith("sync")
        )

        requires_tx = bool(
            criterion and "wait_for_commit" in CRITERIA_DEFINITIONS.get(criterion, "")
        )
        has_required_frames = cur_tx > 0 if requires_tx else cur_wait > 0

        if is_counted and is_prereq and is_fsync and has_required_frames:
            counted_fsync_matches.append({
                "job_id": jid,
                "disk_index": matching_disk.get("disk_index"),
                "classification": matching_disk.get("classification"),
                "step_tag": matching_job.get("step_tag") or ident.get("step_tag"),
                "commit_stacks": cur_tx,
                "wait_stacks": cur_wait,
            })

    if not matched_wrappers:
        return {
            "is_verified": False,
            "reason": "no_matching_request_linked_wrappers",
            "matched_wrappers": 0,
            "total_switch_out_stacks": 0,
            "btrfs_transaction_commit_stacks": 0,
            "btrfs_wait_stacks": 0,
            "counted_fsync_jobs": [],
        }

    if not counted_fsync_matches:
        return {
            "is_verified": False,
            "reason": "no_counted_source_established_fsync_with_claimed_wait_frames",
            "matched_wrappers": len(matched_wrappers),
            "total_switch_out_stacks": total_stacks,
            "btrfs_transaction_commit_stacks": commit_stacks,
            "btrfs_wait_stacks": total_wait_stacks,
            "counted_fsync_jobs": [],
        }

    return {
        "is_verified": True,
        "reason": None,
        "matched_wrappers": len(matched_wrappers),
        "total_switch_out_stacks": total_stacks,
        "btrfs_transaction_commit_stacks": commit_stacks,
        "btrfs_wait_stacks": total_wait_stacks,
        "counted_fsync_jobs": counted_fsync_matches,
    }


def generate_summary(run_dir, declared_operations=None, automatic=False, stacks_path=None):
    run_dir = Path(run_dir)
    repo_root = find_repo_root(run_dir)
    input_hashes = collect_input_hashes(run_dir)

    # Check and bind correlated stack artifact if provided or discoverable
    resolved_stacks = None
    if stacks_path:
        resolved_stacks = Path(stacks_path)
    else:
        candidates = [
            run_dir / "wait-path-v5-joint-stacks.json",
            repo_root / "experiments/rustfs/results/wait-path-v5-joint-stacks.json",
        ]
        for c in candidates:
            if c.is_file():
                resolved_stacks = c
                break

    stacks_data = None
    if resolved_stacks and resolved_stacks.is_file():
        rel = str(resolved_stacks.relative_to(repo_root)) if resolved_stacks.is_relative_to(repo_root) else str(resolved_stacks)
        input_hashes[rel] = sha256_file(resolved_stacks)
        try:
            stacks_data = json.loads(resolved_stacks.read_text())
        except Exception:
            stacks_data = None

    run_data = {}
    for run_subdir in sorted(run_dir.glob("run-*")):
        run_name = run_subdir.name
        tiers_file = run_subdir / "tiers.json"
        probe_file = run_subdir / "fs-probe.bin"
        if not (tiers_file.is_file() and probe_file.is_file()):
            continue
        tiers = json.loads(tiers_file.read_text())
        attempts = {
            req["key"]: {**req, "bucket": tier.get("bucket", "tokio-experiment")}
            for tier in tiers
            for req in tier.get("requests", [])
            if not req.get("client_shed") and req.get("status") == 200
        }
        _, records = fs_probe.read_probe(probe_file)
        jobs, _ = fs_probe.group_jobs(records)
        run_data[run_name] = {
            "tiers": tiers,
            "attempts": attempts,
            "records": records,
            "jobs": jobs,
        }

    if automatic:
        spec = select_automatic_operations(run_data)
    elif declared_operations:
        spec = declared_operations
    elif "run-2" not in run_data:
        spec = DEFAULT_JOINT_OPERATIONS
    else:
        spec = DEFAULT_HISTORICAL_OPERATIONS

    selected_runs = sorted(list(set(item["run"] for item in spec)))

    capture_hashes_valid = False
    capture_hash_reason = "no_stack_artifact"
    if stacks_data:
        capture_hashes_valid, capture_hash_reason = validate_capture_hashes(
            stacks_data, run_dir, input_hashes, selected_runs=selected_runs
        )

    operations = []
    for item in spec:
        run_name = item["run"]
        key = item["key"]
        criterion = item.get("criterion", "declared_operation")
        if run_name not in run_data:
            raise ValueError(f"Run {run_name} not found in {run_dir}")
        data = run_data[run_name]
        req = data["attempts"].get(key)
        if not req:
            raise ValueError(f"Object key {key} not found in {run_name}/tiers.json")

        bucket = req.get("bucket", "tokio-experiment")
        op = fs_probe.op_hash(bucket, key)
        ack = fs_probe.reconstruct_acknowledgements(data["records"], op, data["jobs"])

        client_attempt = {k: v for k, v in req.items() if k != "bucket"}
        op_entry = {
            "run": run_name,
            "criterion": criterion,
            "key": key,
            "op_hash": op,
            "client_attempt": client_attempt,
            "send_kind": ack["send_kind"],
            "send_ts": ack["send_ts"],
            "quorum_trigger_disk": ack["quorum_trigger_disk"],
            "quorum_trigger_attempt": ack["quorum_trigger_attempt"],
            "evidence_status": ack.get("evidence_status", "validated"),
            "disks": ack["disks"],
        }
        if stacks_data:
            if not capture_hashes_valid:
                op_entry["stack_validation"] = {
                    "status": "unverified",
                    "reason": capture_hash_reason,
                    "artifact": str(resolved_stacks.relative_to(repo_root)) if resolved_stacks.is_relative_to(repo_root) else str(resolved_stacks),
                    "request_linked_wrappers": 0,
                    "total_switch_out_stacks": 0,
                    "btrfs_transaction_commit_stacks": 0,
                    "btrfs_wait_stacks": 0,
                    "counted_fsync_jobs": [],
                }
            else:
                val = validate_operation_stack_evidence(stacks_data, run_name, key, op, ack, criterion=criterion)
                op_entry["stack_validation"] = {
                    "status": "verified" if val["is_verified"] else "unverified",
                    "reason": val["reason"],
                    "artifact": str(resolved_stacks.relative_to(repo_root)) if resolved_stacks.is_relative_to(repo_root) else str(resolved_stacks),
                    "request_linked_wrappers": val["matched_wrappers"],
                    "total_switch_out_stacks": val["total_switch_out_stacks"],
                    "btrfs_transaction_commit_stacks": val["btrfs_transaction_commit_stacks"],
                    "btrfs_wait_stacks": val["btrfs_wait_stacks"],
                    "counted_fsync_jobs": val["counted_fsync_jobs"],
                }
        operations.append(op_entry)

    all_verified = bool(stacks_data and capture_hashes_valid and operations and all(
        op.get("stack_validation", {}).get("status") == "verified" for op in operations
    ))
    if automatic:
        selection_mode = "automatic_criteria"
    elif declared_operations:
        selection_mode = "stack_verified_declared_keys" if all_verified else "declared_manual_keys"
    elif "run-2" not in run_data:
        selection_mode = "stack_verified_declared_keys" if all_verified else "declared_manual_keys"
    else:
        selection_mode = "declared_historical_keys"

    git_commit = None
    repo_root = find_repo_root(run_dir)
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root, capture_output=True, text=True)
        if proc.returncode == 0:
            git_commit = proc.stdout.strip()
    except Exception:
        pass

    return {
        "schema": "rustfs-acknowledgement-dependencies/v2",
        "schema_version": "rustfs-acknowledgement-dependencies/v2",
        "provenance": {
            "generator": "experiments/rustfs/wait_path_v5.py",
            "git_commit": git_commit,
            "inputs": input_hashes,
        },
        "parameters": {
            "selection_mode": selection_mode,
            "selected_keys": [{"run": item["run"], "key": item["key"], "criterion": item.get("criterion")} for item in spec],
            "criteria_definitions": CRITERIA_DEFINITIONS,
        },
        "inputs": input_hashes,
        "representative_operations": operations,
        "limitations": LIMITATIONS,
    }


summarize_waitpath_v5 = generate_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="v5 run directory containing run-1 and run-2")
    parser.add_argument("--output", type=Path, help="output JSON path")
    parser.add_argument("--auto", action="store_true", help="select representative operations automatically using criteria")
    parser.add_argument("--keys", nargs="+", help="explicit object keys in run:key:criterion format")
    parser.add_argument("--stacks", type=Path, help="path to correlated stack artifact to validate stack coverage")
    args = parser.parse_args()

    declared = None
    if args.keys:
        declared = []
        for k_spec in args.keys:
            parts = k_spec.split(":", 2)
            if len(parts) == 3:
                declared.append({"run": parts[0], "key": parts[1], "criterion": parts[2]})
            elif len(parts) == 2:
                declared.append({"run": parts[0], "key": parts[1], "criterion": "declared_key"})
            else:
                declared.append({"run": "run-1", "key": parts[0], "criterion": "declared_key"})

    result = generate_summary(args.run_dir, declared_operations=declared, automatic=args.auto, stacks_path=args.stacks)
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
