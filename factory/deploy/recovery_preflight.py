#!/usr/bin/env python3
"""Offline coordinator boot/config comparison; never installs or releases recovery."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

JSON_LIMIT = 64 * 1024
LARGE_LIMIT = 1024 * 1024
FILES = {
    "plan.json": JSON_LIMIT,
    "capture-manifest.json": LARGE_LIMIT,
    "active.json": JSON_LIMIT,
    "active-public.json": JSON_LIMIT,
    "effective-public.json": JSON_LIMIT,
    "boot-config.json": JSON_LIMIT,
    "active-release.json": JSON_LIMIT,
    "active-release-sha256.txt": 128,
    "boot-release.json": JSON_LIMIT,
    "boot-release-sha256.txt": 128,
    "active-references.json": JSON_LIMIT,
    "boot-references.json": JSON_LIMIT,
    "active-WORKFLOW.md": LARGE_LIMIT,
    "boot-WORKFLOW.md": LARGE_LIMIT,
    "recovery-plan.json": JSON_LIMIT,
    "current-release.txt": 128,
}
SHA = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
CAPTURE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
PROJECT = re.compile(r"[a-z][a-z0-9-]{4,62}|[0-9]{6,20}")
NAME = re.compile(r"[A-Za-z0-9_-]{1,128}")
VERSION = re.compile(r"[1-9][0-9]*")
DEFAULT_ENVS = {"LINEAR_API_KEY", "LINEAR_API_TOKEN"}
ENV_PAIRS = (
    {"FACTORY_HIGH_CLIENT_SECRET", "FACTORY_HIGH_WEBHOOK_SECRET"},
    {"FACTORY_DAYBREAK_CLIENT_SECRET", "FACTORY_DAYBREAK_WEBHOOK_SECRET"},
)
ENVS = DEFAULT_ENVS | set().union(*ENV_PAIRS)
PUBLIC_REQUIRED = {"project_id", "role", "service_revision", "release_sha256"}
PUBLIC_OPTIONAL = {
    "coordinator_workflow", "coordinator_secret_env", "coordinator_secret_versions",
    "coordinator_secret_fingerprints", "enable_linear_webhook",
}
REF_REQUIRED = {"project_id", "role", "service_revision"}
REF_OPTIONAL = {
    "coordinator_workflow", "coordinator_secret_env", "secret_ids",
    "secret_versions", "enable_linear_webhook",
}
BLOCKERS = [
    "supplied_snapshots_are_not_live_observations",
    "capture_images_not_checked_by_this_preflight",
    "configuration_digest_not_bound_by_capture_format",
    "release_archive_and_extracted_tree_not_verified",
    "deployment_boot_selection_not_qualified",
    "old_coordinator_and_workers_not_fenced",
    "post_capture_provider_events_not_reconciled",
    "permanent_archives_not_verified",
    "saved_policy_compatibility_not_qualified",
    "dispatch_release_not_implemented",
]


class Rejected(ValueError):
    pass


def reject(code):
    raise Rejected(code)


def matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def exact(value, keys, code="invalid_input_shape"):
    if not isinstance(value, dict) or set(value) != set(keys):
        reject(code)


def inventory(value, required, optional):
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - optional:
        reject("invalid_input_shape")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def clean_absolute(path):
    return isinstance(path, str) and path.startswith("/") and "\0" not in path and ".." not in path.split("/")


def read_inputs(directory):
    if not clean_absolute(directory):
        reject("unsafe_directory")
    path = Path(directory)
    for component in [*reversed(path.parents), path]:
        if stat.S_ISLNK(component.lstat().st_mode):
            reject("unsafe_directory")
    before = path.lstat()
    if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o700:
        reject("unsafe_directory")
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    directory_fd = os.open(path, flags | os.O_DIRECTORY)
    try:
        if identity(os.fstat(directory_fd)) != identity(before):
            reject("inputs_changed")
        names = set()
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                if entry.name not in FILES or len(names) >= len(FILES):
                    reject("invalid_input_inventory")
                names.add(entry.name)
        if names != set(FILES):
            reject("invalid_input_inventory")
        data, observed = {}, {}
        for name, limit in FILES.items():
            fd = os.open(name, flags, dir_fd=directory_fd)
            with os.fdopen(fd, "rb") as handle:
                info = os.fstat(handle.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                        or stat.S_IMODE(info.st_mode) != 0o600):
                    reject("unsafe_input_file")
                if not 0 < info.st_size <= limit:
                    reject("input_size_limit")
                payload = handle.read(limit + 1)
                if len(payload) != info.st_size or len(payload) > limit or identity(os.fstat(handle.fileno())) != identity(info):
                    reject("inputs_changed")
                observed[name] = identity(info)
                data[name] = payload
        if sum(map(len, data.values())) > 4 * LARGE_LIMIT:
            reject("input_size_limit")
        for name, pinned in observed.items():
            if identity(os.stat(name, dir_fd=directory_fd, follow_symlinks=False)) != pinned:
                reject("inputs_changed")
            # Timestamp resolution alone cannot detect every same-size rewrite.
            fd = os.open(name, flags, dir_fd=directory_fd)
            with os.fdopen(fd, "rb") as handle:
                if identity(os.fstat(handle.fileno())) != pinned:
                    reject("inputs_changed")
                if handle.read(FILES[name] + 1) != data[name] or identity(os.fstat(handle.fileno())) != pinned:
                    reject("inputs_changed")
        if identity(os.fstat(directory_fd)) != identity(before) or identity(path.lstat()) != identity(before):
            reject("inputs_changed")
        return data
    finally:
        os.close(directory_fd)


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            reject("duplicate_json_key")
        result[key] = value
    return result


def bounded(value, depth=0):
    if depth > 64:
        reject("input_depth_limit")
    if isinstance(value, dict):
        return 1 + sum(bounded(v, depth + 1) for v in value.values())
    if isinstance(value, list):
        return 1 + sum(bounded(v, depth + 1) for v in value)
    return 1


def document(payload):
    value = json.loads(payload, object_pairs_hook=unique,
                       parse_constant=lambda _value: reject("invalid_json"))
    if not isinstance(value, dict) or bounded(value) > 100_000:
        reject("invalid_json")
    return value


def policy(value):
    keys = {"version", "gate_policy", "lifecycle_sha256", "executor_sha256", "validation_sha256"}
    if not isinstance(value, dict) or set(value) not in (keys, keys | {"child_sha256"}):
        reject("invalid_policy")
    if (type(value["version"]) is not int or value["version"] != 1 or value["gate_policy"] != "exit-status-v1"
            or any(not matches(SHA, v) for k, v in value.items() if k.endswith("_sha256"))):
        reject("invalid_policy")


def plan(value):
    exact(value, {"version", "expected"}, "invalid_plan")
    if type(value["version"]) is not int or value["version"] != 1:
        reject("invalid_plan")
    expected = value["expected"]
    exact(expected, {"capture_id", "manifest_sha256", "service_revision", "release_sha256",
                     "workflow_sha256", "references_sha256", "recovery_plan_sha256", "policy"}, "invalid_plan")
    if (not matches(CAPTURE_ID, expected["capture_id"]) or not matches(REVISION, expected["service_revision"])
            or any(not matches(SHA, v) for k, v in expected.items() if k.endswith("_sha256"))):
        reject("invalid_plan")
    policy(expected["policy"])
    return expected


def manifest(value):
    exact(value, {"version", "kind", "capture_id", "created_at", "restore_ready", "databases", "metadata", "durability"})
    if (type(value["version"]) is not int or value["version"] != 1 or value["kind"] != "local_owner_capture"
            or not matches(CAPTURE_ID, value["capture_id"]) or not isinstance(value["created_at"], str)
            or value["restore_ready"] is not False):
        reject("invalid_capture_manifest")
    exact(value["durability"], {"files_synced", "durable_directory_synced"})
    if value["durability"]["files_synced"] is not True or type(value["durability"]["durable_directory_synced"]) is not bool:
        reject("invalid_capture_manifest")
    exact(value["databases"], {"state", "evidence"})
    for name, owner, version in [("state", "workstream_store", 3), ("evidence", "evidence_index", 1)]:
        record = value["databases"][name]
        exact(record, {"file", "identity", "bytes", "sha256", "schema"})
        exact(record["schema"], {"user_version"})
        if (record["file"] != name + ".sqlite" or record["identity"] != owner
                or type(record["bytes"]) is not int or not 0 < record["bytes"] <= 256 * LARGE_LIMIT
                or not matches(SHA, record["sha256"]) or type(record["schema"]["user_version"]) is not int
                or record["schema"]["user_version"] != version):
            reject("unsupported_capture_inventory")
    metadata = value["metadata"]
    if (not isinstance(metadata, dict) or not matches(REVISION, metadata.get("service_revision"))
            or metadata.get("service_revision_source") != "operator_pin"
            or type(metadata.get("pending_evidence_outbox")) is not int or metadata["pending_evidence_outbox"] != 0):
        reject("invalid_capture_manifest")
    policy(metadata.get("policy"))


def configuration(value, boot=False):
    optional = PUBLIC_OPTIONAL | ({"secret_ids", "secret_versions"} if boot else set())
    inventory(value, PUBLIC_REQUIRED, optional)
    if (not matches(PROJECT, value["project_id"]) or value["role"] != "coordinator"
            or not matches(REVISION, value["service_revision"]) or not matches(SHA, value["release_sha256"])):
        reject("invalid_configuration")
    pins = integration_pins(value)
    versions = value.get("coordinator_secret_versions", {})
    fingerprints = value.get("coordinator_secret_fingerprints", {})
    if (not isinstance(versions, dict) or not isinstance(fingerprints, dict)
            or set(versions) != set(pins["mapping"]) or set(fingerprints) != set(pins["mapping"])
            or any(not version(v) for v in versions.values()) or any(not matches(SHA, v) for v in fingerprints.values())):
        reject("invalid_configuration")
    return {**pins, "versions": {k: str(v) for k, v in versions.items()}, "fingerprints": fingerprints}


def integration_pins(value):
    workflow = value.get("coordinator_workflow", "pilot")
    mapping = value.get("coordinator_secret_env", {})
    webhook = value.get("enable_linear_webhook", False)
    if (workflow not in ("pilot", "linear") or type(webhook) is not bool or not isinstance(mapping, dict)
            or not set(mapping) <= ENVS or len(set(mapping.values())) != len(mapping)
            or any(not matches(NAME, key) for key in mapping.values())):
        reject("invalid_configuration")
    if (workflow == "pilot" and mapping) or (workflow == "linear" and not DEFAULT_ENVS <= set(mapping)):
        reject("invalid_configuration")
    if any(set(mapping) & pair and not pair <= set(mapping) for pair in ENV_PAIRS):
        reject("invalid_configuration")
    return {"workflow": workflow, "mapping": mapping, "webhook": webhook}


def version(value):
    return (type(value) is int or isinstance(value, str)) and matches(VERSION, str(value))


def references(value):
    inventory(value, REF_REQUIRED, REF_OPTIONAL)
    if not matches(PROJECT, value["project_id"]) or value["role"] != "coordinator" or not matches(REVISION, value["service_revision"]):
        reject("invalid_references")
    pins = integration_pins(value)
    ids, versions = value.get("secret_ids", {}), value.get("secret_versions", {})
    if not isinstance(ids, dict) or not isinstance(versions, dict) or set(ids) != set(versions):
        reject("invalid_references")
    prefix = "projects/" + value["project_id"] + "/secrets/"
    if any(not matches(NAME, k) or not isinstance(v, str) or not v.startswith(prefix)
           or not matches(NAME, v[len(prefix):]) or not version(versions[k]) for k, v in ids.items()):
        reject("invalid_references")
    if any(key not in ids for key in pins["mapping"].values()):
        reject("invalid_references")
    selected_versions = {env: str(versions[key]) for env, key in pins["mapping"].items()}
    fingerprints = {env: digest(f"{env}\n{key}\n{ids[key]}\n{versions[key]}".encode()) for env, key in pins["mapping"].items()}
    return {**pins, "versions": selected_versions, "fingerprints": fingerprints}


def release(value):
    exact(value, {"service_revision"})
    if not matches(REVISION, value["service_revision"]):
        reject("invalid_release_identity")
    return value["service_revision"]


def marker(payload):
    value = payload.decode("ascii").removesuffix("\n")
    if not matches(SHA, value):
        reject("invalid_release_identity")
    return value


def recovery_plan(value):
    exact(value, {"version", "mode", "root", "id", "workspace_roots", "manifest_sha256", "service_revision", "policy"})
    if (type(value["version"]) is not int or value["version"] != 1 or value["mode"] != "held_inspection"
            or not clean_absolute(value["root"]) or not matches(CAPTURE_ID, value["id"])
            or not isinstance(value["workspace_roots"], list) or not value["workspace_roots"]
            or not all(clean_absolute(v) for v in value["workspace_roots"])
            or not matches(SHA, value["manifest_sha256"]) or not matches(REVISION, value["service_revision"])):
        reject("invalid_recovery_plan")
    policy(value["policy"])


def compare(data):
    docs = {name: document(payload) for name, payload in data.items() if name.endswith(".json")}
    expected = plan(docs["plan.json"])
    captured = docs["capture-manifest.json"]
    manifest(captured)
    held = docs["recovery-plan.json"]
    recovery_plan(held)
    active = docs["active.json"]
    exact(active, {"service_revision", "release_sha256", "activated_at"})
    if not matches(REVISION, active["service_revision"]) or not matches(SHA, active["release_sha256"]) or not isinstance(active["activated_at"], str):
        reject("invalid_active_identity")
    public, effective, boot = (docs[name] for name in ["active-public.json", "effective-public.json", "boot-config.json"])
    active_pins, effective_pins, boot_pins = configuration(public), configuration(effective), configuration(boot, boot=True)
    active_refs, boot_refs = docs["active-references.json"], docs["boot-references.json"]
    active_ref_pins, boot_ref_pins = references(active_refs), references(boot_refs)
    boot_metadata_ref_pins = references({k: v for k, v in boot.items() if k in REF_REQUIRED | REF_OPTIONAL})
    current = data["current-release.txt"].decode("ascii").removesuffix("\n")
    match = re.fullmatch(r"/opt/factory/releases/([0-9a-f]{40})", current)
    if match is None:
        reject("invalid_current_release")
    active_release, boot_release = release(docs["active-release.json"]), release(docs["boot-release.json"])
    active_sha, boot_sha = marker(data["active-release-sha256.txt"]), marker(data["boot-release-sha256.txt"])
    issues = []

    def check(ok, code):
        if not ok:
            issues.append(code)
        return bool(ok)

    checks = {}
    checks["capture_anchors_match"] = check(
        digest(data["capture-manifest.json"]) == expected["manifest_sha256"]
        and captured["capture_id"] == expected["capture_id"]
        and captured["metadata"]["service_revision"] == expected["service_revision"]
        and captured["metadata"]["policy"] == expected["policy"], "capture_anchor_mismatch")
    checks["active_identity_match"] = check(
        active["service_revision"] == public["service_revision"] == effective["service_revision"] == active_release == match[1] == expected["service_revision"]
        and active["release_sha256"] == public["release_sha256"] == effective["release_sha256"] == active_sha == expected["release_sha256"]
        and public["project_id"] == effective["project_id"] == active_refs["project_id"]
        and active_refs["service_revision"] == active_release, "active_identity_mismatch")
    checks["active_configuration_match"] = check(active_pins == effective_pins == active_ref_pins, "active_configuration_mismatch")
    checks["boot_identity_match"] = check(
        boot["service_revision"] == boot_release == boot_refs["service_revision"]
        and boot["release_sha256"] == boot_sha and boot["project_id"] == boot_refs["project_id"], "boot_identity_mismatch")
    checks["boot_configuration_match"] = check(boot_pins == boot_ref_pins == boot_metadata_ref_pins, "boot_configuration_mismatch")
    same_revision = check(boot["service_revision"] == active["service_revision"], "boot_active_revision_mismatch")
    check(boot["release_sha256"] == active["release_sha256"], "boot_active_release_mismatch")
    check(boot["project_id"] == public["project_id"], "boot_active_project_mismatch")
    checks["selected_reference_pins_match"] = check(active_pins == boot_pins, "same_revision_configuration_changed" if same_revision else "boot_active_configuration_mismatch")
    checks["workflow_pins_match"] = check(
        digest(data["active-WORKFLOW.md"]) == digest(data["boot-WORKFLOW.md"]) == expected["workflow_sha256"], "workflow_digest_mismatch")
    checks["accepted_references_match"] = check(digest(data["active-references.json"]) == expected["references_sha256"], "reference_digest_mismatch")
    checks["recovery_plan_match"] = check(
        digest(data["recovery-plan.json"]) == expected["recovery_plan_sha256"]
        and held["id"] == expected["capture_id"] and held["manifest_sha256"] == expected["manifest_sha256"]
        and held["service_revision"] == expected["service_revision"] and held["policy"] == expected["policy"], "recovery_plan_anchor_mismatch")
    return {
        "version": 1, "status": "mismatch" if issues else "aligned_but_held",
        "dispatch_allowed": False, "restore_ready": False,
        "transition": "same_revision" if same_revision else "pending_boot_transition",
        "capture_id": expected["capture_id"],
        "revisions": {"capture": expected["service_revision"], "active": active["service_revision"], "boot": boot["service_revision"]},
        "checks": checks, "issues": sorted(set(issues)), "blockers": BLOCKERS,
        "input_sha256": {name: digest(payload) for name, payload in data.items()},
        "reference_snapshots_equal": active_refs == boot_refs,
    }


def preflight(directory):
    """Compare bounded, supplied nonsecret snapshots; return no runtime authority."""
    try:
        return compare(read_inputs(directory))
    except Rejected as exc:
        reason = str(exc)
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        reason = "invalid_or_unreadable_input"
    return {"version": 1, "status": "rejected", "reason": reason,
            "dispatch_allowed": False, "restore_ready": False}


class Parser(argparse.ArgumentParser):
    def error(self, _message):
        print(json.dumps({"version": 1, "status": "rejected", "reason": "invalid_arguments",
                          "dispatch_allowed": False, "restore_ready": False}))
        raise SystemExit(1)


def main(argv=None):
    parser = Parser(description=__doc__)
    parser.add_argument("--directory", required=True)
    args = parser.parse_args(argv)
    report = preflight(args.directory)
    print(json.dumps(report, sort_keys=True))
    return 2 if report["status"] == "aligned_but_held" else 1


if __name__ == "__main__":
    sys.exit(main())
