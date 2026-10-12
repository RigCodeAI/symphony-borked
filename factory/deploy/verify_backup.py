#!/usr/bin/env python3
"""Read-only checks for a staged v1 backup; never installs or authorizes restore."""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time

MAX_DATABASE_BYTES = 256 * 1024 * 1024
HEX256 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,120}\.sqlite")
CONFIG_ONLY_NOTE = "This deployed pilot has no durable runtime SQLite database yet."

# Canonical merged migrations, not a new state format or a migration runner.
STATE_SCHEMA = (
    "CREATE TABLE runs (run_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, payload BLOB NOT NULL);"
    "CREATE TABLE events (event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, payload BLOB NOT NULL);"
    "CREATE TABLE stage_attempts (run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY (run_id, attempt_id));"
    "CREATE TABLE operations (operation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, payload BLOB NOT NULL);",
    "CREATE TABLE linear_events (delivery_id TEXT PRIMARY KEY, payload BLOB NOT NULL);"
    "CREATE TABLE linear_tasks (issue_id TEXT PRIMARY KEY, payload BLOB NOT NULL);",
    "CREATE TABLE evidence_outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, snapshot BLOB NOT NULL, event BLOB NOT NULL);",
)
EVIDENCE_SCHEMA = (
    "CREATE TABLE evidence_runs (run_id TEXT PRIMARY KEY, task_key TEXT NOT NULL, snapshot TEXT NOT NULL);"
    "CREATE TABLE evidence_events (seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL, event_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(run_id,event_id));"
    "CREATE TABLE evidence_artifacts (run_id TEXT NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(run_id,name));"
)
RESTORE_BLOCKERS = [
    "v1 does not record a consistent capture barrier across stores and evidence outbox",
    "workflow, reference pins, release availability and archive bundles are outside this manifest",
    "destination metadata does not prove the coordinator is stopped or workers are fenced",
    "pinned payload compatibility, waits and remote side effects require owner reconciliation",
]


class InvalidBackup(ValueError):
    pass


class VerifierParser(argparse.ArgumentParser):
    def error(self, _message):
        # Argument values may contain private input. Keep rejection structured
        # and distinct from exit 2's passed-but-blocked inspection report.
        print(json.dumps({"status": "rejected", "restore_ready": False,
                          "reason": "invalid verifier arguments; see --help"}))
        raise SystemExit(1)


def _no_symlinks(path):
    if not path.is_absolute() or ".." in path.parts:
        raise InvalidBackup("paths must be absolute without parent traversal")
    for component in [*reversed(path.parents), path]:
        if stat.S_ISLNK(component.lstat().st_mode):
            raise InvalidBackup("path contains a symlink")


def _directory(path, uid, gid=None, private=True):
    _no_symlinks(path)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != uid or (gid is not None and info.st_gid != gid):
        raise InvalidBackup("directory type or ownership differs from the supplied plan")
    if info.st_mode & (0o077 if private else 0o027) or info.st_mode & 0o700 != 0o700:
        raise InvalidBackup("directory permissions are unsafe")


def _read(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                or info.st_mode & 0o077 or info.st_size > limit):
            raise InvalidBackup("staged file type, ownership, permissions or size is unsafe")
        data = handle.read(limit + 1)
        if len(data) > limit:
            raise InvalidBackup("staged file exceeds its size limit")
        return data


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidBackup("JSON contains duplicate keys")
        result[key] = value
    return result


def _json(data):
    value = json.loads(data, object_pairs_hook=_unique)
    if not isinstance(value, dict):
        raise InvalidBackup("JSON document must be an object")
    return value


def _digest(data, expected):
    if not isinstance(expected, str) or not HEX256.fullmatch(expected) or hashlib.sha256(data).hexdigest() != expected:
        raise InvalidBackup("checksum differs from its trusted input")


def _schema(db):
    result = {}
    for kind, name, sql in db.execute("SELECT type,name,sql FROM sqlite_master"):
        if not name.startswith("sqlite_"):
            normalized = re.sub(r"\s*([(),])\s*", r"\1", re.sub(r"\s+", " ", sql.strip()).lower()) if sql else None
            result[name] = (kind, normalized)
    return result


def _database(data, role, version):
    if len(data) < 100 or data[:16] != b"SQLite format 3\x00" or data[18:20] != b"\x01\x01":
        raise InvalidBackup("database must be a standalone SQLite backup without WAL dependence")
    if not hasattr(sqlite3.Connection, "deserialize"):
        raise InvalidBackup("verification requires SQLite deserialization support in Python 3.11 or newer")
    with closing(sqlite3.connect(":memory:")) as db, closing(sqlite3.connect(":memory:")) as expected:
        # Inspect the exact hashed bytes in memory. No disk database, sidecar,
        # migration, external-term decoding or runtime code is opened/executed.
        db.deserialize(data)
        db.execute("PRAGMA temp_store=MEMORY")
        db.execute("PRAGMA query_only=ON")
        deadline = time.monotonic() + 30
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
        if db.execute("PRAGMA user_version").fetchone() != (version,):
            raise InvalidBackup("database schema version differs from the supplied plan")
        expected.executescript("".join(STATE_SCHEMA[:version]) if role == "state" else EVIDENCE_SCHEMA)
        if _schema(db) != _schema(expected):
            raise InvalidBackup("database DDL differs from the supported merged schema")
        if db.execute("PRAGMA integrity_check(1)").fetchone() != ("ok",):
            raise InvalidBackup("SQLite integrity check failed")


def verify(directory, manifest_sha256, public_sha256, revision, state_names,
           state_version, include_evidence, destination, owner_uid, owner_gid):
    directory, destination = Path(directory), Path(destination)
    if (not isinstance(revision, str) or not REVISION.fullmatch(revision)
            or not isinstance(state_names, list)
            or any(not isinstance(name, str) or not NAME.fullmatch(name) or name == "evidence-index.sqlite" for name in state_names)
            or len(set(state_names)) != len(state_names)
            or (state_names and (type(state_version) is not int or state_version not in (1, 2, 3)))
            or (not state_names and state_version is not None)
            or owner_uid < 0 or owner_gid < 0):
        raise InvalidBackup("invalid trusted revision, inventory, schema or owner plan")
    _directory(directory, os.geteuid())
    manifest_bytes = _read(directory / "manifest.json", 1024 * 1024)
    _digest(manifest_bytes, manifest_sha256)
    manifest = _json(manifest_bytes)
    if (set(manifest) - {"version", "service_revision", "databases", "note"}
            or type(manifest.get("version")) is not int or manifest["version"] != 1
            or manifest.get("service_revision") != revision or not isinstance(manifest.get("databases"), list)):
        raise InvalidBackup("unsupported or mismatched backup manifest")
    inventory = {name: ("state", "state/" + name, state_version) for name in state_names}
    if include_evidence:
        inventory["evidence-index.sqlite"] = ("evidence", "evidence/index.sqlite", 1)
    if (not inventory and manifest.get("note") != CONFIG_ONLY_NOTE) or (inventory and "note" in manifest):
        raise InvalidBackup("configuration-only note differs from the supported v1 backup form")
    entries = {}
    for entry in manifest["databases"]:
        if (not isinstance(entry, dict) or set(entry) - {"name", "sha256", "restore_path"}
                or not isinstance(entry.get("name"), str) or entry["name"] not in inventory
                or entry["name"] in entries):
            raise InvalidBackup("duplicate, unexpected or invalid database entry")
        name = entry["name"]
        if inventory[name][0] == "state" and "restore_path" in entry:
            raise InvalidBackup("v1 state entries must not override their destination")
        if entry.get("restore_path", "state/" + name) != inventory[name][1]:
            raise InvalidBackup("restore path differs from the fixed destination mapping")
        entries[name] = entry
    if set(entries) != set(inventory) or {path.name for path in directory.iterdir()} != set(inventory) | {"manifest.json", "public.json"}:
        raise InvalidBackup("backup files or manifest do not match the trusted complete inventory")
    public_bytes = _read(directory / "public.json", 64 * 1024)
    _digest(public_bytes, public_sha256)
    public = _json(public_bytes)
    if public.get("role") != "coordinator" or public.get("service_revision") != revision:
        raise InvalidBackup("public configuration differs from the expected coordinator revision")
    if public.get("coordinator_workflow") == "linear" and (not state_names or state_version < 2):
        raise InvalidBackup("Linear configuration requires a state database with schema 2 or newer")
    _no_symlinks(destination)
    root = destination.lstat()
    if not stat.S_ISDIR(root.st_mode) or root.st_uid not in (0, os.geteuid()) or root.st_mode & 0o022:
        raise InvalidBackup("destination root must be an existing trusted directory")
    if state_names:
        _directory(destination / "state", owner_uid, owner_gid, private=False)
    if include_evidence:
        _directory(destination / "evidence", owner_uid, owner_gid)
    checked = []
    for name, (role, relative, version) in sorted(inventory.items()):
        target = destination / relative
        if target.exists() or target.is_symlink():
            raise InvalidBackup("destination file already exists; this verifier never authorizes overwriting it")
        data = _read(directory / name, MAX_DATABASE_BYTES)
        _digest(data, entries[name].get("sha256"))
        _database(data, role, version)
        checked.append({"name": name, "schema_version": version, "destination": relative, "sha256": entries[name]["sha256"]})
    blockers = RESTORE_BLOCKERS + ([] if state_names else ["declared backup contains no workstream database"])
    return {"status": "offline_checks_passed", "restore_ready": False, "restore_blockers": blockers,
            "service_revision": revision, "manifest_sha256": manifest_sha256, "public_sha256": public_sha256,
            "planned_destination_owner": {"uid": owner_uid, "gid": owner_gid},
            "checked_destination_directories": (["state"] if state_names else []) + (["evidence"] if include_evidence else []),
            "databases": checked}


def main():
    parser = VerifierParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--public-sha256", required=True)
    parser.add_argument("--service-revision", required=True)
    state = parser.add_mutually_exclusive_group(required=True)
    state.add_argument("--state-database", action="append")
    state.add_argument("--no-state-databases", action="store_true")
    parser.add_argument("--state-schema", type=int, choices=(1, 2, 3))
    parser.add_argument("--evidence-index", action="store_true")
    parser.add_argument("--destination-root", type=Path, required=True)
    parser.add_argument("--owner-uid", type=int, required=True)
    parser.add_argument("--owner-gid", type=int, required=True)
    args = parser.parse_args()
    if bool(args.state_database) != (args.state_schema is not None):
        parser.error("--state-schema is required only with --state-database")
    try:
        result = verify(args.directory, args.manifest_sha256, args.public_sha256, args.service_revision,
                        args.state_database or [], args.state_schema, args.evidence_index,
                        args.destination_root, args.owner_uid, args.owner_gid)
    except Exception as exc:
        # Untrusted JSON/SQLite errors can contain payloads. Emit only our static diagnostics.
        reason = str(exc) if isinstance(exc, InvalidBackup) else "unreadable or invalid backup input"
        print(json.dumps({"status": "rejected", "restore_ready": False, "reason": reason}))
        return 1
    print(json.dumps(result, sort_keys=True))
    # File checks cannot authorize installation: keep shell callers fail-closed.
    return 2


if __name__ == "__main__":
    sys.exit(main())
