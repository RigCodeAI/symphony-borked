#!/usr/bin/env python3
"""Upload immutable, coordinator-owned evidence bundles to Cloud Storage."""

import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import stat
import time
import unicodedata

MAX_ARTIFACT_BYTES = 10 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024
MAX_BATCH_BYTES = 32 * 1024 * 1024
REQUIRED_ARTIFACTS = {"snapshot.json", "events.jsonl", "transcript.txt"}
REVISION_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
OPTIONAL_ARTIFACT_PATTERN = re.compile(r"artifact-([0-9a-f]{64})\.(?:txt|json)\Z")


def _default_allowed_uids():
    result = {0}
    try:
        result.add(pwd.getpwnam("factory-coordinator").pw_uid)
    except KeyError:
        pass
    return result


def _directory_info(fd, allowed_uids, exact_mode=None):
    info = os.fstat(fd)
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in allowed_uids:
        raise ValueError("evidence directory has an unexpected owner or type")
    if exact_mode is not None and mode != exact_mode:
        raise ValueError("evidence root must have mode 0700")
    if exact_mode is None and mode & 0o022:
        raise ValueError("evidence directory must not be writable by group or other users")
    return info


def _open_dir_at(parent_fd, name, allowed_uids, exact_mode=None):
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    try:
        _directory_info(fd, allowed_uids, exact_mode=exact_mode)
        return fd
    except Exception:
        os.close(fd)
        raise


def _open_absolute_directory(path):
    requested = Path(path)
    if not requested.is_absolute():
        requested = Path(os.getcwd()) / requested
    if ".." in requested.parts:
        raise ValueError("evidence root path must not contain parent traversal")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open("/", directory_flags)
    try:
        for component in requested.parts[1:]:
            child_fd = os.open(component, directory_flags, dir_fd=fd)
            os.close(fd)
            fd = child_fd
        return fd
    except Exception:
        os.close(fd)
        raise


def _read_file_at(directory_fd, name, allowed_uids, limit, expected_gid=None, exact_mode=None):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or
                before.st_uid not in allowed_uids or stat.S_IMODE(before.st_mode) & 0o022 or
                before.st_size > limit):
            raise ValueError("evidence file has an unexpected owner, type, mode, or size")
        if expected_gid is not None and before.st_gid != expected_gid:
            raise ValueError("evidence receipt has an unexpected group")
        if exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode:
            raise ValueError("evidence receipt has unexpected permissions")
        chunks = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(fd)
        if len(data) > limit or len(data) != before.st_size or after.st_size != before.st_size:
            raise ValueError("evidence file changed or exceeded its size limit while reading")
        return data
    finally:
        os.close(fd)


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("evidence JSON contains duplicate keys")
        result[key] = value
    return result


def _valid_task_id(value):
    if not isinstance(value, str) or not value:
        return False
    if any(unicodedata.category(character) == "Cc" for character in value):
        return False
    try:
        return len(value.encode("utf-8")) <= 512
    except UnicodeEncodeError:
        return False


def _decode_manifest(data):
    try:
        manifest = json.loads(data.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("evidence manifest is not valid UTF-8 JSON") from None
    if not isinstance(manifest, dict) or set(manifest) != {"version", "task_id", "run_id", "artifacts"}:
        raise ValueError("evidence manifest has an unsupported shape")
    if type(manifest["version"]) is not int or manifest["version"] != 1:
        raise ValueError("evidence manifest version is unsupported")
    if not _valid_task_id(manifest["task_id"]):
        raise ValueError("evidence manifest task id is invalid")
    if not isinstance(manifest["run_id"], str) or not RUN_ID_PATTERN.fullmatch(manifest["run_id"]):
        raise ValueError("evidence manifest run id is unsafe")
    if not isinstance(manifest["artifacts"], list):
        raise ValueError("evidence manifest artifacts must be a list")

    names = set()
    for artifact in manifest["artifacts"]:
        if not isinstance(artifact, dict) or set(artifact) != {"name", "sha256", "bytes"}:
            raise ValueError("evidence manifest artifact has an unsupported shape")
        name = artifact["name"]
        digest = artifact["sha256"]
        size = artifact["bytes"]
        if not isinstance(name, str) or not (
                name in REQUIRED_ARTIFACTS or OPTIONAL_ARTIFACT_PATTERN.fullmatch(name)):
            raise ValueError("evidence manifest contains an unsafe artifact name")
        if name in names:
            raise ValueError("evidence manifest contains a duplicate artifact")
        names.add(name)
        if not isinstance(digest, str) or not REVISION_PATTERN.fullmatch(digest):
            raise ValueError("evidence manifest contains an invalid artifact checksum")
        if type(size) is not int or size < 0 or size > MAX_ARTIFACT_BYTES:
            raise ValueError("evidence manifest artifact size is outside the allowed range")
        optional_match = OPTIONAL_ARTIFACT_PATTERN.fullmatch(name)
        if optional_match and optional_match.group(1) != digest:
            raise ValueError("optional evidence artifact name does not match its checksum")
    if not REQUIRED_ARTIFACTS.issubset(names):
        raise ValueError("evidence manifest is missing a required artifact")
    return manifest


def _load_bundle(bundles_fd, revision, allowed_uids):
    bundle_fd = _open_dir_at(bundles_fd, revision, allowed_uids)
    try:
        try:
            manifest_data = _read_file_at(bundle_fd, "manifest.json", allowed_uids, MAX_MANIFEST_BYTES)
        except FileNotFoundError:
            # Evidence.export writes payload files first and commits the manifest last.
            return None
        if hashlib.sha256(manifest_data).hexdigest() != revision:
            raise ValueError("evidence manifest checksum does not match its directory")
        manifest = _decode_manifest(manifest_data)

        expected_names = {"manifest.json", *(item["name"] for item in manifest["artifacts"])}
        actual_names = set()
        with os.scandir(bundle_fd) as entries:
            for entry in entries:
                actual_names.add(entry.name)
                entry_info = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(entry_info.st_mode):
                    raise ValueError("evidence bundle contains a non-regular entry")
        if actual_names != expected_names:
            raise ValueError("evidence bundle files do not match its manifest")

        payloads = {}
        total = len(manifest_data)
        for artifact in manifest["artifacts"]:
            data = _read_file_at(bundle_fd, artifact["name"], allowed_uids, MAX_ARTIFACT_BYTES)
            digest = hashlib.sha256(data).hexdigest()
            if len(data) != artifact["bytes"] or digest != artifact["sha256"]:
                raise ValueError("evidence artifact checksum or size does not match its manifest")
            total += len(data)
            if total > MAX_BATCH_BYTES:
                raise ValueError("evidence bundle exceeds the total size limit")
            payloads[artifact["name"]] = data
        payloads["manifest.json"] = manifest_data
        return manifest, manifest_data, payloads
    finally:
        os.close(bundle_fd)


def _ensure_receipts_dir(root_fd, euid, group_id):
    try:
        os.mkdir("receipts", 0o750, dir_fd=root_fd)
    except FileExistsError:
        pass
    fd = os.open("receipts", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("evidence receipts path must be a directory")
        if info.st_uid != euid or info.st_gid != group_id or stat.S_IMODE(info.st_mode) != 0o750:
            os.fchown(fd, euid, group_id)
            os.fchmod(fd, 0o750)
            info = os.fstat(fd)
        if (info.st_uid != euid or info.st_gid != group_id or
                stat.S_IMODE(info.st_mode) != 0o750):
            raise ValueError("evidence receipts directory has unexpected ownership or mode")
        return fd
    except Exception:
        os.close(fd)
        raise


def _read_existing_receipt(receipts_fd, filename, euid, group_id):
    try:
        data = _read_file_at(receipts_fd, filename, {euid}, MAX_MANIFEST_BYTES,
                             expected_gid=group_id, exact_mode=0o640)
    except FileNotFoundError:
        return None
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("existing evidence receipt is invalid") from None


def _write_receipt(receipts_fd, filename, receipt, group_id):
    data = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary = "." + filename + "." + secrets.token_hex(8) + ".tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o640,
                 dir_fd=receipts_fd)
    try:
        os.fchown(fd, os.geteuid(), group_id)
        os.fchmod(fd, 0o640)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    except Exception:
        try:
            os.unlink(temporary, dir_fd=receipts_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)
    os.replace(temporary, filename, src_dir_fd=receipts_fd, dst_dir_fd=receipts_fd)
    os.fsync(receipts_fd)


def _compare_existing_receipt(receipt, manifest, revision, manifest_sha256):
    if receipt is None or receipt.get("status") != "verified":
        return
    if (receipt.get("version") != 1 or receipt.get("run_id") != manifest["run_id"] or
            receipt.get("revision") != revision or
            receipt.get("artifacts") != manifest["artifacts"] or
            receipt.get("manifest_sha256") != manifest_sha256):
        raise ValueError("existing verified receipt does not match the current evidence bundle")


def _archive_bundle(cloud, bucket, revision, manifest, manifest_data, payloads, receipts_fd, euid, group_id,
                    reverify=False):
    run_id = manifest["run_id"]
    filename = revision + ".json"
    existing = _read_existing_receipt(receipts_fd, filename, euid, group_id)
    manifest_sha256 = hashlib.sha256(manifest_data).hexdigest()
    _compare_existing_receipt(existing, manifest, revision, manifest_sha256)
    if existing is not None and existing.get("status") == "verified" and not reverify:
        return existing

    prefix = f"evidence/v1/{run_id}/{revision}/"
    # Upload the manifest last so a visible manifest identifies a complete set.
    for name in sorted(name for name in payloads if name != "manifest.json") + ["manifest.json"]:
        expected = payloads[name]
        cloud_name = prefix + name
        cloud.put(bucket, cloud_name, expected)
        actual = cloud.object(bucket, cloud_name)
        if actual != expected:
            raise RuntimeError("evidence cloud readback did not match the uploaded bytes")

    verified_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    receipt = {
        "version": 1,
        "run_id": run_id,
        "revision": revision,
        "status": "verified",
        "artifacts": manifest["artifacts"],
        "manifest_sha256": manifest_sha256,
        "verified_at": verified_at,
    }
    _write_receipt(receipts_fd, filename, receipt, group_id)
    return receipt


def archive_evidence(cloud, root=Path("/srv/factory/evidence"), *, allowed_uids=None,
                     receipt_gid=None, reverify=False):
    """Validate and upload all durable evidence bundles under ``root``.

    ``allowed_uids`` is an explicit test seam. Production accepts only root and
    the factory-coordinator account as owners of bundle data.
    """
    try:
        root_fd = _open_absolute_directory(root)
    except FileNotFoundError:
        return []
    except OSError:
        raise ValueError("evidence root path contains a symlink or inaccessible component") from None

    owners = set(_default_allowed_uids() if allowed_uids is None else allowed_uids)
    if receipt_gid is None:
        try:
            receipt_gid = pwd.getpwnam("factory-coordinator").pw_gid
        except KeyError:
            receipt_gid = os.getegid()
    try:
        _directory_info(root_fd, owners, exact_mode=0o700)
        try:
            bundles_fd = _open_dir_at(root_fd, "bundles", owners)
        except FileNotFoundError:
            return []
        try:
            revisions = []
            invalid_entries = False
            with os.scandir(bundles_fd) as entries:
                for entry in entries:
                    if not REVISION_PATTERN.fullmatch(entry.name):
                        invalid_entries = True
                        continue
                    revisions.append(entry.name)

            if not revisions and not invalid_entries:
                return []
            receipts_fd = _ensure_receipts_dir(root_fd, os.geteuid(), receipt_gid)
            try:
                receipts = []
                bucket = cloud.config["archive_bucket"]
                failed = invalid_entries
                for revision in sorted(revisions):
                    run_id = None
                    try:
                        loaded = _load_bundle(bundles_fd, revision, owners)
                        if loaded is None:
                            continue
                        manifest, manifest_data, payloads = loaded
                        run_id = manifest["run_id"]
                        receipts.append(_archive_bundle(
                            cloud, bucket, revision, manifest, manifest_data, payloads,
                            receipts_fd, os.geteuid(), receipt_gid, reverify=reverify))
                    except Exception as exc:
                        failure = {
                            "version": 1,
                            "revision": revision,
                            "status": "failed",
                            "type": type(exc).__name__,
                        }
                        if run_id is not None:
                            failure["run_id"] = run_id
                        _write_receipt(receipts_fd, revision + ".json", failure, receipt_gid)
                        failed = True
                if failed:
                    raise RuntimeError("evidence archive failed") from None
                return receipts
            finally:
                os.close(receipts_fd)
        finally:
            os.close(bundles_fd)
    finally:
        os.close(root_fd)
