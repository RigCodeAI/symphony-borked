#!/usr/bin/env python3
"""Root broker and forced-SSH transport for contained worker operations.

The only worker-facing command surface is the literal
``factory-operation rpc ...`` / ``factory-operation stream ...`` grammar.  The
command is carried by sshd as ``SSH_ORIGINAL_COMMAND`` and is parsed here as
data; it is never passed to a shell or executed.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import errno
import grp
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import pwd
import re
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
from typing import Any, BinaryIO, Mapping


CONTROL_SOCKET = Path("/run/factory-operations/control.sock")
DAEMON_CONFIG = Path("/etc/factory/worker-operations.json")
OPERATION_ROOT = Path("/var/lib/factory-operations/gates")
WORKSPACE_ROOT = Path("/srv/factory/contained-workspaces")
WORKER_HOME = Path("/srv/factory/homes/factory-worker")
QUALIFICATION_FILE = Path("/etc/factory/worker-operations-qualified.json")

MAX_COMMAND_BYTES = 256 * 1024
MAX_JSON_BYTES = 128 * 1024
MAX_RESPONSE_BYTES = 128 * 1024
MAX_CONFIG_BYTES = 32 * 1024
MAX_MANIFEST_BYTES = 64 * 1024
MAX_ARGV_BYTES = 32 * 1024
MAX_ARGUMENT_BYTES = 4096
MAX_CANDIDATE_BUNDLE_BYTES = 64 * 1024 * 1024
CANDIDATE_BUNDLE_CHUNK_BYTES = 64 * 1024
MAX_STREAM_BUFFER = 1024 * 1024
STREAM_TIMEOUT_SECONDS = 60 * 60
TERMINAL_STREAM_RETENTION_SECONDS = 60.0
RELEASE_WAIT_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 10

_OP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*$")
_HEX32_RE = re.compile(r"^[0-9a-f]{32}$")
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_BOOT_ID_RE = _HEX32_RE
_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")

IDENTITY_KEYS = frozenset({
    "kind", "operation_id", "machine_id", "boot_id", "unit",
    "invocation_id", "control_group", "request_sha256",
})
RELEASE_KEYS = frozenset({"service_revision", "release_sha256"})
REQUEST_KEYS = frozenset({"operation_id", "argv", "workspace"})


class ProtocolError(ValueError):
    """An untrusted command/request does not match the fixed protocol."""


def _reject_constant(value: str) -> None:
    raise ProtocolError(f"invalid JSON constant: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("duplicate JSON key")
        result[key] = value
    return result


def decode_json_object(raw: bytes, *, maximum: int = MAX_JSON_BYTES) -> dict[str, Any]:
    if len(raw) > maximum:
        raise ProtocolError("JSON request is too large")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ProtocolError("invalid JSON request") from error
    if not isinstance(value, dict):
        raise ProtocolError("request must be a JSON object")
    return value


def _compact_json(value: Any, *, maximum: int = MAX_JSON_BYTES) -> bytes:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise ProtocolError("value cannot be encoded") from error
    if len(raw) > maximum:
        raise ProtocolError("encoded value is too large")
    return raw


def _require_exact_keys(value: dict[str, Any], expected: frozenset[str], what: str) -> None:
    if set(value) != expected:
        raise ProtocolError(f"invalid {what} fields")


def validate_operation_id(value: Any) -> str:
    if not isinstance(value, str) or not _OP_ID_RE.fullmatch(value):
        raise ProtocolError("invalid operation id")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ProtocolError("invalid operation id")
    return value


def validate_release(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ProtocolError("invalid release identity")
    _require_exact_keys(value, RELEASE_KEYS, "release identity")
    revision = value.get("service_revision")
    release_sha256 = value.get("release_sha256")
    if not isinstance(revision, str) or not _HEX40_RE.fullmatch(revision):
        raise ProtocolError("invalid release revision")
    if not isinstance(release_sha256, str) or not _HEX64_RE.fullmatch(release_sha256):
        raise ProtocolError("invalid release digest")
    return {"service_revision": revision, "release_sha256": release_sha256}


def validate_identity(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ProtocolError("invalid operation identity")
    _require_exact_keys(value, IDENTITY_KEYS, "operation identity")
    identity = value
    if identity.get("kind") != "systemd-unit":
        raise ProtocolError("invalid operation identity")
    operation_id = validate_operation_id(identity.get("operation_id"))
    machine_id = identity.get("machine_id")
    boot_id = identity.get("boot_id")
    invocation_id = identity.get("invocation_id")
    request_sha256 = identity.get("request_sha256")
    if not isinstance(machine_id, str) or not _HEX32_RE.fullmatch(machine_id):
        raise ProtocolError("invalid operation identity")
    if not isinstance(boot_id, str) or not _BOOT_ID_RE.fullmatch(boot_id):
        raise ProtocolError("invalid operation identity")
    if not isinstance(invocation_id, str) or not _HEX32_RE.fullmatch(invocation_id):
        raise ProtocolError("invalid operation identity")
    if not isinstance(request_sha256, str) or not _HEX64_RE.fullmatch(request_sha256):
        raise ProtocolError("invalid operation identity")
    expected_unit = f"factory-operation-{hashlib.sha256(operation_id.encode('utf-8')).hexdigest()}.service"
    if identity.get("unit") != expected_unit or identity.get("control_group") != f"/system.slice/{expected_unit}":
        raise ProtocolError("invalid operation identity")
    for key in IDENTITY_KEYS:
        if not isinstance(identity.get(key), str):
            raise ProtocolError("invalid operation identity")
    return {key: identity[key] for key in sorted(IDENTITY_KEYS)}


def validate_prepare_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError("invalid operation request")
    _require_exact_keys(value, REQUEST_KEYS, "operation request")
    operation_id = validate_operation_id(value.get("operation_id"))
    argv = value.get("argv")
    workspace = value.get("workspace")
    if not isinstance(argv, list) or not argv or len(argv) > 128:
        raise ProtocolError("invalid operation argv")
    argv_bytes = 0
    for argument in argv:
        if not isinstance(argument, str) or not argument or "\x00" in argument:
            raise ProtocolError("invalid operation argv")
        size = len(argument.encode("utf-8", errors="strict"))
        if size > MAX_ARGUMENT_BYTES:
            raise ProtocolError("invalid operation argv")
        argv_bytes += size
    if argv_bytes > MAX_ARGV_BYTES:
        raise ProtocolError("invalid operation argv")
    try:
        workspace_size = len(workspace.encode("utf-8", errors="strict")) if isinstance(workspace, str) else 0
    except UnicodeEncodeError as error:
        raise ProtocolError("invalid operation workspace") from error
    if (not isinstance(workspace, str) or not workspace.startswith("/")
            or workspace_size > 4096 or "\x00" in workspace):
        raise ProtocolError("invalid operation workspace")
    return {"operation_id": operation_id, "argv": list(argv), "workspace": workspace}


def validate_rpc_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError("invalid RPC request")
    action = value.get("action")
    if action == "capabilities":
        _require_exact_keys(value, frozenset({"action"}), "capabilities request")
        return {"action": action}
    if action == "prepare":
        _require_exact_keys(value, frozenset({"action", "request", "expected_release"}), "prepare request")
        return {
            "action": action,
            "request": validate_prepare_request(value.get("request")),
            "expected_release": validate_release(value.get("expected_release")),
        }
    if action in {"release", "status", "stop"}:
        _require_exact_keys(value, frozenset({"action", "identity", "expected_release"}), "operation request")
        return {
            "action": action,
            "identity": validate_identity(value.get("identity")),
            "expected_release": validate_release(value.get("expected_release")),
        }
    if action == "candidate_bundle":
        expected = frozenset({
            "action", "identity", "expected_release", "input_sha256", "offset",
            "expected_sha256", "expected_bytes",
        })
        _require_exact_keys(value, expected, "candidate bundle request")
        input_sha256 = value.get("input_sha256")
        expected_sha256 = value.get("expected_sha256")
        expected_bytes = value.get("expected_bytes")
        offset = value.get("offset")
        if not isinstance(input_sha256, str) or not _HEX64_RE.fullmatch(input_sha256):
            raise ProtocolError("invalid candidate bundle identity")
        if not isinstance(expected_sha256, str) or not _HEX64_RE.fullmatch(expected_sha256):
            raise ProtocolError("invalid candidate bundle digest")
        if type(expected_bytes) is not int or not 1 <= expected_bytes <= MAX_CANDIDATE_BUNDLE_BYTES:
            raise ProtocolError("invalid candidate bundle size")
        if type(offset) is not int or offset < 0 or offset > expected_bytes:
            raise ProtocolError("invalid candidate bundle offset")
        if offset != expected_bytes and offset % CANDIDATE_BUNDLE_CHUNK_BYTES != 0:
            raise ProtocolError("invalid candidate bundle offset")
        return {
            "action": action,
            "identity": validate_identity(value.get("identity")),
            "expected_release": validate_release(value.get("expected_release")),
            "input_sha256": input_sha256,
            "offset": offset,
            "expected_sha256": expected_sha256,
            "expected_bytes": expected_bytes,
        }
    raise ProtocolError("unsupported RPC action")


def _decode_base64url(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ProtocolError("invalid encoded request")
    try:
        encoded = value.encode("ascii")
        padded = encoded + b"=" * ((4 - len(encoded) % 4) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise ProtocolError("invalid encoded request") from error
    if len(raw) > MAX_JSON_BYTES:
        raise ProtocolError("encoded request is too large")
    return raw


def parse_ssh_original_command(command: Any) -> tuple[str, dict[str, Any]]:
    """Parse only the forced-SSH command grammar, with no shell evaluation."""
    if not isinstance(command, str) or not command or not command.isascii():
        raise ProtocolError("invalid SSH command")
    try:
        command_bytes = command.encode("ascii")
    except UnicodeEncodeError as error:
        raise ProtocolError("invalid SSH command") from error
    if len(command_bytes) > MAX_COMMAND_BYTES or "\n" in command or "\r" in command or "\x00" in command:
        raise ProtocolError("invalid SSH command")
    parts = command.split(" ")
    if len(parts) != 3 or parts[0] != "factory-operation" or parts[1] not in {"rpc", "stream"}:
        raise ProtocolError("invalid SSH command")
    decoded = decode_json_object(_decode_base64url(parts[2]))
    if parts[1] == "rpc":
        return "rpc", validate_rpc_request(decoded)
    _require_exact_keys(decoded, frozenset({"identity", "expected_release"}), "stream request")
    return "stream", {
        "identity": validate_identity(decoded.get("identity")),
        "expected_release": validate_release(decoded.get("expected_release")),
    }


def encode_ssh_command(kind: str, request: dict[str, Any]) -> str:
    """Test/client helper that emits the canonical forced-command token."""
    if kind not in {"rpc", "stream"}:
        raise ProtocolError("unsupported command")
    raw = _compact_json(request)
    token = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    command = f"factory-operation {kind} {token}"
    if len(command.encode("ascii")) > MAX_COMMAND_BYTES:
        raise ProtocolError("encoded command is too large")
    return command


@dataclass(frozen=True)
class BrokerConfig:
    socket_path: Path
    records_dir: Path
    operation_dir: Path
    workspace_root: Path
    wrapper_path: Path
    worker_home: Path
    cache_paths: tuple[Path, ...]
    machine_id_path: Path
    boot_id_path: Path
    worker_user: str
    worker_group: str
    control_user: str
    expected_machine_id: str
    service_revision: str
    release_sha256: str
    candidate_producer_command_sha256: str | None = None

    @classmethod
    def load(cls, path: Path = DAEMON_CONFIG, *, expected_owner_uid: int = 0) -> "BrokerConfig":
        raw = _read_protected_json(path, expected_owner_uid=expected_owner_uid, maximum=MAX_CONFIG_BYTES,
                                   expected_mode=0o600)
        expected = frozenset({
            "version", "socket_path", "records_dir", "operation_dir", "workspace_root",
            "wrapper_path", "worker_home", "cache_paths", "machine_id_path", "boot_id_path",
            "worker_user", "worker_group", "control_user", "expected_machine_id",
            "service_revision", "release_sha256",
        })
        candidate_pin_key = "candidate_producer_command_sha256"
        if set(raw) not in (expected, expected | {candidate_pin_key}):
            raise ProtocolError("invalid daemon config fields")
        if type(raw.get("version")) is not int or raw["version"] != 1:
            raise ProtocolError("unsupported daemon config version")
        if raw.get("socket_path") != str(CONTROL_SOCKET):
            raise ProtocolError("invalid broker socket path")
        string_paths = (
            "socket_path", "records_dir", "operation_dir", "workspace_root", "wrapper_path",
            "worker_home", "machine_id_path", "boot_id_path",
        )
        paths: dict[str, Path] = {}
        for key in string_paths:
            value = raw.get(key)
            if not isinstance(value, str) or not value.startswith("/") or "\x00" in value:
                raise ProtocolError("invalid daemon config path")
            paths[key] = Path(value)
        cache_paths = raw.get("cache_paths")
        if not isinstance(cache_paths, list) or len(cache_paths) > 32:
            raise ProtocolError("invalid daemon cache paths")
        normalized_cache: list[Path] = []
        for value in cache_paths:
            if not isinstance(value, str) or not value.startswith("/") or "\x00" in value:
                raise ProtocolError("invalid daemon cache paths")
            normalized_cache.append(Path(value))
        worker_user = raw.get("worker_user")
        worker_group = raw.get("worker_group")
        control_user = raw.get("control_user")
        if any(not isinstance(item, str) or not _USER_RE.fullmatch(item)
               for item in (worker_user, worker_group, control_user)):
            raise ProtocolError("invalid daemon account names")
        if worker_user != "factory-worker" or worker_group != "factory-worker" or control_user != "factory-control":
            raise ProtocolError("invalid daemon account names")
        expected_machine_id = raw.get("expected_machine_id")
        if not isinstance(expected_machine_id, str) or not _HEX32_RE.fullmatch(expected_machine_id):
            raise ProtocolError("invalid daemon machine identity")
        release = validate_release({
            "service_revision": raw.get("service_revision"),
            "release_sha256": raw.get("release_sha256"),
        })
        candidate_producer_command_sha256 = raw.get(candidate_pin_key)
        if (candidate_producer_command_sha256 is not None
                and (not isinstance(candidate_producer_command_sha256, str)
                     or not _HEX64_RE.fullmatch(candidate_producer_command_sha256))):
            raise ProtocolError("invalid candidate producer command pin")
        wrapper_suffix = f"/releases/{release['service_revision']}/factory/deploy/worker_operation_exec.py"
        allowed_wrappers = {
            Path(f"/opt/factory{wrapper_suffix}"),
            Path(f"/srv/factory{wrapper_suffix}"),
        }
        if paths["wrapper_path"] not in allowed_wrappers:
            raise ProtocolError("invalid daemon wrapper path")
        if paths["records_dir"] != Path("/var/lib/factory-operations/records"):
            raise ProtocolError("invalid daemon records path")
        if paths["operation_dir"] != OPERATION_ROOT or paths["workspace_root"] != WORKSPACE_ROOT:
            raise ProtocolError("invalid daemon operation paths")
        if paths["worker_home"] != WORKER_HOME:
            raise ProtocolError("invalid daemon worker home")
        if paths["machine_id_path"] != Path("/etc/machine-id") or paths["boot_id_path"] != Path("/proc/sys/kernel/random/boot_id"):
            raise ProtocolError("invalid daemon host identity paths")
        if tuple(normalized_cache) != (
            Path("/srv/factory/tmp"),
            Path(f"/srv/factory/build/{release['service_revision']}"),
        ):
            raise ProtocolError("invalid daemon cache paths")
        try:
            resolved_wrapper = paths["wrapper_path"].resolve(strict=True)
        except OSError as error:
            raise ProtocolError("pinned operation wrapper is unavailable") from error
        expected_wrapper = Path(
            f"/srv/factory/releases/{release['service_revision']}/factory/deploy/worker_operation_exec.py"
        )
        if resolved_wrapper != expected_wrapper:
            raise ProtocolError("pinned operation wrapper is unsafe")
        return cls(
            socket_path=paths["socket_path"],
            records_dir=paths["records_dir"],
            operation_dir=paths["operation_dir"],
            workspace_root=paths["workspace_root"],
            wrapper_path=resolved_wrapper,
            worker_home=paths["worker_home"],
            cache_paths=tuple(normalized_cache),
            machine_id_path=paths["machine_id_path"],
            boot_id_path=paths["boot_id_path"],
            worker_user=worker_user,
            worker_group=worker_group,
            control_user=control_user,
            expected_machine_id=expected_machine_id,
            service_revision=release["service_revision"],
            release_sha256=release["release_sha256"],
            candidate_producer_command_sha256=candidate_producer_command_sha256,
        )

    def expected_release(self) -> dict[str, str]:
        return {"service_revision": self.service_revision, "release_sha256": self.release_sha256}


def containment_is_qualified(
    *,
    expected_machine_id: str,
    service_revision: str,
    release_sha256: str,
    systemd_version: int | None,
    boot_id_path: Path,
    path: Path = QUALIFICATION_FILE,
    expected_owner_uid: int = 0,
) -> bool:
    """Read the operator-installed, host-and-release-bound qualification receipt."""
    required_checks = {
        "held_launch", "duplicate_prepare", "stale_identity_rejected", "setsid_child_terminated",
        "natural_exit", "restart_recovery", "manager_reexec",
    }
    try:
        receipt = _read_protected_json(
            path,
            expected_owner_uid=expected_owner_uid,
            maximum=8192,
            expected_mode=0o600,
        )
        _require_exact_keys(receipt, frozenset({
            "status", "protocol_version", "machine_id", "boot_id", "service_revision",
            "release_sha256", "systemd_version", "checks",
        }), "qualification receipt")
        boot_id = boot_id_path.read_text(encoding="ascii").strip().lower().replace("-", "")
        checks = receipt.get("checks")
        return (
            receipt.get("status") == "passed"
            and type(receipt.get("protocol_version")) is int
            and receipt["protocol_version"] == 1
            and receipt.get("machine_id") == expected_machine_id
            and receipt.get("boot_id") == boot_id
            and bool(_BOOT_ID_RE.fullmatch(boot_id))
            and receipt.get("service_revision") == service_revision
            and receipt.get("release_sha256") == release_sha256
            and type(systemd_version) is int
            and systemd_version >= 252
            and type(receipt.get("systemd_version")) is int
            and receipt["systemd_version"] == systemd_version
            and isinstance(checks, list)
            and all(isinstance(item, str) and len(item) <= 64 for item in checks)
            and len(checks) == len(set(checks))
            and required_checks.issubset(checks)
        )
    except (OSError, ProtocolError, UnicodeError, TypeError, ValueError):
        return False


def _read_protected_json(
    path: Path,
    *,
    expected_owner_uid: int,
    maximum: int,
    expected_mode: int | None = None,
) -> dict[str, Any]:
    try:
        parent = path.parent.lstat()
    except OSError as error:
        raise ProtocolError("protected JSON parent is unavailable") from error
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != expected_owner_uid
            or stat.S_IMODE(parent.st_mode) & 0o022):
        raise ProtocolError("protected JSON parent is unsafe")
    try:
        before = path.lstat()
    except OSError as error:
        raise ProtocolError("protected JSON file is unavailable") from error
    if not stat.S_ISREG(before.st_mode) or before.st_uid != expected_owner_uid or stat.S_ISLNK(before.st_mode):
        raise ProtocolError("protected JSON file is unsafe")
    if expected_mode is not None and stat.S_IMODE(before.st_mode) != expected_mode:
        raise ProtocolError("protected JSON file has unsafe permissions")
    if stat.S_IMODE(before.st_mode) & 0o022:
        raise ProtocolError("protected JSON file has unsafe permissions")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProtocolError("protected JSON file is unavailable") from error
    try:
        opened = os.fstat(descriptor)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != expected_owner_uid
                or opened.st_dev != before.st_dev or opened.st_ino != before.st_ino):
            raise ProtocolError("protected JSON file changed during open")
        chunks = bytearray()
        while len(chunks) <= maximum:
            block = os.read(descriptor, min(65536, maximum + 1 - len(chunks)))
            if not block:
                break
            chunks.extend(block)
        if len(chunks) > maximum:
            raise ProtocolError("protected JSON file is too large")
    finally:
        os.close(descriptor)
    return decode_json_object(bytes(chunks), maximum=maximum)


def _safe_error_response(error: str) -> bytes:
    allowed = {
        "invalid_request", "unsupported_action", "release_mismatch", "operation_unavailable",
        "operation_conflict", "permission_denied", "daemon_unavailable", "internal_error", "invalid_command",
    }
    code = error if error in allowed else "internal_error"
    return _compact_json({"error": code}, maximum=MAX_RESPONSE_BYTES) + b"\n"


def _response_line(value: dict[str, Any]) -> bytes:
    try:
        raw = _compact_json(value, maximum=MAX_RESPONSE_BYTES - 1)
    except ProtocolError:
        return _safe_error_response("internal_error")
    return raw + b"\n"


def _engine_error_code(error: Exception) -> str:
    return {
        "OperationValidationError": "invalid_request",
        "OperationConflict": "operation_conflict",
        "OperationCapacityError": "operation_unavailable",
        "OperationUnavailable": "operation_unavailable",
    }.get(type(error).__name__, "internal_error")


def peer_uid(connection: socket.socket) -> int:
    """Return the kernel-reported peer UID for an accepted Unix socket."""
    peer_option = getattr(socket, "SO_PEERCRED", None)
    if peer_option is not None:
        raw = connection.getsockopt(socket.SOL_SOCKET, peer_option, struct.calcsize("3i"))
        if len(raw) != struct.calcsize("3i"):
            raise OSError("short peer credentials")
        _pid, uid, _gid = struct.unpack("3i", raw)
        return uid
    local_peercred = getattr(socket, "LOCAL_PEERCRED", None)
    if local_peercred is not None:
        raw = connection.getsockopt(getattr(socket, "SOL_LOCAL", 0), local_peercred, struct.calcsize("IIH2x16I"))
        version, uid, groups, *_ = struct.unpack("IIH2x16I", raw)
        if version != 0 or groups > 16:
            raise OSError("invalid local peer credentials")
        return uid
    getpeereid = getattr(connection, "getpeereid", None)
    if getpeereid is not None:
        uid, _gid = getpeereid()
        return uid
    raise OSError("kernel Unix socket peer credentials are unavailable")


def validate_control_socket(
    path: Path = CONTROL_SOCKET,
    *,
    expected_owner_uid: int = 0,
    expected_group_gid: int,
    expected_directory_uid: int = 0,
    expected_directory_gid: int | None = None,
) -> None:
    """Check the fixed root-owned broker socket before a client connects."""
    try:
        parent = path.parent.lstat()
        target = path.lstat()
    except OSError as error:
        raise ProtocolError("broker socket is unavailable") from error
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != expected_directory_uid
            or (expected_directory_gid is not None and parent.st_gid != expected_directory_gid)
            or stat.S_IMODE(parent.st_mode) & 0o022):
        raise ProtocolError("broker socket directory is unsafe")
    if (not stat.S_ISSOCK(target.st_mode) or target.st_uid != expected_owner_uid
            or target.st_gid != expected_group_gid or stat.S_IMODE(target.st_mode) != 0o660):
        raise ProtocolError("broker socket is unsafe")


def _validate_socket_directory(
    path: Path,
    *,
    owner_uid: int,
    group_gid: int,
) -> None:
    parent = path.parent.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode)
            or parent.st_uid != owner_uid or parent.st_gid != group_gid
            or stat.S_IMODE(parent.st_mode) != 0o750):
        raise ProtocolError("broker socket directory is unsafe")


def _unlink_safe_stale_socket(path: Path, *, owner_uid: int, group_gid: int) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != owner_uid or info.st_gid != group_gid:
        raise ProtocolError("existing broker path is unsafe")
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.25)
        probe.connect(str(path))
    except OSError as error:
        if error.errno != errno.ECONNREFUSED:
            raise ProtocolError("existing broker socket may be active") from error
    else:
        raise ProtocolError("broker socket is already active")
    finally:
        probe.close()
    current = path.lstat()
    if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
        raise ProtocolError("existing broker socket changed")
    path.unlink()


class _StreamSession:
    """One broker-owned process stream with replaceable coordinator sockets."""

    def __init__(self, broker: "OperationBroker", identity: Mapping[str, str], process: Any) -> None:
        self.broker = broker
        self.identity = dict(identity)
        self.process = process
        self._lock = threading.Lock()
        self._connection: socket.socket | None = None
        self._reserved = False
        self._finished = False
        self._retire_at: float | None = None
        self.started = False
        self.thread: threading.Thread | None = None

    def attach(self, connection: socket.socket) -> bool:
        """Reserve the single stream slot, send readiness, then hand off the socket."""
        with self._lock:
            if (self._finished or self._reserved or self._connection is not None
                    or (self._retire_at is not None and time.monotonic() >= self._retire_at)):
                return False
            self._reserved = True
        try:
            # Keep the protocol handshake ahead of any buffered app-server
            # bytes. The bridge thread cannot see this socket until afterward.
            connection.sendall(_response_line({"ok": "stream_ready"}))
            connection.setblocking(False)
        except OSError:
            with self._lock:
                self._reserved = False
            return False
        with self._lock:
            if self._finished:
                self._reserved = False
                return False
            self._connection = connection
            self._reserved = False
        return True

    def start(self) -> None:
        with self._lock:
            if self.started:
                return
            self.started = True
            self.thread = threading.Thread(
                target=self.broker._bridge_stdio,
                args=(self,),
                name="factory-operation-stream-session",
                daemon=True,
            )
            thread = self.thread
        thread.start()

    def current_connection(self) -> socket.socket | None:
        with self._lock:
            return self._connection

    def detach(self, connection: socket.socket) -> None:
        with self._lock:
            if self._connection is not connection:
                return
            self._connection = None
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            connection.close()
        except OSError:
            pass

    def detach_current(self) -> None:
        connection = self.current_connection()
        if connection is not None:
            self.detach(connection)

    def mark_finished(self) -> None:
        with self._lock:
            self._finished = True

    def pin_retirement(self, deadline: float) -> None:
        """Set a one-way deadline after terminal status is proven."""
        with self._lock:
            if self._retire_at is None:
                self._retire_at = deadline

    def retirement_deadline(self) -> float | None:
        with self._lock:
            return self._retire_at


class OperationBroker:
    """Threaded local broker. Engine calls are serialized; stdio bridges are not."""

    def __init__(
        self,
        controller: Any,
        *,
        control_uid: int,
        control_gid: int,
        service_revision: str,
        release_sha256: str,
        expected_machine_id: str,
        socket_path: Path = CONTROL_SOCKET,
        boot_id_path: Path = Path("/proc/sys/kernel/random/boot_id"),
        qualification_path: Path = QUALIFICATION_FILE,
        qualification_owner_uid: int = 0,
        expected_owner_uid: int = 0,
        directory_owner_uid: int = 0,
        stream_timeout: float = STREAM_TIMEOUT_SECONDS,
        request_timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        release = validate_release({"service_revision": service_revision, "release_sha256": release_sha256})
        self.controller = controller
        self.manager = controller.manager
        self.control_uid = control_uid
        self.control_gid = control_gid
        self.service_revision = release["service_revision"]
        self.release_sha256 = release["release_sha256"]
        if not isinstance(expected_machine_id, str) or not _HEX32_RE.fullmatch(expected_machine_id):
            raise ProtocolError("invalid expected machine identity")
        self.expected_machine_id = expected_machine_id
        self.socket_path = Path(socket_path)
        self.boot_id_path = Path(boot_id_path)
        self.qualification_path = Path(qualification_path)
        self.qualification_owner_uid = qualification_owner_uid
        self.expected_owner_uid = expected_owner_uid
        self.directory_owner_uid = directory_owner_uid
        self.stream_timeout = stream_timeout
        self.request_timeout = request_timeout
        self._engine_lock = threading.RLock()
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._client_threads: set[threading.Thread] = set()
        self._client_threads_lock = threading.Lock()
        self._bound_inode: tuple[int, int] | None = None
        # A stream socket belongs to the coordinator connection, but the
        # contained process belongs to this long-lived worker broker. Keep the
        # latter after a client disconnect so a coordinator restart cannot
        # turn a transient SSH EOF into app-server stdin EOF.
        self._stream_sessions: dict[tuple[str, ...], _StreamSession] = {}
        self._stream_sessions_lock = threading.RLock()

    def _bind(self) -> socket.socket:
        _validate_socket_directory(
            self.socket_path, owner_uid=self.directory_owner_uid, group_gid=self.control_gid,
        )
        _unlink_safe_stale_socket(
            self.socket_path, owner_uid=self.expected_owner_uid, group_gid=self.control_gid,
        )
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        bound_inode: tuple[int, int] | None = None
        try:
            listener.bind(str(self.socket_path))
            info = self.socket_path.lstat()
            bound_inode = (info.st_dev, info.st_ino)
            os.chown(self.socket_path, self.expected_owner_uid, self.control_gid)
            os.chmod(self.socket_path, 0o660)
            validate_control_socket(
                self.socket_path,
                expected_owner_uid=self.expected_owner_uid,
                expected_group_gid=self.control_gid,
                expected_directory_uid=self.directory_owner_uid,
                expected_directory_gid=self.control_gid,
            )
            listener.listen(32)
            listener.settimeout(0.5)
            self._bound_inode = bound_inode
            return listener
        except BaseException:
            listener.close()
            try:
                info = self.socket_path.lstat()
                if bound_inode == (info.st_dev, info.st_ino):
                    self.socket_path.unlink()
            except OSError:
                pass
            raise

    def serve_forever(self) -> None:
        listener = self._bind()
        self._listener = listener
        try:
            while not self._stop.is_set():
                try:
                    connection, _address = listener.accept()
                except socket.timeout:
                    self._prune_threads()
                    continue
                except OSError:
                    if self._stop.is_set():
                        break
                    raise
                thread = threading.Thread(
                    target=self._serve_connection,
                    args=(connection,),
                    name="factory-operation-client",
                    daemon=True,
                )
                with self._client_threads_lock:
                    self._client_threads.add(thread)
                thread.start()
        finally:
            listener.close()
            self._listener = None
            self._remove_socket_if_ours()
            self._prune_threads(join=True)

    def shutdown(self) -> None:
        self._stop.set()
        listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

    def _remove_socket_if_ours(self) -> None:
        if self._bound_inode is None:
            return
        try:
            info = self.socket_path.lstat()
            if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._bound_inode:
                self.socket_path.unlink()
        except OSError:
            pass
        self._bound_inode = None

    def _prune_threads(self, *, join: bool = False) -> None:
        with self._client_threads_lock:
            threads = list(self._client_threads)
        for thread in threads:
            if join:
                thread.join(timeout=0.2)
            if not thread.is_alive():
                with self._client_threads_lock:
                    self._client_threads.discard(thread)

    def _serve_connection(self, connection: socket.socket) -> None:
        handed_off = False
        try:
            connection.settimeout(self.request_timeout)
            try:
                uid = peer_uid(connection)
            except OSError:
                connection.sendall(_safe_error_response("permission_denied"))
                return
            if uid != self.control_uid:
                connection.sendall(_safe_error_response("permission_denied"))
                return
            try:
                request, remainder = _read_socket_line(connection, MAX_JSON_BYTES)
            except ProtocolError:
                connection.sendall(_safe_error_response("invalid_request"))
                return
            if remainder:
                connection.sendall(_safe_error_response("invalid_request"))
                return
            try:
                decoded = decode_json_object(request)
                validated = validate_rpc_request(decoded)
            except ProtocolError:
                # Stream requests use the same connection but a distinct, fixed
                # envelope. Validate them separately after the regular RPC path.
                try:
                    decoded = decode_json_object(request)
                    _require_exact_keys(decoded, frozenset({"action", "identity", "expected_release"}), "stream request")
                    if decoded.get("action") != "stream":
                        raise ProtocolError("invalid RPC request")
                    stream_request = {
                        "identity": validate_identity(decoded.get("identity")),
                        "expected_release": validate_release(decoded.get("expected_release")),
                    }
                except ProtocolError:
                    connection.sendall(_safe_error_response("invalid_request"))
                    return
                connection.settimeout(None)
                handed_off = self._serve_stream(connection, stream_request)
                return
            try:
                response = self._dispatch_rpc(validated)
                connection.sendall(_response_line({"ok": response}))
            except ProtocolError as error:
                code = str(error)
                if code not in {"release_mismatch", "operation_unavailable", "unsupported_action"}:
                    code = "invalid_request"
                connection.sendall(_safe_error_response(code))
            except Exception as error:
                connection.sendall(_safe_error_response(_engine_error_code(error)))
        except (BrokenPipeError, ConnectionResetError, TimeoutError, socket.timeout, OSError):
            return
        finally:
            if not handed_off:
                try:
                    connection.close()
                except OSError:
                    pass
            current = threading.current_thread()
            with self._client_threads_lock:
                self._client_threads.discard(current)

    def _dispatch_rpc(self, request: dict[str, Any]) -> Any:
        action = request["action"]
        with self._engine_lock:
            if action == "capabilities":
                capabilities = self.controller.capabilities()
                if not isinstance(capabilities, dict):
                    raise ProtocolError("invalid_request")
                if capabilities.get("machine_id") != self.expected_machine_id:
                    raise ProtocolError("operation_unavailable")
                return {
                    **capabilities,
                    "candidate_bundle_v1": bool(
                        getattr(self.controller, "candidate_bundle_available", False)
                    ),
                    "containment_qualified": (
                        capabilities.get("contained") is True
                        and containment_is_qualified(
                            expected_machine_id=self.expected_machine_id,
                            service_revision=self.service_revision,
                            release_sha256=self.release_sha256,
                            systemd_version=capabilities.get("systemd_version"),
                            boot_id_path=self.boot_id_path,
                            path=self.qualification_path,
                            expected_owner_uid=self.qualification_owner_uid,
                        )
                    ),
                    "service_revision": self.service_revision,
                    "release_sha256": self.release_sha256,
                }
            if request["expected_release"] != {
                "service_revision": self.service_revision,
                "release_sha256": self.release_sha256,
            }:
                raise ProtocolError("release_mismatch")
            if action == "prepare":
                result = self.controller.prepare(request["request"])
                if not isinstance(result, dict):
                    raise ProtocolError("invalid_request")
                identity = validate_identity(result)
                if identity["machine_id"] != self.expected_machine_id:
                    raise ProtocolError("operation_unavailable")
                return identity
            identity = request["identity"]
            if identity["machine_id"] != self.expected_machine_id:
                raise ProtocolError("operation_unavailable")
            if action == "candidate_bundle":
                method = getattr(self.controller, "candidate_bundle", None)
                if (not callable(method)
                        or getattr(self.controller, "candidate_bundle_available", False) is not True):
                    raise ProtocolError("unsupported_action")
                result = method(
                    identity,
                    request["input_sha256"],
                    request["offset"],
                    request["expected_sha256"],
                    request["expected_bytes"],
                )
                if not isinstance(result, dict):
                    raise ProtocolError("invalid_request")
                return result
            method = getattr(self.controller, action)
            result = method(identity)
            if not isinstance(result, dict):
                raise ProtocolError("invalid_request")
            return result

    def _serve_stream(self, connection: socket.socket, request: dict[str, Any]) -> bool:
        """Attach one exact operation stream. Return whether the session owns the socket."""
        identity = request["identity"]
        if request["expected_release"] != {
            "service_revision": self.service_revision,
            "release_sha256": self.release_sha256,
        }:
            connection.sendall(_safe_error_response("release_mismatch"))
            return False
        if identity.get("machine_id") != self.expected_machine_id:
            connection.sendall(_safe_error_response("operation_unavailable"))
            return False

        key = tuple(identity[name] for name in sorted(IDENTITY_KEYS))
        with self._stream_sessions_lock:
            session = self._stream_sessions.get(key)
            try:
                with self._engine_lock:
                    status = self.controller.status(identity)
                    if not isinstance(status, dict):
                        connection.sendall(_safe_error_response("operation_unavailable"))
                        return False
                    resumable_terminal = (
                        session is not None
                        and _trusted_terminal_status(status, identity)
                    )
                    if (status.get("operation_id") != identity["operation_id"]
                            or status.get("identity") != identity
                            or (status.get("status") not in {"held", "running"} and not resumable_terminal)):
                        connection.sendall(_safe_error_response("operation_unavailable"))
                        return False
                    if session is None:
                        process = self.controller.claim_stream(identity)
                    else:
                        process = None
            except Exception:
                connection.sendall(_safe_error_response("operation_unavailable"))
                return False

            if session is None:
                if not _is_binary_process(process):
                    connection.sendall(_safe_error_response("operation_unavailable"))
                    return False
                session = _StreamSession(self, identity, process)
                self._stream_sessions[key] = session

            attached = session.attach(connection)
            session.start()
            if not attached:
                connection.sendall(_safe_error_response("operation_unavailable"))
                return False
            return True

    def _forget_stream_session(self, identity: Mapping[str, str], session: _StreamSession) -> None:
        key = tuple(identity[name] for name in sorted(IDENTITY_KEYS))
        with self._stream_sessions_lock:
            if self._stream_sessions.get(key) is session:
                del self._stream_sessions[key]

    def _bridge_stdio(self, session: _StreamSession) -> None:
        """Own process pipes for the broker lifetime and attach transient clients."""
        child_stdin, child_stdout = session.process.stdin, session.process.stdout
        if child_stdin is None or child_stdout is None:
            self._forget_stream_session(session.identity, session)
            return
        input_fd, output_fd = child_stdin.fileno(), child_stdout.fileno()
        os.set_blocking(input_fd, False)
        os.set_blocking(output_fd, False)
        selector = selectors.DefaultSelector()
        to_child, to_client = bytearray(), bytearray()
        child_input_open = child_output_open = True
        client: socket.socket | None = None
        client_read_open = False
        connected_until = 0.0
        next_peer_probe = 0.0
        next_status_poll = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                if now >= next_status_poll:
                    try:
                        with self._engine_lock:
                            status = self.controller.status(session.identity)
                        active = (
                            isinstance(status, dict)
                            and status.get("operation_id") == session.identity["operation_id"]
                            and status.get("identity") == session.identity
                            and status.get("status") in {"held", "running"}
                        )
                        terminal = (
                            _trusted_terminal_status(status, session.identity)
                        )
                        if terminal:
                            session.pin_retirement(now + TERMINAL_STREAM_RETENTION_SECONDS)
                        elif not active:
                            session.detach_current()
                    except Exception:
                        pass
                    next_status_poll = now + 0.5

                attached = session.current_connection()
                if attached is not client:
                    if client is not None:
                        try:
                            selector.unregister(client)
                        except (KeyError, ValueError):
                            pass
                    client = attached
                    if client is not None:
                        client.setblocking(False)
                        connected_until = time.monotonic() + self.stream_timeout
                        client_read_open = True
                        next_peer_probe = 0.0
                    else:
                        client_read_open = False

                if client is not None and time.monotonic() >= connected_until:
                    session.detach(client)
                    continue

                if (client is not None and not client_read_open
                        and time.monotonic() >= next_peer_probe):
                    try:
                        # recv EOF is ambiguous: forced-SSH clients half-close
                        # stdin before they read this socket's output. A zero
                        # length write preserves that distinction without
                        # adding bytes to the raw app-server protocol; a true
                        # peer close raises on Unix stream sockets.
                        client.send(b"")
                    except (BlockingIOError, InterruptedError):
                        pass
                    except OSError:
                        session.detach(client)
                        continue
                    next_peer_probe = time.monotonic() + 0.5

                if client is not None:
                    try:
                        key = selector.get_key(client)
                    except KeyError:
                        key = None
                    client_events = 0
                    if client_read_open and len(to_child) < MAX_STREAM_BUFFER:
                        client_events |= selectors.EVENT_READ
                    if to_client:
                        client_events |= selectors.EVENT_WRITE
                    if client_events:
                        if key is None:
                            selector.register(client, client_events, "socket")
                        elif key.events != client_events:
                            selector.modify(client, client_events, "socket")
                    elif key is not None:
                        selector.unregister(client)

                try:
                    key = selector.get_key(output_fd)
                except KeyError:
                    key = None
                if child_output_open and len(to_client) < MAX_STREAM_BUFFER and key is None:
                    selector.register(output_fd, selectors.EVENT_READ, "output")
                elif (not child_output_open or len(to_client) >= MAX_STREAM_BUFFER) and key is not None:
                    selector.unregister(output_fd)

                try:
                    key = selector.get_key(input_fd)
                except KeyError:
                    key = None
                if child_input_open and to_child and key is None:
                    selector.register(input_fd, selectors.EVENT_WRITE, "input")
                elif (not child_input_open or not to_child) and key is not None:
                    selector.unregister(input_fd)

                # A detached client does not close the app-server's stdin.
                # If the operation exits, continue draining bounded output so
                # a reconnect can receive bytes already produced by the same
                # invocation. At the bound, pipe backpressure pauses the child
                # instead of discarding output.
                if (not child_output_open and not to_client
                        and session.process.poll() is not None):
                    break
                terminal_output_deadline = session.retirement_deadline()
                if terminal_output_deadline is not None and now >= terminal_output_deadline:
                    if to_client or child_output_open:
                        logging.getLogger(__name__).warning(
                            "expired incomplete stream output for terminated worker operation %s",
                            session.identity["operation_id"],
                        )
                    break

                wait_for = min(0.5, max(0, next_status_poll - time.monotonic()))
                for key, mask in selector.select(timeout=wait_for):
                    if key.data == "socket" and mask & selectors.EVENT_READ:
                        try:
                            data = client.recv(min(65536, MAX_STREAM_BUFFER - len(to_child)))
                        except (BlockingIOError, InterruptedError):
                            continue
                        except OSError:
                            data = b""
                        if data:
                            to_child.extend(data)
                        else:
                            # The peer may only have closed its write side.
                            # Keep the read side for app-server output and
                            # probe periodically for a full disconnect.
                            client_read_open = False
                            next_peer_probe = time.monotonic() + 0.5
                    if key.data == "output" and mask & selectors.EVENT_READ:
                        try:
                            data = os.read(output_fd, min(65536, MAX_STREAM_BUFFER - len(to_client)))
                        except (BlockingIOError, InterruptedError):
                            continue
                        except OSError:
                            data = b""
                        if data:
                            to_client.extend(data)
                        else:
                            child_output_open = False
                            self._close_pipe(selector, output_fd, child_stdout)
                    if key.data == "socket" and mask & selectors.EVENT_WRITE:
                        try:
                            sent = client.send(to_client)
                        except (BlockingIOError, InterruptedError):
                            sent = 0
                        except OSError:
                            sent = -1
                        if sent < 0:
                            session.detach(client)
                        elif sent:
                            del to_client[:sent]
                    if key.data == "input" and mask & selectors.EVENT_WRITE and child_input_open:
                        try:
                            sent = os.write(input_fd, to_child)
                        except (BlockingIOError, InterruptedError):
                            sent = 0
                        except OSError:
                            sent = -1
                        if sent < 0:
                            to_child.clear()
                            self._close_pipe(selector, input_fd, child_stdin)
                            child_input_open = False
                        elif sent:
                            del to_child[:sent]
        finally:
            selector.close()
            session.detach_current()
            for stream in (child_stdin, child_stdout):
                try:
                    stream.close()
                except OSError:
                    pass
            if session.retirement_deadline() is not None:
                self._reap_terminal_stream_helper(session.process)
            session.mark_finished()
            self._forget_stream_session(session.identity, session)

    @staticmethod
    def _reap_terminal_stream_helper(process: Any) -> None:
        """Boundedly clean up only the helper whose exact operation is proven terminal."""
        if process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
                process.wait(timeout=0.25)
            except (OSError, subprocess.TimeoutExpired):
                logging.getLogger(__name__).warning(
                    "could not reap stream helper for a terminated worker operation"
                )
        except OSError:
            logging.getLogger(__name__).warning(
                "could not signal stream helper for a terminated worker operation"
            )

    @staticmethod
    def _close_pipe(selector: selectors.BaseSelector, fd: int, pipe: BinaryIO) -> None:
        try:
            selector.unregister(fd)
        except (KeyError, ValueError):
            pass
        try:
            pipe.close()
        except OSError:
            pass


_TERMINATION_PROOF_KEYS = frozenset({
    "machine_id", "boot_id", "unit", "invocation_id", "control_group",
    "cgroup_state", "cgroup_populated", "active_state", "sub_state",
    "exec_main_code", "exec_main_status", "main_pid", "systemd_version",
    "observed_at",
})


def _trusted_terminal_status(status: Any, identity: Mapping[str, str]) -> bool:
    """Require the engine's identity-bound proof before retiring a stream."""
    if (not isinstance(status, dict)
            or status.get("operation_id") != identity.get("operation_id")
            or status.get("identity") != dict(identity)
            or status.get("status") != "terminated"):
        return False
    proof = status.get("termination_proof")
    if not isinstance(proof, dict) or set(proof) != _TERMINATION_PROOF_KEYS:
        return False
    if any(proof.get(key) != identity.get(key) for key in (
        "machine_id", "boot_id", "unit", "invocation_id", "control_group",
    )):
        return False
    if (proof.get("cgroup_state") not in {"present", "released"}
            or type(proof.get("cgroup_populated")) is not int
            or proof.get("cgroup_populated") != 0
            or type(proof.get("main_pid")) is not int
            or proof.get("main_pid") != 0
            or not isinstance(proof.get("systemd_version"), int)
            or isinstance(proof.get("systemd_version"), bool)
            or proof.get("systemd_version") < 1
            or not isinstance(proof.get("exec_main_status"), int)
            or isinstance(proof.get("exec_main_status"), bool)
            or proof.get("exec_main_code") not in {"none", "exited", "killed", "dumped"}
            or not isinstance(proof.get("observed_at"), str)
            or not proof.get("observed_at")):
        return False
    return (
        (proof.get("active_state") == "active" and proof.get("sub_state") == "exited")
        or (proof.get("active_state") == "failed" and proof.get("sub_state") == "failed")
        or (proof.get("active_state") == "inactive" and proof.get("sub_state") in {"dead", "failed"})
    )


def _is_binary_process(process: Any) -> bool:
    return (process is not None and callable(getattr(process, "poll", None))
            and getattr(process, "stdin", None) is not None
            and getattr(process, "stdout", None) is not None
            and callable(getattr(process.stdin, "fileno", None))
            and callable(getattr(process.stdout, "fileno", None)))


def _read_socket_line(connection: socket.socket, maximum: int) -> tuple[bytes, bytes]:
    data = bytearray()
    while len(data) <= maximum:
        chunk = connection.recv(min(4096, maximum + 1 - len(data)))
        if not chunk:
            raise ProtocolError("incomplete request line")
        newline = chunk.find(b"\n")
        if newline >= 0:
            if len(data) + newline > maximum:
                raise ProtocolError("request line is too large")
            data.extend(chunk[:newline])
            if data.endswith(b"\r"):
                raise ProtocolError("invalid request line")
            return bytes(data), chunk[newline + 1:]
        data.extend(chunk)
        if len(data) > maximum:
            raise ProtocolError("request line is too large")
    raise ProtocolError("request line is too large")


def _read_response_line(connection: socket.socket, *, maximum: int = MAX_RESPONSE_BYTES) -> bytes:
    line, remainder = _read_socket_line(connection, maximum)
    if remainder:
        raise ProtocolError("unexpected response bytes")
    return line


def _control_account() -> tuple[int, int]:
    try:
        account = pwd.getpwnam("factory-control")
        group = grp.getgrnam("factory-control")
    except KeyError as error:
        raise ProtocolError("factory-control account is unavailable") from error
    return account.pw_uid, group.gr_gid


def _emit_error(output: BinaryIO, code: str) -> int:
    try:
        output.write(_safe_error_response(code))
        output.flush()
    except (OSError, BrokenPipeError):
        pass
    return 1


def _copy_stdin_to_socket(source: BinaryIO, connection: socket.socket) -> None:
    # Persistent SSH stdin must forward short requests without waiting for EOF
    # or enough bytes to fill a buffered read.
    read = getattr(source, "read1", None)
    if not callable(read):
        read = source.read
    try:
        while True:
            chunk = read(65536)
            if not chunk:
                break
            connection.sendall(chunk)
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    finally:
        try:
            connection.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def ssh_client_main(
    *,
    environment: dict[str, str] | None = None,
    input_stream: BinaryIO | None = None,
    output_stream: BinaryIO | None = None,
    socket_path: Path = CONTROL_SOCKET,
    expected_socket_uid: int = 0,
    expected_socket_gid: int | None = None,
    expected_directory_uid: int = 0,
    expected_directory_gid: int | None = None,
) -> int:
    """Forced-SSH entrypoint. The only client-controlled value is the grammar."""
    environment = os.environ if environment is None else environment
    input_stream = sys.stdin.buffer if input_stream is None else input_stream
    output_stream = sys.stdout.buffer if output_stream is None else output_stream
    command = environment.get("SSH_ORIGINAL_COMMAND")
    try:
        kind, request = parse_ssh_original_command(command)
        if expected_socket_gid is None:
            _uid, expected_socket_gid = _control_account()
        validate_control_socket(
            socket_path,
            expected_owner_uid=expected_socket_uid,
            expected_group_gid=expected_socket_gid,
            expected_directory_uid=expected_directory_uid,
            expected_directory_gid=expected_directory_gid,
        )
    except ProtocolError:
        return _emit_error(output_stream, "invalid_command")
    connection: socket.socket | None = None
    try:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(30.0)
        connection.connect(str(socket_path))
        payload = dict(request)
        if kind == "stream":
            payload = {"action": "stream", **request}
        connection.sendall(_compact_json(payload) + b"\n")
        if kind == "rpc":
            try:
                connection.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            line = _read_response_line(connection)
            output_stream.write(line + b"\n")
            output_stream.flush()
            response = decode_json_object(line, maximum=MAX_RESPONSE_BYTES)
            return 0 if set(response) == {"ok"} else 1
        connection.settimeout(None)
        line = _read_response_line(connection)
        output_stream.write(line + b"\n")
        output_stream.flush()
        response = decode_json_object(line, maximum=MAX_RESPONSE_BYTES)
        if response != {"ok": "stream_ready"}:
            return 1
        bridge = threading.Thread(
            target=_copy_stdin_to_socket,
            args=(input_stream, connection),
            name="factory-operation-ssh-stdin",
            daemon=True,
        )
        bridge.start()
        while True:
            chunk = connection.recv(65536)
            if not chunk:
                break
            output_stream.write(chunk)
            output_stream.flush()
        return 0
    except (OSError, ProtocolError, TimeoutError, socket.timeout):
        return _emit_error(output_stream, "daemon_unavailable")
    finally:
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass


def _safe_path_chain(path: Path, stop_at: Path, *, expected_owner_uid: int) -> None:
    """Check every public descriptor parent without entering the private ledger."""
    path = Path(os.path.abspath(path))
    stop_at = Path(os.path.abspath(stop_at))
    try:
        relative = path.relative_to(stop_at)
    except ValueError as error:
        raise ProtocolError("descriptor is outside the operation root") from error
    current = stop_at
    chain = [current]
    for component in relative.parts[:-1]:
        current = current / component
        chain.append(current)
    for directory in chain:
        try:
            info = directory.lstat()
        except OSError as error:
            raise ProtocolError("operation descriptor directory is unavailable") from error
        if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
                or info.st_uid != expected_owner_uid or stat.S_IMODE(info.st_mode) & 0o022):
            raise ProtocolError("operation descriptor directory is unsafe")


def _read_root_manifest(path: Path, *, expected_owner_uid: int, operation_root: Path) -> dict[str, Any]:
    _safe_path_chain(path, operation_root, expected_owner_uid=expected_owner_uid)
    try:
        before = path.lstat()
    except OSError as error:
        raise ProtocolError("operation manifest is unavailable") from error
    if (not stat.S_ISREG(before.st_mode) or before.st_uid != expected_owner_uid
            or stat.S_IMODE(before.st_mode) != 0o644):
        raise ProtocolError("operation manifest is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProtocolError("operation manifest is unavailable") from error
    try:
        opened = os.fstat(descriptor)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != expected_owner_uid
                or opened.st_dev != before.st_dev or opened.st_ino != before.st_ino):
            raise ProtocolError("operation manifest changed during open")
        data = bytearray()
        while len(data) <= MAX_MANIFEST_BYTES:
            block = os.read(descriptor, min(16384, MAX_MANIFEST_BYTES + 1 - len(data)))
            if not block:
                break
            data.extend(block)
        if len(data) > MAX_MANIFEST_BYTES:
            raise ProtocolError("operation manifest is too large")
    finally:
        os.close(descriptor)
    return decode_json_object(bytes(data), maximum=MAX_MANIFEST_BYTES)


def _read_release_marker(path: Path, *, expected_owner_uid: int, invocation_id: str) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ProtocolError("release marker is unavailable") from error
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != expected_owner_uid
            or stat.S_IMODE(info.st_mode) != 0o644):
        raise ProtocolError("release marker is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProtocolError("release marker is unavailable") from error
    try:
        opened = os.fstat(descriptor)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != expected_owner_uid
                or opened.st_dev != info.st_dev or opened.st_ino != info.st_ino):
            raise ProtocolError("release marker changed during open")
        marker = os.read(descriptor, 65)
        if os.read(descriptor, 1):
            raise ProtocolError("release marker is too large")
    finally:
        os.close(descriptor)
    if marker != (invocation_id + "\n").encode("ascii"):
        raise ProtocolError("release marker does not match invocation")
    return True


_WORKER_ENVIRONMENT_KEYS = frozenset({
    "HOME", "USER", "LOGNAME", "PATH", "LANG", "LC_ALL", "TMPDIR", "CODEX_HOME",
    "CARGO_HOME", "RUSTUP_HOME", "XDG_CACHE_HOME", "MISE_DATA_DIR", "MISE_CACHE_DIR",
    "MIX_HOME", "CARGO_BUILD_JOBS",
})


def _validate_worker_environment(value: Any, *, worker_home: Path) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ProtocolError("invalid worker environment")
    _require_exact_keys(value, _WORKER_ENVIRONMENT_KEYS, "worker environment")
    if any(not isinstance(item, str) or "\x00" in item or len(item) > 4096 for item in value.values()):
        raise ProtocolError("invalid worker environment")
    home = str(worker_home)
    expected = {
        "HOME": home,
        "USER": "factory-worker",
        "LOGNAME": "factory-worker",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TMPDIR": "/srv/factory/tmp",
        "CODEX_HOME": f"{home}/.codex",
        "CARGO_HOME": f"{home}/.cargo",
        "RUSTUP_HOME": "/srv/factory/bootstrap-tools-v1/rustup",
        "XDG_CACHE_HOME": f"{home}/.cache",
        "MISE_DATA_DIR": "/srv/factory/mise",
        "MISE_CACHE_DIR": f"{home}/.cache/mise",
        "MIX_HOME": f"{home}/.mix",
        "CARGO_BUILD_JOBS": "3",
    }
    node_bins = [
        "/srv/factory/bootstrap-tools-v1/rig-tools/node-v24.19.0-linux-x64/bin",
        "/srv/factory/bootstrap-tools-v1/rig-tools/node-v24.19.0-linux-arm64/bin",
    ]
    allowed_paths = {
        f"/srv/factory/bootstrap-tools-v1/cargo/bin:{node_bin}:/srv/factory/bootstrap-tools-v1/rig-tools/codex/bin:/usr/local/bin:/usr/bin:/bin"
        for node_bin in node_bins
    }
    if value.get("PATH") not in allowed_paths or any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise ProtocolError("worker environment is outside the approved runtime")
    return {key: value[key] for key in sorted(_WORKER_ENVIRONMENT_KEYS)}


def held_wrapper_main(
    descriptor_path: str | Path,
    *,
    expected_owner_uid: int = 0,
    expected_worker_gid: int | None = None,
    operation_root: str | Path = OPERATION_ROOT,
    workspace_root: str | Path = WORKSPACE_ROOT,
    worker_home: str | Path = WORKER_HOME,
    wait_timeout: float = RELEASE_WAIT_SECONDS,
    poll_interval: float = 0.1,
    environ: dict[str, str] | None = None,
    exec_function: Any = os.execvpe,
) -> int:
    """Validate one public manifest, wait for its invocation-bound release, exec."""
    environ = os.environ if environ is None else environ
    try:
        path = Path(descriptor_path)
        if not path.is_absolute() or path.name != "manifest.json":
            raise ProtocolError("invalid operation descriptor path")
        operation_root = Path(operation_root)
        manifest = _read_root_manifest(path, expected_owner_uid=expected_owner_uid, operation_root=operation_root)
        _require_exact_keys(
            manifest,
            frozenset({"version", "operation_id", "request_sha256", "argv", "workspace", "release_file", "environment"}),
            "operation manifest",
        )
        if type(manifest.get("version")) is not int or manifest["version"] != 1:
            raise ProtocolError("unsupported operation manifest version")
        operation_id = validate_operation_id(manifest.get("operation_id"))
        expected_directory = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
        if path.parent.name != expected_directory or path.parent.parent != operation_root:
            raise ProtocolError("operation manifest path does not match its identity")
        request_sha256 = manifest.get("request_sha256")
        if not isinstance(request_sha256, str) or not _HEX64_RE.fullmatch(request_sha256):
            raise ProtocolError("invalid operation request digest")
        argv = manifest.get("argv")
        workspace = manifest.get("workspace")
        release_file = manifest.get("release_file")
        if (not isinstance(argv, list) or not argv or len(argv) > 128
                or any(not isinstance(item, str) or not item or "\x00" in item for item in argv)):
            raise ProtocolError("invalid operation argv")
        if sum(len(item.encode("utf-8", errors="strict")) for item in argv) > MAX_ARGV_BYTES:
            raise ProtocolError("invalid operation argv")
        executable = Path(argv[0])
        if not executable.is_absolute():
            raise ProtocolError("operation executable is not absolute")
        try:
            canonical_executable = Path(os.path.realpath(executable, strict=True))
        except OSError as error:
            raise ProtocolError("operation executable is unavailable") from error
        if canonical_executable != executable or not executable.is_file() or not os.access(executable, os.X_OK):
            raise ProtocolError("operation executable is unsafe")
        if not isinstance(workspace, str) or not workspace.startswith("/") or "\x00" in workspace:
            raise ProtocolError("invalid operation workspace")
        worker_environment = _validate_worker_environment(manifest.get("environment"), worker_home=Path(worker_home))
        workspace_path = Path(workspace)
        workspace_root = Path(workspace_root)
        try:
            canonical_workspace = Path(os.path.realpath(workspace_path, strict=True))
            canonical_root = Path(os.path.realpath(workspace_root, strict=True))
            canonical_workspace.relative_to(canonical_root)
        except (OSError, ValueError) as error:
            raise ProtocolError("operation workspace is unavailable") from error
        if canonical_workspace != workspace_path or not canonical_workspace.is_dir():
            raise ProtocolError("operation workspace is unsafe")
        canonical_request = {
            "operation_id": operation_id,
            "argv": argv,
            "workspace": str(canonical_workspace),
        }
        expected_digest = hashlib.sha256(
            json.dumps(canonical_request, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
            + b"\n"
        ).hexdigest()
        if request_sha256 != expected_digest:
            raise ProtocolError("operation request digest does not match")
        workspace_root_info = canonical_root.stat()
        if expected_worker_gid is None:
            try:
                expected_worker_gid = grp.getgrnam("factory-worker").gr_gid
            except KeyError as error:
                raise ProtocolError("worker group is unavailable") from error
        if (workspace_root_info.st_uid != expected_owner_uid or workspace_root_info.st_gid != expected_worker_gid
                or stat.S_IMODE(workspace_root_info.st_mode) != 0o750):
            raise ProtocolError("workspace root is unsafe")
        expected_release_file = path.parent / "release"
        if release_file != str(expected_release_file):
            raise ProtocolError("invalid operation release path")
        _safe_path_chain(expected_release_file, operation_root, expected_owner_uid=expected_owner_uid)
        invocation_id = environ.get("INVOCATION_ID", "")
        if not _HEX32_RE.fullmatch(invocation_id):
            raise ProtocolError("invalid systemd invocation id")
        if wait_timeout < 0 or wait_timeout > RELEASE_WAIT_SECONDS or poll_interval <= 0:
            raise ProtocolError("invalid release wait bounds")
        deadline = time.monotonic() + wait_timeout
        while True:
            if _read_release_marker(expected_release_file, expected_owner_uid=expected_owner_uid,
                                   invocation_id=invocation_id):
                break
            if time.monotonic() >= deadline:
                raise ProtocolError("operation release timed out")
            time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
        exec_function(argv[0], argv, worker_environment)
        return 127
    except (ProtocolError, OSError, ValueError, TypeError, RecursionError):
        try:
            sys.stderr.write("factory operation held wrapper failed\n")
            sys.stderr.flush()
        except OSError:
            pass
        return 1


def _load_engine(config: BrokerConfig) -> tuple[Any, Any]:
    # Imported only by the root daemon. The worker wrapper and SSH-facing mode
    # never import or execute the systemd manager.
    engine_path = Path(__file__).resolve(strict=True).with_name("worker_operation.py")
    if engine_path != config.wrapper_path.with_name("worker_operation.py"):
        raise ProtocolError("worker operation engine is outside the pinned release")
    engine_info = engine_path.lstat()
    if (not stat.S_ISREG(engine_info.st_mode) or engine_info.st_uid != 0
            or stat.S_IMODE(engine_info.st_mode) & 0o022):
        raise ProtocolError("worker operation engine is unsafe")
    spec = importlib.util.spec_from_file_location("_factory_worker_operation_engine", engine_path)
    if spec is None or spec.loader is None:
        raise ProtocolError("worker operation engine is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    manager = module.SystemdManager()
    controller = module.OperationController(
        manager,
        records_dir=config.records_dir,
        operation_dir=config.operation_dir,
        workspace_root=config.workspace_root,
        wrapper_path=config.wrapper_path,
        worker_home=config.worker_home,
        cache_paths=config.cache_paths,
        machine_id_path=str(config.machine_id_path),
        boot_id_path=str(config.boot_id_path),
        worker_user=config.worker_user,
        worker_group=config.worker_group,
        expected_machine_id=config.expected_machine_id,
        service_revision=config.service_revision,
        candidate_producer_command_sha256=config.candidate_producer_command_sha256,
    )
    return controller, manager


def _daemon_main(config_path: Path) -> int:
    if os.geteuid() != 0:
        return _emit_error(sys.stderr.buffer, "permission_denied")
    if config_path != DAEMON_CONFIG:
        return _emit_error(sys.stderr.buffer, "invalid_request")
    try:
        config = BrokerConfig.load(config_path)
        control_uid, control_gid = _control_account()
        if config.control_user != "factory-control":
            raise ProtocolError("invalid control account")
        controller, _manager = _load_engine(config)
        broker = OperationBroker(
            controller,
            control_uid=control_uid,
            control_gid=control_gid,
            service_revision=config.service_revision,
            release_sha256=config.release_sha256,
            expected_machine_id=config.expected_machine_id,
            socket_path=config.socket_path,
            boot_id_path=config.boot_id_path,
            qualification_path=QUALIFICATION_FILE,
        )
    except (ProtocolError, OSError, ImportError, ValueError):
        return _emit_error(sys.stderr.buffer, "invalid_request")

    def stop(_signum: int, _frame: Any) -> None:
        broker.shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        broker.serve_forever()
    except Exception:
        return _emit_error(sys.stderr.buffer, "internal_error")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--ssh"]:
        return ssh_client_main()
    if len(argv) == 2 and argv[0] == "--config":
        return _daemon_main(Path(argv[1]))
    return _emit_error(sys.stderr.buffer, "invalid_request")


if __name__ == "__main__":
    raise SystemExit(main())
