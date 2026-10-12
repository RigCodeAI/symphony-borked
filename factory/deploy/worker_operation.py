"""Contained worker operations backed by systemd transient services.

The controller is intended to run in a small root-owned broker. Client supplied
commands are written to a worker-readable descriptor and are executed only by a
root-owned wrapper as ``factory-worker``. A prepared unit waits for a separate
root-written release file before it can exec the requested command.
"""

from __future__ import annotations

import base64
import contextlib
from collections import OrderedDict
import fcntl
import hashlib
import json
import os
import pwd
import grp
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


PROTOCOL_VERSION = 1
IDENTITY_KEYS = (
    "kind",
    "operation_id",
    "machine_id",
    "boot_id",
    "unit",
    "invocation_id",
    "control_group",
    "request_sha256",
)
TERMINATION_PROOF_KEYS = (
    "machine_id",
    "boot_id",
    "unit",
    "invocation_id",
    "control_group",
    "cgroup_state",
    "cgroup_populated",
    "active_state",
    "sub_state",
    "exec_main_code",
    "exec_main_status",
    "main_pid",
    "systemd_version",
    "observed_at",
)
_UNIT_RE = re.compile(r"^factory-operation-[0-9a-f]{64}\.service$")
_HEX_32_RE = re.compile(r"^[0-9a-f]{32}$")
_HEX_40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_CANDIDATE_BUNDLE_BYTES = 64 * 1024 * 1024
CANDIDATE_BUNDLE_CHUNK_BYTES = 64 * 1024
MAX_CANDIDATE_BUNDLE_CACHE_ENTRIES = 16


class OperationError(RuntimeError):
    """Base error for a rejected or unavailable worker operation."""


class OperationConflict(OperationError):
    """An operation ID is already reserved for a different request."""


class OperationUnavailable(OperationError):
    """An operation is reserved but cannot safely be started or observed."""


class OperationCapacityError(OperationError):
    """The worker already has an active or unproven operation."""


class OperationValidationError(OperationError):
    """A request or identity does not meet the operation contract."""


@dataclass(frozen=True)
class UnitSnapshot:
    """The system manager's view of one transient unit."""

    load_state: str
    invocation_id: str
    control_group: str
    active_state: str
    sub_state: str
    result: str
    exec_main_code: str
    exec_main_status: int
    main_pid: int
    user: str
    group: str
    syslog_identifier: str
    supplementary_groups: str | None = None


class SystemdManager:
    """A bounded, argument-vector-only adapter for the system systemd manager."""

    # v252 supports the fixed isolation properties and invocation/cgroup proof.
    # Task argv is read by the held wrapper, never passed to systemd-run. The
    # optional expansion switch only exists in v254 and later.
    _SYSTEMD_MIN_VERSION = 252

    def __init__(
        self,
        *,
        systemd_run_path: str = "/usr/bin/systemd-run",
        systemctl_path: str = "/usr/bin/systemctl",
        cgroup_root: str = "/sys/fs/cgroup",
        control_socket_path: str = "/run/factory-operations/control.sock",
        timeout: float = 5.0,
        start_timeout: float = 10.0,
    ) -> None:
        self.systemd_run_path = os.path.abspath(systemd_run_path)
        self.systemctl_path = os.path.abspath(systemctl_path)
        self.cgroup_root = Path(cgroup_root).resolve()
        self.control_socket_path = os.path.abspath(control_socket_path)
        self.timeout = timeout
        self.start_timeout = start_timeout
        self._lock = threading.Lock()
        self._processes: dict[str, subprocess.Popen[bytes]] = {}
        self._claimed: set[str] = set()
        self._capabilities: dict[str, Any] | None = None

    def capabilities(self) -> dict[str, Any]:
        """Report host facts; fake managers deliberately cannot claim containment."""

        if self._capabilities is not None:
            return dict(self._capabilities)
        is_root = os.geteuid() == 0
        system_manager = False
        version = 0
        try:
            _assert_trusted_executable(self.systemctl_path, expected_owner_uid=0)
            _assert_trusted_executable(self.systemd_run_path, expected_owner_uid=0)
            result = subprocess.run(
                [self.systemctl_path, "--version"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=self.timeout,
                check=False,
                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
            )
            first = result.stdout.decode("utf-8", "replace").splitlines()[0]
            match = re.match(r"systemd\s+(\d+)", first)
            if result.returncode == 0 and match:
                version = int(match.group(1))
                # Querying the system manager (not --user) proves that the system
                # bus is reachable; the explicit private socket check rejects a
                # user-manager-only installation.
                system_manager = os.path.exists("/run/systemd/private")
                if system_manager:
                    show = subprocess.run(
                        [self.systemctl_path, "show", "--property=Version", "--value"],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                        timeout=self.timeout,
                        check=False,
                        env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
                    )
                    show_version = re.match(rb"\s*(\d+)(?:[ .-]|$)", show.stdout)
                    system_manager = (
                        show.returncode == 0
                        and show_version is not None
                        and int(show_version.group(1)) == version
                    )
        except (OSError, IndexError, OperationUnavailable, subprocess.TimeoutExpired):
            system_manager = False

        cgroup_v2 = (self.cgroup_root / "cgroup.controllers").is_file()
        in_container = self._detect_container()
        isolation = (
            version >= self._SYSTEMD_MIN_VERSION
            and os.path.isfile(self.systemd_run_path)
            and os.access(self.systemd_run_path, os.X_OK)
            and os.path.isfile(self.systemctl_path)
            and os.access(self.systemctl_path, os.X_OK)
            and all(
                os.path.exists(path)
                for path in (
                    self.control_socket_path,
                    "/run/systemd/private",
                    "/run/dbus/system_bus_socket",
                    "/run/user",
                )
            )
        )
        contained = is_root and system_manager and cgroup_v2 and isolation and not in_container
        result = {
            "protocol_version": PROTOCOL_VERSION,
            "contained": contained,
            "systemd_version": version,
            "released_cgroup_proof": bool(contained and version >= self._SYSTEMD_MIN_VERSION),
            "machine_id": _read_machine_id("/etc/machine-id"),
            "boot_id": _read_boot_id("/proc/sys/kernel/random/boot_id"),
            "system_manager": system_manager and not in_container,
            "cgroup_v2": cgroup_v2,
            "required_isolation": isolation and not in_container,
        }
        self._capabilities = result
        return dict(result)

    def _detect_container(self) -> bool:
        detector = shutil.which("systemd-detect-virt")
        if not detector:
            # Without a reliable container probe, do not advertise the strong
            # containment contract.
            return True
        try:
            result = subprocess.run(
                [detector, "--container"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.timeout,
                check=False,
                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
            )
            return result.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return True

    def start(
        self,
        unit: str,
        wrapper_path: str,
        descriptor_path: str,
        properties: Mapping[str, str | Sequence[str]],
    ) -> None:
        """Start one service and keep its live stream handle for one claim."""

        if os.geteuid() != 0:
            raise OperationUnavailable("systemd operation broker must run as root")
        _assert_trusted_executable(self.systemd_run_path, expected_owner_uid=0)
        if not _UNIT_RE.fullmatch(unit):
            raise OperationValidationError("invalid transient unit name")
        if not os.path.isabs(wrapper_path) or not os.path.isabs(descriptor_path):
            raise OperationValidationError("wrapper and descriptor paths must be absolute")
        if any(char in path for path in (wrapper_path, descriptor_path) for char in ("$", "%")):
            raise OperationValidationError("wrapper and descriptor paths cannot contain systemd substitutions")
        facts = self.capabilities()
        if not facts.get("contained"):
            raise OperationUnavailable("system manager containment is unavailable")
        with self._lock:
            if unit in self._processes:
                raise OperationConflict("unit already has a retained systemd-run handle")
            command = [
                self.systemd_run_path,
                "--quiet",
                "--pipe",
                "--wait",
                f"--unit={unit}",
            ]
            if facts["systemd_version"] >= 254:
                command.append("--expand-environment=no")
            for name, value in properties.items():
                if isinstance(value, str):
                    encoded = value
                else:
                    if name in {"ReadWritePaths", "InaccessiblePaths"}:
                        encoded = " ".join(_systemd_path_value(str(part)) for part in value)
                    else:
                        encoded = " ".join(_systemd_word(str(part)) for part in value)
                command.append(f"--property={name}={encoded}")
            command.extend(("--", wrapper_path, "--manifest", descriptor_path))
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=0,
                    env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
                )
            except OSError as exc:
                # The caller's durable reservation remains. It must never retry
                # the start because this boundary may be ambiguous to a broker.
                raise OperationUnavailable("systemd-run could not be started") from exc
            self._processes[unit] = process

    def claim_stream(self, unit: str) -> subprocess.Popen[bytes]:
        """Return the retained raw stream process once; never reconstruct it."""

        with self._lock:
            process = self._processes.get(unit)
            if process is None or unit in self._claimed:
                raise OperationUnavailable("operation stream is unavailable")
            self._claimed.add(unit)
            return process

    def inspect(self, unit: str) -> UnitSnapshot | None:
        if not _UNIT_RE.fullmatch(unit):
            raise OperationValidationError("invalid transient unit name")
        _assert_trusted_executable(self.systemctl_path, expected_owner_uid=0)
        names = (
            "LoadState",
            "InvocationID",
            "ControlGroup",
            "ActiveState",
            "SubState",
            "Result",
            "ExecMainCode",
            "ExecMainStatus",
            "MainPID",
            "User",
            "Group",
            "SyslogIdentifier",
            "SupplementaryGroups",
        )
        try:
            result = subprocess.run(
                [self.systemctl_path, "show", "--no-pager", *(f"--property={name}" for name in names), unit],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                check=False,
                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise OperationUnavailable("system manager inspection timed out or failed") from exc
        if result.returncode != 0:
            raise OperationUnavailable("system manager inspection failed")
        values: dict[str, str] = {}
        for line in result.stdout.decode("utf-8", "replace").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                values[key] = value
        if values.get("LoadState") in (None, "not-found", "error"):
            return None
        try:
            return UnitSnapshot(
                load_state=values.get("LoadState", ""),
                invocation_id=values.get("InvocationID", "").lower(),
                control_group=values.get("ControlGroup", ""),
                active_state=values.get("ActiveState", ""),
                sub_state=values.get("SubState", ""),
                result=values.get("Result", ""),
                exec_main_code=_normalize_exec_main_code(values.get("ExecMainCode", "")),
                exec_main_status=int(values.get("ExecMainStatus", "0") or 0),
                main_pid=int(values.get("MainPID", "0") or 0),
                user=values.get("User", ""),
                group=values.get("Group", ""),
                syslog_identifier=values.get("SyslogIdentifier", ""),
                supplementary_groups=values.get("SupplementaryGroups"),
            )
        except ValueError as exc:
            raise OperationUnavailable("system manager returned invalid unit properties") from exc

    def cgroup_populated(self, control_group: str) -> int:
        if not control_group.startswith("/") or ".." in Path(control_group).parts:
            raise OperationValidationError("invalid cgroup path")
        target = (self.cgroup_root / control_group.lstrip("/")).resolve()
        try:
            target.relative_to(self.cgroup_root)
            events = target / "cgroup.events"
            with events.open("r", encoding="ascii") as source:
                for line in source:
                    key, _, value = line.partition(" ")
                    if key == "populated":
                        result = int(value.strip())
                        if result in (0, 1):
                            return result
                        break
        except (OSError, ValueError, RuntimeError) as exc:
            raise OperationUnavailable("operation cgroup population is unavailable") from exc
        raise OperationUnavailable("operation cgroup has no valid population proof")

    def kill(self, unit: str, signal_name: str) -> None:
        if signal_name not in ("TERM", "KILL"):
            raise OperationValidationError("unsupported operation signal")
        self._systemctl(["kill", "--kill-whom=all", f"--signal={signal_name}", unit])

    def stop(self, unit: str) -> None:
        self._systemctl(["stop", unit])

    def _systemctl(self, arguments: Sequence[str]) -> None:
        _assert_trusted_executable(self.systemctl_path, expected_owner_uid=0)
        try:
            result = subprocess.run(
                [self.systemctl_path, *arguments],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                check=False,
                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise OperationUnavailable("system manager signal or stop timed out") from exc
        if result.returncode != 0:
            raise OperationUnavailable("system manager signal or stop failed")


class OperationController:
    """Own durable reservations and prove an exact transient unit before control."""

    def __init__(
        self,
        manager: Any,
        *,
        records_dir: str,
        operation_dir: str,
        workspace_root: str,
        wrapper_path: str,
        worker_home: str,
        cache_paths: Sequence[str] = (),
        machine_id_path: str = "/etc/machine-id",
        boot_id_path: str = "/proc/sys/kernel/random/boot_id",
        expected_machine_id: str | None = None,
        service_revision: str | None = None,
        candidate_producer_command_sha256: str | None = None,
        worker_user: str = "factory-worker",
        worker_group: str = "factory-worker",
        expected_owner_uid: int = 0,
        operation_timeout: float = 10.0,
        stop_timeout: float = 2.0,
    ) -> None:
        self.manager = manager
        self.records_dir = Path(records_dir).absolute()
        self.operation_dir = Path(operation_dir).absolute()
        self.workspace_root_path = Path(workspace_root).absolute()
        self.workspace_root = Path(workspace_root).resolve()
        self.wrapper_path = os.path.abspath(wrapper_path)
        self.worker_home = os.path.abspath(worker_home)
        self.cache_paths = tuple(os.path.abspath(path) for path in cache_paths)
        self.machine_id_path = machine_id_path
        self.boot_id_path = boot_id_path
        self.expected_machine_id = (expected_machine_id or "").replace("-", "").lower()
        self.service_revision = service_revision or ""
        self.candidate_producer_command_sha256 = candidate_producer_command_sha256 or ""
        self.worker_user = worker_user
        self.worker_group = worker_group
        self.expected_owner_uid = expected_owner_uid
        self.operation_timeout = operation_timeout
        self.stop_timeout = stop_timeout
        self._candidate_bundle_cache: OrderedDict[
            tuple[tuple[Any, ...], tuple[int, ...]], None
        ] = OrderedDict()
        self._candidate_bundle_cache_lock = threading.Lock()

    def capabilities(self) -> dict[str, Any]:
        facts = self.manager.capabilities() if hasattr(self.manager, "capabilities") else {}
        machine_id = _read_machine_id(self.machine_id_path)
        boot_id = _read_boot_id(self.boot_id_path)
        worker_account_isolated = self._worker_account_isolated()
        facts = dict(facts)
        facts.update(
            {
                "protocol_version": PROTOCOL_VERSION,
                "machine_id": machine_id,
                "boot_id": boot_id,
                "systemd_version": facts.get("systemd_version", 0),
                "released_cgroup_proof": bool(
                    isinstance(self.manager, SystemdManager)
                    and facts.get("released_cgroup_proof")
                ),
                "worker_account_isolated": worker_account_isolated,
                "contained": bool(
                    isinstance(self.manager, SystemdManager)
                    and facts.get("contained")
                    and self.expected_owner_uid == 0
                    and machine_id
                    and boot_id
                    and worker_account_isolated
                    and (not self.expected_machine_id or machine_id == self.expected_machine_id)
                ),
            }
        )
        return facts

    def prepare(self, request: Mapping[str, Any]) -> dict[str, str]:
        canonical = self._validate_request(request)
        operation_id = canonical["operation_id"]
        request_sha256 = _sha256_json(canonical)
        unit = _unit_name(operation_id)
        record_path = self._record_path(operation_id)

        with self._locked_records():
            self._assert_storage_layout()
            existing = self._read_record(record_path, missing_ok=True)
            if existing is not None:
                if (
                    existing.get("operation_id") != operation_id
                    or existing.get("request_sha256") != request_sha256
                ):
                    raise OperationConflict("operation ID is already reserved")
                identity = self._recover_identity(existing)
                if identity is None:
                    raise OperationUnavailable("operation reservation has no proven unit identity")
                return identity

            self._assert_capacity_available()
            machine_id = _read_machine_id(self.machine_id_path)
            boot_id = _read_boot_id(self.boot_id_path)
            if not machine_id or not boot_id:
                raise OperationUnavailable("host identity is unavailable")
            if self.expected_machine_id and machine_id != self.expected_machine_id:
                raise OperationUnavailable("worker machine ID does not match protected configuration")
            if isinstance(self.manager, SystemdManager):
                if self.expected_owner_uid != 0:
                    raise OperationUnavailable("systemd operation broker must use root-owned state")
                facts = self.manager.capabilities()
                if not facts.get("contained"):
                    raise OperationUnavailable("system manager or required isolation is unavailable")
                if not self._worker_account_isolated():
                    raise OperationUnavailable("worker account is not an isolated non-root identity")
            record = {
                "version": PROTOCOL_VERSION,
                "operation_id": operation_id,
                "unit": unit,
                "request_sha256": request_sha256,
                "machine_id": machine_id,
                "boot_id": boot_id,
                "state": "reserved",
                "identity": None,
                "release_decision": False,
                "release_written": False,
                "termination_proof": None,
            }
            self._create_record(record_path, record)

            operation_path = self._operation_path(operation_id)
            try:
                self._create_operation_directory(operation_path)
                descriptor = {
                    "version": PROTOCOL_VERSION,
                    "operation_id": operation_id,
                    "request_sha256": request_sha256,
                    "argv": canonical["argv"],
                    "workspace": canonical["workspace"],
                    "release_file": str(operation_path / "release"),
                    "environment": self._runtime_environment(),
                }
                manifest_path = operation_path / "manifest.json"
                self._write_public_file(manifest_path, _canonical_json(descriptor), exclusive=True)
                self._sync_directory(operation_path)
                record["state"] = "starting"
                self._write_record(record_path, record)
                properties = self._unit_properties(
                    unit=unit,
                    request_sha256=request_sha256,
                    workspace=canonical["workspace"],
                    environment=descriptor["environment"],
                )
                if isinstance(self.manager, SystemdManager):
                    _assert_trusted_executable(self.wrapper_path, expected_owner_uid=self.expected_owner_uid)
                self.manager.start(unit, self.wrapper_path, str(manifest_path), properties)
            except Exception as exc:
                # Keep the durable reservation. The manager call may have crossed
                # its side-effect boundary, so a retry must inspect, never start.
                record["state"] = "unknown"
                record["last_error"] = type(exc).__name__
                self._write_record(record_path, record)
                if isinstance(exc, OperationError):
                    raise
                raise OperationUnavailable("operation start is uncertain") from exc

            start_timeout = min(
                self.operation_timeout,
                getattr(self.manager, "start_timeout", self.operation_timeout),
            )
            deadline = time.monotonic() + start_timeout
            while time.monotonic() < deadline:
                try:
                    snapshot = self.manager.inspect(unit)
                    identity = self._identity_from_snapshot(record, snapshot)
                    if identity is not None and self._is_live(
                        snapshot, self.manager.cgroup_populated(identity["control_group"])
                    ):
                        record["identity"] = identity
                        record["state"] = "prepared"
                        self._write_record(record_path, record)
                        return identity
                except OperationUnavailable:
                    pass
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))

            record["state"] = "unknown"
            record["last_error"] = "identity-timeout"
            self._write_record(record_path, record)
            raise OperationUnavailable("started unit identity was not confirmed")

    def claim_stream(self, identity: Mapping[str, Any]) -> subprocess.Popen[bytes]:
        checked = self._validate_identity_shape(identity)
        with self._locked_records():
            _, record = self._record_for_identity_with_path(checked)
            if record is None or not self._identity_equal(record.get("identity"), checked):
                raise OperationUnavailable("operation stream identity is not recorded")
            self._verify_live_identity(record, checked)
            return self.manager.claim_stream(checked["unit"])

    def release(self, identity: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._validate_identity_shape(identity)
        with self._locked_records():
            record_path, record = self._record_for_identity_with_path(checked)
            if record is None or record_path is None:
                return self._status_result(checked, "unknown")
            if not self._identity_equal(record.get("identity"), checked):
                return self._status_result(checked, "unknown")
            if record.get("state") == "terminated" and self._valid_termination_proof(record):
                return self._status_result(checked, "terminated", record)

            try:
                snapshot, populated, cgroup_state, status = self._observe_current_operation(
                    record_path, record, checked
                )
            except OperationError:
                return self._status_result(checked, "unknown")
            if status == "terminated":
                self._cleanup_if_proven(record, checked)
                return self._status_result(checked, status, record)
            if cgroup_state != "present" or populated != 1 or not self._is_live(snapshot, populated):
                return self._status_result(checked, "unknown", record)

            release_path = self._release_path(checked["operation_id"])
            try:
                operation_path = release_path.parent
                self._assert_owned_directory(operation_path, mode_mask=0o022)
                if release_path.exists() or release_path.is_symlink():
                    release_info = release_path.lstat()
                    if (
                        not stat.S_ISREG(release_info.st_mode)
                        or release_info.st_uid != self.expected_owner_uid
                        or stat.S_IMODE(release_info.st_mode) != 0o644
                    ):
                        return self._status_result(checked, "unknown")
                    content = release_path.read_bytes()
                    if content == (checked["invocation_id"] + "\n").encode("ascii"):
                        record["release_decision"] = True
                        record["release_written"] = True
                        record["state"] = "released"
                        self._write_record(record_path, record)
                        return self._status_result(checked, "released", record)
                    return self._status_result(checked, "unknown")
                # The durable decision precedes the worker-visible marker. A
                # crash here is recovered by release replay without another start.
                record["release_decision"] = True
                record["state"] = "release_pending"
                self._write_record(record_path, record)
                self._write_public_file(
                    release_path,
                    (checked["invocation_id"] + "\n").encode("ascii"),
                    exclusive=True,
                )
                self._sync_directory(operation_path)
            except OSError as exc:
                raise OperationUnavailable("release marker could not be written") from exc
            record["release_written"] = True
            record["state"] = "released"
            self._write_record(record_path, record)
            return self._status_result(checked, "released", record)

    def status(self, identity: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._validate_identity_shape(identity)
        with self._locked_records():
            record_path, record = self._record_for_identity_with_path(checked)
            if record is None or record_path is None or not self._identity_equal(record.get("identity"), checked):
                return self._status_result(checked, "unknown")
            if self._valid_termination_proof(record):
                self._cleanup_if_proven(record, checked)
                return self._status_result(checked, "terminated", record)
            try:
                snapshot, populated, cgroup_state, status = self._observe_current_operation(
                    record_path, record, checked
                )
            except OperationError:
                return self._status_result(checked, "unknown", record)

            if status == "terminated":
                self._cleanup_if_proven(record, checked)
                return self._status_result(checked, status, record)
            if cgroup_state == "present" and populated == 1 and self._is_live(snapshot, populated):
                marker_state = self._release_marker_state(checked)
                if marker_state == "mismatch":
                    return self._status_result(checked, "unknown", record)
                if marker_state == "matching" and not record.get("release_written"):
                    record["release_decision"] = True
                    record["release_written"] = True
                    record["state"] = "released"
                    self._write_record(record_path, record)
                state = "running" if record.get("release_written") else "held"
                return self._status_result(checked, state, record)
            return self._status_result(checked, "unknown", record)

    def candidate_bundle(
        self,
        identity: Mapping[str, Any],
        input_sha256: str,
        offset: int,
        expected_sha256: str,
        expected_bytes: int,
    ) -> dict[str, Any]:
        """Read one bounded chunk from an exact, successful candidate producer run."""

        checked = self._validate_identity_shape(identity)
        if not self.candidate_bundle_available:
            raise OperationUnavailable("candidate bundle transport is not configured")
        if not isinstance(input_sha256, str) or not _HEX_64_RE.fullmatch(input_sha256):
            raise OperationValidationError("candidate input identity is invalid")
        if not isinstance(expected_sha256, str) or not _HEX_64_RE.fullmatch(expected_sha256):
            raise OperationValidationError("candidate bundle digest is invalid")
        if (type(expected_bytes) is not int or not 1 <= expected_bytes <= MAX_CANDIDATE_BUNDLE_BYTES
                or type(offset) is not int or offset < 0 or offset > expected_bytes
                or (offset != expected_bytes and offset % CANDIDATE_BUNDLE_CHUNK_BYTES != 0)):
            raise OperationValidationError("candidate bundle bounds are invalid")

        with self._locked_records():
            _record_path, record = self._record_for_identity_with_path(checked)
            if (record is None or not self._identity_equal(record.get("identity"), checked)
                    or record.get("state") != "terminated" or not self._valid_termination_proof(record)):
                raise OperationUnavailable("candidate producer operation is unavailable")
            proof = record["termination_proof"]
            if proof.get("exec_main_code") != "exited" or proof.get("exec_main_status") != 0:
                raise OperationUnavailable("candidate producer operation did not succeed")

            manifest = self._read_operation_manifest(checked, record)
            argv = manifest.get("argv")
            if not self._trusted_candidate_producer_argv(argv, input_sha256):
                raise OperationUnavailable("operation is not the trusted candidate producer")
            workspace = manifest.get("workspace")
            opened_snapshot, bundle_fd, candidates_fd, filename = self._open_candidate_bundle(
                workspace,
                input_sha256,
            )
            try:
                self._assert_candidate_file_stable(
                    candidates_fd, filename, bundle_fd, opened_snapshot
                )
                if opened_snapshot[5] != expected_bytes:
                    self._invalidate_candidate_bundle_cache(
                        self._candidate_bundle_cache_base_key(
                            checked, input_sha256, expected_sha256, expected_bytes
                        )
                    )
                    raise OperationUnavailable("candidate bundle metadata does not match")
                cache_base = self._candidate_bundle_cache_base_key(
                    checked, input_sha256, expected_sha256, expected_bytes
                )
                verified = self._candidate_bundle_cache_lookup(cache_base, opened_snapshot)
                if verified:
                    bundle_bytes, bundle_sha256 = expected_bytes, expected_sha256
                else:
                    bundle_bytes, bundle_sha256 = self._inspect_candidate_bundle(
                        bundle_fd,
                        opened_snapshot,
                        expected_bytes=expected_bytes,
                        expected_sha256=expected_sha256,
                    )
                chunk_size = min(CANDIDATE_BUNDLE_CHUNK_BYTES, bundle_bytes - offset)
                chunk = os.pread(bundle_fd, chunk_size, offset) if hasattr(os, "pread") else self._read_at(
                    bundle_fd, offset, chunk_size
                )
                if len(chunk) != chunk_size:
                    raise OperationUnavailable("candidate bundle changed while reading")
                self._assert_candidate_file_stable(candidates_fd, filename, bundle_fd, opened_snapshot)
                if not verified:
                    self._candidate_bundle_cache_store(cache_base, opened_snapshot)
            finally:
                os.close(bundle_fd)
                os.close(candidates_fd)

        return {
            "operation_id": checked["operation_id"],
            "candidate_input_sha256": input_sha256,
            "bundle_sha256": bundle_sha256,
            "bundle_bytes": bundle_bytes,
            "offset": offset,
            "data_base64": base64.b64encode(chunk).decode("ascii"),
            "eof": offset + len(chunk) == bundle_bytes,
        }

    @staticmethod
    def _candidate_bundle_cache_base_key(
        identity: Mapping[str, str],
        input_sha256: str,
        expected_sha256: str,
        expected_bytes: int,
    ) -> tuple[Any, ...]:
        return (
            tuple(identity[key] for key in IDENTITY_KEYS),
            input_sha256,
            expected_sha256,
            expected_bytes,
        )

    def _candidate_bundle_cache_lookup(
        self,
        base_key: tuple[Any, ...],
        snapshot: tuple[int, ...],
    ) -> bool:
        key = (base_key, snapshot)
        with self._candidate_bundle_cache_lock:
            if key in self._candidate_bundle_cache:
                self._candidate_bundle_cache.move_to_end(key)
                return True
            changed_keys = [
                cached_key for cached_key in self._candidate_bundle_cache
                if cached_key[0] == base_key
            ]
            for cached_key in changed_keys:
                del self._candidate_bundle_cache[cached_key]
        if changed_keys:
            raise OperationUnavailable("candidate bundle changed after verification")
        return False

    def _candidate_bundle_cache_store(
        self,
        base_key: tuple[Any, ...],
        snapshot: tuple[int, ...],
    ) -> None:
        key = (base_key, snapshot)
        with self._candidate_bundle_cache_lock:
            self._candidate_bundle_cache[key] = None
            self._candidate_bundle_cache.move_to_end(key)
            while len(self._candidate_bundle_cache) > MAX_CANDIDATE_BUNDLE_CACHE_ENTRIES:
                self._candidate_bundle_cache.popitem(last=False)

    def _invalidate_candidate_bundle_cache(self, base_key: tuple[Any, ...]) -> None:
        with self._candidate_bundle_cache_lock:
            stale_keys = [
                cached_key for cached_key in self._candidate_bundle_cache
                if cached_key[0] == base_key
            ]
            for cached_key in stale_keys:
                del self._candidate_bundle_cache[cached_key]

    @property
    def candidate_bundle_available(self) -> bool:
        return bool(
            _HEX_40_RE.fullmatch(self.service_revision)
            and _HEX_64_RE.fullmatch(self.candidate_producer_command_sha256)
        )

    def _trusted_candidate_producer_argv(self, argv: Any, input_sha256: str) -> bool:
        """Match the root-configured inline producer and bind it to these inputs."""
        if (not isinstance(argv, list) or len(argv) != 6
                or argv[0] != "/usr/bin/env"
                or not isinstance(argv[1], str)
                or not argv[1].startswith("WORKSTREAM_INPUTS_JSON=")
                or not self.candidate_bundle_available):
            return False
        try:
            inputs = json.loads(
                argv[1][len("WORKSTREAM_INPUTS_JSON="):],
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
            candidate = inputs["candidate"]
            if not isinstance(candidate, dict) or not candidate.get("thread_id"):
                return False
            candidate_context = inputs.get("candidate_context")
            if candidate_context is None:
                input_value = candidate
            else:
                if (not isinstance(candidate_context, dict)
                        or set(candidate_context) != {"id", "repair_parent_sha", "validation_base"}):
                    return False
                context_id = candidate_context.get("id")
                repair_parent_sha = candidate_context.get("repair_parent_sha")
                validation_base = candidate_context.get("validation_base")
                if (not isinstance(context_id, str) or not _HEX_64_RE.fullmatch(context_id)
                        or not isinstance(repair_parent_sha, str)
                        or not (_HEX_40_RE.fullmatch(repair_parent_sha)
                                or _HEX_64_RE.fullmatch(repair_parent_sha))
                        or not isinstance(validation_base, str)
                        or not (_HEX_40_RE.fullmatch(validation_base)
                                or _HEX_64_RE.fullmatch(validation_base))):
                    return False
                input_value = {
                    "candidate": candidate,
                    "candidate_context": candidate_context,
                }
            command_argv = argv[2:]
            command_digest = hashlib.sha256(
                json.dumps(
                    command_argv,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest()
            input_digest = hashlib.sha256(
                json.dumps(
                    input_value,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode("utf-8")
            ).hexdigest()
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            return False
        return (
            command_argv[:3] == ["/usr/bin/python3", "-I", "-c"]
            and command_digest == self.candidate_producer_command_sha256
            and input_digest == input_sha256
        )

    def _read_operation_manifest(
        self,
        identity: Mapping[str, str],
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Read the root-owned immutable descriptor using no-follow directory opens."""
        root_fd = -1
        operation_fd = -1
        descriptor_fd = -1
        try:
            root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            root_fd = self._open_absolute_directory(self.operation_dir)
            self._check_open_directory(root_fd, owner_uid=self.expected_owner_uid, private=True)
            operation_name = hashlib.sha256(identity["operation_id"].encode("utf-8")).hexdigest()
            operation_fd = os.open(operation_name, root_flags, dir_fd=root_fd)
            self._check_open_directory(
                operation_fd,
                owner_uid=self.expected_owner_uid,
                exact_mode=0o755,
                private=True,
            )
            descriptor_fd = os.open(
                "manifest.json",
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=operation_fd,
            )
            before = os.fstat(descriptor_fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != self.expected_owner_uid
                    or stat.S_IMODE(before.st_mode) != 0o644 or before.st_nlink != 1
                    or before.st_size > 64 * 1024):
                raise OperationUnavailable("operation descriptor is unsafe")
            chunks = bytearray()
            while len(chunks) <= 64 * 1024:
                block = os.read(descriptor_fd, min(16 * 1024, 64 * 1024 + 1 - len(chunks)))
                if not block:
                    break
                chunks.extend(block)
            after = os.fstat(descriptor_fd)
            path_info = os.stat("manifest.json", dir_fd=operation_fd, follow_symlinks=False)
            if (len(chunks) > 64 * 1024 or not self._same_file_snapshot(before, after)
                    or not self._same_file_snapshot(before, path_info)):
                raise OperationUnavailable("operation descriptor changed while reading")
            try:
                manifest = json.loads(
                    bytes(chunks).decode("utf-8"),
                    object_pairs_hook=_unique_json_object,
                )
            except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
                raise OperationUnavailable("operation descriptor is corrupt") from exc
        except OperationError:
            raise
        except OSError as exc:
            raise OperationUnavailable("operation descriptor is unavailable") from exc
        finally:
            for descriptor in (descriptor_fd, operation_fd, root_fd):
                if descriptor >= 0:
                    os.close(descriptor)

        expected_keys = {
            "version", "operation_id", "request_sha256", "argv", "workspace", "release_file", "environment",
        }
        if (not isinstance(manifest, dict) or set(manifest) != expected_keys
                or manifest.get("version") != PROTOCOL_VERSION
                or manifest.get("operation_id") != identity["operation_id"]
                or manifest.get("request_sha256") != identity["request_sha256"]
                or record.get("request_sha256") != identity["request_sha256"]
                or manifest.get("release_file") != str(self._release_path(identity["operation_id"]))
                or not isinstance(manifest.get("environment"), dict)):
            raise OperationUnavailable("operation descriptor does not match its identity")
        try:
            request = self._validate_request({
                "operation_id": manifest["operation_id"],
                "argv": manifest["argv"],
                "workspace": manifest["workspace"],
            })
        except (OperationValidationError, KeyError, TypeError) as exc:
            raise OperationUnavailable("operation descriptor request is invalid") from exc
        if _sha256_json(request) != identity["request_sha256"]:
            raise OperationUnavailable("operation descriptor digest does not match")
        return {**manifest, "argv": request["argv"], "workspace": request["workspace"]}

    def _open_candidate_bundle(
        self,
        workspace: Any,
        input_sha256: str,
    ) -> tuple[tuple[int, ...], int, int, str]:
        """Open the canonical worker output beneath its workspace without following links."""
        if not isinstance(workspace, str) or not os.path.isabs(workspace):
            raise OperationUnavailable("candidate workspace is invalid")
        root_fd = workspace_fd = git_fd = candidates_fd = file_fd = -1
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            root_fd = self._open_absolute_directory(self.workspace_root_path)
            self._check_open_directory(root_fd, owner_uid=self.expected_owner_uid, private=True)
            try:
                relative_parts = Path(workspace).relative_to(self.workspace_root).parts
            except ValueError as exc:
                raise OperationUnavailable("candidate workspace is outside the configured root") from exc
            if not relative_parts or any(part in ("", ".", "..") for part in relative_parts):
                raise OperationUnavailable("candidate workspace is invalid")
            current_fd = root_fd
            opened: list[int] = []
            for index, part in enumerate(relative_parts):
                child = os.open(part, directory_flags, dir_fd=current_fd)
                opened.append(child)
                if index == len(relative_parts) - 1:
                    self._check_open_directory(child, owner_uid=self._worker_uid(), private=True)
                else:
                    self._check_open_directory(
                        child,
                        allowed_owners={self.expected_owner_uid, self._worker_uid()},
                        private=True,
                    )
                current_fd = child
            workspace_fd = current_fd
            git_fd = os.open(".git", directory_flags, dir_fd=workspace_fd)
            self._check_open_directory(git_fd, owner_uid=self._worker_uid(), private=True)
            candidates_fd = os.open("factory-candidates", directory_flags, dir_fd=git_fd)
            self._check_open_directory(
                candidates_fd,
                owner_uid=self._worker_uid(),
                exact_mode=0o700,
                private=True,
            )
            filename = f"{input_sha256}.bundle"
            file_fd = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=candidates_fd,
            )
            info = os.fstat(file_fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != self._worker_uid()
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                    or not 1 <= info.st_size <= MAX_CANDIDATE_BUNDLE_BYTES):
                raise OperationUnavailable("candidate bundle file is unsafe")
            by_name = os.stat(filename, dir_fd=candidates_fd, follow_symlinks=False)
            if not self._same_file_object(info, by_name):
                raise OperationUnavailable("candidate bundle changed while opening")
            result_fd = file_fd
            result_directory_fd = candidates_fd
            result_snapshot = self._file_snapshot(info)
            file_fd = -1
            candidates_fd = -1
            return result_snapshot, result_fd, result_directory_fd, filename
        except OperationError:
            raise
        except (OSError, ValueError) as exc:
            raise OperationUnavailable("candidate bundle is unavailable") from exc
        finally:
            for descriptor in (file_fd, candidates_fd, git_fd, workspace_fd, root_fd):
                if descriptor >= 0:
                    os.close(descriptor)
            for descriptor in locals().get("opened", []):
                if descriptor != workspace_fd:
                    with contextlib.suppress(OSError):
                        os.close(descriptor)

    def _inspect_candidate_bundle(
        self,
        descriptor: int,
        opened_snapshot: tuple[int, ...],
        *,
        expected_bytes: int,
        expected_sha256: str,
    ) -> tuple[int, str]:
        before = os.fstat(descriptor)
        if self._file_snapshot(before) != opened_snapshot or before.st_size != expected_bytes:
            raise OperationUnavailable("candidate bundle metadata does not match")
        digest = hashlib.sha256()
        offset = 0
        while offset < expected_bytes:
            block = os.pread(descriptor, min(1024 * 1024, expected_bytes - offset), offset) if hasattr(os, "pread") else self._read_at(
                descriptor,
                offset,
                min(1024 * 1024, expected_bytes - offset),
            )
            if not block:
                raise OperationUnavailable("candidate bundle is truncated")
            digest.update(block)
            offset += len(block)
        after = os.fstat(descriptor)
        actual_sha256 = digest.hexdigest()
        if (self._file_snapshot(after) != opened_snapshot or actual_sha256 != expected_sha256
                or offset != expected_bytes):
            raise OperationUnavailable("candidate bundle digest does not match")
        return expected_bytes, actual_sha256

    def _assert_candidate_file_stable(
        self,
        directory_fd: int,
        filename: str,
        descriptor: int,
        expected_snapshot: tuple[int, ...],
    ) -> None:
        try:
            opened = os.fstat(descriptor)
            path_info = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as exc:
            raise OperationUnavailable("candidate bundle changed while reading") from exc
        if (self._file_snapshot(opened) != expected_snapshot
                or self._file_snapshot(path_info) != expected_snapshot
                or not stat.S_ISREG(path_info.st_mode)):
            raise OperationUnavailable("candidate bundle changed while reading")

    @staticmethod
    def _file_snapshot(info: os.stat_result) -> tuple[int, ...]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_uid,
            info.st_nlink,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    @classmethod
    def _same_file_snapshot(cls, before: os.stat_result, after: os.stat_result) -> bool:
        return cls._file_snapshot(before) == cls._file_snapshot(after)

    @staticmethod
    def _same_file_object(left: os.stat_result, right: os.stat_result) -> bool:
        return left.st_dev == right.st_dev and left.st_ino == right.st_ino

    def _check_open_directory(
        self,
        descriptor: int,
        *,
        owner_uid: int | None = None,
        allowed_owners: set[int] | None = None,
        exact_mode: int | None = None,
        private: bool,
    ) -> None:
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode):
            raise OperationUnavailable("candidate directory is unsafe")
        allowed = allowed_owners if allowed_owners is not None else ({owner_uid} if owner_uid is not None else None)
        if allowed is not None and info.st_uid not in allowed:
            raise OperationUnavailable("candidate directory ownership is unsafe")
        mode = stat.S_IMODE(info.st_mode)
        if exact_mode is not None and mode != exact_mode:
            raise OperationUnavailable("candidate directory permissions are unsafe")
        if private and mode & 0o022:
            raise OperationUnavailable("candidate directory permissions are unsafe")

    def _worker_uid(self) -> int:
        try:
            return pwd.getpwnam(self.worker_user).pw_uid
        except (KeyError, OSError) as exc:
            raise OperationUnavailable("worker account is unavailable") from exc

    @staticmethod
    def _open_absolute_directory(path: Path) -> int:
        if not path.is_absolute() or any(component in ("", ".", "..") for component in path.parts[1:]):
            raise OperationUnavailable("configured directory path is not canonical")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open("/", flags)
        try:
            for component in path.parts[1:]:
                child = os.open(component, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except OSError:
            os.close(descriptor)
            raise

    @staticmethod
    def _read_at(descriptor: int, offset: int, length: int) -> bytes:
        os.lseek(descriptor, offset, os.SEEK_SET)
        return os.read(descriptor, length)

    def stop(self, identity: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._validate_identity_shape(identity)
        with self._locked_records():
            record_path, record = self._record_for_identity_with_path(checked)
            if record is None or record_path is None or not self._identity_equal(record.get("identity"), checked):
                return self._status_result(checked, "unknown")
            if self._valid_termination_proof(record):
                self._cleanup_if_proven(record, checked)
                return self._status_result(checked, "terminated", record)

            try:
                snapshot, populated, cgroup_state, status = self._observe_current_operation(
                    record_path, record, checked
                )
            except OperationError:
                return self._status_result(checked, "unknown", record)
            if status == "terminated":
                self._cleanup_if_proven(record, checked)
                return self._status_result(checked, status, record)
            if cgroup_state != "present" or populated != 1 or not self._is_live(snapshot, populated):
                return self._status_result(checked, "unknown", record)

            try:
                # Every signal follows a fresh exact identity check. The durable
                # unit remains active/exited until proof is written below.
                self._verify_live_identity(record, checked)
                self.manager.kill(checked["unit"], "TERM")
                status = self._wait_for_termination(record_path, record, checked, self.stop_timeout)
                if status != "terminated":
                    self._verify_live_identity(record, checked)
                    self.manager.kill(checked["unit"], "KILL")
                    status = self._wait_for_termination(record_path, record, checked, self.stop_timeout)
            except OperationError:
                return self._status_result(checked, "unknown", record)

            if status != "terminated":
                return self._status_result(checked, "unknown", record)
            # The proof is durable before cleanup can unload the unit identity.
            self._cleanup_if_proven(record, checked)
            return self._status_result(checked, "terminated", record)

    def _wait_for_termination(
        self,
        record_path: Path,
        record: dict[str, Any],
        identity: Mapping[str, str],
        timeout: float,
    ) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                _, _, cgroup_state, status = self._observe_current_operation(
                    record_path, record, identity
                )
            except OperationError:
                return "unknown"
            if status == "terminated":
                return status
            if cgroup_state not in ("present", "released"):
                return "unknown"
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        return "unknown"

    def _observe_current_operation(
        self,
        record_path: Path,
        record: dict[str, Any],
        identity: Mapping[str, str],
    ) -> tuple[UnitSnapshot, int | None, str | None, str]:
        snapshot, populated, cgroup_state, version = self._inspect_operation_state(record, identity)
        if cgroup_state is None or populated is None:
            return snapshot, populated, cgroup_state, "unknown"
        status = self._observe_termination(
            record_path, record, identity, snapshot, populated, cgroup_state, version
        )
        return snapshot, populated, cgroup_state, status

    def _observe_termination(
        self,
        record_path: Path,
        record: dict[str, Any],
        identity: Mapping[str, str],
        snapshot: UnitSnapshot,
        populated: int,
        cgroup_state: str,
        systemd_version: int | None,
    ) -> str:
        if (
            cgroup_state not in ("present", "released")
            or systemd_version is None
            or systemd_version < SystemdManager._SYSTEMD_MIN_VERSION
            or not self._is_terminal(snapshot, populated)
        ):
            return "unknown"
        proof = {
            "machine_id": identity["machine_id"],
            "boot_id": identity["boot_id"],
            "unit": identity["unit"],
            "invocation_id": identity["invocation_id"],
            "control_group": identity["control_group"],
            "cgroup_state": cgroup_state,
            "cgroup_populated": 0,
            "active_state": snapshot.active_state,
            "sub_state": snapshot.sub_state,
            "exec_main_code": snapshot.exec_main_code,
            "exec_main_status": snapshot.exec_main_status,
            "main_pid": snapshot.main_pid,
            "systemd_version": systemd_version,
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        record["state"] = "terminated"
        record["termination_proof"] = proof
        self._write_record(record_path, record)
        return "terminated"

    def _cleanup_if_proven(self, record: Mapping[str, Any], identity: Mapping[str, str]) -> None:
        """Stop only the same loaded unit after its durable empty-cgroup proof."""

        if not self._valid_termination_proof(record):
            return
        if _read_machine_id(self.machine_id_path) != identity["machine_id"]:
            return
        if _read_boot_id(self.boot_id_path) != identity["boot_id"]:
            return
        try:
            snapshot = self._verify_unit_identity(record, identity)
            proof = record["termination_proof"]
            if proof["cgroup_state"] == "present":
                if snapshot.control_group != identity["control_group"]:
                    return
                populated = self.manager.cgroup_populated(identity["control_group"])
                version = self._systemd_version()
            elif proof["cgroup_state"] == "released":
                if snapshot.control_group != "":
                    return
                version = self._released_cgroup_proof_version()
                populated = 0
            else:
                return
            if (
                version != proof["systemd_version"]
                or not self._is_terminal(snapshot, populated)
            ):
                return
            self.manager.stop(identity["unit"])
        except OperationError:
            # A missing or changed identity remains unknown for cleanup purposes;
            # the durable termination proof still safely frees worker capacity.
            return

    def _is_terminal(self, snapshot: UnitSnapshot, populated: int) -> bool:
        if (
            populated != 0
            or isinstance(snapshot.main_pid, bool)
            or snapshot.main_pid != 0
            or isinstance(snapshot.exec_main_status, bool)
            or not isinstance(snapshot.exec_main_status, int)
            or snapshot.exec_main_code not in ("none", "exited", "killed", "dumped")
        ):
            return False
        if snapshot.active_state == "active" and snapshot.sub_state == "exited":
            return True
        if snapshot.active_state == "failed" and snapshot.sub_state == "failed":
            return True
        if snapshot.active_state == "inactive" and snapshot.sub_state in ("dead", "failed"):
            return True
        return False

    def _is_live(self, snapshot: UnitSnapshot, populated: int) -> bool:
        return (
            populated == 1
            and snapshot.active_state == "active"
            and snapshot.sub_state == "running"
            and snapshot.main_pid > 0
        )

    def _verify_live_identity(self, record: Mapping[str, Any], identity: Mapping[str, str]) -> UnitSnapshot:
        snapshot = self._verify_unit_identity(record, identity)
        if snapshot.control_group != identity["control_group"]:
            raise OperationUnavailable("live systemd cgroup does not match the operation")
        return snapshot

    def _verify_unit_identity(self, record: Mapping[str, Any], identity: Mapping[str, str]) -> UnitSnapshot:
        current_machine = _read_machine_id(self.machine_id_path)
        current_boot = _read_boot_id(self.boot_id_path)
        if current_machine != identity["machine_id"] or current_boot != identity["boot_id"]:
            raise OperationUnavailable("operation host or boot identity changed")
        if record.get("machine_id") != current_machine or record.get("boot_id") != current_boot:
            raise OperationUnavailable("recorded operation belongs to another host or boot")
        if self.expected_machine_id and current_machine != self.expected_machine_id:
            raise OperationUnavailable("worker machine ID does not match protected configuration")
        try:
            snapshot = self.manager.inspect(identity["unit"])
        except OperationError:
            raise
        if snapshot is None or not self._snapshot_identity_matches(snapshot, identity):
            raise OperationUnavailable("live systemd identity does not match the operation")
        return snapshot

    def _snapshot_matches(self, snapshot: UnitSnapshot, identity: Mapping[str, str]) -> bool:
        return (
            snapshot.control_group == identity["control_group"]
            and self._snapshot_identity_matches(snapshot, identity)
        )

    def _snapshot_identity_matches(self, snapshot: UnitSnapshot, identity: Mapping[str, str]) -> bool:
        return (
            snapshot.load_state == "loaded"
            and snapshot.invocation_id == identity["invocation_id"]
            and snapshot.user == self.worker_user
            and snapshot.group == self.worker_group
            and snapshot.syslog_identifier == _syslog_identifier(identity["request_sha256"])
            and snapshot.supplementary_groups in (None, "")
        )

    def _systemd_version(self) -> int | None:
        try:
            facts = self.manager.capabilities()
        except (OperationError, OSError, subprocess.TimeoutExpired):
            return None
        version = facts.get("systemd_version") if isinstance(facts, Mapping) else None
        if isinstance(version, int) and not isinstance(version, bool) and version >= SystemdManager._SYSTEMD_MIN_VERSION:
            return version
        return None

    def _released_cgroup_proof_version(self) -> int | None:
        try:
            facts = self.manager.capabilities()
        except (OperationError, OSError, subprocess.TimeoutExpired):
            return None
        if not isinstance(facts, Mapping) or not facts.get("released_cgroup_proof"):
            return None
        return self._systemd_version()

    def _inspect_operation_state(
        self,
        record: Mapping[str, Any],
        identity: Mapping[str, str],
    ) -> tuple[UnitSnapshot, int | None, str | None, int | None]:
        """Inspect the exact invocation and safely classify its cgroup evidence."""

        snapshot = self._verify_unit_identity(record, identity)
        version = self._systemd_version()
        if version is None:
            raise OperationUnavailable("qualified systemd version is unavailable")
        if snapshot.control_group == identity["control_group"]:
            populated = self.manager.cgroup_populated(identity["control_group"])
            return snapshot, populated, "present", version
        if snapshot.control_group == "":
            released_version = self._released_cgroup_proof_version()
            if released_version is not None and self._is_terminal(snapshot, 0):
                return snapshot, 0, "released", released_version
            return snapshot, None, None, version
        raise OperationUnavailable("live systemd cgroup does not match the operation")

    @staticmethod
    def _identity_equal(recorded: Any, supplied: Mapping[str, str]) -> bool:
        return (
            isinstance(recorded, dict)
            and set(recorded) == set(IDENTITY_KEYS)
            and all(recorded.get(key) == supplied.get(key) for key in IDENTITY_KEYS)
        )

    def _identity_from_snapshot(
        self,
        record: Mapping[str, Any],
        snapshot: UnitSnapshot | None,
    ) -> dict[str, str] | None:
        if snapshot is None:
            return None
        invocation_id = snapshot.invocation_id.lower()
        operation_id = record.get("operation_id")
        unit = record.get("unit")
        control_group = f"/system.slice/{unit}"
        identity = {
            "kind": "systemd-unit",
            "operation_id": operation_id,
            "machine_id": record.get("machine_id"),
            "boot_id": record.get("boot_id"),
            "unit": unit,
            "invocation_id": invocation_id,
            "control_group": control_group,
            "request_sha256": record.get("request_sha256"),
        }
        if not _HEX_32_RE.fullmatch(invocation_id):
            return None
        if not self._snapshot_matches(snapshot, identity):
            return None
        return identity

    def _recover_identity(self, record: dict[str, Any]) -> dict[str, str] | None:
        identity = record.get("identity")
        if isinstance(identity, dict):
            try:
                checked = self._validate_identity_shape(identity)
            except OperationValidationError:
                checked = None
            if (
                checked is not None
                and checked["operation_id"] == record.get("operation_id")
                and checked["unit"] == record.get("unit")
                and checked["request_sha256"] == record.get("request_sha256")
                and checked["machine_id"] == record.get("machine_id")
                and checked["boot_id"] == record.get("boot_id")
            ):
                return checked
        if record.get("machine_id") != _read_machine_id(self.machine_id_path):
            return None
        if record.get("boot_id") != _read_boot_id(self.boot_id_path):
            return None
        try:
            snapshot = self.manager.inspect(str(record.get("unit", "")))
        except OperationError:
            return None
        recovered = self._identity_from_snapshot(record, snapshot)
        if recovered is not None:
            record["identity"] = recovered
            released = self._release_marker_matches(recovered)
            record["release_decision"] = bool(released)
            record["release_written"] = bool(released)
            record["state"] = "released" if released else "prepared"
            self._write_record(self._record_path(recovered["operation_id"]), record)
        return recovered

    def _assert_capacity_available(self) -> None:
        for path in sorted(self.records_dir.glob("*.json")):
            try:
                record = self._read_record(path, missing_ok=False)
            except OperationError:
                raise OperationCapacityError("operation ledger is unreadable; capacity remains reserved")
            if record is None:
                continue
            if record.get("state") == "terminated" and self._valid_termination_proof(record):
                continue
            raise OperationCapacityError("factory-worker already has an active or unproven operation")

    def _validate_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(request, Mapping) or set(request) != {"operation_id", "argv", "workspace"}:
            raise OperationValidationError("request must contain only operation_id, argv and workspace")
        operation_id = request["operation_id"]
        if not isinstance(operation_id, str) or not operation_id:
            raise OperationValidationError("operation_id must be a nonempty string up to 200 UTF-8 bytes")
        try:
            operation_id_size = len(operation_id.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise OperationValidationError("operation_id must be valid UTF-8") from exc
        if operation_id_size > 200:
            raise OperationValidationError("operation_id must be a nonempty string up to 200 UTF-8 bytes")
        if any(ord(char) < 0x20 or ord(char) == 0x7f for char in operation_id):
            raise OperationValidationError("operation_id contains a control character")

        argv_value = request["argv"]
        if not isinstance(argv_value, list) or not argv_value or len(argv_value) > 128:
            raise OperationValidationError("argv must be a nonempty list with at most 128 items")
        argv: list[str] = []
        total = 0
        for index, value in enumerate(argv_value):
            if not isinstance(value, str) or "\0" in value:
                raise OperationValidationError("argv items must be strings without NUL")
            try:
                encoded_size = len(value.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise OperationValidationError("argv items must be valid UTF-8 strings") from exc
            total += encoded_size
            if encoded_size > 4096 or total > 32768:
                raise OperationValidationError("argv exceeds the bounded request size")
            if index == 0:
                if not os.path.isabs(value):
                    raise OperationValidationError("argv[0] must be an absolute executable")
                try:
                    executable = Path(value).resolve(strict=True)
                    mode = executable.stat().st_mode
                except OSError as exc:
                    raise OperationValidationError("argv[0] executable does not exist") from exc
                if not stat.S_ISREG(mode) or not os.access(executable, os.X_OK):
                    raise OperationValidationError("argv[0] must resolve to an executable file")
                argv.append(str(executable))
            else:
                argv.append(value)

        workspace_value = request["workspace"]
        if (not isinstance(workspace_value, str) or not os.path.isabs(workspace_value)
                or any(char in workspace_value for char in ("\0", "\n", "\r", "$", "%"))):
            raise OperationValidationError("workspace must be an absolute path without systemd substitutions")
        try:
            requested_workspace = Path(workspace_value)
            workspace = requested_workspace.resolve(strict=True)
            workspace_stat = workspace.stat()
            root = self.workspace_root.resolve(strict=True)
        except OSError as exc:
            raise OperationValidationError("workspace or configured workspace root is unavailable") from exc
        if str(requested_workspace) != str(workspace) or not stat.S_ISDIR(workspace_stat.st_mode):
            raise OperationValidationError("workspace must be a canonical directory path")
        try:
            workspace.relative_to(root)
        except ValueError as exc:
            raise OperationValidationError("workspace is outside the allowed workspace root") from exc
        if workspace == root:
            raise OperationValidationError("workspace must be below the allowed workspace root")
        return {"operation_id": operation_id, "argv": argv, "workspace": str(workspace)}

    def _validate_identity_shape(self, identity: Mapping[str, Any]) -> dict[str, str]:
        if not isinstance(identity, Mapping) or set(identity) != set(IDENTITY_KEYS):
            raise OperationValidationError("identity has an invalid shape")
        checked: dict[str, str] = {}
        for key in IDENTITY_KEYS:
            value = identity[key]
            if not isinstance(value, str):
                raise OperationValidationError("identity values must be strings")
            checked[key] = value
        if checked["kind"] != "systemd-unit":
            raise OperationValidationError("unsupported operation identity kind")
        if checked["unit"] != _unit_name(checked["operation_id"]):
            raise OperationValidationError("identity unit does not match operation ID")
        if not _HEX_32_RE.fullmatch(checked["invocation_id"]):
            raise OperationValidationError("identity invocation ID is invalid")
        if not _HEX_32_RE.fullmatch(checked["machine_id"]):
            raise OperationValidationError("identity machine ID is invalid")
        if not _HEX_32_RE.fullmatch(checked["boot_id"]):
            raise OperationValidationError("identity boot ID is invalid")
        if checked["control_group"] != f"/system.slice/{checked['unit']}":
            raise OperationValidationError("identity control group is invalid")
        if not _HEX_64_RE.fullmatch(checked["request_sha256"]):
            raise OperationValidationError("identity request digest is invalid")
        return checked

    def _unit_properties(
        self,
        *,
        unit: str,
        request_sha256: str,
        workspace: str,
        environment: Mapping[str, str],
    ) -> dict[str, str | Sequence[str]]:
        try:
            worker_account = pwd.getpwnam(self.worker_user)
            worker_group = grp.getgrnam(self.worker_group)
        except KeyError as exc:
            raise OperationUnavailable("configured worker account does not exist") from exc
        worker_uid = worker_account.pw_uid
        worker_gid = worker_group.gr_gid
        if isinstance(self.manager, SystemdManager) and not self._worker_account_isolated():
            raise OperationUnavailable("worker account is not an isolated non-root identity")
        if not Path(self.worker_home).is_absolute():
            raise OperationValidationError("worker home must be absolute")
        read_write_paths = [workspace, self.worker_home, "/srv/factory/tmp", *self.cache_paths]
        for path in read_write_paths:
            if not os.path.isabs(path):
                raise OperationValidationError("writable paths must be absolute")
        inaccessible = [
            str(self.records_dir),
            getattr(self.manager, "control_socket_path", "/run/factory-operations/control.sock"),
            "/run/systemd/private",
            "/run/dbus/system_bus_socket",
            "/run/user",
        ]
        if isinstance(self.manager, SystemdManager) and worker_account.pw_dir != self.worker_home:
            raise OperationUnavailable("configured worker home does not match the passwd entry")
        del worker_uid, worker_gid
        return {
            "Type": "exec",
            "RemainAfterExit": "yes",
            "Restart": "no",
            "KillMode": "control-group",
            "SendSIGKILL": "yes",
            "TimeoutStopSec": "2s",
            "Slice": "system.slice",
            "User": self.worker_user,
            "Group": self.worker_group,
            "WorkingDirectory": workspace,
            "SupplementaryGroups": "",
            "NoNewPrivileges": "yes",
            "CapabilityBoundingSet": "",
            "AmbientCapabilities": "",
            "ProtectControlGroups": "yes",
            "ProtectSystem": "strict",
            "ProtectHome": "read-only",
            "ProtectKernelTunables": "yes",
            "ProtectKernelModules": "yes",
            "ProtectHostname": "yes",
            "PrivateTmp": "yes",
            "Delegate": "no",
            "RestrictSUIDSGID": "yes",
            "LockPersonality": "yes",
            "SystemCallArchitectures": "native",
            "UMask": "0077",
            "SyslogIdentifier": _syslog_identifier(request_sha256),
            "ReadWritePaths": read_write_paths,
            "InaccessiblePaths": inaccessible,
            "Environment": [f"{name}={value}" for name, value in environment.items()],
        }

    def _runtime_environment(self) -> dict[str, str]:
        machine = os.uname().machine.lower()
        if machine in ("x86_64", "amd64"):
            node_arch = "x64"
        elif machine in ("aarch64", "arm64"):
            node_arch = "arm64"
        else:
            raise OperationUnavailable("worker architecture has no qualified runtime path")
        return {
            "HOME": self.worker_home,
            "USER": self.worker_user,
            "LOGNAME": self.worker_user,
            "PATH": (
                "/srv/factory/bootstrap-tools-v1/cargo/bin:"
                f"/srv/factory/bootstrap-tools-v1/rig-tools/node-v24.19.0-linux-{node_arch}/bin:"
                "/srv/factory/bootstrap-tools-v1/rig-tools/codex/bin:"
                "/usr/local/bin:/usr/bin:/bin"
            ),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TMPDIR": "/srv/factory/tmp",
            "XDG_CACHE_HOME": f"{self.worker_home}/.cache",
            "CARGO_HOME": f"{self.worker_home}/.cargo",
            "RUSTUP_HOME": "/srv/factory/bootstrap-tools-v1/rustup",
            "CODEX_HOME": f"{self.worker_home}/.codex",
            "MISE_DATA_DIR": "/srv/factory/mise",
            "MISE_CACHE_DIR": f"{self.worker_home}/.cache/mise",
            "MIX_HOME": f"{self.worker_home}/.mix",
            "CARGO_BUILD_JOBS": "3",
        }

    def _worker_account_isolated(self) -> bool:
        """Require a dedicated, non-root account with no extra group access."""

        if isinstance(self.manager, SystemdManager) and (
            self.worker_user != "factory-worker"
            or self.worker_group != "factory-worker"
            or self.expected_owner_uid != 0
        ):
            return False
        try:
            account = pwd.getpwnam(self.worker_user)
            group = grp.getgrnam(self.worker_group)
            memberships = set(os.getgrouplist(self.worker_user, account.pw_gid))
            primary_users = {
                entry.pw_name for entry in pwd.getpwall() if entry.pw_gid == group.gr_gid
            }
        except (KeyError, OSError, AttributeError):
            return False
        return (
            account.pw_uid != 0
            and account.pw_gid == group.gr_gid
            and memberships == {group.gr_gid}
            and primary_users == {self.worker_user}
            and not any(name != self.worker_user for name in group.gr_mem)
        )

    def _record_path(self, operation_id: str) -> Path:
        return self.records_dir / f"{hashlib.sha256(operation_id.encode('utf-8')).hexdigest()}.json"

    def _operation_path(self, operation_id: str) -> Path:
        return self.operation_dir / hashlib.sha256(operation_id.encode("utf-8")).hexdigest()

    def _release_path(self, operation_id: str) -> Path:
        return self._operation_path(operation_id) / "release"

    @contextlib.contextmanager
    def _locked_records(self) -> Iterator[None]:
        self._assert_owned_directory(self.records_dir, exact_mode=0o700)
        lock_path = self.records_dir / ".lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            lock_stat = os.fstat(descriptor)
            if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid != self.expected_owner_uid:
                raise OperationUnavailable("operation ledger lock is unsafe")
            if stat.S_IMODE(lock_stat.st_mode) & 0o077:
                raise OperationUnavailable("operation ledger lock permissions are unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _assert_owned_directory(
        self,
        path: Path,
        *,
        exact_mode: int | None = None,
        mode_mask: int | None = None,
    ) -> None:
        try:
            info = path.lstat()
        except OSError as exc:
            raise OperationUnavailable("operation directory is unavailable") from exc
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != self.expected_owner_uid:
            raise OperationUnavailable("operation directory ownership is unsafe")
        mode = stat.S_IMODE(info.st_mode)
        if exact_mode is not None and mode != exact_mode:
            raise OperationUnavailable("operation directory permissions are unsafe")
        if mode_mask is not None and mode & mode_mask:
            raise OperationUnavailable("operation directory must not be writable by group or others")

    def _assert_storage_layout(self) -> None:
        self._assert_owned_directory(self.operation_dir, exact_mode=0o755)
        try:
            private_root = self.records_dir.resolve(strict=True)
            public_root = self.operation_dir.resolve(strict=True)
        except OSError as exc:
            raise OperationUnavailable("operation storage roots are unavailable") from exc
        if (
            private_root == public_root
            or private_root in public_root.parents
            or public_root in private_root.parents
        ):
            raise OperationUnavailable("private ledger and worker-visible files must be separate")

    def _create_operation_directory(self, path: Path) -> None:
        self._assert_owned_directory(path.parent, exact_mode=0o755)
        try:
            path.mkdir(mode=0o755)
        except FileExistsError as exc:
            raise OperationUnavailable("operation descriptor directory already exists") from exc
        os.chmod(path, 0o755)
        self._assert_owned_directory(path, exact_mode=0o755)
        self._sync_directory(path.parent)

    def _create_record(self, path: Path, record: Mapping[str, Any]) -> None:
        self._assert_owned_directory(self.records_dir, exact_mode=0o700)
        payload = _canonical_json(record)
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError as exc:
            raise OperationConflict("operation ID is already reserved") from exc
        try:
            self._write_all(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._sync_directory(self.records_dir)

    def _write_record(self, path: Path, record: Mapping[str, Any]) -> None:
        self._assert_owned_directory(self.records_dir, exact_mode=0o700)
        current = self._read_record(path, missing_ok=False)
        if current is None:
            raise OperationUnavailable("operation record disappeared")
        payload = _canonical_json(record)
        descriptor, temporary_name = tempfile.mkstemp(prefix=".record-", dir=self.records_dir)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            self._write_all(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, path)
            self._sync_directory(self.records_dir)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def _read_record(self, path: Path, *, missing_ok: bool) -> dict[str, Any] | None:
        try:
            info = path.lstat()
        except FileNotFoundError:
            if missing_ok:
                return None
            raise OperationUnavailable("operation record is missing")
        except OSError as exc:
            raise OperationUnavailable("operation record cannot be inspected") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_uid != self.expected_owner_uid or stat.S_IMODE(info.st_mode) != 0o600:
            raise OperationUnavailable("operation record ownership or permissions are unsafe")
        try:
            with path.open("r", encoding="utf-8") as source:
                value = json.load(source)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise OperationUnavailable("operation record is corrupt") from exc
        if not isinstance(value, dict) or value.get("version") != PROTOCOL_VERSION:
            raise OperationUnavailable("operation record version is invalid")
        return value

    def _write_public_file(self, path: Path, content: bytes, *, exclusive: bool) -> None:
        self._assert_owned_directory(path.parent, mode_mask=0o022)
        if exclusive:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o644,
            )
            try:
                os.fchmod(descriptor, 0o644)
                self._write_all(descriptor, content)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            info = path.lstat()
            if info.st_uid != self.expected_owner_uid or not stat.S_ISREG(info.st_mode):
                raise OperationUnavailable("worker-visible operation file ownership is unsafe")
            return
        raise OperationValidationError("public operation files are immutable")

    @staticmethod
    def _write_all(descriptor: int, content: bytes) -> None:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _record_for_identity_with_path(
        self,
        identity: Mapping[str, str],
    ) -> tuple[Path | None, dict[str, Any] | None]:
        path = self._record_path(identity["operation_id"])
        try:
            record = self._read_record(path, missing_ok=True)
        except OperationError:
            return path, None
        if record is None:
            return path, None
        if record.get("operation_id") != identity["operation_id"]:
            return path, None
        return path, record

    def _valid_termination_proof(self, record: Mapping[str, Any]) -> bool:
        proof = record.get("termination_proof")
        identity = record.get("identity")
        if (
            not isinstance(proof, dict)
            or set(proof) != set(TERMINATION_PROOF_KEYS)
            or not isinstance(identity, dict)
        ):
            return False
        return (
            record.get("state") == "terminated"
            and proof.get("machine_id") == record.get("machine_id") == identity.get("machine_id")
            and proof.get("boot_id") == record.get("boot_id") == identity.get("boot_id")
            and proof.get("unit") == record.get("unit") == identity.get("unit")
            and proof.get("invocation_id") == identity.get("invocation_id")
            and proof.get("control_group") == identity.get("control_group")
            and proof.get("cgroup_state") in ("present", "released")
            and type(proof.get("cgroup_populated")) is int
            and proof.get("cgroup_populated") == 0
            and type(proof.get("main_pid")) is int
            and proof.get("main_pid") == 0
            and isinstance(proof.get("systemd_version"), int)
            and not isinstance(proof.get("systemd_version"), bool)
            and proof.get("systemd_version") >= SystemdManager._SYSTEMD_MIN_VERSION
            and isinstance(proof.get("exec_main_status"), int)
            and not isinstance(proof.get("exec_main_status"), bool)
            and proof.get("exec_main_code") in ("none", "exited", "killed", "dumped")
            and isinstance(proof.get("observed_at"), str)
            and bool(proof.get("observed_at"))
            and (
                (proof.get("active_state") == "active" and proof.get("sub_state") == "exited")
                or (proof.get("active_state") == "failed" and proof.get("sub_state") == "failed")
                or (proof.get("active_state") == "inactive" and proof.get("sub_state") in ("dead", "failed"))
            )
        )

    def _release_marker_state(self, identity: Mapping[str, str]) -> str:
        path = self._release_path(identity["operation_id"])
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != self.expected_owner_uid or stat.S_IMODE(info.st_mode) != 0o644:
                return "mismatch"
            if path.read_bytes() == (identity["invocation_id"] + "\n").encode("ascii"):
                return "matching"
            return "mismatch"
        except FileNotFoundError:
            return "absent"
        except OSError:
            return "mismatch"

    def _release_marker_matches(self, identity: Mapping[str, str]) -> bool:
        return self._release_marker_state(identity) == "matching"

    def _status_result(
        self,
        identity: Mapping[str, str],
        status: str,
        record: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"operation_id": identity["operation_id"], "status": status}
        if record is not None and isinstance(record.get("identity"), dict):
            result["identity"] = dict(record["identity"])
        else:
            result["identity"] = dict(identity)
        if status == "terminated" and record is not None and isinstance(record.get("termination_proof"), dict):
            result["termination_proof"] = dict(record["termination_proof"])
            result["exit_status"] = record["termination_proof"].get("exec_main_status")
            result["exit_code"] = record["termination_proof"].get("exec_main_code")
        return result


def _unit_name(operation_id: str) -> str:
    digest = hashlib.sha256(operation_id.encode("utf-8")).hexdigest()
    return f"factory-operation-{digest}.service"


def _syslog_identifier(request_sha256: str) -> str:
    return f"factory-operation-{request_sha256[:32]}"


def _normalize_exec_main_code(value: str) -> str:
    return {
        "0": "none",
        "1": "exited",
        "2": "killed",
        "3": "dumped",
        "none": "none",
        "exited": "exited",
        "killed": "killed",
        "dumped": "dumped",
    }.get(value.lower(), "unknown")


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _read_machine_id(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="ascii").strip().lower()
    except (OSError, UnicodeError):
        return ""
    if not re.fullmatch(r"[0-9a-f]{32}", value):
        return ""
    return value


def _read_boot_id(path: str) -> str:
    try:
        value = Path(path).read_text(encoding="ascii").strip().lower().replace("-", "")
    except (OSError, UnicodeError):
        return ""
    if not _HEX_32_RE.fullmatch(value):
        return ""
    return value


def _assert_trusted_executable(path: str, *, expected_owner_uid: int) -> Path:
    try:
        executable = Path(path).resolve(strict=True)
        info = executable.stat()
    except OSError as exc:
        raise OperationUnavailable("configured executable is unavailable") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != expected_owner_uid
        or stat.S_IMODE(info.st_mode) & 0o022
        or not os.access(executable, os.X_OK)
    ):
        raise OperationUnavailable("configured executable ownership or permissions are unsafe")
    parent = executable.parent
    while True:
        try:
            parent_info = parent.stat()
        except OSError as exc:
            raise OperationUnavailable("configured executable parent is unavailable") from exc
        if not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != expected_owner_uid or stat.S_IMODE(parent_info.st_mode) & 0o022:
            raise OperationUnavailable("configured executable parent ownership or permissions are unsafe")
        if parent == parent.parent:
            break
        parent = parent.parent
    return executable


def _systemd_path_value(value: str) -> str:
    """Escape one path word for a systemd unit-property array."""

    if not os.path.isabs(value) or "\0" in value or "\n" in value or "\r" in value:
        raise OperationValidationError("systemd paths must be absolute single-line paths")
    # systemd's unit-file parser accepts C-style hex escapes inside path words.
    return _systemd_word(value)


def _systemd_word(value: str) -> str:
    if "\0" in value or "\n" in value or "\r" in value:
        raise OperationValidationError("systemd property values must be single-line values")
    return "".join("\\x20" if char == " " else "\\x5c" if char == "\\" else char for char in value)


__all__ = [
    "IDENTITY_KEYS",
    "PROTOCOL_VERSION",
    "OperationCapacityError",
    "OperationConflict",
    "OperationController",
    "OperationError",
    "OperationUnavailable",
    "OperationValidationError",
    "SystemdManager",
    "UnitSnapshot",
]
