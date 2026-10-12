from __future__ import annotations

import hashlib
import grp
import io
import json
import os
from pathlib import Path
import pwd
import select
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import textwrap
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from factory.deploy import worker_operation_transport as transport
from factory.deploy import worker_operation as engine
from factory.deploy import worker_operation_exec as wrapper


REVISION = "c" * 40
RELEASE_SHA256 = "d" * 64
MACHINE_ID = "a" * 32
BOOT_ID = "1" * 32
INVOCATION_ID = "b" * 32


def load_shipped_local_commit_command() -> list[str]:
    workstream_path = Path(__file__).resolve().parents[3] / "factory/workstreams/software-change.yaml"
    source = workstream_path.read_text(encoding="utf-8")
    local_commit = source.index("  - id: local-commit\n")
    command_start = source.index("      command:\n", local_commit) + len("      command:\n")
    command_end = source.index("      timeout_ms:", command_start)
    command_lines = source[command_start:command_end].splitlines(keepends=True)
    if command_lines[:4] != [
        "        - /usr/bin/python3\n",
        "        - -I\n",
        "        - -c\n",
        "        - |\n",
    ]:
        raise AssertionError("shipped local-commit command shape changed")
    script = textwrap.dedent("".join(command_lines[4:]))
    return ["/usr/bin/python3", "-I", "-c", script]


def compact_utf8_command_sha256(command_argv: list[str]) -> str:
    raw = json.dumps(command_argv, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class HeldWrapperCommandTest(unittest.TestCase):
    def test_fixed_manifest_command_reaches_held_wrapper(self):
        held = Mock(return_value=37)
        module = SimpleNamespace(held_wrapper_main=held)
        spec = SimpleNamespace(name="_factory_operation_transport", loader=SimpleNamespace(exec_module=lambda _module: None))
        with patch.object(sys, "argv", ["wrapper", "--manifest", "/protected/manifest.json"]), \
                patch.object(Path, "lstat", return_value=SimpleNamespace(st_mode=stat.S_IFREG | 0o644, st_uid=0)), \
                patch.object(wrapper.importlib.util, "spec_from_file_location", return_value=spec), \
                patch.object(wrapper.importlib.util, "module_from_spec", return_value=module), \
                patch.dict(sys.modules):
            self.assertEqual(wrapper.main(), 37)
        held.assert_called_once_with("/protected/manifest.json")

    def test_other_command_shapes_do_not_load_the_helper(self):
        with patch.object(wrapper.importlib.util, "spec_from_file_location") as load:
            for argv in (["wrapper", "/manifest"], ["wrapper", "--other", "/manifest"], ["wrapper", "--manifest"]):
                with patch.object(sys, "argv", argv):
                    self.assertEqual(wrapper.main(), 1)
            load.assert_not_called()


def identity(operation_id: str = "run-alpha/implement/1") -> dict[str, str]:
    unit = f"factory-operation-{hashlib.sha256(operation_id.encode()).hexdigest()}.service"
    return {
        "kind": "systemd-unit",
        "operation_id": operation_id,
        "machine_id": MACHINE_ID,
        "boot_id": BOOT_ID,
        "unit": unit,
        "invocation_id": INVOCATION_ID,
        "control_group": f"/system.slice/{unit}",
        "request_sha256": "e" * 64,
    }


def approved_environment(home: Path) -> dict[str, str]:
    return {
        "HOME": str(home),
        "USER": "factory-worker",
        "LOGNAME": "factory-worker",
        "PATH": (
            "/srv/factory/bootstrap-tools-v1/cargo/bin:"
            "/srv/factory/bootstrap-tools-v1/rig-tools/node-v24.19.0-linux-x64/bin:"
            "/srv/factory/bootstrap-tools-v1/rig-tools/codex/bin:/usr/local/bin:/usr/bin:/bin"
        ),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TMPDIR": "/srv/factory/tmp",
        "CODEX_HOME": str(home / ".codex"),
        "CARGO_HOME": str(home / ".cargo"),
        "RUSTUP_HOME": "/srv/factory/bootstrap-tools-v1/rustup",
        "XDG_CACHE_HOME": str(home / ".cache"),
        "MISE_DATA_DIR": "/srv/factory/mise",
        "MISE_CACHE_DIR": str(home / ".cache/mise"),
        "MIX_HOME": str(home / ".mix"),
        "CARGO_BUILD_JOBS": "3",
    }


def write_manifest(operation_root: Path, workspace: Path, worker_home: Path,
                   argv: list[str], *, operation_id: str = "run-alpha/implement/1") -> Path:
    operation_dir = operation_root / hashlib.sha256(operation_id.encode()).hexdigest()
    operation_dir.mkdir(mode=0o755)
    os.chmod(operation_dir, 0o755)
    request = {"operation_id": operation_id, "argv": argv, "workspace": str(workspace.resolve(strict=True))}
    canonical = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    manifest = {
        "version": 1,
        **request,
        "request_sha256": hashlib.sha256(canonical).hexdigest(),
        "release_file": str(operation_dir / "release"),
        "environment": approved_environment(worker_home),
    }
    path = operation_dir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    path.chmod(0o644)
    return path


class CountingController:
    def __init__(self, *, status: str = "held") -> None:
        self.manager = self
        self.controlled_status = status
        self.calls: list[str] = []
        self._claimed = False
        self._process: subprocess.Popen[bytes] | None = None
        self.identity = identity()

    def capabilities(self):
        self.calls.append("capabilities")
        return {
            "protocol_version": 1,
            "contained": True,
            "machine_id": MACHINE_ID,
            "boot_id": BOOT_ID,
        }

    def prepare(self, request):
        self.calls.append("prepare")
        return identity(request["operation_id"])

    def release(self, value):
        self.calls.append("release")
        self.controlled_status = "running"
        return {"operation_id": value["operation_id"], "identity": value, "status": "released"}

    def status(self, value):
        self.calls.append("status")
        status = self.controlled_status
        result = {"operation_id": value["operation_id"], "identity": value, "status": status}
        if self._process is not None and self._process.poll() is not None:
            status = "terminated"
            result["status"] = status
            result.update({"exit_status": 0, "exit_code": "exited"})
            result["termination_proof"] = {
                "machine_id": value["machine_id"],
                "boot_id": value["boot_id"],
                "unit": value["unit"],
                "invocation_id": value["invocation_id"],
                "control_group": value["control_group"],
                "cgroup_state": "released",
                "cgroup_populated": 0,
                "active_state": "active",
                "sub_state": "exited",
                "exec_main_code": "exited",
                "exec_main_status": 0,
                "main_pid": 0,
                "systemd_version": 252,
                "observed_at": datetime.now(timezone.utc).isoformat(),
            }
        return result

    def stop(self, value):
        self.calls.append("stop")
        return {"operation_id": value["operation_id"], "identity": value, "status": "unknown"}

    def claim_stream(self, value):
        self.calls.append("claim_stream")
        if value != self.identity or self._claimed:
            raise RuntimeError("one-shot stream unavailable")
        self._claimed = True
        self._process = subprocess.Popen(
            [sys.executable, "-u", "-c",
             "import sys; print('started', flush=True); line=sys.stdin.buffer.readline(); "
             "print('got:'+line.decode('ascii').strip(), flush=True)"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )
        return self._process


class ReconnectableController(CountingController):
    def __init__(self, launch_log: Path, *, initialize_delay: float = 0.5) -> None:
        super().__init__()
        self.launch_log = launch_log
        self.effects_log = launch_log.with_name("effects.log")
        self.initialize_delay = initialize_delay

    def status(self, value):
        self.calls.append("status")
        if value != self.identity:
            return {"operation_id": value.get("operation_id"), "identity": self.identity, "status": "unknown"}
        if self._process is not None and self._process.poll() is not None:
            result = {"operation_id": value["operation_id"], "identity": value, "status": "terminated"}
            result.update({"exit_status": self._process.returncode, "exit_code": "exited"})
            result["termination_proof"] = {
                "machine_id": value["machine_id"],
                "boot_id": value["boot_id"],
                "unit": value["unit"],
                "invocation_id": value["invocation_id"],
                "control_group": value["control_group"],
                "cgroup_state": "released",
                "cgroup_populated": 0,
                "active_state": "active",
                "sub_state": "exited",
                "exec_main_code": "exited",
                "exec_main_status": self._process.returncode,
                "main_pid": 0,
                "systemd_version": 252,
                "observed_at": datetime.now(timezone.utc).isoformat(),
            }
            return result
        return {"operation_id": value["operation_id"], "identity": value, "status": self.controlled_status}

    def claim_stream(self, value):
        self.calls.append("claim_stream")
        if value != self.identity or self._claimed:
            raise RuntimeError("one-shot stream unavailable")
        self._claimed = True
        code = (
            "import json,sys,time\n"
            f"open({str(self.launch_log)!r}, 'a', encoding='ascii').write('launch\\n')\n"
            "print(json.dumps({'event':'ready'}), flush=True)\n"
            "for line in sys.stdin:\n"
            " request=json.loads(line)\n"
            f" open({str(self.effects_log)!r}, 'a', encoding='ascii').write(request.get('method', '')+'\\n')\n"
            " print(json.dumps({'id':request.get('id'),'result':'accepted'}), flush=True)\n"
            " if request.get('method') == 'initialize':\n"
            f"  time.sleep({self.initialize_delay!r})\n"
            "  print(json.dumps({'event':'continued'}), flush=True)\n"
            " if request.get('method') == 'finish':\n"
            "  time.sleep(0.8)\n"
            "  print(json.dumps({'event':'final'}), flush=True)\n"
            "  break\n"
        )
        self._process = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )
        return self._process

    def stop(self, value):
        self.calls.append("stop")
        if value != self.identity or self._process is None:
            return {"operation_id": value["operation_id"], "identity": self.identity, "status": "unknown"}
        if self._process.poll() is None:
            self._process.terminate()
            self._process.wait(timeout=2)
        return self.status(value)


class BufferedOutputController(ReconnectableController):
    def __init__(self, launch_log: Path) -> None:
        super().__init__(launch_log)
        self.force_terminal = False

    def status(self, value):
        if self.force_terminal and value == self.identity:
            self.calls.append("status")
            return {
                "operation_id": value["operation_id"],
                "identity": value,
                "status": "terminated",
                "termination_proof": {
                    "machine_id": value["machine_id"],
                    "boot_id": value["boot_id"],
                    "unit": value["unit"],
                    "invocation_id": value["invocation_id"],
                    "control_group": value["control_group"],
                    "cgroup_state": "released",
                    "cgroup_populated": 0,
                    "active_state": "active",
                    "sub_state": "exited",
                    "exec_main_code": "exited",
                    "exec_main_status": 0,
                    "main_pid": 0,
                    "systemd_version": 252,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        return super().status(value)

    def claim_stream(self, value):
        self.calls.append("claim_stream")
        if value != self.identity or self._claimed:
            raise RuntimeError("one-shot stream unavailable")
        self._claimed = True
        code = (
            "import sys,time\n"
            "time.sleep(0.5)\n"
            f"sys.stdout.buffer.write(b'x' * {transport.MAX_STREAM_BUFFER})\n"
            "sys.stdout.buffer.flush()\n"
            "time.sleep(5)\n"
        )
        self._process = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )
        return self._process


class WorkerOperationTransportTests(unittest.TestCase):
    def test_stdin_pipe_forwards_short_request_before_eof(self):
        read_fd, write_fd = os.pipe()
        source = os.fdopen(read_fd, "rb")
        sender, receiver = socket.socketpair()
        thread = threading.Thread(target=transport._copy_stdin_to_socket, args=(source, sender), daemon=True)
        request = b'{"method":"initialize","id":0}\n'
        thread.start()
        try:
            os.write(write_fd, request)
            receiver.settimeout(0.5)
            self.assertEqual(receiver.recv(1024), request)
        finally:
            os.close(write_fd)
            thread.join(timeout=1)
            source.close()
            sender.close()
            receiver.close()
        self.assertFalse(thread.is_alive(), "stdin copy thread did not stop at EOF")

    def make_broker(self, directory: Path, controller, *, control_uid: int | None = None,
                    stream_timeout: float = 5.0) -> transport.OperationBroker:
        directory.chmod(0o750)
        return transport.OperationBroker(
            controller,
            control_uid=os.getuid() if control_uid is None else control_uid,
            control_gid=os.getgid(),
            service_revision=REVISION,
            release_sha256=RELEASE_SHA256,
            expected_machine_id=MACHINE_ID,
            socket_path=directory / "control.sock",
            qualification_path=directory / "qualification.json",
            expected_owner_uid=os.getuid(),
            directory_owner_uid=os.getuid(),
            stream_timeout=stream_timeout,
            request_timeout=2,
        )

    def start_broker(self, broker: transport.OperationBroker) -> threading.Thread:
        thread = threading.Thread(target=broker.serve_forever, daemon=True)
        thread.start()
        deadline = time.monotonic() + 2
        while not broker.socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(broker.socket_path.exists(), "broker did not bind its local socket")
        return thread

    def stop_broker(self, broker: transport.OperationBroker, thread: threading.Thread) -> None:
        broker.shutdown()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive(), "broker did not stop")

    def test_peer_uid_denial_happens_before_rpc_dispatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = CountingController()
            broker = self.make_broker(directory, controller, control_uid=os.getuid() + 100_000)
            thread = self.start_broker(broker)
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.connect(str(broker.socket_path))
                    response = client.recv(1024)
                self.assertEqual(json.loads(response), {"error": "permission_denied"})
                self.assertEqual(controller.calls, [])
            finally:
                self.stop_broker(broker, thread)

    def test_forced_ssh_parser_rejects_shell_syntax_and_unknown_fields(self):
        valid = transport.encode_ssh_command("rpc", {"action": "capabilities"})
        self.assertEqual(transport.parse_ssh_original_command(valid), ("rpc", {"action": "capabilities"}))
        for command in (
            "factory-operation rpc eA==;id",
            "factory-operation rpc " + "a" * 300_000,
            "factory-operation exec e30",
            "factory-operation  rpc e30",
        ):
            with self.subTest(command=command[:40]), self.assertRaises(transport.ProtocolError):
                transport.parse_ssh_original_command(command)

        raw = transport._compact_json({"action": "capabilities", "extra": True})
        token = transport.base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        with self.assertRaises(transport.ProtocolError):
            transport.parse_ssh_original_command(f"factory-operation rpc {token}")

        output = __import__("io").BytesIO()
        result = transport.ssh_client_main(
            environment={"SSH_ORIGINAL_COMMAND": "factory-operation rpc e30;id"},
            output_stream=output,
            socket_path=Path("/does/not/exist"),
            expected_socket_gid=os.getgid(),
        )
        self.assertNotEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue()), {"error": "invalid_command"})

    def test_prepare_request_rejects_nul_and_non_utf8_arguments(self):
        request = {
            "operation_id": "run-transport/prepare/1",
            "argv": ["/usr/bin/env", "ARG=value"],
            "workspace": "/srv/factory/workspace",
        }
        for argument in ("ARG=bad\x00value", "ARG=\ud800"):
            with self.subTest(argument=repr(argument)), self.assertRaises(transport.ProtocolError):
                transport.validate_prepare_request({**request, "argv": [request["argv"][0], argument]})

    def test_forced_ssh_forwards_only_bounded_candidate_wrapper_shape(self):
        command_argv = load_shipped_local_commit_command()
        self.assertGreater(len(command_argv[3].encode("utf-8")), transport.MAX_ARGUMENT_BYTES)
        inputs_json = json.dumps(
            {"candidate": {"thread_id": "task-transport", "revision": "main"}},
            sort_keys=True,
            separators=(",", ":"),
        )
        request = {
            "operation_id": "run-transport/local-commit/1",
            "argv": ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={inputs_json}", *command_argv],
            "workspace": "/srv/factory/contained-workspaces/run-transport",
        }
        envelope = {
            "action": "prepare",
            "request": request,
            "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
        }
        encoded = transport.encode_ssh_command("rpc", envelope)

        kind, forwarded = transport.parse_ssh_original_command(encoded)

        self.assertEqual(kind, "rpc")
        self.assertEqual(forwarded["request"], request)
        with self.assertRaises(transport.ProtocolError):
            transport.validate_rpc_request(envelope)

        ordinary = json.loads(json.dumps(envelope))
        ordinary["request"]["argv"] = ["/usr/bin/env", "ARG=" + "x" * 4093]
        with self.assertRaises(transport.ProtocolError):
            transport.parse_ssh_original_command(transport.encode_ssh_command("rpc", ordinary))

        oversized = json.loads(json.dumps(envelope))
        oversized_script_bytes = transport.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES + 1
        oversized_script = command_argv[3] + "x" * (
            oversized_script_bytes - len(command_argv[3].encode("utf-8"))
        )
        oversized["request"]["argv"][5] = oversized_script
        with self.assertRaises(transport.ProtocolError):
            transport.parse_ssh_original_command(transport.encode_ssh_command("rpc", oversized))

    def test_root_socket_revalidates_candidate_producer_pin_before_manager_start(self):
        command_argv = load_shipped_local_commit_command()
        command_pin = compact_utf8_command_sha256(command_argv)
        inputs_json = json.dumps(
            {"candidate": {"thread_id": "task-transport", "revision": "main"}},
            sort_keys=True,
            separators=(",", ":"),
        )
        candidate_argv = [
            "/usr/bin/env",
            f"WORKSTREAM_INPUTS_JSON={inputs_json}",
            *command_argv,
        ]

        def dispatch(candidate_pin: str | None, argv: list[str]):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                records = root / "records"
                operation_root = root / "operations"
                workspace_root = root / "workspaces"
                worker_home = root / "worker-home"
                broker_directory = root / "broker"
                for path, mode in (
                    (records, 0o700),
                    (operation_root, 0o755),
                    (workspace_root, 0o755),
                    (worker_home, 0o700),
                    (broker_directory, 0o750),
                ):
                    path.mkdir(mode=mode)
                    path.chmod(mode)
                workspace = workspace_root / "candidate"
                workspace.mkdir(mode=0o755)
                machine_file = root / "machine-id"
                machine_file.write_text(MACHINE_ID + "\n", encoding="ascii")
                boot_file = root / "boot-id"
                boot_file.write_text(BOOT_ID + "\n", encoding="ascii")

                class StartManager:
                    start_timeout = 0.5

                    def __init__(self):
                        self.starts = []
                        self.unit = None
                        self.properties = None

                    def start(self, unit, _wrapper, _descriptor_path, properties):
                        self.starts.append(unit)
                        self.unit = unit
                        self.properties = dict(properties)

                    def inspect(self, unit):
                        if unit != self.unit:
                            return None
                        return engine.UnitSnapshot(
                            load_state="loaded",
                            invocation_id=INVOCATION_ID,
                            control_group=f"/system.slice/{unit}",
                            active_state="active",
                            sub_state="running",
                            result="",
                            exec_main_code="none",
                            exec_main_status=0,
                            main_pid=4242,
                            user="factory-worker",
                            group="factory-worker",
                            syslog_identifier=self.properties["SyslogIdentifier"],
                            supplementary_groups="",
                        )

                    def cgroup_populated(self, _control_group):
                        return 1

                manager = StartManager()
                controller = engine.OperationController(
                    manager,
                    records_dir=str(records),
                    operation_dir=str(operation_root),
                    workspace_root=str(workspace_root),
                    wrapper_path="/bin/true",
                    worker_home=str(worker_home),
                    machine_id_path=str(machine_file),
                    boot_id_path=str(boot_file),
                    expected_machine_id=MACHINE_ID,
                    service_revision=REVISION,
                    candidate_producer_command_sha256=candidate_pin,
                    worker_user="factory-worker",
                    worker_group="factory-worker",
                    expected_owner_uid=os.getuid(),
                    operation_timeout=0.5,
                )
                broker = self.make_broker(broker_directory, controller)
                thread = self.start_broker(broker)
                account = SimpleNamespace(pw_uid=os.getuid(), pw_gid=os.getgid(), pw_dir=str(worker_home))
                group = SimpleNamespace(gr_gid=os.getgid())
                request = {
                    "operation_id": "run-transport/local-commit/1",
                    "argv": argv,
                    "workspace": str(workspace),
                }
                envelope = {
                    "action": "prepare",
                    "request": request,
                    "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
                }
                output = io.BytesIO()
                try:
                    with patch("factory.deploy.worker_operation.pwd.getpwnam", return_value=account), \
                            patch("factory.deploy.worker_operation.grp.getgrnam", return_value=group):
                        status = transport.ssh_client_main(
                            environment={"SSH_ORIGINAL_COMMAND": transport.encode_ssh_command("rpc", envelope)},
                            output_stream=output,
                            socket_path=broker.socket_path,
                            expected_socket_uid=os.getuid(),
                            expected_socket_gid=os.getgid(),
                            expected_directory_uid=os.getuid(),
                            expected_directory_gid=os.getgid(),
                        )
                    return status, json.loads(output.getvalue()), manager.starts
                finally:
                    self.stop_broker(broker, thread)

        valid_status, valid_response, valid_starts = dispatch(command_pin, candidate_argv)
        self.assertEqual(valid_status, 0)
        self.assertIn("ok", valid_response)
        self.assertEqual(len(valid_starts), 1)

        altered_argv = list(candidate_argv)
        altered_argv[5] += " # altered"
        altered_status, altered_response, altered_starts = dispatch(command_pin, altered_argv)
        self.assertNotEqual(altered_status, 0)
        self.assertEqual(altered_response, {"error": "invalid_request"})
        self.assertEqual(altered_starts, [])

        unpinned_status, unpinned_response, unpinned_starts = dispatch(None, candidate_argv)
        self.assertNotEqual(unpinned_status, 0)
        self.assertEqual(unpinned_response, {"error": "invalid_request"})
        self.assertEqual(unpinned_starts, [])

    def test_review_preparation_rpc_has_bounded_large_command_and_bound_operation_ids(self):
        request = {
            "current_operation_id": "run-100/review/2",
            "run_id": "run-100",
            "stable_operation_id": "run-100/review/1",
            "source_workspace": "/srv/factory/contained-workspaces/run-100",
            "identity": {
                "number": 241,
                "url": "https://github.com/RigCodeAI/rig/pull/241",
                "base_sha": "a" * 40,
                "head_sha": "b" * 40,
                "head_branch": "feature/review",
                "base_branch": "main",
                "rubric_sha256": "c" * 64,
                "cloud_thread_id": "cloud-thread-1",
                "candidate_context_id": None,
            },
            "command_argv": ["/usr/bin/python3", "-I", "-c", "#" + "x" * 4800],
        }
        envelope = {
            "action": "prepare_review_workspace",
            "request": request,
            "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
        }
        validated = transport.validate_rpc_request(envelope)
        self.assertEqual(validated["request"], request)
        self.assertGreater(len(validated["request"]["command_argv"][3].encode("utf-8")), 4096)

        mismatched = json.loads(json.dumps(envelope))
        mismatched["request"]["current_operation_id"] = "run-other/review/2"
        with self.assertRaises(transport.ProtocolError):
            transport.validate_rpc_request(mismatched)

        ordinary = {
            "action": "prepare",
            "request": {
                "operation_id": "run-100/review/2",
                "argv": request["command_argv"],
                "workspace": request["source_workspace"],
            },
            "expected_release": envelope["expected_release"],
        }
        with self.assertRaises(transport.ProtocolError):
            transport.validate_rpc_request(ordinary)

    def test_qualification_receipt_is_bound_to_current_host_boot_release_and_systemd(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            boot_file = directory / "boot-id"
            boot_file.write_text("11111111-1111-1111-1111-111111111111\n", encoding="ascii")
            receipt_path = directory / "receipt.json"
            receipt = {
                "status": "passed",
                "protocol_version": 1,
                "machine_id": MACHINE_ID,
                "boot_id": BOOT_ID,
                "service_revision": REVISION,
                "release_sha256": RELEASE_SHA256,
                "systemd_version": 256,
                "checks": [
                    "held_launch", "duplicate_prepare", "stale_identity_rejected",
                    "setsid_child_terminated", "natural_exit", "restart_recovery", "manager_reexec",
                ],
            }
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            receipt_path.chmod(0o600)
            kwargs = {
                "expected_machine_id": MACHINE_ID,
                "service_revision": REVISION,
                "release_sha256": RELEASE_SHA256,
                "systemd_version": 256,
                "boot_id_path": boot_file,
                "path": receipt_path,
                "expected_owner_uid": os.getuid(),
            }
            self.assertTrue(transport.containment_is_qualified(**kwargs))
            self.assertFalse(transport.containment_is_qualified(**{**kwargs, "systemd_version": 255}))
            receipt["systemd_version"] = 252
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertTrue(transport.containment_is_qualified(**{**kwargs, "systemd_version": 252}))
            receipt["checks"].remove("manager_reexec")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertFalse(transport.containment_is_qualified(**{**kwargs, "systemd_version": 252}))

    def test_held_stream_is_claimed_once_and_release_rpc_runs_while_it_is_open(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = CountingController()
            controller.identity = identity()
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            envelope = {
                "action": "stream",
                "identity": controller.identity,
                "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
            }
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
                    stream.settimeout(3)
                    stream.connect(str(broker.socket_path))
                    stream.sendall(transport._compact_json(envelope) + b"\n")
                    reader = stream.makefile("rb")
                    self.assertEqual(json.loads(reader.readline()), {"ok": "stream_ready"})

                    rpc = {
                        "action": "release",
                        "identity": controller.identity,
                        "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
                    }
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as release_client:
                        release_client.settimeout(2)
                        release_client.connect(str(broker.socket_path))
                        release_client.sendall(transport._compact_json(rpc) + b"\n")
                        release_client.shutdown(socket.SHUT_WR)
                        release_reply = json.loads(release_client.makefile("rb").readline())
                    self.assertEqual(release_reply["ok"]["status"], "released")

                    stream.sendall(b"ping\n")
                    self.assertEqual(reader.readline(), b"started\n")
                    self.assertEqual(reader.readline(), b"got:ping\n")
                    self.assertEqual(reader.read(), b"")

                status_request = {
                    "action": "status",
                    "identity": controller.identity,
                    "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
                }
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as status_client:
                    status_client.settimeout(2)
                    status_client.connect(str(broker.socket_path))
                    status_client.sendall(transport._compact_json(status_request) + b"\n")
                    status_client.shutdown(socket.SHUT_WR)
                    status_reply = json.loads(status_client.makefile("rb").readline())
                self.assertEqual(status_reply["ok"]["status"], "terminated")
                self.assertEqual(status_reply["ok"]["termination_proof"]["cgroup_populated"], 0)
                self.assertEqual(controller.calls.count("claim_stream"), 1)
            finally:
                self.stop_broker(broker, thread)

    def test_stream_reconnect_preserves_process_io_and_exact_stop(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            launch_log = directory / "launches.log"
            controller = ReconnectableController(launch_log, initialize_delay=1.5)
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}

            def connect_stream(value):
                stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                stream.settimeout(3)
                stream.connect(str(broker.socket_path))
                request = {"action": "stream", "identity": value, "expected_release": release}
                stream.sendall(transport._compact_json(request) + b"\n")
                reader = stream.makefile("rb")
                return stream, reader, json.loads(reader.readline())

            def rpc(action, value):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(broker.socket_path))
                    request = {"action": action, "identity": value, "expected_release": release}
                    client.sendall(transport._compact_json(request) + b"\n")
                    client.shutdown(socket.SHUT_WR)
                    return json.loads(client.makefile("rb").readline())

            try:
                stale = dict(controller.identity, invocation_id="f" * 32)
                stale_stream, stale_reader, stale_reply = connect_stream(stale)
                self.assertEqual(stale_reply, {"error": "operation_unavailable"})
                stale_reader.close()
                stale_stream.close()
                self.assertEqual(controller.calls.count("claim_stream"), 0)

                stream, reader, reply = connect_stream(controller.identity)
                self.assertEqual(reply, {"ok": "stream_ready"})
                self.assertEqual(json.loads(reader.readline()), {"event": "ready"})
                self.assertEqual(rpc("release", controller.identity)["ok"]["status"], "released")

                duplicate, duplicate_reader, duplicate_reply = connect_stream(controller.identity)
                self.assertEqual(duplicate_reply, {"error": "operation_unavailable"})
                duplicate_reader.close()
                duplicate.close()

                stream.sendall(b'{"method":"initialize","id":1}\n')
                self.assertEqual(json.loads(reader.readline()), {"id": 1, "result": "accepted"})
                session = next(iter(broker._stream_sessions.values()))
                reader.close()
                stream.close()

                detach_deadline = time.monotonic() + 1.5
                while session.current_connection() is not None and time.monotonic() < detach_deadline:
                    time.sleep(0.02)
                self.assertIsNone(session.current_connection(), "full disconnect should release the stream slot")
                self.assertIsNone(controller._process.poll(), "full disconnect must leave the operation alive")
                resumed, resumed_reader, resumed_reply = connect_stream(controller.identity)
                self.assertEqual(resumed_reply, {"ok": "stream_ready"})
                self.assertEqual(json.loads(resumed_reader.readline()), {"event": "continued"})
                resumed.sendall(b'{"method":"ping","id":2}\n')
                self.assertEqual(json.loads(resumed_reader.readline()), {"id": 2, "result": "accepted"})
                resumed_reader.close()
                resumed.close()

                stopped = rpc("stop", controller.identity)
                self.assertEqual(stopped["ok"]["status"], "terminated")
                self.assertEqual(controller._process.poll(), controller._process.returncode)
                self.assertEqual(launch_log.read_text(encoding="ascii"), "launch\n")
                self.assertEqual(controller.effects_log.read_text(encoding="ascii"), "initialize\nping\n")
                self.assertEqual(controller.calls.count("claim_stream"), 1)
                self.assertIn("stop", controller.calls)

                deadline = time.monotonic() + 3
                while broker._stream_sessions and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(broker._stream_sessions, {})
            finally:
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_stream_half_close_preserves_output_for_the_reading_peer(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = ReconnectableController(directory / "launches.log")
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}

            def rpc(action):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(broker.socket_path))
                    request = {"action": action, "identity": controller.identity, "expected_release": release}
                    client.sendall(transport._compact_json(request) + b"\n")
                    client.shutdown(socket.SHUT_WR)
                    return json.loads(client.makefile("rb").readline())

            try:
                stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                stream.settimeout(3)
                stream.connect(str(broker.socket_path))
                envelope = {
                    "action": "stream",
                    "identity": controller.identity,
                    "expected_release": release,
                }
                stream.sendall(transport._compact_json(envelope) + b"\n")
                reader = stream.makefile("rb")
                self.assertEqual(json.loads(reader.readline()), {"ok": "stream_ready"})
                self.assertEqual(json.loads(reader.readline()), {"event": "ready"})
                self.assertEqual(rpc("release")["ok"]["status"], "released")

                stream.sendall(b'{"method":"finish","id":5}\n')
                stream.shutdown(socket.SHUT_WR)
                self.assertEqual(json.loads(reader.readline()), {"id": 5, "result": "accepted"})
                self.assertEqual(json.loads(reader.readline()), {"event": "final"})
                self.assertEqual(reader.readline(), b"")
                reader.close()
                stream.close()

                session_deadline = time.monotonic() + 3
                while broker._stream_sessions and time.monotonic() < session_deadline:
                    time.sleep(0.02)
                self.assertEqual(broker._stream_sessions, {})
                self.assertEqual(controller.calls.count("claim_stream"), 1)
                self.assertEqual(controller.effects_log.read_text(encoding="ascii"), "finish\n")
                self.assertIsNotNone(controller._process.poll())
            finally:
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_forced_ssh_half_closed_stdin_still_returns_worker_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = ReconnectableController(directory / "launches.log")
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}

            def rpc(action):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(broker.socket_path))
                    request = {"action": action, "identity": controller.identity, "expected_release": release}
                    client.sendall(transport._compact_json(request) + b"\n")
                    client.shutdown(socket.SHUT_WR)
                    return json.loads(client.makefile("rb").readline())

            try:
                self.assertEqual(rpc("release")["ok"]["status"], "released")
                output = io.BytesIO()
                result = transport.ssh_client_main(
                    environment={
                        "SSH_ORIGINAL_COMMAND": transport.encode_ssh_command(
                            "stream",
                            {"identity": controller.identity, "expected_release": release},
                        ),
                    },
                    input_stream=io.BytesIO(b'{"method":"finish","id":6}\n'),
                    output_stream=output,
                    socket_path=broker.socket_path,
                    expected_socket_uid=os.getuid(),
                    expected_socket_gid=os.getgid(),
                    expected_directory_uid=os.getuid(),
                    expected_directory_gid=os.getgid(),
                )

                self.assertEqual(result, 0)
                self.assertEqual(
                    [json.loads(line) for line in output.getvalue().splitlines()],
                    [
                        {"ok": "stream_ready"},
                        {"event": "ready"},
                        {"id": 6, "result": "accepted"},
                        {"event": "final"},
                    ],
                )
                self.assertEqual(controller.calls.count("claim_stream"), 1)
                self.assertEqual(controller.effects_log.read_text(encoding="ascii"), "finish\n")
                self.assertIsNotNone(controller._process.poll())
            finally:
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_forced_ssh_process_exits_cleanly_while_stdin_remains_open(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = ReconnectableController(directory / "launches.log")
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}
            controller.release(controller.identity)
            command = transport.encode_ssh_command(
                "stream", {"identity": controller.identity, "expected_release": release},
            )
            script = """
import os, sys
from pathlib import Path
from factory.deploy.worker_operation_transport import ssh_client_main
sys.exit(ssh_client_main(
    environment={"SSH_ORIGINAL_COMMAND": sys.argv[2]},
    socket_path=Path(sys.argv[1]),
    expected_socket_uid=os.getuid(), expected_socket_gid=os.getgid(),
    expected_directory_uid=os.getuid(), expected_directory_gid=os.getgid(),
))
"""
            process = subprocess.Popen(
                [sys.executable, "-c", script, str(broker.socket_path), command],
                cwd=Path(__file__).resolve().parents[3],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                process.stdin.write(b'{"method":"finish","id":6}\n')
                process.stdin.flush()
                # communicate() would close stdin and hide the interpreter-shutdown defect.
                process.wait(timeout=5)
                self.assertEqual(process.returncode, 0, process.stderr.read().decode())
                self.assertEqual(
                    [json.loads(line) for line in process.stdout.read().splitlines()],
                    [{"ok": "stream_ready"}, {"event": "ready"},
                     {"id": 6, "result": "accepted"}, {"event": "final"}],
                )
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=2)
                for pipe in (process.stdin, process.stdout, process.stderr):
                    pipe.close()
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_terminal_detached_output_can_be_drained_and_session_reaped(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            launch_log = directory / "launches.log"
            controller = ReconnectableController(launch_log)
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}

            def rpc(action):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(broker.socket_path))
                    request = {"action": action, "identity": controller.identity, "expected_release": release}
                    client.sendall(transport._compact_json(request) + b"\n")
                    client.shutdown(socket.SHUT_WR)
                    return json.loads(client.makefile("rb").readline())

            try:
                stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                stream.settimeout(3)
                stream.connect(str(broker.socket_path))
                envelope = {"action": "stream", "identity": controller.identity, "expected_release": release}
                stream.sendall(transport._compact_json(envelope) + b"\n")
                reader = stream.makefile("rb")
                self.assertEqual(json.loads(reader.readline()), {"ok": "stream_ready"})
                first_session = next(iter(broker._stream_sessions.values()))
                self.assertEqual(json.loads(reader.readline()), {"event": "ready"})
                self.assertEqual(rpc("release")["ok"]["status"], "released")
                stream.sendall(b'{"method":"finish","id":3}\n')
                self.assertEqual(json.loads(reader.readline()), {"id": 3, "result": "accepted"})
                reader.close()
                stream.close()
                time.sleep(0.2)

                deadline = time.monotonic() + 3
                while controller._process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertIsNotNone(controller._process.poll())

                resumed = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                resumed.settimeout(3)
                resumed.connect(str(broker.socket_path))
                resumed.sendall(transport._compact_json(envelope) + b"\n")
                resumed_reader = resumed.makefile("rb")
                self.assertEqual(json.loads(resumed_reader.readline()), {"ok": "stream_ready"})
                final_line = resumed_reader.readline()
                self.assertEqual(json.loads(final_line), {"event": "final"})
                self.assertEqual(resumed_reader.readline(), b"")
                resumed_reader.close()
                resumed.close()

                deadline = time.monotonic() + 3
                while broker._stream_sessions and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(broker._stream_sessions, {})
                self.assertFalse(first_session.thread.is_alive())
                self.assertIsNotNone(controller._process.poll())
                self.assertEqual(controller.calls.count("claim_stream"), 1)

                # A subsequent exact operation can claim its own stream after
                # the prior detached session has drained and reaped.
                controller.identity = identity("run-beta/implement/1")
                controller.controlled_status = "held"
                controller._claimed = False
                controller._process = None
                second_envelope = {
                    "action": "stream",
                    "identity": controller.identity,
                    "expected_release": release,
                }
                second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                second.settimeout(3)
                second.connect(str(broker.socket_path))
                second.sendall(transport._compact_json(second_envelope) + b"\n")
                second_reader = second.makefile("rb")
                self.assertEqual(json.loads(second_reader.readline()), {"ok": "stream_ready"})
                second_session = next(iter(broker._stream_sessions.values()))
                self.assertEqual(json.loads(second_reader.readline()), {"event": "ready"})
                self.assertEqual(rpc("release")["ok"]["status"], "released")
                second.sendall(b'{"method":"finish","id":4}\n')
                self.assertEqual(json.loads(second_reader.readline()), {"id": 4, "result": "accepted"})
                self.assertEqual(json.loads(second_reader.readline()), {"event": "final"})
                self.assertEqual(second_reader.readline(), b"")
                second_reader.close()
                second.close()

                deadline = time.monotonic() + 3
                while broker._stream_sessions and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(broker._stream_sessions, {})
                self.assertFalse(second_session.thread.is_alive())
                self.assertEqual(controller.calls.count("claim_stream"), 2)
                self.assertEqual(launch_log.read_text(encoding="ascii"), "launch\nlaunch\n")
                self.assertEqual(controller.effects_log.read_text(encoding="ascii"), "finish\nfinish\n")
            finally:
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_full_terminal_buffer_is_reaped_after_bounded_retention(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            controller = BufferedOutputController(directory / "launches.log")
            broker = self.make_broker(directory, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}
            envelope = {
                "action": "stream",
                "identity": controller.identity,
                "expected_release": release,
            }

            try:
                with patch.object(transport, "TERMINAL_STREAM_RETENTION_SECONDS", 0.15):
                    stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    stream.settimeout(3)
                    stream.connect(str(broker.socket_path))
                    stream.sendall(transport._compact_json(envelope) + b"\n")
                    reader = stream.makefile("rb")
                    self.assertEqual(json.loads(reader.readline()), {"ok": "stream_ready"})
                    session = next(iter(broker._stream_sessions.values()))
                    reader.close()
                    stream.close()

                    # The child fills the bridge's bounded retained-output
                    # queue after the client is gone. Simulate the engine's
                    # exact terminal proof while the stream helper remains
                    # alive, so cleanup cannot depend on observing pipe EOF.
                    time.sleep(0.8)
                    controller.force_terminal = True
                    with self.assertLogs(transport.__name__, level="WARNING") as captured:
                        controller._process.wait(timeout=3)
                        deadline = time.monotonic() + 3
                        while broker._stream_sessions and time.monotonic() < deadline:
                            time.sleep(0.02)

                    self.assertEqual(broker._stream_sessions, {})
                    self.assertFalse(session.thread.is_alive())
                    self.assertIn("expired incomplete stream output", "\n".join(captured.output))
                    self.assertEqual(controller.calls.count("claim_stream"), 1)
            finally:
                self.stop_broker(broker, thread)
                if controller._process is not None and controller._process.poll() is None:
                    controller._process.kill()
                    controller._process.wait(timeout=2)

    def test_controller_and_broker_hold_then_release_a_real_stream_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            records = root / "records"
            operation_root = root / "gates"
            workspace_root = root / "workspaces"
            worker_home = root / "worker-home"
            for path, mode in ((records, 0o700), (operation_root, 0o755),
                               (workspace_root, 0o750), (worker_home, 0o700)):
                path.mkdir(mode=mode)
                path.chmod(mode)
            workspace = workspace_root / "candidate"
            workspace.mkdir()
            machine_file = root / "machine-id"
            machine_file.write_text(MACHINE_ID + "\n", encoding="ascii")
            boot_file = root / "boot-id"
            boot_file.write_text("11111111-1111-1111-1111-111111111111\n", encoding="ascii")
            user_account = SimpleNamespace(pw_uid=os.getuid(), pw_gid=os.getgid(), pw_dir=str(worker_home))
            worker_group = SimpleNamespace(gr_gid=os.getgid())

            class TransientManager:
                def __init__(self):
                    self.unit = None
                    self.properties = None
                    self.descriptor_path = None
                    self.process = None
                    self.claimed = set()
                    self.stopped = []

                def capabilities(self):
                    return {
                        "contained": False,
                        "system_manager": False,
                        "cgroup_v2": False,
                        "required_isolation": False,
                        "systemd_version": 256,
                        "released_cgroup_proof": True,
                    }

                def start(self, unit, wrapper, descriptor_path, properties):
                    self.unit = unit
                    self.descriptor_path = descriptor_path
                    self.properties = dict(properties)
                    self.finished_path = Path(descriptor_path).parent / "operation-finished"
                    self.stopped_path = Path(descriptor_path).parent / "manager-stopped"

                def inspect(self, unit):
                    if unit != self.unit:
                        return None
                    terminal = self.finished_path is not None and self.finished_path.exists()
                    exit_status = int(self.finished_path.read_text(encoding="ascii")) if terminal else 0
                    return engine.UnitSnapshot(
                        load_state="loaded",
                        invocation_id=INVOCATION_ID,
                        control_group="" if terminal else f"/system.slice/{unit}",
                        active_state="active",
                        sub_state="exited" if terminal else "running",
                        result="success" if terminal else "",
                        exec_main_code="exited" if terminal else "none",
                        exec_main_status=exit_status,
                        main_pid=0 if terminal else 4242,
                        user=str(self.properties["User"]),
                        group=str(self.properties["Group"]),
                        syslog_identifier=str(self.properties["SyslogIdentifier"]),
                        supplementary_groups="",
                    )

                def cgroup_populated(self, _control_group):
                    return 0 if self.finished_path.exists() else 1

                def claim_stream(self, unit):
                    if unit != self.unit or unit in self.claimed:
                        raise engine.OperationUnavailable("stream already claimed")
                    self.claimed.add(unit)
                    descriptor = Path(self.descriptor_path)
                    helper = Path(transport.__file__).resolve(strict=True)
                    wrapper_code = (
                        "import importlib.util,sys; "
                        f"spec=importlib.util.spec_from_file_location('_held_transport', {str(helper)!r}); "
                        "module=importlib.util.module_from_spec(spec); sys.modules[spec.name]=module; "
                        "spec.loader.exec_module(module); "
                        f"raise SystemExit(module.held_wrapper_main({str(descriptor)!r}, "
                        f"expected_owner_uid={os.getuid()}, expected_worker_gid={os.getgid()}, "
                        f"operation_root={str(operation_root)!r}, workspace_root={str(workspace_root)!r}, "
                        f"worker_home={str(worker_home)!r}))"
                    )
                    code = (
                        "import pathlib,subprocess,sys,time\n"
                        f"child=subprocess.Popen([sys.executable,'-c',{wrapper_code!r}], "
                        "stdin=sys.stdin.buffer,stdout=sys.stdout.buffer,stderr=sys.stdout.buffer, "
                        f"cwd={str(workspace)!r},env={{'INVOCATION_ID':{INVOCATION_ID!r}}})\n"
                        "result=child.wait()\n"
                        f"pathlib.Path({str(self.finished_path)!r}).write_text(str(result))\n"
                        f"stopped=pathlib.Path({str(self.stopped_path)!r})\n"
                        "deadline=time.monotonic()+5\n"
                        "while not stopped.exists() and time.monotonic()<deadline:\n"
                        "    time.sleep(.01)\n"
                        "raise SystemExit(0)\n"
                    )
                    self.process = subprocess.Popen(
                        [sys.executable, "-c", code],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        bufsize=0,
                        cwd=workspace,
                        env={"INVOCATION_ID": INVOCATION_ID},
                    )
                    return self.process

                def stop(self, unit):
                    self.stopped.append(unit)
                    self.stopped_path.write_text("stopped\n", encoding="ascii")

                def kill(self, unit, signal_name):
                    raise AssertionError(f"unexpected fake signal: {unit} {signal_name}")

            manager = TransientManager()
            controller = engine.OperationController(
                manager,
                records_dir=str(records),
                operation_dir=str(operation_root),
                workspace_root=str(workspace_root),
                wrapper_path=str(Path(transport.__file__).resolve().with_name("worker_operation_exec.py")),
                worker_home=str(worker_home),
                machine_id_path=str(machine_file),
                boot_id_path=str(boot_file),
                worker_user="factory-worker",
                worker_group="factory-worker",
                expected_machine_id=MACHINE_ID,
                expected_owner_uid=os.getuid(),
            )
            controller._worker_account_isolated = lambda: True
            broker = self.make_broker(root, controller)
            thread = self.start_broker(broker)
            release = {"service_revision": REVISION, "release_sha256": RELEASE_SHA256}

            def rpc(request):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(broker.socket_path))
                    client.sendall(transport._compact_json(request) + b"\n")
                    client.shutdown(socket.SHUT_WR)
                    return json.loads(client.makefile("rb").readline())

            request = {
                "operation_id": "integration-run/implement/1",
                "argv": [str(Path(sys.executable).resolve(strict=True)), "-u", "-c",
                         "import sys; print('started', flush=True); line=sys.stdin.buffer.readline(); "
                         "print('got:'+line.decode('ascii').strip(), flush=True)"],
                "workspace": str(workspace),
            }
            try:
                with patch("factory.deploy.worker_operation.pwd.getpwnam", return_value=user_account), \
                        patch("factory.deploy.worker_operation.grp.getgrnam", return_value=worker_group):
                    capabilities = rpc({"action": "capabilities"})
                    self.assertEqual(capabilities["ok"]["machine_id"], MACHINE_ID)
                    self.assertFalse(capabilities["ok"]["containment_qualified"])
                    prepared = rpc({"action": "prepare", "request": request, "expected_release": release})
                    self.assertIn("ok", prepared)
                    prepared_identity = prepared["ok"]
                    self.assertEqual(prepared_identity["boot_id"], BOOT_ID)
                    held = rpc({"action": "status", "identity": prepared_identity,
                                "expected_release": release})
                    self.assertEqual(held["ok"]["status"], "held")

                stream_request = {
                    "action": "stream",
                    "identity": prepared_identity,
                    "expected_release": release,
                }
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
                    stream.settimeout(3)
                    stream.connect(str(broker.socket_path))
                    stream.sendall(transport._compact_json(stream_request) + b"\n")
                    reader = stream.makefile("rb")
                    self.assertEqual(json.loads(reader.readline()), {"ok": "stream_ready"})
                    self.assertEqual(
                        rpc({"action": "status", "identity": prepared_identity,
                             "expected_release": release})["ok"]["status"],
                        "held",
                    )
                    readable, _, _ = select.select([stream], [], [], 0.1)
                    if readable:
                        self.fail(f"held stream emitted before release: {reader.readline()!r}")

                    released = rpc({"action": "release", "identity": prepared_identity,
                                    "expected_release": release})
                    self.assertEqual(released["ok"]["status"], "released")
                    self.assertEqual(reader.readline(), b"started\n")
                    stream.sendall(b"ping\n")
                    self.assertEqual(reader.readline(), b"got:ping\n")
                    for _ in range(30):
                        if manager.stopped:
                            break
                        time.sleep(0.1)
                    if not manager.stopped:
                        current = rpc({"action": "status", "identity": prepared_identity,
                                       "expected_release": release})
                        self.fail(f"stream status poll did not clean up terminal unit: {current!r}")
                    self.assertEqual(reader.read(), b"")

                terminal = rpc({"action": "status", "identity": prepared_identity,
                                "expected_release": release})["ok"]
                self.assertEqual(terminal["status"], "terminated")
                self.assertEqual(terminal["termination_proof"]["cgroup_state"], "released")
                self.assertEqual(terminal["termination_proof"]["systemd_version"], 256)
                self.assertEqual(manager.claimed, {prepared_identity["unit"]})
                self.assertIn(prepared_identity["unit"], manager.stopped)
                self.assertEqual(set(manager.stopped), {prepared_identity["unit"]})
            finally:
                self.stop_broker(broker, thread)
                if manager.process is not None and manager.process.poll() is None:
                    manager.process.kill()
                    manager.process.wait(timeout=2)

    def test_held_wrapper_execs_only_after_matching_release_and_cleans_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            operation_root = base / "gates"
            operation_root.mkdir(mode=0o755)
            workspace_root = base / "workspaces"
            workspace_root.mkdir(mode=0o750)
            workspace = workspace_root / "candidate"
            workspace.mkdir()
            worker_home = base / "worker-home"
            worker_home.mkdir()
            invocation = "f" * 32
            executable = str(Path(sys.executable).resolve(strict=True))
            child = "import json,os; print(json.dumps({'keys':sorted(os.environ), 'leak':os.environ.get('LEAK')}))"
            manifest_path = write_manifest(
                operation_root, workspace, worker_home,
                [executable, "-c", child],
            )
            release_path = Path(json.loads(manifest_path.read_text())["release_file"])
            wrapper_code = (
                "import sys; from factory.deploy import worker_operation_transport as t; "
                f"raise SystemExit(t.held_wrapper_main({str(manifest_path)!r}, "
                f"expected_owner_uid={os.getuid()}, expected_worker_gid={os.getgid()}, "
                f"operation_root={str(operation_root)!r}, workspace_root={str(workspace_root)!r}, "
                f"worker_home={str(worker_home)!r}, wait_timeout=3))"
            )
            env = dict(os.environ, INVOCATION_ID=invocation, LEAK="must-not-reach-worker")
            process = subprocess.Popen(
                [sys.executable, "-c", wrapper_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=env,
            )

            def release() -> None:
                time.sleep(0.15)
                release_path.write_text(invocation + "\n", encoding="ascii")
                release_path.chmod(0o644)

            releaser = threading.Thread(target=release)
            releaser.start()
            stdout, stderr = process.communicate(timeout=5)
            releaser.join(timeout=2)
            self.assertEqual(process.returncode, 0, stderr)
            child_environment = json.loads(stdout)
            self.assertIsNone(child_environment["leak"])
            self.assertEqual(
                set(child_environment["keys"]) - {"__CF_USER_TEXT_ENCODING"},
                set(approved_environment(worker_home)),
            )

    def test_held_wrapper_rejects_stale_release_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            operation_root = base / "gates"
            operation_root.mkdir(mode=0o755)
            workspace_root = base / "workspaces"
            workspace_root.mkdir(mode=0o750)
            workspace = workspace_root / "candidate"
            workspace.mkdir()
            worker_home = base / "worker-home"
            worker_home.mkdir()
            executable = str(Path(sys.executable).resolve(strict=True))
            manifest_path = write_manifest(operation_root, workspace, worker_home, [executable, "-c", "print('ran')"])
            manifest = json.loads(manifest_path.read_text())
            Path(manifest["release_file"]).write_text("0" * 32 + "\n", encoding="ascii")
            Path(manifest["release_file"]).chmod(0o644)
            result = transport.held_wrapper_main(
                manifest_path,
                expected_owner_uid=os.getuid(),
                expected_worker_gid=os.getgid(),
                operation_root=operation_root,
                workspace_root=workspace_root,
                worker_home=worker_home,
                wait_timeout=0.2,
                environ={"INVOCATION_ID": INVOCATION_ID},
                exec_function=lambda *_args: self.fail("stale release must not execute"),
            )
            self.assertEqual(result, 1)

    def test_held_wrapper_rejects_a_worker_writable_workspace_parent(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            gates = base / "gates"
            gates.mkdir(mode=0o755)
            workspaces = base / "workspaces"
            workspaces.mkdir(mode=0o750)
            workspaces.chmod(0o750)
            workspace = workspaces / "candidate"
            workspace.mkdir()
            home = base / "worker-home"
            home.mkdir()
            manifest = write_manifest(gates, workspace, home, [str(Path(sys.executable).resolve()), "-c", "print('unexpected')"])
            workspaces.chmod(0o770)
            result = transport.held_wrapper_main(
                manifest, expected_owner_uid=os.getuid(),
                expected_worker_gid=os.getgid(), operation_root=gates, workspace_root=workspaces,
                worker_home=home, wait_timeout=0, environ={"INVOCATION_ID": INVOCATION_ID},
                exec_function=lambda *_args: self.fail("writable workspace parent must not execute"),
            )
            self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
