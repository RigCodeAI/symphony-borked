from __future__ import annotations

import base64
import ctypes
import grp
import hashlib
import json
import os
import pwd
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from factory.deploy import worker_operation as engine
from factory.deploy import worker_operation_transport as transport


REVISION = "c" * 40
MACHINE_ID = "a" * 32
BOOT_ID = "b" * 32
INVOCATION_ID = "d" * 32
RELEASE_SHA256 = "e" * 64


def digest_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


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
    return hashlib.sha256(
        json.dumps(command_argv, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def make_identity(operation_id: str, request_sha256: str) -> dict[str, str]:
    unit = f"factory-operation-{hashlib.sha256(operation_id.encode()).hexdigest()}.service"
    return {
        "kind": "systemd-unit",
        "operation_id": operation_id,
        "machine_id": MACHINE_ID,
        "boot_id": BOOT_ID,
        "unit": unit,
        "invocation_id": INVOCATION_ID,
        "control_group": f"/system.slice/{unit}",
        "request_sha256": request_sha256,
    }


class CandidateBundleRpcTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.records = self.root / "records"
        self.operations = self.root / "operations"
        self.workspace_root = self.root / "workspaces"
        for path, mode in ((self.records, 0o700), (self.operations, 0o755), (self.workspace_root, 0o755)):
            path.mkdir(mode=mode)
            path.chmod(mode)

        self.workspace = self.workspace_root / "candidate"
        self.workspace.mkdir(mode=0o755)
        git_dir = self.workspace / ".git"
        git_dir.mkdir(mode=0o755)
        self.candidates = git_dir / "factory-candidates"
        self.candidates.mkdir(mode=0o700)
        self.candidate = {"thread_id": "task-100", "revision": "main"}
        self.input_sha256 = digest_json(self.candidate)
        self.inputs_json = json.dumps({"candidate": self.candidate}, sort_keys=True, separators=(",", ":"))
        self.script = "print('pinned local candidate producer')"
        self.command_argv = ["/usr/bin/python3", "-I", "-c", self.script]
        self.command_sha256 = digest_json(self.command_argv)
        self.argv = [
            "/usr/bin/env",
            f"WORKSTREAM_INPUTS_JSON={self.inputs_json}",
            *self.command_argv,
        ]
        self.operation_id = "run-100/local-commit/1"
        self.controller = engine.OperationController(
            object(),
            records_dir=str(self.records),
            operation_dir=str(self.operations),
            workspace_root=str(self.workspace_root),
            wrapper_path="/bin/true",
            worker_home=str(self.root / "worker-home"),
            machine_id_path=str(self.root / "machine-id"),
            boot_id_path=str(self.root / "boot-id"),
            expected_machine_id=MACHINE_ID,
            service_revision=REVISION,
            candidate_producer_command_sha256=self.command_sha256,
            worker_user=pwd.getpwuid(os.getuid()).pw_name,
            worker_group=grp.getgrgid(os.getgid()).gr_name,
            expected_owner_uid=os.getuid(),
        )
        (self.root / "machine-id").write_text(MACHINE_ID + "\n", encoding="ascii")
        (self.root / "boot-id").write_text(BOOT_ID + "\n", encoding="ascii")
        self.bundle_path = self.candidates / f"{self.input_sha256}.bundle"
        self.bundle_bytes = bytes(range(256)) * 640 + b"tail"
        self.write_bundle(self.bundle_bytes)
        self.identity = self.install_operation(argv=self.argv)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_root_request_accepts_the_shipped_pinned_local_commit_command(self):
        command_argv = load_shipped_local_commit_command()
        script_size = len(command_argv[3].encode("utf-8"))
        self.assertGreater(script_size, 4096)
        self.assertLessEqual(script_size, engine.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES)
        self.controller.candidate_producer_command_sha256 = compact_utf8_command_sha256(command_argv)
        inputs_json = json.dumps({"candidate": self.candidate}, sort_keys=True, separators=(",", ":"))
        request = {
            "operation_id": "run-100/local-commit/1",
            "argv": ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={inputs_json}", *command_argv],
            "workspace": str(self.workspace),
        }

        validated = self.controller._validate_request(request)

        self.assertEqual(validated["argv"][5], command_argv[3])

    def test_large_script_exception_requires_the_exact_root_pinned_candidate_command(self):
        command_argv = load_shipped_local_commit_command()
        self.controller.candidate_producer_command_sha256 = compact_utf8_command_sha256(command_argv)
        inputs_json = json.dumps({"candidate": self.candidate}, sort_keys=True, separators=(",", ":"))
        argv = ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={inputs_json}", *command_argv]
        request = {
            "operation_id": "run-100/local-commit/1",
            "argv": argv,
            "workspace": str(self.workspace),
        }

        altered = list(argv)
        altered[5] += " # changed"
        with self.subTest(reason="altered command"):
            with self.assertRaises(engine.OperationValidationError):
                self.controller._validate_request({**request, "argv": altered})

        self.controller.candidate_producer_command_sha256 = ""
        with self.subTest(reason="candidate command is unpinned"):
            with self.assertRaises(engine.OperationValidationError):
                self.controller._validate_request(request)

        ordinary_argv = ["/usr/bin/env", "ARG=" + "x" * 4093]
        with self.subTest(reason="ordinary 4097-byte argument"):
            with self.assertRaises(engine.OperationValidationError):
                self.controller._validate_request({**request, "argv": ordinary_argv})

    def test_pinned_candidate_script_limit_is_8192_utf8_bytes(self):
        command_argv = load_shipped_local_commit_command()
        script = command_argv[3]
        inputs_json = json.dumps({"candidate": self.candidate}, sort_keys=True, separators=(",", ":"))

        at_limit_padding_bytes = engine.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES - len(script.encode("utf-8"))
        at_limit = script + "é" * (at_limit_padding_bytes // 2) + "x" * (at_limit_padding_bytes % 2)
        self.assertEqual(len(at_limit.encode("utf-8")), engine.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES)
        at_limit_command = [*command_argv[:3], at_limit]
        self.controller.candidate_producer_command_sha256 = compact_utf8_command_sha256(at_limit_command)
        at_limit_request = {
            "operation_id": "run-100/local-commit/1",
            "argv": ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={inputs_json}", *at_limit_command],
            "workspace": str(self.workspace),
        }
        self.controller._validate_request(at_limit_request)

        over_limit_padding_bytes = engine.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES + 1 - len(script.encode("utf-8"))
        over_limit = script + "é" * (over_limit_padding_bytes // 2) + "x" * (over_limit_padding_bytes % 2)
        self.assertEqual(len(over_limit.encode("utf-8")), engine.MAX_CANDIDATE_PRODUCER_SCRIPT_BYTES + 1)
        over_limit_command = [*command_argv[:3], over_limit]
        self.controller.candidate_producer_command_sha256 = compact_utf8_command_sha256(over_limit_command)
        over_limit_request = {
            "operation_id": "run-100/local-commit/1",
            "argv": ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={inputs_json}", *over_limit_command],
            "workspace": str(self.workspace),
        }
        with self.assertRaises(engine.OperationValidationError):
            self.controller._validate_request(over_limit_request)

    def write_bundle(self, data: bytes) -> None:
        self.bundle_path.write_bytes(data)
        self.bundle_path.chmod(0o600)

    def install_operation(self, *, argv: list[str] | None = None, state: str = "terminated",
                          exit_status: int = 0) -> dict[str, str]:
        record_path = self.controller._record_path(self.operation_id)
        operation_directory = self.controller._operation_path(self.operation_id)
        if record_path.exists():
            record_path.unlink()
        if operation_directory.exists():
            shutil.rmtree(operation_directory)
        operation_argv = list(argv or self.argv)
        request = self.controller._validate_request({
            "operation_id": self.operation_id,
            "argv": operation_argv,
            "workspace": str(self.workspace),
        })
        request_sha256 = engine._sha256_json(request)
        identity = make_identity(self.operation_id, request_sha256)
        operation_hash = hashlib.sha256(self.operation_id.encode()).hexdigest()
        operation_directory = self.operations / operation_hash
        operation_directory.mkdir(mode=0o755)
        operation_directory.chmod(0o755)
        manifest = {
            "version": engine.PROTOCOL_VERSION,
            **request,
            "request_sha256": request_sha256,
            "release_file": str(self.operations / operation_hash / "release"),
            "environment": {},
        }
        manifest_path = operation_directory / "manifest.json"
        manifest_path.write_bytes(engine._canonical_json(manifest))
        manifest_path.chmod(0o644)
        proof = {
            "machine_id": MACHINE_ID,
            "boot_id": BOOT_ID,
            "unit": identity["unit"],
            "invocation_id": INVOCATION_ID,
            "control_group": identity["control_group"],
            "cgroup_state": "released",
            "cgroup_populated": 0,
            "active_state": "active",
            "sub_state": "exited",
            "exec_main_code": "exited",
            "exec_main_status": exit_status,
            "main_pid": 0,
            "systemd_version": 252,
            "observed_at": "2026-10-11T00:00:00+00:00",
        }
        record = {
            "version": engine.PROTOCOL_VERSION,
            "operation_id": self.operation_id,
            "unit": identity["unit"],
            "request_sha256": request_sha256,
            "machine_id": MACHINE_ID,
            "boot_id": BOOT_ID,
            "state": state,
            "identity": identity,
            "release_decision": True,
            "release_written": True,
            "termination_proof": proof,
        }
        self.controller._create_record(self.controller._record_path(self.operation_id), record)
        return identity

    def context_argv(
        self,
        candidate_context: object,
        *,
        candidate: dict[str, object] | None = None,
    ) -> tuple[list[str], str]:
        candidate_value = self.candidate if candidate is None else candidate
        inputs = {"candidate": candidate_value, "candidate_context": candidate_context}
        inputs_json = json.dumps(inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        argv = [
            "/usr/bin/env",
            f"WORKSTREAM_INPUTS_JSON={inputs_json}",
            *self.command_argv,
        ]
        input_sha256 = digest_json({
            "candidate": candidate_value,
            "candidate_context": candidate_context,
        })
        return argv, input_sha256

    def request(self, *, identity: dict[str, str] | None = None, offset: int = 0,
                expected_sha256: str | None = None, expected_bytes: int | None = None,
                input_sha256: str | None = None) -> dict[str, object]:
        return {
            "action": "candidate_bundle",
            "identity": identity or self.identity,
            "expected_release": {"service_revision": REVISION, "release_sha256": RELEASE_SHA256},
            "input_sha256": input_sha256 or self.input_sha256,
            "offset": offset,
            "expected_sha256": expected_sha256 or hashlib.sha256(self.bundle_bytes).hexdigest(),
            "expected_bytes": expected_bytes if expected_bytes is not None else len(self.bundle_bytes),
        }

    def fetch(self, **kwargs: object) -> dict[str, object]:
        request = transport.validate_rpc_request(self.request(**kwargs))
        broker = transport.OperationBroker(
            self.controller,
            control_uid=os.getuid(),
            control_gid=os.getgid(),
            service_revision=REVISION,
            release_sha256=RELEASE_SHA256,
            expected_machine_id=MACHINE_ID,
        )
        return broker._dispatch_rpc(request)

    @unittest.skipUnless(sys.platform == "linux" and os.geteuid() == 0,
                         "requires Linux root with capabilities to create a separate worker fixture")
    def test_real_producer_bundle_through_broker_with_empty_capability_bound(self):
        capabilities = next(line.split()[1] for line in Path("/proc/self/status").read_text().splitlines()
                            if line.startswith("CapEff:"))
        required = sum(1 << bit for bit in (0, 1, 6, 7, 8))  # Fixture setup only; child drops all caps.
        if int(capabilities, 16) & required != required:
            self.skipTest("fixture setup requires CHOWN, DAC_OVERRIDE, SETGID, SETUID and SETPCAP")
        worker = pwd.getpwnam("nobody")
        self.root.chmod(0o755)
        # Recreate every ancestor in the live /srv/factory/contained-workspaces chain.
        for parent in (self.root / "srv", self.root / "srv/factory"):
            parent.mkdir(mode=0o755)
            parent.chmod(0o755)
            os.chown(parent, 0, 0)
        relocated_root = self.root / "srv/factory/contained-workspaces"
        self.workspace_root.rename(relocated_root)
        self.workspace_root = relocated_root
        self.workspace = relocated_root / "candidate"
        self.candidates = self.workspace / ".git/factory-candidates"
        self.bundle_path = self.candidates / f"{self.input_sha256}.bundle"
        self.controller.workspace_root = relocated_root
        self.controller.workspace_root_path = relocated_root
        self.controller.worker_user = worker.pw_name
        self.controller.worker_group = grp.getgrgid(worker.pw_gid).gr_name
        self.bundle_path.unlink()
        for path in (self.workspace, self.workspace / ".git", self.candidates):
            os.chown(path, worker.pw_uid, worker.pw_gid)
        # Match the live root-owned shared root and private worker task boundary.
        os.chown(self.workspace_root, 0, worker.pw_gid)
        self.workspace_root.chmod(0o750)
        self.workspace.chmod(0o700)
        (self.workspace / ".git").chmod(0o755)
        self.candidates.chmod(0o700)

        def run_worker(argv):
            return subprocess.run(
                argv, cwd=self.workspace, check=True, capture_output=True, text=True,
                user=worker.pw_uid, group=worker.pw_gid, extra_groups=[], umask=0o077,
                env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent",
                     "WORKSTREAM_INPUTS_JSON": self.inputs_json},
            )

        run_worker(["git", "init", "--quiet", "--initial-branch=cycle/bundle-test"])
        source = self.workspace / "candidate"
        source.write_text("base\n")
        os.chown(source, worker.pw_uid, worker.pw_gid)
        source.chmod(0o644)
        run_worker(["git", "add", "candidate"])
        run_worker(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                    "commit", "--quiet", "-m", "base"])
        source.write_text("candidate\n")
        self.command_argv = load_shipped_local_commit_command()
        self.controller.candidate_producer_command_sha256 = compact_utf8_command_sha256(self.command_argv)
        result = json.loads(run_worker(self.command_argv).stdout)
        self.assertEqual(self.workspace_root.stat().st_mode & 0o777, 0o750)
        self.assertEqual(self.workspace.stat().st_mode & 0o777, 0o710)
        self.assertEqual((self.workspace / ".git").stat().st_mode & 0o777, 0o710)
        self.assertEqual(self.candidates.stat().st_mode & 0o777, 0o750)
        self.assertEqual(self.bundle_path.stat().st_mode & 0o777, 0o640)
        self.bundle_bytes = self.bundle_path.read_bytes()
        self.assertEqual(result["bundle_sha256"], hashlib.sha256(self.bundle_bytes).hexdigest())
        linked = self.workspace / ".git" / "linked-bundle"
        os.link(self.bundle_path, linked)
        with self.assertRaises(subprocess.CalledProcessError) as rejected:
            run_worker(self.command_argv)
        self.assertIn("unsafe bundle file", rejected.exception.stderr)
        linked.unlink()
        secret = self.workspace / ".git" / "worker-private"
        secret.write_bytes(b"private worker data")
        os.chown(secret, worker.pw_uid, worker.pw_gid)
        secret.chmod(0o600)
        private_worktree = self.workspace / "private-worktree-file"
        private_worktree.write_bytes(b"private worktree data")
        os.chown(private_worktree, worker.pw_uid, worker.pw_gid)
        private_worktree.chmod(0o600)
        self.argv = ["/usr/bin/env", f"WORKSTREAM_INPUTS_JSON={self.inputs_json}", *self.command_argv]
        self.identity = self.install_operation(argv=self.argv)
        record_path = self.controller._record_path(self.operation_id)
        record_before = record_path.read_bytes()

        def probe(private, with_worker_group=True):
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                try:
                    os.setgroups([worker.pw_gid] if with_worker_group else [])
                    os.setgid(12345)  # Distinct broker primary group; use the existing supplementary group.
                    libc = ctypes.CDLL(None, use_errno=True)
                    for capability in range(64):
                        # Drop every supported bounding capability before effective/permitted caps.
                        result = libc.prctl(24, capability, 0, 0, 0)
                        if result != 0 and ctypes.get_errno() != 22:
                            raise OSError(ctypes.get_errno(), "capability bounding drop failed")
                    class Header(ctypes.Structure):
                        _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]
                    class Data(ctypes.Structure):
                        _fields_ = [("effective", ctypes.c_uint32), ("permitted", ctypes.c_uint32),
                                    ("inheritable", ctypes.c_uint32)]
                    header, data = Header(0x20080522, 0), (Data * 2)()
                    self.assertEqual(libc.capset(ctypes.byref(header), ctypes.byref(data)), 0)
                    self.assertEqual(libc.prctl(38, 1, 0, 0, 0), 0)  # NoNewPrivileges
                    status = Path("/proc/self/status").read_text()
                    for name in ("CapEff", "CapPrm", "CapBnd"):
                        self.assertIn(name + ":\t0000000000000000", status)
                    if private:
                        with self.assertRaises(engine.OperationUnavailable) as caught:
                            self.fetch()
                        self.assertIsInstance(caught.exception.__cause__, PermissionError)
                        self.assertEqual(caught.exception.__cause__.errno, 13)
                    else:
                        reply = self.fetch()
                        self.assertEqual(base64.b64decode(reply["data_base64"]), self.bundle_bytes)
                        self.assertTrue(reply["eof"])
                        # This known-path access is the explicitly approved traversal consequence.
                        self.assertEqual(source.read_text(), "candidate\n")
                        with self.assertRaises(PermissionError):
                            os.open(self.bundle_path, os.O_WRONLY)
                        for private_file in (secret, private_worktree):
                            with self.assertRaises(PermissionError):
                                private_file.read_bytes()
                            with self.assertRaises(PermissionError):
                                os.open(private_file, os.O_WRONLY)
                        for parent in (self.workspace, self.workspace / ".git"):
                            with self.assertRaises(PermissionError):
                                os.listdir(parent)
                            with self.assertRaises(PermissionError):
                                os.open(parent / "broker-write", os.O_WRONLY | os.O_CREAT, 0o600)
                    report = {"ok": True}
                except BaseException as error:
                    report = {"error": repr(error)}
                os.write(write_fd, json.dumps(report).encode())
                os.close(write_fd)
                os._exit(0)
            os.close(write_fd)
            report = json.loads(os.read(read_fd, 16384))
            os.close(read_fd)
            _, status = os.waitpid(pid, 0)
            self.assertEqual(status, 0)
            self.assertEqual(report, {"ok": True})

        self.workspace.chmod(0o700)
        probe(private=True)  # Readable artifacts still fail beneath the actual private task parent.
        # Repair access only by replaying the shipped producer, never by a test chmod workaround.
        private_worktree.unlink()
        replay = json.loads(run_worker(self.command_argv).stdout)
        self.assertEqual(replay, result)
        private_worktree.write_bytes(b"private worktree data")
        os.chown(private_worktree, worker.pw_uid, worker.pw_gid)
        private_worktree.chmod(0o600)
        probe(private=False)
        probe(private=True, with_worker_group=False)
        self.assertEqual(record_path.read_bytes(), record_before)
        self.assertEqual(self.records.stat().st_mode & 0o777, 0o700)
        self.assertEqual(record_path.stat().st_mode & 0o777, 0o600)

    def test_reads_fixed_chunks_and_replays_offsets_exactly(self):
        first = self.fetch()
        expected_first = self.bundle_bytes[:transport.CANDIDATE_BUNDLE_CHUNK_BYTES]
        self.assertEqual(set(first), {
            "operation_id", "candidate_input_sha256", "bundle_sha256", "bundle_bytes",
            "offset", "data_base64", "eof",
        })
        self.assertEqual(first["candidate_input_sha256"], self.input_sha256)
        self.assertEqual(first["bundle_sha256"], hashlib.sha256(self.bundle_bytes).hexdigest())
        self.assertEqual(first["bundle_bytes"], len(self.bundle_bytes))
        self.assertEqual(base64.b64decode(first["data_base64"]), expected_first)
        self.assertFalse(first["eof"])
        self.assertLessEqual(
            len(transport._response_line({"ok": first})),
            transport.MAX_RESPONSE_BYTES,
        )

        replay = self.fetch(offset=0)
        self.assertEqual(replay, first)
        second = self.fetch(offset=transport.CANDIDATE_BUNDLE_CHUNK_BYTES)
        self.assertEqual(base64.b64decode(second["data_base64"]), self.bundle_bytes[65536:131072])
        self.assertFalse(second["eof"])

        last_offset = (len(self.bundle_bytes) // 65536) * 65536
        last = self.fetch(offset=last_offset)
        self.assertEqual(base64.b64decode(last["data_base64"]), self.bundle_bytes[last_offset:])
        self.assertTrue(last["eof"])
        eof = self.fetch(offset=len(self.bundle_bytes))
        self.assertEqual(base64.b64decode(eof["data_base64"]), b"")
        self.assertTrue(eof["eof"])

    def test_verified_bundle_cache_avoids_rehashing_each_chunk_and_is_bounded(self):
        original_inspect = self.controller._inspect_candidate_bundle
        with patch.object(self.controller, "_inspect_candidate_bundle", wraps=original_inspect) as inspect:
            self.fetch()
            self.fetch(offset=transport.CANDIDATE_BUNDLE_CHUNK_BYTES)
            self.fetch(offset=2 * transport.CANDIDATE_BUNDLE_CHUNK_BYTES)
        self.assertEqual(inspect.call_count, 1)

        for index in range(engine.MAX_CANDIDATE_BUNDLE_CACHE_ENTRIES + 1):
            self.controller._candidate_bundle_cache_store(
                ("operation", index), (index,) * 8
            )
        self.assertEqual(
            len(self.controller._candidate_bundle_cache),
            engine.MAX_CANDIDATE_BUNDLE_CACHE_ENTRIES,
        )

    def test_cached_bundle_metadata_or_content_mutation_invalidates_and_rejects(self):
        self.fetch()
        before = self.bundle_path.stat()
        os.utime(
            self.bundle_path,
            ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000),
        )
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch(offset=transport.CANDIDATE_BUNDLE_CHUNK_BYTES)
        self.assertFalse(self.controller._candidate_bundle_cache)

        self.write_bundle(self.bundle_bytes)
        self.controller._candidate_bundle_cache.clear()
        self.fetch()
        changed = bytes([self.bundle_bytes[0] ^ 0xFF]) + self.bundle_bytes[1:]
        before = self.bundle_path.stat()
        self.write_bundle(changed)
        # Fast same-size rewrites can share a timestamp tick on the Docker filesystem.
        os.utime(self.bundle_path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000))
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch(offset=transport.CANDIDATE_BUNDLE_CHUNK_BYTES)
        self.assertFalse(self.controller._candidate_bundle_cache)

    def test_rejects_unrecorded_identity_and_nonterminal_or_failed_operations(self):
        stale = dict(self.identity, invocation_id="f" * 32)
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                stale, self.input_sha256, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes)
            )

        self.install_operation(state="running")
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, self.input_sha256, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes)
            )

        self.install_operation(exit_status=1)
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, self.input_sha256, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes)
            )

    def test_rejects_wrong_producer_or_input_binding(self):
        altered = [*self.argv]
        altered[-1] = "print('different command')"
        self.install_operation(argv=altered)
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, self.input_sha256, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes)
            )

        self.install_operation(argv=self.argv)
        wrong_input = "9" * 64
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, wrong_input, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes)
            )

    def test_context_digest_is_derived_from_protected_descriptor_inputs(self):
        candidate = {"thread_id": "task-100", "revision": "main", "label": "café"}
        context = {
            "id": "1" * 64,
            "repair_parent_sha": "2" * 40,
            "validation_base": "3" * 64,
        }
        argv, input_sha256 = self.context_argv(context, candidate=candidate)
        self.bundle_path = self.candidates / f"{input_sha256}.bundle"
        self.write_bundle(self.bundle_bytes)
        identity = self.install_operation(argv=argv)

        response = self.fetch(identity=identity, input_sha256=input_sha256)
        self.assertEqual(response["candidate_input_sha256"], input_sha256)
        self.assertEqual(
            base64.b64decode(response["data_base64"]),
            self.bundle_bytes[:transport.CANDIDATE_BUNDLE_CHUNK_BYTES],
        )

    def test_context_digest_rejects_changed_or_invalid_descriptor_claims(self):
        context = {
            "id": "1" * 64,
            "repair_parent_sha": "2" * 40,
            "validation_base": "3" * 64,
        }
        expected_input_sha256 = digest_json({
            "candidate": self.candidate,
            "candidate_context": context,
        })
        changed_contexts = {
            "id": {**context, "id": "4" * 64},
            "repair parent": {**context, "repair_parent_sha": "5" * 64},
            "validation base": {**context, "validation_base": "6" * 40},
            "extra field": {**context, "extra": "not allowed"},
            "invalid id": {**context, "id": "not-a-digest"},
            "invalid SHA type": {**context, "repair_parent_sha": 7},
        }
        for label, changed_context in changed_contexts.items():
            with self.subTest(context=label):
                argv, _ = self.context_argv(changed_context)
                identity = self.install_operation(argv=argv)
                with self.assertRaises(engine.OperationUnavailable):
                    self.fetch(identity=identity, input_sha256=expected_input_sha256)

    def test_null_context_keeps_legacy_candidate_only_digest(self):
        inputs_json = json.dumps(
            {"candidate": self.candidate, "candidate_context": None},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        argv = [
            "/usr/bin/env",
            f"WORKSTREAM_INPUTS_JSON={inputs_json}",
            *self.command_argv,
        ]
        identity = self.install_operation(argv=argv)
        response = self.fetch(identity=identity, input_sha256=self.input_sha256)
        self.assertEqual(response["candidate_input_sha256"], self.input_sha256)

    def test_unicode_inline_command_pin_uses_compact_utf8_json(self):
        unicode_command_argv = ["/usr/bin/python3", "-I", "-c", "print('café')"]
        utf8_encoded = json.dumps(
            unicode_command_argv,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        escaped_encoded = json.dumps(
            unicode_command_argv,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        utf8_pin = hashlib.sha256(utf8_encoded).hexdigest()
        escaped_pin = hashlib.sha256(escaped_encoded).hexdigest()
        self.assertNotEqual(utf8_pin, escaped_pin)

        self.controller.candidate_producer_command_sha256 = utf8_pin
        unicode_argv = [self.argv[0], self.argv[1], *unicode_command_argv]
        self.identity = self.install_operation(argv=unicode_argv)
        response = self.fetch()
        self.assertEqual(response["bundle_sha256"], hashlib.sha256(self.bundle_bytes).hexdigest())

        self.controller.candidate_producer_command_sha256 = escaped_pin
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()

    def test_rejects_digest_size_mutation_and_path_substitution(self):
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, self.input_sha256, 0, "0" * 64, len(self.bundle_bytes)
            )
        with self.assertRaises(engine.OperationUnavailable):
            self.controller.candidate_bundle(
                self.identity, self.input_sha256, 0, hashlib.sha256(self.bundle_bytes).hexdigest(), len(self.bundle_bytes) + 1
            )

        original_inspect = self.controller._inspect_candidate_bundle

        def mutate_after_hash(*args, **kwargs):
            result = original_inspect(*args, **kwargs)
            self.bundle_path.write_bytes(self.bundle_bytes + b"changed")
            return result

        with patch.object(self.controller, "_inspect_candidate_bundle", side_effect=mutate_after_hash):
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()

        self.write_bundle(self.bundle_bytes)

        def replace_with_symlink(*args, **kwargs):
            result = original_inspect(*args, **kwargs)
            saved = self.root / "saved-bundle"
            self.bundle_path.rename(saved)
            self.bundle_path.symlink_to(saved)
            return result

        with patch.object(self.controller, "_inspect_candidate_bundle", side_effect=replace_with_symlink):
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()

        self.bundle_path.unlink()
        self.write_bundle(self.bundle_bytes)
        self.bundle_path.unlink()
        outside = self.root / "outside.bundle"
        outside.write_bytes(self.bundle_bytes)
        self.bundle_path.symlink_to(outside)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()

    def test_rejects_nonprivate_or_linked_bundle_and_wrong_worker_owner(self):
        self.bundle_path.chmod(0o644)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()
        self.bundle_path.chmod(0o600)

        self.bundle_path.unlink()
        os.mkfifo(self.bundle_path, mode=0o600)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()
        self.bundle_path.unlink()
        self.write_bundle(self.bundle_bytes)

        hard_link = self.root / "second-link.bundle"
        os.link(self.bundle_path, hard_link)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()
        hard_link.unlink()

        self.candidates.chmod(0o755)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()
        self.candidates.chmod(0o700)

        with patch.object(self.controller, "_worker_uid", return_value=os.getuid() + 1), \
                patch.object(self.controller, "_check_open_directory"):
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()

        self.bundle_path.unlink()
        self.write_bundle(self.bundle_bytes)
        git_dir = self.workspace / ".git"
        saved_git_dir = self.workspace / "saved-git"
        git_dir.rename(saved_git_dir)
        git_dir.symlink_to(saved_git_dir, target_is_directory=True)
        with self.assertRaises(engine.OperationUnavailable):
            self.fetch()

    def test_group_access_requires_configured_group_and_never_write_or_world_access(self):
        for mode in (0o660, 0o644):
            self.bundle_path.chmod(mode)
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()
        self.bundle_path.chmod(0o640)
        for mode in (0o770, 0o755):
            self.candidates.chmod(mode)
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()
        self.candidates.chmod(0o750)
        with patch.object(self.controller, "_worker_gid", return_value=os.getgid() + 1):
            with self.assertRaisesRegex(engine.OperationUnavailable, "directory group is unsafe"):
                self.fetch()  # Parent group mismatch.
        with patch.object(self.controller, "_worker_gid", return_value=os.getgid() + 1), \
                patch.object(self.controller, "_check_open_directory"):
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()  # Transport directory group mismatch.
            self.candidates.chmod(0o700)
            with self.assertRaises(engine.OperationUnavailable):
                self.fetch()  # Bundle group mismatch, private legacy directory.
        self.bundle_path.chmod(0o600)
        self.assertEqual(self.fetch()["bundle_bytes"], len(self.bundle_bytes))

    def test_rejects_invalid_rpc_fields_and_disables_unpinned_capability(self):
        valid = self.request()
        self.assertEqual(transport.validate_rpc_request(valid)["action"], "candidate_bundle")
        invalid_requests = (
            {key: value for key, value in valid.items() if key != "expected_bytes"},
            {**valid, "offset": True},
            {**valid, "offset": 1},
            {**valid, "offset": len(self.bundle_bytes) + 1},
            {**valid, "expected_bytes": transport.MAX_CANDIDATE_BUNDLE_BYTES + 1},
            {**valid, "expected_sha256": "not-a-digest"},
        )
        for request in invalid_requests:
            with self.subTest(request=request):
                with self.assertRaises(transport.ProtocolError):
                    transport.validate_rpc_request(request)

        controller_without_pin = engine.OperationController(
            object(),
            records_dir=str(self.records),
            operation_dir=str(self.operations),
            workspace_root=str(self.workspace_root),
            wrapper_path="/bin/true",
            worker_home=str(self.root / "worker-home"),
            machine_id_path=str(self.root / "machine-id"),
            boot_id_path=str(self.root / "boot-id"),
            expected_machine_id=MACHINE_ID,
            service_revision=REVISION,
            worker_user=pwd.getpwuid(os.getuid()).pw_name,
            worker_group=grp.getgrgid(os.getgid()).gr_name,
            expected_owner_uid=os.getuid(),
        )
        self.assertFalse(controller_without_pin.candidate_bundle_available)
        controller_without_pin.capabilities = lambda: {"machine_id": MACHINE_ID, "contained": False}
        broker = transport.OperationBroker(
            controller_without_pin,
            control_uid=os.getuid(),
            control_gid=os.getgid(),
            service_revision=REVISION,
            release_sha256=RELEASE_SHA256,
            expected_machine_id=MACHINE_ID,
        )
        capabilities = broker._dispatch_rpc({"action": "capabilities"})
        self.assertFalse(capabilities["candidate_bundle_v1"])
        with self.assertRaises(transport.ProtocolError):
            broker._dispatch_rpc(transport.validate_rpc_request(valid))


if __name__ == "__main__":
    unittest.main()
