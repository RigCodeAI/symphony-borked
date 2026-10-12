from __future__ import annotations

import base64
import hashlib
import json
import os
import pwd
from pathlib import Path
import shutil
import tempfile
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
            worker_group=pwd.getpwuid(os.getuid()).pw_name,
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
        self.write_bundle(changed)
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
            worker_group=pwd.getpwuid(os.getuid()).pw_name,
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
