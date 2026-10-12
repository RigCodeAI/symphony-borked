import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


DEPLOY = Path(__file__).resolve().parents[1]
MODULE_PATH = DEPLOY / "recovery_preflight.py"
spec = importlib.util.spec_from_file_location("recovery_preflight", MODULE_PATH)
recovery_preflight = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery_preflight)

REVISION = "a" * 40
OLD_REVISION = "b" * 40
RELEASE_SHA = "c" * 64
OLD_RELEASE_SHA = "d" * 64
POLICY = {
    "version": 1,
    "gate_policy": "exit-status-v1",
    "lifecycle_sha256": "1" * 64,
    "executor_sha256": "2" * 64,
    "validation_sha256": "3" * 64,
}
INVENTORY = {
    "plan.json", "capture-manifest.json", "active.json", "active-public.json",
    "effective-public.json", "boot-config.json", "active-release.json",
    "active-release-sha256.txt", "boot-release.json", "boot-release-sha256.txt",
    "active-references.json", "boot-references.json", "active-WORKFLOW.md",
    "boot-WORKFLOW.md", "recovery-plan.json", "current-release.txt",
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def secret_fingerprint(env, key, resource, version):
    return sha256(f"{env}\n{key}\n{resource}\n{version}".encode())


class RecoveryPreflightTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        self.reset_fixture()

    def reset_fixture(self):
        for entry in self.directory.iterdir():
            entry.unlink()
        self.directory.chmod(0o700)
        self.capture_id = "capture-dev242-test"
        self.release_sha = RELEASE_SHA
        self.active_revision = REVISION
        self.boot_revision = REVISION
        self.boot_release_sha = RELEASE_SHA
        self.active_workflow = b"linear coordinator workflow\n"
        self.boot_workflow = self.active_workflow
        self.write_valid_inventory()

    def write(self, name, payload):
        path = self.directory / name
        path.write_bytes(payload)
        path.chmod(0o600)
        return path

    def write_json(self, name, value):
        return self.write(name, json_bytes(value))

    def read_json(self, name):
        return json.loads((self.directory / name).read_bytes())

    def references(self, revision, workflow="linear", webhook=False, unselected="model-auth"):
        mapping = ({
            "LINEAR_API_KEY": "linear_api",
            "LINEAR_API_TOKEN": "linear_signing",
        } if workflow == "linear" else {})
        return {
            "project_id": "factory-test",
            "role": "coordinator",
            "service_revision": revision,
            "coordinator_workflow": workflow,
            "coordinator_secret_env": mapping,
            "secret_ids": {
                "linear_api": "projects/factory-test/secrets/linear-api",
                "linear_signing": "projects/factory-test/secrets/linear-signing",
                "model_auth": f"projects/factory-test/secrets/{unselected}",
                "worker_ssh": "projects/factory-test/secrets/worker-ssh",
            },
            "secret_versions": {
                "linear_api": "4",
                "linear_signing": "8",
                "model_auth": "2",
                "worker_ssh": "1",
            },
            "enable_linear_webhook": webhook,
        }

    def public_projection(self, references, release_sha):
        mapping = references["coordinator_secret_env"]
        selected_versions = {
            env: references["secret_versions"][key]
            for env, key in mapping.items()
        }
        fingerprints = {
            env: secret_fingerprint(
                env, key, references["secret_ids"][key], references["secret_versions"][key]
            )
            for env, key in mapping.items()
        }
        return {
            "project_id": references["project_id"],
            "role": references["role"],
            "service_revision": references["service_revision"],
            "release_sha256": release_sha,
            "coordinator_workflow": references["coordinator_workflow"],
            "coordinator_secret_env": mapping,
            "coordinator_secret_versions": selected_versions,
            "coordinator_secret_fingerprints": fingerprints,
            "enable_linear_webhook": references["enable_linear_webhook"],
        }

    def boot_config(self, references, release_sha):
        return {
            **self.public_projection(references, release_sha),
            "secret_ids": references["secret_ids"],
            "secret_versions": references["secret_versions"],
        }

    def write_valid_inventory(self):
        active_refs = self.references(self.active_revision)
        boot_refs = self.references(self.boot_revision)
        state = b"opaque state database image\n"
        evidence = b"opaque evidence database image\n"
        manifest = {
            "version": 1,
            "kind": "local_owner_capture",
            "capture_id": self.capture_id,
            "created_at": "2026-10-12T12:00:00Z",
            "restore_ready": False,
            "databases": {
                "state": {
                    "file": "state.sqlite", "identity": "workstream_store", "bytes": len(state),
                    "sha256": sha256(state), "schema": {"user_version": 3},
                },
                "evidence": {
                    "file": "evidence.sqlite", "identity": "evidence_index", "bytes": len(evidence),
                    "sha256": sha256(evidence), "schema": {"user_version": 1},
                },
            },
            "metadata": {
                "service_revision": self.active_revision,
                "service_revision_source": "operator_pin",
                "policy": POLICY,
                "pending_evidence_outbox": 0,
                "run_ids": [],
            },
            "durability": {"files_synced": True, "durable_directory_synced": True},
        }
        manifest_data = json_bytes(manifest)
        self.write("capture-manifest.json", manifest_data)

        recovery_plan = {
            "version": 1,
            "mode": "held_inspection",
            "root": str(self.directory),
            "id": self.capture_id,
            "workspace_roots": ["/srv/factory/workspaces"],
            "manifest_sha256": sha256(manifest_data),
            "service_revision": self.active_revision,
            "policy": POLICY,
        }
        recovery_plan_data = json_bytes(recovery_plan)
        self.write("recovery-plan.json", recovery_plan_data)

        self.write_json("active.json", {
            "service_revision": self.active_revision,
            "release_sha256": self.release_sha,
            "activated_at": "2026-10-12T12:00:00Z",
        })
        self.write_json("active-public.json", self.public_projection(active_refs, self.release_sha))
        self.write_json("effective-public.json", self.public_projection(active_refs, self.release_sha))
        self.write_json("boot-config.json", self.boot_config(boot_refs, self.boot_release_sha))
        self.write_json("active-release.json", {"service_revision": self.active_revision})
        self.write("active-release-sha256.txt", (self.release_sha + "\n").encode())
        self.write_json("boot-release.json", {"service_revision": self.boot_revision})
        self.write("boot-release-sha256.txt", (self.boot_release_sha + "\n").encode())
        active_references_data = json_bytes(active_refs)
        self.write("active-references.json", active_references_data)
        self.write_json("boot-references.json", boot_refs)
        self.write("active-WORKFLOW.md", self.active_workflow)
        self.write("boot-WORKFLOW.md", self.boot_workflow)
        self.write("current-release.txt", f"/opt/factory/releases/{self.active_revision}\n".encode())

        expected = {
            "capture_id": self.capture_id,
            "manifest_sha256": sha256(manifest_data),
            "service_revision": self.active_revision,
            "release_sha256": self.release_sha,
            "workflow_sha256": sha256(self.active_workflow),
            "references_sha256": sha256(active_references_data),
            "recovery_plan_sha256": sha256(recovery_plan_data),
            "policy": POLICY,
        }
        self.write_json("plan.json", {"version": 1, "expected": expected})

    def set_expected(self, key, value):
        plan = self.read_json("plan.json")
        plan["expected"][key] = value
        self.write_json("plan.json", plan)

    def result(self):
        try:
            return recovery_preflight.preflight(str(self.directory))
        except Exception as exc:  # The API is specified to return a report for every rejection.
            self.fail(f"preflight raised {type(exc).__name__} instead of returning a report")

    def assert_rejected(self, report, reason=None):
        self.assertEqual(report.get("status"), "rejected", report)
        self.assertFalse(report.get("restore_ready"))
        self.assertFalse(report.get("dispatch_allowed"))
        self.assertRegex(report.get("reason", ""), r"^[a-z][a-z0-9_]*$")
        if reason is not None:
            self.assertEqual(report["reason"], reason)

    def directory_snapshot(self):
        return {
            entry.name: (
                entry.lstat().st_mode,
                entry.lstat().st_uid,
                entry.lstat().st_nlink,
                entry.lstat().st_size,
                entry.lstat().st_mtime_ns,
                entry.read_bytes() if entry.is_file() and not entry.is_symlink() else None,
            )
            for entry in self.directory.iterdir()
        }

    def test_aligned_snapshots_remain_held_and_are_read_only(self):
        before = self.directory_snapshot()

        report = self.result()

        self.assertEqual(report["status"], "aligned_but_held")
        self.assertFalse(report["restore_ready"])
        self.assertFalse(report["dispatch_allowed"])
        self.assertEqual(report["transition"], "same_revision")
        self.assertEqual(set(INVENTORY), set(report["input_sha256"]))
        self.assertEqual(before, self.directory_snapshot())

    def test_changed_file_after_read_rejects_the_stale_snapshot(self):
        original_stat = os.stat
        original_bytes = (self.directory / "plan.json").read_bytes()
        changed = False

        def racing_stat(path, *args, **kwargs):
            nonlocal changed
            if path == "plan.json" and not changed:
                changed = True
                replacement = original_bytes.replace(b'"version":1', b'"version":2', 1)
                self.assertEqual(len(replacement), len(original_bytes))
                self.assertNotEqual(replacement, original_bytes)
                (self.directory / "plan.json").write_bytes(replacement)
            return original_stat(path, *args, **kwargs)

        with patch.object(recovery_preflight.os, "stat", side_effect=racing_stat):
            report = self.result()

        self.assertTrue(changed)
        self.assert_rejected(report, "inputs_changed")

    def test_coherent_older_boot_is_reported_as_a_pending_transition(self):
        self.boot_revision = OLD_REVISION
        self.boot_release_sha = OLD_RELEASE_SHA
        self.write_valid_inventory()

        report = self.result()

        self.assertEqual(report["status"], "mismatch")
        self.assertIn("boot_active_revision_mismatch", report["issues"])
        self.assertEqual(report["transition"], "pending_boot_transition")
        self.assertFalse(report["restore_ready"])
        self.assertFalse(report["dispatch_allowed"])

    def test_active_receipt_public_current_and_release_mismatches_are_rejected(self):
        cases = (
            ("active.json", {"service_revision": OLD_REVISION}, "active_identity_mismatch"),
            ("active-public.json", {"service_revision": OLD_REVISION}, "active_identity_mismatch"),
            ("effective-public.json", {"release_sha256": OLD_RELEASE_SHA}, "active_identity_mismatch"),
            ("active-release.json", {"service_revision": OLD_REVISION}, "active_identity_mismatch"),
            ("active-release-sha256.txt", None, "active_identity_mismatch"),
            ("current-release.txt", None, "active_identity_mismatch"),
            ("boot-release.json", {"service_revision": OLD_REVISION}, "boot_identity_mismatch"),
            ("boot-release-sha256.txt", None, "boot_identity_mismatch"),
        )
        for filename, changes, issue in cases:
            with self.subTest(filename=filename):
                self.reset_fixture()
                if filename.endswith(".json"):
                    value = self.read_json(filename)
                    value.update(changes)
                    self.write_json(filename, value)
                elif filename.endswith("-sha256.txt"):
                    self.write(filename, (OLD_RELEASE_SHA + "\n").encode())
                else:
                    self.write(filename, f"/opt/factory/releases/{OLD_REVISION}\n".encode())
                report = self.result()
                self.assertEqual(report["status"], "mismatch", report)
                self.assertIn(issue, report["issues"])
                self.assertFalse(report["dispatch_allowed"])

    def test_same_revision_selected_reference_workflow_and_webhook_drift_is_rejected(self):
        mutations = ("selected_reference", "workflow", "webhook")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.reset_fixture()
                refs = self.read_json("active-references.json")
                if mutation == "selected_reference":
                    refs["secret_ids"]["linear_api"] = "projects/factory-test/secrets/replaced-api"
                    self.write_json("active-references.json", refs)
                    self.set_expected("references_sha256", sha256((self.directory / "active-references.json").read_bytes()))
                    projection = self.public_projection(refs, self.release_sha)
                    self.write_json("active-public.json", projection)
                    self.write_json("effective-public.json", projection)
                elif mutation == "workflow":
                    refs["coordinator_workflow"] = "pilot"
                    refs["coordinator_secret_env"] = {}
                    self.write_json("active-references.json", refs)
                    self.set_expected("references_sha256", sha256((self.directory / "active-references.json").read_bytes()))
                    self.write("active-WORKFLOW.md", b"pilot coordinator workflow\n")
                    self.set_expected("workflow_sha256", sha256((self.directory / "active-WORKFLOW.md").read_bytes()))
                    projection = self.public_projection(refs, self.release_sha)
                    self.write_json("active-public.json", projection)
                    self.write_json("effective-public.json", projection)
                else:
                    refs["enable_linear_webhook"] = True
                    self.write_json("active-references.json", refs)
                    self.set_expected("references_sha256", sha256((self.directory / "active-references.json").read_bytes()))
                    projection = self.public_projection(refs, self.release_sha)
                    self.write_json("active-public.json", projection)
                    self.write_json("effective-public.json", projection)

                report = self.result()

                self.assertEqual(report["status"], "mismatch", report)
                self.assertIn("same_revision_configuration_changed", report["issues"])
                self.assertFalse(report["dispatch_allowed"])

    def test_unselected_secret_reference_drift_does_not_break_selected_pins(self):
        boot_refs = self.read_json("boot-references.json")
        boot_refs["secret_ids"]["model_auth"] = "projects/factory-test/secrets/model-auth-rotated"
        boot_refs["secret_versions"]["model_auth"] = "3"
        self.write_json("boot-references.json", boot_refs)
        boot_config = self.read_json("boot-config.json")
        boot_config["secret_ids"] = boot_refs["secret_ids"]
        boot_config["secret_versions"] = boot_refs["secret_versions"]
        self.write_json("boot-config.json", boot_config)

        report = self.result()

        self.assertEqual(report["status"], "aligned_but_held", report)
        self.assertFalse(report["restore_ready"])
        self.assertFalse(report["dispatch_allowed"])
        self.assertFalse(report["reference_snapshots_equal"])

    def test_capture_and_supplied_expected_hash_mismatches_are_rejected(self):
        cases = ("capture", "workflow", "references", "recovery_plan", "invalid_policy")
        for case in cases:
            with self.subTest(case=case):
                self.reset_fixture()
                if case == "capture":
                    manifest = self.read_json("capture-manifest.json")
                    manifest["created_at"] = "2026-10-12T12:00:01Z"
                    self.write_json("capture-manifest.json", manifest)
                elif case == "workflow":
                    self.set_expected("workflow_sha256", "f" * 64)
                elif case == "references":
                    self.set_expected("references_sha256", "f" * 64)
                elif case == "recovery_plan":
                    self.set_expected("recovery_plan_sha256", "f" * 64)
                else:
                    plan = self.read_json("plan.json")
                    plan["expected"]["policy"]["unexpected"] = True
                    self.write_json("plan.json", plan)

                report = self.result()

                if case == "invalid_policy":
                    self.assert_rejected(report, "invalid_policy")
                else:
                    self.assertEqual(report["status"], "mismatch", report)
                    self.assertTrue(report["issues"], report)
                self.assertFalse(report["dispatch_allowed"])

    def test_policy_accepts_the_five_field_and_optional_six_field_pin_sets(self):
        policy_with_child = {**POLICY, "child_sha256": "4" * 64}
        manifest = self.read_json("capture-manifest.json")
        manifest["metadata"]["policy"] = policy_with_child
        manifest_data = json_bytes(manifest)
        self.write("capture-manifest.json", manifest_data)

        held = self.read_json("recovery-plan.json")
        held["manifest_sha256"] = sha256(manifest_data)
        held["policy"] = policy_with_child
        held_data = json_bytes(held)
        self.write("recovery-plan.json", held_data)

        expected = self.read_json("plan.json")["expected"]
        expected["manifest_sha256"] = sha256(manifest_data)
        expected["recovery_plan_sha256"] = sha256(held_data)
        expected["policy"] = policy_with_child
        self.write_json("plan.json", {"version": 1, "expected": expected})

        report = self.result()

        self.assertEqual(report["status"], "aligned_but_held", report)
        self.assertFalse(report["restore_ready"])
        self.assertFalse(report["dispatch_allowed"])

    def test_missing_duplicate_unknown_and_extra_inputs_are_rejected(self):
        cases = ("missing", "duplicate_json_key", "unknown_field", "extra_file")
        for case in cases:
            with self.subTest(case=case):
                self.reset_fixture()
                if case == "missing":
                    (self.directory / "boot-config.json").unlink()
                elif case == "duplicate_json_key":
                    payload = (self.directory / "plan.json").read_text()
                    self.write("plan.json", payload.replace('"version":1', '"version":1,"version":1', 1).encode())
                elif case == "unknown_field":
                    plan = self.read_json("plan.json")
                    plan["unexpected"] = True
                    self.write_json("plan.json", plan)
                else:
                    self.write("extra.json", b"{}\n")

                report = self.result()

                self.assert_rejected(report)

    def test_symlink_hardlink_modes_nonregular_files_and_traversal_are_rejected(self):
        cases = ("symlink", "hardlink", "file_mode", "directory_mode", "fifo", "directory_symlink", "traversal")
        for case in cases:
            with self.subTest(case=case):
                self.reset_fixture()
                if case == "symlink":
                    path = self.directory / "active.json"
                    payload = path.read_bytes()
                    path.unlink()
                    target = self.directory / "target.json"
                    target.write_bytes(payload)
                    target.chmod(0o600)
                    path.symlink_to(target.name)
                elif case == "hardlink":
                    outside = self.directory.parent / (self.directory.name + "-hardlink")
                    self.addCleanup(lambda path=outside: path.unlink(missing_ok=True))
                    os.link(self.directory / "active.json", outside)
                elif case == "file_mode":
                    (self.directory / "active.json").chmod(0o640)
                elif case == "directory_mode":
                    self.directory.chmod(0o750)
                elif case == "fifo":
                    path = self.directory / "boot-release.json"
                    path.unlink()
                    os.mkfifo(path, 0o600)
                elif case == "directory_symlink":
                    alias = self.directory.parent / (self.directory.name + "-alias")
                    alias.symlink_to(self.directory, target_is_directory=True)
                    self.addCleanup(lambda path=alias: path.unlink(missing_ok=True))
                    report = recovery_preflight.preflight(str(alias))
                    self.assert_rejected(report, "unsafe_directory")
                    continue
                else:
                    held = self.read_json("recovery-plan.json")
                    held["root"] = "/private/tmp/../tmp/preflight-must-not-follow"
                    self.write_json("recovery-plan.json", held)
                    self.set_expected("recovery_plan_sha256", sha256((self.directory / "recovery-plan.json").read_bytes()))

                report = self.result()
                self.assert_rejected(report)

    def test_size_limits_are_enforced_for_small_json_manifest_workflow_and_markers(self):
        cases = (
            ("active.json", 64 * 1024 + 1),
            ("capture-manifest.json", 1024 * 1024 + 1),
            ("active-WORKFLOW.md", 1024 * 1024 + 1),
            ("active-release-sha256.txt", 129),
        )
        for filename, size in cases:
            with self.subTest(filename=filename):
                self.reset_fixture()
                self.write(filename, b"x" * size)

                report = self.result()

                self.assert_rejected(report, "input_size_limit")

    def test_cli_reports_json_exit_codes_without_leaking_input_or_writing(self):
        before = self.directory_snapshot()
        command = [sys.executable, str(MODULE_PATH), "--directory", str(self.directory)]

        held = subprocess.run(command, capture_output=True, text=True, timeout=5)

        self.assertEqual(held.returncode, 2, held.stderr)
        held_report = json.loads(held.stdout)
        self.assertEqual(held_report["status"], "aligned_but_held")
        self.assertFalse(held_report["restore_ready"])
        self.assertFalse(held_report["dispatch_allowed"])
        self.assertNotIn(str(self.directory), held.stdout + held.stderr)
        self.assertEqual(before, self.directory_snapshot())

        private_marker = "private-raw-input-marker-88421"
        self.write("plan.json", json.dumps({"version": 1, "unexpected": private_marker}).encode() + b"\n")
        before_rejected = self.directory_snapshot()
        rejected = subprocess.run(command, capture_output=True, text=True, timeout=5)

        self.assertEqual(rejected.returncode, 1)
        rejected_report = json.loads(rejected.stdout)
        self.assertEqual(rejected_report["status"], "rejected")
        self.assertRegex(rejected_report["reason"], r"^[a-z][a-z0-9_]*$")
        self.assertNotIn(private_marker, rejected.stdout + rejected.stderr)
        self.assertNotIn(str(self.directory), rejected.stdout + rejected.stderr)
        self.assertEqual(before_rejected, self.directory_snapshot())

        self.reset_fixture()
        mismatch_marker = "private-mismatch-marker-22517"
        active = self.read_json("active.json")
        active["service_revision"] = OLD_REVISION
        active["activated_at"] = mismatch_marker
        self.write_json("active.json", active)
        before_mismatch = self.directory_snapshot()
        mismatch = subprocess.run(command, capture_output=True, text=True, timeout=5)

        self.assertEqual(mismatch.returncode, 1)
        mismatch_report = json.loads(mismatch.stdout)
        self.assertEqual(mismatch_report["status"], "mismatch")
        self.assertFalse(mismatch_report["dispatch_allowed"])
        self.assertNotIn(mismatch_marker, mismatch.stdout + mismatch.stderr)
        self.assertNotIn(str(self.directory), mismatch.stdout + mismatch.stderr)
        self.assertEqual(before_mismatch, self.directory_snapshot())


if __name__ == "__main__":
    unittest.main()
