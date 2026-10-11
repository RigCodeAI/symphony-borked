import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "evidence_archive.py"
CROSS_LANGUAGE_TASK_ID = "linear/9d2c0479-c1f9-441c-9ad2-6d6500072eb1"
spec = importlib.util.spec_from_file_location("evidence_archive", MODULE_PATH)
evidence_archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence_archive)
cloud_io_spec = importlib.util.spec_from_file_location("cloud_io", MODULE_PATH.with_name("cloud_io.py"))
cloud_io = importlib.util.module_from_spec(cloud_io_spec)
cloud_io_spec.loader.exec_module(cloud_io)


class FakeCloud:
    config = {"archive_bucket": "archive"}

    def __init__(self):
        self.objects = {}
        self.put_count = 0
        self.object_count = 0
        self.fail_on_put = None
        self.corrupt_reads = set()

    def put(self, bucket, name, payload):
        self.put_count += 1
        if self.fail_on_put == self.put_count:
            raise RuntimeError("simulated interruption")
        key = (bucket, name)
        if key in self.objects and self.objects[key] != payload:
            raise RuntimeError("immutable object collision")
        self.objects[key] = payload

    def object(self, bucket, name):
        self.object_count += 1
        key = (bucket, name)
        if key in self.corrupt_reads:
            return b"corrupt remote object"
        return self.objects[key]


class EvidenceArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.root = self.base / "evidence"
        self.root.mkdir(mode=0o700)
        self.root.chmod(0o700)
        self.bundles = self.root / "bundles"
        self.bundles.mkdir(mode=0o700)
        self.bundles.chmod(0o700)
        self.cloud = FakeCloud()
        self.allowed_uids = {os.getuid()}
        self.revision, self.bundle = self.make_bundle()

    def tearDown(self):
        self.temporary.cleanup()

    def make_bundle(self, run_id="run-1", task_id=CROSS_LANGUAGE_TASK_ID):
        payloads = {
            "snapshot.json": b'{"ok":true}\n',
            "events.jsonl": b'{"event":"done"}\n',
            "transcript.txt": b"redacted transcript\n",
        }
        manifest = {
            "version": 1,
            "task_id": task_id,
            "run_id": run_id,
            "artifacts": [
                {"name": name, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
                for name, data in payloads.items()
            ],
        }
        manifest_data = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        revision = hashlib.sha256(manifest_data).hexdigest()
        bundle = self.bundles / revision
        bundle.mkdir(mode=0o700)
        bundle.chmod(0o700)
        (bundle / "manifest.json").write_bytes(manifest_data)
        (bundle / "manifest.json").chmod(0o600)
        for name, data in payloads.items():
            (bundle / name).write_bytes(data)
            (bundle / name).chmod(0o600)
        return revision, bundle

    def archive(self, reverify=False):
        return evidence_archive.archive_evidence(
            self.cloud, root=self.root, allowed_uids=self.allowed_uids,
            receipt_gid=os.getegid(), reverify=reverify)

    def receipt(self, revision=None):
        revision = revision or self.revision
        return json.loads((self.root / "receipts" / f"{revision}.json").read_text())

    def test_uploads_bundle_and_writes_verified_receipt_after_readback(self):
        receipts = self.archive()

        self.assertEqual(len(receipts), 1)
        receipt = self.receipt()
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(receipt["run_id"], "run-1")
        self.assertEqual(receipt["manifest_sha256"], self.revision)
        self.assertEqual(json.loads((self.bundle / "manifest.json").read_text())["task_id"],
                         CROSS_LANGUAGE_TASK_ID)
        self.assertTrue(receipt["verified_at"].endswith("Z"))
        self.assertIn(("archive", f"evidence/v1/run-1/{self.revision}/manifest.json"), self.cloud.objects)
        self.assertEqual((self.root / "receipts").stat().st_mode & 0o777, 0o750)
        self.assertEqual((self.root / "receipts" / f"{self.revision}.json").stat().st_mode & 0o777, 0o640)

    def test_cloud_io_loads_archive_helper_from_its_sibling_file(self):
        loader = (
            "import importlib.util, sys; "
            "spec=importlib.util.spec_from_file_location('cloud_io', sys.argv[1]); "
            "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
            "assert callable(module._load_evidence_archiver())"
        )
        subprocess.run(
            [sys.executable, "-I", "-c", loader, str(MODULE_PATH.with_name("cloud_io.py"))],
            check=True, capture_output=True, text=True)

    def test_partial_upload_can_be_retried_and_failed_receipt_is_replaced(self):
        self.cloud.fail_on_put = 2
        with self.assertRaises(RuntimeError):
            self.archive()
        failed = self.receipt()
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(set(failed), {"version", "revision", "status", "type", "run_id"})

        self.cloud.fail_on_put = None
        self.archive()
        self.assertEqual(self.receipt()["status"], "verified")

    def test_corrupt_remote_readback_removes_existing_verified_receipt(self):
        self.archive()
        first_key = next(iter(self.cloud.objects))
        self.cloud.corrupt_reads.add(first_key)

        with self.assertRaises(RuntimeError):
            self.archive(reverify=True)
        self.assertEqual(self.receipt()["status"], "failed")
        self.assertNotIn("message", self.receipt())

    def test_matching_verified_receipt_skips_network_unless_reverified(self):
        self.archive()
        puts, reads = self.cloud.put_count, self.cloud.object_count

        self.archive()
        self.assertEqual((self.cloud.put_count, self.cloud.object_count), (puts, reads))

        self.archive(reverify=True)
        self.assertGreater(self.cloud.put_count, puts)
        self.assertGreater(self.cloud.object_count, reads)

    def test_bundle_without_manifest_is_pending_until_manifest_is_published(self):
        manifest = self.bundle / "manifest.json"
        manifest_data = manifest.read_bytes()
        manifest.unlink()

        self.assertEqual(self.archive(), [])
        self.assertEqual(self.cloud.objects, {})
        self.assertFalse((self.root / "receipts" / f"{self.revision}.json").exists())

        manifest.write_bytes(manifest_data)
        manifest.chmod(0o600)
        self.archive()
        self.assertEqual(self.receipt()["status"], "verified")

    def test_missing_local_file_writes_failed_receipt_before_upload(self):
        (self.bundle / "snapshot.json").unlink()

        with self.assertRaises(RuntimeError):
            self.archive()

        self.assertEqual(self.cloud.objects, {})
        self.assertEqual(self.receipt()["status"], "failed")

    def test_symlink_artifact_is_rejected_and_recorded(self):
        outside = self.base / "private.txt"
        outside.write_text("must not be read")
        (self.bundle / "transcript.txt").unlink()
        (self.bundle / "transcript.txt").symlink_to(outside)

        with self.assertRaises(RuntimeError):
            self.archive()

        self.assertEqual(outside.read_text(), "must not be read")
        self.assertEqual(self.receipt()["status"], "failed")
        self.assertEqual(self.cloud.objects, {})

    def test_symlink_bundle_directory_is_rejected_and_recorded(self):
        outside = self.base / "outside"
        outside.mkdir(mode=0o700)
        outside.chmod(0o700)
        (self.bundles / self.revision).rename(self.base / "saved-bundle")
        (self.bundles / self.revision).symlink_to(outside, target_is_directory=True)

        with self.assertRaises(RuntimeError):
            self.archive()

        self.assertEqual(self.receipt()["status"], "failed")
        self.assertNotIn("run_id", self.receipt())

    def test_immutable_collision_writes_failure_receipt(self):
        self.cloud.objects[("archive", f"evidence/v1/run-1/{self.revision}/events.jsonl")] = b"different"

        with self.assertRaises(RuntimeError):
            self.archive()

        self.assertEqual(self.receipt()["status"], "failed")
        self.assertEqual(self.receipt()["type"], "RuntimeError")

    def test_root_mode_and_symlink_safety(self):
        self.root.chmod(0o750)
        with self.assertRaises(ValueError):
            self.archive()
        self.root.chmod(0o700)

        with self.assertRaises(ValueError):
            evidence_archive.archive_evidence(
                self.cloud, root=self.root, allowed_uids={os.getuid() + 1},
                receipt_gid=os.getegid())

        link = self.base / "evidence-link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            evidence_archive.archive_evidence(self.cloud, root=link, allowed_uids=self.allowed_uids)

        parent_link = self.base / "parent-link"
        parent_link.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(ValueError):
            evidence_archive.archive_evidence(
                self.cloud, root=parent_link / "evidence", allowed_uids=self.allowed_uids)

    def test_verified_receipt_is_compared_with_current_bundle(self):
        self.archive()
        receipt_path = self.root / "receipts" / f"{self.revision}.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["run_id"] = "other-run"
        receipt_path.write_text(json.dumps(receipt))

        with self.assertRaises(RuntimeError):
            self.archive()

        self.assertEqual(self.receipt()["status"], "failed")

    def test_legacy_archive_does_not_mark_run_before_manifest_readback(self):
        root = self.base / "legacy"
        run = root / "runs" / "smoke-1"
        run.mkdir(parents=True)
        for name in ("report.json", "mix.log", "candidate.diff", "finished.json", "revision.json",
                     "submission.json"):
            (run / name).write_text("redacted evidence")

        class ManifestCorruptingCloud(FakeCloud):
            config = {"role": "coordinator", "archive_bucket": "archive"}

            def object(self, bucket, name):
                if name.endswith("manifest.json"):
                    return b"corrupt manifest readback"
                return super().object(bucket, name)

        with patch.object(cloud_io, "DATA_ROOT", root), \
                patch.object(cloud_io, "credential_values", return_value=[]):
            with self.assertRaises(RuntimeError):
                cloud_io.archive_runs(ManifestCorruptingCloud())

        self.assertFalse((run / ".archived").exists())


if __name__ == "__main__":
    unittest.main()
