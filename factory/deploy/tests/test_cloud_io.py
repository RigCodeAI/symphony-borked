import hashlib
from contextlib import closing
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("cloud_io", Path(__file__).resolve().parents[1] / "cloud_io.py")
cloud_io = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud_io)

sanitize_spec = importlib.util.spec_from_file_location(
    "factory_sanitize", Path(__file__).resolve().parents[1] / "sanitize.py")
sanitize = importlib.util.module_from_spec(sanitize_spec)
sanitize_spec.loader.exec_module(sanitize)


class FakeCloud:
    config = {"role": "coordinator", "archive_bucket": "archive", "backup_bucket": "backups", "service_revision": "a" * 40}

    def __init__(self):
        self.objects = {}

    def put(self, bucket, name, data):
        key = (bucket, name)
        if key in self.objects and self.objects[key] != data:
            raise ValueError("immutable object changed")
        self.objects[key] = data


def archive_bytes(name="file.txt", kind=tarfile.REGTYPE):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        member = tarfile.TarInfo(name)
        member.type = kind
        if kind == tarfile.REGTYPE:
            member.size = 2
            archive.addfile(member, io.BytesIO(b"ok"))
        else:
            member.linkname = "/etc/passwd"
            archive.addfile(member)
    return data.getvalue()


class ReleaseTests(unittest.TestCase):
    def test_unsafe_release_cannot_write_outside_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, kind in (("../outside", tarfile.REGTYPE), ("/tmp/outside", tarfile.REGTYPE),
                               ("link", tarfile.SYMTYPE), ("link", tarfile.LNKTYPE)):
                with self.subTest(name=name, kind=kind), self.assertRaises(ValueError):
                    cloud_io.safe_unpack(archive_bytes(name, kind), root / "release")
                self.assertFalse((root / "release").exists())
                self.assertFalse((root / "outside").exists())

    def test_existing_release_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "release"
            cloud_io.safe_unpack(archive_bytes(), root)
            with self.assertRaises(ValueError):
                cloud_io.safe_unpack(archive_bytes("replacement"), root)
            self.assertEqual((root / "file.txt").read_bytes(), b"ok")
            self.assertFalse((root / "replacement").exists())


class EvidenceTests(unittest.TestCase):
    def prepare_run(self, directory):
        run = Path(directory) / "runs" / "smoke-1"
        run.mkdir(parents=True)
        for name in ("report.json", "mix.log", "candidate.diff", "finished.json", "revision.json", "submission.json"):
            (run / name).write_text("exact-secret-value ghp_examplecredential eyJabc.def.ghi")
        return run

    def test_archive_is_redacted_and_manifest_matches_uploaded_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            run = self.prepare_run(directory)
            cloud = FakeCloud()
            with patch.object(cloud_io, "DATA_ROOT", Path(directory)), \
                    patch.object(cloud_io, "credential_values", return_value=["exact-secret-value"]):
                cloud_io.archive_runs(cloud)
            manifest = json.loads(cloud.objects[("archive", "runs/smoke-1/manifest.json")])
            for name, entry in manifest["artifacts"].items():
                uploaded = cloud.objects[("archive", f"runs/smoke-1/{name}")]
                self.assertNotIn(b"exact-secret-value", uploaded)
                self.assertNotIn(b"ghp_", uploaded)
                self.assertNotIn(b"eyJabc", uploaded)
                self.assertEqual(hashlib.sha256(uploaded).hexdigest(), entry["sha256"])
            self.assertTrue((run / ".archived").is_file())

    def test_symlink_evidence_is_rejected_without_reading_target(self):
        with tempfile.TemporaryDirectory() as directory:
            run = self.prepare_run(directory)
            outside = Path(directory) / "private.txt"
            outside.write_text("private credential")
            (run / "report.json").unlink()
            (run / "report.json").symlink_to(outside)
            cloud = FakeCloud()
            with patch.object(cloud_io, "DATA_ROOT", Path(directory)), \
                    patch.object(cloud_io, "credential_values", return_value=[]), self.assertRaises(OSError):
                cloud_io.archive_runs(cloud)
            self.assertEqual(outside.read_text(), "private credential")
            self.assertEqual(cloud.objects, {})
            self.assertFalse((run / ".archived").exists())

    def test_existing_completion_marker_is_not_followed(self):
        with tempfile.TemporaryDirectory() as directory:
            run = self.prepare_run(directory)
            outside = Path(directory) / "private.txt"
            outside.write_text("untouched")
            (run / ".archived").symlink_to(outside)
            with patch.object(cloud_io, "DATA_ROOT", Path(directory)), self.assertRaises(ValueError):
                cloud_io.archive_runs(FakeCloud())
            self.assertEqual(outside.read_text(), "untouched")


class BackupTests(unittest.TestCase):
    def test_sqlite_online_backup_contains_committed_wal_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            state = root / "state"
            state.mkdir()
            public = root / "public.json"
            public.write_text('{"role":"coordinator"}')
            source = sqlite3.connect(state / "runs.sqlite")
            source.execute("PRAGMA journal_mode=WAL")
            source.execute("CREATE TABLE runs(id TEXT)")
            source.execute("INSERT INTO runs VALUES ('committed-in-wal')")
            source.commit()
            cloud = FakeCloud()
            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public):
                cloud_io.backup(cloud)
            source.close()
            key = next(key for key in cloud.objects if key[1].endswith("/runs.sqlite"))
            restored = root / "restored.sqlite"
            restored.write_bytes(cloud.objects[key])
            with closing(sqlite3.connect(restored)) as db:
                self.assertEqual(db.execute("SELECT id FROM runs").fetchone(), ("committed-in-wal",))
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            manifest_key = next(key for key in cloud.objects if key[1].endswith("/manifest.json"))
            manifest = json.loads(cloud.objects[manifest_key])
            self.assertEqual([item["name"] for item in manifest["databases"]], ["runs.sqlite"])

    def test_evidence_index_is_backed_up_with_restore_path_and_wal_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            evidence = root / "evidence"
            evidence.mkdir()
            source_path = evidence / "index.sqlite"
            source = sqlite3.connect(source_path)
            source.execute("PRAGMA journal_mode=WAL")
            source.execute("CREATE TABLE artifacts(name TEXT)")
            source.execute("INSERT INTO artifacts VALUES ('committed-evidence')")
            source.commit()
            public = root / "public.json"
            public.write_text('{"role":"coordinator"}')
            cloud = FakeCloud()

            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public):
                cloud_io.backup(cloud)
            source.close()

            manifest_key = next(key for key in cloud.objects if key[1].endswith("/manifest.json"))
            manifest = json.loads(cloud.objects[manifest_key])
            evidence_entry = next(item for item in manifest["databases"]
                                  if item["name"] == "evidence-index.sqlite")
            self.assertEqual(evidence_entry["restore_path"], "evidence/index.sqlite")
            backup_key = ("backups", manifest_key[1].rsplit("/", 1)[0] + "/evidence-index.sqlite")
            restored = root / "restored-evidence.sqlite"
            restored.write_bytes(cloud.objects[backup_key])
            with closing(sqlite3.connect(restored)) as db:
                self.assertEqual(db.execute("SELECT name FROM artifacts").fetchone(), ("committed-evidence",))
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone(), ("ok",))

    def test_unsafe_evidence_index_paths_block_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root = base / "root"
            root.mkdir()
            public = base / "public.json"
            public.write_text('{"role":"coordinator"}')
            outside = base / "outside"
            outside.mkdir()
            (outside / "index.sqlite").write_bytes(b"not a database")
            cloud = FakeCloud()

            evidence = root / "evidence"
            evidence.symlink_to(outside, target_is_directory=True)
            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                    self.assertRaises(ValueError):
                cloud_io.backup(cloud)
            self.assertEqual(cloud.objects, {})

            evidence.unlink()
            evidence.mkdir()
            (evidence / "index.sqlite").symlink_to(outside / "index.sqlite")
            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                    self.assertRaises(ValueError):
                cloud_io.backup(cloud)
            self.assertEqual(cloud.objects, {})

            root_alias = base / "root-alias"
            root_alias.symlink_to(root, target_is_directory=True)
            with patch.object(cloud_io, "DATA_ROOT", root_alias), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                    self.assertRaises(ValueError):
                cloud_io.backup(cloud)
            self.assertEqual(cloud.objects, {})

    def test_evidence_index_backup_enforces_size_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            evidence = root / "evidence"
            evidence.mkdir()
            index = evidence / "index.sqlite"
            index.touch()
            index.write_bytes(b"x")
            with index.open("r+b") as handle:
                handle.truncate(cloud_io.MAX_EVIDENCE_INDEX_BYTES + 1)
            public = root / "public.json"
            public.write_text('{"role":"coordinator"}')
            cloud = FakeCloud()

            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                    self.assertRaises(ValueError):
                cloud_io.backup(cloud)

            self.assertEqual(cloud.objects, {})


class TransportTests(unittest.TestCase):
    def test_put_retries_identical_object_but_refuses_changed_content(self):
        cloud = cloud_io.Cloud({})
        with patch.object(cloud, "headers", return_value={}), \
                patch.object(cloud_io, "request", side_effect=RuntimeError("cloud request failed: HTTP 412")), \
                patch.object(cloud, "object", return_value=b"first"):
            cloud.put("bucket", "immutable", b"first")
            with self.assertRaisesRegex(RuntimeError, "checksum differs"):
                cloud.put("bucket", "immutable", b"changed")

    def test_secret_version_and_project_reject_before_network(self):
        for ref, version in (("projects/other/secrets/secret", "1"),
                             ("projects/pilot/secrets/secret", "latest")):
            cloud = cloud_io.Cloud({"project_id": "pilot", "secret_ids": {"key": ref}, "secret_versions": {"key": version}})
            with patch.object(cloud_io, "request") as request, self.assertRaises(ValueError):
                cloud.secret("key")
            request.assert_not_called()


def auth_bytes(access="access-token-value-123", refresh="refresh-token-value-123"):
    return json.dumps({
        "auth_mode": "chatgpt",
        "tokens": {
            "access_token": access,
            "refresh_token": refresh,
            "id_token": "eyJsynthetic.id.token-value",
        },
    }, sort_keys=True).encode()


class ModelAuthTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "homes" / "factory-worker"
        self.codex = self.home / ".codex"
        self.codex.mkdir(parents=True, mode=0o700)
        self.auth = self.codex / "auth.json"
        self.marker = self.root / "model-auth.source.json"
        self.legacy = self.root / "legacy-model-auth.json"
        self.worker_uid = os.getuid()
        self.worker_gid = os.getgid()
        self.root_uid = os.getuid()
        self.config = {
            "role": "worker",
            "project_id": "pilot",
            "secret_ids": {"model_auth": "projects/pilot/secrets/model-auth"},
            "secret_versions": {"model_auth": "7"},
        }

    def ensure(self, cloud):
        return cloud_io.ensure_model_auth(
            cloud, self.auth, self.marker, self.legacy,
            worker_uid=self.worker_uid, worker_gid=self.worker_gid,
            root_uid=self.root_uid, root_gid=self.worker_gid, strict_paths=False)

    def fake_cloud(self, secret):
        return type("FakeAuthCloud", (), {"config": self.config, "secret": staticmethod(secret)})()

    def test_same_pinned_source_preserves_rotated_refresh_token_without_fetch(self):
        cloud = self.fake_cloud(lambda _key: auth_bytes())
        self.assertEqual(self.ensure(cloud), "seeded")
        rotated = auth_bytes("access-after-refresh-123", "rotated-refresh-token-123")
        self.auth.write_bytes(rotated)
        self.auth.chmod(0o600)

        def unexpected_fetch(_key):
            raise AssertionError("same pinned secret version must not be fetched again")

        cloud.secret = unexpected_fetch
        self.assertEqual(self.ensure(cloud), "preserved")
        self.assertEqual(self.auth.read_bytes(), rotated)

    def test_changed_pinned_version_reseeds_and_updates_root_only_marker(self):
        self.ensure(self.fake_cloud(lambda _key: auth_bytes()))
        self.auth.write_bytes(auth_bytes("refreshed-access-token-123", "refreshed-token-123"))
        self.auth.chmod(0o600)
        self.config["secret_versions"]["model_auth"] = "8"
        replacement = auth_bytes("new-access-token-123", "new-refresh-token-123")
        self.assertEqual(self.ensure(self.fake_cloud(lambda _key: replacement)), "seeded")
        self.assertEqual(self.auth.read_bytes(), replacement)
        marker = json.loads(self.marker.read_bytes())
        self.assertEqual(marker["secret_version"], "8")
        self.assertEqual(self.marker.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.marker.stat().st_uid, self.root_uid)
        self.assertEqual(self.auth.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.auth.stat().st_uid, self.worker_uid)

    def test_changed_pinned_secret_id_reseeds_even_when_version_is_unchanged(self):
        self.ensure(self.fake_cloud(lambda _key: auth_bytes()))
        self.auth.write_bytes(auth_bytes("rotated-access-token-123", "rotated-refresh-token-123"))
        self.auth.chmod(0o600)
        self.config["secret_ids"]["model_auth"] = "projects/pilot/secrets/model-auth-next"
        replacement = auth_bytes("new-access-token-456", "new-refresh-token-456")
        self.assertEqual(self.ensure(self.fake_cloud(lambda _key: replacement)), "seeded")
        self.assertEqual(self.auth.read_bytes(), replacement)
        marker = json.loads(self.marker.read_bytes())
        self.assertEqual(marker["secret_id"], self.config["secret_ids"]["model_auth"])

    def test_insecure_existing_permissions_fail_closed(self):
        self.ensure(self.fake_cloud(lambda _key: auth_bytes()))
        self.auth.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "unsafe type, owner, permissions"):
            self.ensure(self.fake_cloud(lambda _key: self.fail("unsafe auth should not be read or replaced")))

    def test_known_legacy_symlink_migrates_but_arbitrary_symlink_is_rejected(self):
        payload = auth_bytes("legacy-access-token-123", "legacy-refresh-token-123")
        self.legacy.write_bytes(payload)
        self.legacy.chmod(0o600)
        self.auth.symlink_to(self.legacy)
        marker = json.dumps({
            "secret_id": self.config["secret_ids"]["model_auth"],
            "secret_version": "7",
        }).encode()
        cloud_io._atomic_private_write(self.marker, marker, self.root_uid, self.worker_gid)
        self.assertEqual(self.ensure(self.fake_cloud(lambda _key: self.fail("unexpected fetch"))), "migrated")
        self.assertFalse(self.auth.is_symlink())
        self.assertEqual(self.auth.read_bytes(), payload)

        other = self.root / "outside.json"
        other.write_bytes(auth_bytes())
        other.chmod(0o600)
        self.auth.unlink()
        self.auth.symlink_to(other)
        with self.assertRaisesRegex(ValueError, "known legacy path"):
            self.ensure(self.fake_cloud(lambda _key: auth_bytes()))
        self.assertEqual(other.read_bytes(), auth_bytes())

    def test_invalid_chatgpt_secret_is_rejected_without_writing_files(self):
        with self.assertRaisesRegex(ValueError, "ChatGPT access and refresh"):
            self.ensure(self.fake_cloud(lambda _key: b'{"auth_mode":"chatgpt","tokens":{}}'))
        self.assertFalse(self.auth.exists())
        self.assertFalse(self.marker.exists())

    def test_cloud_and_worker_redactors_load_exact_values_from_retained_and_legacy_auth(self):
        current = self.codex / "auth.json"
        current.write_bytes(auth_bytes("retained-access-value-123", "retained-refresh-value-123"))
        current.chmod(0o600)
        self.legacy.write_bytes(auth_bytes("legacy-access-value-123", "legacy-refresh-value-123"))
        self.legacy.chmod(0o600)
        with patch.object(cloud_io, "MODEL_AUTH_PATH", current), \
                patch.object(cloud_io, "LEGACY_MODEL_AUTH_PATH", self.legacy), \
                patch.object(sanitize, "MODEL_AUTH_PATH", current), \
                patch.object(sanitize, "LEGACY_MODEL_AUTH_PATH", self.legacy):
            values = cloud_io.credential_values()
            text = " ".join(values)
            self.assertIn("retained-refresh-value-123", text)
            self.assertIn("legacy-refresh-value-123", text)
            sanitized = sanitize.scrub_text(
                "retained-refresh-value-123 legacy-refresh-value-123", sanitize.known_model_auth_values())
            self.assertNotIn("retained-refresh-value-123", sanitized)
            self.assertNotIn("legacy-refresh-value-123", sanitized)
            self.assertEqual(sanitized.count("[REDACTED]"), 2)


@unittest.skipUnless(shutil.which("ssh-keygen"), "ssh-keygen is required for host-key lifecycle tests")
class WorkerHostKeyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.key_dir = self.root / "ssh-host-keys"
        self.marker = self.root / "worker-hostkey.json"
        self.owner_uid = os.getuid()

    def ensure(self):
        return cloud_io.ensure_worker_host_key(
            self.key_dir, self.marker, owner_uid=self.owner_uid, strict_paths=False)

    def test_repeated_bootstrap_preserves_durable_identity_and_pin_marker(self):
        public = self.ensure()
        private_path = self.key_dir / "ssh_host_ed25519_key"
        public_path = self.key_dir / "ssh_host_ed25519_key.pub"
        private_before = private_path.read_bytes()
        marker_before = self.marker.read_bytes()

        self.assertEqual(public, public_path.read_text().strip())
        self.assertEqual(public.split()[0], "ssh-ed25519")
        self.assertEqual(private_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(public_path.stat().st_mode & 0o777, 0o644)
        self.assertEqual(self.key_dir.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.marker.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.ensure(), public)
        self.assertEqual(private_path.read_bytes(), private_before)
        self.assertEqual(self.marker.read_bytes(), marker_before)

    def test_initialized_identity_missing_private_key_fails_closed(self):
        self.ensure()
        private_path = self.key_dir / "ssh_host_ed25519_key"
        private_path.unlink()
        with self.assertRaisesRegex(ValueError, "private key is missing"):
            self.ensure()
        self.assertFalse(private_path.exists())

    def test_symlinked_host_key_directory_is_rejected(self):
        target = self.root / "real-host-keys"
        target.mkdir(mode=0o700)
        self.key_dir.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "unsafe type, owner, or mode"):
            self.ensure()
        self.assertEqual(list(target.iterdir()), [])

    def test_symlinked_private_key_is_rejected_without_following_target(self):
        self.ensure()
        private_path = self.key_dir / "ssh_host_ed25519_key"
        outside = self.root / "outside-key"
        outside.write_text("untouched")
        outside.chmod(0o600)
        private_path.unlink()
        private_path.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "unsafe type, owner, mode"):
            self.ensure()
        self.assertEqual(outside.read_text(), "untouched")


if __name__ == "__main__":
    unittest.main()
