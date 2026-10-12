from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

DEPLOY = Path(__file__).resolve().parents[1]
REPOSITORY = DEPLOY.parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, DEPLOY / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verifier = load("verify_backup")
cloud_io = load("cloud_io")


def digest(data):
    return hashlib.sha256(data).hexdigest()


class BackupVerifierTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.stage = self.base / "stage"
        self.target = self.base / "target"
        self.source = self.base / "source"
        for root in (self.stage, self.target, self.source):
            root.mkdir(mode=0o700)
        for name in ("state", "evidence"):
            (self.source / name).mkdir(mode=0o700)
            (self.target / name).mkdir(mode=0o700)
        # Use the actual merged migrations as the oracle, not verifier constants.
        text = (REPOSITORY / "elixir/lib/symphony_elixir/workstream_store.ex").read_text()
        self.migrations = re.findall(r'@migration_v[123] """(.*?)"""', text, re.S)
        text = (REPOSITORY / "elixir/lib/symphony_elixir/evidence.ex").read_text()
        self.evidence_sql = re.search(r'@schema """(.*?)"""', text, re.S).group(1)
        self.revision = "a" * 40
        self.fixture(3, True)

    def fixture(self, version, include_evidence, include_state=True):
        for root in (self.stage, self.source / "state", self.source / "evidence"):
            for path in root.iterdir():
                path.unlink()
        if include_state:
            with closing(sqlite3.connect(self.source / "state/runs.sqlite")) as db:
                db.executescript("".join(self.migrations[:version]))
                db.execute("PRAGMA user_version=" + str(version))
                db.execute("INSERT INTO runs VALUES ('run-one','task-one',?)", (b"opaque application payload",))
                db.commit()
        if include_evidence:
            with closing(sqlite3.connect(self.source / "evidence/index.sqlite")) as db:
                db.executescript(self.evidence_sql)
        public = self.source / "public.json"
        public.write_text(json.dumps({"role": "coordinator", "service_revision": self.revision,
                                      "coordinator_workflow": "linear" if include_state and version >= 2 else "pilot"}))
        class FakeCloud:
            config = {"backup_bucket": "fixture", "service_revision": self.revision}
            objects = {}
            def put(_, bucket, name, data):
                FakeCloud.objects[name] = data
        with patch.object(cloud_io, "DATA_ROOT", self.source), patch.object(cloud_io, "PUBLIC_CONFIG", public):
            cloud_io.backup(FakeCloud())
        for name, data in FakeCloud.objects.items():
            self.write(name.rsplit("/", 1)[1], data)
        self.version = version if include_state else None
        self.include_evidence = include_evidence
        self.state_names = ["runs.sqlite"] if include_state else []
        self.manifest_hash = digest((self.stage / "manifest.json").read_bytes())
        self.public_hash = digest((self.stage / "public.json").read_bytes())

    def write(self, name, data):
        path = self.stage / name
        path.write_bytes(data)
        path.chmod(0o600)

    def manifest_edit(self, edit):
        manifest = json.loads((self.stage / "manifest.json").read_bytes())
        edit(manifest)
        self.write("manifest.json", json.dumps(manifest).encode())
        # Model a separately approved manifest for shape/semantics tests only.
        self.manifest_hash = digest((self.stage / "manifest.json").read_bytes())

    def check(self, **changes):
        kwargs = dict(directory=self.stage, manifest_sha256=self.manifest_hash, public_sha256=self.public_hash,
                      revision=self.revision, state_names=self.state_names, state_version=self.version,
                      include_evidence=self.include_evidence, destination=self.target,
                      owner_uid=os.geteuid(), owner_gid=os.getegid())
        kwargs.update(changes)
        return verifier.verify(**kwargs)

    def tree(self):
        return {str(path.relative_to(self.base)): (path.stat().st_mode, path.read_bytes())
                for path in self.base.rglob("*") if path.is_file()}

    def command(self):
        return [sys.executable, str(DEPLOY / "verify_backup.py"), str(self.stage),
                "--manifest-sha256", self.manifest_hash, "--public-sha256", self.public_hash,
                "--service-revision", self.revision,
                *( ["--state-database", "runs.sqlite", "--state-schema", str(self.version)] if self.state_names else ["--no-state-databases"] ),
                *( ["--evidence-index"] if self.include_evidence else [] ),
                "--destination-root", str(self.target), "--owner-uid", str(os.geteuid()), "--owner-gid", str(os.getegid())]

    def test_merged_backup_formats_pass_offline_checks_without_restore_or_writes(self):
        for version, evidence in ((1, False), (2, False), (3, True)):
            with self.subTest(version=version):
                self.fixture(version, evidence)
                (self.target / "state").chmod(0o750)  # Actual bootstrap mode.
                before = self.tree()
                report = self.check()
                self.assertEqual(report["status"], "offline_checks_passed")
                self.assertIs(report["restore_ready"], False)
                self.assertTrue(report["restore_blockers"])
                self.assertEqual(before, self.tree())
                self.assertEqual(list((self.target / "state").iterdir()), [])
                self.assertNotIn("opaque application payload", json.dumps(report))

    def test_explicit_configuration_only_and_evidence_only_backups_remain_blocked(self):
        for evidence in (False, True):
            with self.subTest(evidence=evidence):
                self.fixture(3, evidence, include_state=False)
                report = self.check()
                self.assertIs(report["restore_ready"], False)
                self.assertIn("declared backup contains no workstream database", report["restore_blockers"])
                self.assertEqual(report["checked_destination_directories"], ["evidence"] if evidence else [])
                result = subprocess.run(self.command(), capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn("--state-schema", self.command())
        self.fixture(3, False, include_state=False)
        self.manifest_edit(lambda m: m.update(note="pretend runtime exists"))
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        self.fixture(3, False, include_state=False)
        public = json.loads((self.stage / "public.json").read_bytes())
        public["coordinator_workflow"] = "linear"
        self.write("public.json", json.dumps(public).encode())
        self.public_hash = digest((self.stage / "public.json").read_bytes())
        with self.assertRaisesRegex(verifier.InvalidBackup, "Linear configuration requires"):
            self.check()

    def test_checksum_anchors_reject_tampering(self):
        for name in ("manifest.json", "public.json", "runs.sqlite", "evidence-index.sqlite"):
            with self.subTest(file=name):
                self.fixture(3, True)
                self.write(name, (self.stage / name).read_bytes() + b"tamper")
                with self.assertRaises(verifier.InvalidBackup):
                    self.check()

    def test_inventory_and_paths_reject_missing_duplicate_extra_and_override(self):
        edits = (
            lambda m: m["databases"].pop(),
            lambda m: m["databases"].append(dict(m["databases"][0])),
            lambda m: m["databases"][0].update(name="../outside.sqlite"),
            lambda m: m["databases"][0].update(restore_path="state/runs.sqlite"),
            lambda m: m["databases"][1].update(restore_path="../index.sqlite"),
            lambda m: m.update(version=2),
            lambda m: m.update(version=True),
        )
        for edit in edits:
            with self.subTest(edit=edit):
                self.fixture(3, True)
                self.manifest_edit(edit)
                with self.assertRaises(verifier.InvalidBackup):
                    self.check()
        self.fixture(3, True)
        self.write("runs.sqlite-wal", b"not a standalone backup")
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        self.fixture(3, True)
        (self.stage / "runs.sqlite").unlink()
        with self.assertRaises(verifier.InvalidBackup):
            self.check()

    def test_hash_valid_database_with_changed_constraints_or_version_rejects(self):
        for change in (
            "DROP TABLE evidence_outbox",
            "CREATE VIEW sneaky AS SELECT * FROM runs",
            "PRAGMA user_version=4",
            "DROP TABLE evidence_outbox; CREATE TABLE evidence_outbox (seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, snapshot BLOB NOT NULL, event BLOB NOT NULL)",
            "DROP TABLE evidence_outbox; CREATE TABLE evidence_outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, snapshot BLOB NOT NULL, event BLOB NOT NULL)",
            "DROP TABLE stage_attempts; CREATE TABLE stage_attempts (run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, payload BLOB NOT NULL)",
        ):
            with self.subTest(change=change):
                self.fixture(3, True)
                path = self.stage / "runs.sqlite"
                with closing(sqlite3.connect(path)) as db:
                    db.executescript(change)
                self.manifest_edit(lambda m: m["databases"][0].update(sha256=digest(path.read_bytes())))
                with self.assertRaises(verifier.InvalidBackup):
                    self.check()

    def test_unsafe_source_and_destination_permissions_links_and_existing_store(self):
        with self.assertRaises(verifier.InvalidBackup):
            self.check(owner_uid=os.geteuid() + 1)
        with self.assertRaises(verifier.InvalidBackup):
            self.check(owner_gid=os.getegid() + 1)
        for path in (self.stage, self.target / "state", self.target / "evidence"):
            original = path.stat().st_mode & 0o777
            path.chmod(0o777)
            with self.assertRaises(verifier.InvalidBackup):
                self.check()
            path.chmod(original)
        source = self.stage / "runs.sqlite"
        data = source.read_bytes()
        source.unlink()
        outside = self.base / "outside.sqlite"
        outside.write_bytes(data)
        outside.chmod(0o600)
        source.symlink_to(outside)
        with self.assertRaises(OSError):
            self.check()
        source.unlink()
        self.write("runs.sqlite", data)
        os.link(source, self.base / "hardlink.sqlite")
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        (self.base / "hardlink.sqlite").unlink()
        (self.target / "state/runs.sqlite").write_bytes(b"retained original")
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        self.assertEqual((self.target / "state/runs.sqlite").read_bytes(), b"retained original")

    def test_changed_evidence_uniqueness_constraint_rejects(self):
        path = self.stage / "evidence-index.sqlite"
        with closing(sqlite3.connect(path)) as db:
            db.executescript("DROP TABLE evidence_events; CREATE TABLE evidence_events (seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL, event_id TEXT NOT NULL, payload TEXT NOT NULL)")
        self.manifest_edit(lambda m: m["databases"][1].update(sha256=digest(path.read_bytes())))
        with self.assertRaises(verifier.InvalidBackup):
            self.check()

    def test_directory_aliases_and_missing_memory_deserialization_reject(self):
        with self.assertRaises(verifier.InvalidBackup):
            self.check(destination=self.target / ".." / "target")
        alias = self.base / "alias"
        alias.symlink_to(self.stage, target_is_directory=True)
        with self.assertRaises(verifier.InvalidBackup):
            self.check(directory=alias)
        state = self.target / "state"
        state.rename(self.target / "original-state")
        state.symlink_to(self.target / "original-state", target_is_directory=True)
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        state.unlink()
        (self.target / "original-state").rename(state)
        with patch.object(verifier.sqlite3, "Connection", object), \
                self.assertRaisesRegex(verifier.InvalidBackup, "deserialization support"):
            self.check()

    def test_cli_passed_checks_remain_blocked_with_no_side_effects(self):
        before = self.tree()
        result = subprocess.run(self.command(), capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["status"], "offline_checks_passed")
        self.assertIs(json.loads(result.stdout)["restore_ready"], False)
        self.assertEqual(before, self.tree())

    def test_duplicate_json_keys_corruption_and_size_limits_reject(self):
        original = (self.stage / "manifest.json").read_bytes()
        self.write("manifest.json", original[:-1] + b',"version":1}')
        self.manifest_hash = digest((self.stage / "manifest.json").read_bytes())
        with self.assertRaisesRegex(verifier.InvalidBackup, "duplicate keys"):
            self.check()
        self.fixture(3, True)
        self.write("runs.sqlite", b"invalid database")
        self.manifest_edit(lambda m: m["databases"][0].update(sha256=digest(b"invalid database")))
        with self.assertRaises(verifier.InvalidBackup):
            self.check()
        self.fixture(3, True)
        with patch.object(verifier, "MAX_DATABASE_BYTES", 1), self.assertRaises(verifier.InvalidBackup):
            self.check()

    def test_cli_rejection_is_redacted_and_read_only(self):
        self.write("manifest.json", b'{"credential":"secret-marker", invalid-json')
        self.manifest_hash = digest((self.stage / "manifest.json").read_bytes())
        result = subprocess.run(
            self.command(),
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIs(json.loads(result.stdout)["restore_ready"], False)
        self.assertNotIn("secret-marker", result.stdout + result.stderr)
        self.assertEqual(list((self.target / "state").iterdir()), [])

    def test_cli_argument_errors_reject_without_private_values(self):
        command = self.command()
        schema_position = command.index("--state-schema")
        variants = (
            command[:schema_position] + command[schema_position + 2:],
            command + ["--unknown-private-argument", "secret-marker"],
        )
        for args in variants:
            with self.subTest(args=args):
                result = subprocess.run(args, capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(json.loads(result.stdout)["status"], "rejected")
                self.assertNotIn("secret-marker", result.stdout + result.stderr)
                self.assertEqual(list((self.target / "state").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
