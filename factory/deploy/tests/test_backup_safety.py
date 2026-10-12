"""Local regression tests; no cloud, coordinator or root access required."""
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("backup_cloud_io", Path(__file__).resolve().parents[1] / "cloud_io.py")
cloud_io = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud_io)


class FakeCloud:
    config = {"backup_bucket": "backups", "service_revision": "a" * 40}

    def __init__(self):
        self.objects = {}

    def put(self, bucket, name, payload):
        self.objects[(bucket, name)] = payload


class BackupSafetyTests(unittest.TestCase):
    def test_locked_snapshot_exits_with_cli_failure_event_then_recovers(self):
        # An outer subprocess timeout makes a reintroduced endless retry fail the
        # test instead of hanging the test runner. Exercise the actual CLI handler.
        for relative in ("state/runs.sqlite", "evidence/index.sqlite"):
            with self.subTest(database=relative), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                source_path = root / relative
                source_path.parent.mkdir()
                public = root / "public.json"
                public.write_text('{"role":"coordinator"}')
                if relative == "evidence/index.sqlite":
                    (root / "state").mkdir()
                    with closing(sqlite3.connect(root / "state" / "earlier.sqlite")) as earlier:
                        earlier.execute("CREATE TABLE earlier(value TEXT)")
                        earlier.commit()
                with closing(sqlite3.connect(source_path)) as holder:
                    holder.execute("PRAGMA journal_mode=DELETE")
                    holder.execute("PRAGMA locking_mode=EXCLUSIVE")
                    holder.execute("CREATE TABLE saved(value TEXT)")
                    holder.execute("INSERT INTO saved VALUES ('retained')")
                    holder.commit()
                    code = """
import importlib.util, io, json, sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
spec = importlib.util.spec_from_file_location('cloud_io', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
root = Path(sys.argv[2])
class FakeCloud:
    config = {'backup_bucket':'backups','service_revision':'a'*40}
    objects = {}
    def put(self, bucket, name, payload): self.objects[(bucket,name)] = payload
cloud = FakeCloud()
stderr = io.StringIO()
with patch.object(module,'DATA_ROOT',root), patch.object(module,'PUBLIC_CONFIG',root/'public.json'), \
     patch.object(module,'SQLITE_BACKUP_TIMEOUT_SECONDS',0.15), patch.object(module,'EVENTS',root/'events.log'), \
     patch.object(module,'Cloud',return_value=cloud), patch.object(module.os,'geteuid',return_value=0), \
     patch.object(module,'CONFIG',SimpleNamespace(stat=lambda: SimpleNamespace(st_uid=0,st_mode=0o600),read_text=lambda: '{}')), \
     patch.object(sys,'argv',['cloud_io.py','backup']), patch.object(sys,'stderr',stderr):
    try: module.run_cli()
    except SystemExit as exc:
        assert exc.code == 1
    else: raise AssertionError('locked backup unexpectedly succeeded')
assert 'SQLite backup failed' in stderr.getvalue() or 'SQLite backup timed out' in stderr.getvalue()
assert 'FACTORY_EVENT backup_failed' in (root/'events.log').read_text()
assert not any(name.endswith('/manifest.json') for _, name in cloud.objects)
if (root/'state'/'earlier.sqlite').exists():
    assert len(cloud.objects) == 1  # Earlier DB is an orphan, never a completed set.
    assert next(iter(cloud.objects))[1].endswith('/earlier.sqlite')
else:
    assert not cloud.objects
print('bounded CLI failure; no manifest; backup_failed recorded')
"""
                    result = subprocess.run(
                        [sys.executable, "-c", code, str(Path(cloud_io.__file__).resolve()), str(root)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=4,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("bounded CLI failure", result.stdout)
                    self.assertEqual(holder.execute("SELECT value FROM saved").fetchone(), ("retained",))

                # Once the owner releases its lock, the same original DB backs up
                # consistently, including the retained transaction, without copying it.
                cloud = FakeCloud()
                with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public):
                    cloud_io.backup(cloud)
                expected_name = "runs.sqlite" if relative.startswith("state/") else "evidence-index.sqlite"
                key = next(key for key in cloud.objects if key[1].endswith("/" + expected_name))
                restored = root / "restored.sqlite"
                restored.write_bytes(cloud.objects[key])
                with closing(sqlite3.connect(restored)) as db:
                    self.assertEqual(db.execute("SELECT value FROM saved").fetchone(), ("retained",))
                    self.assertEqual(db.execute("PRAGMA integrity_check").fetchone(), ("ok",))

    def test_unsafe_state_inventory_rejects_before_sqlite_or_upload(self):
        cases = ("directory_symlink", "dangling_directory_symlink", "file_parent", "database_symlink", "hardlink", "fifo")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                base = Path(directory).resolve()
                root = base / "root"
                root.mkdir()
                outside = base / "outside"
                outside.mkdir()
                database = outside / "outside.sqlite"
                database.write_bytes(b"must not be read")
                public = root / "public.json"
                public.write_text('{}')
                state = root / "state"
                if case == "directory_symlink":
                    state.symlink_to(outside, target_is_directory=True)
                elif case == "dangling_directory_symlink":
                    state.symlink_to(base / "missing", target_is_directory=True)
                elif case == "file_parent":
                    state.write_bytes(b"not a directory")
                else:
                    state.mkdir()
                    # A safe-looking earlier entry must not be read/uploaded first.
                    (state / "a.sqlite").touch()
                    invalid = state / "z.sqlite"
                    if case == "database_symlink":
                        invalid.symlink_to(database)
                    elif case == "hardlink":
                        os.link(database, invalid)
                    else:
                        os.mkfifo(invalid)
                cloud = FakeCloud()
                with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                        patch.object(cloud_io.sqlite3, "connect") as connect, \
                        patch.object(cloud_io.subprocess, "run") as launch, self.assertRaises(ValueError):
                    cloud_io.backup(cloud)
                connect.assert_not_called()
                launch.assert_not_called()
                self.assertEqual(cloud.objects, {})
                self.assertEqual(database.read_bytes(), b"must not be read")

    def test_symlink_replacement_at_sqlite_launch_is_rejected(self):
        for replace_parent in (False, True):
            with self.subTest(replace_parent=replace_parent), tempfile.TemporaryDirectory() as directory:
                base = Path(directory).resolve()
                root = base / "root"
                state = root / "state"
                state.mkdir(parents=True)
                source = state / "runs.sqlite"
                with closing(sqlite3.connect(source)) as db:
                    db.execute("CREATE TABLE saved(value TEXT)")
                outside = base / "outside"
                outside.mkdir()
                with closing(sqlite3.connect(outside / "runs.sqlite")) as db:
                    db.execute("CREATE TABLE secret(value TEXT)")
                    db.execute("INSERT INTO secret VALUES ('must not upload')")
                    db.commit()
                public = root / "public.json"
                public.write_text('{}')
                cloud = FakeCloud()
                real_run = subprocess.run

                def replace_at_open(args, **kwargs):
                    if args[-1] == ".backup snapshot.sqlite":
                        if replace_parent:
                            state.rename(root / "old-state")
                            state.symlink_to(outside, target_is_directory=True)
                        else:
                            source.unlink()
                            source.symlink_to(outside / "runs.sqlite")
                    return real_run(args, **kwargs)

                with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public), \
                        patch.object(cloud_io.subprocess, "run", side_effect=replace_at_open), \
                        self.assertRaisesRegex(RuntimeError, "SQLite backup failed"):
                    cloud_io.backup(cloud)
                self.assertEqual(cloud.objects, {})

    def test_old_or_unknown_sqlite_cli_fails_before_copy(self):
        for version in ("3.39.2 build", "3.38.5 build", "unknown"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                with patch.object(cloud_io.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout=version)) as run, \
                        self.assertRaisesRegex(RuntimeError, "requires CLI version 3.39.3"):
                    cloud_io._sqlite_backup(root / "unused.sqlite", root / "snapshot.sqlite")
                self.assertEqual(run.call_count, 1)
                self.assertFalse((root / "snapshot.sqlite").exists())

    def test_snapshot_deadline_kills_and_reaps_stalled_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            pidfile = root / "child.pid"
            real_run = subprocess.run

            def stall(_args, **kwargs):
                if _args[-1] == "-version":
                    return real_run(_args, **kwargs)
                return real_run(
                    [sys.executable, "-c",
                     "import os,time; from pathlib import Path; Path(" + repr(str(pidfile)) + ").write_text(str(os.getpid())); time.sleep(60)"],
                    **kwargs,
                )

            with patch.object(cloud_io.subprocess, "run", side_effect=stall), \
                    patch.object(cloud_io, "SQLITE_BACKUP_TIMEOUT_SECONDS", 1), \
                    self.assertRaisesRegex(RuntimeError, "SQLite backup timed out"):
                cloud_io._sqlite_backup(root / "unused.sqlite", root / "snapshot.sqlite")
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)
            self.assertFalse((root / "snapshot.sqlite").exists())

    def test_missing_state_keeps_configuration_only_pilot_compatibility(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            public = root / "public.json"
            public.write_text('{}')
            cloud = FakeCloud()
            with patch.object(cloud_io, "DATA_ROOT", root), patch.object(cloud_io, "PUBLIC_CONFIG", public):
                cloud_io.backup(cloud)
            manifest = json.loads(next(payload for (_, name), payload in cloud.objects.items() if name.endswith("manifest.json")))
            self.assertEqual(manifest["databases"], [])


if __name__ == "__main__":
    unittest.main()
