# Backup failure handling (partial DEV-242)

This slice fixes the existing backup helper. It does not implement restore,
change coordinator ownership, or qualify a deployed recovery procedure.

`maintenance.sh backup` uses the existing `/usr/bin/sqlite3` CLI's
[`.backup` command](https://sqlite.org/cli.html) to snapshot each discovered
database, including committed WAL data. The child opens the source read-only
with `-nofollow`, so SQLite rejects symbolic links at the actual open, including
a directory/file replaced after preflight. It skips user initialization files
with `-init /dev/null` and receives a fixed backup command with no shell.
The helper requires [SQLite CLI 3.39.3 or newer](https://sqlite.org/releaselog/3_39_3.html), which rejects symlinks in every
path component. Missing tools, older/unknown versions and unsupported options
fail; there is no permissive fallback. Bootstrap already installs `sqlite3`.

Each child has a 30-second timeout; a stalled child is killed and reaped before
the helper reports failure. Integrity is checked before uploading that database.
It never falls back to copying a live SQLite file or its journal. Each database
is a separate snapshot; this does not add a shared snapshot barrier between the
workstream database and evidence index or a deadline for cloud uploads.

The durable WorkstreamStore keeps an exclusive lock while its connection is
open. A separate backup connection cannot snapshot that live store. The helper
now fails with a source/lock error, or `SQLite backup timed out; quiesce the
database owner before retrying` for a stalled child. The normal root CLI error
handler exits nonzero and emits `FACTORY_EVENT backup_failed`, which the existing
monitoring policy consumes. This does not automatically stop the coordinator or
its workers. Releasing that lock requires a coordinated owner stop or a later
snapshot interface through the owner; never delete a journal or weaken its lock.

The helper rejects a symlinked or non-directory `/srv/factory/state` and rejects
symlinked, hardlinked or special `*.sqlite` files. It validates the state inventory
before opening or uploading any database. A missing state directory retains the
existing configuration-only pilot behavior. An error never publishes the final
manifest; already uploaded database objects from an interrupted set are not a
completed backup and should not be selected for restoration.

These checks are not a new isolation boundary against malicious changes by the
trusted coordinator identity. Keep the existing private state ownership and
task-workspace separation intact. The evidence index
uses the same bounded SQLite copy while retaining its existing path/size checks.

## Local verification

From this source checkout:

```bash
cd factory/deploy/tests
python3 -m unittest test_backup_safety test_cloud_io
```

The new tests hold real exclusive SQLite connections while a child invokes the
CLI handler. They require a lock/deadline error, nonzero CLI exit, failure event
and no completed manifest, then release the owner and verify the restored
transaction and integrity. An outer timeout prevents a regression from hanging
the suite. A separate stalled-child test verifies timeout, termination and reap.
Tests also reject unsafe inventory before SQLite opens/uploads and reject
symlink substitution at process launch and reject older/unknown CLI versions.
They preserve the configuration-only
pilot. Existing tests cover committed WAL data and evidence-index safety.
Fixtures and children are cleaned up automatically.

No root, cloud credentials, VM, shared worker or model turn is required. A
successful local test does not prove deployed alerts or full DEV-242 acceptance.
