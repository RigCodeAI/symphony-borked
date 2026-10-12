# Offline backup checks (partial DEV-242)

`verify_backup.py` checks a privately staged v1 backup. It does not install
files, migrate state, stop a coordinator, contact providers or replay actions.
Passing checks **always** return `restore_ready: false` and CLI exit **2**.
Rejected inputs return exit **1**. There is no successful restore exit code.

## What can be established now

The merged backup helper uploads a v1 manifest with service revision and database
hashes. `public.json` is separate and has no manifest digest. The caller must
supply a trusted manifest digest, independently approved public-config digest,
expected revision, expected database names/schema, and destination UID:GID.
Do not derive these expected values from the files being checked. A downloaded
manifest plus its own newly computed digest is not independent provenance.

The checker rejects partial/extra inventory, duplicate JSON keys or database
names, destination overrides, traversal, symlinked/hardlinked/non-private staged
files and mismatched hashes. Staging must be an absolute, real directory owned
by the invoking user with private permissions; files must be single-link regular
files owned by that user with no group/other access. No sidecars or extra files
are accepted. Supported state filenames contain only letters, digits, underscores
or hyphens followed by `.sqlite`. `evidence-index.sqlite` is reserved.

Each database is read once (maximum 256 MiB), hashed and inspected using those
same bytes in an in-memory SQLite connection. The inspector requires Python
3.11+ with [SQLite deserialization support](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.deserialize).
It rejects WAL-dependent images and unsupported schema/DDL, including changed
keys, uniqueness, nullability, AUTOINCREMENT, or extra views/triggers. It checks
integrity. Supported schemas are WorkstreamStore 1–3 and evidence index 1, from
the merged migrations. State databases larger than the inspection limit are
unsupported, not silently skipped. No Erlang payload or run is deserialized.

Destination checks only read metadata. The root must already exist, have a
trusted owner (root or invoking user), and forbid group/other writes. State and
optional evidence directories must have the supplied service UID:GID. State
allows deployed mode 0750 or private mode 0700; evidence must be private.
Symlinks and existing destination database files reject. The checker does not
create, chmod, chown or overwrite anything. These checks cannot prove that an
old coordinator is stopped, that a worker is fenced, or that ownership will
remain unchanged later. They are not a new isolation boundary against the
trusted operator changing inputs or paths.

## Prepared operator command

Use a new private staging directory containing exactly `manifest.json`,
`public.json`, the approved state databases and, when required,
`evidence-index.sqlite`. Get anchors/inventory from the trusted capture record
and reviewed workflow, not the staged manifest. If the old capture has no
independent public-config digest or known inventory, stop and obtain those
records; this tool cannot manufacture them. The current helper does not create
that complete capture record automatically.

After the destination directories have been independently provisioned according
to the approved ownership plan, a **read-only** inspection can use:

```bash
python3 -I factory/deploy/verify_backup.py /absolute/private/staged-backup \
  --manifest-sha256 "$capture_manifest_sha" \
  --public-sha256 "$approved_public_sha" \
  --service-revision "$capture_revision" \
  --state-database linear.sqlite --state-schema 3 --evidence-index \
  --destination-root /absolute/private/restored-data \
  --owner-uid "$coordinator_uid" --owner-gid "$coordinator_gid"
```

Use the actual approved state name(s), repeating `--state-database` as needed.
For an explicitly known configuration-only or evidence-only backup, use
`--no-state-databases` instead, omitting `--state-schema`. An empty manifest
`databases` list requires the helper's exact v1 configuration-only note;
evidence-only manifests have a non-empty list and no note. Those forms receive a separate missing-state
blocker; they do not become runtime recovery evidence. Only declared destination
directories are checked, and the report names them.
Linear configuration with no state database rejects as incomplete.
Exit 2 means `offline_checks_passed` **and restoration is blocked**. A schema
check says nothing about whether an opaque run payload decodes, retains its
approved revision, or is compatible with current service policy. Do not use
this report to copy files into a live service or dispatch a replacement worker.
The report omits configuration values, application rows and raw parser errors.

## Required owner barrier/API proposal

Implementation of this section needs coordinator, WorkstreamStore and Evidence
owner release. No API below is implemented by this slice.

Proposed owner interface: `prepare_capture(capture_id, expected_config_revision)`
returns the held inventory, schema/policy pins, delivery cutoff and in-flight
operation IDs. Owner-only `snapshot_for_capture(capture_id, approved_path)`
returns a checksum plus its store/outbox watermark. `finish_capture(capture_id,
verified_manifest_digest)` releases the hold after verified publication;
`abort_capture(capture_id)` records failure and releases it without publishing a
usable capture. Repeated IDs must reuse their original outcome, not take another
snapshot or reapply an external action. Paths come from a fixed operator plan,
never agent input. These interfaces extend the current owners.

1. The single coordinator starts an idempotent capture ID and holds dispatch,
   stage transitions and all producers that can write either store. Persist
   incoming delivery IDs before acknowledgement, with a declared capture cutoff;
   events beyond it need a separately recoverable queue or provider reconciliation.
   Record held/in-flight operations instead of assuming worker termination.
2. Under those owners, drain the workstream evidence outbox and flush the evidence
   store. Record the outbox/event watermarks and run/stage/operation identities.
   Both stores must remain unchanged until their snapshots finish. A table count
   or independent per-file integrity check is not this barrier.
3. Have the existing connection owner produce its SQLite snapshot, or keep both
   owners stopped/quiesced while the existing helper captures them. Preserve the
   exclusive-writer policy. Do not copy live SQLite/journal files or introduce
   another scheduler. Snapshot requests must be idempotent and use fixed private
   operator-owned destinations outside issue workspaces.
4. Extend the capture record to hash every required file: database schemas and
   paths, public settings, effective workflow, nonsecret credential reference
   pins, exact service/release/policy identities, and archive-bundle manifests.
   Bind them to the capture ID/cutoff and both-store watermarks. Verify remote
   readback and publish the completed capture record last. Failure must not
   release a partially verified capture as usable. Keep secret payloads separate.
5. Before restore, establish that the old coordinator cannot return as a writer,
   discover/reconcile worker-ledger identities, and fence or prove termination
   before replacement. Reconcile **post-capture** provider events, PRs, reviews
   and other external operations by their original IDs. A shared DB snapshot
   does not roll back remote side effects or prove they were never applied.
6. Restore behind a dispatch hold with compatible policy; ask the application
   owners to validate opaque payloads, pinned definitions, waits, counters,
   parent/child links and evidence outbox consistency. Verify retained archive
   bundles and stable authenticated URLs. Only then release one exact continuation.

DEV-238 publication and DEV-241 cleanup remain independent owners; DEV-240 adds
review reconciliation when available. This verifier grants none of their actions.
GCP recreation, freshness alerts and the full deployed rehearsal remain pending.

## Local test/demo

```bash
cd factory/deploy/tests
python3 -m unittest test_backup_verifier
```

Fixtures use the real merged backup helper and DDL extracted from the actual
Elixir migrations. They prove offline checks, rejection and unchanged staged
bytes/destinations; opaque fixture payloads are explicitly not real recovered
runs. No cloud, root, model turn or worker access is required. Fixtures clean
themselves up. Keep original backups, archives and live state out of demo cleanup.
