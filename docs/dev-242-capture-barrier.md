# DEV-242: smallest owner capture increment

Status: the PR #18 baseline passed compilation, build, specs, format and 85
focused tests in the isolated Docker runtime. PR #19's merged source is now
integrated; its combined capture/cleanup checks are queued after DEV-239's
correction pass. PR #20's validation timer changes are still unmerged. No live capture or
restore is authorized or qualified. Reviewed against merged main
`e7fd8290ed7e508547867c9a5233d9b9eb0bfd16` (PR #18), and live DEV-239,
DEV-241, DEV-242 and DEV-243 scope. Recheck the seams after their integrations.

The first increment captures the two existing databases at a quiet durable
boundary. It rejects active or uncertain work rather than pause a running
agent, publisher or cleanup operation. This is a local capture primitive; full
deployed recovery remains a later acceptance step.

## What the merged code actually does

| Current seam | Consequence for capture |
| --- | --- |
| `orchestrator.ex:2452`, `commit_workstream/4` | Commits the run and evidence outbox, drains that outbox, and retains validation artifacts. A Store snapshot alone can precede Evidence persistence. |
| `workstream_store.ex:525`, `configure_database/2` | The connection keeps an exclusive lock. An external SQLite backup still cannot read the live store. |
| `workstream_store.ex:295`, `linear_receive/3` handler | Intake is a write and does not check caller ownership. A capture hold must cover it as well as owner-only run/task/outbox writes. |
| `workstream_runner.ex:817`, evidence callback | Workers write conversations directly to Evidence. Holding coordinator transitions alone does not freeze that database. |
| `evidence.ex:348-396` and `:flush` at 492 | Snapshot, event and artifact calls mutate the index; flush creates immutable bundles. These producers need the same hold. |
| `publisher.ex:174`, `publish_locked/3` | Remote push follows durable intent, but can finish after a lost response. Intent and a local pause do not prove absence of a PR. |
| `orchestrator.ex:2671`, stopped publication observer | Observation jobs are read-only remotely but commit results locally. Historical `retry_at` entries are not live jobs. |
| `orchestrator.ex:2744`, cancellation request | Termination runs asynchronously. Only the persisted trusted termination result is proof; an empty worker map is insufficient. |
| `linear/coordinator.ex:487`, `start_job/3` | Linear API jobs run outside the coordinator, including activity publication. Empty worker maps do not cover these jobs. |
| `elixir/lib/symphony_elixir_web/controllers/linear_webhook_controller.ex:35`, `dispatch_event/2` | A coordinator error already returns HTTP 503. Capture can reject intake promptly without falsely acknowledging durable receipt. |

The normal `{:uncertain, reason}` publisher result remains `reconciling` through
`accept_workstream_result/5`. However, a generic error after pushing can finish a
publish attempt as completed and leave its run blocked. The stopped-publication
observer does not reconcile blocked runs. Admission must inspect publication
intent and its verified completion, not infer an absent PR from terminal status.

## Concrete first implementation

`BackupCapture` is a service-owned local action, called through the existing
Orchestrator. No HTTP endpoint, agent tool, second scheduler or new stage type.
Use a fixed, private, operator-provisioned capture root outside all issue
workspaces. Its files remain confidential: Store payloads are not redacted prose.

1. A short coordinator call admits a capture ID and sets a capture hold. Require
   durable manual mode, no polling workers, no workstream workers, no Linear job,
   no cancellation job and no running stopped-publication observation. Inspect
   durable operations as well: reject `executing`/`reconciling` runs, unresolved
   transport retries, and canceled operations lacking trusted termination.
   For every publication intent, require an already verified successful
   publication completion or a conclusive stopped-publication observation;
   otherwise reject, even if the operation says completed or the run is blocked.
   Reject unresolved Linear acknowledgement/activity publications, not just
   live Linear jobs. Unknown states reject. Ready runs and human waits are
   eligible only when their operations prove a quiet boundary; run status alone
   is not the predicate. Workers awaiting receipt acknowledgement perform no
   further stage writes; do not equate every supervisor child with an active writer.
2. Require the evidence outbox to be empty; do not silently acknowledge or discard
   pending entries. Existing normal commits already drain it. Hold Store writes
   and Evidence writes/flush through leases owned by this coordinator. Register
   exactly one capture worker and delegate only the fixed snapshot destinations
   to it. This worker cannot commit runs, acknowledge evidence or publish remotely.
3. Launch capture asynchronously. During the hold, new enqueue/answer/intake,
   worker-registration/question/publication calls return `capture_in_progress`.
   Do not process advance, reconciliation or Linear timers; re-schedule the
   necessary work after release. Successful receipts before the hold are in the
   snapshot; rejected deliveries are outside it and have received no acceptance.
   The new capture action is separate from DEV-239's published-wait reconciliation.
4. Each existing SQLite owner creates a standalone snapshot from its own
   connection using prepared `VACUUM main INTO ?1`, with a bound, fixed output
   path. Do not expose the connection or use an external read connection.
   Keep both leases until both copies, integrity checks, sizes, schemas and
   digests are verified. Preserve the Store's exclusive-writer settings.
5. Record a local completion manifest last, while held. Include capture ID,
   exact service revision, database identities/schema/digests, the included run
   and operation IDs, included Linear delivery IDs, zero pending outbox and the
   Evidence event watermark. Export the current Evidence bundles under its owner
   and list their immutable manifest revisions. This manifest explicitly says
   `restore_ready: false` and is not the existing v1 cloud backup manifest.
6. Release both leases only after the capture worker has finished. Duplicate IDs
   return the original completed result after verifying its bytes; partial IDs
   reject rather than take a later snapshot under the same identity. Process
   death, timeout or disk failure before completion leaves no valid marker. A
   lost result after a valid completion is recovered by verifying that original
   manifest, rather than recapturing. A mismatched token cannot release the hold.
   Cancel a timed-out SQLite operation and wait for its completion before
   releasing the hold. If that completion cannot be established, keep dispatch
   held and report failure; a caller timeout alone must never thaw the owners.
   Coordinator restart discards no files and never treats an orphan as complete.

The locked Exqlite version is 0.42.0. Its published
[connection API](https://raw.githubusercontent.com/elixir-sqlite/exqlite/v0.42.0/lib/exqlite/sqlite3.ex)
supports prepare/bind/step and cancellation; it has no `backup/…` function.
[SQLite documents VACUUM INTO](https://www.sqlite.org/lang_vacuum.html) as a
consistent copy that leaves the source unchanged. Interrupted output can be
incomplete. The actual Exqlite connection path, standalone output, cancellation
and exclusive-lock preservation must be proven in implementation tests.

This increment does not claim a recoverable provider-event cutoff. Recovery
must reconcile later Linear deliveries, GitHub mutations and worker ledgers
before dispatch, even when the local two-database snapshot is consistent.

## Source API and startup pins

The trusted local startup option is `backup_capture`, with `root`,
`service_revision`, `workspace_roots` and `cleanup: :disabled` or `:enabled`; an optional
`evidence_server` selects a service-owned test/server name. There is no workflow
frontmatter field or deployment wiring in this increment. The root must already
exist with private ownership, outside the supplied issue roots. Evidence must
name the actual coordinator in its `capture_owner` startup option (the default
is `SymphonyElixir.Orchestrator`). Limits are 128 runs and 256 MiB per database.
The cleanup mode must match `merged_cleanup`; enabled mode still requires
`cleanup_active_or_uncertain?/1` to report no owned work or uncertain deletion.
Manifest inventories are capped at 10,000 stored IDs per table; exceeding a
bound rejects the capture rather than omitting records.

`Orchestrator.capture_backup(server, id)` starts asynchronous capture and
`Orchestrator.backup_capture_status(server, id)` returns its local result or
visible failure/held state in this coordinator. After restart, starting the same
ID verifies a completed on-disk capture and repopulates status; it does not
recapture that ID. The new module provides `prepare/3`, `snapshot/2`,
`complete/2` and `verify/3`. Store provides `begin_capture/4`,
`capture_inventory/2`, `snapshot_capture/2` and `end_capture/2`; inventory IDs
come from the held database, not only the coordinator's memory. Evidence's `begin_capture/5` also takes
the current run maps so it can check snapshot agreement. Only the coordinator
can lease/release owners; only its delegated worker can snapshot with that token.

The local manifest's kind is `local_owner_capture`, with fixed `state.sqlite`
and `evidence.sqlite` identities. The existing Python offline verifier accepts
the older cloud v1 format only. Local manifests label the service revision as
`operator_pin`, retain database payloads as opaque bytes, and always state
`restore_ready: false`. They do not prove application-level restore compatibility.
SQLite integrity is checked from the same bytes used for the digest, in memory.
Path checks and file-identity comparisons are not an atomic no-follow open.
The manifest separately reports file synchronization and whether directory
synchronization was supported and completed; a false directory flag does not
qualify power-loss durability.

## Released ownership seams

| File | Requested seam | Reserved ownership |
| --- | --- | --- |
| `elixir/lib/symphony_elixir/orchestrator.ex` | Add a capture field to the durable state built by `init_workstreams/2`; add capture start/status/result handling and hold guards for existing mutating callbacks; re-arm existing timers on release. Admission reads `workers`, `cancellations`, `publications`, `linear.job` and durable operation state. | DEV-239 owns adapter config and published-wait validation reconciliation. DEV-241 must supply its active cleanup-job predicate before admission can include cleanup-enabled operation. |
| `elixir/lib/symphony_elixir/workstream_store.ex` | Add an owner lease to `State`; begin/end capture and delegated snapshot calls; hold guards for `commit`, `linear_receive`, `linear_commit`, `linear_reassign`, `linear_task`, `acknowledge_evidence`. Snapshot metadata reads the existing schema and outbox. | No migration, run BLOB changes, child contract changes or writer-policy change. |
| `elixir/lib/symphony_elixir/evidence.ex` | Add a coordinator lease and delegated snapshot/export call; hold guards on snapshot/event/artifact writes and `:flush`; monitor the coordinator and reject stale capture tokens. | DEV-241 owns cleanup archival qualification. Publication and receipt checks retain their existing behavior. |
| New `elixir/lib/symphony_elixir/backup_capture.ex` | Path checks, fixed local file layout, manifest verification/idempotency, snapshot result assembly. | No GitHub/Linear/GCS mutation or credential resolution. |
| New focused capture test plus existing Store/Evidence tests | Real owner connections, concurrent requests, duplicate IDs, owner death and interrupted capture. | No test-only PR or live agent/cloud resources. |
| `SPEC.md`, `docs/durable-workstreams.md`, `factory/deploy/RESTORE.md` | Document the implemented hold and local capture contract in the same change. | Other owners' contract sections stay outside this increment. |

No edits to Config/Schema, Workstream/WorkstreamRun, WorkstreamRunner, Publisher
or Linear.Coordinator. The line references above describe the merged baseline.

The actual shared APIs are `Orchestrator.capture_held?(state)` and
`WorkstreamStore.capture_write_allowed?(store_state)`. The latter must be checked
inside the same Store callback as a new child commit; a separate client read
would race the hold. Admission checks `workstreams.validation_checks`; held
callbacks reject `validation_monitor_owner`, `validation_stage_verifier_owner`
and publication checks (including
the github-validation use); the actual Task result envelopes for
`:validation_stage_verified` and `:validation_observation` acceptance are held.
Admission includes stage read-verifier jobs in `validation_checks`.
`:reconcile_validation_checks` is suspended and re-armed. This leaves DEV-239's
adapter config, policy and gates intact.
Its pending-only wake is `reconcile_validation_checks(state, true)` inside
`advance_workstreams/1`, rather than a new two-element mailbox message. Capture
suppresses the advance message and step call, then re-arms advance on release;
the queued reply stays in the saved run without requiring synchronous GitHub reads.
The latest unmerged PR #20 separates immediate `:reconcile_validation_checks`
from tokenized `{:validation_checks_tick, token}` and owns
`validation_check_timer: {ref, token}`. The capture guard recognizes the tokenized
message. After that PR merges, integrate the actual timer field and reuse its
`schedule_validation_checks_tick/1` on release; do not reintroduce a recurring
timer from the immediate wake. Until then an unknown timer-owner field rejects
capture, so this branch does not claim that timer integration is complete.

DEV-241 merged as `c498b78044cd5392615f4f623955fe1fbca2004e` (PR #19).
Its actual `cleanup_active_or_uncertain?(state)` covers active jobs, surviving
cleanup tasks and uncertain deletion outcomes. The merged job map is recognized,
its hold hook now calls `capture_held?/1`, and capture cancels/re-arms the cleanup
timer. Evidence's new `retained` action exports a bundle, so it is also write-held.
The merge preserved DEV-241's retained-archive checks and cleanup reconciliation.
These integration edits await the queued combined tests. Unknown durable-owner
fields continue to reject rather than silently bypass a new producer.

DEV-239 integration must expose any active or uncertain validation-check job to
admission without changing its adapter configuration or gates. DEV-241 integration
also changes the admission contract. Capture must reject while
merge observation, cancellation archive, deletion or final evidence capture is
active or uncertain. Do not infer quiescence from a completed run or Linear Done.
Its job/state predicate is now present; no cleanup-enabled live capture is
qualified by these source edits. DEV-243 child records will be preserved as Store bytes;
no child semantics are invented here.

DEV-239's queued `pending_validation_reply` is preserved in the run BLOB. A queued
reply is eligible when its durable operations are quiet; an active validation job
is not. Its exact registered github-validation recovery path stays with DEV-239,
and capture does not broaden generic executable-check retry behavior.

## Required verification after ownership release

- Capture a real persisted human wait and a ready boundary through both live
  owners. Reopen staged SQLite copies and prove saved attempts, waits, counters,
  pins, operation identities and evidence agree; prove the live exclusive lock
  remains owned throughout.
- Concurrent Store intake and direct Evidence events/artifacts must reject during
  the hold and work afterward. The webhook must return 503 without a receipt.
- Active/uncertain publication, cancellation, Linear jobs and DEV-241 cleanup
  reject capture. Historical observer retry metadata alone does not reject it.
- Interrupt after either copy and before completion; verify no completed manifest,
  no capture-authorized dispatch and no lost live rows. Duplicate ID never shifts the
  cutoff; stale completion cannot release a newer hold.
- Run focused compilation/tests only after the orchestrator grants a build slot.
  Heavy order remains DEV-239 then DEV-241. Broad CI follows the alpha policy.

The test runtime is the qualified `dev230-checked:latest` image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
(Elixir 1.19.5/OTP 28). Use an isolated DEV-242 container with this worktree at
`/workspace`, workdir `/workspace/elixir`, and separate deps/build volumes. Run
Mix directly there. Stopped DEV-238 caches may seed those volumes read-only;
the running DEV-233/234 containers and their caches remain outside this pass.

### Focused pass on the PR #18 baseline

Executed in `dev242-check`, with its own `dev242-deps` and `dev242-build` volumes;
stopped DEV-238 volumes were mounted read-only solely to seed those caches.
The container is stopped and its heavy slot released after these checks.

- `mix compile --warnings-as-errors`, `mix build`, `mix specs.check` and
  `mix format --check-formatted`: passed.
- `mix test test/symphony_elixir/backup_capture_test.exs
  test/symphony_elixir/backup_capture_owner_test.exs
  test/symphony_elixir/evidence_test.exs
  test/symphony_elixir/workstream_store_test.exs`: **33 tests, zero failures**.
- `mix test test/symphony_elixir/durable_workstream_test.exs
  test/symphony_elixir/durable_publication_test.exs
  test/symphony_elixir/workstream_store_reassignment_test.exs
  test/symphony_elixir/workstream_cancellation_test.exs
  test/symphony_elixir/linear_delegation_test.exs`: **52 tests, zero failures**.
- The initial invalid webhook-delivery fixture returned HTTP 400; replacing it
  with a valid UUID reached the real hold and verified HTTP 503. Its unused helper
  parameter was removed. The full 33-test set passed after that repair.
- `mix credo --strict --format json`: failed with **382 static issues**
  (177 readability, 188 refactor, 17 design), including nine in the new helper.
  Complexity/style warnings remain; this is not an all-CI-green claim.
- No live agents, GitHub/GCP/Linear mutations, cloud backup or restore ran in these
  tests. The default ExUnit `validation_docker` exclusion remained active.

The concise self-review checked direct Store/Evidence writes, stale-token
rejection, lost-worker settlement, publication-intent ambiguity and snapshot
byte/manifest identity. New and existing focused tests cover those observable
paths. Power-loss durability still depends on the explicit directory-sync flag;
an atomic no-follow open and full deployed restore are not qualified here.

## Remaining full-ticket work

Bind public settings, effective workflow/nonsecret references, release/policy
availability and permanent archives into a remotely read-back capture record.
Extend the offline verifier for that explicit format. Add freshness monitoring,
old-coordinator fencing, startup dispatch hold and application/provider
reconciliation. Then rehearse the deployed coordinator/worker-loss scenarios.
The current verifier stays blocked, and DEV-242 stays In Progress.
