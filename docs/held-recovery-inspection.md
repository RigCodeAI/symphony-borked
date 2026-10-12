# Held local recovery inspection (partial DEV-242)

This increment consumes the `local_owner_capture` produced by the merged
[owner capture](dev-242-capture-barrier.md). It opens the two verified images
only in memory, exposes their saved runs through the existing coordinator, and
holds every continuation. It does not install a database, reconcile providers,
fence another coordinator, or release dispatch. Compilation and 109 focused
tests passed in the isolated Docker runtime; deployed recovery remains pending.

## Trusted input and startup

Record the manifest digest independently when accepting the capture. Obtain its
service revision and policy map from the approved capture record. Computing a
digest from an arbitrary downloaded manifest does not establish provenance.
The manifest describes its service revision as an operator pin; this source
does not turn that pin into proof of the captured executable.

The trusted startup option is an Elixir map, not workflow frontmatter or an agent
tool. Start an isolated `AgentRuntimeSupervisor` with these options:

```elixir
recovery_capture: %{
  root: "/absolute/private/captures",
  id: "accepted-capture-id",
  workspace_roots: ["/absolute/managed/workspaces"],
  manifest_sha256: accepted_manifest_digest,
  service_revision: accepted_source_revision,
  policy: accepted_policy_map
}
```

Use dedicated supervisor, coordinator and task-supervisor names for a local
rehearsal. Omit `workstream_store_path`; a mutable Store and held inspection
cannot be combined. Polling must be disabled. The source uses the same early
startup branch after every supervisor restart.

This is an isolated runtime-supervisor rehearsal. Ordinary full application
startup still starts its separately configured Evidence child, which opens its
index and schedules local flushes. This increment does not install a full-service
or deployment activation path for held recovery. Do not point that normal child
at capture files. The held runtime never opens an Evidence root.

## What is checked and preserved

`BackupCapture.read_images/4` checks the independent digest against the bounded
manifest bytes it actually parses. It then returns the exact bounded database
bytes whose hashes, standalone SQLite headers, integrity and private-file
identity were checked. Inspection uses those bytes, without reopening paths for
SQLite or invoking normal Store initialization/migrations. The existing
filesystem boundary remains preflight plus post-read identity checks, not an
atomic no-follow open against a trusted operator changing paths concurrently.

The Store owner supplies `inspect_recovery_image/1`. It requires the exact current
migration schema, bounded rows/payloads, safe external-term decoding, no runtime
PID/port/reference/function values, correlated SQL and payload identities,
matching attempt/operation rows, no orphan input events, valid Linear records,
and an empty Evidence outbox. Compressed terms are unsupported. Older or newer
schemas reject instead of migrating the capture.

The Evidence owner supplies its own in-memory inspector. It checks its exact
schema, snapshots against the Store's saved redacted projections, task keys,
event/artifact ownership and bounds, JSON key uniqueness, and event watermark.
The manifest's run/operation/delivery inventories and immutable bundle references
must agree. Bundle references do not prove that permanent objects or receipts
are available; those remain a visible blocker.

Saved stage/attempt identities, waits, artifacts, repair/retry counters and
definition/resource texts remain in their original maps. Frozen resource digests
and agent references are checked without reading current YAML or instruction
files. The current service policy is compared with each saved policy. A mismatch
is reported through `policy_compatible: false` and `incompatible_run_ids`; it does
not replace a saved policy, reset a run or synthesize an approval.

This is structural inspection under trusted anchors, not complete qualification
of every evaluator or future child/review contract. Child and review replay must
be integrated with their owners after those contracts merge.

## Held behavior and diagnostics

`Orchestrator.recovery_status/1` reports `status: :dispatch_held`,
`dispatch_allowed: false`, `restore_ready: false`, the anchored revision/digest,
included identities, policy compatibility and static blocker codes. Saved run
reports remain readable through `workstream_state/2`. The regular snapshot also
shows the recovery state.

The held startup path skips normal mutable Store/Evidence recovery, Linear
initialization, worker cancellation/replacement, validation and cleanup
reconciliation, timer setup and stage advancement. Every other GenServer call
returns `recovery_dispatch_held`; all normal runtime messages are ignored while
held, including stale results and future dispatch/replay messages. No task or
provider adapter is started by this mode. There is no release API.

## Reconciliation and fencing required before a later release

Keep the original coordinator stopped and prevent automatic restart; a copied
SQLite image does not fence its original disk or host. A later service-owned
release contract must bind the accepted capture identity to independently
verified old-owner fencing, exact worker ownership and current external state.
Operator input or a missing local PID must not manufacture that proof.

Use each saved operation identity to inspect the protected worker ledger:
machine, boot, unit invocation, cgroup, process start identity and request digest.
Unreachable or mismatched workers stay unknown and reserve ownership. Confirm
termination or write fencing before starting a replacement. Restore cannot infer
post-capture work absent merely because its event IDs are absent locally.

Reconcile Linear deliveries/questions/replies and delegation; existing branch/PR
and candidate SHA; validation App/check/contract; review base/head and decisions;
child linkage/artifact receipts; and merge/cleanup state through their existing
owners. Do not replay successful external actions, clear decision pins or start a
second scheduler. Verify permanent archive objects and stable viewer identities
separately from the SQLite index. Preserve public configuration and protected
authentication reference pins, and qualify the exact executing release.

Only after those proofs are persisted and bound to this exact recovery should a
future owner release one continuation through the existing coordinator. That
release path, fresh remote capture/readback, freshness alerts and disposable GCP
loss/restore rehearsals remain outside this increment. DEV-242 remains In Progress.

## Local qualification

Qualified against merged main `4b33f5d5e99032fe1cd06e6070108faa7008e145`
(PR #22 cleanup/output contracts), using the isolated `dev242-check` container,
its dedicated deps/build volumes and the `dev230-checked` image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
(Elixir 1.19.5/OTP 28).

Warnings-as-errors compilation, `mix build`, public specs, full format and
PR-body checks passed. The final command passed **109 tests, zero failures**:

```sh
mix test test/symphony_elixir/backup_capture_test.exs \
  test/symphony_elixir/backup_capture_owner_test.exs \
  test/symphony_elixir/evidence_test.exs \
  test/symphony_elixir/workstream_store_test.exs \
  test/symphony_elixir/workstream_cleanup_test.exs \
  test/symphony_elixir/validation_check_test.exs \
  test/symphony_elixir/durable_workstream_test.exs \
  test/symphony_elixir/durable_publication_test.exs --seed 405886
```

The final development log is `/private/tmp/dev242-held-regression.log`. It is a
local test diagnostic, not a trusted factory or deployed restore receipt.
The initial fixture used a coordinator name outside Evidence's authorized owner;
it now uses the configured owner. The stale-policy fixture now records its old
policy before first Evidence persistence, preserving immutable identity checks.
Expected startup rejection runs in a test process that traps its linked shutdown.
The original failing seed passed after those fixture repairs.

Tests used real owner captures, both SQLite images and actual OTP held boot/restart.
They checked unchanged saved maps, current-YAML independence, anchors, stale
policy, denied calls/messages/casts and unchanged source/capture/evidence files.
Disposable copies exercised DDL, row identity, outbox, payload/runtime-object,
compressed/deep/oversized term and Evidence JSON/ownership rejection. The concise
self-review checked startup ordering, the read-only byte boundary, callback
admission and policy preservation. Ordinary runtime cleanup/output, validation,
restart and publication regressions passed in the same final set.

The container was stopped and the build slot released after the focused pass.
The default `validation_docker` exclusion remained active. No model turns,
provider mutations, state installation or dispatch release ran. Broad CI follows
the alpha non-blocking policy; strict Credo/full CI are not claimed green.
