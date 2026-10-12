# Durable local workstreams

DEV-232 adds SQLite-backed manual runs to the existing Elixir orchestrator. It extends
DEV-229's loader, pinned agent/skill resources and executable gate. Tracker-mode deployments
keep their existing behavior. This alpha does not wire persistence into Linear polling or
prove remote-worker, GCP, publication, review or merge recovery.

## Engineer test card

From `elixir/`, with its pinned toolchain and dependencies installed:

```bash
mise exec -- mix setup
mise exec -- mix run --no-start ../factory/scripts/recovery-smoke.exs /tmp/factory-recovery-demo
cat /tmp/factory-recovery-demo/waiting.json
cat /tmp/factory-recovery-demo/report.json
cat /tmp/factory-recovery-demo/migration-error.txt
```

Use a new absolute directory; an existing path is rejected. The command copies the trusted
sample definitions into that disposable directory and creates two dedicated Git clones.
Its agent executor is explicitly controlled code, with no model inference or credentials.
Its check stages run real shell commands. The sample agent and shared skill are the same
resources used by DEV-229. DEV-229's real Rig/model qualification remains separate evidence.

Expected: the summary says `passed`, names two run IDs and the database/report paths.
The passing run survives a coordinator kill at the committed agent → check boundary and
again while waiting for a human. Its saved definition, workspace, evidence and counters
stay unchanged. Duplicate queue and answer events reuse the run and finish once.
`checks.log` in the passing workspace has exactly one line. A deliberately failed migration
reports a version-2 error, rolls back its schema change, and preserves the waiting run.
Changing the disposable skill affects the new failing run's digest only; its real check
returns nonzero and leaves it blocked. Reports include stage and operation identities.

Inspect before cleanup. To remove only this demo and its retained state:

```bash
rm -rf /tmp/factory-recovery-demo
```

## Coordinator API

Run without starting the tracker application, after starting `:yaml_elixir`, `:jason` and
`:exqlite`. Start `SymphonyElixir.AgentRuntimeSupervisor` with:

```elixir
[
  workstream_store_path: "/absolute/private/state/runs.sqlite",
  orchestrator_name: MyCoordinator,
  task_supervisor_name: MyTasks,
  max_concurrent_agents: 1
]
```

Durable mode uses one coordinator and a surviving task supervisor. It skips tracker config,
polling and terminal-workspace cleanup. `polling: true` is rejected in this mode. The usual
tracker runtime keeps its existing `one_for_all` restart behavior. The database must be on
local storage that supports SQLite locks, not a network share. The connection holds an
exclusive DELETE-journal lock; a second coordinator fails startup before dispatch. New data
directories/files use 0700/0600 permissions; existing operator permissions are preserved.

Public operations on `SymphonyElixir.Orchestrator`:

- `queue_workstream(server, event_id, task_id, definition_path, inputs, options)` returns
  `{:ok, run_id}`. Options require `workspace` and `workspace_root`; optional `branch`,
  `issue_id` and `codex_command` are strings. The database must be outside the writable worker workspace. The dedicated workspace is checked before
  enqueue and before actual stage execution. The canonical workspace/root and resolved
  Codex command are pinned. Different tasks cannot share a recorded workspace.
- `workstream_state(server, run_id)` returns `{:ok, readable_report}` with definition
  digests, requested settings, observed session metadata, attempts, artifacts, waits,
  operations and counters. Initial task inputs, full definitions and execution command are
  omitted from this report. The database retains the complete pinned run for recovery.
- `answer_workstream(server, event_id, run_id, wait_id, outputs)` delivers exactly the
  human-wait stage's declared string output keys as JSON values. Missing/extra keys,
  null outputs and stale waits reject. Repeated identical events succeed without another
  transition; reusing an event ID with different content rejects.
- `reconcile_workstreams(server)` rechecks uncertain operations. There is no automatic
  assumption that an unreachable or missing process stopped.
- `step_workstreams(server)` starts ready stages once. Use `auto_advance: false` at startup
  to inspect a stable boundary; automatic advancement is otherwise the default.

Caller event IDs use a separate namespace from internal stage events. Event IDs are unique
in that namespace. Task IDs permanently identify one run; use a new task
ID for a new run. Inputs and replies must be plain JSON data and must contain no credentials.
A second event for the same task returns its existing run. Definitions on disk affect new
runs only; migrating an existing run is not supported by this increment.

A human wait has required fields `id`, `type: human_wait`, `inputs`, `outputs`, `prompt` and
`next`. `next` is a stage ID or `complete`. The pending wait pins its resolved input evidence
and artifact identities. Receiving its outputs advances that local stage; it does not grant
execution permissions, model elevation or GitHub merge approval.

## Recovery and storage contract

SQLite version 1 contains `runs`, `events`, `stage_attempts` and `operations`. Each event
transaction updates the complete run and its attempt/operation records atomically. Payloads
use Erlang external terms with safe decoding and pinned service-policy digests. Opaque
agent/check evidence is normalized to JSON data so callback-specific BEAM atoms cannot
prevent recovery in a fresh process. Use the
coordinator report for readable inspection. Stop the coordinator before direct database
inspection or direct external backup. The optional
[local owner capture](dev-242-capture-barrier.md) holds both database owners and
uses their existing connections; its source passed the focused owner/recovery tests.
Never copy a live database without its journal or discard it to resolve a startup error.
Versioned migrations run transactionally; failed/unsupported migrations fail startup and
leave previously committed records recoverable. Keep the database through workspace cleanup.

Before spawning a stage, the coordinator persists its attempt and outgoing-operation ID.
The worker waits for permission until its identity is also committed. A completion receipt
is held in that supervised worker until the coordinator commits the result, gate, artifacts,
counters and next stage together. Replayed receipts cannot create another stage. A receipt
whose transition already committed is acknowledged during recovery without reapplying it.

An uncertain operation stays `reconciling`, reserves execution capacity, and blocks a
replacement writer. This includes a lost action result: termination alone does not show
whether an action already had a side effect. For future trusted integrations, an explicitly
configured `workstream_reconciler` receives the pinned run and operation and may return:

- `{:completed, result}` after observing the original operation's outcome;
- `:terminated` for an agent only, after proving its entire external process tree stopped;
- `:not_applied` after proving the operation ended without applying its side effect;
- any other outcome, which keeps execution blocked.

This callback is service code, never agent-provided evidence. Its result must reconcile the
recorded side-effect identity. Transport retries preserve that identity and increment their
own counters; repair budgets remain separate and never reset on restart. Service-policy digest changes block unfinished runs as `policy_blocked`; an explicit
future migration is required before resumption under different policy code. Default recovery
has no remote liveness adapter and therefore blocks unresolved full-VM or external-process
failures. A crash between spawn and durable worker registration also remains blocked
conservatively; this alpha does not guess whether an unregistered worker is safe to adopt.
Host/process disappearance is not a fencing guarantee.

For stopped runs, trusted termination confirmation cancels the current executing attempt
and operation in the same durable transition and clears stale reconciliation metadata. Restart normalizes
older stopped records with an already committed termination confirmation without repeating
external cancellation or changing their pinned definitions. Unknown cancellation keeps its
capacity reservation and is retried through the trusted cancellation adapter; it cannot
silently restart the stopped task.

## Checks

```bash
mise exec -- mix test test/symphony_elixir/durable_workstream_test.exs \
  test/symphony_elixir/workstream_run_test.exs test/symphony_elixir/workstream_store_test.exs \
  test/symphony_elixir/core_test.exs test/symphony_elixir/orchestrator_status_test.exs
mise exec -- make all
```

The process tests additionally kill a live controlled worker, test uncertain liveness,
reconcile an already-applied action, preserve repair/transport counters, and acknowledge a
committed receipt after a lost acknowledgement. These tests and the controlled smoke prove
the local persistence boundary; they do not qualify a real cloud agent or trusted validator.
