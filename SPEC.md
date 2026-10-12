# Symphony Service Specification

## Local file-defined workstreams (version 1 extension)

The Elixir implementation provides opt-in local workstreams alongside the tracker-driven
`WORKFLOW.md` contract. See [the schema and commands](docs/local-workstreams.md) and
[durable local recovery](docs/durable-workstreams.md). Definitions contain agent stages,
executable checks and human waits, with declared inputs/outputs, inline exit-status gates,
and repair edges bounded to three additional dispatches per gate. Invalid definitions,
references, data dependencies, transitions and unbounded cycles reject before dispatch.

A repair agent receives the immediately preceding failed check's result and gate detail
in its prompt, including when its executing attempt is already durably recorded. A
passing check or an older failure does not add repair feedback to a later agent turn.

The shipped `software-change` workstream leaves model-produced changes uncommitted.
A pinned service-owned `local-commit` executable check runs as the unprivileged
worker after the agent operation has terminated. It requires a dedicated clone and
local branch, rejects unsafe Git metadata/configuration, disables hooks, signing,
interactive authentication and network protocols, and excludes service credentials.
It commits candidate changes locally and requires a clean checkout. A clean replay
requires the same persisted candidate-input marker; a pristine base cannot qualify.
Its output records the actual candidate SHA, bundle prerequisite SHA, branch and
checksum/size of an incremental Git bundle under protected Git metadata. This is
candidate preparation, not trusted validation or authorization to publish/merge.
Existing runs retain their original stage definitions and commit ownership. See
[local candidate commits](docs/local-candidate-commits.md).

The existing orchestrator can own manually queued durable runs with SQLite storage.
One coordinator holds the database write lock. Enqueue pins resolved workstream, agent,
instruction and skill text/digests, execution context, and service policy digests. Each
stage attempt and outgoing operation is committed before execution; the completion event,
observed gate outcome, artifacts, counters and next stage commit together. Duplicate input
identities cannot create another run or repeat a completion. Conflicting event payloads
are rejected. Human waits retain resolved inputs and artifact identities, release execution
capacity, and accept their declared JSON outputs once for the current wait identity.

Coordinator-only restart preserves local workers. Workers retain their result until its
durable acknowledgement. Recovery redelivers an existing receipt, acknowledges an already
committed receipt, or reserves capacity and blocks on uncertain liveness. A missing BEAM
process is not proof that its external process or side effect ended. Only a trusted adapter
may reconcile an existing outcome or prove termination/non-application before retry.
Transport retries preserve the side-effect identity and do not reset repair counters.
Transactional migrations fail startup clearly and preserve previously committed data.
Loading a run saved before question/reply support adds only missing empty question,
inbox and activity-deduplication state plus a nil continuation. Existing run/attempt/
operation identities, pinned definitions, policy, artifacts and waits are preserved.
This additive normalization does not authorize an incompatible saved service policy.

This is a controlled local alpha. Durable mode does not poll a tracker, publish PRs, merge,
or implement general remote-worker scheduling. The existing tracker-mode workflow, supervision and
in-memory scheduling behavior remain compatible.

### Opt-in native Linear delegation

`linear_delegation` enables authenticated `POST /hooks/linear` intake into the same durable
coordinator. The service verifies HMAC against the original request bytes and requires a
signed timestamp within 60 seconds. Delivery identity and normalized event contents are
persisted before HTTP acknowledgement; conflicting replay contents reject. Only bounded, redacted human prompt bodies are retained for reply delivery; raw webhook payloads and
credentials are not persisted in receipt records.

Native `prompted` messages are associated with the saved session and persisted once by
activity ID, independently of HTTP delivery ID. Questions pin the run, stage attempt and
artifact identities. They publish native elicitations through a persisted outbox; progress,
failure and stop use the same idempotent publication path. Agent-authored activities do not
trigger human replies. Necessary input closes the app-server operation with trusted
termination before releasing capacity. Unknown termination retains the ownership reservation.
Restart preserves waits and pending replies. Continuation reconstructs pinned stage context
in a fresh conversation; pending JSON-RPC IDs from the closed connection are never replayed.
Native user-input requests become clarifications; unsupported permission approvals remain
explicit blockers. Trusted `human_wait` stages can declare `approval: true`, requiring an
explicit approval of the current wait and artifact digest. Ordinary clarification and stale
artifact approval cannot satisfy that gate. See [question/reply contract](docs/linear-questions.md).

Delegation can use coordinator-only OAuth client credentials through `client_secret_env`
instead of the compatible legacy `token_env`. Exactly one is configured. App tokens use
fixed `read,write,app:assignable` scopes, are held in bounded memory per issue run and never
persisted or passed to workers. Expiry or one HTTP 401 triggers renewal; a second 401,
unavailable credentials or failed acquisition blocks the API operation. Restart reacquires
tokens. Signing and client secrets remain distinct stripped coordinator environment names.

The authoritative issue must have the configured DEV team, `DEV-` identifier, explicit Rig
label, active DEV status and installed app delegate. Project membership and human assignment
do not authorize dispatch. The current agent session must belong to the configured app and
issue. The human assignee is preserved. A created session selects the explicit software-change
entry and cloud agent, pins one run per issue, and acknowledges using a persisted activity UUID
before any stage launches. Session and activity ownership is read through GraphQL relations;
only the provider's specific missing-activity response permits creating the saved activity UUID.
Other lookup errors and conflicting session identities block acknowledgement. Unknown routing,
unavailable readiness or unsafe credentials block.
Production dispatch also requires the pinned SSH/systemd worker control path and a current
root-owned containment qualification receipt. Missing or stale qualification blocks dispatch.
Enqueue must match the resolved definition digest used for qualification; a source change in
that interval blocks. Recovery requalifies an orphaned pinned run before linking its task.

Ownership is checked again before each stage, with a short authorization lease. Stop or
undelegation receipts persist a tombstone before cancellation; later queued events cannot
restart that task. Restart replays pending stops before restoring workers and reuses an already
pinned run if its task link was interrupted. External termination requires a trusted adapter;
an unknown outcome remains visible and reserves capacity. Local controlled tests do not prove
that a remote process tree stopped or that a Linear app is installed. See
[configuration and live acceptance limits](docs/linear-delegation.md).

DEV-235 extends native intake with optional `cloud_agents` entries for the two additional
named cloud apps, sharing the backend, worker pool and workstream. Each identity has an
exact organization/app-user/client tuple and a distinct signing secret; a valid signature
for one app cannot authenticate another. Review definitions cannot be configured as apps.
The selected cloud definition is bound before workspace/policy pinning and its revision,
requested settings and shared resource hashes remain on the durable run through resume.

A newer authenticated created event for a different configured cloud agent requests a
recorded reassignment. The coordinator revokes stage authorization, stops the old operation,
and requires trusted quiescence before it atomically updates the run, Linear task and event.
Task/run/workspace/branch, artifacts, waits, questions and repair counters remain intact;
old app/session replies and stops cannot affect the new assignment. Unknown worker liveness
blocks replacement. Unsupported Daybreak blocks visibly without a fallback. Service secret
references from every route are excluded from launches. Existing runs lacking newly configured
credential exclusions block before reuse. An operator may add exclusion names through the trusted
store only while dispatch is stopped and operations are confirmed quiescent, preserving the
pinned service policy, definition, wait and attempts. Runtime policy changes still block recovery;
credential maintenance does not authorize rewriting those pins. Additional app installation and
live acceptance are separate prerequisites.
See the [integration contract and verification](docs/dev-235-agent-mapping.md) and
[exact administrator setup plan](docs/dev-235-app-setup.md).

A held executor can register its external operation identity through the current worker.
The coordinator commits that identity before acknowledging release and preserves it across
restart. The provisional local Linux adapter fences machine, OS boot, process group and
process start identity before bounded signaling. It always reports unknown, since descendants
can escape the group. Explicit reconciliation frees a stopped slot only after trusted
termination proof; it never restarts the stopped task. Confirmed termination marks the
current executing attempt and operation canceled together and removes stale reconciliation metadata.
Restart normalizes older stopped records that already contain committed termination confirmation;
it does not repeat external cancellation or alter their pinned definitions. Unknown termination
continues to reserve capacity until the trusted adapter confirms it.

For a configured remote worker, a separate forced-command control account reaches a root
broker over a peer-UID-checked Unix socket. The broker reserves each operation before launching
a fixed held wrapper in a unique systemd unit. It accepts no client-selected unit properties
or environment. Preparation waits for a live process in the exact contained unit, with its working
directory set to the canonical workspace. Workspace paths cannot contain systemd
substitution characters. Durable acknowledgement precedes release. Machine, boot, unit invocation and
request digest fence every later action. There is one active or unknown operation per worker
UID/host. This contains processes; it does not isolate historical tasks sharing that UID.
Contained operation workspaces use a root-owned, worker-group parent with mode 0750,
separate from the legacy worker-writable workspace tree. An operator provisions each
worker-owned leaf and its matching coordinator path before adding the explicit issue
workspace mapping. The worker cannot replace sibling workspace names. The root-owned
launch gate remains separate; task code never selects unit properties or writable roots.
The worker broker owns a claimed operation's stdin and stdout across coordinator connection
loss. A disconnected stream detaches the client without closing the app-server input or
launching a replacement. Closing only the client's input keeps its output connection open
until output drains; a full peer close releases the attachment. Only one client may attach
to the exact retained identity at a time.
Buffers are bounded with pipe backpressure; terminal output retention is bounded. This does
not resume the coordinator conversation automatically. A recovered run with unknown liveness
continues to reserve capacity until trusted reconciliation or explicit stop.
Stop work runs in supervised jobs after the tombstone commits, keeping intake responsive.
SSH completion alone cannot finish a stage or free capacity. A terminal exact invocation must
have an empty cgroup, proved by readable recursive population zero or by systemd's release of
that cgroup on a qualified systemd version. Missing evidence remains unknown. The existing
systemd 252 worker now has a passing root containment receipt, and the actual coordinator
forced-SSH RPC/stream path passed its live checks. The Elixir `WorkerOperation.qualify/1`
call and a fresh native run passed on the repaired release: the same foreground worker
survived coordinator restart, then native undelegation terminated its exact operation.
Confirmed stopped attempts and reconciliation metadata now match the retained termination
proofs. Conversation resume and worker broker restart remain unqualified. See the
[restart repair acceptance](docs/evidence/dev-233/restart-repair-acceptance.md),
[contained worker operations](docs/contained-worker-operations.md) and its
[retained live evidence](docs/evidence/dev-233/README.md#live-contained-worker-qualification-2026-10-10).

### Opt-in private task evidence

`evidence` config adds a coordinator-owned SQLite evidence index and immutable
archive outbox outside disposable workspaces. Stable task URLs group retained
runs, stage attempts, retries, questions, parent/child links and validation
artifacts. Snapshot events retain original redacted state and pinned definition
content. Pins include the selected agent and stage-to-agent bindings. A
definition/agent change on the same run requires an explicit durable reassignment
matching prior/current pins; silent mutation rejects. Task and
policy identity remain fixed. Old bundles remain immutable.

Observable completed messages/tool events are allowlisted and credentials are
redacted before persistence. Reasoning/raw protocol fields are omitted. The
viewer projects retained agent messages and task input as escaped readable text,
with stage/attempt labels and the latest gate/check status. Full run/policy/event
details remain expandable; this presentation does not change archived bytes. Every
viewer/artifact request requires a verified signed IAP JWT with the configured
audience and domain/email allowlist; unsigned identity headers never authorize
access. A root uploader validates manifests and object bytes after immutable GCS
upload. Only an exact verified receipt for the current bundle can satisfy the
evidence part of cleanup; upload failure cannot qualify. GitHub merge and worker
termination remain separate prerequisites. Disabled config preserves upstream
workflows. Enabled unavailable storage fails visibly.

Workstream schema v3 journals redacted snapshots in the run commit transaction.
The coordinator drains the outbox before dispatch/acknowledgement and on restart;
failed replay blocks advancement. Retained pre-evidence runs receive an explicit
recovered baseline, while conflicting indexed snapshots fail startup. Durable
agent callbacks persist observable events and fail visibly on capture failure.
Managed native Linear activities retain one stable task URL. Authenticated
validation bytes are copied per attempt, excluding producer keys. These shared
hooks are locally tested against merged main; live qualification is pending.
Older schema-v2 releases require a compatible upgrade or stopped pre-migration
backup restore; no automatic database downgrade is provided. See
[private evidence](docs/private-evidence.md) and its
[acceptance record](docs/evidence/dev-237/acceptance.md).

Status: Draft v1 (language-agnostic)

Purpose: Define a service that orchestrates coding agents to get project work done.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `RECOMMENDED`, `MAY`, and
`OPTIONAL` in this document are to be interpreted as described in RFC 2119.

`Implementation-defined` means the behavior is part of the implementation contract, but this
specification does not prescribe one universal policy. Implementations MUST document the selected
behavior.

## 1. Problem Statement

Symphony is a long-running automation service that continuously reads work from a configured issue
tracker, creates an isolated workspace for each issue, and runs a coding agent session for that
issue inside the workspace.

The service solves four operational problems:

- It turns issue execution into a repeatable daemon workflow instead of manual scripts.
- It isolates agent execution in per-issue workspaces so agent commands run only inside per-issue
  workspace directories.
- It keeps the workflow policy in-repo (`WORKFLOW.md`) so teams version the agent prompt and runtime
  settings with their code.
- It provides enough observability to operate and debug multiple concurrent agent runs.

Implementations are expected to document their trust and safety posture explicitly. This
specification does not require a single approval, sandbox, or operator-confirmation policy; some
implementations target trusted environments with a high-trust configuration, while others require
stricter approvals or sandboxing.

Important boundary:

- Symphony is a scheduler/runner and tracker reader.
- Ticket writes (state transitions, comments, PR links) are typically performed by the coding agent
  through provider-native tools executed by Symphony with the configured tracker credential.
- When tracker credentials are supplied through host-side secret references, the coding-agent child
  process does not need a duplicate tracker login or direct access to raw tracker credentials.
- A successful run can end at a workflow-defined handoff state (for example `Human Review`), not
  necessarily `Done`.

## 2. Goals and Non-Goals

### 2.1 Goals

- Poll the issue tracker on a fixed cadence and dispatch work with bounded concurrency.
- Maintain a single authoritative orchestrator state for dispatch, retries, and reconciliation.
- Create deterministic per-issue workspaces and preserve them across runs.
- Stop active runs when issue state changes make them ineligible.
- Recover from transient failures with exponential backoff.
- Load runtime behavior from a repository-owned `WORKFLOW.md` contract.
- Expose operator-visible observability (at minimum structured logs).
- Support tracker/filesystem-driven restart recovery without requiring a persistent database; exact
  in-memory scheduler state is not restored.

### 2.2 Non-Goals

- Rich web UI or multi-tenant control plane.
- Prescribing a specific dashboard or terminal UI implementation.
- General-purpose workflow engine or distributed job scheduler.
- Built-in business logic for how to edit tickets, PRs, or comments. (That logic lives in the
  workflow prompt and agent tooling.)
- Mandating strong sandbox controls beyond what the coding agent and host OS provide.
- Mandating a single default approval, sandbox, or operator-confirmation posture for all
  implementations.

## 3. System Overview

### 3.1 Main Components

1. `Workflow Loader`
   - Reads `WORKFLOW.md`.
   - Parses YAML front matter and prompt body.
   - Returns `{config, prompt_template}`.

2. `Config Layer`
   - Exposes typed getters for workflow config values.
   - Applies defaults and environment variable indirection.
   - Performs validation used by the orchestrator before dispatch.

3. `Issue Tracker Adapter`
   - Fetches candidate issues in active states.
   - Fetches current states for specific issue IDs (reconciliation).
   - Fetches terminal-state issues during startup cleanup.
   - Normalizes tracker payloads into a stable issue model.
   - MAY expose provider-native agent tools without adding provider-specific write APIs to the
     orchestrator.

4. `Orchestrator`
   - Owns the poll tick.
   - Owns the in-memory runtime state.
   - Decides which issues to dispatch, retry, stop, or release.
   - Tracks session metrics and retry queue state.

5. `Workspace Manager`
   - Maps issue identifiers to workspace paths.
   - Ensures per-issue workspace directories exist.
   - Runs workspace lifecycle hooks.
   - Cleans workspaces for terminal issues.

6. `Agent Runner`
   - Creates workspace.
   - Builds prompt from issue + workflow template.
   - Launches the coding agent app-server client.
   - Streams agent updates back to the orchestrator.

7. `Status Surface` (OPTIONAL)
   - Presents human-readable runtime status (for example terminal output, dashboard, or other
     operator-facing view).

8. `Logging`
   - Emits structured runtime logs to one or more configured sinks.

### 3.2 Abstraction Levels

Symphony is easiest to port when kept in these layers:

1. `Policy Layer` (repo-defined)
   - `WORKFLOW.md` prompt body.
   - Team-specific rules for ticket handling, validation, and handoff.

2. `Configuration Layer` (typed getters)
   - Parses front matter into typed runtime settings.
   - Handles defaults, environment tokens, and path normalization.

3. `Coordination Layer` (orchestrator)
   - Polling loop, issue eligibility, concurrency, retries, reconciliation.

4. `Execution Layer` (workspace + agent subprocess)
   - Filesystem lifecycle, workspace preparation, coding-agent protocol.

5. `Integration Layer` (selected tracker adapter)
   - API calls and normalization for tracker data.
   - Provider-native agent tools and centralized tracker authentication.

6. `Observability Layer` (logs + OPTIONAL status surface)
   - Operator visibility into orchestrator and agent behavior.

### 3.3 External Dependencies

- One configured issue tracker API.
- Local filesystem for workspaces and logs.
- OPTIONAL workspace population tooling (for example Git CLI, if used).
- Coding-agent executable that supports the targeted Codex app-server mode.
- Host environment authentication for the issue tracker and coding agent. Host-side tracker secret
  environment variables SHOULD NOT be inherited by the coding-agent child process.

## 4. Core Domain Model

### 4.1 Entities

#### 4.1.1 Issue

Normalized schedulable work item used by orchestration, prompt rendering, and observability output.
The name `Issue` is generic in this specification; an adapter MAY map it from a ticket, card,
project item, or another provider-native work object.

Fields:

- `id` (string)
  - REQUIRED stable dispatch identity within the configured tracker scope.
  - Opaque to the orchestrator. It MAY be a project-item or board-entry ID instead of the
    provider's underlying ticket ID.
- `native_ref` (object or null)
  - OPTIONAL non-secret provider identifiers needed by provider-native tools.
  - Opaque to the orchestrator and preserved for prompt/tool context.
- `identifier` (string)
  - REQUIRED human-readable ticket key (example: `ABC-123`).
  - MUST be unique within the configured tracker scope because it names workspaces and
    operator-facing routes. An adapter spanning multiple namespaces MUST disambiguate it.
- `title` (string)
- `description` (string or null)
- `priority` (integer or null)
  - Lower numbers are higher priority in dispatch sorting.
- `state` (string)
  - REQUIRED current provider-native state name.
- `branch_name` (string or null)
  - Tracker-provided branch metadata if available.
- `url` (string or null)
- `assignee_id` (string or null)
- `labels` (list of strings)
  - Normalized to lowercase.
- `blocked_by` (list of blocker refs)
  - Best-effort provider metadata. Each blocker ref contains:
    - `id` (string or null)
    - `identifier` (string or null)
    - `state` (string or null)
- `dispatchable` (boolean)
  - REQUIRED adapter-derived eligibility for provider-specific rules that the generic scheduler
    cannot infer safely, such as assignment, board membership, or blocker semantics.
  - The orchestrator still applies configured state, label, claim, retry, and concurrency rules.
- `created_at` (timestamp or null)
- `updated_at` (timestamp or null)

#### 4.1.2 Workflow Definition

Parsed `WORKFLOW.md` payload:

- `config` (map)
  - YAML front matter root object.
- `prompt_template` (string)
  - Markdown body after front matter, trimmed.

#### 4.1.3 Service Config (Typed View)

Typed runtime values derived from `WorkflowDefinition.config` plus environment resolution.

Examples:

- poll interval
- workspace root
- active and terminal issue states
- concurrency limits
- coding-agent executable/args/timeouts
- workspace hooks

#### 4.1.4 Workspace

Filesystem workspace assigned to one issue identifier.

Fields (logical):

- `path` (absolute workspace path)
- `workspace_key` (collision-resistant sanitized issue identifier)
- `created_now` (boolean, used to gate `after_create` hook)

#### 4.1.5 Run Attempt

One execution attempt for one issue.

Fields (logical):

- `issue_id`
- `issue_identifier`
- `attempt` (integer or null, `null` for first run, `>=1` for retries/continuation)
- `workspace_path`
- `started_at`
- `status`
- `error` (OPTIONAL)

#### 4.1.6 Live Session (Agent Session Metadata)

State tracked while a coding-agent subprocess is running.

Fields:

- `session_id` (string, `<thread_id>-<turn_id>`)
- `thread_id` (string)
- `turn_id` (string)
- `codex_app_server_pid` (string or null)
- `last_codex_event` (string/enum or null)
- `last_codex_timestamp` (timestamp or null)
- `last_codex_message` (summarized payload)
- `codex_input_tokens` (integer)
- `codex_output_tokens` (integer)
- `codex_total_tokens` (integer)
- `last_reported_input_tokens` (integer)
- `last_reported_output_tokens` (integer)
- `last_reported_total_tokens` (integer)
- `turn_count` (integer)
  - Number of coding-agent turns started within the current worker lifetime.

#### 4.1.7 Retry Entry

Scheduled retry state for an issue.

Fields:

- `issue_id`
- `identifier` (best-effort human ID for status surfaces/logs)
- `attempt` (integer, 1-based for retry queue)
- `due_at_ms` (monotonic clock timestamp)
- `timer_handle` (runtime-specific timer reference)
- `error` (string or null)

#### 4.1.8 Orchestrator Runtime State

Single authoritative in-memory state owned by the orchestrator.

Fields:

- `poll_interval_ms` (current effective poll interval)
- `max_concurrent_agents` (current effective global concurrency limit)
- `running` (map `issue_id -> running entry`)
- `claimed` (set of issue IDs reserved/running/retrying)
- `retry_attempts` (map `issue_id -> RetryEntry`)
- `completed` (set of issue IDs; bookkeeping only, not dispatch gating)
- `codex_totals` (aggregate tokens + runtime seconds)
- `codex_rate_limits` (latest rate-limit snapshot from agent events)

### 4.2 Stable Identifiers and Normalization Rules

- `Issue ID`
  - Use for tracker refresh calls and internal map keys.
  - Treat it as an opaque dispatch identity; do not assume it is the provider's underlying ticket
    ID.
- `Native Ref`
  - Preserve as opaque non-secret data for provider-native agent tools and prompt rendering.
  - Never use it as an orchestrator map key or interpret provider-specific fields in core logic.
- `Issue Identifier`
  - Use for human-readable logs and workspace naming.
  - Require uniqueness within the configured tracker scope.
- `Workspace Key`
  - Derive from `issue.identifier` by replacing any character not in `[A-Za-z0-9._-]` with `_`.
  - If sanitization changes the identifier, append a stable hash suffix of the original identifier
    with at least 64 bits of entropy using only allowed workspace-key characters, making keys for
    distinct identifiers that sanitize to the same text collision-resistant.
  - Use the resulting value for the workspace directory name.
- `Normalized Issue State`
  - Compare states after trimming surrounding whitespace and applying `lowercase`.
- `Session ID`
  - Compose from coding-agent `thread_id` and `turn_id` as `<thread_id>-<turn_id>`.

## 5. Workflow Specification (Repository Contract)

### 5.1 File Discovery and Path Resolution

Workflow file path precedence:

1. Explicit application/runtime setting (set by CLI startup path).
2. Default: `WORKFLOW.md` in the current process working directory.

Loader behavior:

- If the file cannot be read, return `missing_workflow_file` error.
- The workflow file is expected to be repository-owned and version-controlled.

### 5.2 File Format

`WORKFLOW.md` is a Markdown file with OPTIONAL YAML front matter.

Design note:

- `WORKFLOW.md` SHOULD be self-contained enough to describe and run different workflows (prompt,
  runtime settings, hooks, and tracker selection/config) without requiring out-of-band
  service-specific configuration.

Parsing rules:

- If file starts with `---`, parse lines until the next `---` as YAML front matter.
- Remaining lines become the prompt body.
- If front matter is absent, treat the entire file as prompt body and use an empty config map.
- YAML front matter MUST decode to a map/object; non-map YAML is an error.
- Prompt body is trimmed before use.

Returned workflow object:

- `config`: front matter root object (not nested under a `config` key).
- `prompt_template`: trimmed Markdown body.

### 5.3 Front Matter Schema

Top-level keys:

- `tracker`
- `polling`
- `workspace`
- `hooks`
- `agent`
- `codex`

Unknown keys SHOULD be ignored for forward compatibility.

Note:

- The workflow front matter is extensible. Extensions MAY define additional top-level keys without
  changing the core schema above.
- Extensions SHOULD document their field schema, defaults, validation rules, and whether changes
  apply dynamically or require restart.

#### 5.3.1 `tracker` (object)

Fields:

- `kind` (string)
  - REQUIRED for dispatch.
  - Selects one implementation-supported tracker adapter.
- `provider` (object)
  - Default: `{}`.
  - Adapter-owned configuration such as endpoint, scope/project selector, and credentials.
  - Core Symphony MUST preserve unknown keys and MUST NOT prescribe one cross-provider credential
    or scope schema.
  - Each adapter MUST document its required keys, defaults, secret keys, `$VAR_NAME` support, and
    validation errors.
  - If a documented secret `$VAR_NAME` resolves to an empty string, treat that secret as missing.
- `required_labels` (list of strings)
  - Default: `[]`.
  - An issue MUST contain every configured label to dispatch or continue.
  - Matching ignores case and surrounding whitespace.
  - A blank configured label matches no issue.
- `active_states` (list of strings)
  - REQUIRED unless the selected adapter profile documents a default.
  - Values are provider-native state names compared case-insensitively by the scheduler.
- `terminal_states` (list of strings)
  - REQUIRED unless the selected adapter profile documents a default.
  - Values are provider-native state names compared case-insensitively by the scheduler.

#### 5.3.2 `polling` (object)

Fields:

- `interval_ms` (integer)
  - Default: `30000`
  - Changes SHOULD be re-applied at runtime and affect future tick scheduling without restart.

#### 5.3.3 `workspace` (object)

Fields:

- `root` (path string or `$VAR`)
  - Default: `<system-temp>/symphony_workspaces`
  - `~` is expanded.
  - Relative paths are resolved relative to the directory containing `WORKFLOW.md`.
  - The effective workspace root is normalized to an absolute path before use.

#### 5.3.4 `hooks` (object)

Fields:

- `after_create` (multiline shell script string, OPTIONAL)
  - Runs only when a workspace directory is newly created.
  - Failure aborts workspace creation.
- `before_run` (multiline shell script string, OPTIONAL)
  - Runs before each agent attempt after workspace preparation and before launching the coding
    agent.
  - Failure aborts the current attempt.
- `after_run` (multiline shell script string, OPTIONAL)
  - Runs after each agent attempt (success, failure, timeout, or cancellation) once the workspace
    exists.
  - Failure is logged but ignored.
- `before_remove` (multiline shell script string, OPTIONAL)
  - Runs before workspace deletion if the directory exists.
  - Failure is logged but ignored; cleanup still proceeds.
- `timeout_ms` (integer, OPTIONAL)
  - Default: `60000`
  - Applies to all workspace hooks.
  - Invalid values fail configuration validation.
  - Changes SHOULD be re-applied at runtime for future hook executions.

#### 5.3.5 `agent` (object)

Fields:

- `max_concurrent_agents` (integer)
  - Default: `10`
  - Changes SHOULD be re-applied at runtime and affect subsequent dispatch decisions.
- `max_turns` (positive integer)
  - Default: `20`
  - Limits the number of coding-agent turns within one worker session.
  - Invalid values fail configuration validation.
- `max_retry_backoff_ms` (integer)
  - Default: `300000` (5 minutes)
  - Changes SHOULD be re-applied at runtime and affect future retry scheduling.
- `max_concurrent_agents_by_state` (map `state_name -> positive integer`)
  - Default: empty map.
  - State keys are normalized (`trim + lowercase`) for lookup.
  - Invalid entries (non-positive or non-numeric) are ignored.

#### 5.3.6 `codex` (object)

Fields:

For Codex-owned config values such as `approval_policy`, `thread_sandbox`, and
`turn_sandbox_policy`, supported values are defined by the targeted Codex app-server version.
Implementors SHOULD treat them as pass-through Codex config values rather than relying on a
hand-maintained enum in this spec. To inspect the installed Codex schema, run
`codex app-server generate-json-schema --out <dir>` and inspect the relevant definitions referenced
by `v2/ThreadStartParams.json` and `v2/TurnStartParams.json`. Implementations MAY validate these
fields locally if they want stricter startup checks.

- `command` (string shell command)
  - Default: `codex app-server`
  - The runtime launches this command via `bash -lc` in the workspace directory.
  - The launched process MUST speak a compatible app-server protocol over stdio.
- `approval_policy` (Codex `AskForApproval` value)
  - Default: implementation-defined.
- `thread_sandbox` (Codex `SandboxMode` value)
  - Default: implementation-defined.
- `turn_sandbox_policy` (Codex `SandboxPolicy` value)
  - Default: implementation-defined.
- `turn_timeout_ms` (integer)
  - Default: `3600000` (1 hour)
- `read_timeout_ms` (integer)
  - Default: `5000`
- `stall_timeout_ms` (integer)
  - Default: `300000` (5 minutes)
  - If `<= 0`, stall detection is disabled.

### 5.4 Prompt Template Contract

The Markdown body of `WORKFLOW.md` is the per-issue prompt template.

Rendering requirements:

- Use a strict template engine (Liquid-compatible semantics are sufficient).
- Unknown variables MUST fail rendering.
- Unknown filters MUST fail rendering.

Template input variables:

- `issue` (object)
  - Includes all normalized issue fields, including labels and blockers.
- `attempt` (integer or null)
  - `null`/absent on first attempt.
  - Integer on retry or continuation run.

Fallback prompt behavior:

- If the workflow prompt body is empty, the runtime MAY use a minimal default prompt
  (`You are working on an issue from the configured tracker.`).
- Workflow file read/parse failures are configuration/validation errors and SHOULD NOT silently fall
  back to a prompt.

### 5.5 Workflow Validation and Error Surface

Error classes:

- `missing_workflow_file`
- `workflow_parse_error`
- `workflow_front_matter_not_a_map`
- `template_parse_error` (during prompt rendering)
- `template_render_error` (unknown variable/filter, invalid interpolation)

Dispatch gating behavior:

- Workflow file read/YAML errors block new dispatches until fixed.
- Template errors fail only the affected run attempt.

## 6. Configuration Specification

### 6.1 Configuration Resolution Pipeline

Configuration is resolved in this order:

1. Select the workflow file path (explicit runtime setting, otherwise cwd default).
2. Parse YAML front matter into a raw config map.
3. Apply built-in defaults for missing OPTIONAL fields.
4. Resolve `$VAR_NAME` indirection for config values that explicitly contain `$VAR_NAME`, plus any
   adapter-owned fallback environment names documented for omitted provider fields.
5. Coerce and validate typed values.

Environment variables do not globally override YAML values. They are used only when a config value
explicitly references them, or when an adapter profile documents a host-side fallback for an
omitted provider field. Such a fallback is adapter-local, not a cross-provider convention.

Value coercion semantics:

- Path/command fields support:
  - `~` home expansion
  - `$VAR` expansion for env-backed path values
  - Apply expansion only to values intended to be local filesystem paths; do not rewrite URIs or
    arbitrary shell command strings.
- Relative `workspace.root` values resolve relative to the directory containing the selected
  `WORKFLOW.md`.

### 6.2 Dynamic Reload Semantics

Dynamic reload is REQUIRED:

- The software MUST detect `WORKFLOW.md` changes.
- On change, it MUST re-read and re-apply workflow config and prompt template without restart.
- The software MUST attempt to adjust live behavior to the new config (for example polling
  cadence, concurrency limits, active/terminal states, codex settings, workspace paths/hooks, and
  prompt content for future runs).
- Reloaded config applies to future dispatch, retry scheduling, reconciliation decisions, hook
  execution, and agent launches.
- Implementations are not REQUIRED to restart in-flight agent sessions automatically when config
  changes.
- Extensions that manage their own listeners/resources (for example an HTTP server port change) MAY
  require restart unless the implementation explicitly supports live rebind.
- Implementations SHOULD also re-validate/reload defensively during runtime operations (for example
  before dispatch) in case filesystem watch events are missed.
- Invalid reloads MUST NOT crash the service; keep operating with the last known good effective
  configuration and emit an operator-visible error.

### 6.3 Dispatch Preflight Validation

This validation is a scheduler preflight run before attempting to dispatch new work. It validates
the workflow/config needed to poll and launch workers, not a full audit of all possible workflow
behavior.

Startup validation:

- Validate configuration before starting the scheduling loop.
- If startup validation fails, fail startup and emit an operator-visible error.

Per-tick dispatch validation:

- Re-validate before each dispatch cycle.
- If validation fails, skip dispatch for that tick, keep reconciliation active, and emit an
  operator-visible error.

Validation checks:

- Workflow file can be loaded and parsed.
- `tracker.kind` is present and supported.
- The selected adapter accepts `tracker.provider` after documented defaults and `$VAR`
  resolution.
- `codex.command` is present and non-empty.

### 6.4 Core Config Fields Summary (Cheat Sheet)

This section is intentionally redundant so a coding agent can implement the config layer quickly.
Extension fields are documented in the extension section that defines them. Core conformance does
not require recognizing or validating extension fields unless that extension is implemented.

- `tracker.kind`: string, REQUIRED, selects one supported adapter
- `tracker.provider`: object, default `{}`, adapter-owned endpoint/scope/auth settings
- `tracker.required_labels`: list of strings, default `[]`
- `tracker.active_states`: list of provider-native state names, adapter-defined default
- `tracker.terminal_states`: list of provider-native state names, adapter-defined default
- `polling.interval_ms`: integer, default `30000`
- `workspace.root`: path resolved to absolute, default `<system-temp>/symphony_workspaces`
- `hooks.after_create`: shell script or null
- `hooks.before_run`: shell script or null
- `hooks.after_run`: shell script or null
- `hooks.before_remove`: shell script or null
- `hooks.timeout_ms`: integer, default `60000`
- `agent.max_concurrent_agents`: integer, default `10`
- `agent.max_turns`: integer, default `20`
- `agent.max_retry_backoff_ms`: integer, default `300000` (5m)
- `agent.max_concurrent_agents_by_state`: map of positive integers, default `{}`
- `codex.command`: shell command string, default `codex app-server`
- `codex.approval_policy`: Codex `AskForApproval` value, default implementation-defined
- `codex.thread_sandbox`: Codex `SandboxMode` value, default implementation-defined
- `codex.turn_sandbox_policy`: Codex `SandboxPolicy` value, default implementation-defined
- `codex.turn_timeout_ms`: integer, default `3600000`
- `codex.read_timeout_ms`: integer, default `5000`
- `codex.stall_timeout_ms`: integer, default `300000`

## 7. Orchestration State Machine

The orchestrator is the only component that mutates scheduling state. All worker outcomes are
reported back to it and converted into explicit state transitions.

### 7.1 Issue Orchestration States

This is not the same as tracker states (`Todo`, `In Progress`, etc.). This is the service's internal
claim state.

1. `Unclaimed`
   - Issue is not running and has no retry scheduled.

2. `Claimed`
   - Orchestrator has reserved the issue to prevent duplicate dispatch.
   - In practice, claimed issues are either `Running` or `RetryQueued`.

3. `Running`
   - Worker task exists and the issue is tracked in `running` map.

4. `RetryQueued`
   - Worker is not running, but a retry timer exists in `retry_attempts`.

5. `Released`
   - Claim removed because issue is terminal, non-active, missing, or retry path completed without
     re-dispatch.

Important nuance:

- A successful worker exit does not mean the issue is done forever.
- The worker MAY continue through multiple back-to-back coding-agent turns before it exits.
- After each normal turn completion, the worker re-checks the tracker issue state.
- If the issue is still in an active state, the worker SHOULD start another turn on the same live
  coding-agent thread in the same workspace, up to `agent.max_turns`.
- The first turn SHOULD use the full rendered task prompt.
- Continuation turns SHOULD send only continuation guidance to the existing thread, not resend the
  original task prompt that is already present in thread history.
- Once the worker exits normally, the orchestrator still schedules a short continuation retry
  (about 1 second) so it can re-check whether the issue remains active and needs another worker
  session.

### 7.2 Run Attempt Lifecycle

A run attempt transitions through these phases:

1. `PreparingWorkspace`
2. `BuildingPrompt`
3. `LaunchingAgentProcess`
4. `InitializingSession`
5. `StreamingTurn`
6. `Finishing`
7. `Succeeded`
8. `Failed`
9. `TimedOut`
10. `Stalled`
11. `CanceledByReconciliation`

Distinct terminal reasons are important because retry logic and logs differ.

### 7.3 Transition Triggers

- `Poll Tick`
  - Reconcile active runs.
  - Validate config.
  - Fetch candidate issues.
  - Dispatch until slots are exhausted.

- `Worker Exit (normal)`
  - Remove running entry.
  - Update aggregate runtime totals.
  - Schedule continuation retry (attempt `1`) after the worker exhausts or finishes its in-process
    turn loop.

- `Worker Exit (abnormal)`
  - Remove running entry.
  - Update aggregate runtime totals.
  - Schedule exponential-backoff retry.

- `Codex Update Event`
  - Update live session fields, token counters, and rate limits.

- `Retry Timer Fired`
  - Re-fetch active candidates and attempt re-dispatch, or release claim if no longer eligible.

- `Reconciliation State Refresh`
  - Stop runs whose issue states are terminal or no longer active.

- `Stall Timeout`
  - Kill worker and schedule retry.

### 7.4 Idempotency and Recovery Rules

- The orchestrator serializes state mutations through one authority to avoid duplicate dispatch.
- `claimed` and `running` checks are REQUIRED before launching any worker.
- Reconciliation runs before dispatch on every tick.
- Restart recovery is tracker-driven and filesystem-driven (without a durable orchestrator DB).
- Startup terminal cleanup removes stale workspaces for issues already in terminal states.

## 8. Polling, Scheduling, and Reconciliation

### 8.1 Poll Loop

At startup, the service validates config, performs startup cleanup, schedules an immediate tick, and
then repeats every `polling.interval_ms`.

The effective poll interval SHOULD be updated when workflow config changes are re-applied.

Tick sequence:

1. Reconcile running issues.
2. Run dispatch preflight validation.
3. Fetch candidate issues from tracker using active states.
4. Sort issues by dispatch priority.
5. Dispatch eligible issues while slots remain.
6. Notify observability/status consumers of state changes.

If per-tick validation fails, dispatch is skipped for that tick, but reconciliation still happens
first.

### 8.2 Candidate Selection Rules

An issue is dispatch-eligible only if all are true:

- It has `id`, `identifier`, `title`, and `state`.
- Its state is in `active_states` and not in `terminal_states`.
- Its adapter-provided `dispatchable` value is `true`.
- It contains every label in `tracker.required_labels`.
- It is not already in `running`.
- It is not already in `claimed`.
- Global concurrency slots are available.
- Per-state concurrency slots are available.

For refresh and continuation checks, `issue_routable(issue)` means only that adapter-provided
`dispatchable` is true and all `tracker.required_labels` match. State, claims, and concurrency are
checked separately by the surrounding algorithm.

Sorting order (stable intent):

1. `priority` ascending for values `1..4`; all other integers and null sort after that bucket
2. `created_at` oldest first; null sorts last
3. `identifier` lexicographic tie-breaker

### 8.3 Concurrency Control

Global limit:

- `available_slots = max(max_concurrent_agents - running_count, 0)`

Per-state limit:

- `max_concurrent_agents_by_state[state]` if present (state key normalized)
- otherwise fallback to global limit

The runtime counts issues by their current tracked state in the `running` map.

### 8.4 Retry and Backoff

Retry entry creation:

- Cancel any existing retry timer for the same issue.
- Store `attempt`, `identifier`, `error`, `due_at_ms`, and new timer handle.

Backoff formula:

- Normal continuation retries after a clean worker exit use a short fixed delay of `1000` ms.
- Failure-driven retries use `delay = min(10000 * 2^(attempt - 1), agent.max_retry_backoff_ms)`.
- Power is capped by the configured max retry backoff (default `300000` / 5m).

Retry handling behavior:

1. Refresh the specific issue with `fetch_issues_by_ids([issue_id])`.
2. If not found, release claim.
3. If found in a terminal state, clean its workspace and release claim.
4. If found and still active and routable:
   - Dispatch if slots are available.
   - Otherwise requeue with error `no available orchestrator slots`.
5. If found but no longer active or routable, release claim without dispatch.

Note:

- Terminal-state workspace cleanup is handled by startup cleanup, active-run reconciliation, and
  retry refreshes that observe a terminal transition.
- ID refresh avoids treating a terminal, non-active, or newly unroutable issue as merely absent.

### 8.5 Active Run Reconciliation

Reconciliation runs every tick and has two parts.

Part A: Stall detection

- For each running issue, compute `elapsed_ms` since:
  - `last_codex_timestamp` if any event has been seen, else
  - `started_at`
- If `elapsed_ms > codex.stall_timeout_ms`, terminate the worker and queue a retry.
- If `stall_timeout_ms <= 0`, skip stall detection entirely.

Part B: Tracker state refresh

- Fetch current issue states for all running issue IDs.
- For each running issue:
  - If tracker state is terminal: terminate worker and clean workspace.
  - If tracker state is still active and routable: update the in-memory issue snapshot.
  - If tracker state is active but no longer routable: terminate worker without workspace cleanup.
  - If tracker state is neither active nor terminal: terminate worker without workspace cleanup.
- If state refresh fails, keep workers running and try again on the next tick.

### 8.6 Startup Terminal Workspace Cleanup

When the service starts:

1. Query tracker for issues in terminal states.
2. For each returned issue identifier, remove the corresponding workspace directory.
3. If the terminal-issues fetch fails, log a warning and continue startup.

This prevents stale terminal workspaces from accumulating after restarts.

## 9. Workspace Management and Safety

### 9.1 Workspace Layout

Workspace root:

- `workspace.root` (normalized absolute path)

Per-issue workspace path:

- `<workspace.root>/<workspace_key>`

Workspace persistence:

- Workspaces are reused across runs for the same issue.
- Successful runs do not auto-delete workspaces.

### 9.2 Workspace Creation and Reuse

Input: `issue.identifier`

Algorithm summary:

1. Derive `workspace_key` using Section 4.2, including the stable original-identifier hash when
   sanitization changes the identifier.
2. Compute workspace path under workspace root.
3. Ensure the workspace path exists as a directory.
4. Mark `created_now=true` only if the directory was created during this call; otherwise
   `created_now=false`.
5. If `created_now=true`, run `after_create` hook if configured.

Notes:

- This section does not assume any specific repository/VCS workflow.
- Workspace preparation beyond directory creation (for example dependency bootstrap, checkout/sync,
  code generation) is implementation-defined and is typically handled via hooks.

### 9.3 OPTIONAL Workspace Population (Implementation-Defined)

The spec does not require any built-in VCS or repository bootstrap behavior.

Implementations MAY populate or synchronize the workspace using implementation-defined logic and/or
hooks (for example `after_create` and/or `before_run`).

Failure handling:

- Workspace population/synchronization failures return an error for the current attempt.
- If failure happens while creating a brand-new workspace, implementations MAY remove the partially
  prepared directory.
- Reused workspaces SHOULD NOT be destructively reset on population failure unless that policy is
  explicitly chosen and documented.

### 9.4 Workspace Hooks

Supported hooks:

- `hooks.after_create`
- `hooks.before_run`
- `hooks.after_run`
- `hooks.before_remove`

Execution contract:

- Execute in a local shell context appropriate to the host OS, with the workspace directory as
  `cwd`.
- On POSIX systems, `sh -lc <script>` (or a stricter equivalent such as `bash -lc <script>`) is a
  conforming default.
- Hook timeout uses `hooks.timeout_ms`; default: `60000 ms`.
- Log hook start, failures, and timeouts.

Failure semantics:

- `after_create` failure or timeout is fatal to workspace creation.
- `before_run` failure or timeout is fatal to the current run attempt.
- `after_run` failure or timeout is logged and ignored.
- `before_remove` failure or timeout is logged and ignored.

### 9.5 Safety Invariants

Workspace removal uses the current configured workspace root as its authorization
boundary, including for previously recorded paths. After a root change, old-root
work is retained with an error until its recovery/cleanup is explicitly resolved.
Removal must reject the root itself, outside paths and dangling links before
running hooks. A genuinely absent direct path may return a successful no-op without
running a hook or issuing a deletion; absence never authorizes recorded outside paths.

The Elixir local and SSH removal paths require Python 3.11+ with Unix descriptor-relative
operations and symlink-resistant `shutil.rmtree`. Missing or unsupported runtimes fail
closed with an actionable error; there is no pathname-based deletion fallback. The
guard opens ancestors without following links, checks root/ancestor/target identities
after the hook, and deletes relative to retained directory descriptors. Hook failure
and timeout remain logged and ignored, but path changes prevent deletion. Helper
time and output are bounded, and hook output is not copied into removal logs.
Mounted subtrees are retained: Linux removal checks kernel mount IDs for opened
directories, including same-device bind mounts; macOS checks device boundaries.
Other platforms or unavailable mount identity support fail closed. Streaming
transport output is capped before it can accumulate beyond the response limit.
Removal replies must match a fresh per-call nonce; static startup output cannot
stand in for the helper's result. Hook timeout reaps ordinary process-group
descendants, but a child escaping that group is not proof of termination.

Issue-workspace cleanup returns removal/path errors and stops host fanout at the
first failure. This does not add coordinator retries, prove worker termination,
confirm a GitHub merge, or satisfy a permanent-archive gate.

Factory managed cleanup is a separate, opt-in service action. The existing
coordinator records directory identities before new runs execute; older unrecorded
paths remain retained. Admission and cleanup reject overlapping owners. Idle
reconciliation must confirm the recorded published head and real GitHub merge,
require fresh contained termination of every external operation, persist the
deletion intent, and verify the exact current permanent archive before deleting
each recorded directory. A recreated path never inherits the old deletion intent.
Supported service-only checks require a durable service-key seal created after
authenticated gate verification and bound to the run, pinned definitions and
execution, stage attempt, result and gate. Unknown agents/executable checks cannot
use this distinction. Typed output projections preserve actual redacted metadata
and stdout bytes, bind to original output identities and require immutable archive
receipts. Printed filenames grant no retained file-content guarantee; unknown
structured file outputs retain their directories.
Failures remain pending and restart retries all gates. A Linear terminal state
alone must not trigger automatic deletion. Cancellation stays retained until an
approved retention and verified recovery contract exists. Parent output artifacts
must be readable from verified immutable evidence outside the child workspace.
A service-owned read-only merged-revision/archive proof is available independently
of workspace deletion. Capture holds fence cleanup scheduling and its final
ownership check. No cleanup action closes, changes or merges a GitHub PR.

This is the most important portability constraint.

Invariant 1: Run the coding agent only in the per-issue workspace path.

- Before launching the coding-agent subprocess, validate:
  - `cwd == workspace_path`

Invariant 2: Workspace path MUST stay inside workspace root.

- Normalize both paths to absolute.
- Require `workspace_path` to have `workspace_root` as a prefix directory.
- Reject any path outside the workspace root.

Invariant 3: Workspace key is sanitized.

- Only `[A-Za-z0-9._-]` allowed in workspace directory names.
- Replace all other characters with `_`.
- If replacement changes the identifier, append a stable original-identifier hash suffix with at
  least 64 bits of entropy so keys remain collision-resistant after sanitization.

## 10. Agent Runner Protocol (Coding Agent Integration)

This section defines Symphony's language-neutral responsibilities when integrating a Codex
app-server. The Codex app-server protocol for the targeted Codex version is the source of truth for
protocol schemas, message payloads, transport framing, and method names.

Protocol source of truth:

- Implementations MUST send messages that are valid for the targeted Codex app-server version.
- Implementations MUST consult the targeted Codex app-server documentation or generated schema
  instead of treating this specification as a protocol schema.
- If this specification appears to conflict with the targeted Codex app-server protocol, the Codex
  protocol controls protocol shape and transport behavior.
- Symphony-specific requirements in this section still control orchestration behavior, workspace
  selection, prompt construction, continuation handling, and observability extraction.

### 10.1 Launch Contract

Subprocess launch parameters:

- Command: `codex.command`
- Invocation: `bash -lc <codex.command>`
- Working directory: workspace path
- Transport/framing: the protocol transport required by the targeted Codex app-server version

Notes:

- The default command is `codex app-server`.
- Approval policy, sandbox policy, cwd, prompt input, and OPTIONAL tool declarations are supplied
  using fields supported by the targeted Codex app-server version.

RECOMMENDED additional process settings:

- Max line size: 10 MB (for safe buffering)

### 10.2 Session Startup Responsibilities

Reference: https://developers.openai.com/codex/app-server/

Startup MUST follow the targeted Codex app-server contract. Symphony additionally requires the
client to:

- Start the app-server subprocess in the per-issue workspace.
- Initialize the app-server session using the targeted Codex app-server protocol.
- Create or resume a coding-agent thread according to the targeted protocol.
- Supply the absolute per-issue workspace path as the thread/turn working directory wherever the
  targeted protocol accepts cwd.
- Start the first turn with the rendered issue prompt.
- Start later in-worker continuation turns on the same live thread with continuation guidance rather
  than resending the original issue prompt.
- Supply the implementation's documented approval and sandbox policy using fields supported by the
  targeted protocol.
- Include issue-identifying metadata, such as `<issue.identifier>: <issue.title>`, when the targeted
  protocol supports turn or session titles.
- Advertise implemented client-side tools using the targeted protocol.

Session identifiers:

- Extract `thread_id` from the thread identity returned by the targeted Codex app-server protocol.
- Extract `turn_id` from each turn identity returned by the targeted Codex app-server protocol.
- Emit `session_id = "<thread_id>-<turn_id>"`
- Reuse the same `thread_id` for all continuation turns inside one worker run

### 10.3 Streaming Turn Processing

The client processes app-server updates according to the targeted Codex app-server protocol until
the active turn terminates.

Completion conditions:

- Targeted-protocol turn completion signal -> success
- Targeted-protocol turn failure signal -> failure
- Targeted-protocol turn cancellation signal -> failure
- turn stream silence timeout (`turn_timeout_ms`) -> failure
- subprocess exit -> failure

Continuation processing:

- If the worker decides to continue after a successful turn, it SHOULD start another turn on the same
  live thread using the targeted protocol.
- The app-server subprocess SHOULD remain alive across those continuation turns and be stopped only
  when the worker run is ending.

Transport handling requirements:

- Follow the transport and framing rules of the targeted Codex app-server version.
- For stdio-based transports, keep protocol stream handling separate from diagnostic stderr
  handling unless the targeted protocol specifies otherwise.

### 10.4 Emitted Runtime Events (Upstream to Orchestrator)

The app-server client emits structured events to the orchestrator callback. Each event SHOULD
include:

- `event` (enum/string)
- `timestamp` (UTC timestamp)
- `codex_app_server_pid` (if available)
- OPTIONAL `usage` map (token counts)
- payload fields as needed

Important emitted events include, for example:

- `session_started`
- `startup_failed`
- `turn_completed`
- `turn_failed`
- `turn_cancelled`
- `turn_ended_with_error`
- `turn_input_required`
- `approval_auto_approved`
- `unsupported_tool_call`
- `notification`
- `other_message`
- `malformed`

### 10.5 Approval, Tool Calls, and User Input Policy

Approval, sandbox, and user-input behavior is implementation-defined.

Policy requirements:

- Each implementation MUST document its chosen approval, sandbox, and operator-confirmation
  posture.
- Approval requests and user-input-required events MUST NOT leave a run stalled indefinitely. An
  implementation MAY either satisfy them, surface them to an operator, auto-resolve them, or
  fail the run according to its documented policy.

Example high-trust behavior:

- Auto-approve command execution approvals for the session.
- Auto-approve file-change approvals for the session.
- Treat user-input-required turns as hard failure.

Unsupported dynamic tool calls:

- Supported dynamic tool calls that are explicitly implemented and advertised by the runtime SHOULD
  be handled according to their extension contract.
- If the agent requests a dynamic tool call that is not supported, return a tool failure response
  using the targeted protocol and continue the session.
- This prevents the session from stalling on unsupported tool execution paths.

Optional provider-native agent tool extension:

- An adapter MAY expose provider-native tools to the app-server session.
- The selected adapter's tool specs SHOULD be advertised during session startup using the protocol
  mechanism supported by the targeted Codex app-server version.
- Tool specs, adapter selection, and effective tracker settings MUST be bound to one session
  snapshot. A workflow reload applies to future sessions; it MUST NOT make an in-flight session
  advertise one provider and execute another.
- Tool names, schemas, and result payloads are adapter-owned. Symphony does not standardize a
  lowest-common-denominator CRUD API.
- The runtime MUST execute advertised tool calls host-side with the active adapter configuration and
  MUST NOT require the coding-agent child process to read raw tracker tokens from disk or
  environment.
- The runtime SHOULD pass the current normalized issue to the adapter as internal execution context.
  The adapter MAY use `issue.id` and `issue.native_ref` to preserve provider-specific richness
  without teaching the orchestrator provider semantics.
- Tracker credentials SHOULD NOT be inherited by the coding-agent child process. An adapter that
  resolves credentials from environment variables MUST declare authentication-related environment
  names for removal from local and remote child environments. Implementations SHOULD consult current
  provider and client documentation when identifying credential names and aliases, as these can
  change over time. Literal credentials in a repo-owned `WORKFLOW.md` remain readable to a child
  with workspace access and SHOULD NOT be used when this isolation matters.
- Unsupported tool names MUST return a structured failure result using the targeted protocol and
  continue the session.
- Each adapter that ships tools MUST document:
  - tool names and input schemas;
  - whether a tool can mutate tracker state;
  - scope/authorization behavior;
  - result and error semantics;
  - any provider-side idempotency or rate-limit expectations.

Minimal language-neutral adapter hooks for this OPTIONAL extension:

```text
agent_tool_specs() -> list<ToolSpec>
secret_environment_names() -> list<string>
execute_agent_tool(name, arguments, context={issue}) -> ToolResult
```

`ToolResult` MUST distinguish success from failure and carry JSON-safe structured output that can
be translated to the targeted app-server protocol. The context contains the normalized issue, never
the credential.

User-input-required policy:

- Implementations MUST document how targeted-protocol user-input-required signals are handled.
- A run MUST NOT stall indefinitely waiting for user input.
- A conforming implementation MAY fail the run, surface the request to an operator, satisfy it
  through an approved operator channel, or auto-resolve it according to its documented policy.
- The example high-trust behavior above fails user-input-required turns immediately.

### 10.6 Timeouts and Error Mapping

Timeouts:

- `codex.read_timeout_ms`: request/response timeout during startup and sync requests
- `codex.turn_timeout_ms`: maximum silence interval while a turn stream is active; each
  app-server output resets it, so it is not a total turn runtime cap
- `codex.stall_timeout_ms`: enforced by orchestrator based on event inactivity

Error mapping (RECOMMENDED normalized categories):

- `codex_not_found`
- `invalid_workspace_cwd`
- `response_timeout`
- `turn_timeout`
- `port_exit`
- `response_error`
- `turn_failed`
- `turn_cancelled`
- `turn_input_required`

### 10.7 Agent Runner Contract

The `Agent Runner` wraps workspace + prompt + app-server client.

Behavior:

1. Create/reuse workspace for issue.
2. Build prompt from workflow template.
3. Start app-server session.
4. Forward app-server events to orchestrator.
5. On any error, fail the worker attempt (the orchestrator will retry).

Note:

- Workspaces are intentionally preserved after successful runs.

## 11. Issue Tracker Integration Contract

The issue tracker boundary is deliberately small: a portable read kernel for scheduling plus
OPTIONAL provider-native agent tools. Do not add generic comment/state/attachment CRUD merely to
make providers look alike; those operations lose useful provider semantics and are not needed by
the orchestrator.

### 11.1 REQUIRED Adapter Operations

An implementation MUST support these adapter operations:

1. `fetch_issues_by_states(state_names)`
   - Return normalized issues visible in the configured tracker scope and requested state names.
   - The adapter MUST apply provider-side scope selection and pagination.
   - Used with configured active states for candidate polling and terminal states for startup
     cleanup.
   - When used for candidate polling, include active scoped issues even when
     `dispatchable=false`; the scheduler owns that final filter.
   - The orchestrator applies `required_labels`, `dispatchable`, claims, retries, and concurrency
     after normalization.
   - An empty `state_names` list MUST return an empty result without a provider request.

2. `fetch_issues_by_ids(issue_ids)`
   - Return current normalized issue snapshots for the supplied opaque dispatch IDs.
   - Used for active-run reconciliation and stale-dispatch revalidation.
   - An empty `issue_ids` list MUST return an empty result without a provider request.
   - IDs no longer visible in the configured scope are omitted; the orchestrator treats omission as
     "no longer visible" rather than inventing a synthetic state.

Both operations return either `ok(list<Issue>)` or an adapter error. For portability, an adapter
error SHOULD expose a stable category and human-readable message. An implementation MAY use a
language-native tagged error, exception, tuple, or enum instead of a literal error object when its
adapter profile documents how those public error forms map to category and message. The
orchestrator relies only on success versus failure.

The operations are atomic from the scheduler's perspective after a paging or transport failure. For
these rules, a record is malformed only when the adapter cannot produce the required normalized
fields (`id`, `identifier`, `title`, `state`, and explicit `dispatchable`) or cannot produce a
valid `Issue` after applying the optional-field fallback rules in Section 11.3. Unusable nullable
or best-effort provider metadata MAY normalize to `null`, an empty list, or omitted best-effort
entries; that fallback alone does not make a record malformed.

A state-list call MAY omit an individually malformed provider record because it was never safe to
dispatch, and SHOULD log that omission. An ID-refresh call MUST fail instead of silently omitting a
malformed requested record, because omission is meaningful. A successful
`fetch_issues_by_ids` result is complete for that call. Output order is not significant, input IDs
are treated as a set, and each dispatch ID appears at most once.

The refresh operation returns full normalized snapshots, not only state strings, because label,
assignment, routing, and provider-specific dispatchability can change while a run is active.

### 11.2 Adapter Responsibilities

Each adapter owns:

- construction from the current effective tracker configuration, including active/terminal states;
- endpoint, authentication, transport, timeouts, pagination, and rate-limit handling;
- provider-specific scope selection (project, board, team, repository, query, or equivalent);
- mapping provider payloads into the normalized Issue fields in Section 4.1.1;
- choosing a stable dispatch identity and preserving any distinct underlying IDs in `native_ref`;
- deriving `dispatchable` from provider-specific routing rules;
- preserving provider-native state names while allowing case-insensitive scheduler comparison;
- OPTIONAL provider-native agent tools and their authorization boundary.

The orchestrator MUST NOT inspect provider payloads, assume that `issue.id` is an underlying
ticket ID, or branch on provider-specific blocker, board, transition, or comment semantics.

Each adapter MUST publish a compact profile in implementation documentation, not only code,
containing:

- exact supported `tracker.kind` value;
- exact `tracker.provider` keys, defaults, secret keys/environment names, and validation errors;
- scope selection, pagination behavior, and provider request limits;
- `id` and `native_ref` mapping;
- state, label, priority, timestamp, `dispatchable`, malformed-record, and optional-field
  normalization;
- provider-native tool names/schemas, mutation capability, scope, and result/error behavior if any;
- mapping from public language-native error forms to portable transport/auth/rate-limit error
  categories and human-readable messages.

### 11.3 Normalization Rules

Adapter output MUST satisfy Section 4.1.1. In addition:

- Every listed field MUST be present in the normalized record. Nullable fields use `null`;
  collection fields use an empty list when absent.
- `id`, `identifier`, `title`, and `state` MUST be non-empty strings.
- `labels` MUST be trimmed, lowercased strings; blank labels MUST be dropped and duplicate labels
  SHOULD be removed.
- `priority` MUST be an integer or null.
- The scheduler ranks priorities `1..4` before null/unknown values; other integers sort with
  null/unknown unless an implementation documents a different mapping.
- `created_at` and `updated_at` MUST represent parsed RFC 3339 instants or null; the in-memory
  timestamp type is implementation-defined.
- Unusable provider values for nullable fields MAY normalize to `null`. Unusable best-effort
  collection entries MAY be dropped; if no usable entries remain, use an empty list. These
  fallbacks MUST NOT be used for `id`, `identifier`, `title`, `state`, or explicit
  `dispatchable`.
- Preserve provider spelling in `state`, but trim and lowercase only for scheduler comparisons.
- `blocked_by` is best-effort metadata; adapters MUST NOT invent blocker semantics they cannot
  represent reliably.
- `dispatchable` MUST be explicit. It is `true` only when provider-specific eligibility checks
  pass; the generic scheduler never tries to reconstruct those checks from `native_ref`.
- `native_ref` MUST be null or a JSON-safe object containing only non-secret values safe to expose
  in prompt/tool context. If provider metadata cannot be represented safely, normalize
  `native_ref` to null; otherwise preserve the retained object verbatim.

### 11.4 Error Handling Contract

RECOMMENDED adapter error categories:

- `unsupported_tracker_kind`
- `invalid_tracker_config`
- `missing_tracker_secret`
- `tracker_request` (transport failure)
- `tracker_status` (non-success response)
- `tracker_response` (malformed or semantically invalid payload)
- `tracker_pagination` (pagination integrity failure)
- `tracker_rate_limited`

For portability, every adapter profile MUST document how each public language-native error form
maps to a stable `category` and human-readable `message`. A literal `{category, message}`
object is not required. Adapters MAY add `retryable`, `retry_after_ms`, provider status, and
provider-specific detail, but the orchestrator only relies on success vs. failure.

Orchestrator behavior on tracker errors:

- Candidate fetch failure: log and skip dispatch for this tick.
- Running-state refresh failure: log and keep active workers running.
- Startup terminal cleanup failure: log warning and continue startup.

### 11.5 Tracker Writes and Agent Tools (Important Boundary)

Symphony does not require first-class tracker write APIs in the orchestrator.

- Ticket mutations (state transitions, comments, attachments, PR metadata) are typically handled by
  the coding agent through the selected adapter's provider-native tools.
- Tools execute in Symphony with the configured adapter credential; the child receives tool results,
  not a raw token.
- The current normalized issue is available to tool execution as context, including opaque
  `native_ref`, so adapters can retain provider richness without adding it to the core scheduler.
- The service remains a scheduler/runner and tracker reader.
- Workflow-specific success often means "reached the next handoff state" (for example
  `Human Review`) rather than tracker terminal state `Done`.

## 12. Prompt Construction and Context Assembly

### 12.1 Inputs

Inputs to prompt rendering:

- `workflow.prompt_template`
- normalized `issue` object
- OPTIONAL `attempt` integer (retry/continuation metadata)

### 12.2 Rendering Rules

- Render with strict variable checking.
- Render with strict filter checking.
- Convert issue object keys to strings for template compatibility.
- Preserve nested arrays/maps (labels, blockers) so templates can iterate.

### 12.3 Retry/Continuation Semantics

`attempt` SHOULD be passed to the template as a 1-based retry/continuation count:

- first run: `attempt` is null or absent;
- any later run: `attempt` is an integer.

The core `attempt` value does not distinguish a normal continuation from an error/timeout/stall
retry. An implementation MAY expose an additional `retry_kind` template field if workflows need
that distinction, but it is not part of core conformance.

### 12.4 Failure Semantics

If prompt rendering fails:

- Fail the run attempt immediately.
- Let the orchestrator treat it like any other worker failure and decide retry behavior.

## 13. Logging, Status, and Observability

### 13.1 Logging Conventions

REQUIRED context fields for issue-related logs:

- `issue_id`
- `issue_identifier`

REQUIRED context for coding-agent session lifecycle logs:

- `session_id`

Message formatting requirements:

- Use stable `key=value` phrasing.
- Include action outcome (`completed`, `failed`, `retrying`, etc.).
- Include concise failure reason when present.
- Avoid logging large raw payloads unless necessary.

### 13.2 Logging Outputs and Sinks

The spec does not prescribe where logs are written (stderr, file, remote sink, etc.).

Requirements:

- Operators MUST be able to see startup/validation/dispatch failures without attaching a debugger.
- Implementations MAY write to one or more sinks.
- If a configured log sink fails, the service SHOULD continue running when possible and emit an
  operator-visible warning through any remaining sink.

### 13.3 Runtime Snapshot / Monitoring Interface (OPTIONAL but RECOMMENDED)

If the implementation exposes a synchronous runtime snapshot (for dashboards or monitoring), it
SHOULD return:

- `running` (list of running session rows)
- each running row SHOULD include `turn_count`
- `retrying` (list of retry queue rows)
- session and retry rows SHOULD include the tracker-provided issue URL when available
- `codex_totals`
  - `input_tokens`
  - `output_tokens`
  - `total_tokens`
  - `seconds_running` (aggregate runtime seconds as of snapshot time, including active sessions)
- `rate_limits` (latest coding-agent rate limit payload, if available)

RECOMMENDED snapshot error modes:

- `timeout`
- `unavailable`

### 13.4 OPTIONAL Human-Readable Status Surface

A human-readable status surface (terminal output, dashboard, etc.) is OPTIONAL and
implementation-defined.

If present, it SHOULD draw from orchestrator state/metrics only and MUST NOT be REQUIRED for
correctness.

### 13.5 Session Metrics and Token Accounting

Token accounting rules:

- Agent events can include token counts in multiple payload shapes.
- Prefer absolute thread totals when available, such as:
  - `thread/tokenUsage/updated` payloads
  - `total_token_usage` within token-count wrapper events
- Ignore delta-style payloads such as `last_token_usage` for dashboard/API totals.
- Extract input/output/total token counts leniently from common field names within the selected
  payload.
- For absolute totals, track deltas relative to last reported totals to avoid double-counting.
- Do not treat generic `usage` maps as cumulative totals unless the event type defines them that
  way.
- Accumulate aggregate totals in orchestrator state.

Runtime accounting:

- Runtime SHOULD be reported as a live aggregate at snapshot/render time.
- Implementations MAY maintain a cumulative counter for ended sessions and add active-session
  elapsed time derived from `running` entries (for example `started_at`) when producing a
  snapshot/status view.
- Add run duration seconds to the cumulative ended-session runtime when a session ends (normal exit
  or cancellation/termination).
- Continuous background ticking of runtime totals is not REQUIRED.

Rate-limit tracking:

- Track the latest rate-limit payload seen in any agent update.
- Any human-readable presentation of rate-limit data is implementation-defined.

### 13.6 Humanized Agent Event Summaries (OPTIONAL)

Humanized summaries of raw agent protocol events are OPTIONAL.

If implemented:

- Treat them as observability-only output.
- Do not make orchestrator logic depend on humanized strings.

### 13.7 OPTIONAL HTTP Server Extension

This section defines an OPTIONAL HTTP interface for observability and operational control.

If implemented:

- The HTTP server is an extension and is not REQUIRED for conformance.
- The implementation MAY serve server-rendered HTML or a client-side application for the dashboard.
- The dashboard/API MUST be observability/control surfaces only and MUST NOT become REQUIRED for
  orchestrator correctness.

Extension config:

- `server.port` (integer, OPTIONAL)
  - Enables the HTTP server extension.
  - `0` requests an ephemeral port for local development and tests.
  - CLI `--port` overrides `server.port` when both are present.
- `server.webhook_port` (integer, OPTIONAL, 0–65535) starts a separate native Linear
  listener. `server.webhook_host` defaults to `127.0.0.1`. It exposes only exact
  `POST /hooks/linear`, with the same raw-byte signature, timestamp and durable-intake
  checks; all other paths and methods return 404. It exposes no dashboard, API,
  method override, session or LiveView surface. Listener changes require restart.

Enablement (extension):

- Start the HTTP server when a CLI `--port` argument is provided.
- Start the HTTP server when `server.port` is present in `WORKFLOW.md` front matter.
- The `server` top-level key is owned by this extension.
- Positive `server.port` values bind that port.
- Implementations SHOULD bind loopback by default (`127.0.0.1` or host equivalent) unless explicitly
  configured otherwise.
- Changes to HTTP listener settings (for example `server.port`) do not need to hot-rebind;
  restart-required behavior is conformant.

#### 13.7.1 Human-Readable Dashboard (`/`)

- Host a human-readable dashboard at `/`.
- The returned document SHOULD depict the current state of the system (for example active sessions,
  retry delays, token consumption, runtime totals, recent events, and health/error indicators).
- It is up to the implementation whether this is server-generated HTML or a client-side app that
  consumes the JSON API below.

#### 13.7.2 JSON REST API (`/api/v1/*`)

Provide a JSON REST API under `/api/v1/*` for current runtime state and operational debugging.

Minimum endpoints:

- `GET /api/v1/state`
  - Returns a summary view of the current system state (running sessions, retry queue/delays,
    aggregate token/runtime totals, latest rate limits, and any additional tracked summary fields).
  - Suggested response shape:

    ```json
    {
      "generated_at": "2026-02-24T20:15:30Z",
      "counts": {
        "running": 2,
        "retrying": 1
      },
      "running": [
        {
          "issue_id": "abc123",
          "issue_identifier": "MT-649",
          "issue_url": "https://tracker.example/issues/MT-649",
          "state": "In Progress",
          "session_id": "thread-1-turn-1",
          "turn_count": 7,
          "last_event": "turn_completed",
          "last_message": "",
          "started_at": "2026-02-24T20:10:12Z",
          "last_event_at": "2026-02-24T20:14:59Z",
          "tokens": {
            "input_tokens": 1200,
            "output_tokens": 800,
            "total_tokens": 2000
          }
        }
      ],
      "retrying": [
        {
          "issue_id": "def456",
          "issue_identifier": "MT-650",
          "issue_url": "https://tracker.example/issues/MT-650",
          "attempt": 3,
          "due_at": "2026-02-24T20:16:00Z",
          "error": "no available orchestrator slots"
        }
      ],
      "codex_totals": {
        "input_tokens": 5000,
        "output_tokens": 2400,
        "total_tokens": 7400,
        "seconds_running": 1834.2
      },
      "rate_limits": null
    }
    ```

- `GET /api/v1/<issue_identifier>`
  - Returns issue-specific runtime/debug details for the identified issue, including any information
    the implementation tracks that is useful for debugging.
  - Suggested response shape:

    ```json
    {
      "issue_identifier": "MT-649",
      "issue_id": "abc123",
      "status": "running",
      "workspace": {
        "path": "/tmp/symphony_workspaces/MT-649"
      },
      "attempts": {
        "restart_count": 1,
        "current_retry_attempt": 2
      },
      "running": {
        "session_id": "thread-1-turn-1",
        "turn_count": 7,
        "state": "In Progress",
        "started_at": "2026-02-24T20:10:12Z",
        "last_event": "notification",
        "last_message": "Working on tests",
        "last_event_at": "2026-02-24T20:14:59Z",
        "tokens": {
          "input_tokens": 1200,
          "output_tokens": 800,
          "total_tokens": 2000
        }
      },
      "retry": null,
      "logs": {
        "codex_session_logs": [
          {
            "label": "latest",
            "path": "/var/log/symphony/codex/MT-649/latest.log",
            "url": null
          }
        ]
      },
      "recent_events": [
        {
          "at": "2026-02-24T20:14:59Z",
          "event": "notification",
          "message": "Working on tests"
        }
      ],
      "last_error": null,
      "tracked": {}
    }
    ```

  - If the issue is unknown to the current in-memory state, return `404` with an error response (for
    example `{\"error\":{\"code\":\"issue_not_found\",\"message\":\"...\"}}`).

- `POST /api/v1/refresh`
  - Queues an immediate tracker poll + reconciliation cycle (best-effort trigger; implementations
    MAY coalesce repeated requests).
  - Suggested request body: empty body or `{}`.
  - Suggested response (`202 Accepted`) shape:

    ```json
    {
      "queued": true,
      "coalesced": false,
      "requested_at": "2026-02-24T20:15:30Z",
      "operations": ["poll", "reconcile"]
    }
    ```

API design notes:

- The JSON shapes above are the RECOMMENDED baseline for interoperability and debugging ergonomics.
- Implementations MAY add fields, but SHOULD avoid breaking existing fields within a version.
- Endpoints SHOULD be read-only except for operational triggers like `/refresh`.
- Unsupported methods on defined routes SHOULD return `405 Method Not Allowed`.
- API errors SHOULD use a JSON envelope such as `{"error":{"code":"...","message":"..."}}`.
- If the dashboard is a client-side app, it SHOULD consume this API rather than duplicating state
  logic.

## 14. Failure Model and Recovery Strategy

### 14.1 Failure Classes

1. `Workflow/Config Failures`
   - Missing `WORKFLOW.md`
   - Invalid YAML front matter
   - Unsupported tracker kind or invalid adapter-owned tracker configuration
   - Missing coding-agent executable

2. `Workspace Failures`
   - Workspace directory creation failure
   - Workspace population/synchronization failure (implementation-defined; can come from hooks)
   - Invalid workspace path configuration
   - Hook timeout/failure

3. `Agent Session Failures`
   - Startup handshake failure
   - Turn failed/cancelled
   - Turn timeout
   - User input requested and handled as failure by the implementation's documented policy
   - Subprocess exit
   - Stalled session (no activity)

4. `Tracker Failures`
   - Provider transport errors
   - Non-success provider responses
   - Provider-reported application errors
   - Malformed payloads

5. `Observability Failures`
   - Snapshot timeout
   - Dashboard render errors
   - Log sink configuration failure

### 14.2 Recovery Behavior

- Dispatch validation failures:
  - Skip new dispatches.
  - Keep service alive.
  - Continue reconciliation where possible.

- Worker failures:
  - Convert to retries with exponential backoff.

- Tracker candidate-fetch failures:
  - Skip this tick.
  - Try again on next tick.

- Reconciliation state-refresh failures:
  - Keep current workers.
  - Retry on next tick.

- Dashboard/log failures:
  - Do not crash the orchestrator.

### 14.3 Partial State Recovery (Restart)

Current design is intentionally in-memory for scheduler state.
Restart recovery means the service can resume useful operation by polling tracker state and reusing
preserved workspaces. It does not mean retry timers, running sessions, or live worker state survive
process restart.

After restart:

- No retry timers are restored from prior process memory.
- No running sessions are assumed recoverable.
- Service recovers by:
  - startup terminal workspace cleanup
  - fresh polling of active issues
  - re-dispatching eligible work

### 14.4 Operator Intervention Points

Operators can control behavior by:

- Editing `WORKFLOW.md` (prompt and most runtime settings).
- `WORKFLOW.md` changes are detected and re-applied automatically without restart according to
  Section 6.2.
- Changing issue states in the tracker:
  - terminal state -> running session is stopped and workspace cleaned when reconciled
  - non-active state -> running session is stopped without cleanup
- Restarting the service for process recovery or deployment (not as the normal path for applying
  workflow config changes).

## 15. Security and Operational Safety

### 15.1 Trust Boundary Assumption

Each implementation defines its own trust boundary.

Operational safety requirements:

- Implementations SHOULD state clearly whether they are intended for trusted environments, more
  restrictive environments, or both.
- Implementations SHOULD state clearly whether they rely on auto-approved actions, operator
  approvals, stricter sandboxing, or some combination of those controls.
- Workspace isolation and path validation are important baseline controls, but they are not a
  substitute for whatever approval and sandbox policy an implementation chooses.

### 15.2 Filesystem Safety Requirements

Mandatory:

- Workspace path MUST remain under configured workspace root.
- Coding-agent cwd MUST be the per-issue workspace path for the current run.
- Workspace directory names MUST use sanitized identifiers.

RECOMMENDED additional hardening for ports:

- Run under a dedicated OS user.
- Restrict workspace root permissions.
- Mount workspace root on a dedicated volume if possible.

### 15.3 Secret Handling

- Support `$VAR` indirection in workflow config.
- Do not log API tokens or secret env values.
- Validate presence of secrets without printing them.
- Execute provider-native tracker tools in the Symphony host process with the configured adapter
  credential.
- Do not pass tracker credentials through the coding-agent child environment. Adapters MUST declare
  secret environment names so local and remote launchers can remove them from child environments.
- Do not place literal tracker credentials in a repo-owned `WORKFLOW.md` when the child can read
  that workspace; use host-side secret references instead.

### 15.4 Hook Script Safety

Workspace hooks are arbitrary shell scripts from `WORKFLOW.md`.

Implications:

- Hooks are fully trusted configuration.
- Hooks run inside the workspace directory.
- Hook output SHOULD be truncated in logs.
- Hook timeouts are REQUIRED to avoid hanging the orchestrator.

### 15.5 Harness Hardening Guidance

Running Codex agents against repositories, issue trackers, and other inputs that can contain
sensitive data or externally-controlled content can be dangerous. A permissive deployment can lead
to data leaks, destructive mutations, or full machine compromise if the agent is induced to execute
harmful commands or use overly-powerful integrations.

Implementations SHOULD explicitly evaluate their own risk profile and harden the execution harness
where appropriate. This specification intentionally does not mandate a single hardening posture, but
implementations SHOULD NOT assume that tracker data, repository contents, prompt inputs, or tool
arguments are fully trustworthy just because they originate inside a normal workflow.

Possible hardening measures include:

- Tightening Codex approval and sandbox settings described elsewhere in this specification instead
  of running with a maximally permissive configuration.
- Adding external isolation layers such as OS/container/VM sandboxing, network restrictions, or
  separate credentials beyond the built-in Codex policy controls.
- Filtering which issues, projects, boards, teams, labels, or other tracker sources are eligible
  for dispatch so untrusted or out-of-scope tasks do not automatically reach the agent.
- Narrowing provider-native tools so they can only read or mutate data inside the intended tracker
  scope, rather than exposing general workspace-wide tracker access.
- Reducing the set of client-side tools, credentials, filesystem paths, and network destinations
  available to the agent to the minimum needed for the workflow.

The correct controls are deployment-specific, but implementations SHOULD document them clearly and
treat harness hardening as part of the core safety model rather than an optional afterthought.

## 16. Reference Algorithms (Language-Agnostic)

### 16.1 Service Startup

```text
function start_service():
  configure_logging()
  start_observability_outputs()
  start_workflow_watch(on_change=reload_and_reapply_workflow)

  state = {
    poll_interval_ms: get_config_poll_interval_ms(),
    max_concurrent_agents: get_config_max_concurrent_agents(),
    running: {},
    claimed: set(),
    retry_attempts: {},
    completed: set(),
    codex_totals: {input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
    codex_rate_limits: null
  }

  validation = validate_dispatch_config()
  if validation is not ok:
    log_validation_error(validation)
    fail_startup(validation)

  startup_terminal_workspace_cleanup()
  schedule_tick(delay_ms=0)

  event_loop(state)
```

### 16.2 Poll-and-Dispatch Tick

```text
on_tick(state):
  state = reconcile_running_issues(state)

  validation = validate_dispatch_config()
  if validation is not ok:
    log_validation_error(validation)
    notify_observers()
    schedule_tick(state.poll_interval_ms)
    return state

  issues = tracker.fetch_issues_by_states(active_states)
  if issues failed:
    log_tracker_error()
    notify_observers()
    schedule_tick(state.poll_interval_ms)
    return state

  for issue in sort_for_dispatch(issues):
    if no_available_slots(state):
      break

    if should_dispatch(issue, state):
      state = dispatch_issue(issue, state, attempt=null)

  notify_observers()
  schedule_tick(state.poll_interval_ms)
  return state
```

### 16.3 Reconcile Active Runs

```text
function reconcile_running_issues(state):
  state = reconcile_stalled_runs(state)

  running_ids = keys(state.running)
  if running_ids is empty:
    return state

  refreshed = tracker.fetch_issues_by_ids(running_ids)
  if refreshed failed:
    log_debug("keep workers running")
    return state

  for issue in refreshed:
    if issue.state in terminal_states:
      state = terminate_running_issue(state, issue.id, cleanup_workspace=true)
    else if issue.state in active_states and issue_routable(issue):
      state.running[issue.id].issue = issue
    else:
      state = terminate_running_issue(state, issue.id, cleanup_workspace=false)

  returned_ids = set(issue.id for issue in refreshed)
  for missing_id in running_ids - returned_ids:
    state = terminate_running_issue(state, missing_id, cleanup_workspace=false)

  return state
```

### 16.4 Dispatch One Issue

```text
function dispatch_issue(issue, state, attempt):
  worker = spawn_worker(
    fn -> run_agent_attempt(issue, attempt, parent_orchestrator_pid) end
  )

  if worker spawn failed:
    return schedule_retry(state, issue.id, next_attempt(attempt), {
      identifier: issue.identifier,
      error: "failed to spawn agent"
    })

  state.running[issue.id] = {
    worker_handle,
    monitor_handle,
    identifier: issue.identifier,
    issue,
    session_id: null,
    codex_app_server_pid: null,
    last_codex_message: null,
    last_codex_event: null,
    last_codex_timestamp: null,
    codex_input_tokens: 0,
    codex_output_tokens: 0,
    codex_total_tokens: 0,
    last_reported_input_tokens: 0,
    last_reported_output_tokens: 0,
    last_reported_total_tokens: 0,
    retry_attempt: normalize_attempt(attempt),
    started_at: now_utc()
  }

  state.claimed.add(issue.id)
  state.retry_attempts.remove(issue.id)
  return state
```

### 16.5 Worker Attempt (Workspace + Prompt + Agent)

```text
function run_agent_attempt(issue, attempt, orchestrator_channel):
  workspace = workspace_manager.create_for_issue(issue.identifier)
  if workspace failed:
    fail_worker("workspace error")

  if run_hook("before_run", workspace.path) failed:
    fail_worker("before_run hook error")

  session = app_server.start_session(workspace=workspace.path)
  if session failed:
    run_hook_best_effort("after_run", workspace.path)
    fail_worker("agent session startup error")

  max_turns = config.agent.max_turns
  turn_number = 1

  while true:
    prompt = build_turn_prompt(workflow_template, issue, attempt, turn_number, max_turns)
    if prompt failed:
      app_server.stop_session(session)
      run_hook_best_effort("after_run", workspace.path)
      fail_worker("prompt error")

    turn_result = app_server.run_turn(
      session=session,
      prompt=prompt,
      issue=issue,
      on_message=(msg) -> send(orchestrator_channel, {codex_update, issue.id, msg})
    )

    if turn_result failed:
      app_server.stop_session(session)
      run_hook_best_effort("after_run", workspace.path)
      fail_worker("agent turn error")

    refreshed_issue = tracker.fetch_issues_by_ids([issue.id])
    if refreshed_issue failed:
      app_server.stop_session(session)
      run_hook_best_effort("after_run", workspace.path)
      fail_worker("issue state refresh error")

    if refreshed_issue is empty:
      break

    issue = refreshed_issue[0]

    if issue.state is not active or not issue_routable(issue):
      break

    if turn_number >= max_turns:
      break

    turn_number = turn_number + 1

  app_server.stop_session(session)
  run_hook_best_effort("after_run", workspace.path)

  exit_normal()
```

### 16.6 Worker Exit and Retry Handling

```text
on_worker_exit(issue_id, reason, state):
  running_entry = state.running.remove(issue_id)
  state = add_runtime_seconds_to_totals(state, running_entry)

  if reason == normal:
    state.completed.add(issue_id)  # bookkeeping only
    state = schedule_retry(state, issue_id, 1, {
      identifier: running_entry.identifier,
      delay_type: continuation
    })
  else:
    state = schedule_retry(state, issue_id, next_attempt_from(running_entry), {
      identifier: running_entry.identifier,
      error: format("worker exited: %reason")
    })

  notify_observers()
  return state
```

```text
on_retry_timer(issue_id, state):
  retry_entry = state.retry_attempts.pop(issue_id)
  if missing:
    return state

  refreshed = tracker.fetch_issues_by_ids([issue_id])
  if fetch failed:
    return schedule_retry(state, issue_id, retry_entry.attempt + 1, {
      identifier: retry_entry.identifier,
      error: "retry refresh failed"
    })

  issue = find_by_id(refreshed, issue_id)
  if issue is null:
    state.claimed.remove(issue_id)
    return state

  if not retry_dispatch_allowed(issue, state, ignore_existing_claim=issue_id):
    state.claimed.remove(issue_id)
    return state

  if no_available_slots(state):
    return schedule_retry(state, issue_id, retry_entry.attempt + 1, {
      identifier: issue.identifier,
      error: "no available orchestrator slots"
    })

  return dispatch_issue(issue, state, attempt=retry_entry.attempt)
```

## 17. Test and Validation Matrix

A conforming implementation SHOULD include tests that cover the behaviors defined in this
specification.

Validation profiles:

- `Core Conformance`: deterministic tests REQUIRED for all conforming implementations.
- `Extension Conformance`: REQUIRED only for OPTIONAL features that an implementation chooses to
  ship.
- `Real Integration Profile`: environment-dependent smoke/integration checks RECOMMENDED before
  production use.

Unless otherwise noted, Sections 17.1 through 17.7 are `Core Conformance`. Bullets that begin with
`If ... is implemented` are `Extension Conformance`.

### 17.1 Workflow and Config Parsing

- Workflow file path precedence:
  - explicit runtime path is used when provided
  - cwd default is `WORKFLOW.md` when no explicit runtime path is provided
- Workflow file changes are detected and trigger re-read/re-apply without restart
- Invalid workflow reload keeps last known good effective configuration and emits an
  operator-visible error
- Missing `WORKFLOW.md` returns typed error
- Invalid YAML front matter returns typed error
- Front matter non-map returns typed error
- Config defaults apply when OPTIONAL values are missing
- `tracker.kind` validation enforces an implementation-supported adapter
- `tracker.provider` preserves adapter-owned keys and validates them through the selected adapter
- `$VAR` resolution works for documented adapter secret keys and path values
- `~` path expansion works
- `codex.command` is preserved as a shell command string
- Per-state concurrency override map normalizes state names and ignores invalid values
- Prompt template renders `issue` and `attempt`
- Prompt rendering fails on unknown variables (strict mode)

### 17.2 Workspace Manager and Safety

- Deterministic workspace path per issue identifier
- Missing workspace directory is created
- Existing workspace directory is reused
- Existing non-directory path at workspace location is handled safely (replace or fail per
  implementation policy)
- OPTIONAL workspace population/synchronization errors are surfaced
- `after_create` hook runs only on new workspace creation
- `before_run` hook runs before each attempt and failure/timeouts abort the current attempt
- `after_run` hook runs after each attempt and failure/timeouts are logged and ignored
- `before_remove` hook runs on cleanup and failures/timeouts are ignored
- Workspace path sanitization, stable original-identifier-hash collision resistance, and root
  containment invariants are enforced before agent launch
- Identifiers unchanged by sanitization keep their deterministic workspace key; conformance tests
  include distinct identifiers that sanitize to the same text and verify distinct keys
- Agent launch uses the per-issue workspace path as cwd and rejects out-of-root paths

### 17.3 Issue Tracker Adapter

- Candidate issue fetch applies configured active states and adapter-owned scope selection
- Empty `fetch_issues_by_states([])` returns empty without a provider call
- Empty `fetch_issues_by_ids([])` returns empty without a provider call
- Pagination preserves order across multiple pages
- Labels are normalized to lowercase
- Unusable optional provider metadata normalizes to null/empty without hiding valid required fields
- State-list reads log omitted malformed required records; ID refresh fails malformed requested
  records instead of treating them as invisible
- Refresh by opaque dispatch ID returns full normalized issue snapshots
- A distinct provider ticket ID or project-item ID is preserved in `native_ref` when needed
- Provider-specific routing/blocker/assignment rules become explicit `dispatchable`
- The adapter publishes the required compact profile for config, scope, normalization, tools, and
  portable error mapping
- Error mapping covers config, request, non-success response, malformed payload, pagination, and
  rate limiting, including documented category/message mappings for language-native errors

### 17.4 Orchestrator Dispatch, Reconciliation, and Retry

- Dispatch sort order is priority then oldest creation time
- `dispatchable=false` issues are not eligible
- Required-label filtering is case-insensitive and applies after adapter normalization
- Active-state issue refresh updates running entry state
- Non-active state stops running agent without workspace cleanup
- Terminal state stops running agent and cleans workspace
- Reconciliation with no running issues is a no-op
- Normal worker exit schedules a short continuation retry (attempt 1)
- Abnormal worker exit increments retries with 10s-based exponential backoff
- Retry backoff cap uses configured `agent.max_retry_backoff_ms`
- Retry queue entries include attempt, due time, identifier, and error
- Stall detection kills stalled sessions and schedules retry
- Slot exhaustion requeues retries with explicit error reason
- If a snapshot API is implemented, it returns running rows, retry rows, token totals, and rate
  limits
- If a snapshot API is implemented, timeout/unavailable cases are surfaced

### 17.5 Coding-Agent App-Server Client

- Launch command uses workspace cwd and invokes `bash -lc <codex.command>`
- Session startup follows the targeted Codex app-server protocol.
- Client identity/capability payloads are valid when the targeted Codex app-server protocol requires
  them.
- Policy-related startup payloads use the implementation's documented approval/sandbox settings
- Thread and turn identities exposed by the targeted protocol are extracted and used to emit
  `session_started`
- Request/response read timeout is enforced
- Turn timeout is enforced
- Transport framing required by the targeted protocol is handled correctly
- For stdio-based transports, diagnostic stderr handling is kept separate from the protocol stream
- Command/file-change approvals are handled according to the implementation's documented policy
- Unsupported dynamic tool calls are rejected without stalling the session
- User input requests are handled according to the implementation's documented policy and do not
  stall indefinitely
- Usage and rate-limit telemetry exposed by the targeted protocol is extracted
- Approval, user-input-required, usage, and rate-limit signals are interpreted according to the
  targeted protocol
- If client-side tools are implemented, session startup advertises the supported tool specs
  using the targeted app-server protocol
- If provider-native agent tools are implemented:
  - only the selected adapter's tools are advertised to the session
  - valid inputs execute host-side with configured adapter auth
  - the current normalized issue and `native_ref` are available as internal tool context
  - tracker secrets are not inherited by the coding-agent child process
  - invalid arguments, missing auth, and transport failures return structured failure payloads
  - unsupported tool names still fail without stalling the session

### 17.6 Observability

- Validation failures are operator-visible
- Structured logging includes issue/session context fields
- Logging sink failures do not crash orchestration
- Token/rate-limit aggregation remains correct across repeated agent updates
- If a human-readable status surface is implemented, it is driven from orchestrator state and does
  not affect correctness
- If humanized event summaries are implemented, they cover key wrapper/agent event classes without
  changing orchestrator behavior

### 17.7 CLI and Host Lifecycle

- CLI accepts a positional workflow path argument (`path-to-WORKFLOW.md`)
- CLI uses `./WORKFLOW.md` when no workflow path argument is provided
- CLI errors on nonexistent explicit workflow path or missing default `./WORKFLOW.md`
- CLI surfaces startup failure cleanly
- CLI exits with success when application starts and shuts down normally
- CLI exits nonzero when startup fails or the host process exits abnormally

### 17.8 Real Integration Profile (RECOMMENDED)

These checks are RECOMMENDED for production readiness and MAY be skipped in CI when credentials,
network access, or external service permissions are unavailable.

- A real tracker smoke test can be run with valid credentials supplied through the selected
  adapter's documented secret mechanism.
- Real integration tests SHOULD use isolated test identifiers/workspaces and clean up tracker
  artifacts when practical.
- A skipped real-integration test SHOULD be reported as skipped, not silently treated as passed.
- If a real-integration profile is explicitly enabled in CI or release validation, failures SHOULD
  fail that job.

## 18. Implementation Checklist (Definition of Done)

Use the same validation profiles as Section 17:

- Section 18.1 = `Core Conformance`
- Section 18.2 = `Extension Conformance`
- Section 18.3 = `Real Integration Profile`

### 18.1 REQUIRED for Conformance

- Workflow path selection supports explicit runtime path and cwd default
- `WORKFLOW.md` loader with YAML front matter + prompt body split
- Typed config layer with defaults and `$` resolution
- Dynamic `WORKFLOW.md` watch/reload/re-apply for config and prompt
- Polling orchestrator with single-authority mutable state
- Issue tracker adapter with state-list + ID-refresh reads
- Workspace manager with sanitized, collision-resistant per-issue workspaces
- Workspace lifecycle hooks (`after_create`, `before_run`, `after_run`, `before_remove`)
- Hook timeout config (`hooks.timeout_ms`, default `60000`)
- Coding-agent app-server subprocess client with the targeted transport/framing protocol
- Codex launch command config (`codex.command`, default `codex app-server`)
- Strict prompt rendering with `issue` and `attempt` variables
- Exponential retry queue with continuation retries after normal exit
- Configurable retry backoff cap (`agent.max_retry_backoff_ms`, default 5m)
- Reconciliation that stops runs on terminal/non-active tracker states
- Workspace cleanup for terminal issues (startup sweep + active transition)
- Structured logs with `issue_id`, `issue_identifier`, and `session_id`
- Operator-visible observability (structured logs; OPTIONAL snapshot/status surface)

### 18.2 RECOMMENDED Extensions (Not REQUIRED for Conformance)

- HTTP server extension honors CLI `--port` over `server.port`, uses a safe default bind host, and
  exposes the baseline endpoints/error semantics in Section 13.7 if shipped.
- Provider-native agent tools, when shipped, execute through the app-server session using
  host-side configured adapter auth without passing tracker secrets to the child.
- TODO: Persist retry queue and session metadata across process restarts.
- TODO: Make observability settings configurable in workflow front matter without prescribing UI
  implementation details.
- TODO: Extract common semantic helper tools only after multiple adapters demonstrate real
  duplication; do not preemptively replace provider-native tools with generic CRUD.

### 18.3 Operational Validation Before Production (RECOMMENDED)

- Run the `Real Integration Profile` from Section 17.8 with valid credentials and network access.
- Verify hook execution and workflow path resolution on the target host OS/shell environment.
- If the OPTIONAL HTTP server is shipped, verify the configured port behavior and loopback/default
  bind expectations on the target environment.

## Appendix A. SSH Worker Extension (OPTIONAL)

This appendix describes a common extension profile in which Symphony keeps one central
orchestrator but executes worker runs on one or more remote hosts over SSH.

Extension config:

- `worker.ssh_hosts` (list of SSH host strings, OPTIONAL)
  - When omitted, work runs locally.
- `worker.max_concurrent_agents_per_host` (positive integer, OPTIONAL)
  - Shared per-host cap applied across configured SSH hosts.

### A.1 Execution Model

- The orchestrator remains the single source of truth for polling, claims, retries, and
  reconciliation.
- `worker.ssh_hosts` provides the candidate SSH destinations for remote execution.
- Each worker run is assigned to one host at a time, and that host becomes part of the run's
  effective execution identity along with the issue workspace.
- `workspace.root` is interpreted on the remote host, not on the orchestrator host.
- The coding-agent app-server is launched over SSH stdio instead of as a local subprocess, so the
  orchestrator still owns the session lifecycle even though commands execute remotely.
- Continuation turns inside one worker lifetime SHOULD stay on the same host and workspace.
- A remote host SHOULD satisfy the same basic contract as a local worker environment: reachable
  shell, writable workspace root, coding-agent executable, and any required auth or repository
  prerequisites.

### A.2 Scheduling Notes

- SSH hosts MAY be treated as a pool for dispatch.
- Implementations MAY prefer the previously used host on retries when that host is still
  available.
- `worker.max_concurrent_agents_per_host` is an OPTIONAL shared per-host cap across configured SSH
  hosts.
- When all SSH hosts are at capacity, dispatch SHOULD wait rather than silently falling back to a
  different execution mode.
- Implementations MAY fail over to another host when the original host is unavailable before work
  has meaningfully started.
- Once a run has already produced side effects, a transparent rerun on another host SHOULD be
  treated as a new attempt, not as invisible failover.

### A.3 Problems to Consider

- Remote environment drift:
  - Each host needs the expected shell environment, coding-agent executable, auth, and repository
    prerequisites.
- Workspace locality:
  - Workspaces are usually host-local, so moving an issue to a different host is typically a cold
    restart unless shared storage exists.
- Path and command safety:
  - Remote path resolution, shell quoting, and workspace-boundary checks matter more once execution
    crosses a machine boundary.
- Startup and failover semantics:
  - Implementations SHOULD distinguish host-connectivity/startup failures from in-workspace agent
    failures so the same ticket is not accidentally re-executed on multiple hosts.
- Host health and saturation:
  - A dead or overloaded host SHOULD reduce available capacity, not cause duplicate execution or an
    accidental fallback to local work.
- Cleanup and observability:
  - Operators need to know which host owns a run, where its workspace lives, and whether cleanup
    happened on the right machine.

### Trusted candidate validation extension (local increment)

Version-1 workstream check gates may declare `evaluator: candidate_validation` and
inline required check/assertion/equals entries. The coordinator pins a validation
policy outside the candidate workspace, selects all applicable checks from the
final base/head diff, and blocks unknown scope or unavailable coverage adapters.
Development checks are feedback only. Final validation uses a clean committed
snapshot in an unprivileged isolated container, without producer credentials.

The service stores authenticated manifests and checked log artifacts outside the
disposable workspace. It verifies current source, candidate/base commit, pinned
policy/environment/service identity and task/run/attempt identity before deciding
a gate. Evaluator evidence and gate rationale are distinct records. Caller verdicts
cannot override required assertions. Failed validation rounds have a separate
persisted three-round limit from review fixes and transport retries. Workstream
restart preserves pinned policy and evidence; changed service policy blocks an
incompatible run. Existing command gates and WORKFLOW.md remain compatible.

This increment provides local durable receipts and adapters, not GitHub publication
or check enforcement, GCS archival, or Coverage Factory evaluators. See
[trusted validation](docs/trusted-validation.md) for implemented limits and commands.


## 21. Named factory agent qualification

Named agent definitions own model, reasoning effort, boolean Daybreak, subscription
authentication reference, permissions and instruction/skill references. Resolved agent
and resource SHA-256 digests are pinned per attempt. No separate profile object is used.
Legacy local definitions may inherit subscription authentication; explicit references
MUST equal the service's resolved worker reference. A reference is an identifier, never
credential bytes. This comparison does not attest the contents of an authentication file.

Before a named session starts a thread, verify ChatGPT account type, exact advertised
model, reasoning effort, access program and any explicit usage pause. Missing capabilities
or unsupported settings MUST block without switching models, efforts or authentication.
Set provider model fallback false; reject mismatched thread configuration and model reroutes.
Legacy unnamed `WORKFLOW.md` sessions keep their protocol behavior.

Normal named dispatch MUST reject Daybreak until effective execution is separately
observable and verified. An operator-only qualification probe may request an advertised
program, with one turn, 30-second RPC timeouts and a 90-second turn timeout. It has no
dynamic tools, network access or factory publication authority. Subscription-backed ordinary
success qualifies only the bounded task and verified thread configuration. Saved Daybreak
configuration, catalog eligibility and a successful response MUST NOT be treated as proof
of the effective access program. Unsupported or unobservable behavior yields an explicit
blocker. Receipts distinguish requested settings, configured settings, per-turn telemetry
(unknown fields stay null), limits and revision identities. They are investigation evidence,
not trusted candidate-validation receipts or permission to expand worker capacity.

Optional `secret_environment_names` supplied by the coordinator are additional validated
variable names to remove from local or SSH Codex child environments. They MUST be unioned
with existing exclusions, MUST NOT forward values, and MUST NOT weaken publisher/check
credential isolation. Qualification receipts exclude raw server diagnostics, credential
values, account IDs and reset-credit IDs; preserve bounded diagnostic codes instead.

## 22. Trusted Rig candidate publication

Version-1 workstreams MAY declare a fixed service-owned `publish` stage with
`id`, `inputs`, `outputs` and an inline gate. The gate contains `validation_stage`,
`required`, `success` and `failure: blocked`; the referenced upstream stage MUST
use candidate validation and its output MUST be among the publication inputs.
Definitions cannot supply publisher commands, credentials or their own verdict.
The synchronous local runner MUST reject publication without durable ownership.

Before every remote mutation, the service MUST authenticate the exact saved final
validation receipt, recompute both inline gates against the current clean candidate
and pinned policy, and verify immutable permanent archival for that validation attempt.
Development, missing, failed, forged, incomplete and stale evidence MUST block.
Later conversation events MUST NOT invalidate an already verified immutable evidence
revision; the latest incomplete bundle does not qualify as archival proof.

The coordinator MUST persist the stage operation before execution and the exact safe
publication intent before the first remote write. It MUST recheck current task ownership
before further writes. A stopped run MUST remain stopped while read-only reconciliation
records any already accepted branch/PR outcome; unchanged pending observations MUST NOT
produce repeated durable events. Publication
identity is bound to run, stage, candidate and validation receipt; a deterministic
task branch is preserved across retries and fixes. Pushes MUST target the exact
validated SHA with an expected-head lease. Restart, duplicate completion and lost
remote responses MUST reconcile the existing branch/PR rather than create a new PR.
Closed, ambiguous or unexpectedly changed remote state MUST block. Completion MUST
confirm the actual GitHub PR and branch SHA, not accept agent prose.

For contained workers, the coordinator MUST consume only a bundle from the exact
terminated successful service commit-preparation operation. The read-only worker RPC
MUST derive its path from a protected operation descriptor and authenticated input
identity, require a root-pinned producer command digest, and bound/check chunk size,
total bytes and checksum. It MUST reject arbitrary paths, running/failed/unrelated
operations and symlink substitution without executing candidate code as root. A
separate coordinator-owned snapshot MUST bound metadata copying and imported tree/file
sizes and verify candidate SHA and parent/base before
validation or publication, preserving the worker's original clone.

Publisher App/private-key credentials MUST stay outside agent and validator processes,
mounts and cloud metadata reach. Publication uses a selected-repository GitHub App
installation for RigCodeAI/rig; no PAT fallback, automatic review, protection changes
or merge is authorized by this stage. Preserve the repository template and all human
PR text outside the managed evidence section. The GitHub body API lacks atomic compare-
and-swap: observed body changes MUST block the update, and the remaining final PATCH
race MUST be reported as a deployment limitation.

Publish a durable PR link back to the Linear conversation and evidence links in the PR.
Human GitHub merge remains a separate boundary. Contract tests and source capability
MUST NOT be described as live Linear/GCP/GitHub acceptance. See
[trusted publication](docs/trusted-publication.md) for the operator plan and limits.

## 23. Trusted GitHub validation checks

A version-1 check stage MAY use `evaluator: github_validation` with inline
`validation_stage`, `required`, `success`, and `failure: blocked`. The referenced
candidate-validation stage MUST be a normal ancestor and supply a validation input.
The coordinator MUST recompute both contracts from authenticated final evidence,
verify permanent archival and reconcile the actual Rig PR head and current main
base. Failed selected executable assertions MUST NOT be averaged away by scores.
Candidate-owned definitions MUST NOT replace pinned policy.

A separately configured validation App MUST publish `factory/validation`. Its
credentials MUST be separate from publisher/worker/test credentials. Completed
aggregates MUST use success or failure, never neutral/skipped. Unknown remote or
archive outcomes MUST NOT qualify readiness. Check retries MUST reconcile the
expected App/head/contract identity before creating another check. The coordinator
MUST independently verify completion and recheck current evidence before protected
human waits advance. It MUST reconcile enrolled published waits through restart;
remote head/base changes invalidate qualification. Human merge remains external.

Repository protection MUST require the expected App and up-to-date branches, but
activation MUST wait until affected factory and non-factory PRs have qualified
validation paths. The source increment provides transports, evaluator and polling;
it does not install an App, activate protection, or prove live merge enforcement.
See [the rollout plan](docs/trusted-validation-check.md).

Validation-only service workstreams MAY start with registered candidate validation
when their stages contain only registered candidate/GitHub validation evaluators
and human waits. Generic executable-check entries and mixed agent/publish flows
MUST retain the normal entry restrictions. An empty agent map is supported only
for this validation-only case.

Protected human replies MUST be persisted before queued acknowledgement and
consumed exactly once after async current-validation verification. Stale evidence,
changed wait/artifact identity or stopped ownership MUST retain but not consume
the reply. Stage completion remote verification MUST run outside the coordinator;
the coordinator authenticates the domain-separated service seal and current local
proof, with a bounded observation age. GitHub reads MUST NOT block reply callbacks.

## 24. Optional local owner capture

The durable manual coordinator MAY expose a service-owned local capture action
through trusted startup options. It MUST NOT expose capture to candidate code or
treat it as restoration, publication, worker fencing or a gate verdict.

Admission MUST reject active or unresolved workers, outgoing publication,
cancellation, Linear activity, validation-check work and active or uncertain cleanup
jobs. Terminal status alone does not prove an external operation settled.

The coordinator MUST hold advancement and reject new mutating calls without
acknowledging unpersisted input. Both SQLite owners MUST enforce the same hold on
their write handlers, including direct evidence producers and future child-run
commits. The evidence outbox MUST be empty and the Evidence snapshots MUST match
the saved runs before capture starts. The original exclusive Store owner MUST
retain its connection and lock throughout capture. The coordinator MUST cancel
and invalidate its periodic validation timer while held and rearm exactly one
periodic chain after release. Expired or stale tokens MUST NOT restart it while
held or create another chain after release. Immediate reconciliation wakes MUST
remain separate from periodic scheduling.

Snapshots MUST come from those existing connections into fixed private files
outside issue workspaces. Completion MUST verify both standalone copies and
record their digests, schema versions, included identities and evidence watermark
before writing the local manifest last. Partial or altered captures MUST reject;
an ID MUST NOT be reused for a different boundary. Stale tokens and caller timeout
MUST NOT release a newer hold or an outstanding SQLite operation.

The local manifest MUST say `restore_ready: false`. A release revision supplied
by the operator MUST be identified as an operator pin, not proof of the deployed
binary. Provider reconciliation, remote archival, old-writer fencing, compatible
payload validation and a dispatch-held restore remain separate requirements.
See [the capture source and qualification boundary](docs/dev-242-capture-barrier.md).

### 24.1 Held recovery inspection

A trusted startup option MAY inspect an independently anchored local capture
through the existing coordinator without installing it. That branch MUST run
before normal Store initialization, Evidence recovery, Linear initialization or
worker/provider reconciliation. It MUST inspect the exact verified bytes in
memory, reject unsupported schemas and invalid correlated payload identities,
and preserve saved definitions, policy pins, waits, artifacts and counters.

Policy incompatibility MUST be visible without rewriting the saved run. The
inspection MUST remain dispatch-held on every restart, reject mutating calls,
and suppress all runtime replay/dispatch messages. It MUST NOT infer fencing,
worker termination, provider state or permanent archive availability from the
local image. It MUST NOT expose a dispatch-release action until those separate
proofs are supplied through a qualified service-owned contract.

This optional mode does not redefine ordinary application startup or the legacy
cloud backup verifier. See [held recovery inspection](docs/held-recovery-inspection.md)
for the isolated runtime-supervisor and full-service qualification boundaries.

An explicit operator plan MAY select that held mode at full application startup.
The plan MUST be private, bounded, duplicate-key checked and use fixed field
mappings. Startup selection MUST precede normal child configuration and MUST
omit mutable Evidence, provider credentials, web intake and normal scheduling
children. An invalid configured plan MUST fail without normal-runtime fallback.
CLI selection MUST reject a locally running runtime rather than imply it became
held. Application restart MUST retain the held selection. Logs MUST identify
held mode, compatibility and unavailable dispatch; ordinary startup without the
plan MUST remain compatible. Deployment boot selection, external fencing and
dispatch release remain separately qualified requirements.

Held inspection MAY accept the exact legacy policy inventory without upgrading
saved pins. Saved child definitions, parent/child lineage and paired atomic
handoff receipts MUST be correlated in memory. Saved child completion proof
MUST NOT authorize continuation or provider replay; proof freshness remains
unreconciled.

## 25. Independent review and same-PR repair

Version-1 check stages MAY use `review_checkout` or `review_publication` evaluators.
Both MUST reference an ancestor `github_validation` stage. A review agent MUST
declare its `review_checkout_stage`; checkout, agent and publication references
MUST form one normal success chain. Bounded review repair MAY return through the
normal validation/publication path to a distinct Cloud Agent, with at most three
completed unsuccessful review-fix rounds. Generic gate repair restrictions remain.

The service MUST match the current PR to the latest passed publication in the same
candidate context before preparation, model launch and submission. It MUST pin
current PR/base/head and the exact rubric before creating a separate standalone
reviewer checkout. Contained preparation MUST use a root-pinned command capability
that derives its destination from the run and stable operation identity. Only its
private review leaf MAY be writable; source, sibling tasks and credential roots
MUST remain read-only or inaccessible. Durable registration and validated release
MUST precede execution. Competing live or unknown writers for the same leaf MUST
block retry. Interrupted staging and unknown paths MUST be retained.
It MUST start a separate conversation and
retain its session independently from the Cloud Agent. Exact rubric JSON MUST be
validated in full against the committed head-side diff. Malformed, stale or any
unplaceable finding MUST block; no finding may be discarded to produce approval.

A separate selected-repository review App MUST submit reviews with verified bot
identity distinct from the PR author. Correct/no findings maps to APPROVE,
incorrect maps to REQUEST_CHANGES, and correct/with findings maps to COMMENT.
Actionable findings under any event MAY authorize repair. The service MUST verify
the submitted head/verdict/body/author and every inline comment, with current trusted
validation before and after submission. Replayed deliveries MUST reuse the stable
review identity or block ambiguity. They MUST NOT create concurrent fixes.

Review-authorized repair MUST retain the same run, PR, branch, original Cloud Agent
definition and real publication history. The coordinator MUST append a sealed
immutable candidate context and link each new attempt to it; previous execution
pins, attempts and receipts MUST NOT be rewritten. The context MUST bind verified
prior publication/review attempts, exact PR identity, trusted current-main validation
base and the separate prior published head used for direct-parent transfer lineage.
For a repair, the candidate MUST have the exact prior published head as its sole
direct parent, and the separate trusted validation base MUST be an ancestor of
that candidate. An incompatible main advance MUST block; the service MUST NOT
substitute the parent for the validation base. Protected producer inputs and their
digest MUST bind the same context ID, parent and validation base, while legacy
producer and receipt shapes remain unchanged. Publication MUST retain the context's
exact authenticated PR identity, including when recovering an already pushed head.
Signed validation identities MUST bind the new context. Cross-context receipts,
context tampering or any main advance invalidate readiness; ancestry alone MUST NOT
requalify a receipt for an old base. Legacy receipt identity shape remains unchanged.

Only completed fix attempts consume review rounds. Transport retry, duplicate delivery,
restart and model changes MUST NOT reset them. Disagreement, the three-round limit
or invalid evidence MUST retain receipts and escalate to a human. Review approval,
quality score and Linear replies MUST NOT authorize merge. Rig feedback ownership
MUST be released as one coordinated cutover so one system owns factory branch fixes.
See [the source contract and unqualified live boundaries](docs/review-repair.md).

Retained completion evidence MAY read an attempt's historical authenticated
candidate context without changing the current selector or execution authority.
The read MUST verify immutable context lineage and the attempt's position in it.
Version-2 service completion seals MUST bind that context, exact external-worker
identity and captured input producer identities. They MAY cover review checkout
and submission checks, but MUST NOT waive fresh external-worker termination.
Typed reviewer text MUST match a submitted authenticated report linked to the
same reviewer and checkout attempts. Locations remain annotations, not file
contents. Cleanup MUST require the latest publication and its archived validation
to share the current authenticated context. Review directory deletion MUST remain
disabled until its ownership contract is independently qualified.

## Bounded child workstream handoff

A version-1 parent MAY declare one `child` stage with `id`, `inputs`, `outputs`,
relative `workstream`, complete `input_mapping` and `next`. The child MUST be a
pinned `software-change` definition without child stages. It MUST transition to
a check stage for independent re-evaluation, never directly to parent completion.
Child definitions, agents and referenced resources MUST be pinned with the parent.

The coordinator MUST bind exactly the declared parent values and validate a separate
prepared workspace. Child identity MUST bind parent run, stage attempt, input and
definition/execution fingerprints. Parent wait, child creation, linkage, operation
records and evidence events MUST commit atomically. Replay MUST return the same stored
child; conflicting fingerprints, stale parent state and ownership aliases MUST reject.
The parent wait MUST release execution capacity. Shared capture holds MUST prevent
handoff writes and dispatch/cancellation callbacks during a consistent capture.

Stopping the parent MUST prevent owned-child dispatch and propagate through the
existing cancellation/reconciliation owner, including after restart. Unknown child
termination MUST remain unresolved. Late results MUST retain evidence without making
a stopped parent ready. Cleanup MUST retain output evidence needed by the parent.

Child completion, a final human reply and matching lineage IDs MUST NOT certify
GitHub merge or coverage. Resume MUST consume the service-owned verified merge/revision
and retained-evidence receipt for the exact linked child before independent re-evaluation.
The coordinator MUST obtain that proof outside its callbacks, recheck exact parent/child
snapshots, and complete the saved waiting attempt once before making the declared check
stage ready. Capture MUST reject active proof tasks and hold result callbacks. Later
parent cancellation MUST NOT cancel a child whose handoff already completed. Retained
output production MUST use the service's typed, permanently archived references.
Parent result values MUST come from verified retained bytes, with a complete output
map and matching original producer identities; raw workspace-dependent values MUST
NOT be copied into the parent. Positive runtime contract verification uses synthetic
remote receipts with real local storage. Native owned-child work MUST require a fresh,
short-lived service grant from the parent's acknowledged Linear assignment. The grant
MUST bind the issue/session/assignee, assignment revision, parent stage attempt, exact
child identity, child stage attempt and candidate context, and both pinned
definition/execution contexts. Child agent settings
MUST match the parent's selected Cloud Agent, and its separate clone MUST come from
trusted configuration. Expiry, restart, stop and reassignment MUST revoke the grant.
Unmatched child IDs MUST fail closed regardless of task ID prefix. Creation, child
dispatch/recovery and parent proof acceptance MUST recheck that exact grant. This
source contract does not qualify native Coverage intake, child question/reply routing
or live external execution.
Synthetic fixtures MUST NOT authorize production publication or support claims.
Coverage Factory remains the owner of scenario/oracle approval, specimen qualification
and trusted scoring. See [child workstreams](docs/child-workstreams.md) for current limits.
