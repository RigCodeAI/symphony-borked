# DEV-237 acceptance record

Date: 2026-10-10. Branch: `cycle/dev-237-private-evidence`. Base:
`6f1822cd7cfe45c6bb2733567c3380d1d7ea01f2`.

Status: the original implementation is preserved at `54f13a5`. The current
candidate combines it with deployed DEV-235 source `3667340`, preserving live
routing and adding explicit selection history. The human approved staging,
activation and disposable tests on 2026-10-10. No DEV-237 deployment or PR has
been performed yet; ticket acceptance remains incomplete.

The user selected `factory.rig.ai` and resource-scoped IAP access for
`domain:rig.ai`. This work consumes the existing project, archive bucket and
budget; it does not propose another IAM binding or budget.

## Acceptance boundaries

| Ticket behavior | Current evidence | Remaining acceptance |
| --- | --- | --- |
| Linear link opens live/completed conversation and test evidence | Stable task URL, synchronous redacted event capture, HTML viewer and artifact downloads implemented; local store tests pass | Signed-in live task demonstration; shared hooks now pass local integration tests |
| Unauthorized access denied; credentials redacted before persistence | Real ES256 verifier tests cover wrong signature/issuer/audience/domain, expiry, malformed/duplicate headers and unavailable keys. Real SQLite/outbox tests cover fixture credentials and exclusion of reasoning/image data. Real HTTP routes verify signed access, unsigned/unauthorized denial, escaped HTML and old artifact retrieval | Unauthorized GCP pilot request and allowed Google identity demonstration |
| URL survives workspace deletion; failure cannot qualify | Real store restart and deleted-workspace test retains transcript; exact current root receipt is required for cleanup. Upload retry/corrupt readback/conflict tests pass | Live task archive/readback, workspace deletion through the actual cleanup owner, then retained URL inspection |
| Retry vs stage, child links and original definitions | Contract run includes failure/retry and approval wait, parent/child navigation, separate later run pins, explicit same-run reassignment history and immutable old snapshot; replay cannot regress current view | Inspect the live sample; combined DEV-235 selection history passes local integration; child links may use contract data |

An existing browser session reached the currently deployed observability dashboard
at the chosen domain. That is a baseline only: it does not prove this viewer,
signed-JWT validation, new transcript capture or DEV-237 acceptance.

## Local checks

Host lacks Elixir/Mix. Checks use a separate local `dev237-check` container with
Elixir 1.19.5 / OTP 28.5, one CPU and 3 GiB. No other ticket's build container or
live worker was used. Commands below run from this checkout unless noted.

- `docker exec dev237-check mix compile --warnings-as-errors`: passed.
- `docker exec dev237-check mix specs.check`: passed.
- `docker exec dev237-check mix format --check-formatted <changed Elixir files>`: passed.
- `docker exec dev237-check mix test test/symphony_elixir/evidence_test.exs test/symphony_elixir/evidence_access_test.exs test/symphony_elixir/evidence_http_test.exs`: final source, 12 tests, zero failures. Includes interrupted-export orphan recovery and the actual HTTP endpoint.
- `docker exec dev237-check mix test`: 537 tests, zero failures, six skipped and ten excluded, an earlier module-only checkpoint before shared hooks. The first run lacked the shipped `factory/workstreams/software-change.yaml` fixture in the container and failed one test; copying `factory/.` fixed the environment and both the focused case and full rerun passed. This historical result is superseded by the shared-integration checks below.
- `python3 -m unittest discover -s factory/deploy/tests -p 'test_*.py'`: 92 tests, passed, 13 skipped.
- Uploader-specific subset: 13 evidence archive tests; cloud I/O subset: 22 tests.
- `bash -n factory/deploy/maintenance.sh`, `python3 -I factory/deploy/cloud_io.py --help`, Python compilation and `git diff --check`: passed.
- `mix credo --strict`: nonzero. It reports existing and new readability/refactoring findings, including the repository formatter's 200-column lines versus Credo's 120-column guidance, nesting/complexity and HTML string style. It is not recorded as a clean lint result. Root alpha guidance makes broad CI non-blocking; focused failures still block.

## Shared integration checks after slot release

- `docker exec dev237-check mix test` with the evidence, store, durable workstream
  and native Linear test files: **79 tests, zero failures**. The exact file set is
  `evidence_capture_test.exs`, `evidence_runtime_test.exs`,
  `evidence_outbox_test.exs`, `evidence_http_test.exs`, `evidence_test.exs`,
  `evidence_access_test.exs`, `workstream_store_test.exs`,
  `durable_workstream_test.exs`, and `linear_delegation_test.exs` under
  `test/symphony_elixir/`.
- Real durable retry/wait/restart, observable callback capture, pre-persistence
  redaction, missing-store failed attempt, transactional outbox rollback/order,
  explicit pin reassignment replay and managed URL publication pass locally.
- Authenticated validation manifest/log import binds stage/attempt and separate
  source/stored digests. Replayed completed import survives deletion of its
  original archive and excludes the producer key. Receipts use contract fixtures;
  this is not live trusted-validation qualification.
- `mix compile --warnings-as-errors`, `mix build`, `mix specs.check`, and whole
  project `mix format --check-formatted`: passed. The build is a local development
  escript; production deployment still builds on the intended machine.
- Python deployment suite rerun: **92 tests, zero failures, 13 skipped**. Shell
  syntax, isolated Python helper invocation, local doc links and diff checks pass.
- Broader tests found and repaired a test configuration leak, then a fresh-BEAM
  compatibility bug: evidence omission must work without an initialized global
  workflow store or valid unrelated tracker credentials. The 79 focused tests
  include both affected paths. Final broad results are recorded below.
- `docker exec dev237-check mix test --cover`: **549 tests, zero failures, six
  skipped and ten excluded**. Command exits 3 because total coverage is 79.08%
  against the configured 100% threshold. This is passing behavior tests with a
  nonpassing coverage gate. The threshold was not changed.
- `mix dialyzer --format short`: exits 2 with three pre-existing findings in
  `WorkstreamRun` and `WorkstreamStore`. An unreachable fallback in the new IAP
  verifier was removed; its focused authentication/HTTP tests were rerun. Broad
  coverage preceded this removal of unreachable code. No existing warnings or
  gates were suppressed.
- `mix lint`: `specs.check` passes; Credo remains nonzero (128 refactoring,
  115 readability and 11 design findings, including new evidence code). This is
  a nonpassing alpha CI result, not a claim of clean lint.

Read-only GCP queries confirmed project `292978199748`, IAP-enabled backend
`7237567286073591668`, and its existing resource-scoped `domain:rig.ai` accessor
binding. They do not prove a deployed viewer. Exact inputs and migration rollback
steps are in [the deployment plan](deployment-plan.md).

## Concise self-review

- Every private route has the signed-IAP verifier; downloads use the same boundary.
  Unsigned email headers and invalid/missing verification inputs fail closed.
- Secrets are redacted before SQLite/outbox writes; hidden reasoning/raw protocol
  data is excluded. This cannot detect every possible arbitrary prose secret.
- The coordinator owns one local index; events carry run/stage/attempt identities.
  Exact duplicate event IDs do not mutate the current snapshot; conflicting
  duplicates reject transactionally.
- Reassignment requires an explicit previous/current pin transition with fixed
  task/policy identity. Snapshot history and old immutable bundles retain original
  definitions across reassignment and restart.
- Root-owned current receipts qualify only after immutable cloud upload and exact
  byte readback. New data invalidates an older receipt; failed/pending/corrupt
  evidence cannot permit cleanup. Caller must separately confirm merge and worker
  termination.
- Store/bundle/index paths reject symlinks and hard-linked files. Uploader opens
  validated ancestors without following links. Index backup uses online SQLite
  backup, including committed WAL data. Export removes only regular, single-link
  files with its exact temporary-name pattern left by an interrupted write,
  before publishing/revalidating the manifest. The focused restart test covers
  that recovery. Existing upstream config stays disabled.
- Run/event/outbox writes share one SQLite transaction; replay is ordered and
  duplicates cannot regress snapshots. The coordinator drains before dispatch or
  acknowledgement, including after restart. A failed drain retains pending data
  and blocks advancement. Event/combined serialized size limits align with the
  evidence index so a commit cannot enqueue an oversized unreplayable snapshot.
- Current shared capture and native URL hooks pass local behavior checks. The
  combined DEV-235 reassignment owner now adds and drains the identity envelope.
  The later managed cleanup owner remains separate and is not implemented here.

## Remaining live acceptance

Follow [the prepared deployment plan](deployment-plan.md): approved package
staging, a protected new per-revision workflow, compatible migration/rollback,
service activation, allowed/unauthorized IAP requests, one retained native task,
archive failure/retry/readback and workspace deletion with the actual separate
cleanup prerequisites. No new app, IAM binding, budget or model credential is
required. The human approval covers this concrete deployment plan and disposable live tests.
Additional DEV-235 app installation remains pending its own passkey step.

No live acceptance step is inferred from local tests or the existing dashboard.
Only after ticket boundaries pass should a real PR be opened for human merge.

## Combined deployed-source qualification

- Candidate includes actual deployed DEV-235 commit
  `366734057481aa2c160319f05205e8e7f582a30e` and preserves checkpoint `54f13a5`.
- Central container compilation with warnings as errors and `mix specs.check` pass.
- Focused evidence, durable run, cancellation, store, mapping, routing, HTTP,
  delegation and OAuth tests: **123 tests, zero failures**.
- Actual `AgentMapping.reassign` changes require a matching prior/current envelope
  including `agent_selection` and stage bindings. Silent changes reject; earlier
  bundles retain their exact bytes after restart. Evidence-disabled routing works.
- Python deployment suite: **97 tests, zero failures, 18 skipped**.
- Whole-project format and `mix build` pass. `make all` stops at Credo (149
  refactoring, 131 readability, 17 design findings). The broad coverage/Dialyzer
  results are below; no gate is represented as clean.
- Live preflight confirms active coordinator `3667340`, five retained stopped
  runs with trusted termination recorded, no active worker operation and no
  pending public config. Protected config remains root-owned 0600; public 0644.
  The running store uses exclusive SQLite locking, so schema inspection/backup
  must occur after stopping dispatch. The read-only attempt returned locked and
  changed no state.

- Combined broad behavior run (`mix test --cover`): **577 tests, zero failures,
  six skipped, ten excluded**. Command exits 3 at 79.54% coverage against the
  unchanged 100% threshold.
