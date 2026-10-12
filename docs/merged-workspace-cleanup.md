# Merged workspace cleanup (DEV-241)

This slice connects the existing publisher and permanent evidence index to the
existing coordinator's cleanup loop. Scoped compilation and contract tests passed.
It is not a deployment or a live deletion receipt. Cancellation
snapshots, the proposed seven-day retention and live acceptance remain gated.

## Ownership and activation

Cleanup is disabled by default. A trusted coordinator owner can configure
`merged_cleanup: true` in its startup options; `cleanup_interval_ms` and
`cleanup_retry_ms` default to 60,000. These are service startup options, not
workstream YAML, agent tools or a new scheduler. There is no deployment change in
this slice. The periodic loop also runs while the ordinary tracker poller is
disabled, so an idle completed task can be reconciled after a missed merge event.

New runs enrolled by that coordinator record the exact task/run owner, local
directory and, when configured, corresponding SSH worker directory. Each record
contains its pinned physical root, path, host, and root/ancestor/target directory
identities and creation times. Owned-resource enrollment/removal also requires
Linux filesystem `statx` creation-time support, or macOS birth-time support;
unavailable identity support retains work. [Linux's descriptor-based statx API](https://man7.org/linux/man-pages/man2/statx.2.html)
supplies this creation time. Python 3.11+ and the descriptor/mount guards from
[the deletion-boundary slice](dev-241-deletion-boundaries.md) are required. The
same directory identity is checked again before hooks and deletion. A replaced
tree is retained, even if its pathname is unchanged. A missing original target
can reconcile after a lost response; a recreated target cannot.

Existing runs without resource records retain their work. Cleanup does not
retrofit authorization from their paths, derive sibling review paths, scan
workers, remove publication branches or close PRs. Unrecorded review and candidate
transfer directories remain retained until their lifecycle owners provide an
explicit registration contract. Current-root changes and old service-policy pins
also retain work; stored pins are never rewritten to match a new build.

The coordinator records a durable `service_completion` attestation only after
recomputing an authenticated `candidate_validation`, `github_validation`,
`review_checkout` or `review_publication` gate during active completion.
Version 2 seals bind the run/task, pinned policy/definitions/execution, stage,
attempt, result, gate, authenticated candidate context, external-worker identity
and exact input producer identities. Captured inputs remain fixed when later
attempts replace the current output map. Legacy version 1 seals remain readable
only for their original context-free validation contract, without an external
worker. Cleanup authenticates seals with the service-only validation key;
it does not require an open PR or a still-present candidate checkout. Invalid
receipts and observations produce no seal. Authenticated failed candidate checks
can also record completion, without becoming passing validation decisions.
Unknown agents and executable checks still require fresh contained termination.
An external operation on a sealed service check also requires fresh containment
proof; the service seal cannot replace it.
Every executed attempt must retain its matching operation record; an absent
record is uncertainty, not evidence of termination.
Old runs with no seal retain their directories; there is no retrofit.

The shipped flow's supported service checks and known outputs now have cleanup
contracts. Actual worker termination, GitHub merge and permanent upload receipts
remain required. The shipped end-to-end path has not been demonstrated live.

Admission denies equal and ancestor/descendant workspace overlap with any recorded
run. Cleanup jobs stay tracked by the sole coordinator; another task cannot claim
an overlapping path during observation or deletion.

## Merge, archive and deletion

Only a completed passing publish attempt with the matching durable publication
intent qualifies. Its context must be current and match the exact archived
validation attempt. The current GitHub response must name the same Rig PR, branch
and candidate head, target `main`, and report a merge commit, time and a user
account merger. Closed-but-unmerged PRs, changed heads, missing metadata, Bot
accounts and bot login suffixes deny cleanup. Symphony performs read-only GitHub
requests here and never merges. GitHub account metadata cannot prove whether a
person clicked the button or automation used that person's credentials.

A completed run can qualify. An unanswered wait qualifies only when it explicitly
consumes `publication`, goes directly to completion, and is neither an approval
nor a clarification. Other waits, active stages, pending questions and stopped
runs retain their work. A verified GitHub merge resolves only that final
publication wait; it does not answer arbitrary human questions.

Before deletion the coordinator:

1. Requires fresh termination evidence for every recorded external operation.
   Only the exact systemd operation's contained termination qualifies. Missing,
   local process-group, running and unknown identities fail closed. An exited
   BEAM task or a completed stage is not enough.
2. Persists a sanitized deletion intent with publication/merge identities,
   resource ownership, external identities, retained parent outputs and service
   policy. Further dispatch remains stopped.
3. Requires a new permanently verified archive for the exact current snapshot,
   including that intent and the deletion claim. The earlier publication receipt
   cannot authorize cleanup. Local archived bytes are rechecked against hashes
   and the root uploader's receipt.
4. Rechecks the archive, worker proof and current coordinator ownership before
   removing each resource through the descriptor guard. The current configured
   root remains the authorization boundary.
5. Commits the result to the same durable run. Failures remain pending, with
   bounded periodic attempts. Restart retries the saved intent through all gates.
   A lost deletion response does not permit deleting a replacement tree.

Legacy cleanup triggered only by a Linear terminal state now retains work. It has
no durable resource, GitHub merge or archive proof. Automatic Linear Done updates
and GitHub webhook intake are not added here; periodic GitHub observation supplies
the missed-event path. These remain separate full-ticket integration boundaries.

The viewer/index and immutable archive are outside disposable workspaces. A
cleanup result creates another evidence revision that the existing uploader must
archive; the pre-deletion intent revision stays retained throughout.

## Read-only composition result

`WorkstreamCleanup.merge_proof(run, options)` independently reads GitHub and
verifies publication and current evidence archives. It does not remove a
workspace and does not depend on cleanup succeeding. DEV-243 can consume this
service result from the recorded child run, rather than agent-authored prose.
The result schema is:

```elixir
%{
  version: 1,
  run_id: "recorded run",
  task_id: "recorded task",
  publication_id: "recorded publication digest",
  publication_attempt_id: "recorded publish attempt",
  candidate_context: %{candidate_context_id: nil, validation_base: "pinned base", repair_parent_sha: nil, lineage_sha256: "digest"},
  repository: "RigCodeAI/rig",
  pr: %{number: 123, url: "https://github.com/RigCodeAI/rig/pull/123"},
  published_head: "40-character SHA",
  merge: %{commit: "40-character SHA", merged_at: "ISO8601", merged_by: %{"id" => 1, "login" => "user", "type" => "User"}},
  archive: %{run_id: "recorded run", task_key: "digest", revision: "digest", manifest_sha256: "digest", artifacts: []},
  retained_outputs: %{},
  retained_outputs_complete: false
}
```

For a child with a parent, every declared output needs an immutable retained
artifact reference in `run.retained_output_artifacts`, keyed by output name:

```elixir
%{run_id: child.id, task_key: Evidence.task_key(child.task_id), revision: revision,
  name: artifact_name, sha256: artifact_sha256, output_id: child.artifacts[output_name]}
```

`WorkstreamCleanup.retain_outputs(run, options)` produces version 2 references.
It writes redacted projections through Evidence, then waits for a verified
permanent receipt for those exact bytes. An unavailable upload visibly retains
the workspace. Cleanup stores the references with its durable intent. DEV-243
can use this producer independently of deletion and persist the returned map
through its existing owner before consuming read-only merge proof.

A version 2 reference adds `version: 2` and `kind`. Its JSON payload is:

```json
{"version":2,"kind":"agent_metadata","output_id":{"run_id":"run", "attempt_id":"attempt", "sha256":"original output digest"},"value":{"thread_id":"thread"}}
```

The service derives the type from the pinned producer stage and exact output
identity. Supported types are `agent_metadata`, `repair_agent_metadata`,
`review_agent_report`, `command_log`, `candidate_validation`, `github_validation`,
`review_checkout`, `review_publication`, `publication` and `inline` numeric,
boolean or null data. Agent metadata removes the disposable workspace locator
and accepts only the Runner's known ID/model fields. Service validation outputs
require the authenticated completion seal. Unknown fields and output identities
fail closed. Redacted projections retain their original source digest without
claiming byte equality to secret-bearing originals.

Ordinary metadata may include the Runner's pinned `validation_base`. Repair
metadata must match the attempt's authenticated context ID, validation base and
prior published parent, with its bounded feedback fields. Historical context
reads verify signed lineage and the attempt's original position; they never
change the current selector or authorize execution of an older candidate.
Reviewer JSON must match the exact authenticated submitted report, reviewer IDs
and captured checkout input. The report retains text and validated source
annotations without exporting source files. Checkout projections remove their
disposable root, workspace and source-workspace locators. These reads need no surviving checkout.
Review directories remain unregistered and retained until the broker ownership
contract is qualified; typed outputs do not authorize their deletion.

`command_log` retains actual stdout text. A printed bundle or report filename
remains text; it grants no file handle and promises no file contents. Candidate
bundle transport is not an exported retained binary artifact. Unknown structured
file outputs, binary data and unsupported producer types retain the directory.
Consumers requiring files must use a separately verified file-content contract.

`Evidence.retained_artifact/2` verifies the exact receipt and bytes.
`WorkstreamCleanup.retained_output(run, name, options)` additionally verifies
the typed projection and original output identity before returning its envelope.
This works after child deletion. Legacy references still require exact JSON
output equality and reject workspace/file fields. Read-only `merge_proof/2`
never produces references or deletes data; consumers enforce required output
names and accepted content types before advancing their own lifecycle.
`retained_outputs_complete` reports whether verified references cover the entire
current artifact map. A partial map can still accompany a read-only merge proof;
it never qualifies for parent-dependent deletion or parent advancement.

## Capture boundary

`Orchestrator.cleanup_active_or_uncertain?/1` is true for any active cleanup job
or pending deletion with an uncertain outcome. DEV-242's coordinator capture hold
must stop new observation/deletion scheduling and the final ownership check.
The source checks a non-nil `workstreams.capture`. DEV-242's capture APIs are
merged in PR21; combined qualification of this follow-up remains pending.

## Review-output follow-up verification

This follow-up is based on merged PR29 `1362cb463a0d65ea82a50b08b43afc4fdf2ffb45`.
The final isolated run passed 130 tests with zero failures (seed `658343`). It
covered review outputs, candidate context history/tampering, cleanup, authenticated
validation, review execution, durable runs, capture ownership and child result
consumers. The new tests use real stage completion, SQLite restart and retained
artifact bytes with synthetic review and root-upload receipts. They remove only
the temporary main workspace and leave the review checkout retained.

Warnings-as-errors compilation, full formatting and public specs passed in the
same immutable image documented below. All 30 executor source pins remain present.
The strict deployment helper executor pin is refreshed from the same complete
30-source union; its lifecycle, child and validation pins are unchanged. The
helper still rejects saved runs with incompatible policy and never rewrites them.
The additional pin refresh passed warnings-as-errors compilation, full format,
public specs and 9 focused tests (seed `61075`) on source `086db851`:

```sh
mix test test/symphony_elixir/factory_secret_name_migration_test.exs \
  test/symphony_elixir/review_output_cleanup_test.exs
```

This includes the three offline credential-name migration tests, fresh-VM replay,
policy mismatch rejection, active-operation rejection and review-output identity
checks. The implementation modules are unchanged from the earlier 130-test
snapshot. Hosted PR31 CI on `e4c3b937` passed build, formatting, public specs,
CodeQL and the PR body; broad `make-all` stopped at Credo (285 refactor, 348
readability and 18 design findings). Coverage and Dialyzer were not reached.
The new completion seal binds the contained-preparation result fields from PR29;
no broker deployment, live review-directory enrollment or deletion is qualified.
Earlier fixture setup failures were fixed before the final passing run. Broad
hosted CI is reviewed separately; source checks do not establish live acceptance.

## Earlier validation-output verification

The service-completion and version 2 output follow-up is qualified on merged
DEV-239 revision `9fb13f8acf701f444e7a8a6f6e323193632a9d17` and DEV-242 revision
`797c61464e2e835100218233feee3041eebf6102`. Warnings-as-errors compilation, full
formatting, public specs and PR-body checks passed in the same isolated image
below. The final combined run reported 79 tests, zero failures and one excluded
Docker validation test (seed `879902`). It covered cleanup, authenticated
validation, lifecycle, publication, durable archival and capture ownership.

New checks prove sealed service completion through store restart and checkout
removal; altered attempts, execution pins and missing keys fail closed. Both local
and registered remote command-log shapes are supported. Typed metadata/text
requires actual archived bytes, survives owned-directory removal and rejects
forged types, changed source identities and unsupported file fields. The earlier
removal retry and coordinator restart checks passed in this combined run.

The scoped review found no concrete seal replay or generic-worker termination
bypass. Read-only merge proof now explicitly reports retained-output
completeness, while the parent deletion gate requires every artifact reference.
This is source/contract qualification, not a live shipped-workflow demonstration.
The container is removed and the serialized build slot is released. Broad hosted
CI is inspected separately; the receipts below describe the earlier PR19 slice.

The combined follow-up command was:

```sh
mix test test/symphony_elixir/workstream_cleanup_test.exs \
  test/symphony_elixir/validation_check_test.exs \
  test/symphony_elixir/workstream_run_test.exs \
  test/symphony_elixir/validation_lifecycle_test.exs \
  test/symphony_elixir/workstream_publication_test.exs \
  test/symphony_elixir/durable_publication_test.exs \
  test/symphony_elixir/evidence_publication_test.exs \
  test/symphony_elixir/backup_capture_owner_test.exs \
  test/symphony_elixir/backup_capture_test.exs
```

The focused suite is `test/symphony_elixir/workstream_cleanup_test.exs`. It uses
real temporary directories, immutable evidence files, SQLite and coordinator
restart. GitHub responses, termination outcomes and uploader receipts are explicit
contract fixtures. They do not prove a GitHub merge, cgroup termination, GCS
upload, IAP access or a live parent workflow.

In a disposable development environment with the pinned runtime and dependencies:

```sh
cd elixir
mix compile --warnings-as-errors
mix test test/symphony_elixir/workstream_cleanup_test.exs
mix test test/symphony_elixir/workspace_removal_safety_test.exs
```

PR19 checks passed in the isolated, network-disabled qualified image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
with Elixir 1.19.5, OTP 28 and Python 3.13.5. The checkout was copied into the
container without shared source/build mounts.

- `mix compile --warnings-as-errors`, changed-file format and `mix specs.check`
- Cleanup suite: 16 passed, including surviving-helper restart and one-hook proof
- Removal safety, GitHub transport, evidence/publication and durable publication: 35 passed
- Narrow core terminal-cleanup regressions: 2 passed
- `mix pr_body.check`: passed

These are 53 distinct focused tests. The final reference-proof change reran all
16 cleanup tests plus compile/format/spec checks; related unchanged regressions
had already passed. A separate actual-helper Linux smoke passed resource
inspection, replacement denial, simulated inode reuse, restoration and absent
retry. Initial test setup and a nil-boolean restart defect were repaired before
the passing run.

Scoped Luna/max review found archive binding, output retention, asynchronous
capture coordination and surviving-task restart risks. Exact snapshot/byte
verification, unsupported-output retention, capture fencing/predicate and task
tracking address those source seams. Combined capture admission and the actual
DEV-239/242/243 adapters still require validation after their source merges.
No broad local suite was run under the scoped slot. Fixtures and the owned
container were removed; the slot is released. No live deletion, deployment,
GitHub mutation or cancellation retention approval has occurred.
