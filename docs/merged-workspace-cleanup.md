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

The shipped `software-change` definition does **not** currently qualify for
deletion. Its `candidate_validation` stage uses `CandidateTransfer.sync` followed
by the local trusted `Validation.execute` adapter, without registering a systemd
external-process identity. The current all-checks quiescence gate therefore
retains that run. Recognizing supported service-only checks with authenticated,
attempt-bound completion evidence is a missing integration distinction, not
proof that an unknown check terminated. DEV-239's service GitHub-check adapter
needs the same explicit treatment after it merges. Agents and arbitrary
executable checks must keep their fresh contained-termination requirement.

The shipped candidate outputs also contain opaque strings that the supported
output contract below retains. The usable source boundary is read-only
merge/archive proof plus the owned-removal/retry integration exercised by
contained-operation contract fixtures. The shipped end-to-end cleanup path is
not enabled or qualified by this PR.

Admission denies equal and ancestor/descendant workspace overlap with any recorded
run. Cleanup jobs stay tracked by the sole coordinator; another task cannot claim
an overlapping path during observation or deletion.

## Merge, archive and deletion

Only a completed passing publish attempt with the matching durable publication
intent qualifies. The current GitHub response must name the same Rig PR, branch
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
  repository: "RigCodeAI/rig",
  pr: %{number: 123, url: "https://github.com/RigCodeAI/rig/pull/123"},
  published_head: "40-character SHA",
  merge: %{commit: "40-character SHA", merged_at: "ISO8601", merged_by: %{"id" => 1, "login" => "user", "type" => "User"}},
  archive: %{run_id: "recorded run", task_key: "digest", revision: "digest", manifest_sha256: "digest", artifacts: []},
  retained_outputs: %{}
}
```

For a child with a parent, every declared output needs an immutable retained
artifact reference in `run.retained_output_artifacts`, keyed by output name:

```elixir
%{run_id: child.id, task_key: Evidence.task_key(child.task_id), revision: revision,
  name: artifact_name, sha256: artifact_sha256, output_id: child.artifacts[output_name]}
```

`Evidence.retained_artifact/2` verifies the exact receipt and bytes before returning
them. The payload must decode to the child's saved output and match its artifact
identity. Workspace-dependent values and file/path fields block this JSON-output
contract; binary/file outputs need their own retained-content contract. The
parent can read the immutable output after child deletion. Registering these
references in a real parent lifecycle is DEV-243's integration boundary.

Deletion currently supports the known service-owned publication metadata and
JSON containers of numeric, boolean or null values. Other string-valued outputs
may name files even without an extension, so they retain the workspace until a
typed retained-content adapter supplies actual bytes. This restriction also
applies to top-level runs. It does not block read-only merge/archive proof: that
API verifies any supplied immutable references independently, and consumers must
enforce their own output-content contract. JSON path text is never proof that the
referenced file was retained.

## Capture boundary

`Orchestrator.cleanup_active_or_uncertain?/1` is true for any active cleanup job
or pending deletion with an uncertain outcome. DEV-242's coordinator capture hold
must stop new observation/deletion scheduling and the final ownership check.
The source checks a non-nil `workstreams.capture`; combined validation against
DEV-242's merged capture APIs remains required.

## Verification

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

Checks passed in the isolated, network-disabled qualified image
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
