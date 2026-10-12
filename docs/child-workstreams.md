# Bounded child handoffs (DEV-243)

Status: source integration on merged PR22 (`4b33f5d5e99032fe1cd06e6070108faa7008e145`),
including cleanup, capture and typed output retention APIs. The final selected pass
contains 105 tests with zero failures across child contracts, durable execution,
Store, cleanup and capture. Compilation with warnings treated as errors, formatting
and public-function specs pass in the qualified isolated Docker runtime. The positive
runtime proof-to-resume contract passes with real SQLite and immutable local bytes,
using synthetic GitHub and remote-upload receipts. This is not live Coverage Factory qualification,
native child dispatch, or a deployed workflow.

The first increment admits one child stage in a parent definition. Its referenced
definition must be named `software-change` and cannot contain another child. The
loader pins the child file, agents, instructions and skills with the parent. Parent
execution does not reread those files to select a newer child definition.

```yaml
- id: implement-gap
  type: child
  inputs: [approved-gap]
  outputs: [child-result]
  workstream: software-change.yaml
  input_mapping:
    task: approved-gap
  next: re-evaluate
```

`workstream` is relative to the parent definition. `input_mapping` maps every child
input to a declared parent input/output. Extra or missing mappings, multiple child
stages, nested children, invalid references and direct child-to-completion transitions
are rejected. `next` must name a check stage. These fields define the internal
workstream handoff; they do not define scenario, oracle or scorer semantics.

The durable queue's `child_workspace` option supplies an already prepared, canonical
dedicated clone under the configured workspace root, on a separate branch. The
coordinator checks it against the parent, source/configuration paths, database,
evidence and other reserved workspaces. Child execution and validation references are
pinned when the parent is queued. No resource provisioning or clone hook is added.

At the child stage, the coordinator binds the declared inputs, derives a stable
handoff and child-run ID, and commits the waiting parent and fresh child in one SQLite
transaction. Exact replay returns the stored runs. Changed pins, stale parent state
or conflicting ownership reject creation. The parent has no execution worker while
waiting; the child uses the existing scheduling and capacity owner. Store writes and
dispatch/cancellation callbacks must honor DEV-242's common capture holds.

The identity fingerprint includes the pinned definitions, execution references and
input values. Cancellation must match the stopped parent's saved handoff and the
child's lineage and its still-waiting attempt/operation. A later parent stop after
successful child composition does not cancel the completed child. A persisted stop
at the child wait prevents child dispatch even if restart
occurs before child cancellation; recovery requests cancellation through the existing
operation adapter. Unknown termination remains unresolved. Evidence and output
identities are retained; this increment does not delete workspaces or close PRs.

Child completion is recorded as an observation. It cannot alone satisfy the parent's
stage or create a coverage claim. DEV-241 provides
`WorkstreamCleanup.merge_proof/2` API for service-verified merge/revision and retained
evidence. Linked children also carry its required top-level `parent_run_id`, so its
cleanup retention check cannot be bypassed through a missing lineage field.
`WorkstreamChild.completion_proof/3` requires the exact waiting parent attempt and
child lineage, a nonempty retained-output reference for every child artifact, then
delegates the service merge and reference verification to `merge_proof/2`. It performs
no parent transition. The current merge API alone accepts an empty reference map;
the child composition check rejects it. The coordinator first invokes `retain_outputs/2`
in a tracked service task and persists its verified output references on the child.
Missing archive receipts hold this phase; retries reuse the immutable bytes. A second
task reads merge proof and `retained_output/3` for every artifact, requiring
`retained_outputs_complete: true`. The parent envelope contains those verified typed
projections, including their producer/output identities, rather than the child's raw
output values. The coordinator reads proof in a tracked
service task, rejects changed parent/child snapshots, and commits only the exact
waiting attempt through `finish_child/3`. The declared parent output receives the
verified merge/archive/retained-output envelope, then the parent becomes ready at
the declared re-evaluation check. Duplicate and late task results cannot repeat this
transition. Capture admission rejects active proof jobs; idle retry timers are paused
and resumed with token checks. Quiescent owned child waits can be captured along with
their child records.

The positive `finish_child/3` test uses an explicit synthetic service-shaped proof
only to exercise the pure transition. The runtime test uses real SQLite and local
immutable evidence bytes with synthetic GitHub responses and remote-upload receipts.
It demonstrates archive holds, typed output retention, exact parent resume and one
local re-evaluation through repeated restarts. A real external service qualification remains pending.
A final human reply,
`child.status == :complete`, matching
lineage IDs or an asserted `merged: true` is insufficient.

Native child dispatch also remains unqualified: the current Linear authorization
contract covers the directly delegated run. The child path fails closed in native
mode rather than treating an internal task ID as authorization. This local contract
increment adds no public approval endpoint or synthetic production success path.

The fixtures in `elixir/test/fixtures/dev243_child/` are test-only. They do not publish
PRs or provide trusted scoring assertions. Full DEV-243 still requires an agreed
Coverage Factory adapter/schema, approved frozen scenario/oracle/stack identities,
one real qualified vulnerable/safe pair, trusted scorer receipts, security-owner
approval and release-owner support-claim review.

Validation used image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`,
with networking disabled and a dedicated container. The selected `mix test` pass
included all seven child test files plus `durable_workstream_test.exs`,
`workstream_store_test.exs`, `workstream_cleanup_test.exs`,
`backup_capture_owner_test.exs` and `backup_capture_test.exs`. `mix compile
--warnings-as-errors`, `mix specs.check` and formatting checks also passed.
Broad local CI was not run; broad GitHub CI remains a separate review boundary.
