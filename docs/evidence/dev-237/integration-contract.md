# DEV-237 integration contract

The original DEV-237 implementation is preserved at `54f13a5`, based on merged
main `6f1822c`. The deployment candidate combines it with the actual deployed
DEV-235 source `366734057481aa2c160319f05205e8e7f582a30e`. This preserves the
live agent routing while adding retained evidence. DEV-235 remains unmerged.

- `WorkstreamStore.commit/4` writes a redacted evidence journal in the same
  transaction as the run/event. Schema v3 supplies ordered, owner-only pending
  reads and acknowledgements. Disabled evidence preserves existing behavior.
- `Orchestrator` drains that outbox before dispatch or acknowledgement. Startup
  drains it before liveness reconciliation, then compares each retained run with
  its evidence snapshot. An absent snapshot gets a recovered baseline; a conflict
  or unavailable enabled store blocks advancement. Initial queue, stage, wait,
  reply, cancellation and duplicate-commit paths use the same owner.
- `WorkstreamRunner` uses `Evidence.capture!` for durable agent callbacks.
  Completed observable items and lifecycle events are allowlisted; reasoning,
  unknown events and raw protocol data are omitted. Capture failure returns an
  explicit failed attempt. Local command runs preserve their existing behavior.
- Native Linear acknowledgement and one persisted `evidence-url` managed activity
  carry `Evidence.url(run)`. Restart reuses the publication identity.
- `Evidence.retain_validation!` authenticates saved validation receipts and log
  digests, copies redacted bytes with stage/attempt metadata, and records an
  idempotent completion marker. Completed imports survive loss of the original
  validation directory. It does not grant validation eligibility.

## DEV-235 reassignment integration

The available durable commit interface accepts an explicit authorized transition:

```elixir
event = %{
  kind: :agent_reassigned,
  reassignment: %{
    previous: Evidence.identity(previous_run),
    current: Evidence.identity(new_run)
  }
}
```

Commit the updated run and this event atomically through the durable owner, then
call `Evidence.sync!` before dispatch or acknowledgement. Pins must match the
index's previous pins and the incoming pins exactly. Task and policy stay fixed.
The identity includes the selected agent and each stage's agent binding. The
focused test uses `AgentMapping.reassign`, rejects a silent selection change,
replays both revisions after restart and proves the prior bundle is unchanged.

`WorkstreamStore.linear_reassign` journals in its common `persist_commit`
transaction. The coordinator adds the previous/current identity envelope when
evidence is enabled, then drains before acknowledgement or redispatch. Disabled
evidence retains the existing route behavior. DEV-235's worker fence, route
qualification and authorization remain in place. Live reassignment qualification
still belongs to DEV-235 and is distinct from this integration test.

## Cleanup seam

`Evidence.cleanup_ready?(run_id)` requires a terminal run and an exact root-owned
verified GCS receipt for its current complete bundle. The later managed-workspace
cleanup owner must require this immediately before deletion, alongside GitHub
merge confirmation and trusted worker termination. That cleanup owner is not
implemented here. The local deletion test checks retained bytes and this evidence
predicate with a clearly labeled contract receipt, not live GCS archival.

The human approved staging, activation and disposable live testing in the
coordinating chat on 2026-10-10. Live verification remains pending. See
[the concrete deployment plan](deployment-plan.md) and [acceptance](acceptance.md).
