# Stopped local publisher termination bookkeeping

This one-shot maintenance command resolves an occupied slot only for a stopped,
pre-intent local publisher whose saved result is
`validation_not_permanently_archived`. It does not resume a run, publish a candidate,
change capacity, replace a worker, or alter any run's pinned policy.

The helper is `factory/deploy/reconcile_stopped_local_publisher.exs`. It opens the
existing schema-3 store through its normal exclusive owner and uses
`WorkstreamRun.confirm_termination/1`. The stopped run remains stopped. Only its
current attempt/operation cancellation bookkeeping changes. The old reconciliation
reason and worker identity are retained in a durable event with before/after hashes
and fresh host proof. The matching evidence outbox entry is committed atomically.
A repeated invocation returns `duplicate`; it does not emit a second event.

## Qualified boundary

The operator must first establish from the retained deployment/owner receipts that
the saved local publisher ran on this machine, as `factory-coordinator`, inside
`factory-coordinator.service`. Legacy worker metadata records a BEAM boot ID and
PID, not the operating-system host/unit identity. The helper cannot infer that
historical placement from a changed boot ID. Supply the expected machine ID from
that verified original-host identity, never from an unrelated restored destination.
Moving the store to another host is outside this command's qualification.

The command independently checks fresh local facts before opening the store and
again before committing:

- The machine ID matches; the fixed systemd unit is loaded, inactive/dead, has no
  main PID or control group, and declares the fixed user and `KillMode=control-group`.
- The old fixed cgroup is absent or reports `populated 0` on cgroup v2.
- `/proc` is readable, the helper runs as the fixed service UID, and no process of
  that UID survives outside the helper's own process tree. A surviving BEAM, an
  escaped child, unreadable inventory or an unrelated same-UID shell blocks it.
- The exclusive store lock is available. The exact run/attempt and original worker
  boot ID/PID match, and the run is stopped at a built-in local publish stage.
- The saved uncertainty event exists. The entire publication retry history has no
  external process or publication intent. Other outcomes, remote nodes, custom
  transports, agent/check operations and unsupported source policies are rejected.

Saved JSON receipts support historical attribution; they never replace these live
checks. The command cannot start or stop the service and has no GitHub adapter.

## Operator sequence after review

1. Retain the original owner receipts and a consistent database backup. Verify the
   original machine/unit/user attribution above and the exact target projection.
2. Deliberately stop the fixed coordinator service through the deployment owner.
   Keep native intake/dispatch stopped for this bounded maintenance window. Do not
   run the command while another coordinator or same-UID administrative shell lives.
3. Install only the reviewed maintenance helper from this change; keep the existing
   68cc09a coordinator build, workflow, worker broker and their release identities.
   Use that built Mix project and the service runtime environment established by
   `factory/deploy/service.sh`. Run as `factory-coordinator`, with an `exec` launcher
   so no extra same-UID shell remains. Do not start the application. First omit
   `--apply` to qualify the exact operation without committing termination:

   ```sh
   mix run --no-start --no-compile --no-deps-check \
     /ABSOLUTE/REVIEWED/reconcile_stopped_local_publisher.exs \
     --workflow-path /ABSOLUTE/EXISTING/CONFIGURED/WORKFLOW.md \
     --store-path /ABSOLUTE/CONFIGURED/runs.sqlite \
     --run-id run-9d25985073b61a304344523aa255fd78 \
     --attempt-id run-9d25985073b61a304344523aa255fd78/publish/5 \
     --worker-boot-id 9a73f85a8f0ad3b3eed20c56ecca83ba \
     --worker-pid '#PID<0.2224.0>' \
     --machine-id ORIGINAL_VERIFIED_MACHINE_ID \
     --operator DEV-259
   ```

   Replace paths and machine ID with verified values. The configured workflow store
   must match the argument; private Evidence configuration is required. No-apply
   qualification acquires the normal exclusive SQLite connection but commits no
   run, event or outbox changes. It is not an online inspection command.
4. After `qualified`, use the identical arguments with `--apply`. Retain the returned
   event ID. Confirm stopped status, terminated cancellation, canceled current
   attempt/operation, unchanged candidate/artifacts/pins, and the durable event/outbox.
   If post-commit confirmation fails, the error includes the event ID: inspect it
   before deciding whether to retry. Never restore old cancellation or edit pins.
5. Restore the matched coordinator through the deployment owner. Its existing startup
   recovery drains the outbox and its ordinary capacity rule releases the confirmed
   canceled operation. Require fresh native authorization before queued work starts.

## DEV-259 policy compatibility

Changing `orchestrator.ex` or another executor-owned module would change
`WorkstreamRun.policy().executor_sha256` and block DEV-259's saved ready run before
its first attempt. This maintenance helper is outside the executor source digest;
no executor module, source policy, definition or saved pin changes in this fix.
The supported running executor remains
`ec8ae546aeb408243529f96a1a7048c92d4a61bbe268eee6decbccfb169f4c22` (deployed 68cc09a).
The helper accepts stopped local publisher pins from that executor or the inspected
87ffc28 executor `505e0686567846995558b576a12f5e63a1edf2cfdd0e2a285e890683a9d15179`,
with identical lifecycle, validation and child pins. Future policies fail closed.

DEV-259's executor policy therefore remains compatible. Worker operations separately
use the run's saved `execution.worker_control` service revision and release digest;
the broker rejects a different active release. The redacted admission snapshot does
not expose those fields, so the deployment owner must verify their exact values from
the authoritative store before maintenance. Do not assume a coordinated promotion
to a new source revision preserves worker compatibility merely because the executor
digest is unchanged.

The minimal supported path is to qualify/apply only this standalone helper using the
existing 68cc09a build and workflow, then restart that same coordinator and worker
release. Retain its referenced definitions and verify the saved worker control still
matches the broker. This avoids changing either policy or worker-release pins; the
same queued DEV-259 can then pass ordinary fresh native admission. No repinning or
replacement task is needed for this path. DEV-258 remains stopped and unpublished.
Other stopped unknown operations still reserve capacity.

Local tests use the real store/coordinator with synthetic host proof and show one-slot
admission, live-owner refusal, remote/intent rejection, duplicate/cold replay, outbox
retention, and exact pin preservation. They do not prove the deployed host's systemd,
procfs visibility or historical owner placement; the operator qualification above is
still required. No live maintenance was performed by the source task.
