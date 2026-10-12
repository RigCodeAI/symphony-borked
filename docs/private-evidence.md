# Private task evidence

DEV-237 adds an opt-in evidence store and an authenticated HTML viewer. The
durable runner/coordinator/Linear hooks are integrated against merged main. Live
deployment and ticket acceptance are still pending. Local tests
are recorded in [the acceptance record](evidence/dev-237/acceptance.md); they do
not qualify the live factory.

The coordinator owns this store; it is not another scheduler. `/tasks/<task-key>`
is a stable URL derived from the task ID, shared by all runs for that task. The
viewer shows stage attempts, retries, gates, questions, pinned definitions,
parent/child links, observable completed messages/tool events, and test artifacts.
Completed agent messages appear first as a readable conversation with their stage
and attempt. Task input can be expanded beside those messages. Waits, attempt data, pinned policy and raw event details remain
available in expandable sections. Reloading the page reads current persisted
events. Every route, including artifact downloads, verifies Google's signed IAP
JWT and the configured domain/email allowlist. Unsigned identity headers grant no
access. Evidence responses use `Cache-Control: private, no-store`.

## Configuration

Add the following to a trusted deployment workflow. The root must be outside
task workspaces and writable only by the coordinator. The production maintenance
uploader and backup expect `/srv/factory/evidence`.

```yaml
evidence:
  root: /srv/factory/evidence
  base_url: https://factory.rig.ai
  iap_audience: /projects/PROJECT_NUMBER/global/backendServices/BACKEND_SERVICE_ID
  allowed_domains: [rig.ai]
  allowed_emails: []
```

Use the actual numeric project and HTTPS backend service identifiers for the
audience. Omitting `evidence` leaves existing workflows unchanged. A nonempty
invalid configuration fails startup. The viewer fails closed when configuration,
verification keys or evidence storage are unavailable. Enabling the store or
changing its root requires a service restart. This does not provision IAP, IAM or DNS; those remain
operator-controlled resources. Identity verification follows
[Google's signed-header guidance](https://docs.cloud.google.com/iap/docs/signed-headers-howto).

## Persistence and archive

Before persistence, the coordinator removes reasoning/raw protocol fields and
redacts known environment secrets, credential-shaped tokens, private keys and
secret fields/assignments. User messages retain text only. This is bounded
redaction, not a claim that arbitrary secret-looking prose can always be detected.
Trusted producers must not deliberately submit credentials as evidence.

Durable run commits include a redacted evidence outbox row in the same SQLite
transaction (workstream schema v3). The coordinator drains it before dispatch or
acknowledgement and on restart. A failed drain stops advancement and leaves the
row available for recovery. Pre-evidence retained runs receive an explicit
recovered baseline; a conflicting retained snapshot fails startup.

`index.sqlite` uses SQLite WAL and synchronous FULL writes. It retains redacted
snapshots, deduplicated events and artifacts outside disposable workspaces.
Snapshot and artifact writes synchronously publish immutable bundles; completed
conversation events are committed synchronously and bundled on the next viewer
read or ten-second flush. The manifest is written last. Artifacts are bounded to
10 MiB each and bundles to 32 MiB. Exceeding limits or corrupt files returns an
error; missing evidence never counts as success.

The root-only `factory/deploy/maintenance.sh archive` loads the uploader from the
same verified release. It uploads bundles under
`evidence/v1/<run-id>/<manifest-sha256>/` in the existing archive bucket and reads
each object's bytes back. Only exact readback produces a root-owned verified
receipt. Partial uploads can be retried. Immutable conflicts and corrupt readback
produce failed receipts and a failing maintenance command. Existing exact verified
receipts avoid repeat uploads; an operator can explicitly recheck remote objects
with `python3 -I factory/deploy/cloud_io.py archive --reverify-evidence` from the
verified root-owned release.

The viewer reports archive `pending`, `failed`, or `verified`. Cleanup requires a
terminal run with a verified receipt for the current complete bundle. New events,
snapshots or artifacts invalidate older receipts. This evidence gate does not
replace GitHub merge confirmation or trusted worker termination. No evidence
cleanup or retention expiry is introduced.

`maintenance.sh backup` also takes an online SQLite backup of the evidence index,
including committed WAL data. The backup manifest names `evidence-index.sqlite`
with `restore_path: evidence/index.sqlite`. Restore into the private evidence root
while the coordinator is stopped, then restore required immutable bundle objects
from their recorded archive prefixes. The application does not fetch GCS objects
on a request or automate disaster recovery.

## Shared-owner hooks and reassignment

See [the integration contract](evidence/dev-237/integration-contract.md) for the
implemented hooks and future DEV-235 reassignment seam. Durable agent callbacks
now persist observable events, and managed native Linear activities publish the
stable task URL. Evidence failures fail the agent attempt visibly. Authenticated
validation receipts and logs are copied per attempt with separate original and
redacted digests; producer keys are excluded. These paths are locally tested,
with live qualification pending. DEV-235 remains an unmerged independent change.

Every snapshot event retains its redacted state. Definition/agent changes on one
run require an explicit durable reassignment event matching the exact prior and
current pins. Task ID and policy remain fixed. Silent mutation fails. Previously
published bundles and their original definitions remain immutable and downloadable.

The workstream database migration is forward-only. Before activation, take an
online backup. An older schema-v2 release cannot reopen schema v3: rollback must
use a compatible release or restore the pre-activation backup while stopped.
Restoring that backup discards later state; reconcile workers and external
operations first. See [the deployment plan](evidence/dev-237/deployment-plan.md).
