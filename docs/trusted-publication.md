# Trusted Rig publication (DEV-238)

This increment adds a service-owned publication stage to Symphony. Local contract
tests are separate from the real Linear-to-Rig-PR acceptance demo. GitHub App
installation, live publication and worker candidate transfer remain deployment
prerequisites until their recorded checks pass.

## GitHub App setup plan

Prepare a **private Rig Factory Publisher App** under the RigCodeAI organization:

1. Open <https://github.com/organizations/RigCodeAI/settings/apps> and register the
   App. Use `https://factory.rig.ai` as its homepage. Disable webhook delivery for
   this publication-only increment; it needs no OAuth callback or user tokens.
2. Grant repository **Contents: read and write**, **Pull requests: read and write**,
   and the automatic **Metadata: read** permission. Request no organization,
   administration, merge-protection, workflow or checks permissions here. A
   candidate needing an additional permission must block for a scoped decision.
3. Install it on **only `RigCodeAI/rig`**. Record App ID and installation ID. An
   org owner must approve the installation; configuring IDs is not proof of access.
4. Generate a private key and deliver its bytes through the existing protected
   coordinator secret/bootstrap route. Do not put it in YAML, Git, chat, logs,
   Terraform state, worker homes or a task workspace. Materialize it as a regular
   coordinator-owned file with mode `0600`, outside candidate and test directories.
   Do not provision a new secret resource or change IAM without a separate scoped
   plan. Record only the reference and version in the deployment evidence.
5. Add these non-secret references to the protected coordinator workflow:

   ```yaml
   publisher:
     app_id: 'APP_ID'
     installation_id: 'INSTALLATION_ID'
     private_key_path: /srv/factory/homes/factory-coordinator/.secrets/github-publisher.pem
   ```

   Replace IDs with positive numeric strings. The example key path is a reference,
   not a deployment command. The service requests short-lived installation tokens
   narrowed to Rig, verifies the permissions/repository, and uses no PAT fallback.
   Native delegation also supplies the existing validation runner's non-secret
   paths and a full trusted base SHA:

   ```yaml
   linear_delegation:
     # Preserve the existing app, workspace and worker-control configuration.
     validation:
       policy: /protected/coordinator/rig-policy.yaml
       archive: /srv/factory/validation-receipts
       scratch: /srv/factory/validation-scratch
       candidates: /srv/factory/validation-candidates
       base: FULL_TRUSTED_BASE_SHA
   ```

   These are reviewed path examples. A candidate snapshot is separate from the
   original clone, archive, scratch directory and protected definitions. The
   shipped focused architecture policy qualifies only the small demo's scope,
   not arbitrary Rust changes or packaging.
6. Verify worker and validator boundaries before enabling publication: no key mount,
   publisher token environment, service credential file or cloud metadata access.
   Keep the model's edit/test sandbox unchanged. DEV-237 owns worker local commit
   preparation; a model-authored SHA or bundle metadata is not trusted transfer.

This plan adds no compute, slots, API billing or repository protection changes.
It uses the existing coordinator/archive. Estimate incremental archive and network
usage from the small pilot's actual artifact sizes before rollout; this document
does not claim a measured cost. The dedicated merge-check App/protection setup is
DEV-239, and human GitHub merge remains the boundary.

A read-only organization-installation inventory on 2026-10-11 found the existing
`sivere-source-reader` App with Contents/Metadata read access and several unrelated
integrations. No dedicated factory publisher installation was identified. Do not
reuse those unrelated integrations' credentials or expand their permissions.

GitHub documents [Git access permissions](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/choosing-permissions-for-a-github-app)
and [PR creation permissions](https://docs.github.com/en/rest/pulls/pulls#create-a-pull-request).

## Gate and replay contract

A `publish` stage references an upstream `candidate_validation` stage and declares
its own required assertions inline. The publisher authenticates the saved receipt,
recomputes both stage gates against the clean current candidate and pinned policy,
then requires permanent archive verification for that exact validation attempt.
Missing, failed, incomplete, forged, stale or development evidence cannot publish.

If an active publisher returns `validation_not_permanently_archived`, the coordinator
wakes its existing reconciliation path after ten seconds. It checks the same run,
attempt and durable wait count and reattaches a surviving worker before considering
replacement. New attempts retain the publication side-effect identity, branch,
candidate and validation, and still need normal native authorization and archive
qualification. Capture holds defer checks; stopped runs never resume this way.

The automatic wait is limited to thirty checks per publication side-effect identity,
including checks spent waiting for the original worker to exit. The count survives
transport retries and coordinator restart. On exhaustion the run stays reconciling
and records `publication_archive_wait_exhausted` for operator inspection; restart
and ordinary reconciliation do not reset that budget. Other uncertain publication
outcomes keep their existing recovery behavior. This is locally tested service
behavior, not proof that a deployment or a stopped run has recovered.

`Evidence.publication/4` finds a verified immutable revision containing the saved
passing attempt, its authenticated validation-manifest identity and the completed
artifact import. It verifies the local retained bytes against the root uploader's
receipt. `publication_revision/5` rechecks an exact revision after later conversation
events. Neither API grants validation eligibility by itself.

The coordinator commits the stage operation before execution and saves the exact
publication intent before the first GitHub write. It checks current task ownership
before subsequent writes. Cancellation cannot undo an already accepted request;
stopped recovery observes the deterministic branch and PR without writing to GitHub,
records the outcome, and keeps the run stopped. Absent or mismatched outcomes remain
pending for read-only checks; repeated unchanged observations add no durable events.

The branch is a
deterministic `cycle/factory-<task hash>`; the publication identity binds run, stage,
candidate and validation receipt. The service looks up all PR states for that branch
and `main` before creating anything. A closed or ambiguous PR blocks. A retry
reconciles the remote branch and PR, including a lost push/create response. A new
validated candidate can update the same PR only when the recorded previous head
matches the remote head and is an ancestor. An unexpected remote head blocks.

Review-authorized repairs consume the append-only sealed candidate context. The
producer, transfer, validator and publisher bind the same context ID. A repair's
sole direct parent is the exact prior published head; its separately pinned
validation base must be an ancestor of the new candidate. If main advances outside
that lineage, this version blocks repair until a supported integration path exists.
It does not substitute the old head for current main or reuse an older receipt.
The publisher also requires the authenticated prior PR number, URL and branches,
even when retry finds the new candidate already pushed.

The protected local-commit input includes `candidate_context` with exactly `id`,
`repair_parent_sha` and `validation_base`. Present context hashes compact sorted
JSON `{candidate, candidate_context}`; absent or null context preserves the legacy
candidate-only hash and seven-field producer output. The repair output adds
`candidate_context_id`, `repair_parent_sha` and `validation_base`. Broker RPC shape
is unchanged: its protected descriptor binds these inputs through the digest.
The runner must inject this service-derived input before executing the producer;
model output cannot supply authorization.

Only the evidence section between `factory-evidence:start` and
`factory-evidence:end` is managed. Text outside it is preserved byte for byte. New
PRs retain the repository template from the pinned base commit and link the Linear
task. Duplicate/malformed section markers block. GitHub has no atomic compare-and-
swap API for PR bodies: the transport rereads the expected body before PATCH and
blocks an observed concurrent edit; a human edit racing the final PATCH is a
remaining API limitation that must be reported at rollout.

The publisher uses copied Git metadata without candidate config/hooks or global
Git configuration. Pushes name the exact validated commit and fixed HTTPS remote,
with an exact lease. Credentials reach only that service subprocess. Candidate
code is never run by the publisher. Remote GitHub branch/PR identities are checked
again before the coordinator accepts stage completion.

## Worker-control rollout plan (not executed)

The original producer was qualified in DEV-237 PR 16 at
`7a8a87ae295188ae830527d2d26205a51b9bb744`. This parent-traversal source changes its
script and command pin. The new script SHA-256 is
`3cfde2cf353b66bdfea2a2e3193eede1c21472b3a4b162a308f20ecbb2bb7204`;
the UTF-8 compact command argv SHA-256 is
`97c7e721d4583bfd2f0f97b2dd55c6d606db30b18fa4f125b179869f6961c3d9`.
These are source identities, pending combined integration testing and worker
qualification. They do not authorize rollout. Preserve old run pins and qualify
this exact final producer/broker/coordinator package before activation.

1. Wait for DEV-237's qualified commit-preparation command. Record the exact
   protected workstream revision and SHA-256 of the UTF-8 compact JSON command argv
   (`/usr/bin/python3`, `-I`, `-c`, script), preserving non-ASCII script bytes.
   Do not approve an in-progress script or
   derive authorization from a model-supplied filename or check label.
2. Prepare an immutable source package and release checksum containing the updated
   broker/client and that qualified producer. Preserve the current root-owned
   broker configuration and install receipts. The added optional
   `candidate_producer_command_sha256` is a root-owned configuration field, not a
   request field. Omission keeps old protocol operation and disables bundle export.
3. Review the exact package/config diff, tests and independent root-boundary review.
   Confirm no active/unknown worker operations before the maintenance window. Stop
   dispatch, reconcile the current worker, and back up the protected ledger/config.
   A transport disconnect alone never proves termination.
4. Using the existing operator installation route, stage the pinned broker release
   and candidate command pin, then explicitly restart the existing broker. No new
   account, forwarding, sudo/polkit, public endpoint or cloud IAM grant is needed.
   Pin its exact release revision/checksum in new coordinator runs, and qualify
   `candidate_bundle_v1` along with the existing containment checks.
5. Export a disposable successful service-preparation bundle using its exact saved
   operation identity. Verify chunk/total digest, size, candidate SHA, first parent
   and base on a service-owned coordinator snapshot. Running, failed, unrelated,
   stale-release, mismatched-input and oversize exports must reject. The 64 MiB
   bundle bound is explicit. Imports also cap each checked-out file at 64 MiB,
   the total tree at 256 MiB and blob count at 50,000. Git subprocess memory, output
   and file size are bounded. An oversized task pauses rather than silently changing
   the producer/consumer strategy.
6. Only after exact candidate validation, permanent upload verification and approved
   App access, run the one delegated Rig publication acceptance task. Preserve the
   existing worker and original clone; no publisher credentials reach either.

Rollback requires dispatch held and exact worker liveness reconciled. Restore the
previous pinned broker/config and coordinator workflow; their earlier protocol has
no bundle capability and publication must remain blocked. Preserve durable ledgers,
candidate snapshots and archives. Do not close PRs, discard an unknown operation,
rewrite a run's policy pins or silently migrate old runs. Compute/IAM cost delta is
zero resources in this plan; bounded snapshots use existing disk. Measure existing
disk headroom and variable archive/network usage during the disposable pilot.

## Verification boundary

The ordinary local runner cannot publish: publication requires durable coordinator
ownership. The opt-in live demo must produce one real Rig PR with the tested SHA,
evidence table and permanent links, then force validation failure, publication
timeout/retry, restart/replay and a changed candidate. Confirm branch and commit
through GitHub, and preserve the same PR and human text. Do not mark acceptance
complete from synthetic contract receipts or agent prose.

After inspection, stop only the disposable task's processes. Preserve its permanent
evidence and PR. Do not close/delete the PR or merge it as automatic demo cleanup.
