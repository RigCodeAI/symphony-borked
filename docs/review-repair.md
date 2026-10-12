# Independent review and repair — source increment

The software-change workstream now declares three review stages after publication
and the trusted GitHub validation check. They use the existing coordinator and
durable worker ledger. This document describes source behavior; App provisioning,
deployed worker qualification, live review quality and the real same-PR pilot remain
separate acceptance gates.

The combined source includes the shared context/attempt APIs, review stages and
their transfer, producer, publisher and GitHub validation consumers. The runner
passes only authenticated context claims to the protected commit command; a model
cannot supply those claims. Source checks and the live release remain separate.

## Review stages

`prepare-review` verifies the current trusted check and matches its PR to the latest
passed publication in the same candidate context. PR number, URL, head and branch
identity must agree before preparation, model launch and submission, including the
first review. It records the PR, main base, head, rubric digest and original Cloud
Agent thread. One contained check prepares
a standalone reviewer clone at
`<workspace-root>/.factory-reviews/<sha256(run-id + newline + stable-operation-id)>/checkout`.
Staging is created only inside that same private leaf.
Its own Git metadata and service marker bind it to the recorded operation. Replay
reuses that exact checkout; a changed identity or unsafe metadata blocks it.

Preparation clones into a fresh private staging directory and verifies checkout
and clean state before writing the marker. It publishes the completed directory
with exclusive atomic rename, so an existing final path is never replaced. A
process interruption leaves staging retained; retry uses fresh staging. Unknown
directories and symlinks remain blocked and untouched. Filesystems without that
exclusive operation block preparation rather than falling back to overwrite.

`review` uses the pinned review agent in a fresh app-server conversation. It receives
the embedded rubric verbatim. Its writable root is only the reviewer clone, and
question/wait tools for the Cloud Agent are disabled. Its thread/session are retained
separately and never replace the Cloud Agent's conversation identity. Exactly one
final JSON message is required. Missing, oversized, ambiguous or malformed output
cannot become a clean result.

`submit-review` freshly checks trusted validation and the recorded base/head. It
validates every finding against the independently imported exact commits; remote
reviewer paths must stay under the sealed reviewer checkout. Every finding becomes
a verified RIGHT-side inline comment. No invalid finding is dropped.

The verdict mapping is:

| Rubric result | GitHub event | Next action |
| --- | --- | --- |
| Incorrect | REQUEST_CHANGES | Repair actionable findings; otherwise ask a human. |
| Correct, no findings | APPROVE | Wait for human merge in GitHub. |
| Correct, findings | COMMENT | Repair all actionable findings. |

The service uses a distinct GitHub App. It verifies the authenticated App's bot
author differs from the PR author, and re-reads the submitted review and every
inline comment. Exact body/head/state/author/comment mismatches block advancement.
The body marker binds the stable operation, base/head, rubric and result digest.
A lost response is reconciled by reading that marker before creating another review;
duplicates or a copied marker from another author block instead of creating more.
Remote identity changes after submission leave the old review recorded but prevent
its use for the changed candidate.

## Repair authorization

Actionable submitted findings return to the original Cloud Agent stage on the same
run, branch and PR. The run keeps all real publication attempts; it does not create
a new run or copy fabricated history. A coordinator-owned transition appends a
sealed candidate context. Original definitions, execution pins, policy and earlier
receipts remain unchanged.

Each context records the exact prior publication/review attempts and PR identity,
the prior published head as `repair_parent_sha`, and the trusted current main base
as `validation_base`. These have different purposes. Transfer must prove the new
candidate's direct parent is the previous published head. Final validation must
test against the pinned current main base and authenticate a fresh receipt for the
new context and attempt. A main advance invalidates the old-base receipt even if
the new main is an ancestor of the candidate. It requires a new authorized context
and fresh tests; ancestry alone cannot requalify stale evidence.

`CandidateContext.view/2` resolves an ephemeral run view for the selected attempt.
Consumers read `execution[:candidate_context_id]`, `[:repair_parent_sha]` and
`[:validation_base]`. `validation_context/2` supplies the common signed identity;
legacy receipts retain their original four-key identity. `same_context?/3` rejects
mixed-context consumers. Context digests and HMAC seals bind immutable original
execution/definition/policy and real prior receipts, with a separate domain from
validation and review seals. Only service credential references are persisted.

One completed Cloud Agent fix attempt consumes a review round. Duplicate results,
transport retries and restart do not consume or reset rounds. Three completed
unsuccessful rounds, disagreement, invalid review evidence or missing lineage block
for human action. A repair turn must return structured disagreement/explanation
feedback. Required executable validation still runs before republishing and
reviewing the new head. A bot review or Linear reply never authorizes merge.

## Configuration and live release choices

The trusted workflow's `review` object accepts `app_id`, `installation_id` and
`private_key_path`, using the same reference format as publisher/check configuration.
Use a separate App and private key from both publisher and validation App. Keep its
key outside all worker/reviewer workspace roots. Installation tokens request only
Contents read, Pull requests write and Metadata read for `RigCodeAI/rig`. No token
or private key enters a coding, review or validator process. Missing or overlapping
configuration blocks preparation; there is no PAT or publisher-identity fallback.

Before the live release, provision that App, verify its actual bot author and
permissions, and qualify DefaultReview's configured model/effort/subscription on
the intended contained worker. The source definition remains authoritative; desktop
settings and an earlier model probe do not establish current review quality.

Use one fix owner for the factory's actual `cycle/factory-` branch namespace. The
recommended migration is to exclude those branches from Rig's existing PR-feedback
workflow while Symphony handles their submitted review findings. Keep the existing
feedback owner for other branches. The Rig workflow change and ownership cutover
must be reviewed and released together; this source increment does not modify it.

Automated review and repair are required for the first alpha. Its minimum accepted
flow is **delegate a Linear ticket → Cloud Agent implementation → validated PR →
separate review agent → automatic fix on the same PR → fresh validation and review
→ ready for human merge**. PR creation alone does not complete the demo.

Keep one existing worker and retain all workspaces. Recovery exercises, scaling,
Coverage Factory scoring and automatic cleanup are deferred. Existing stale-result,
identity, validation and three-round/disagreement guards remain enforced; source
tests do not establish a live review/fix demo. DEV-240 remains In Progress until
the live minimum is accepted; deferred work must not replace that minimum.

## Initial source qualification

On 2026-10-12, the qualified isolated toolchain image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
received a fresh archive of merged source plus the reviewed changes. Warnings-as-errors
compilation, public specs and **252 focused tests passed**, seed `240`. The tests
cover context tampering/restart/round limits, real disposable reviewer clones,
structured output capture, App transport contracts and affected workstream,
delegation, capacity, validation, cleanup and capture behavior. Validation receipts
in review contract fixtures are explicitly synthetic, with real authentication and
source checking; they do not claim an executable or live model check.

The container had two CPUs, 4 GiB memory, no network or shared volumes, and no
supplied credentials. It was stopped and removed after scoped formatting was copied
back. [Command](evidence/dev240/source-contract-command.txt),
[test output](evidence/dev240/source-contract-tests.log) and
[source digests](evidence/dev240/source-contract-snapshot.json) record the boundary.
That checkpoint preceded the consumer integration and broad hosted CI.

## Combined source qualification

The final combined source includes tested publication consumer `24d0996` and
validation consumer `c9150a6`, with main at `dc636ee` (child handoff and held recovery
inspection). In the same isolated image and resource limits, warnings-as-errors
compilation, public specs and **127 focused tests passed**, seed `240`; four other
capture cases were excluded by explicit test selection. The first run had one
owned fixture assertion mismatch (`"complete"` versus `:complete`). After correcting
only that assertion, the complete focused gate passed.

The tests exercise real protected-command input delivery, authenticated context
claims overriding forged model inputs, legacy input preservation, actual review
preparation and fake-App submission, verdicts, every inline comment, replay,
wrong-PR rejection, transfer/producer/publisher/check consumers, the shipped flow,
final offline migration pins, child handoff/cancellation and held inspection.
The migration helper includes the final child source hash; no migration was run.
The exact PR body validator also passed. Both check containers were removed.

[Command](evidence/dev240/integration-command.txt),
[passing output](evidence/dev240/integration-tests.log),
[first-run output](evidence/dev240/integration-first-check.log) and
[final runtime digests](evidence/dev240/integration-snapshot.json) record this source
boundary. Broad hosted CI remains a separate alpha check. Real reviewer quality,
App provisioning, feedback-owner cutover and the live same-PR pilot remain gated.

## Checkout interruption and held-recovery integration

Review found that the old direct-to-final clone could leave a markerless final
directory after interruption. The staged preparation fix passed compilation,
specs and 31 focused tests. Its real Git/Python regression kills the process after
clone and before checkout/marker creation, then proves retry, completed replay and
retention of the interrupted data. The exclusive-publication race test creates an
empty unknown final directory during checkout and proves failure preserves that
directory's inode. Changed completed identity also blocks without replacing data.
A disposable macOS smoke passed interruption, retry and replay as well.

After main `58cbe54` added full held startup, the final combined source passed
warnings-as-errors compilation, public specs, PR-body validation and **41 focused
tests**, seed `240`; eight other capture cases were excluded by explicit selection.
This includes actual linked-child capture, cold full application held boot/restart,
and readonly validation of child attempts. That validator accepts only an optional
`nil` context field (including the legacy absent field); non-nil context claims or
unknown keys remain blocked. Both executor source inventories and the final child
source hash are included in the refreshed offline migration pins. No migration,
deployment, dispatch release or live review was performed.

[Restart command](evidence/dev240/restart-command.txt),
[restart output](evidence/dev240/restart-tests.log),
[merged-source command](evidence/dev240/restart-recovery-command.txt),
[merged-source output](evidence/dev240/restart-recovery-tests.log) and
[source digests](evidence/dev240/restart-recovery-snapshot.json) record the boundary.

## Contained preparation release requirements

The remote client uses `prepare_review_workspace`, not a generic check with sibling
write access. Its exact request contains the current operation, run and stable
operation IDs, source workspace, nine review identity fields and command argv.
The root broker derives all destination paths and input pins. The configured
`review_preparation_command_sha256` must equal `ReviewWorkspace.command_sha256()`
from the exact packaged coordinator source; omission disables this capability.
The protected Python script has a separate 16 KiB limit. Generic command arguments
retain their 4 KiB limit. The held unit still requires durable registration,
stream readiness and root-validated release before executing.

The broker creates the review parent and private leaf with checked ownership and
permissions. Its protected owner record is
`<records-dir>/review-owners/<leaf-digest>.json` (mode `0600`, in a `0700` directory).
Version 1 stores `kind=review-preparation-leaf-owner`, `run_id`,
`stable_operation_id`, `source_workspace`, all nine `identity` pins, `leaf`, and
`binding_sha256` over the compact sorted binding JSON plus newline (excluding
`version` and `binding_sha256` itself). It records no
current attempt ID. Only a missing leaf can receive a new owner record; a plausible
worker marker never grants ownership. The root-owned public operation manifest
adds `review_preparation_command_sha256`; the held wrapper verifies that pin and
restricted environment against its exact argv and derived paths before release. Preparation grants only that leaf in `ReadWritePaths`; source Git is
read-only. It grants no worker home, shared cache or shared temporary root and uses
an environment with no model or GitHub credential references. The worker home is
inaccessible during preparation. A different operation
cannot prepare the same leaf while its previous operation is live or termination is
unknown. Completed replay binds the stable ID and source, while each new attempt
retains its own contained operation identity. Interrupted and unknown data remains
retained. The reviewer model subsequently receives only the completed checkout.

This is a source capability. Unit-property derivation tests do not prove that a
systemd unit actually enforces these permissions on the deployed worker.

### First-alpha setup and exact human actions

These are requirements for an approved rollout, not authorization to change cloud
resources, access, budgets, secrets or Rig workflows. Reuse the merged review code
and one existing worker. DEV-238 owns publication; DEV-239 owns trusted validation
and its GitHub check/protection setup. The coordinator selects one reviewed release
containing their integration and the review service-completion support. Automatic
cleanup stays disabled (`merged_cleanup` is omitted or false).

1. **Organization owner: create and install the separate reviewer App.** Open
   [RigCodeAI new GitHub App](https://github.com/organizations/RigCodeAI/settings/apps/new).
   Use name **Rig Factory Review**, homepage `https://github.com/RigCodeAI/symphony`,
   no user OAuth request/device flow, webhook **Active** unchecked, and **Only on
   this account**. Repository permissions: **Contents: Read-only**, **Pull requests:
   Read & write**, automatic **Metadata: Read-only**; other permissions stay **No
   access**. Choose **Install App → RigCodeAI → Only select repositories → rig**.
   Record App ID, installation ID from its installation page, App slug and verified
   bot login; these must differ from publisher and validation identities. Follow
   [GitHub registration](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
   and [installation](https://docs.github.com/en/apps/using-github-apps/installing-your-own-github-app).
   In App settings, use **Credentials → Key pairs → New key** and transfer its PEM
   through the approved coordinator secret route; see
   [key instructions](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps).
   The operator installs a regular coordinator-only file with mode `0600`, outside
   coding, reviewer and validator roots. Share IDs and the protected key reference,
   never PEM bytes. Configure the protected workflow (replace example values):

   ```yaml
   review:
     app_id: 'REVIEW_APP_ID'
     installation_id: 'REVIEW_INSTALLATION_ID'
     private_key_path: /srv/factory/homes/factory-coordinator/.secrets/github-review.pem
   ```

2. **Release owner: approve deployment to the existing coordinator and worker.**
   Name the exact reviewed commit/archive, maintenance window and one demo's
   allowed model/compute scope. Confirm no active or unknown branch writer before
   replacing services; unknown termination blocks replacement. Deploy the matched
   coordinator, broker, wrapper, producer and protected configuration together.
   Preserve existing state, publisher/validation references and all workspaces.
   Set the root-owned broker's `review_preparation_command_sha256` from
   `ReviewWorkspace.command_sha256()` in that packaged release; keep the producer
   pin, machine identity and release checksum correct. No new VM or access grant is
   part of this checklist. Keep dispatch held until the next two steps pass.

3. **Deployment operator: verify the one-worker review path.** On the approved
   release, require a current containment receipt and `review_preparation_v1=true`.
   Check the actual preparation unit grants only its private leaf, hides the worker
   home and excludes GitHub/Linear credentials; source and sibling writes must fail.
   Check that preparation returns the exact clean recorded head. Candidate validation
   invokes Docker in the coordinator's trusted validator process after candidate
   transfer, not in the worker's model process. DEV-239 verifies that process can
   use the pinned validation image and execute the actual approved Rig check with
   network disabled, no host/credential mounts and the declared limits. The worker
   must not gain Docker group/socket access. A job is capped at one CPU, 512 MiB
   RAM, 128 PIDs and a 512 MiB writable `/tmp`; host capacity must additionally
   cover the coordinator/daemon. Measure space for the pinned image, sealed source
   layer, source/export and after-copy scratch, evidence and daemon metadata before
   the demo. No fixed unmeasured disk-size claim qualifies the host. The shipped
   [rig-local policy](../factory/policies/rig-local.yaml) pins
   `python@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea`
   and one architecture test; DEV-239 must approve its fit to the selected ticket
   or supply the intended pinned policy/image. Images and dependencies must already
   exist because the runner never pulls and execution has no network. Verify subscription
   readiness and observed effective settings for **DefaultCloud: gpt-6-luna,
   medium**, and **DefaultReview: gpt-6.1-sol, high**; both have Daybreak disabled,
   `approval_policy: never` and `sandbox: workspace-write`. Definitions in the
   selected release remain authoritative. Do not substitute a model/account or API
   billing when readiness fails. The internal reviewer receives its own checkout
   and conversation, with no Linear delegation identity.

4. **Rig feedback/protection owners: enable one fix owner before the pilot.** Review
   and release this condition in the existing
   [Rig feedback workflow](https://github.com/RigCodeAI/rig/blob/main/.github/workflows/codex-pr-feedback.yml)
   `dispatch` job: `!startsWith(github.event.pull_request.head.ref, 'cycle/factory-') &&`.
   Other branches keep their current feedback owner. At
   [Rig branch settings](https://github.com/RigCodeAI/rig/settings/branches) or the
   existing [ruleset](https://github.com/RigCodeAI/rig/settings/rules), DEV-239's owner
   requires `factory/validation` from the distinct trusted validation App on `main`;
   no bot bypass or automatic merge authority. The exact pilot head must have fresh
   passed validation before review; every repaired head is revalidated before
   publication/review. GitHub supports an
   [expected App source for required checks](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

5. **Human demo owner: approve one small Rig ticket and observe the whole loop.**
   Choose its scope and a genuine small actionable defect to demonstrate. Open the
   ticket in [Rig Linear](https://linear.app/rigai), retain the human assignee, add
   `factory:rig`, use Backlog/Todo/In Progress, and delegate to **Default Cloud Agent**.
   The operator maps that ticket's exact UUID to matching dedicated Rig clones under
   `/srv/factory/contained-workspaces` and selects the packaged `software-change`
   definition through `linear_delegation.workstream_path`; see
   [existing delegation configuration](linear-delegation.md#configuration).
   After all setup gates pass, release that one ticket. Observe a real submitted
   review with a valid inline finding, a separate reviewer thread/checkout, an
   automatic fix by the same pinned Cloud Agent definition on the same PR/branch,
   fresh validation for the new head and a new independent review. Save the Linear
   ticket/conversation link, PR number/URL, before/after SHAs, review IDs/URLs,
   distinct identities and passed check links. End at **ready for human merge**;
   the human decides whether to merge. Retain the workspaces and evidence.

A first PR with no actionable review finding proves review execution but does not
prove the required fix loop. Missing or malformed review evidence, stale heads,
unsupported completion or readiness failure must remain visible as a blocker;
they cannot be reported as first-alpha completion. The broader recovery, Coverage,
scaling and cleanup plans remain later work rather than prerequisites to this demo.

Rollback fences new dispatch first, reconciles exact contained identities and
retains all unknown, staged, completed and archived data. Restore the reviewed
coordinator/broker/configuration package together, without adopting new-release
runs into an old execution pin. Missing preparation capability must block review.
Do not restore the old feedback owner for factory branches while any Symphony fix
writer may remain live. Resume only after ownership and qualification are proven.

### Combined alpha source package candidate

The locally qualified source candidate is merged commit
`818c59a1a9f93479cff2048ec343c3c6745e33cb`, including review service-completion
support. It is packaged with the existing `cloud_io.py pack` helper:

| Pin | Value |
| --- | --- |
| Archive SHA-256 | `2e6fcaef7ae03bd97d268e089b7f1e2420b9f8f76e7c567f4a3a16bb90bc11fb` |
| Release object | `releases/818c59a1a9f93479cff2048ec343c3c6745e33cb.tar.gz` |
| Candidate producer command | `a7bf9930b1e99665d598a30c94db43896d9056a3aaeaf28347d5a1a12a1a1ed9` |
| Review preparation command | `e9484451418b48094ace5c5694ff396733c5e2929c0f5e65b35620ed2ec06a25` |
| Executor policy | `87d9bc93c5db6b5c1afd01529a7dff35982798727a2b484ff71b527446f5643e` |

[Package metadata](evidence/dev240/alpha-release-818c59a/package.json) and
[all definition/policy pins](evidence/dev240/alpha-release-818c59a/pins.json)
identify the exact candidate. The local source archive remains at
`/private/tmp/dev240-alpha-release-818c59a/release.tar.gz`; it has not been uploaded
or deployed. App IDs, key references, approved Rig policy/base/image and live
machine/boot qualification references are protected rollout inputs supplied by
their existing owners; the source archive contains no credentials.

[Source qualification](evidence/dev240/alpha-release-818c59a/qualification.log)
passed formatting, warnings-as-errors compilation, public specs and 172 focused
core-flow tests. [Broker/transport tests](evidence/dev240/alpha-release-818c59a/broker-tests.log)
passed 55 tests. The local toolchain is Linux ARM64; its compiled dependencies and
Exqlite artifacts must not be copied as x86_64 VM binaries. Build the exact source
with the target's qualified native toolchain before service activation.

Release approval must name this exact source/archive and its matched broker,
wrapper, producer, workflow and private configuration, with the existing native
startup/activation path reviewed by the deployment owner. The current protected
settings and prior release must be recorded before replacement. A successful local
compile does not prove Docker validation, effective reviewer model execution or a
live review/fix loop. Those remain step 3 and step 5 checks above. No cloud, budget,
access or runtime setting was changed by preparing this package.
