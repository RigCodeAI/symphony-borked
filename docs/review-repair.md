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

The live pilot must demonstrate a real valid inline review, a validated repair on
the same PR, a new-head review, stale/malformed rejection, duplicate/restart recovery
and the three-round/disagreement human boundary. Source tests are contract evidence
only. Review checkouts currently remain retained; extending merged cleanup to their
roots and typed review outputs requires the cleanup owner's qualified contract.

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
`binding_sha256` over the compact sorted binding JSON plus newline. It records no
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

### Activation order and exact human actions

1. The organization owner creates a private **Rig Factory Review** GitHub App,
   installs it only on `RigCodeAI/rig`, and grants Contents read and Pull requests
   write (Metadata read is automatic). Record its App/installation IDs and verified
   bot login, distinct from publisher and validation identities. Generate its RSA
   key through the approved coordinator secret route. Place the regular key file
   with mode `0600` outside worker, reviewer and validator roots. Do not copy key
   bytes to chat, Git or Terraform state. In the protected workflow, set `review`
   fields `app_id`, `installation_id`, and `private_key_path` to that actual file.
2. The release owner approves a maintenance window for one exact coordinator,
   broker, producer and protected configuration revision. Before replacement,
   reconcile every active or unknown worker identity; fence scheduling and prove
   exact termination where required. Retain old pinned runs and directories. Set
   the root-owned preparation command hash from that packaged release, with the
   existing producer hash, release identity and containment receipt preserved.
3. On the intended worker, qualify real systemd preparation: inspect its actual
   command, environment and mount grants; prove source/siblings/home/cache/key
   writes fail; prove final/staging writes succeed; exercise interruption, duplicate
   preparation, cross-task/symlink tampering, restart and cancellation. Save redacted
   unit identity, journal and cgroup termination receipts. Qualify the configured
   DefaultReview model, effort, subscription account and observed effective runtime.
4. The cleanup owner completes sealed typed review output archival and private-leaf
   ownership/termination checks. The Coverage Factory owner supplies an approved
   disposable PR and frozen review-quality oracle. Run a real inline review,
   same-PR repair, new-head validation/review, stale/malformed rejection,
   duplicate/restart retry, and three-round/disagreement escalation. A human merges.
5. Before enabling factory fix dispatch, the Rig feedback owner adds this condition
   to the existing `dispatch` job in `.github/workflows/codex-pr-feedback.yml`:
   `!startsWith(github.event.pull_request.head.ref, 'cycle/factory-') &&`.
   Review and release that exclusion with Symphony activation so only one owner
   can push factory fixes; other branches keep their existing owner. The workflow
   was read on 2026-10-12; this source PR does not change Rig. Verify the trusted
   `factory/validation` check is required on the current head before human merge.

Rollback fences new dispatch first, reconciles exact contained identities and
retains all unknown, staged, completed and archived data. Restore the reviewed
coordinator/broker/configuration package together, without adopting new-release
runs into an old execution pin. Missing preparation capability must block review.
Do not restore the old feedback owner for factory branches while any Symphony fix
writer may remain live. Resume only after ownership and qualification are proven.
