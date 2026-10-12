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
a standalone reviewer clone under the workspace root's `.factory-reviews` directory.
Its own Git metadata and service marker bind it to the recorded operation. Replay
reuses that exact checkout; a changed identity or unsafe metadata blocks it.

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
