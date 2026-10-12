# Trusted GitHub validation (DEV-239)

This source increment adds the dedicated App transport and service-owned
`github_validation` evaluator. It has not installed an App, enabled protection,
qualified a live Rig PR, or changed cloud/worker access. DEV-238's merged publisher
APIs are available; its live acceptance remains separate.

## Service contract

`software-change` now runs validate → publish → github-validation → human wait.
The registered evaluator has inline `validation_stage`, `required`, `success` and
`failure: blocked` fields. Its validation stage must be a normal ancestor, use
`candidate_validation`, and provide a validation input. Definitions cannot supply
check commands, credentials, verdicts or an arbitrary GitHub repository.

`ValidationCheck.reconcile/3` authenticates the latest final receipt and recomputes
both the referenced validation gate and the check stage's required assertions. It
requires permanent archive confirmation and a private HTTPS evidence URL. It reads
the actual PR and current main commit, verifies repository/PR identity, head/base,
and base ancestry, then checks them again around the write. Missing, malformed,
failed, incomplete, skipped or stale evidence yields `failure`, never `neutral`
or `skipped`. A quality score cannot cancel a failed selected executable check.
Candidate policy edits cannot replace the externally pinned trusted contract.

A successful prepublication receipt is reused when its exact source, head, base,
policy, environment and attempt still match. `read_receipt/2` alone is not enough.
The App check identifies the tested commit, policy, receipt and permanent artifact.
Its external ID binds the run, pinned definitions/service policy, inline contract,
validation policy and remote identity. The transport validates installed App ID,
repository-scoped tokens and returned check identity. It rejects extra permissions.

Check creation first reconciles existing App-owned checks for the head. A lost
accepted response is reusable. A changed contract/base replaces the same check's
external ID rather than leaving a second same-name success. Ambiguous identities
block. Stage completion uses a separately owned asynchronous verification task to read the
remote check again. The service seals that fresh observation using the existing
service-only validation producer key with a distinct HMAC domain. The coordinator
re-verifies the receipt, archive, pinned identity, seal and 30-second observation
bound locally; worker-provided success is not accepted. `verify_ready/3` is the read-only readiness
interface for the subsequent review increment.

### Immutable repair context

The review repair increment keeps two separate commits: `validation_base` is the
trusted current `main` integration base; `repair_parent_sha` is the previous
published head used for transfer lineage. A repair never substitutes its parent
for the current-main validation base.

DEV-240 owns the append-only sealed `CandidateContext` and attempt links. The gate
consumes `view/2`, `validation_context/2` and `same_context?/3`: the consuming
check or wait and the selected validation attempt must share the authorized
context. The fresh authenticated receipt binds that context, exact candidate,
policy and current-main base. The live PR must also match the sealed context's
number, URL and branches. Original execution pins and earlier attempts stay
unchanged. New context IDs enter the check identity and sealed completion path;
the original context keeps its prior receipt and check identity shape.

A changed `main` invalidates an old-base receipt before or after tests, even when
the newer main commit is already an ancestor of the candidate. A candidate that
does not contain current main also fails. A newly authorized context and fresh
validation are required; ancestry alone cannot requalify old tests. The consumer is tested against shared source
`1865a6acfc411a9a0cd112ffe3f6794f8c7c31aa`; that dependency branch is not a
merged prerequisite or live release. The first bounded 119-test run had seven
fixture failures: six repair fixtures changed an uncovered file and the shipped
flow fixture lacked the new separate review setup. Repair fixtures now change
the frozen policy's covered README without changing policy. All 32 native gate
tests passed. The final bounded 119-test run passed with DEV-240's corrected
shipped-flow fixture in a disposable test-file overlay; that fixture is excluded
from this consumer commit and remains DEV-240-owned. Compile with warnings as
errors, public specs, full formatting and diff checks passed. No broad suite was
repeated. DEV-240 owns combined integration with DEV-238's producer, transfer and
publisher consumers, and will qualify the actual combined source before its PR.

The existing coordinator polls enrolled human-wait PRs every 60 seconds, outside
worker slots. It persists only changed observations, with a durable sequence number.
Head/base changes revoke qualification. Human replies are first durably queued as
`pending_validation_reply`, retaining the wait, attempt, artifacts, activity and
approval identity. A separately owned monitor verifies fresh evidence and the
expected-App success before consuming the reply exactly once. Failed/stale evidence
retains the queued reply visibly; duplicate delivery does not repeat advancement.
GitHub reads are never performed in the coordinator's human-reply callback. Restart reconciles again.
Normal discussion and human merging remain external. No merge endpoint is exposed.
GitHub protection, once approved and enabled, is the independent merge boundary.

The merged capture barrier guards `validation_monitor_owner`, the
`github_validation` publication-owner callback and validation scheduling, and
drains `workstreams.validation_checks` before capture. This includes
`stage/<attempt>` read-verifier tasks and their `:validation_stage_verified`
result acceptance and `:validation_stage_verifier_owner`. The bounded
consumer suite includes capture and cleanup ownership regressions. Deployed
capture and restore qualification remain separate.

## Separate App and rollout proposal — not executed

1. Register an organization-owned private **Rig Factory Validation** App and install
   only on `RigCodeAI/rig`. Grant Checks write, Contents read, Pull requests read,
   and automatic Metadata read. No publisher, administration, statuses, workflows,
   organization, or merge permissions. The initial polling adapter does not need
   webhook delivery; do not configure an unsupported webhook endpoint. A later
   approved ingress adapter may add pull_request/push/check rerun subscriptions.
2. Materialize its RSA key through the existing coordinator secret route as a
   coordinator-owned regular file, mode 0600. Keep the App, installation and key
   path distinct from the publisher. Keep bytes out of YAML, Git, Terraform state,
   logs, worker homes, candidate/test containers and host metadata. Record only
   IDs and secret reference/version. Review actual worker credential exclusion.
3. Configure only the protected coordinator workflow:

   ```yaml
   validation_check:
     app_id: 'VALIDATION_APP_ID'
     installation_id: 'VALIDATION_INSTALLATION_ID'
     private_key_path: /srv/factory/homes/factory-coordinator/.secrets/github-validation.pem
   ```

   IDs must be positive numeric strings. Omission disables the configuration;
   workstreams requiring the evaluator then block before execution. This reference
   example is not an installation or activation command.
4. Keep checks informational initially. Enroll the publisher pilot and current
   non-factory PRs before making the check required. The validation-only workstream
   below uses no coding agent and has no push/PR creation authority. Trusted service
   inputs supply `validation_pr: <positive PR number>` plus the clean exact candidate,
   base SHA, external policy and archive configuration. Never accept these settings
   from candidate files or an agent-provided proof. Existing/new non-factory PRs need
   an operator enrollment/rerun path, an available runner and an approved scope policy.
5. The shipped `rig-local` policy executes one architecture regression and is a
   pilot policy, **not a general Rig build/test contract**. Before repository-wide
   enforcement, qualify policies covering every affected PR scope, including Rust
   compilation where required. Missing coverage validators remain blockers. Do not
   waive required checks or claim one pilot test qualifies unrelated changes.
6. Prove missing, failed, stale and passing states on the real pilot, then a harmless
   new head and base incorporation. Also prove wrong-App success, bot approval,
   failed-required-test plus high score, and candidate policy weakening cannot
   authorize merge. Retain actual GitHub/App IDs, SHAs, private receipt links and
   protection readbacks. Do not perform a merge merely to demonstrate its availability.
7. Only after the all-PR validation path and rollout window are approved, apply one
   main-branch protection configuration: require `factory/validation` with the
   explicit validation App ID, strict up-to-date behavior, protection for admins,
   no routine bypass and no force push/deletion. Do not set “any source.” Prefer
   the existing admin identity for this one operation; never grant admin to the App.
   Re-read the configuration and rerun the pilot states. Merge queues are deferred.
8. Audit existing integrations and administrative authority before activation.
   Read-only discovery found no active main rules/protection and repository admins
   adampatarino, jacobwarren and MichaelSynnott on 2026-10-11. Refresh this inventory.
   An admin who can change protections remains an explicit boundary; document
   emergency changes rather than providing a standing check bypass.

Approval package: App owner/installation and protected key reference; approved
non-factory policy/enrollment path; rollout window and exact protection diff.
Proposed resource expansion: none. Validation-only runs make no model calls and
reuse the approved runner; actual worker time and permanent archive growth must be
measured on the pilot before setting an automatic-run budget. This source work
creates no billable resources. Preserve protections and report unavailable checks
on service failure; do not manufacture success as rollback.

## Local verification and safe cleanup

With the pinned Elixir/OTP environment and dependencies, from `elixir/`:

```bash
mix compile --warnings-as-errors
mix specs.check
mix test test/symphony_elixir/validation_check_test.exs \
  test/symphony_elixir/validation_github_test.exs \
  test/symphony_elixir/software_change_test.exs
```

Expected: signed synthetic fixture pass/fail/tamper states, simulated App credential
and HTTP checks, retry/restart and stale-head/base checks pass. These tests perform
no live publication, upload, protection change or merge. Test fixtures clean only
their own temporary directories; preserve recorded development logs for handoff.

Read-only live inventory, after an approved live pilot exists:

```bash
gh api repos/RigCodeAI/rig/branches/main/protection
gh api repos/RigCodeAI/rig/rules/branches/main
gh pr view PILOT_NUMBER --repo RigCodeAI/rig --json headRefOid,baseRefOid,mergeStateStatus
gh api repos/RigCodeAI/rig/commits/PILOT_SHA/check-runs
```

Replace the two explicit placeholders with the recorded pilot identities. Expect
an App-bound required check and matching current head; missing/failed states remain
blocked. Safe live cleanup preserves the pilot PR, evidence and protections; close
or delete resources only under the agreed grouped-checkpoint cleanup decision.

## Repair consumer source diagnostics

Local diagnostics: `/private/tmp/dev239-context-consumers-tests.log` (initial
119 tests, seven fixture failures), `/private/tmp/dev239-context-gate-recheck.log`
(32 passed), `/private/tmp/dev239-software-change-fixture-check.log` (one passed
with owner-provided fixture), and `/private/tmp/dev239-context-consumers-final-tests.log`
(119 passed, seed 239, same production source with that explicit fixture overlay).
The overlay is `/private/tmp/dev239-software-change-fixture-snapshot.exs`; only its
source-file directory reference was adjusted for the disposable location. These
are source tests and synthetic signed contracts, not live factory receipts. The
local container was stopped and the shared build slot released after the pass.
