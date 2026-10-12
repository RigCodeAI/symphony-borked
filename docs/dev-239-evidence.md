# DEV-239 source evidence and rollout boundary

Development PR: <https://github.com/RigCodeAI/symphony/pull/20>.
Tested source commit: `aa8583d6968fa6fced8c5bdf19a47e593c746745`.
This record-only follow-up does not change the tested implementation.

Branch: `cycle/dev-239`. Source base: merged DEV-238 PR 18,
`e7fd8290ed7e508547867c9a5233d9b9eb0bfd16`. GitHub independently confirmed merge.
The orchestrator released source implementation without claiming DEV-238's live
publication acceptance. No App, permissions, protection, worker or deployment
settings were changed by this ticket owner.

## Acceptance matrix and self-review

| Ticket criterion | Source evidence | Remaining live boundary |
| --- | --- | --- |
| Missing/failed evidence blocks merge; expected App alone qualifies | Dedicated scoped App transport validates installation/App/token/check identities; registered gate authenticates exact receipt and archive; wrong-App/neutral/skipped tests fail closed | Real App installation, real pilot and App-bound Rig main protection not performed |
| Out-of-band head/base integration requires matching evidence before readiness | Actual head/main identity before and after writes; postwrite identity change demotes exact check; async completion verification; persisted wait reconciliation and queued reply tests | Actual external push/base incorporation and live review/readiness demonstration not performed |
| Approval cannot replace validation; remove bypass/document admins | Ordinary and approval waits retain queued replies until owned fresh validation succeeds; docs provide no-routine-bypass/admin-enforcement proposal and observed admin boundary | Actual protection/bypass audit and settings readback after approved rollout still required |
| High score cannot override failed test; candidate cannot weaken contract | Required executable assertion recomputation, signed high-score/failed/skipped fixtures, candidate policy-edit and stale-policy tests | Real pilot evidence failure/policy-tamper exercise not performed |

Self-review checked the final diff against these four criteria, ownership boundaries,
credential exclusion, duplicate/restart semantics and unchanged human-only merge.
No candidate/test code receives App tokens or signing keys. No merge API was added.
The validation-only flow permits only registered service checks and human waits;
its pilot policy is not treated as sufficient validation for arbitrary Rig changes.

## Independent material-risk review and fixes

GPT-6 Luna/max found four concrete risks. The source addresses each:

1. Lost check responses previously stranded recovery. Registered GitHub validation
   checks now reconcile exact remote identity before safe retry; generic executable
   checks retain their previous recovery behavior. The dropped-create/store and
   coordinator process-loss regression confirms one reused check.
2. A base change during success could leave green on the old head. The postwrite
   check now demotes that exact App/name/head check before returning failure.
   Blocked enrolled runs remain reconciled; protection is still required separately.
3. Approval waits skipped freshness. Both approval and ordinary replies now queue
   with the original wait/attempt/artifacts/activity identity and require fresh
   validation before consumption. Stale head/base, changed artifacts and stopped
   ownership remain blocked.
4. Human reply callbacks performed synchronous GitHub reads. All remote verification
   runs in owned async tasks. Stage completion uses a domain-separated service-key
   observation seal; the coordinator authenticates the fresh local proof/seal.
   A delayed transport test proves the coordinator still answers synchronous calls.
   Failed monitoring retains the pending reply; duplicate replies do not advance twice.

A narrow follow-up found no attestation forgery or reply replay. Its two residuals
were bounded fixes: advance now wakes only pending reply monitors promptly, and
registered stage-verifier ownership is checked between GitHub reads. Focused tests
cover prompt wake, cancellation during a paused read, and altered/expired seals.
The observer initially used a strict boolean operator on a nil reservation, which
restarted its coordinator; the OTP monitor regression reproduced it and now passes.

## Checks actually run

Own local container `dev239-check`, pinned image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`,
Elixir 1.19.5 / OTP 28, 6 GiB limit, d2f9 worktree bind and separate
`dev239-deps` / `dev239-build` volumes. Only the stopped DEV-238 dependency volume
was copied read-only. Other ticket containers and cloud workers were not modified.

- `mix compile --warnings-as-errors`: passed.
- `mix specs.check`: passed.
- `mix format --check-formatted`: passed.
- New gate/transport/shipped-flow suite: **43 tests, zero failures**.
- Final related validation/publication/workstream/durable lifecycle suite:
  **117 tests, zero failures, one Docker validation test excluded**.
- `git diff --check`: passed.
- `make all`: executed; failed at strict Credo, including baseline and new
  refactoring/readability categories. Setup/build/format/spec stages were reached.
  Coverage and Dialyzer were not reached. Broad CI is not green.

Development logs are `/private/tmp/dev239-focused-final.log` and
`/private/tmp/dev239-make-all.log`. They are local diagnostics, not trusted factory
receipts. Synthetic signed manifests, archive confirmations and simulated GitHub
responses exist only inside tests. No live GitHub validation or GCP archive success
is claimed. The final bounded recheck also passed after both follow-up fixes; PR-body format
validation passed. The heavy slot was released to the orchestrator after the focused pass;
this ticket's local container was stopped and its volumes retained.

## Dependencies and operator package

The [concrete rollout plan](trusted-validation-check.md) covers the minimum separate
App, protected secret references, validation-only ordinary PR enrollment and policy,
pilot states, explicit App-bound strict protection, admin boundary and safe recovery.
No resource expansion or model calls are proposed by the ordinary validation route.
Real worker time and archive growth require pilot measurement before an automatic
run budget. Existing Rig PRs must have a qualified path before enforcement.

DEV-242's capture hold is unmerged into this source: integration must gate validation
scheduling, the monitor and stage-verifier owner callbacks, async stage-result acceptance and drain all
`validation_checks` tasks. The orchestrator has the exact seam list. Combined
capture readiness is not claimed. DEV-243 owns child linkage and DEV-241 cleanup;
this increment does not alter Store/Evidence ownership or those barriers.

## Short reproducible local demo

On this host, after acquiring the shared build slot:

```bash
/opt/homebrew/bin/docker start dev239-check
/opt/homebrew/bin/docker exec dev239-check mix test \
  test/symphony_elixir/validation_check_test.exs \
  test/symphony_elixir/validation_github_test.exs \
  test/symphony_elixir/software_change_test.exs
/opt/homebrew/bin/docker stop dev239-check
```

Expected: the 43 source/contract tests pass with no live App, PR, upload or merge.
Tests delete only their own temporary fixtures. Stop only this ticket's container;
preserve its cache volumes and diagnostics until the grouped checkpoint is reviewed.
Live demo commands and expected readbacks are in the rollout plan and remain gated
on the real publisher pilot, App/access choices and approved all-PR validation path.
