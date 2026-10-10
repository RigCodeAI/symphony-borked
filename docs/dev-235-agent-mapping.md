# DEV-235 named-agent mapping and integration handoff

Status: source work in progress. The mapping helper has not been connected to
native dispatch or deployed. Additional apps are not installed. No acceptance
claim or PR is made by this document.

## Settings and boundaries

The merged DEV-231 definitions remain the source of model settings:

| Delegatable app | Agent reference | Model | Effort | Daybreak |
| --- | --- | --- | --- | --- |
| Default Cloud Agent | `default-cloud` | `gpt-6-luna` | medium | off |
| High Effort Cloud Agent | `high-effort-cloud` | `gpt-6-luna` | high | off |
| Daybreak Cloud Agent | `daybreak-cloud` | `gpt-6-sol` | high | on; dispatch blocked |

Default and High Effort share implementation instructions and
`factory/skills/factory-rig/SKILL.md`. The mapping records the selected definition
revision, requested settings and shared resource hashes. Daybreak readiness
returns `daybreak_execution_unverified`; neither an accepted request nor a saved
toggle proves effective Daybreak execution. There is no API billing fallback.
`default-review` resolves `DefaultReview`, `gpt-6.1-sol`/high/Daybreak off. It has
no delegatable identity. The pilot software-change definition still stops at
candidate inspection; this change does not introduce the future review pipeline.

## Requested shared-file interface

DEV-234 owns `linear/coordinator.ex`, `linear/client.ex`, durable waits, AppServer
and runner integration. No changes to these files are included yet. The following
small interface needs owner agreement before integration:

1. **Configuration and credentials** — extend `Linear.Delegation.validate/1`
   with optional `cloud_agents` and invoke `Linear.AgentMapping.validate/1`.
   Keep all backend, scope, workspace, worker-control and workstream settings
   shared. `AgentMapping.routes(config)` returns per-app config overlays;
   `select(authenticated_event, config)` requires the exact organization, app-user
   and OAuth-client tuple. Additional entries contain only `app_user_id`,
   `oauth_client_id`, `agent_id`, `webhook_secret_env` and exactly one
   `client_secret_env` or `token_env`. Never persist secret values. Read config
   through `SymphonyElixir.Config`; strip all route credential/signing environment
   names from workers through the shared runtime's explicit strip list.
2. **Webhook authentication** — `LinearWebhookController` calls the implemented
   `Config.linear_webhook_secrets(config)` and
   `AgentMapping.verify(raw_body, headers, config, secrets, opts \\ [])`. These
   require the normalized event's exact route to match the verifying app. No
   direct credential environment reads are needed outside Config. Duplicate
   resolved signing secrets reject with `ambiguous_signing_secrets`, even when
   their environment names differ. For issue-update events, which do not carry
   an app identity, use the task's pinned route after organization verification.
   One app's valid signature must never authorize another app's session. The
   existing public `/hooks/linear` endpoint is sufficient; no new ingress.
3. **Definition binding** — add a validated `:cloud_agent_id` option to
   `WorkstreamRunner.prepare/3`. Call `AgentMapping.bind(definition, agent_id)`
   immediately after loading and before workspace validation, policy pinning and
   fingerprinting. Add this string option to Orchestrator's enqueue allowlist.
   `Delegation.prepare/3` supplies the selected ID and records the returned
   `definition.agent_selection`. Both prepare and enqueue must compute the same
   bound definition digest. Initial routing binds implementation/repair stages;
   explicit reviewer stages stay unchanged. Normal resume uses the persisted
   definition rather than resolving files again.
4. **Reassignment** — the coordinator stops dispatch, revokes its stage lease,
   reconciles every existing operation and then calls
   `AgentMapping.reassign(run, target_agent_id, delivery_id)`. Unknown termination
   must reserve capacity and block replacement. The helper preserves run/task,
   branch/workspace, artifacts, attempt history, waits and repair counters. It
   records prior/current selections and prior conversation IDs, clears old
   transport-retry and conversation pointers, and uses only pinned resources.
   Commit the updated run, event and owning Linear task together before dispatch.
   Do not reuse `linear-run/<issue>`'s original enqueue event as a new migration
   identity. Late stop/prompt events from the prior app/session cannot stop or
   resume the new selection. Explicit stop tombstones remain authoritative unless
   a newer verified reassignment/delegation transition is recorded.
5. **Internal reviewers** — `AgentMapping.reviewer(definition, agent_id)` resolves
   an explicit internal review definition. The future review-stage owner consumes
   this mapping without deriving the reviewer from the cloud identity.

Build-slot readiness: this worktree has neither Elixir/mise nor a build/dependency
cache. Docker Elixir `1.19.5-otp-28` is available. The released interim heavy-build
slot has now completed compilation and focused checks below and is released again.
DEV-234 retains the live-worker slot. The checked mapping files are
`test/symphony_elixir/linear_agent_mapping_test.exs` and
`test/symphony_elixir/linear_agent_mapping_config_test.exs`, followed by relevant existing
workstream/delegation tests; integrated behavior still needs its own checks. Broad checks remain required before
engineer handoff under the alpha guidance.

## Minimum additional-app setup card

Recommendation: install exactly two Linear applications in the existing Rig
workspace, limited to DEV, using the existing backend and worker pool. Do not
install or mention internal review agents. This requires explicit installation
permission and a Linear administrator; Default installation alone does not
authorize the other two.

For each app:

1. Name it **High Effort Cloud Agent** or **Daybreak Cloud Agent**. Enable native
   agent delegation with the same minimal `read,write,app:assignable` permissions
   used by Default. Configure native agent-session, undelegation and relevant
   issue-update events at `https://factory.rig.ai/hooks/linear`.
2. Install with access limited to Rig/DEV. Record the app-user ID, OAuth client ID,
   organization/team IDs and granted scopes. Keep client and signing values in
   coordinator-only secret storage; provide references and version pins only.
   Distinct app credential/signing environment references are required. Any new
   secret container or access grant needs a separately reviewed setup plan.
3. Add the exact tuple and definition reference to the optional `cloud_agents`
   list after its shared integration is validated. No routing depends on display
   name, project membership or human assignee. Retain existing Default IDs:
   app-user `ce9f2ad8-ef28-4253-bdc8-ae208f63a425`, OAuth client
   `571c3dc755d9e63cfe74748787f6a533` (DEV-233 recorded installation).
4. Keep Daybreak dispatch blocked while effective execution is unverified. The
   installed identity can report a blocker; it must never run ordinary settings
   under the Daybreak name.

Incremental compute: zero VMs, disks, pools or public endpoints. No cloud IAM or
budget change is proposed. Model calls use the already approved subscription;
no new API billing or Daybreak elevation. Linear licensing and any additional
secret-storage charges remain to be checked before installation.

## Acceptance still needed

Run exact identity routing tests, including mismatched signer/app, duplicates,
restart and late events. Run one real small software-change task through each
qualified cloud identity, with publication disabled; inspect recorded revisions,
settings and shared skill hashes. Reassign an active disposable task and verify
the old exact operation is terminated before replacement, preserving its run,
branch and wait. Verify unsupported Daybreak blocks visibly in Linear and internal
review definitions cannot receive delegation. Keep duplicate/restart evidence
separate from automatic conversation resume, which DEV-233 did not qualify.

Until app permission/access and the shared runtime interface are supplied, live
criteria are blocked. Preserve the branch and evidence; do not create a partial PR.

## Current evidence and manual check

Lightweight interpreter checks use the existing Docker dependency image without
running Mix, starting the factory, or touching the active DEV-234 container. The
current Workstream, AgentReadiness, Webhook, Config and mapping sources are loaded
explicitly. The image's old Workstream/Config modules produce redefinition and
stale export-metadata warnings; its missing Delegation module also produces a
warning in an uncalled existing Config function. All 14 helper/config tests pass.
This interpreter command provides helper-level verification. The later full
application compile and focused Mix run are recorded separately below.

From this managed checkout, run:

```bash
docker run --rm --network none --cpus 1 --memory 1g \
  -e ERL_FLAGS='+S 1:1' -v "$PWD:/source:ro" dev230-checked:latest \
  elixir -pa '/workspace/elixir/_build/test/lib/*/ebin' \
  -r /source/elixir/lib/symphony_elixir/agent_readiness.ex \
  -r /source/elixir/lib/symphony_elixir/workstream.ex \
  -r /source/elixir/lib/symphony_elixir/linear/webhook.ex \
  -r /source/elixir/lib/symphony_elixir/linear/agent_mapping.ex \
  -r /source/elixir/lib/symphony_elixir/config.ex \
  -e 'ExUnit.start(); Code.require_file("/source/elixir/test/symphony_elixir/linear_agent_mapping_test.exs"); Code.require_file("/source/elixir/test/symphony_elixir/linear_agent_mapping_config_test.exs")'
```

Expected: 14 tests, zero failures. `--rm` removes only the disposable interpreter
container; no other cleanup is needed. The named cache image is local development
setup, not a published worker dependency or portable production recipe. The normal
Mix test command remains required after the shared build slot is released.

| Criterion | Current evidence | Remaining |
| --- | --- | --- |
| Each identity records settings | Exact tuple routing and binding tests load the committed software-change/agent YAML, inspect settings and revision/resource hashes | Native coordinator records and real Default/High Effort tasks |
| Reassignment preserves task/branch and one writer | Helper tests reject unresolved/canceled-without-proof writers; preserve task/branch/history/counters/wait; clear old retry/conversation pointers | Atomic coordinator integration, restart/race tests and live proof |
| Internal reviews and unsupported Daybreak | Mapping rejects reviewer identities; explicit reviewer lookup stays separate; Daybreak binding records the exact requested program and readiness rejects it | Visible Linear blocked activity and installed-app inventory |
| Shared workstream/skill and safe transition | Same YAML/shared hashes with different effort; reviewer stage fixture stays explicit; recorded transition is idempotent | Actual review-stage integration belongs to DEV-240; durable safe-transition integration still required here |

Self-review corrected human-wait restoration after a stopped run and prevented
reuse of the previous agent's transport side-effect/conversation identity on a
new selection. Shared files have not been edited beyond the Config accessor;
no automatic resume, app install or live dispatch is claimed.

The targeted independent GPT-6 Luna/max review found that different signing
references could resolve to the same value. The verifier now rejects such a
configuration before authentication; its regression test signs a Daybreak-app
body with the reused key and requires `ambiguous_signing_secrets`. The reviewer
reported no further issue in the inspected reassignment/wait paths.

## Interim application checks, 2026-10-10

The coordinator released a bounded heavy-build slot for checkpoint
`e019b409b064c901944426055ff1dcfb6300e549`. An isolated Docker container copied
that Git archive over the existing dependency cache, removing old source
directories first. The active DEV-234 and DEV-233 containers/files were preserved.
No shared-file integration or live worker probe ran.

The complete application compiled with warnings treated as errors (76 source files).
The first focused run found one failure: the existing software-change fixture
copied only Default Cloud, but the updated workstream also references High Effort,
Daybreak and Default Review. The fixture now copies all shipped agent definitions
and their shared resources. Its simulated stage result remains test-only.

After that fix, these exact commands passed from the isolated `elixir/` directory:

```bash
MIX_ENV=test mix compile --force --warnings-as-errors
MIX_ENV=test mix test \
  test/symphony_elixir/linear_agent_mapping_test.exs \
  test/symphony_elixir/linear_agent_mapping_config_test.exs \
  test/symphony_elixir/workstream_test.exs \
  test/symphony_elixir/workstream_runner_test.exs \
  test/symphony_elixir/linear_delegation_test.exs \
  test/symphony_elixir/linear_webhook_test.exs \
  test/symphony_elixir/linear_http_test.exs \
  test/symphony_elixir/agent_readiness_test.exs \
  test/symphony_elixir/software_change_test.exs
mix specs.check
mix format --check-formatted \
  lib/symphony_elixir/config.ex \
  lib/symphony_elixir/linear/agent_mapping.ex \
  test/symphony_elixir/linear_agent_mapping_test.exs \
  test/symphony_elixir/linear_agent_mapping_config_test.exs \
  test/symphony_elixir/software_change_test.exs
```

Expected and observed: compile success, **112 tests with zero failures**, all public
functions specified, and formatting success. The [receipt](evidence/dev235/checkpoint-validation.json)
pins exact tested source hashes and commands; retained [compile](evidence/dev235/compile.log),
[focused tests](evidence/dev235/focused-tests.log) and [specs](evidence/dev235/specs.log)
outputs record the result. Docker used two CPU schedulers/cores and 4 GiB memory.

Both DEV-235 containers and the temporary DEV-235 cache image were removed. The
heavy slot is released. Broad CI remains queued for final integration as directed;
this interim run is not engineer handoff or live acceptance. Additional app
installation, visible Linear blocking, atomic reassignment integration and real
qualified cloud-identity tasks still remain.
