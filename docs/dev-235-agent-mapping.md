# DEV-235 named cloud-agent routing

Status: controlled integration verified; additional app setup and native acceptance pending. Additional apps are
not installed. No complete acceptance claim or PR is made by this document.
The read-only 2026-10-10 preflight observed coordinator `4ea729c6` still active;
DEV-234's merged upgrade correction is included in this branch for its next
scoped deployment.

## Settings and boundaries

The merged DEV-231 definitions supply the settings:

| Delegatable app | Agent reference | Model | Effort | Daybreak |
| --- | --- | --- | --- | --- |
| Default Cloud Agent | `default-cloud` | `gpt-6-luna` | medium | off |
| High Effort Cloud Agent | `high-effort-cloud` | `gpt-6-luna` | high | off |
| Daybreak Cloud Agent | `daybreak-cloud` | `gpt-6-sol` | high | on; dispatch blocked |

Cloud agents share `factory/instructions/cloud-implementation.md` and
`factory/skills/factory-rig/SKILL.md`. The selected definition revision, requested
settings and shared resource paths/hashes are stored as
`run.definition.agent_selection`. Resource bytes remain pinned on the run.
Daybreak dispatch returns `daybreak_execution_unverified`; no downgrade or API
billing fallback is allowed. Default Review is an explicit internal definition,
`gpt-6.1-sol`/high/Daybreak off. It cannot be installed as a delegation identity.
The local software-change definition still pauses after candidate inspection;
the future review/publication pipeline is separate.

## Configuration and launch

The existing Default app fields remain at the root of `linear_delegation`.
Optional `cloud_agents` contains at most two additional cloud identities. Each
entry has only `app_user_id`, `oauth_client_id`, `agent_id`, `webhook_secret_env`
and exactly one `client_secret_env` or `token_env`. Organization/team/scope,
worker control, workspace mapping, store and workstream remain shared.
[The setup plan](dev-235-app-setup.md) provides exact proposed environment and
secret references; actual app IDs must be verified before enabling routes.

`Config.linear_delegation/0` validates mappings. `Config.linear_webhook_secrets/1`
resolves every signing reference; the HTTP receiver authenticates the original
bytes against a distinct key and requires the exact signing app/client tuple.
Resolved duplicate signing keys fail closed, even when references differ.
Issue updates without an app tuple retain signer provenance and use the current
task's pinned route. A prior app's issue update cannot change the new owner.
The same public `POST /hooks/linear` endpoint handles every installed identity.

`WorkstreamRunner.prepare/3` accepts `cloud_agent_id`, binds the selected cloud
stages before workspace/policy pinning and definition fingerprinting, and leaves
explicit reviewer stages unchanged. Enqueue uses the same bound digest. Ordinary
resume executes the saved definition without rereading files. All configured
service credential/signing names are pinned in execution context and stripped
from local and remote agent launches, including shell-profile reintroduction.
Older pinned launches lacking required exclusions block before reuse. Check
processes retain their existing environment allowlist.

Deployment accepts Default plus complete optional High Effort/Daybreak secret
pairs with numeric version pins. The protected entrypoint exports a nonsecret
`FACTORY_LINEAR_SECRET_NAMES_JSON` marker; Config checks that the actual workflow's
credential references match those pins using its existing YAML reader. Local and
upstream workflows without that deployment marker retain compatibility. The
marker itself is excluded from new agent launches.

## Safe reassignment

Only a newer authenticated created event for a different configured cloud agent
requests reassignment. Session prompts do not change models. Stop tombstones
prevent silent same-agent restarts. The coordinator revokes stage authorization,
records a pending handoff, and uses its existing cancellation/reconciliation path.
Unreachable, executing or incompletely canceled operations prevent replacement;
unknown liveness retains execution capacity.

After trusted quiescence, `AgentMapping.reassign/3` uses only the run's pinned
agent/resource definitions. It preserves task/run IDs, workspace, branch,
artifacts, outputs, attempt history, pending waits/questions/inbox and repair
counters. Old transport retry and conversation pointers are cleared. Clarification
and approval waits retain their existing wait rather than launching immediately.
A new agent attempt receives a fresh conversation after the preserved answer.

`WorkstreamStore.linear_reassign` atomically writes the resolved run, new task
ownership and processed delivery receipt, checking the previous run/task snapshots
inside the transaction. A failed/conflicting write advances none of them. Only
then can the new app acknowledge and receive a dispatch lease. Prior session
publications remain in assignment history; late old-app/session stops and prompts
cannot affect the new selection. Stop receipts are scoped to assignment generation
and attempt so a second safe handoff is independently durable.

## Archive integration seam for DEV-237

DEV-235 has no dependency on the unmerged Evidence module. A successful atomic
reassignment is a run commit boundary alongside queue/stage/question/reply/stop
commits; DEV-237 must snapshot it once using `linear-reassignment/<delivery-id>`
or the recorded run event ID. Stable archive URLs remain keyed by run/task ID.

`run.agent_transitions` is append-only. Every element records `id`, `from` and `to`
selection maps (agent ID/name/revision, requested model/effort/Daybreak/auth ref,
resource paths/hashes), plus previous conversation IDs and cancellation evidence.
All definition resource pins remain present on the resolved run. Archive identity
must preserve the initial pins and each authorized selection revision, rather
than reject every changed resolved-definition digest under the same run ID.
An unrecorded definition/policy/resource mutation still rejects. Use the matching
transition ID and continuous prior/current selection to authorize a revision;
never treat arbitrary snapshot contents as a transition. Keep each historical
manifest/readable transcript and the current selection accessible. Publishing and
validation gates remain separate service decisions.

The pending integration hooks are documented in DEV-237's
`docs/evidence/dev-237/integration-contract.md`; the root coordinates their shared
file merge after DEV-235 stabilizes. DEV-237's current first-write-only pin rule
must change before archive integration can accept a reassigned run.

## Verification and pending live acceptance

The integrated application compiles with warnings treated as errors (77 files),
and **164 focused tests pass**. Specs, formatting and the escript build pass. The
checks cover authenticated per-app routing, saved settings/shared resources,
atomic handoff/reopen/rollback, stale events, unknown termination, question and
reply preservation, older-run decoding, credential exclusion and unsupported
Daybreak visibility. Deployment tests pass: 17 protected-runtime cases and 19
cloud-I/O cases (four existing platform-dependent skips). Terraform 1.9.8
formatting checks pass for both modified variable files; no Terraform plan/apply
has run.

The preliminary runs exposed and repaired an orphan-run credential exclusion
case, an initial-delegation boolean crash, duplicate scope invalidation and two
new test-fixture mistakes. The final passing receipt records the tested source
hashes and exact commands. Broad results and the bounded coordinator deployment
are recorded separately below. These controlled checks do not prove additional
apps are installed or live reassignment.

Live acceptance still requires one small real Rig task through each qualified
identity, saved requested/configured settings and shared skill hashes, an exact
old-operation termination before reassignment, and visible unsupported Daybreak
blocking with no worker launch. Preserve the human assignee. Keep actual live
results separate from contract fixtures and restart/duplicate tests.

DEV-234's `.git` read-only sandbox diagnosis remains applicable. Do not broaden the
sandbox or claim candidate publication. A trusted local commit step remains its
separate follow-up. Until app setup and native multi-app acceptance are complete,
retain the local branch and evidence; do not create a partial or placeholder PR.

## Integrated check evidence, 2026-10-10

The [integrated receipt](evidence/dev235/integrated-validation.json) records exact
commands and source hashes. [Focused tests](evidence/dev235/integrated-focused-tests.log)
pass 164/164; the separate [full suite](evidence/dev235/integrated-full-tests.log)
passes 553 tests with six skips and ten exclusions. Validation-Docker and native
live cases are not enabled. Compilation, public specs, formatting and escript
build logs are retained alongside the receipt.

`make all` ran in the background and [stopped at Credo](evidence/dev235/integrated-all.log).
Its saved log has trailing spaces removed; the raw container output is retained
for this run. It reports existing and new readability/design/complexity findings; coverage and
dialyzer were not reached. This is an alpha nonblocking broad-CI result, not a
green CI claim. The separate full test run covers the broad behavior suite.

One concise self-review checked signer routing, resolved-key uniqueness, credential
exclusion on new/recovered runs, stage leases, old-writer termination and atomic
CAS handoff. The passing focus includes DEV-234 questions/replies and its additive
legacy decoder. No additional formal review was required.

## Historical helper checkpoint checks, 2026-10-10

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

That checkpoint's containers and temporary image were removed and its slot released.
Those results cover the helper checkpoint only; the integrated source requires the
separate checks above. They do not establish native multi-app acceptance.
