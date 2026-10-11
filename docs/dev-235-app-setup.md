# DEV-235 additional app setup plan

This exact plan was approved in the Factory Orchestrator chat. Both OAuth apps
are installed with DEV-only team access. The four secret containers and four
coordinator-only grants were applied on 2026-10-11, and every secret is pinned
to enabled version `1`. Activation of the additional routes and native acceptance
remain pending. The existing coordinator and one qualified worker are sufficient.

| App | Client ID | Verified app-user ID |
| --- | --- | --- |
| High Effort Cloud Agent | `456e94735c924ab9b1148ce1a870a133` | `a7d15aec-f9a2-4806-9fbf-ca36a141178b` |
| Daybreak Cloud Agent | `56cdf6931848e6c10e5e6f27192abfb4` | `3dc1a02b-4b99-4b73-8135-c0901efe50d9` |

The app token checks resolved the expected Rig organization, DEV team and exactly
`read,write,app:assignable` scopes. Linear's installed-app settings then confirmed
**Only select teams → Dev** for both apps. All three webhook signing keys compare
distinct. Credential values and access tokens are absent from task artifacts.
See [installation evidence](evidence/dev235/app-installation.json),
[container apply](evidence/dev235/secret-container-apply.json) and
[version metadata](evidence/dev235/secret-version-metadata.json).

## Existing-run preservation

The active release hashes lifecycle and executor source into each run’s pinned
policy. Deploying changed runtime code blocks a recovered nonterminal run even
when its wait remains intact. The rejected `09706e8` activation was restored to
`fb0cad3` with its stopped schema-3 backups; DEV-252’s task and evidence match
exactly. No new delegation or worker operation occurred, and its native session
has no new activity from that activation.

Additional routes require a policy-neutral release and a stopped-service,
metadata-only exclusion update through the trusted store. Preserve policy,
status, wait, attempts and definition; never rewrite them to pass recovery.
See [the failed activation record](evidence/dev235/rejected-policy-activation.json).

The bounded helper is `factory/deploy/pin_linear_secret_names.exs`. It accepts
only the supported service policy and exact expected run/wait fields. Run it
with the compatible old runtime using `mix run --no-start`; it never starts an
orchestrator. Its operator event contains names and hashes, never values.

Before invoking it, the root activation wrapper must confirm the worker is idle,
stop the coordinator, and create integrity-checked schema-3 and evidence-index
backups. The candidate must have byte-identical runtime source. Pass the protected
candidate workflow so the helper derives all configured credential names. It
commits through `WorkstreamStore` as the store owner and verifies exact readback
plus the durable evidence outbox. Before activation, a fresh runtime that does
not load the helper must decode the stored event and prove the whole run differs
only in the exclusion names. The restarted service drains that journal.
If maintenance or activation fails, restore the compatible stopped backup before
restarting; preserve the failed candidate and receipt.

## Exact additions

Install **High Effort Cloud Agent** and **Daybreak Cloud Agent** in the existing
Rig workspace (`9b259b98-cb6c-4256-88af-3a3f385c3fa7`). Limit each app's team access
to DEV (`41e1aa00-b853-44a9-930d-79e424259565`). Enable client-credentials tokens,
`read,write,app:assignable`, Agent session events, Inbox Notifications and Issue
updates. Use redirect `https://factory.rig.ai/` and webhook
`https://factory.rig.ai/hooks/linear`, matching Default's client-credentials setup.
The service has no interactive authorization-code callback. Keep reviewer agents
internal. [Linear's setup documentation](https://linear.app/developers/agents)
requires workspace-admin installation and explains these delegation scopes.

Keep Default's existing client `571c3dc755d9e63cfe74748787f6a533` and app user
`ce9f2ad8-ef28-4253-bdc8-ae208f63a425`. No display-name routing or human-assignee
change is needed.

Create exactly four automatically replicated Secret Manager containers in
`factory-511117`. Load values privately outside Terraform; pin the resulting
numeric versions rather than `latest`. Each app's client and signing secrets
must differ, and the two resolved signing keys must differ from Default's key.

| App | Terraform key | Secret container | Coordinator environment |
| --- | --- | --- | --- |
| High Effort | `linear_high_client` | `linear-high-effort-client-secret` | `FACTORY_HIGH_CLIENT_SECRET` |
| High Effort | `linear_high_signing` | `linear-high-effort-webhook-signing` | `FACTORY_HIGH_WEBHOOK_SECRET` |
| Daybreak | `linear_daybreak_client` | `linear-daybreak-client-secret` | `FACTORY_DAYBREAK_CLIENT_SECRET` |
| Daybreak | `linear_daybreak_signing` | `linear-daybreak-webhook-signing` | `FACTORY_DAYBREAK_WEBHOOK_SECRET` |

Use the existing `optional_integration_secrets` input, which grants
`roles/secretmanager.secretAccessor` on these four containers only to the existing
coordinator service account. Confirm its email from the saved Terraform output
before applying. Do not grant the worker, agent, publisher or a project-wide
principal access. Preserve existing Default, model-auth, Git and worker-SSH pins.
The code keeps runtime files root-owned, coordinator-group-readable, and binds
new credentials to a distinct service revision with prior references available
for rollback.

Add the four key/version pairs to `optional_integration_secrets` and these four
entries to `coordinator_secret_env`, alongside the existing Default pair:

```hcl
FACTORY_HIGH_CLIENT_SECRET      = "linear_high_client"
FACTORY_HIGH_WEBHOOK_SECRET     = "linear_high_signing"
FACTORY_DAYBREAK_CLIENT_SECRET  = "linear_daybreak_client"
FACTORY_DAYBREAK_WEBHOOK_SECRET = "linear_daybreak_signing"
```

The implementation accepts complete optional pairs and rejects partial pairs.
No new VM, disk, pool, hostname, public route, model subscription, API billing,
Daybreak entitlement or project-wide IAM change is part of this plan.

Linear documents that installed agents are not billable users.
[Linear agent setup](https://linear.app/developers/agents). Four active secret
versions with automatic replication add at most $0.24/month at the published
$0.06/version/month rate before the billing account's free allowance, plus access
operations above its free allowance. This is an incremental estimate, not an
account bill or credit-coverage claim. [Secret Manager pricing](https://cloud.google.com/secret-manager/pricing).

## Values the administrator returns

For each app, return only its Client ID, app-user ID, verified workspace/team
scope, and the four secret references with numeric version pins. Resolve the
app-user ID with `{ viewer { id } }` using that app's credential; verify the Client
ID against its signed `oauthClientId`. Never send raw values to chat or persist
access tokens in the workstream database.

After those values exist, add this `cloud_agents` list under the existing
`linear_delegation` config. Replace the explicit placeholders; do not enable a
route with placeholder IDs:

```yaml
cloud_agents:
  - agent_id: high-effort-cloud
    app_user_id: <verified-high-app-user-id>
    oauth_client_id: <verified-high-client-id>
    client_secret_env: FACTORY_HIGH_CLIENT_SECRET
    webhook_secret_env: FACTORY_HIGH_WEBHOOK_SECRET
  - agent_id: daybreak-cloud
    app_user_id: <verified-daybreak-app-user-id>
    oauth_client_id: <verified-daybreak-client-id>
    client_secret_env: FACTORY_DAYBREAK_CLIENT_SECRET
    webhook_secret_env: FACTORY_DAYBREAK_WEBHOOK_SECRET
```

## Activation and acceptance

First review the Terraform plan: only four secret containers and four scoped
access grants plus the intended configuration/release update may be added. Stop
and resolve any broader resource or privilege change. Load pinned values, stage
a new coordinator release with the updated protected workflow, preserve rollback
snapshots and verify health before native delegation. Keep the worker broker,
subscription authentication and sandbox unchanged.

Run one dedicated small Rig software-change task through each qualified identity.
Verify saved definition/settings/resource hashes and effective worker settings.
Then reassign one disposable task between Default and High Effort: the old exact
operation must be terminated before the new attempt starts, the run/workspace/
branch must be the same, and old session stops/replies must have no effect.
Unsupported Daybreak must publish a clear block with no worker launch or downgrade.
Candidate publication stays disabled; the existing `.git` sandbox boundary is a
separate trusted-commit follow-up.

Approval covers these two named app installations and this four-secret/scoped-IAM
plan. Installation, secret versions and access grants are complete. Route activation
and native acceptance remain pending. See the installation and preservation
receipts linked above.

## Disposable native task draft

Title: DEV-235 disposable acceptance — reject a hook configuration file symlink

Problem: the worker seed at Rig commit
`39d6d5d998366bf13af19eb74ae47d598ecf92bd` has a directory-symlink regression
in `crates/rig-agent-hooks/src/agent.rs`, but lacks the equivalent configuration-file
symlink case. This is a small real code task for checking named-agent selection.

Scope: add `agent_hook_refuses_a_configuration_file_symlink` alongside the existing
Unix-only hook path tests. Point `.codex/hooks.json` at a file outside the clone;
assert the unsafe-path result and unchanged external contents. Reuse existing
fixtures and keep the change within that test module.

Acceptance: the new regression passes through the real Rig test command; the
saved factory run records the selected cloud definition/settings and shared skill
hashes. Use separate disposable clones for initial qualification, and preserve one
clone/run/branch during the explicit Default → High Effort reassignment case.

Verification:

```bash
cargo test --locked -p sivere-agent-hooks --lib \
  agent::tests::agent_hook_refuses_a_configuration_file_symlink -- --exact
```

Publish the draft only as a clearly labeled disposable DEV acceptance issue, keep
Adam as the human assignee, and delegate after its dedicated clone and protected
workspace mapping exist. The draft is now published as DEV-250 (Default and reassignment) and DEV-251
(High Effort), with Adam assigned and no delegates. The known `.git`
sandbox restriction means a successful coding turn and test are separate from
candidate commit/publication acceptance.
