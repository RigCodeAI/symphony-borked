# Native Linear delegation (DEV-233)

The current coordinator and root worker control use source commit
`064f892f95847ea2eed0eb684da36c6e1bb5fce6`, archive SHA-256
`67267e73e9c587f0b6115398d89cdac462b23dcb8749df34895a34b9d0a0af89`. The worker app remains
`541279aeb6a5366571e1ee8935e134c728d91c63`.

DEV-248 passed the scoped native delegation, restart-survival, active-stop and durable-metadata
acceptance. DEV-246's earlier restart attempt failed when SSH stream closure ended the worker;
DEV-247's separate live undelegation test stopped its operation with exact root proof. Startup
repair reconciled the old confirmed stops without repeating cancellation. Rig candidate publication and merge
were not attempted. DEV-234 adds [durable question/reply handling](linear-questions.md); native human
prompt activities now supply persisted reply inputs rather than being discarded as duplicate delegation. See the [restart repair acceptance record](evidence/dev-233/restart-repair-acceptance.md)
and [earlier native checkpoint](evidence/dev-233/native-live-acceptance.md).

## Configuration

Use a dedicated factory workflow, never the upstream `elixir/WORKFLOW.md`. Add this map to
its YAML front matter, replacing every example ID and path before startup:

```yaml
tracker:
  kind: memory
server:
  port: 4000
linear_delegation:
  organization_id: installed-organization-uuid
  team_id: dev-team-uuid
  app_user_id: installed-app-user-uuid
  oauth_client_id: installed-app-client-id
  webhook_secret_env: LINEAR_API_TOKEN
  client_secret_env: LINEAR_API_KEY
  store_path: /srv/factory/state/linear.sqlite
  workspace_root: /srv/factory/contained-workspaces
  workstream_path: /srv/factory/definitions/software-change.yaml
  agent_id: default-cloud
  rig_label: factory:rig
  reconcile_interval_ms: 5000
  workspaces:
    pilot-issue-uuid: /srv/factory/contained-workspaces/pilot-rig
```

The contained-workspace parent on the worker is root:factory-worker 0750. An
operator must provision the worker-owned issue leaf and matching coordinator
clone before adding the issue mapping. The worker can edit inside the leaf but
cannot create or replace sibling names. The legacy `/srv/factory/workspaces`
parent remains unchanged and is not accepted by the contained broker. See the
[contained operation setup](contained-worker-operations.md).

Keep the signing secret and OAuth client secret in separate coordinator environment variables.
`LINEAR_API_TOKEN` above is the webhook signing secret; `LINEAR_API_KEY` is the OAuth client
secret despite the legacy variable name. Neither value belongs in YAML, logs or evidence.
For this increment, the two references must be distinct names in `LINEAR_API_KEY`,
`LINEAR_API_TOKEN`, or `OAUTH_TOKEN`, which the current agent launch strips. Custom references
block before dispatch until the runtime supports a pinned explicit strip list.

`client_secret_env` enables server-to-server app authentication with fixed scopes
`read,write,app:assignable`. The coordinator mints a token on the first API lookup for
each delegated issue run and keeps it in memory only, scoped to that run. This increment
allows one immutable run per issue. It renews before expiry or once after HTTP 401;
a second 401 fails rather than looping. Network/token failures are redacted, with no
automatic retry or redirect. Restart discards tokens and mints fresh ones as needed.
The cache is bounded to 1,000 unexpired run entries; an exhausted cache blocks new
authentication. Tokens and client credentials do not enter task records, evidence or
worker environments. Existing `token_env` workflows remain compatible; configure exactly
one of `token_env` and `client_secret_env`. Do not use copied access tokens as durable
production credentials. See [Linear client-credentials authentication](https://linear.app/developers/oauth-2-0-authentication).

The cache limit bounds process memory, not Linear's app-wide token quota. Tokens discarded
by a crash/restart or a blocked candidate can remain active at Linear until their provider
expiry (up to 30 days). Repeated restarts/runs can therefore exhaust the provider's
1,000-active-token limit; acquisition then fails closed. This single-run pilot does not
implement app-wide token lifecycle/revocation accounting. Before sustained operation or
scaling, add that lifecycle and verify it against the installed app; do not infer quota
recovery from local cache pruning or restart. Operator secret rotation invalidates existing
client-credentials tokens and requires a new pinned secret version/release.

During preflight, revoking the last client-credentials token made the app user inactive. For
the observed delegation, the coordinator retained a bootstrap token in bounded RAM for up to
180 seconds through routing, then revoked it after acquiring the per-run token. This observed
startup path is not a proven long-term token-quota or app-availability solution.

## Linear app setup

Create a private OAuth app named **Default Cloud Agent** and enable client-credentials
tokens and webhooks. Use `read`, `write`, `app:assignable`; no `admin` scope is required.
`app:mentionable` is optional. Limit app-user team access to DEV. Subscribe to Agent session
events (`AgentSessionEvent`), Inbox Notifications (`AppUserNotification`) and Issue updates.
Permission Change events are not consumed by this increment. Store separate
`linear-client-secret` and `linear-webhook-signing` values privately in Secret Manager;
share only the Client ID and secret references/versions with the deployment operator.

Use the app token's `{ viewer { id } }` query to get its app-user ID. Copy the exact Client ID
from app details and verify it against signed events' `oauthClientId`; do not assume that
field is an OAuth object UUID. Rig organization is `9b259b98-cb6c-4256-88af-3a3f385c3fa7`,
DEV team is `41e1aa00-b853-44a9-930d-79e424259565`. Confirm the `factory:rig` label and use
one disposable DEV issue with its human assignee intact; select the app as delegate.

The selected registration values are redirect URL `https://factory.rig.ai/` and webhook
URL `https://factory.rig.ai/hooks/linear`. The redirect is an unused dashboard landing
URL for the client-credentials grant; this service implements no interactive OAuth
callback or authorization-code exchange. Do not start an interactive authorization flow
with it. The dashboard HTTPS load balancer is installed with IAP for `domain:rig.ai`;
certificate issuance and the exact webhook route are healthy. Signed-in dashboard access
remains untested; see the [ingress evidence](evidence/dev-233/https-ingress.md). The installed
app has client ID `571c3dc755d9e63cfe74748787f6a533`, app-user ID
`ce9f2ad8-ef28-4253-bdc8-ae208f63a425`, and `read,write,app:assignable` scopes. The app token
is limited to the Rig organization and DEV team. A workspace admin completed app setup. See
[Linear agent setup](https://linear.app/developers/agents),
[app authentication](https://linear.app/developers/oauth-actor-authorization) and
[interaction best practices](https://linear.app/developers/agent-best-practices).

The configured paths must be absolute. The issue workspace must already exist as a dedicated
Git clone beneath `workspace_root`. No clone is created implicitly. The definition must be
named `software-change`, with an agent entry using `default-cloud`; its instruction, skill
and agent references must resolve. A qualified `AgentReadiness.dispatch/1` implementation is
required for production dispatch. Its absence fails closed with `worker_readiness_unavailable`.
The shipped [software-change definition](../factory/workstreams/software-change.yaml) references
DEV-231's `factory/agents/default-cloud.yaml`. It implements a local candidate, checks that
HEAD exists and the working tree is clean, then pauses at a durable human wait. This local
inspection does not qualify arbitrary Rig candidates using the
demo validation policy or publish them. DEV-231 supplies the shared agent/readiness implementation; the intended worker must
remain qualified before production dispatch.
Production qualification also requires `worker_control` with an exact machine and verified
release, plus a current root-owned containment receipt. Earlier qualification deployed worker
control revision `a4c95481b898d05817d24cbcf76635d9c4b739c5` on the pinned worker under systemd
252 and passed its then-current transport suite and root card. The current source commit
`064f892f95847ea2eed0eb684da36c6e1bb5fce6` passed the seven-check root card, 16 transport tests,
eight actual SSH checks and real Elixir preflight for four candidate routes, default callback and
invalid references. The worker app remains `541279aeb6a5366571e1ee8935e134c728d91c63`. App
Server startup and subscription readiness passed on GPT-6 Luna, medium effort, Daybreak disabled;
it made no model turn and stopped with exact termination proof. See the [restart repair
acceptance record](evidence/dev-233/restart-repair-acceptance.md), [deployment checkpoint](evidence/dev-233/transport-deployment.md)
and [contained worker operations](contained-worker-operations.md). Controlled test callbacks
cannot enable production control.

From `elixir/`, start the configured workflow with:

```bash
mise exec -- mix run --no-start -e 'SymphonyElixir.CLI.main(System.argv())' -- \
  --i-understand-that-this-will-be-running-without-the-usual-guardrails \
  /absolute/path/to/factory/WORKFLOW.md --port 4000
```

Use the built Mix project for durable SQLite workflows: Exqlite requires its native library
in the dependency's physical `priv` directory, which the escript does not package. The
managed coordinator service uses this same CLI startup path from its release-specific
built project, without compiling or fetching dependencies during startup.

The service only accepts the configured organization/app identity. It fetches the current
issue and session through the app token. Eligible issues have the configured DEV team,
`DEV-` identifier, `factory:rig` label, app delegate and Backlog, Todo or In Progress status.
Completed/canceled state types reject. Project membership never dispatches work.

## Event and recovery behavior

- `AgentSessionEvent` created: validate current ownership/session and routing, queue one run,
  persist its activity UUID, reconcile acknowledgement through `agentActivity(id)`, then allow
  a freshly authorized stage. The acknowledgement identifies the run and disabled publication.
- Duplicate delivery: return HTTP 200 without repeating execution. A reused delivery ID with
  different normalized contents rejects. A repeated session-created event reuses the task.
- Prompted stop and `issueUnassignedFromYou`: persist a permanent stop tombstone, request
  cancellation and reject advancement from later queued events.
- Issue delegate/state updates: clear the authorization lease and recheck current scope;
  an earlier in-flight check cannot restore a cleared lease.
- Coordinator restart: reuse existing worker/run identities, reconcile pending activity IDs,
  replay pending stop receipts and reuse pinned definitions even if their source file changed.

Qualification records the resolved definition digest. Enqueue checks that digest again;
changing the definition between qualification and enqueue blocks before creating a run.
An orphaned existing run reuses its pinned definition and rechecks readiness and current
credential isolation before linking it to the task.

The dashboard/JSON snapshot exposes issue, session, run, selected agent, current stage, attempt
and cancellation outcome. Receipt records omit prompt text and credentials. Stopped tasks
retain their evidence and workspaces. There is no automatic cleanup, merge or redelegation
reset in this increment.

A stage executor receives an `on_process_start` callback. It must hold execution until the
coordinator durably accepts the operation's external identity. The production registration
path accepts a systemd operation identity: operation ID, machine, OS boot, unit,
invocation, cgroup and request digest. Only the current supervised worker can register
it. Repeated identical registration is safe across coordinator restart; conflicting or
stale identity rejects. The Codex launcher and executable checks use the same held-launch
control path when configured.

The provisional local process-group adapter remains available for controlled tests. It
always reports `unknown`, since a descendant can escape with `setsid`. Terminating the
Elixir task or SSH client cannot prove remote termination. Stop commits immediately, then
bounded supervised cancellation verifies the exact worker invocation and its whole cgroup.
Unknown execution reserves capacity through restart. Reconciliation frees the slot only after
trusted termination proof; the stopped task cannot restart. A completed SSH stream without proof
leaves the run reconciling. Earlier DEV-246 testing failed at this boundary because SSH stream
closure ended the worker; DEV-247 separately proved active undelegation. DEV-248 passed restart
survival with all seven worker process identities unchanged and no replacement launch, then
native undelegation stopped the same live operation. The final audit found four stopped runs,
four trusted termination proofs, no executing attempts and no operation awaiting reconciliation.
See the [restart repair acceptance record](evidence/dev-233/restart-repair-acceptance.md) and
[earlier native checkpoint](evidence/dev-233/native-live-acceptance.md).

The verifier bounds raw intake to 256 KiB, requires unique signature/delivery/event headers,
checks HMAC-SHA256 over original bytes and checks the signed `webhookTimestamp` within
60 seconds. It returns 401 for failed authentication, 400 for malformed requests, 422 for
unsupported/scope-unrelated events and 503 when disabled/unavailable. Durable intake precedes
HTTP 200; asynchronous API/readiness checks do not hold the webhook response open.

## Engineer test card

From `elixir/`:

```bash
mise exec -- mix compile
mise exec -- mix specs.check
mise exec -- mix test test/symphony_elixir/software_change_test.exs test/symphony_elixir/linear_store_test.exs test/symphony_elixir/linear_agent_client_test.exs test/symphony_elixir/linear_webhook_test.exs test/symphony_elixir/linear_http_test.exs test/symphony_elixir/linear_delegation_test.exs test/symphony_elixir/workstream_cancellation_test.exs test/symphony_elixir/workstream_store_test.exs test/symphony_elixir/durable_workstream_test.exs test/symphony_elixir/extensions_test.exs
mise exec -- mix run --no-start ../factory/scripts/linear-intake-smoke.exs /tmp/factory-linear-intake-demo
mise exec -- make all
```

The HTTP test sends signed, pretty-printed JSON to a real local server, repeats the delivery,
changes its raw bytes and changes the app identity. Lifecycle tests use controlled workers,
a real SQLite store and a real executable gate. They prove acknowledgement ordering, one
run/worker through restart, pinned definitions, routing failures, stop tombstones and no
subsequent stage after controlled cancellation. They do not run a model or contact Linear.
Each fixture owns a fresh temporary directory and removes only that directory after its test.
Linux-only cancellation tests launch real held processes and process groups, including a
TERM-ignoring child and an escaped descendant. They verify identity fencing, bounded cleanup,
durable registration before effects and preservation of unknown outcomes. They skip on
other operating systems and do not prove a remote systemd boundary.

The smoke requires a new absolute directory and retains `report.json`, its workflow and
SQLite database there. It starts the real application through workflow configuration, uses
only a loopback ephemeral HTTP port, sends a controlled undelegation receipt and verifies
duplicate/invalid-signature behavior across coordinator restart. Inspect its report before
removing only that disposable output directory. It makes no Linear API call or agent turn.

## GCP coordinator deployment

The source default remains the idle pilot. GCP is running the Linear workflow on coordinator
and root worker-control source commit `064f892f95847ea2eed0eb684da36c6e1bb5fce6`, archive
SHA-256 `67267e73e9c587f0b6115398d89cdac462b23dcb8749df34895a34b9d0a0af89`. The worker app
remains `541279aeb6a5366571e1ee8935e134c728d91c63`. See the
[restart repair acceptance record](evidence/dev-233/restart-repair-acceptance.md).
Map exactly `LINEAR_API_KEY` to the OAuth client secret and `LINEAR_API_TOKEN` to the
separate signing secret using existing `optional_integration_secrets` keys and pinned
numeric versions. Pilot mode accepts no credential mapping. No payload goes through Terraform.

Startup fetches selected values as root into per-release files under
`/run/factory/coordinator`, root-owned mode 0640 with only the coordinator group allowed
to read. The unprivileged entrypoint checks ownership/mode, release, finite workflow,
environment allowlist, version and reference fingerprints before exec. Inherited credential
variables are discarded. Worker environments continue to strip these names and the root
broker uses its fixed credential-free runtime environment.

Install the dedicated workflow as root at
`/etc/factory/workflows/<service_revision>.md`, root-owned mode 0644 under root-owned
0755 directories. Use a new release revision and do not overwrite an existing workflow.
The workflow must set dashboard `server.host: 0.0.0.0`, `server.port: 8080`,
`server.webhook_host: 0.0.0.0` and `server.webhook_port: 8081`, plus the real IDs,
dedicated workspace and pinned worker-control configuration described above.
The entrypoint uses the matching protected pending config during candidate startup;
rollback uses the prior revision's active config and workflow. Configuration/credential
pin changes at the same release revision reject before any secret fetch. Make a distinct
committed release for those changes.

Root-private snapshots in `/srv/factory/coordinator-config` retain only reference/version
and deployment settings, never fetched payloads. On cold boot, startup reloads the prior
active Linear release's pinned credentials before preparing a different candidate, so
health rollback can start the previous release. Preserve its secret containers/access and
workflow until the new release is healthy. Missing snapshots or unavailable prior secrets
block preparation rather than silently losing rollback. Do not include credential files
in evidence or copy them to workers.

The deployed HTTPS URL map keeps the IAP dashboard backend as default. The exact
`https://factory.rig.ai/hooks/linear` route reaches the separate no-IAP backend on port 8081
and is healthy. Dashboard and all other paths retain IAP. The webhook listener accepts only
signed POST intake and returns 404 for dashboard/API/other paths and methods. Firewall access
is limited to Google load-balancer proxy and health ranges.

See the [current decision card](dev-233-morning-decision.md) for deployment revisions,
the observed run results and remaining qualification.

## Current live status and remaining qualification

The current coordinator and root worker control use source commit
`064f892f95847ea2eed0eb684da36c6e1bb5fce6`, archive SHA-256
`67267e73e9c587f0b6115398d89cdac462b23dcb8749df34895a34b9d0a0af89`. The worker app is
unchanged. App Server startup and subscription readiness passed with GPT-6 Luna, medium effort
and Daybreak disabled. The probe made no model turn, then stopped its exact operation. No reset
or API-billing fallback was used; earlier `usage_paused` and startup-timeout observations are
historical.

DEV-245's original run failed before a thread or turn. DEV-246's first restart attempt retained
run/session/acknowledgement state but lost the worker after SSH stream closure. DEV-247's active
undelegation test stopped its foreground worker with exact root proof. DEV-248 passed the
restart boundary: its same foreground worker and all seven process identities survived a
coordinator-only restart, with the same task/session/acknowledgement/attempt and no replacement.
Recovery reserved capacity in reconciliation; the conversation did not automatically resume.
Native undelegation then stopped the same live operation. Authentic created and stop replays
returned 200 `duplicate`; invalid signatures returned 401. A description-only event returned
422 `unsupported_event`, with its negative signature control returning 401.

The final durable audit contains four stopped runs and trusted termination proofs, four stage
attempts with none executing, and no operation awaiting reconciliation. Startup repaired
historical confirmed stops without repeating cancellation. A stopped run may retain its current-attempt pointer
to the canceled attempt for audit. An idle restart retained the four task identities and launched
no worker processes. The scoped DEV-233 native delegation, restart-survival, active-stop and
durable-metadata acceptance passed. Automatic conversation resume, worker broker restart,
signed-in dashboard access, provider token lifecycle, Daybreak and scaled capacity remain
unqualified. DEV-234 still owns human replies; prompted events do not supply reply inputs. No Rig candidate
publication or merge was attempted; this does not establish the full Factory Setup flow. See the
[restart repair acceptance record](evidence/dev-233/restart-repair-acceptance.md) and
[earlier native checkpoint](evidence/dev-233/native-live-acceptance.md).

## Primary contracts

The integration follows Linear's [agent authentication/delegation](https://linear.app/developers/agents),
[session interaction](https://linear.app/developers/agent-interaction),
[webhook authentication](https://linear.app/developers/webhooks),
[stop signals](https://linear.app/developers/agent-signals) and
[undelegation guidance](https://linear.app/developers/agent-best-practices).
The GraphQL schema supports a caller-supplied activity UUID and lookup; it does not promise
that repeated create mutations are idempotent. Reconciliation uses the stable lookup. The
live API exposes `AgentSession.issue { id }` and `AgentActivity.agentSession { id }`; the client
normalizes these relations for the existing coordinator contract. A fresh activity lookup
returns HTTP 200 with the specific `INPUT_ERROR` / `Entity not found: AgentActivity` error.
Only that missing response (or a null activity) permits creation; permission, schema and
other lookup errors remain blockers. These contracts were checked against the installed app
on 2026-10-10 before native delegation.
