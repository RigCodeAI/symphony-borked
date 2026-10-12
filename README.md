# Rig Symphony

This is `RigCodeAI/symphony`, a fork of OpenAI Symphony and the implementation repository
for Rig's **Factory Setup** project. We are extending the existing Elixir coordinator
into a software factory for the separate `RigCodeAI/rig` repository.

The intended flow is a delegated Linear issue → implementation → trusted validation →
one linked PR → independent review and bounded fixes → human merge in GitHub → archive
and cleanup. This is the target design, not a claim that the factory is deployed.

## Start here

- **Working agents:** read [AGENTS.md](AGENTS.md), then
  [elixir/AGENTS.md](elixir/AGENTS.md) for Elixir work.
- **Current scope and acceptance criteria:** read the
  [Factory Setup project](https://linear.app/rigai/project/factory-setup-a2dff7721c28/overview)
  and the assigned ticket, including dependencies and discussion.
- **Existing service contract:** [SPEC.md](SPEC.md).
- **Current implementation, setup and tests:** [elixir/README.md](elixir/README.md).

Factory Setup tickets change this repository. The factory being built will execute Rig
coding tasks in separate workspaces; do not confuse the repositories or run factory
coding sessions inside this service's source checkout.

## Current foundation and planned additions

The existing implementation provides tracker polling, bounded scheduling, isolated issue
workspaces, local/SSH workers, Codex app-server execution, retries, reconciliation and a
runtime dashboard. See the Elixir README for its current behavior and limitations.

Factory Setup adds file-defined workstreams, durable run/stage records, native Linear
delegation and questions, trusted validation and PR publication, independent reviews,
permanent evidence, and reproducible GCP deployment. Check current code and ticket
evidence before treating any planned addition as available.

Workstreams define stages and inline gates. Agent definitions select instructions,
models and runtime permissions; skills supply reusable procedures. Evaluators return
evidence, and the service enforces gates. There is no separate agent-profile object or
reusable gate registry in the initial design.

The first increments are:

1. [DEV-229](https://linear.app/rigai/issue/DEV-229): run a local file-defined agent stage
   followed by an executable check, proving passing and failing gates. This demo needs
   neither cloud infrastructure, Linear integration, Daybreak nor publication.
2. [DEV-232](https://linear.app/rigai/issue/DEV-232): durable local runs/stages, restart
   recovery and human waits. See [the controlled recovery demo](docs/durable-workstreams.md).
3. Bind deployment, delegation, validation, publication, review and cleanup to those
   stages through the remaining tickets. Integrate Coverage Factory's separate contracts
   and qualify capacity after the pilot works.

The current coordinator and root worker control use source commit
`064f892f95847ea2eed0eb684da36c6e1bb5fce6`, archive SHA-256
`67267e73e9c587f0b6115398d89cdac462b23dcb8749df34895a34b9d0a0af89`. The worker app remains
`541279aeb6a5366571e1ee8935e134c728d91c63`; worker machine
`fe9b1ece921d40aeac95b10940000311` is on boot
`f76f87db6972439ca13738e1d54594fa` under systemd 252.39. Root checks, transport tests,
actual SSH qualification and Elixir route checks passed. App Server startup and subscription
readiness also passed with GPT-6 Luna, medium effort and Daybreak disabled; it made no model
turn and was stopped with exact termination proof. No reset or API-billing fallback was used.

DEV-248 passed the scoped native restart and durable metadata acceptance. Its same foreground
worker survived a coordinator service restart with all seven process identities, task IDs,
session and acknowledgement unchanged; no replacement launched. Native undelegation then
stopped that operation. The final audit found four stopped runs with trusted termination proofs,
no executing attempts and no operation awaiting reconciliation. DEV-245's initial run failed
before a thread or turn; DEV-246's earlier restart attempt failed when its SSH stream ended the
worker; DEV-247's live undelegation did stop its worker. Those remain historical results. See
the [restart repair acceptance record](docs/evidence/dev-233/restart-repair-acceptance.md),
[Linear delegation](docs/linear-delegation.md) and
[current decision card](docs/dev-233-morning-decision.md).

This accepts the scoped DEV-233 native delegation, restart-survival, active-stop and durable
metadata boundary. Automatic conversation resume, worker broker restart, signed-in dashboard
access, provider token lifecycle, Daybreak, scaled capacity and the wider Factory Setup flow
remain unqualified. Rig candidate publication and merge were not attempted.

DEV-234 adds durable native questions, replies and artifact-bound approval waits. Agents
can continue independent work before suspending; suspension releases capacity after confirmed
termination. Continuation reconstructs saved context in a fresh Codex conversation. See the
[question/reply contract](docs/linear-questions.md) for commands and verification boundaries.

Coordinator-only client credentials mint app tokens in memory per delegated run; see
[Linear app setup](docs/linear-delegation.md#linear-app-setup) for scopes, events and secret references.

DEV-237 adds an opt-in private evidence index, signed-IAP HTML viewer and verified
GCS archive receipts. Durable capture, restart replay and native Linear links are
locally tested against merged main; live qualification remains pending. See [private task evidence](docs/private-evidence.md) for configuration,
retention and the current acceptance boundary.

DEV-242 adds source for a private local capture of the workstream and Evidence
databases at a quiet boundary. Its [owner capture contract](docs/dev-242-capture-barrier.md)
passed compilation and 85 focused tests on the PR #18 baseline; merged cleanup
integration passed 49 focused tests. Deployed restore, fencing and freshness
monitoring remain pending.

DEV-235 adds [named cloud-agent routing and safe reassignment](docs/dev-235-agent-mapping.md)
to the existing coordinator. Both additional apps are installed with DEV-only access;
route activation and native multi-app acceptance remain pending. Existing human waits
require policy-compatible credential-exclusion maintenance before activation.
See the [administrator setup plan](docs/dev-235-app-setup.md).

Use live ticket dependencies to schedule work. The local files
`docs/rig-software-factory-spec.md` and `docs/factory-setup-ticket-drafts.md`, if present,
are earlier drafts. They predate the workstream additions and published ticket updates;
their profile terminology and unpublished-ticket notes are stale. They are background,
not the current task queue.

## Build and test the existing service

From this checkout, with [mise](https://mise.jdx.dev/) installed:

```bash
cd elixir
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build
mise exec -- make all
```

The pinned runtime is in [elixir/mise.toml](elixir/mise.toml). `make all` runs setup,
build, format checks, lint, test coverage and Dialyzer. See the Elixir README for focused
tests and opt-in live integration tests, which create external resources.

To run the existing service, provide an explicitly configured workflow from `elixir/`:

```bash
mise exec -- ./bin/symphony /absolute/path/to/configured/WORKFLOW.md
```

**Do not use [elixir/WORKFLOW.md](elixir/WORKFLOW.md) unchanged for Factory Setup.** It is
an upstream example with an upstream project/clone, different Linear statuses, rework
that closes the old PR, an agent-driven merge path, and a cleanup hook that can close
PRs. The factory design instead preserves one PR, waits for a human GitHub merge, and
requires archival before cleanup. Documentation changes do not change that sample's
runtime behavior. Existing workflow compatibility is preserved. DEV-229's local entry point is
`mix workstream.run`; see [Local workstreams](docs/local-workstreams.md) for
versioned definitions, Linux bootstrap, passing/failing demos and exact commands.

Named cloud/review definitions and a bounded subscription qualification command are
available. See [worker qualification](docs/worker-qualification.md) for exact requested
settings, readiness checks, receipts and the Daybreak verification boundary.

The shipped software-change definition uses a [service-owned local commit check](docs/local-candidate-commits.md)
after the agent edits and tests. This preserves Codex's protected Git metadata while
preparing a real local candidate and transport bundle. Activation and publication
remain separate verified boundaries.

## Factory boundaries

- Extend the existing coordinator; keep one writer per task and reconcile before retrying.
- Dispatch only explicitly delegated eligible Rig issues, preserving their human owner.
- Bind validation and review to exact revisions. Agent claims alone do not satisfy gates.
- Keep cloud implementation and review agents separate. The development subagent setting
  in `AGENTS.md` does not choose production factory models.
- Retain private, redacted evidence after workspace deletion. Keep Coverage Factory's
  independent qualification and scoring responsibilities in that project.
- GCP deployment and roughly twelve combined cloud/review slots are planned and require
  qualification. A build, configuration file or Terraform plan is not a live deployment.


Trusted candidate validation is available locally through the pinned-policy runner and
inline stage gates. See [trusted validation](docs/trusted-validation.md) for development/final commands, evidence and limits.

DEV-238 adds a service-owned publication stage: exact trusted validation plus permanent
archival must pass before the coordinator pushes a candidate or creates/reuses its Rig PR.
See [trusted publication](docs/trusted-publication.md) for App permissions, replay behavior
and the live acceptance boundary. This source increment does not claim an installed App
or a completed Linear-to-PR deployment.

DEV-241 adds opt-in idle cleanup using recorded directory ownership, fresh worker
termination, the exact GitHub merge and a verified current archive. Legacy Linear
terminal-state cleanup retains work. See [merged workspace cleanup](docs/merged-workspace-cleanup.md)
for the read-only child-result proof, runtime requirements and remaining live gates.

## Upstream and license

This fork builds on [OpenAI Symphony](https://github.com/openai/symphony).
The upstream [demo](https://player.vimeo.com/video/1186371009?h=5626e4b899) illustrates
Symphony's original workflow; it is not evidence of this factory's readiness.

Licensed under the [Apache License 2.0](LICENSE).
