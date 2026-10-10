# DEV-234 question and reply acceptance

The question, released worker slot, coordinator restart, native reply and one
same-stage continuation were observed on the scoped GCP pilot. The saved answer
was correct. The demo's later candidate check failed because the worker sandbox
made `.git` read-only, preventing its requested local commit. This record does
not claim a successful candidate commit or publication.

Live duplicate replay was **not executed**: automatic approval review rejected
sending the captured signed webhook to the public endpoint without specific
approval. The original signature then expired. Duplicate/restart behavior passed
controlled tests; those tests are not a live duplicate-delivery result.

## Scope and deployed versions

The coordinator stores questions before publishing native elicitation, handles
authenticated human prompts, and delivers replies once by provider activity ID.
A wait stops its contained operation before releasing capacity. Continuation
starts a fresh app-server thread with pinned stage context, saved input and the
same workspace. Old JSON-RPC request IDs are not replayed. Trusted approval waits
require an explicit current artifact digest.

- Coordinator source: `4ea729c6bb1b44e379161ea3543e68086cdbd1e6`.
- Coordinator release archive SHA-256:
  `9232e8ef3100e9abe779fdc9ac7f279c4424d4365fece95a6b38988d61c5a616`.
- Worker broker unchanged: `064f892f95847ea2eed0eb684da36c6e1bb5fce6`, archive
  `67267e73e9c587f0b6115398d89cdac462b23dcb8749df34895a34b9d0a0af89`.
- Worker application unchanged: `541279aeb6a5366571e1ee8935e134c728d91c63`.
- Runtime: qualified Default Cloud, GPT-6 Luna medium, Daybreak off, subscription
  authentication; no API billing fallback.

[Deployment receipts](deployment.json) record activation and protected config
permissions. Existing IAP/SSH access staged the release directly. No IAM, budget,
billing, VM or endpoint change was made. Recovery with the release disk absent
was not qualified.

## Live demo

[DEV-249](https://linear.app/rigai/issue/DEV-249/disposable-dev-234-question-and-restart-acceptance)
remained owned by Adam and in Backlog. Matching dedicated clones came from Rig
seed `39d6d5d998366bf13af19eb74ae47d598ecf92bd`, branch
`cycle/dev-249-disposable`, with push URLs `DISABLED`. Other disposable clones
were preserved. Factory publication remained disabled throughout.

The native session is `b3fce47a-584d-4364-a138-8e01ea95358e`; the durable run is
`run-3ad6097ae6d960249b681a580aba6119`. It asked one durable question,
`question-d01376404b130d3a43adf607a0f8aadb`: “Which harmless label should the demo
save?” Linear displayed an initial elicitation and a separate waiting-state
elicitation stating that execution capacity was released.

1. At 19:33 UTC the native task started. It wrote `INDEPENDENT.txt` containing
   `ready` before waiting. [Waiting observation](observed-waiting.json) showed no
   worker processes and no running tasks.
2. The coordinator was stopped, its persisted state inspected, and restarted
   while waiting. [Durable wait audit](wait-restart.json) records the exact broker
   termination proof: matching operation identity, `main_pid: 0`, released cgroup
   and `cgroup_populated: 0`. [After restart](observed-after-restart.json), the run,
   question, stage and native session were unchanged.
3. At 19:36 UTC the reply `answer <question-id>: blue` was submitted through the
   native agent chat. [Resume audit](resume-audit.json) records one accepted
   provider activity, `dcc1a30f-00fa-4283-a942-d01d602372f3`, and exactly one
   continuation, `implement/2`. The original question remains linked to
   `implement/1`; its active attempt is `implement/2`. The fresh app-server thread
   was `01a12751-36ef-7cd3-839d-133f7f84e08d`, following the original
   `01a1274e-e268-7e60-b4ca-546a009f1875`.
4. [Resumed observation](observed-resumed.json) verified `ANSWER.txt` contained
   `blue`. The subsequent real Git check failed. [Agent report](worker-diagnosis.json)
   identifies the read-only `.git/index.lock` boundary; the files remained
   uncommitted. Linear displayed the error. No approval or sandbox permission was
   broadened to make that check pass.
5. Delegation was removed. The native conversation displayed “Work stopped.
   Further replies do not restart this task.” [Stop audit](stopped-audit.json)
   and [final observation](observed-stopped.json) retain termination receipts for
   both agent attempts and the failed check, with no live worker processes.
6. The temporary observer stopped and seven expired raw capture bodies/signatures
   were removed. [Cleanup receipt](capture-cleanup.json) retains only event
   metadata. Workspaces and Codex conversations were preserved.

![Native question](native-question.png)

![Native reply, failure and stop](native-reply-stop.png)

## Read-only Git diagnosis and follow-up

This is an existing worker policy limitation, with no observed continuation
policy regression. The policy construction in `WorkstreamRunner.run_ready_agent/5`
(`elixir/lib/symphony_elixir/workstream_runner.ex`, runtime settings) is unchanged
from base `7e6e55fa36ac0dea445e2357b1d47179e599da14`: `workspaceWrite`, only the
workspace as a writable root, network off, and the agent's `never` approval policy.
`factory/agents/default-cloud.yaml` still selects `workspace-write` / `never`.
The continuation changes prompt context and tools, not these sandbox parameters.

[Saved worker policies](worker-policy.json) show identical effective policy,
workspace, model and effort for the original and reconstructed threads. Outside
Codex, the same worker UID owns `.git` (mode `0755`) and `os.access(..., W_OK)` is
true. The recorded agent report identifies the sandbox's read-only index lock.
The [official protected-path rule](https://learn.chatgpt.com/docs/agent-approvals-security#protected-paths-in-writable-roots)
explains that workspace-write protects `.git` recursively and that disabling
approval prompts does not remove the sandbox boundary. No native permission
request was converted into a clarification or approved here.

The smallest proposed follow-up belongs to the existing worker/candidate Git
owner (`WorkstreamRunner` / `CandidateGit`), not the Linear reply coordinator:
define a trusted local candidate-commit step after confirmed agent termination.
It should verify the dedicated clone and expected base, collect permitted changes,
record an exact candidate SHA, and leave publication credentials and `.git` writes
outside agent commands. Its checks must cover the initial turn and reconstructed
turn with the same policy. This is a proposed contract, not implemented behavior;
no new route or sandbox permission was added by this diagnosis.

## Acceptance boundaries

| Behavior | Evidence |
| --- | --- |
| Native question, independent work, released slot and reply | Live DEV-249 observations and screenshots |
| Restart while waiting; one same-stage continuation | Live durable audits; real app-server terminated and fresh thread started |
| Reply during an active turn survives coordinator restart | Controlled OTP coordinator test in `linear_delegation_test.exs` |
| Duplicate delivery/activity and conflicting activity bodies | Controlled coordinator/client/webhook tests; live replay not executed |
| Progress, failure and stop visible; agent output does not create replies | Live native conversation; signed agent-output rejection tests |
| Stale stage/artifacts and ordinary clarification cannot approve | Controlled run and coordinator tests |
| Current artifact digest required for trusted approval | Controlled run/schema tests |
| Multiple questions and saved input survive transport retry | Controlled run/runner tests |
| Permission, secret and unsupported multi-question requests block | Controlled app-server tests |

## Local checks

Checks ran in isolated `dev234-check`, Elixir 1.19.5 / OTP 28, with the existing
cache. This source checkout has no host Elixir runtime. Commands ran from
`/workspace/elixir` after copying the final source, tests and factory fixtures:

- `mix compile --warnings-as-errors`: passed.
- `mix specs.check`: passed.
- Focused question/client/webhook/coordinator/runner/run suites: 167 tests passed;
  follow-up run/runner/delegation suites: 54 passed; webhook checks: 17 passed.
- `mix test`: [520 tests, zero failures](tests-final.log), six skipped, ten excluded.

Broad CI was run and reviewed under the root AGENTS alpha exception. It is not
fully green:

- [`make all`](all.log) stopped on Credo style/complexity findings after format,
  build and specs checks passed.
- [`mix dialyzer`](dialyzer.log) reported three warnings: the added defensive
  `valid_wait_outputs?/2` catch-all is inferred unreachable; two existing warnings
  are in `workstream_store.ex`.
- [`mix test --cover`](coverage.log) reported 77.42% against the 100% threshold and
  initially lacked the factory fixture in the container. The fixture was copied,
  its affected test passed, and the final full `mix test` run passed as recorded
  above. Coverage was not rerun or claimed passing.

## Upgrade review correction

Review of PR #7 at `340b50053b146aa17c103f72731899a4a912529a` found that
pre-DEV-234 run snapshots lack `questions`, `inbox`, `activity_ids` and
`continuation`. The original decoder returned these maps unchanged, so ready
agent dispatch and active completion/retry could access missing fields. The four
stopped historical pilot runs never exercised those paths; their restart did not
qualify active-run upgrade behavior.

`WorkstreamStore.decode_run/1` now adds only absent defaults on both bulk startup
loads and individual fetches. It preserves every saved value, including IDs,
definitions, execution context, policy pins, artifacts, waits, counters and
operation identities. Existing question/reply state takes precedence over the
defaults. This is additive in-memory normalization, persisted by the next normal
transition; it does not recreate a run, rewrite its policy or bypass policy
compatibility. Historical policy mismatches still block execution.

The new controlled restart fixtures persist a current run with only these four
fields removed before closing/reopening storage. They exercise the old snapshot
shape under a matching pinned policy, rather than silently upgrading a historical
policy. These checks are separate from the earlier live pilot. The live
coordinator remains at `4ea729c6bb1b44e379161ea3543e68086cdbd1e6`; this correction
has not been deployed to it.

Before the fix, a close/reopen store test failed because the loaded run lacked
the four fields. A separate temporary OTP reproduction invoked
`Orchestrator.step_workstreams/1` on a reopened ready run and exited with
`{:badkey, :inbox, legacy_run}` in `WorkstreamRun.start_stage/1`. After copying the
fixed decoder into the same isolated container, that dispatch returned `:ok`.
The temporary crash-asserting reproduction was removed; permanent regression
tests cover successful behavior instead.

Correction checks used the existing isolated `dev234-check` container
(Elixir 1.19.5 / OTP 28), with no live agent or deployment:

- `mix compile --warnings-as-errors` and `mix specs.check`: passed.
- `mix test test/symphony_elixir/durable_workstream_test.exs
  test/symphony_elixir/workstream_store_test.exs
  test/symphony_elixir/workstream_run_test.exs
  test/symphony_elixir/linear_delegation_test.exs`: 68 tests, zero failures.
- `mix test test/symphony_elixir/linear_store_test.exs`: five tests, zero
  failures. Its migration-rollback fixture now asserts every original saved
  field survives plus the four absent defaults.
- `mix format --check-formatted`: passed after the test correction.
- Final `mix test`: 526 tests, zero failures, six skipped and ten excluded.
- `make all`: build, formatting and spec checks passed, then Credo failed;
  coverage and Dialyzer were not reached in this rerun. The alpha exception
  treats broad CI as non-blocking; GitHub protections remain in force.

The focused checks cover old-shape ready dispatch, human-wait reply followed by
agent dispatch, completed-operation recovery without replay, terminated retry
with the same side-effect ID, and incompatible historical-policy blocking. Store
checks cover both load paths, preserving nonempty reply state and persisting the
normalized snapshot without duplicating attempts or operations.
Correction output is in [upgrade-checks.log](upgrade-checks.log) and
[upgrade-all.log](upgrade-all.log), with trailing spaces trimmed from the lint
display. The first full run exposed the outdated
migration-rollback assertion; the final result above follows its correction.

## Self-review and remaining verification

The review checked session/activity identity, reply commit/replay ordering,
worker/attempt callback authorization, termination before releasing capacity, and
clarification/approval separation. Regression tests cover two data-loss risks
found during implementation: a second question after same-stage continuation,
and saved input lost before a successful transport retry. No mandatory general
independent review was added under the alpha guidance.
The upgrade correction was also checked for saved-value precedence and coverage
of both startup load and individual fetch. It changes no policy pin, dispatch
authorization, side-effect identity or credential boundary.

A fresh signed live duplicate-delivery test still requires explicit approval for
the replay payload and destination. The disposable candidate-commit boundary is
also unqualified with the current read-only Git sandbox. Neither is hidden by
the successful question/restart/resume result. DEV-234 is not marked Done and no
PR is merged by this change.
