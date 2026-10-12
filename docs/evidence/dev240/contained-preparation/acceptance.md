# Contained reviewer preparation: source evidence

This increment fixes the source boundary between ReviewWorkspace, the Elixir
WorkerOperation client and the root broker. No VM, service, App, feedback workflow,
model run or deployment was changed. Earlier systemd qualification does not prove
these new mount grants.

## Checks

The actual embedded preparation script is 4,835 bytes, beyond the generic 4,096-byte
argument limit. Its compact-JSON command pin is
`e9484451418b48094ace5c5694ff396733c5e2929c0f5e65b35620ed2ec06a25`.
A disposable local Git clone passed initial preparation and replay with a changed
current attempt and unchanged stable operation. The host fixture remains at
`/private/tmp/dev240-contained-smoke-g25_p2ym`; it contains no external credentials.

The first isolated gate passed warnings-as-errors compilation, public specs,
53 selected Elixir tests (zero failures, eight nonselected capture tests excluded),
and the actual PR-body validator. See [the initial log](elixir-tests.log).
It ran on main `7807af0` plus this change. The final integrated gate below
supersedes that initial source boundary. The final Python broker suite passed 38 tests; the transport suite passed 17 tests.
See [broker](python-broker-tests.log) and [transport](python-transport-tests.log)
receipts. They cover command pin/path tampering, symlinks, unknown/unowned leaf
retention, run/stage identity binding, immutable ownership/source/head pins,
duplicate/restart replay, prior terminal proof, actual broker-generated manifest
through the held wrapper and invocation-bound release, and the exact script on Git.
The first combined run hit an existing socket shutdown `Errno 57`; the isolated
case and subsequent full transport suite passed. These tests do not start a real
systemd unit.

## Ownership and release contract

The root broker, not the caller, derives
`.factory-reviews/<sha256(run-id + newline + stable-operation-id)>/checkout`.
Staging remains under that exact leaf. The root-owned command pin is required.
The request cannot carry destination paths or systemd properties. Its current and
stable attempt IDs must bind the same run and stage. The root-owned owner record
binds run, stable attempt, source and all nine review identity pins; it excludes
the current attempt ID. Unknown/unowned paths cannot be adopted.

The held operation still reserves its canonical argv/source digest before launch,
registers its exact machine/boot/unit/invocation/cgroup identity durably, and waits
for validated root release. One prior live or unknown writer blocks a replacement.
Only exact terminal proof permits another attempt. Each current attempt keeps its
own worker identity; stable replay does not reset cancellation or guess termination.
Unknown and interrupted data remains retained. Cleanup is deferred to DEV-241's
separate ownership, archival and termination contract.

For activation and rollback, follow
[the review release requirements](../../../review-repair.md#contained-preparation-release-requirements).

## Source self-review

The review checked the caller/broker/wrapper boundary, bounded script allowance,
read/write grants, immutable owner records, prior unknown-operation capacity,
release ordering and cancellation reuse. It found that the old wrapper rejected
the reduced environment; the final source adds a root-protected pinned manifest
variant and tests that exact release path. Preparation also hides the worker home.
No competing scheduler or cleanup ownership was introduced. Live systemd mount
and credential exclusion proof remains required at the release gate.

## Hosted alpha CI inspection

At head `5fb7072aa1ab680acc01f14c2c07ecab4b80ac36`, hosted build, formatting,
public specs, CodeQL actions/Python and PR-description checks passed. The first
hosted run found one formatting layout in the changed client test; the next commit
applied its exact diff, and the second hosted formatting check passed.

[The broad make-all job](https://github.com/RigCodeAI/symphony/actions/runs/38218918097/job/114710160777)
stopped at strict Credo: 283 refactoring opportunities, 342 readability issues and
18 design suggestions across 195 source files, including changed code. Broad
runtime tests, coverage and Dialyzer were not reached. This is a separate,
non-blocking alpha result under AGENTS.md; focused changed-behavior gates remain
required. No repository protection was changed or bypassed.

## Final integrated source gate

Exact executable code head:
`5fb7072aa1ab680acc01f14c2c07ecab4b80ac36`, including merged main `bfadf93`.
The snapshot had only a documentation overlay. The later receipt commit changes
only documentation and logs; executable source and tests match this tested head.

The immutable isolated toolchain image was
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`.
Its container used two CPUs, 4 GiB and no network or supplied credentials. It
received a fresh Git archive plus the documentation overlay. Snapshot:
`/var/folders/ly/jv9hb9s14k1bxb1vvtb6npcm0000gn/T/dev240-final-source-1wfmcwy0`.

[The final log](integrated-tests.log) records:

- `mix format --check-formatted`: passed.
- `mix compile --warnings-as-errors`: passed.
- `mix specs.check`: passed.
- Complete 30-entry executor source policy refresh and helper formatting: passed;
  the copied helper is byte-identical to the committed helper.
- Client, real Git preparation/retry/interruption/race, context, secret-name
  migration, cancellation, native child authorization/delegation and selected
  anchored/cold/legacy held-capture tests: 64 tests, zero failures, eight
  nonselected capture tests excluded, seed 240.
- Actual `mix pr_body.check`: passed.

The container `dev240-contained-integrated` exited zero and was removed; the
shared heavy build slot was released. The exact-final Python files are unchanged
from the 38+17 passing tests above. No live deployment or real review was performed.
