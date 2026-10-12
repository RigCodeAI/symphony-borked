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
It ran on main `7807af0` plus this change; the integrated-main follow-up will be
recorded before handoff. The final Python broker suite passed 38 tests; the transport suite passed 17 tests.
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
