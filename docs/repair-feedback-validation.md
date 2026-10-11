# Durable repair feedback validation

A durable run records the executing repair attempt before building its prompt.
Previously, feedback looked only at the last attempt and missed the preceding
failed check. The runner now skips the current attempt and includes the immediately
preceding failed check result and gate detail. A passed check does not add stale
repair feedback to a later turn.

## Verification

The regression drives actual durable run transitions and captures app-server
turn prompts through a controlled local server. It checks feedback after a failed
check and its absence after a passing check. A restart test preserves the saved
wait, attempts, artifacts and old policy while an executor mismatch blocks dispatch.

- Compilation with warnings as errors, formatting and public specs passed.
- 55 focused runner, run and durable recovery tests passed.
- The previous broad run passed 579 tests, with six skipped and ten excluded.
- Broad CI remains non-green: existing lint findings, 79.53% coverage against a
  100% threshold, and five existing Dialyzer errors.

The code and tests are unchanged from the tested repair patch. Broad results were
reviewed and need no repeat for this publication-only reduction.

## Deployment boundary

This fix changes the pinned executor policy digest. It has not been activated.
Existing human waits must continue on their current release and retain their
original definitions, policy, attempts and history.

The narrow deployment path is to finish existing nonterminal runs on their pinned
release, pause new intake, prove all workers terminated, and take an
application-consistent backup. Then stage the new immutable release, restart with
the preserved store, check retained terminal evidence and run a fresh disposable
failure-and-repair probe. Preserve the previous release and backups for recovery.
Do not restore an old database over newly committed events.

If an existing wait must remain active, deployment requires a separately qualified
compatibility mechanism that executes its original pinned policy. Do not repin
saved runs, waive digest mismatches, discard waits or start another scheduler.
