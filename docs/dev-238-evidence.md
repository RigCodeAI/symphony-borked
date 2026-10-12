# DEV-238 evidence and remaining acceptance

Implementation branch: `cycle/dev-238-trusted-publisher`.
Starting merged source: `ec6c083e2c5e3e698bc81df5111bea70374158c2`.
This record distinguishes source/contract tests from live acceptance.

## Prerequisite reads

On 2026-10-11, GitHub independently confirmed these Symphony PRs merged:

- DEV-233: PR 6, `7e6e55fa36ac0dea445e2357b1d47179e599da14`.
- DEV-236: PR 3, `9e673389691236b07b6ecae9cb3de9e28b1692fd`.
- Integrated DEV-237 implementation: PR 8, `0ea19fb70366ec36bbb4bd6f84fc6ff7e2627add`.
- Repair feedback: PR 9, `ec6c083e2c5e3e698bc81df5111bea70374158c2`.

Live DEV-237 evidence remains a separate prerequisite even where Linear currently
shows Done. The coordinator explicitly released implementation against merged APIs,
without authorizing live publication or treating all archive/viewer acceptance as passed.

## Acceptance matrix

| Criterion | Local implementation/evidence | Live boundary |
| --- | --- | --- |
| One delegated task produces a real Rig PR with tested SHA/table/permanent links | Fixed publish stage, both trusted validation gates, exact archive verifier, selected-repo App transport, template and Linear links | Not run; App and qualified worker-control rollout remain required |
| Failed/missing/forged/stale validation blocks publication; no agent publisher credentials | Service receipt authentication and source/policy binding before every mutation; App key only service-side; synthetic contract failure fixtures | Worker exclusion and actual App process boundaries require live verification |
| Timeout/retry reuses branch/PR and preserves human text | Deterministic task branch, all-state PR lookup, exact head lease, remote confirmation, managed section only | Real GitHub timeout/retry not run; final PATCH race documented |
| Restart/duplicate completion never produces another PR; changed candidate blocks | Durable operation and safe publication intent precede side effects; trusted completion and recovery; store restart/replay and candidate-change contract tests | Real replay/restart/changed candidate demo not run |
| Exact worker candidate reaches validator/publisher | Root-pinned read-only bundle RPC and isolated coordinator snapshot with bytes/hash/commit/parent checks | Producer qualification and exact worker-control installation not performed here |

Synthetic signed validation manifests and upload receipts are test fixtures only.
They never count as real checks, GCS uploads, or permission for target publication.

## Qualified producer integration

The producer was read from published DEV-237 PR 16 head
`9a0aab36cc229bf471279e5b4b4b73493f69829c`, merged as
`7a8a87ae295188ae830527d2d26205a51b9bb744`. The local implement/commit stage
block matches that published source exactly. Its shared Cloud Agent instruction
was copied from the merged revision. Downstream inspection, trusted validation,
publication and human wait remain this increment's stages.

- Inline script SHA-256: `9b7f2d4ea6e70c3ca71642c13ba0a1cb88b16d1a2b35a27cb5317c28777072c5`.
- Compact UTF-8 command argv SHA-256: `aeae2623255e9c74ec80718023f5bda386d87d5943ca3dd2e171967b15483da8`.

The full current `origin/main` through `7a8a87ae295188ae830527d2d26205a51b9bb744`
was merged at `7904a8d`. Two flow/test conflicts retain the exact qualified producer
and this increment's downstream validation/publication stages. The archive/viewer,
review, capacity, backup and cleanup changes were retained.

These pin source preparation only. No broker package/config was installed or
coordinator service activated by this owner.

## Checks actually run

The isolated local container used Elixir 1.19.5 with OTP 28. It did not alter the
existing DEV-233/DEV-234 containers, worker VM, primary checkout or cloud services.

- Final `mix compile`: passed. The initial pass exposed a helper-name typo, which
  was corrected before the passing compile.
- Focused publication, transport, archive, cancellation, transfer, metadata and
  credential-migration files: 56 tests passed after correcting test fixtures.
- Shipped flow and existing workstream, worker-operation, durable restart, evidence
  and Linear delegation regressions: 130 tests passed.
- The shipped-flow test was rerun after copying the qualified shared instruction:
  1 test passed. It executes the actual local producer and checks both pinned hashes.
- `mix specs.check`: passed. Formatting checks passed for every changed Elixir file.
- Credo warning/error checks passed (23 checks across 155 source files). This is
  not a full strict Credo or broad CI result.
- Python candidate-bundle RPC: 9 tests passed after the verified-file cache and
  UTF-8 command-pin changes. Broker/transport/new test `py_compile` passed.
- Existing worker-operation Python regression: 27 tests passed.
- Existing worker transport Python regression: 16 tests passed.
- `git diff --check`: passed.

The credential-migration child fixture now explicitly uses `MIX_ENV=test`: without
it, its `mix run` child defaults to development and the required helper starts its
CLI entry point before the fixture can call `run_cli`. No production approval or
policy check was weakened. The migration's supported source hashes were refreshed
from the final compiled service policy; old durable runs remain incompatible.

Logs stayed under `/private/tmp/dev238-*.log`. They are local development diagnostics,
not trusted factory validation receipts. Broad `make all`/coverage/Dialyzer and live
publication acceptance have not been run by this ticket owner. The local container
is stopped after the granted focused pass; retained volumes support a later check.

## Final merged candidate qualification

The initial local checkpoint is `c1bcdc8`; `7904a8d` merges current main through
`7a8a87a`. The final recheck passed 225 tests over publication/transport/archive,
worker transfer, cancellation/restart, migration, workstream/Linear, and the merged
producer, viewer, capacity, review-result and cleanup interfaces. Compile, specs,
changed-file formatting, Credo warning/error checks and PR-body validation passed.
The supported migration hashes match this final compiled service policy.

The final delta queries all base branches for the task head, blocking a PR aimed at
another base instead of hiding it behind a `main` filter. Completion also verifies
the actual PR still contains the expected managed evidence table and permanent links.
Both regressions passed. These checks remain contract/local integration proof.

The newer PR 17 cleanup merge `5a6b228` is not part of this tested candidate. The
coordinator explicitly requested a fixed candidate rather than repeatedly chasing
main. No coordinator or broker deployment has been performed by this owner. Broad
CI and the live acceptance matrix remain open after development PR publication.

## Development PR and broad CI

Development PR 18: <https://github.com/RigCodeAI/symphony/pull/18>.
The pushed/tested head is `4bb747da381c7d073a5cd1d410d4e03717f6c99a` before this
record-only update. The remote branch and PR head matched independently.

GitHub `make-all` run `38200290681` failed at strict Credo: 174 refactoring,
166 readability and 17 design findings across 162 source files. Findings include
both existing code and this publisher increment. Setup, build, format and spec checks
were reached; coverage and Dialyzer were not reached. The 225 local functional checks
and 52 Python checks remain separate passing proof, not an all-green CI claim.
CodeQL actions/Python and the CodeQL gate passed. Repository alpha guidance treats
broad CI as non-blocking while retaining human review and GitHub protections.
No merge or deployment was performed; the live acceptance matrix is still open.

## Material-risk review

A separate read-only review identified cancellation bookkeeping and import resource
limits. The implementation now saves safe publication intent before the first write,
rechecks current coordinator ownership, and observes accepted GitHub actions after
cancellation without resuming the stopped run. It also caps compressed bundle size,
expanded candidate files/tree, Git subprocess resources and metadata-copy size/count.
The review initially described a worker metadata directory copy; source tracing
confirmed that import copies the original coordinator base clone and receives the
worker candidate only through the bounded bundle RPC. The local metadata helper was
still hardened. The related focused regressions passed in the final Elixir pass.

## Reproducible local check

From the managed worktree, use the qualified Elixir/OTP runtime and normal dependencies:

```bash
cd elixir
mise exec -- mix compile
mise exec -- mix test \
  test/symphony_elixir/publisher_test.exs \
  test/symphony_elixir/publisher_git_test.exs \
  test/symphony_elixir/publisher_github_test.exs \
  test/symphony_elixir/evidence_publication_test.exs \
  test/symphony_elixir/workstream_publication_test.exs \
  test/symphony_elixir/durable_publication_test.exs \
  test/symphony_elixir/worker_candidate_bundle_test.exs \
  test/symphony_elixir/candidate_transfer_test.exs
cd ..
python3 -m unittest discover -s factory/deploy/tests -p test_candidate_bundle_rpc.py
```

Expected: contract fixtures demonstrate blocked writes and one reused simulated PR;
the RPC rejects unauthorized and altered bundles. These commands publish nothing.
The test harness removes only its own disposable fixture directories. Preserve
development logs until the grouped checkpoint is reviewed.

The exact live demo commands depend on the approved installed App, qualified
producer/worker-control release and permanent candidate archive. Follow the
[reviewable setup/rollout plan](trusted-publication.md); do not substitute an
operator cache or a model-authored receipt for the automated candidate path.

## Qualified repair-context consumer source

The consumer branch starts from the qualified shared integration commit
`1865a6acfc411a9a0cd112ffe3f6794f8c7c31aa`. It preserves the frozen
CandidateContext API, original execution pins, legacy producer metadata, and
broker request/response shape. This source is intended for the single combined
DEV-240 integration PR; it is not a separate release or deployed prerequisite.

Python checks passed: 12 candidate-bundle RPC tests, 27 worker-operation tests,
and 16 worker transport tests. A disposable real-Git smoke check executed the
embedded producer directly and passed legacy commit/replay, context-bound repair
commit/replay, and rejection of a main revision outside the parent's ancestry
before staging. The script and command argv hashes matched the source pins in
`software_change_test.exs` and `trusted-publication.md`.

Final Elixir checks passed: compilation with warnings as errors, changed-file
format verification, public spec checks, and 69 focused tests. Logs are
`/private/tmp/dev238-context-compile.log`, `dev238-context-format.log`,
`dev238-context-specs.log`, and `dev238-context-final-focused.log` in the same
directory. The focused set covers transfer, local producer, Publisher, Git/App
transports, permanent evidence, durable publication/restart, worker bundle client,
publication definition and offline secret-name migration. Default Docker-validation
exclusion remained active. The consumer container `dev238-check` was stopped after
these checks; its build/dependency volumes remain available for later use.

Self-review corrected publication-type normalization in historical lookup, exact
PR-field comparisons, shallow permissions in the private copied Git metadata,
raw persisted-context fixtures, and JSON digest handling for Unicode/DEL. Added
cases reject historical development receipts, changed policy/environment,
unauthenticated manifests and missing artifacts before new worker access.
Publisher repair fixtures use synthetic authenticated service seals and retained
receipts; they are not live review, upload or publication proof.

The combined runner must inject the authorized `candidate_context` into the
protected local-commit inputs before the final seam checks. The consumer change
does not edit the runner owned by DEV-240. `software_change_test.exs` contains only
producer pin/legacy-output assertion changes here; its new review-App setup and
flow fixture remain DEV-240's integration responsibility and that file was not in
the focused pass. Preserve those separate sections when combining the commits.
The migration helper's supported policy was refreshed for this exact consumer
source; recalculate it for the final combined source rather than retaining these
intermediate hashes. Broad CI remains for the combined PR under the alpha policy.
No separate PR, App, worker or coordinator rollout has been performed.

Exact final focused set (from `elixir/`):

```bash
mise exec -- mix test \
  test/symphony_elixir/candidate_transfer_test.exs \
  test/symphony_elixir/local_candidate_commit_test.exs \
  test/symphony_elixir/publisher_test.exs \
  test/symphony_elixir/publisher_git_test.exs \
  test/symphony_elixir/publisher_github_test.exs \
  test/symphony_elixir/evidence_publication_test.exs \
  test/symphony_elixir/durable_publication_test.exs \
  test/symphony_elixir/worker_candidate_bundle_test.exs \
  test/symphony_elixir/workstream_publication_test.exs \
  test/symphony_elixir/factory_secret_name_migration_test.exs
```
