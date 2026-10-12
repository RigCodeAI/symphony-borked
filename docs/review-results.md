# Strict review results — DEV-240 independent slice

This slice pins the review rubric and validates its output against recorded commit
identities and local Git diff locations. It does not implement the full DEV-240
review/fix loop. Publication, remote-head validation, review execution, durable
dispatch, repair rounds and feedback ownership still require DEV-238/DEV-239 and
the subsequent integration release.

## Caller contract

Use `SymphonyElixir.ReviewResult.validate(json, recorded, current, workspace)`.

- `json` is the model's exact rubric JSON, without prose or Markdown fences.
- `recorded` is a service-owned map with `base_sha`, `head_sha` and `rubric_sha256`.
- `current` contains independently reconciled `base_sha` and `head_sha`. Future
  GitHub integration must read these from GitHub, not from the model or its workspace.
- `workspace` is the absolute path of a separate standalone clone checked out at
  the recorded head. It must have its own `.git` directory; managed Git worktrees
  and metadata symlinks are rejected by the existing `CandidateGit` contract.

Both SHA pairs must match exactly, including results with no findings. Require full
lowercase SHA-1 or SHA-256 commit IDs; abbreviated refs, tags and mixed formats are
rejected. The base/head must exist as commit objects and share a merge base. The
local checkout's HEAD must match the recorded head.

Record the rubric digest when starting the review. Adding service identity fields
to the model's JSON would violate the pinned rubric schema. A matching envelope
does not authenticate the reviewer: that remains the future service's responsibility.

```elixir
alias SymphonyElixir.ReviewResult

recorded = %{
  base_sha: base_sha,
  head_sha: head_sha,
  rubric_sha256: ReviewResult.rubric().sha256
}

current = %{base_sha: current_base_sha, head_sha: current_head_sha}
ReviewResult.validate(json, recorded, current, review_clone)
```

## Accepted output and errors

An accepted result returns `{:ok, validated}` with the exact base/head, rubric pin,
original parsed `result`, and `locations` in finding order. Each normalized location
contains repository-relative `path`, `start_line`, `end_line` and `side: :right`.
These are verified coordinates, not a publication operation or a passing gate.

The schema requires every rubric field and rejects unknown or duplicate JSON keys
at every depth. Findings require nonempty titles/bodies, a title of at most 80
characters with a `[P0]`–`[P3]` prefix, matching integer priority when provided,
numeric confidence in `0..1`, an absolute path, and a positive ordered integer
line range. Numeric priority may be absent or null, as upstream permits. Confidence
numbers may use integer endpoints (`0` or `1`) as well as decimals. JSON is limited
to 1 MiB. Natural-language instructions such as imperative phrasing, rule citations
and actionable reasoning still require actual review quality checks.

The original correctness verdict is preserved. A correct verdict with findings is
not a clean review; an incorrect verdict without findings is not approval. This
module neither maps events nor authorizes advancement, publication or merging.

Any invalid finding fails the entire result. There is no dropping invalid locations,
turning parse errors into an empty list, or treating stale results as clean.
Errors include `:stale_review`, `:invalid_review_rubric`,
`:review_checkout_head_mismatch`, and indexed schema/location failures. An invalid
result needs repair or explicit human escalation in the future integration.

## Inline locations

Locations are checked against the committed PR-style `base...head` diff, not dirty
working-tree contents. Existing `CandidateGit` isolates repository metadata and
configuration. Diff commands explicitly disable external diff and text conversion.
Literal pathspecs preserve spaces, Unicode and wildcard characters in filenames.
The reader also verifies that a selected patch names only its requested file;
unexpected additional selections fail closed. Committed blobs containing NUL
bytes are rejected even when candidate attributes force Git to display them as text.

Each path must be lexically within the reviewer workspace, with no traversal or
control characters, and identify a regular tracked file at the head. Each range
must fit within one head-side hunk of a three-context-line diff and overlap at
least one added/changed line. Context-only, unchanged and out-of-range findings
are rejected. Renames are viewed as deletion/addition, with findings on the new path.

The rubric has no side field. Deleted-only, binary, symlink, gitlink and ambiguous
cross-hunk locations fail closed. Supporting those later requires an explicit
mapping contract; this slice does not guess old-side coordinates or discard a finding.

The caller must recheck the remote base/head and trusted validation immediately
before posting or advancing. A successful local validation can become stale after
it returns. Local shape/location validation is not trusted validation evidence.

## Reproducible local verification

From this checkout, verify the unchanged rubric:

```bash
git hash-object factory/review/rubric.md
shasum -a 256 factory/review/rubric.md
```

Expected: the blob and SHA-256 in [the resource README](../factory/review/README.md).

From `elixir/`, use the configured isolated toolchain:

```bash
mise exec -- mix compile --warnings-as-errors
mise exec -- mix test test/symphony_elixir/review_result_test.exs
```

The focused tests create disposable local Git repositories and exercise valid,
malformed and stale outputs plus actual committed diff locations. Expected: all
focused tests pass. Their temporary repositories are removed automatically.
They make no model calls, post no reviews and create no remote branches or PRs.

The full pilot must later show a real inline review, validated fix on the same PR,
review of the new head, and restart without duplicate repair dispatch. None of
those live acceptance criteria is demonstrated by this slice.

## Slice evidence

On 2026-10-11, an isolated container based on qualified image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
received merged Symphony source `ec6c083e2c5e3e698bc81df5111bea70374158c2` plus
only this slice's new files. It had no network or shared volumes; no credentials
were supplied to the test container.
The container was removed after the commands completed.

| Released slice criterion | Evidence |
| --- | --- |
| Pin the unchanged upstream rubric | Git blob and SHA-256 match; compile-time checks and focused pin test passed. |
| Reject malformed output without dropping findings | Exact schema, duplicate-key, control-character and mixed valid/invalid finding tests passed. |
| Bind results to recorded base/head and rubric | Stale base/head, missing/wrong rubric digest, tag/ref and checkout mismatch tests passed. |
| Verify inline positions from committed data | Literal paths, renames, context/range rejection and dirty working-tree tests passed. |
| Reject unsupported or ambiguous locations | Binary/forced-text, symlink/gitlink, deleted paths and unexpected file-selection tests passed. |
| Preserve incomplete full-ticket status | No review dispatch, publisher, coordinator/store, agent settings or Rig workflow was changed. |

Executed from `/workspace/elixir` in the isolated image:

```bash
mix compile --warnings-as-errors
mix format --check-formatted lib/symphony_elixir/review_result.ex test/symphony_elixir/review_result_test.exs
mix specs.check
mix test test/symphony_elixir/review_result_test.exs
```

Final result: compilation, formatting and public specs passed; **21 tests,
0 failures** (seed `796721`). Full local `make all` was not run under the
coordinator's focused-test-only slot; repository broad CI remains separate.

The independent review identified a forced-text binary edge, resolved by checking
the committed blob for NUL with a regression test. Focused execution also exposed
Git option ordering in the existing adapter; the new reader uses an explicit path
separator and verifies the selected file set. No shared adapter file was changed.
