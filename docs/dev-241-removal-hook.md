# Removal hook safety (DEV-241, slice 1)

`mix workspace.before_remove` leaves GitHub pull requests unchanged. Earlier
versions looked up the current branch and closed open PRs before deleting a
workspace. A terminal tracker status or workspace deletion must not close an
unmerged PR implicitly.

The task now succeeds quietly without invoking Git or GitHub. Existing calls
remain valid:

```sh
mix workspace.before_remove
mix workspace.before_remove --branch feature/my-branch
mix workspace.before_remove --repo openai/symphony
mix workspace.before_remove --branch feature/my-branch --repo RigCodeAI/rig
```

`--branch` and `--repo` are accepted for compatibility and have no effect.
`--help` and `-h` describe this behavior; unknown options or missing option values
still fail. The task works without a Git repository, GitHub authentication, or
either executable.

The sample `elixir/WORKFLOW.md` hook can keep invoking this task. This change does
not alter the generic hook contract in `SPEC.md`: the workspace manager runs
`before_remove` in the workspace and still ignores hook failure or timeout.
It changes only this bundled task's behavior. Other user-supplied hook scripts
remain responsible for their own side effects.

Success from this compatibility task is not authorization to delete a workspace.
DEV-241's merge confirmation, owned-path deletion, permanent archival gates,
cleanup retries, parent-output retention, and cancellation recovery remain
separate work. This slice does not satisfy the whole ticket or make the upstream
sample workflow suitable for Factory Setup.

## Focused verification

From `elixir/` with the repository's pinned runtime:

```sh
mise exec -- mix compile --warnings-as-errors
mise exec -- mix test test/mix/tasks/workspace_before_remove_test.exs
```

The dedicated tests use local fake Git/GitHub commands and an open-PR fixture.
They check that legacy invocations never call those commands or change the PR,
that missing tools do not cause failure, and that option/help behavior remains
compatible. A disposable workspace test invokes the real task in a shell,
confirms the hook completed before workspace removal, and verifies that the PR
fixture remains open. It creates no remote PR and does not prove live factory
cleanup.

## Recorded checks

On 2026-10-11, the slice passed these checks in an isolated Docker container
using the qualified image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
(Elixir 1.19.5, OTP 28). The source was overlaid from merged main `ec6c083`
plus this slice, with no shared worker or external GitHub calls.

| Slice requirement | Observed result |
| --- | --- |
| Keep legacy invocations compatible | No-argument, branch, repo, combined and positional calls passed. |
| Leave open PRs unchanged | Fake Git/GitHub call logs stayed empty; the PR fixture stayed open. |
| Work without repository/tools | Invocation from a non-repository directory with an empty PATH passed. |
| Preserve help and option errors | Both help aliases and unknown/missing option failures passed. |
| Execute through the removal hook | The real task ran in a shell before disposable workspace deletion; the PR stayed open. |

Commands run from `/workspace/elixir`:

```sh
mix compile --warnings-as-errors
mix test test/mix/tasks/workspace_before_remove_test.exs
mix format --check-formatted lib/mix/tasks/workspace.before_remove.ex test/mix/tasks/workspace_before_remove_test.exs
mix specs.check
mix pr_body.check --file /tmp/dev241-removal-hook-pr.md
```

All commands passed; the focused suite reported **5 tests, 0 failures**.
Running the same tests with the original task from `ec6c083` produced **3
expected failures**, including both direct invocation and shell-hook invocation
closing the simulated PR. The changed task was restored and the suite passed.

Broad `make all` is left to the PR's GitHub workflow under the approved alpha
policy. Full DEV-241 live acceptance remains pending; these tests prove only
the released removal-task safety slice.
