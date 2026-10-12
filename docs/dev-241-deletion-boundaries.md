# Workspace deletion boundaries (DEV-241, slice 2)

This slice hardens the workspace removal primitive. It does not authorize live
factory cleanup, verify GitHub merges, add coordinator retries, snapshot cancelled
work, or satisfy permanent archival and parent-output retention gates.

## Runtime and compatibility

Local removal and SSH hosts require **Python 3.11+ on PATH**, Unix directory
descriptors, `O_NOFOLLOW`, and `shutil.rmtree.avoids_symlink_attacks == True`.
Supported platforms are Linux (readable kernel `/proc/self/fdinfo` mount IDs)
and macOS (directory device identities). Mounted subtrees are rejected before
hooks and on each recursive directory open. Linux mount IDs also identify
same-device bind mounts. This does not introduce unmounting or elevated access.
Missing or unsupported runtimes return `workspace_remove_unavailable`; the
workspace remains retained. There is no fallback to `rm -rf` or `File.rm_rf`.
Verify the deployed pilot interpreter before any later activation. No real worker
deletion or deployment is part of this slice.

The current configured root is the authorization boundary. Previously recorded
paths under an old root return an error after configuration changes. Retain that
work until an operator resolves its recovery/cleanup with the correct trusted
configuration; never infer an allow root from the recorded path's parent.
The old recorded-symlink diagnostic remains only for compatibility.

Successful issue cleanup still returns `:ok`. Path/removal errors now propagate,
and multi-host cleanup stops at the first failure. The coordinator's existing
callers are unchanged and do not yet persist or retry those failures.

Ordinary direct removal of a truly absent path remains a successful no-op, even
outside the root, because no hook or deletion occurs. Dangling final or ancestor
links are not absence. Recorded outside paths and root equality are denied even
when the target is absent. Remote `~` roots retain their host-side expansion.

## Deletion and hook behavior

The same bounded helper runs locally or over the existing SSH transport. It opens
the physical configured root and ancestor components without following links,
retains directory descriptors, and checks root/ancestor/target identities after
the trusted hook. A hook-induced replacement retains work with an error.
Deletion uses descriptor-relative unlink or Python's symlink-resistant recursive
remover; a replacement symlink is not traversed into an outside tree.

Hooks still run before removal. Hook failure or timeout is logged and ignored
when the path remains safe, preserving the `SPEC.md` hook policy. Hook output is
discarded rather than copied into logs. A timeout kills/reaps the hook's process
group, including ordinary descendants. A child deliberately escaping that group
cannot be proven stopped by this primitive. Trusted hooks and independent worker
termination/fencing remain prerequisites for later managed cleanup; this slice
does not establish quiescence or ownership of arbitrary files within the root.

The helper has a hook timeout plus five-second total grace deadline. The Elixir
transport allows another second for termination. Responses are bounded to 64 KiB
while streaming, and contain status/error metadata only. A deadline or deletion error can leave a
partially removed owned tree; the caller receives a failure. Permanent verified
archival must precede actual factory deletion in the later cleanup owner.

## Verification status

Standalone Python tests on 2026-10-11 passed on disposable local directories:
ordinary deletion, ignored hook failure/timeout, outside/root/dangling denial,
recorded-root rejection, and hook-induced target/ancestor/root replacements.
Outside evidence and original moved work survived every replacement test.
A timeout test confirmed that an ordinary hook descendant cannot write its
marker after timeout. These checks used the actual embedded helper source.
The same disposable smoke also passed under Linux/Python 3.13.5 in an isolated
container from the qualified image; that container was removed afterward.
A deterministic replacement injected after the final identity check also left
the outside tree unchanged: recursive deletion used the retained parent
descriptor and removed only the original owned task directory. The dedicated
Elixir suite includes this same injection.

The scoped Elixir checks passed in an isolated, network-disabled container using
the qualified image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`
(Elixir 1.19.5, OTP 28, Python 3.13.5). The source checkout was copied into the
container; no shared source/build mounts or live worker paths were used.

- `mix compile --warnings-as-errors`
- Changed-file `mix format --check-formatted` and `mix specs.check`
- `mix test test/symphony_elixir/workspace_removal_safety_test.exs`: 17 passed
- Existing removal-hook suite: 5 passed
- `mix test test/symphony_elixir/workspace_and_config_test.exs`: 54 passed
- `mix test test/symphony_elixir/core_test.exs:541 test/symphony_elixir/core_test.exs:618`: 2 passed

These are 78 distinct focused tests. The dedicated suite executes generated SSH
scripts through a local fake SSH command, without contacting a remote worker.
Its fresh-caller recovery fixture reads a retained path after a configuration
reload; this is not a coordinator restart or durable-store acceptance test.
Running the recorded-root and recovery regressions against the original module
produced both expected failures: it deleted the disposable retained work. After
restoring this slice, all 17 safety tests passed again. Broad local CI was not run
under the coordinator's scoped build-slot release.

Independent Luna/max review found three gaps: recursive traversal of mounted
outside data, an output limit applied only after buffering, and an uncaught
interpreter-start failure. The source now checks mount identities before hooks
and on recursive opens, limits transport data while receiving it, and returns
startup errors. A synthetic mount-identity test retained the subtree without
running the hook. The Elixir regression suite passed. No privileged mount was
created; real mount qualification remains unverified.

A second review found that static SSH startup output could imitate a success
reply. Replies now require one matching per-call nonce, with a dedicated spoof
regression. This distinguishes accidental startup output; it is not attestation
of a malicious SSH host, interpreter or login profile. Those remain trusted.

The shared specification change is limited to **SPEC.md section 9.5**.

For a disposable development environment with the pinned Elixir and Python
prerequisites, run the dedicated suite from `elixir/`:

```sh
mix test test/symphony_elixir/workspace_removal_safety_test.exs
```

The fixtures create and clean up their own temporary directories. This command
does not authorize a production deletion or establish the later archival gates.
