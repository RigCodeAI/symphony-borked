# Local candidate commits

The software-change workstream now uses `implement` → `local-commit` →
`inspect-candidate` → `candidate-ready`. The agent edits and runs development checks,
then leaves its changes uncommitted. The fixed service check creates the local commit
as the existing unprivileged worker. Publication and human GitHub merge remain separate.

## Why the commit is service-owned

DEV-251's real Rust test passed, but `git add` could not create `.git/index.lock`.
The worker owns its clone and Git metadata, and its underlying mount is writable.
Codex's inner `workspace-write` sandbox protects `.git`; `approval_policy: never`
does not grant access beyond that sandbox. Changing ownership would not fix this
boundary. The new flow retains the sandbox rather than giving the model writable
Git configuration and hooks.

The commit command is literal Python in the pinned workstream definition. It runs
through the existing contained executable-check path, after trusted termination of
the agent, with the check's operation identity saved before release. It uses fixed
system executables and a clean Git environment. Hooks, signing, prompts and network
protocols are disabled. Linked/shared metadata, metadata symlinks, detached HEAD,
missing candidate input and unsupported Git configuration block the gate.

Remote check and launch validation allow this exact service-pinned producer's
inline script up to 8 KiB. The runner supplies the saved producer command digest
only for its pinned stage; the root worker broker independently checks its
protected digest. Ordinary arguments remain limited to 4 KiB. This allowance
does not change the producer body, input identity or repair ancestry checks.
The SSH parser can forward the bounded fixed wrapper envelope; the root socket
parser requires the protected producer digest before dispatch, and the operation
controller verifies it again before preparation. Forwarding does not authorize execution.

The service stages nonignored changes and creates one commit with a fixed author and
`Factory-Candidate` marker derived from the saved implementation result. Repeating
the same check on a clean checkout succeeds only with that marker; a clean base or
another implementation result cannot be reported as a prepared candidate. Failure
preserves the checkout for diagnosis rather than resetting or discarding changes.

## Candidate transport contract

The `candidate-commit` check output contains `exit_status` and a JSON object in its
`output` field:

```json
{
  "candidate_commit": "<full Git SHA>",
  "base_commit": "<immediate parent SHA>",
  "candidate_input_sha256": "<saved candidate-input digest>",
  "branch": "refs/heads/<local branch>",
  "candidate_bundle": ".git/factory-candidates/<input digest>.bundle",
  "bundle_sha256": "<bundle SHA-256>",
  "bundle_bytes": 1234
}
```

The fixed check generates that path and a Git bundle containing the new commit,
excluding its immediate parent. Git verifies the bundle prerequisite and exact head
before the output is emitted. `base_commit` is the bundle prerequisite, not a substitute
for the repository's approved validation base. The bundle resides under the model's
protected Git metadata. No model-selected snapshot path is accepted.

After verifying the bundle, the trusted producer grants read access to its existing
worker group: `0750` on `.git/factory-candidates` and `0640` on that bundle only.
Both objects must retain the producer's UID/GID; the file must be regular with one
hard link. Permission updates use no-follow file descriptors. The broker already
belongs to `factory-worker` and retains its empty capability bounding set. Its reader
checks the configured worker group, exact modes, ownership, no-follow paths, stable
file identity, size and checksum after exact successful operation/termination checks.
The producer also sets only its task root and `.git` to `0710`, checking their UID/GID
with no-follow directory descriptors and granting task-root search last. The broker
uses Linux `O_PATH` handles for those parents and checks their configured worker group;
search permission suffices without granting listing or write access. The root-owned
shared workspace root keeps `0750`. No recursive permission changes are performed.
Existing worker-group members can reach known descendant paths and read files whose
own permissions permit it (for example `0644`); unrelated `0600` files remain private.
The producer grants no group write or world access. Private broker records keep their
permissions. Legacy `0700`/`0600` bundles remain accepted when already readable;
deployment does not automatically change old artifacts or run pins.
This changes the pinned producer command and workstream definition. Deploy matching
pins and regenerate through an authorized producer run; an old private bundle alone
is not made readable by upgrading the broker.

A consumer must bind the output to the pinned successful service check and its exact
terminated worker operation, retrieve through an authorized bounded channel, verify
bytes/checksum, and import into a controlled coordinator checkout of the prerequisite.
Then it must verify the candidate head, branch and source before trusted validation.
The coordinator's unchanged seed clone cannot stand in for the worker candidate.
Candidate transfer reads the registered systemd identity from the completed operation's
`external_process`; `worker` identifies the coordinator's local Elixir task. Legacy
records may supply the remote identity in `worker` only when `external_process` is absent
or nil. A present invalid identity blocks transfer, and exact attempt, stage, completion
and fresh remote termination checks still apply.
DEV-238 owns automated bundle retrieval and publication-side consumption; the producer
alone does not prove automated transport, validation, archival or publication.

## Verification and rollout

Focused contracts exercise real Git commits, clean-base rejection, same-input replay,
different-input rejection, disabled hooks/signing, configuration rejection, and bundle
import into a receiver that did not already contain the candidate commit. Run from
`elixir/`:

```bash
mise exec -- mix test test/symphony_elixir/local_candidate_commit_test.exs test/symphony_elixir/software_change_test.exs
mise exec -- mix test test/symphony_elixir/worker_operation_test.exs
```

From the repository root, run the broker and transport regressions:

```bash
python3 -m unittest factory.deploy.tests.test_candidate_bundle_rpc factory.deploy.tests.test_worker_operation factory.deploy.tests.test_worker_operation_transport
```

The bundle RPC suite includes a Linux root regression that runs the real producer as
a separate worker UID beneath the actual root-owned `0750` shared root and initially
worker-owned `0700` task root / `0755` Git directory. Only the shipped producer changes
permissions before broker dispatch with all effective, permitted and bounding capabilities
dropped. It reproduces the old parent `EACCES`, proves the full bundle read, rejects parent
listing/writes and unrelated private-file reads/writes, and demonstrates the disclosed
known-`0644`-path access. It skips when Linux/root
or the fixture setup capabilities are unavailable; such a skip is not verification.

Live verification uses a dedicated disposable Rig clone with push disabled, the approved
contained worker, an isolated journal outside the clone, real agent/check operations,
and exact termination proofs. Compilation/focused checks and live results must be
recorded separately; this document does not assert deployment.

New definitions apply only to new runs. Old questions, waits, attempts, stop tombstones
and uncommitted changes retain their original pins and history. Do not repin an old run
or answer a stale question to force it through this new pipeline.

The command-bound fix changes the executor fingerprint. Deploy a matched coordinator
and broker release with the existing producer digest, then use an explicitly supported
new-run or recovery path. A previously blocked run must keep its original pins and
failure receipt. Local remote-validation tests do not qualify live recovery.
