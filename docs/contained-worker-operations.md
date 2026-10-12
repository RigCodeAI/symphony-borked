# Contained worker operations (DEV-233)

This is the SSH/systemd control path for native delegation. Root containment, forced-SSH
RPC/stream checks, the live Elixir client, subscription readiness and native active
cancellation passed. The fresh DEV-248 run survived coordinator restart with the same
foreground worker, then native undelegation stopped its exact operation. Confirmed stopped
attempts and reconciliation metadata now match the trusted termination proofs. See the
[restart repair acceptance](evidence/dev-233/restart-repair-acceptance.md) and the
[historical native checkpoint](evidence/dev-233/native-live-acceptance.md).

The existing `factory-worker` identity keeps its qualified subscription auth.
A separate `factory-control` account receives a forced SSH command through the
existing pinned coordinator key. It has no sudo or polkit grant, PTY or forwarding.
A root-owned Unix socket broker checks the kernel peer UID. The agent cannot use
that socket or create system units. The broker asks PID 1 to launch a fixed,
root-owned wrapper as `factory-worker`; it never executes task code as root.

There is one active or unknown operation per worker UID/host. This proves process
containment, not isolation between historical tasks sharing the worker home and
subscription auth. Publisher, Linear and control credentials stay outside the
agent account and its environment.

## Operation contract

The coordinator pins the SSH host, machine identity, service revision and release
checksum in the durable run. It never retries an operation on another machine.
The worker reserves the operation ID and canonical request digest before launch.
A repeated ID cannot create another unit; changed arguments reject. A lost launch
response reserves capacity. Unit names are hashes of operation IDs, not paths or
client-selected systemd properties.

Preparation waits for a live process in the exact contained unit. The unit
starts in its canonical workspace; workspace paths reject systemd substitution
characters. The prepared unit waits. Its operation ID, OS boot, machine, unit invocation,
cgroup and request digest are returned and committed by the current coordinator
worker before release. Only a matching root-written release marker allows the
wrapper to exec. Stream loss, coordinator restart or a stale identity cannot
authorize another launch.

The broker owns the operation pipes after the first stream claim. A full coordinator SSH
disconnect detaches that client; the broker keeps app-server stdin open. Closing only SSH
input preserves the client's output connection so it can receive the remaining output.
A later connection
may attach to the same exact identity, with one attached client at a time and no second
launch. Each input/output buffer is bounded to 1 MiB and applies backpressure. Terminal
output retention expires 60 seconds after trusted termination; cleanup then reaps only
that stream helper. Coordinator recovery still reserves unknown
operations; this transport behavior does not automatically resume a conversation.
A worker broker process restart loses its in-memory stream sessions. This repair qualifies
coordinator restart survival only; other disconnects still require trusted reconciliation.

The unit has no capabilities, no delegation, a read-only cgroup hierarchy and
hidden system/control sockets. Only its dedicated workspace and qualified worker
home/caches are writable. It inherits a fixed runtime environment; request and
coordinator secret environments are discarded. Child `setsid` calls remain inside
the same cgroup.

Reviewer preparation has a separate, opt-in `prepare_review_workspace` capability.
Its root-owned daemon configuration accepts `review_preparation_command_sha256`,
the digest returned by `ReviewWorkspace.command_sha256()` in the packaged source.
Omitting this key disables preparation; `review_preparation_v1` advertises availability.
The broker derives one private leaf from run and stable operation IDs and stores
its immutable ownership pins in protected state. Only that leaf is writable for
preparation, with source Git read-only and no worker home/cache grants. Destination
paths and systemd properties cannot be supplied by the caller. The pinned script
has a separate bounded allowance; generic argv limits are unchanged. Held identity,
release, cancellation and terminal proof rules remain the same. This capability
requires a new deployed containment qualification; historical DEV-233 receipts do
not qualify its mount grants. See [release and rollback requirements](review-repair.md#contained-preparation-release-requirements).

Cancellation commits a stop tombstone immediately and runs bounded control work
outside the webhook handler. It validates the exact operation/boot/invocation,
sends TERM, then KILL if needed. Capacity releases only after a recorded empty
cgroup and terminal main process. An exact terminal invocation with an empty `ControlGroup`
property can prove systemd released the empty cgroup; a still-reported path requires
readable `cgroup.events` with recursive population zero. A missing unit, missing evidence, changed boot,
connection failure or timeout stays unknown. Confirmed termination cancels the current
executing attempt and operation together and removes stale reconciliation metadata.
Restart normalizes older stopped records with committed trusted confirmation without
repeating cancellation or changing pinned definitions. An unknown stop still reserves
capacity and retries exact cancellation. The stopped run cannot restart.
Normal stream completion also requires termination proof before stage completion;
an unproven outcome stays reconciling and reserves capacity.

## Reviewable installation change

The [installer](../factory/deploy/install-worker-operations.sh) and
[service template](../factory/deploy/factory-worker-operations.service) are explicit
operator actions. Existing bootstrap/activation does not invoke them. Installation
creates the control account, a root-owned forced-command key, private operation
ledger, worker-readable held manifests, a protected contained-workspace parent,
and broker configuration/service. It does
not start or enable the service and does not modify public ingress or cloud IAM.
It rejects account groups or writable root code paths that broaden access.

Review the root broker and control-account grant as an explicit operator action.
That review, installation and service activation are complete on the existing worker;
its receipt and forced-SSH results are linked above. For another worker, run the
installer on a verified release as root, using only the coordinator's bare public key,
then review the emitted configuration before explicit service activation. Do not run
it from this source checkout or execute worker-controlled caches as root. SSH still
requires the existing authenticated host key and private-network access. Use
`factory-control@<existing-worker-alias>` to reuse the existing strict host/key pin,
or add a dedicated control alias with `User factory-control`. Keep the operator pilot
route separate.

The installer creates `/srv/factory/contained-workspaces` as root:factory-worker
0750 and preserves the legacy `/srv/factory/workspaces` tree. Before configuring an
issue workspace, an operator must provision its worker-owned leaf under this
protected parent and its matching coordinator clone path. The worker can clone
and edit inside the leaf, but cannot rename or replace sibling workspace names.
The current pilot uses explicit, pre-provisioned issue workspace mappings; it
does not automatically provision contained workspaces.

The concrete access change is:

- Create the system account/private group `factory-control` with home
  `/var/lib/factory-control` and a forced-command Ed25519 key. Its root-owned
  home and `.ssh` are 0755; `authorized_keys` is 0644. No sudo or polkit rule.
- Create root-owned `/var/lib/factory-operations/records` (0700) and `gates`
  (0755); manifests and release markers are root-owned 0644. The agent can
  read its launch gate but cannot write it or read the private ledger.
- Write `/etc/factory/worker-operations.json` as root 0600, pinned to the
  machine and verified immutable release. Write one system unit,
  `factory-worker-operations.service`. It runs the protected Python broker as
  root with group `factory-control` and supplementary `factory-worker` only
  for workspace traversal, an empty capability bounding set, a protected
  filesystem and writable ledger/runtime directories only.
- The broker creates `/run/factory-operations/control.sock` (0660) beneath a
  root-owned control-group directory (0750) and checks the peer UID. Each
  operation creates one uniquely hashed transient service as `factory-worker`.
  No cloud IAM, public ingress, dashboard access or publisher grant changes.

The following commands are the retained qualification procedure. They were run on the
existing idle worker and produced the passing evidence linked above. Substitute the
verified release and coordinator's **public** key file only when qualifying another
idle disposable worker; never use an active factory worker:

```bash
/opt/factory/releases/<revision>/factory/deploy/install-worker-operations.sh \
  /opt/factory/releases/<revision> /root/coordinator-public-key
FACTORY_SYSTEMD_TEST_DISPOSABLE=1 \
  FACTORY_SYSTEMD_TEST_MANAGER_REEXEC=1 \
  FACTORY_SYSTEMD_TEST_RECEIPT=/root/worker-operations-qualification.json \
  /usr/bin/python3 -I \
  /opt/factory/releases/<revision>/factory/deploy/tests/test_worker_operation_systemd.py
```

Inspect the receipt and test output before enabling dispatch. The current passing
receipt is installed as root mode 0600 at
`/etc/factory/worker-operations-qualified.json`, and the service is active. The
coordinator's live Elixir `WorkerOperation.qualify/1` call using the pinned control
configuration passed in the later native pilot. A successful host card alone does not
prove native Linear acceptance.

Rollback first disables native dispatch and removes the qualification receipt.
Keep the broker available until exact stop/reconciliation proves every existing
operation terminated; an unknown outcome keeps capacity reserved. Then disable
and stop the broker and remove the control account's authorized key to revoke
access. Keep operation records and the immutable release for recovery/audit;
do not delete ledgers, worker home, auth or workspaces as rollback. Existing
operator pilot SSH and the prior active release are unchanged by this installer.

The expected coordinator configuration, once qualified, is:

```yaml
linear_delegation:
  # Existing organization/app/team/token/path settings also required.
  worker_control:
    host: qualified-worker-control-alias
    machine_id: qualified-worker-machine-id
    service_revision: 40-character-verified-release-revision
    release_sha256: 64-character-verified-release-checksum
    argv:
      - /srv/factory/bootstrap-tools-v1/rig-tools/codex/bin/codex
      - --disable
      - apps
      - --disable
      - plugins
      - --disable
      - multi_agent
      - -c
      - 'forced_login_method="chatgpt"'
      - app-server
```

Both coordinator and worker must map the pinned dedicated workspace path. The
coordinator keeps definition/workspace safety checks; the agent and executable
candidate check run on the worker. `codex_command` overrides cannot accompany
`worker_control`. The resolved agent authentication reference and credential
exclusions are pinned with the run. Daybreak remains blocked by DEV-231.

## Verification boundary

Controlled engine tests check crash/replay, held release, exact identities,
TERM/KILL outcomes and unknown capacity. Transport tests check real Unix peers,
framing and real held wrappers. Elixir tests check the SSH protocol, durable
registration ordering and trusted-proof-only completion. Controlled managers do
not prove real systemd containment.

The [opt-in systemd card](../factory/deploy/tests/test_worker_operation_systemd.py)
requires an explicitly supplied disposable Linux host with PID 1 systemd,
cgroup v2, the qualified runtime/account and systemd 252 or newer. It launches
real units with a `setsid` child that ignores TERM, verifies held launch and
replay, rejects a stale identity, and requires proof that both processes stopped.
It also checks natural exit through systemd's released-cgroup proof and durable
controller recovery. With the separate explicit `FACTORY_SYSTEMD_TEST_MANAGER_REEXEC=1`
opt-in, it re-executes PID 1 while held and while a child is live, then verifies the same
invocation/cgroup before stopping the whole tree. This is a host-wide manager operation;
run it only on the idle disposable qualification worker. That check is required in the receipt.
It uses a private test socket and only test-created units.
It skips without that host; a skip is unverified acceptance. The card has now passed on the
existing worker, including manager re-execution, held launch, duplicate prepare, stale
identity rejection, `setsid` child termination, natural exit and restart recovery. The
coordinator's actual forced-SSH path separately passed RPC, stream, stop proof/replay and
natural-exit proof/replay; an arbitrary command was rejected. See the retained
[root-card](evidence/dev-233/worker-live-card.json),
[forced-SSH report](evidence/dev-233/worker-ssh-qualification.json) and
[test output](evidence/dev-233/worker-live-report.log).

The initial worker control source was `cc85ca7871a27afb03bc2b6e86722a142ff93e7e`, archived as
`59a9c8668cc258ce19fc50d9d339d3c6fa1920b70b8b14c531bdbfeb6124cb01`. The recorded systemd
version is 252. The active application release remains
`541279aeb6a5366571e1ee8935e134c728d91c63`. The maintenance preserved the same VM instance,
restored metadata exactly, and preserved disks and network configuration. That initial
result did not establish native Linear/Codex dispatch. The later client and cancellation
results are recorded in the native checkpoint above. The card log also contains Python
`ResourceWarning`s for helper
`Popen` handles. A later read-only check found both warned helper PIDs absent, while the
operation cgroup and both task processes were independently observed empty/dead after stop.
This does not establish that Python closed every helper handle; the warnings remain in the
retained log. See [the process check](evidence/dev-233/worker-helper-process-check.json).
Do not run this card on an active factory worker.

A root-owned mode-0600 qualification receipt at
`/etc/factory/worker-operations-qualified.json` must match the tested machine, OS
boot, systemd version, service revision and release checksum. Missing, stale or incomplete receipts
block production qualification. The installer never creates a passing receipt.
The card can write a new root-private output receipt when
`FACTORY_SYSTEMD_TEST_RECEIPT` is supplied; it writes only after every check and
cleanup succeeds. Review that output before installing it at the fixed receipt
path. Run the card from the configured immutable release with
`FACTORY_SYSTEMD_TEST_DISPOSABLE=1` and `FACTORY_SYSTEMD_TEST_MANAGER_REEXEC=1`;
its default skip cannot generate a receipt.
The fresh restart-survival and stopped-state pilot passed on source `064f892` and the
current worker boot. See the [live acceptance record](evidence/dev-233/restart-repair-acceptance.md)
for current pins and remaining limits; historical host cards qualify only their recorded releases.

### Existing systemd 252 worker

The adapter supports systemd 252 and newer. It adds `--expand-environment=no` on 254+
and omits that unsupported flag on 252/253. Only the protected wrapper, literal
`--manifest` and hashed descriptor path reach systemd-run; `$` and `%` in those fixed
paths reject before launch. Task argv stays in the protected descriptor and is executed
directly by the held wrapper after durable release. Isolation properties remain identical.

This compatibility decision follows the [v252.39 CLI and argv construction](https://github.com/systemd/systemd-stable/blob/v252.39/src/run/run.c),
[recursive-empty cgroup release](https://github.com/systemd/systemd-stable/blob/v252.39/src/core/cgroup.c),
[ControlGroup reporting](https://github.com/systemd/systemd-stable/blob/v252.39/src/core/dbus-unit.c)
and [cgroup serialization/restoration during manager re-execution](https://github.com/systemd/systemd-stable/blob/v252.39/src/core/unit-serialize.c).
Terminal state, matching invocation/principal/request, and zero MainPID remain mandatory
for released-cgroup proof. The live receipt now qualifies this adapter on the recorded
machine, boot and release; it does not qualify a different boot or source revision.

The containment design follows systemd's
[whole-cgroup kill behavior](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml)
and [cgroup protection](https://github.com/systemd/systemd/blob/main/man/systemd.exec.xml),
and the kernel's [recursive population contract](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html).
