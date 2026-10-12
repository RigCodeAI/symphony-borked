# Portable pilot deployment

For the bounded, read-only comparison of active versus desired boot pins, see
the [offline recovery preflight](../../docs/dev-242-boot-preflight.md). An aligned
report remains dispatch-held and does not qualify a deployed restore.

Native delegation uses a separate, opt-in contained-operation broker. Review
[its installation and qualification card](../../docs/contained-worker-operations.md)
before installing the control account or root service. Existing bootstrap and
activation do not enable that path.

These scripts operate a pilot release extracted under
`/opt/factory/releases/<service-revision>`. A deployment contains
`RELEASE.json` with a 40-character `service_revision` and a root-written
`.verified-sha256` file containing the artifact's SHA-256. Startup stages the
desired public settings in `/etc/factory/public.pending.json`; activation checks
that settings file against the release before it can become effective.

The service source is read-only after deployment. Runtime build data, logs,
workspaces, and evidence live under `/srv/factory`. `env.sh` pins Mix's build
and dependency paths to the release revision and uses the separate retained
home for the active service role. Startup enters the actual root account when
Compute's startup unit omits its home environment. Unprivileged DNS remains
available while metadata HTTP access is blocked.

Startup runs Mix setup, archive installation and compilation as the service
account with a cleared environment. Build dependencies and role-specific Mix
and Cargo caches are writable only for that unprivileged work. Root never
executes those caches or the legacy `/srv/factory/tools` tree. Rust, Node and
Codex provisioning uses `/srv/factory/bootstrap-tools-v1`, a separate root-owned
tree checked before reuse; root does not adopt worker-owned cached code by
changing its owner. Runtime reads those protected binaries while Cargo writes
its cache under the service account's retained home.
The coordinator runs the CLI from its release-specific built Mix project, keeping
Exqlite’s native SQLite library available. Service startup does not compile or fetch
dependencies.

Mix cache variables are set after mise selects its runtime, so mise's backend
defaults cannot redirect archive writes into the protected runtime tree.
The worker uses the executable `mix.sh` entrypoint under `timeout`. Run
`tests/mix-timeout.sh` in the disposable Linux image to check the service UID,
cache paths, argument handling and timeout termination.
The service account builds a project copy under
`/srv/factory/build/<revision>/project`; the coordinator launches its matching
CLI through `mix run --no-start --no-compile --no-deps-check` there. The verified
release source stays root-owned.
The Linux `tests/build-user.sh` regression builds the actual project twice,
forces a worker-controlled dependency to reload, and checks its recorded UID
and cleared credential environment. Run it in the disposable `dev230-checked`
image with `factory/` mounted read-only at `/factory-source`.

The worker's regular mode-0600 `.codex/auth.json` stays on the retained disk so
Codex can persist refreshed subscription tokens. `cloud_io.py model-auth` seeds
it from the pinned Secret Manager version only when absent or when that explicit
secret ID/version changes. A root-owned source marker records that choice.
Credential files are excluded from evidence and configuration object backups;
protected disk snapshots require the same private access as the retained disk.

The worker SSH host key also stays in root-only storage on the retained data
disk. Worker SSH waits for that mount; startup creates its safe runtime
directory and verifies that sshd offers only the retained Ed25519 key. Public
pin publication remains immutable. An existing deployment with GCE-regenerated
keys needs an operator to authenticate the new public key through the project
serial console before rotating that one public pin; never accept a handshake
fingerprint or disable strict checking as a recovery shortcut.

## Activate a release

Run activation as root after verifying and extracting a release:

```bash
sudo /opt/factory/releases/<service-revision>/factory/deploy/activate.sh \
  /opt/factory/releases/<service-revision>
```

Activation checks the release SHA against staged settings (or the current
effective settings during an explicit root activation), validates both
pass/fail workstream inputs as the service user, and atomically changes
`/opt/factory/current`. It promotes pending public settings and saves the last
effective settings in `/srv/factory/active-public.json` only after activation
succeeds. Coordinator activation restarts
`factory-coordinator.service` and waits for the localhost state endpoint. If
restart or health fails, the previous current link and active metadata are
restored, while the previous public settings remain effective. Worker activation
performs the same definition checks and link
switch without stopping an in-flight worker task; each runner already uses its
immutable release path and rejects a run if the active release changes before
it finishes.

## Submit the worker smoke

By default the coordinator's Symphony service uses `PILOT-WORKFLOW.md`, an idle memory
tracker with no hooks. It listens on port 8080 on the VM's private interfaces;
the health check uses localhost, and GCP firewall rules restrict ingress to
load-balancer health checks and proxies when optional HTTPS ingress is enabled.
Pilot tasks start only when an operator invokes `factory-pilot` as the
coordinator user. Submit separate pass and fail runs with unique IDs:

DEV-233 adds an explicit Linear deployment mode, protected per-release credentials and
`/etc/factory/workflows/<revision>.md`. Install workflows without overwriting an existing
revision; changes to integration pins require a distinct committed release. The signed
webhook uses a separate listener, while dashboard/API remain behind IAP.
See [Linear deployment](../../docs/linear-delegation.md#gcp-coordinator-deployment) and
the [pending morning card](../../docs/dev-233-morning-decision.md). These paths are locally
tested and have not been applied to the pilot.

```bash
sudo -u factory-coordinator -- \
  /opt/factory/current/factory/deploy/factory-pilot submit worker-a dev230-pass-01 pass
sudo -u factory-coordinator -- \
  /opt/factory/current/factory/deploy/factory-pilot submit worker-a dev230-fail-01 fail
```

The command resolves the worker's private IP from public config and connects
with `/run/factory/ssh_config`, whose host key is pinned. It passes the active
release revision and SHA to `worker-run.sh`; the worker verifies both before
running `mix workstream.run` from the matching immutable release. The worker
clones `/srv/factory/seed` with `--no-local`, pins the clone to the seed HEAD,
and replaces the fetch and push URLs with `DISABLED` before any agent turn.
The coordinator and worker each hold a per-worker lock, and the worker rejects
run ID reuse.

The `pass` run must complete with a successful executable gate. The `fail` run
must block on its intentionally failing gate and return a nonzero Mix status.
The operator command treats those expected outcomes as a successful smoke run.
Both runs keep their workspaces and evidence separate.

Each worker run records `report.json`, `mix.log`, `candidate.diff`,
`revision.json`, and `finished.json` under `/srv/factory/runs/<run-id>`. It
redacts model credentials and common token formats before writing those files.
The coordinator validates an allowlisted tar containing only these regular
files, checks their hashes, and records the coordinator-side submission in
`submission.json`. It publishes its `finished.json` marker only after that
receipt is final. The existing root archive timer uploads completed evidence;
this CLI does not create or publish cloud resources itself.

This is an operator-driven two-run smoke for the local workstream. It does not
add scheduler behavior to Symphony or establish durable run recovery, review,
publication, merge, or a general remote-work protocol.
