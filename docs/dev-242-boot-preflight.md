# Offline coordinator boot/config preflight

DEV-242 can compare a bounded set of supplied nonsecret snapshots before a
future disposable restore. Run this checker locally. It does not contact GCP,
read runtime credentials, execute workflows, install state, activate a release,
start an owner or release dispatch. DEV-242's deployed acceptance remains open.

The known gap is between the release recorded as active and the release selected
by Compute boot metadata. This checker diagnoses that gap; it does not repair it.
`active.json` records a completed activation. Compute `factory-config` is the
boot target. A failed or staged activation can leave a different desired target
beside a valid older active release. Such drift is reported as
`pending_boot_transition`, with `boot_active_revision_mismatch`, rather than
being called corrupt state. Inconsistent records within either set are reported
separately.

## Prepare explicit inputs

Use a new canonical absolute staging directory, owned by the invoking UID with
mode 0700. Stage copies as regular, single-link mode-0600 files. The checker
requires exactly the following names, rejects symlink ancestry and traversal,
opens files through a pinned directory descriptor without following final
symlinks, and compares file/directory identities before and after reading. A
bounded byte readback also rejects same-size changes hidden by coarse timestamps. These
checks do not fence a live host or atomically protect ancestor paths against a
privileged concurrent rename. All supplied observations can become stale.

Do not stage `/run/factory/coordinator/*.json`, `.codex/auth.json`, token responses,
private keys or credential values. No input path found inside JSON or a workflow
is followed. Recovery-plan workspace/root paths are checked as syntax only.

| Staged name | Nonsecret source or operator input |
| --- | --- |
| `plan.json` | Independently accepted anchors described below. |
| `capture-manifest.json` | Exact completed local owner-capture manifest bytes. |
| `active.json` | Activation receipt from `/srv/factory/active.json`. |
| `active-public.json` | Selected public fields from `/srv/factory/active-public.json`. |
| `effective-public.json` | Same selected fields from `/etc/factory/public.json`. |
| `boot-config.json` | Selected fields from Compute's desired `factory-config`. |
| `active-release.json` | Active release's exact `RELEASE.json`. |
| `active-release-sha256.txt` | Active release's `.verified-sha256` marker. |
| `boot-release.json` | Desired boot release's exact `RELEASE.json`. |
| `boot-release-sha256.txt` | Desired boot release's `.verified-sha256` marker. |
| `active-references.json` | Retained nonsecret config-reference snapshot for the active revision. |
| `boot-references.json` | Retained nonsecret config-reference snapshot for the boot revision. |
| `active-WORKFLOW.md` | Workflow bytes selected by the active revision. |
| `boot-WORKFLOW.md` | Workflow bytes selected by the desired boot revision. |
| `recovery-plan.json` | Exact separately accepted private held-inspection plan. |
| `current-release.txt` | Observed resolved current path, `/opt/factory/releases/<40-hex-revision>`, plus newline. |

If both revisions match, copies of the same release/workflow/reference files
still occupy their two distinct names. If a required file is unavailable, the
checker rejects the set. Do not fabricate it or substitute today's main branch.
This is a coordinator-only checker; worker ledger/machine observations are later
requirements.

Public projections contain exactly the four required fields `project_id`,
`role`, `service_revision`, `release_sha256`, and only these optional fields:
`coordinator_workflow`, `coordinator_secret_env`, `coordinator_secret_versions`,
`coordinator_secret_fingerprints`, `enable_linear_webhook`. Preserve selected
values; absence retains the existing pilot/empty/false defaults. The boot
projection may additionally contain `secret_ids` and `secret_versions`, which
are resource references and numeric versions, never fetched values. Compute
metadata already contains the derived per-environment pins; preserve them too.
A supplied unknown field is rejected, not silently dropped by the checker.

Reference snapshots use the existing deployment allowlist: `project_id`, `role`,
`service_revision`, and optional `coordinator_workflow`, `coordinator_secret_env`,
`secret_ids`, `secret_versions`, `enable_linear_webhook`. Linear requires the
existing default environment pair and complete selected High Effort/Daybreak
pairs. Selected versions/fingerprints are recomputed locally using the deployment
formula; no secret is fetched. Unselected reference differences are visible
through `reference_snapshots_equal` but are not treated as changed active pins.

Linear workflows come from `/etc/factory/workflows/<revision>.md`; pilot
workflows are bundled in their release. Existing activation records do not bind
a workflow-content digest. The supplied independent workflow digest is an
operator anchor, not newly discovered historical proof.

`plan.json` has exactly `version: 1` and `expected`. The latter has exactly:

```json
{
  "capture_id": "accepted-capture-id",
  "manifest_sha256": "<accepted lowercase SHA-256>",
  "service_revision": "<accepted 40-hex capture/service revision>",
  "release_sha256": "<accepted release archive SHA-256>",
  "workflow_sha256": "<accepted active workflow bytes SHA-256>",
  "references_sha256": "<accepted active reference snapshot bytes SHA-256>",
  "recovery_plan_sha256": "<accepted held-plan bytes SHA-256>",
  "policy": {
    "version": 1,
    "gate_policy": "exit-status-v1",
    "lifecycle_sha256": "<accepted SHA-256>",
    "executor_sha256": "<accepted SHA-256>",
    "child_sha256": "<accepted SHA-256, omit for legacy policy>",
    "validation_sha256": "<accepted SHA-256>"
  }
}
```

Replace the placeholders with independently approved values. Policy has the
exact existing five-field legacy or six-field current shape: `version: 1`,
`gate_policy: "exit-status-v1"`, lifecycle/executor/validation SHA-256 pins and,
for the current shape, `child_sha256`. No policy pin is upgraded. The checked
recovery-plan schema is the [held startup plan](held-recovery-inspection.md).

JSON files are bounded to 64 KiB except the manifest at 1 MiB. Each workflow is
bounded to 1 MiB, markers/current path to 128 bytes, and the whole input set to
4 MiB. Duplicate JSON keys, unsupported schemas, deep input, unsafe files and
unknown fields reject. Rejection output contains static codes, not raw paths,
file contents or exception messages.

## Run and interpret

From the Symphony checkout:

```sh
python3 factory/deploy/recovery_preflight.py \
  --directory /canonical/private/offline-preflight
```

The JSON report always has `dispatch_allowed: false` and `restore_ready: false`.
Exit 2 means the supplied identities/pins aligned but recovery remains held.
Exit 1 means a mismatch or rejected input. It is not a successful-restore exit
code. The report includes input digests, static issue/blocker codes, capture ID
and validated revision identities; it does not print reference IDs or values.

The active set must agree across receipt, effective/retained public projections,
current path observation, release manifest/marker, and accepted capture/release
anchors. The boot release and selected reference snapshot must agree with the
boot projection. Boot versus active revision/archive differences remain visible.
Same-revision selected workflow/mapping/version/fingerprint/webhook changes are
reported as `same_revision_configuration_changed`. Both workflow copies must
match the independently supplied workflow digest. The private held plan must
match the accepted capture identity, revision and policy.

The helper checks the manifest's declared inventory, not the SQLite images.
Use the existing held inspector for their exact bytes and correlated payloads.
A `.verified-sha256` match is only a supplied archive-checksum receipt; this tool
does not read an archive or prove that the extracted tree still matches it.
Local owner-capture format does not bind deployment workflow/reference digests.
Even an aligned report therefore retains those explicit blockers.

## Remaining acceptance checklist

Source work and live prerequisites are separate. None of the following becomes
complete merely because this preflight aligns or a capability PR merges.

| Remaining step | Boundary |
| --- | --- |
| Integrate DEV-240's review/context shapes into held inspection and test real child capture/startup. | Source work owned by DEV-240; preserve existing exact linkage checks. |
| Bind deployment workflow, reference and release identities to a complete owner-capture contract. | Source work; do not infer missing historical digests from new files. |
| Wire approved held selection into cold-boot/service entry before normal activation or credential/intake startup. | Source work plus later boot qualification; ordinary `gcp-startup.sh` still invokes activation. |
| Export complete owner captures to immutable backup storage, verify readback, persist success and expose freshness/failure/disabled-schedule diagnostics. | Source work; the legacy cloud v1 backup remains a separate contract. |
| Install a verified capture into a new private destination with the original coordinator stopped, retaining rollback data. | Source work and later stopped-owner qualification; no installer is supplied here. |
| Bind old-owner fencing, exact worker machine/boot/unit/cgroup/process/request identity, and post-capture external events to one held recovery. | Source work through existing owners; unreachable stays unknown, with no replacement writer. |
| Reconcile original Linear deliveries/questions/replies, branch/PR, validation head/check, review decisions and child operations without repeated side effects. | Source integration, then live exact-identity proof. |
| Verify permanent archive objects and stable viewer identities outside disposable state/workspaces. | Source/provider reconciliation plus allowed signed-in viewer test. |
| Select exact approved releases, pinned secret references, disposable resource names and cost/retention/cleanup bounds. | Approval-dependent live preparation; existing cloud/IAM/budget boundaries persist. |
| Recreate disposable compute from IaC, apply held boot selection, restore during a wait and after an external action, and show old/new definition pins. | Approval-dependent live rehearsal; no deployment or IAM change ran here. |
| Demonstrate a stale/unreachable worker, one writer/PR/review lineage, retained counters/approval artifacts, backup freshness/failure alerts and archive lookup. | Full DEV-242 live acceptance; compare before/after identities before any continuation release. |

Keep the same PR/branch and human GitHub merge authority. Release a continuation
only through a separately qualified service-owned fencing/reconciliation
contract. Clean up only the released disposable environment after retaining its
evidence; never remove original recovery data or permanent archives.

## Local qualification

The focused standalone suite passed **12 tests** on host Python 3.14.6 and in
Linux Python 3.13.5, using image
`sha256:930b428bffd3752c5a53310b0b93173e0fdf95e307970bb2c5854d894611d3c9`.
The Linux test had no network, a read-only source mount, UID 65534, dropped
capabilities, 0.25 CPU and 128 MiB memory; its container was removed on exit.
No Mix build or shared-worker mutation ran.

```sh
python3 -m unittest discover -s factory/deploy/tests -p test_recovery_preflight.py
python3 -m py_compile factory/deploy/recovery_preflight.py \
  factory/deploy/tests/test_recovery_preflight.py
```

The local diagnostic logs are `/private/tmp/dev242-boot-preflight-tests.log` and
`/private/tmp/dev242-boot-preflight-linux.log`. Tests use synthetic nonsecret
snapshots, not a deployed capture or a trusted factory receipt. They check
aligned/old-boot cases, active and boot consistency, same-revision selected-pin
changes, unselected-reference drift, accepted hashes, legacy/current policy
shapes, unsafe/duplicate/oversized inputs and unchanged staging files. CLI checks
cover structured output, exit classes and absence of raw input/path leakage.

The initial Linux run exposed a same-size write whose timestamp did not advance;
the final suite includes that adversarial case and the bounded byte-readback fix.
The self-review checked fixed input inventory, read limits, static error output,
source-derived reference comparisons and the lack of any activation/credential
or provider path. The actual deployed Python/runtime, live metadata, release
availability, and all disposable restore acceptance remain unqualified here.


CodeQL initially classified dictionary reads of the fixture's `secret_ids`,
`secret_versions` and environment mapping as credential values flowing to a
file. The fixture now models synthetic resource identities/version pins
explicitly, validates their fake project/resource namespace and numeric versions,
and exports the existing nonsecret JSON schema. No scanner suppression or alert
dismissal was added. Both 12-test platform runs passed again after this change.
