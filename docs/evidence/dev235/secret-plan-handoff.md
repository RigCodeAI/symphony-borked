# DEV-235 Secret Manager Terraform handoff

Final result: the refreshed full preprovision plan is complete and contains exactly four secret-container creates and four coordinator-only access grants. Nothing was applied.

This is a sanitized, read-only planning receipt. Terraform init/state reads/plans only were run. This planning task performed no apply, secret-version upload, application creation, or cloud resource mutation. Earlier approved OAuth record creation is recorded separately in deployment-boundary.json. The raw plan files and staged inputs remain private in this directory; this report contains no secret contents.

## Existing deployment and state

- Project: `factory-511117`; region/zone: `us-central1` / `us-central1-a`.
- Existing Terraform backend: GCS bucket `factory-511117-symphony-tfstate`, prefix `dev-230/pilot` (`infra/gcp/pilot/backend.hcl.example`).
- State snapshot used for this plan: serial `51`; Terraform `1.13.5`.
- Existing inputs: `/private/tmp/dev233-ingress-plan/dashboard.tfvars.json` and `/private/tmp/dev230-private/pilot.tfvars.json`. The supplied pilot tfvars are older than live state; a private review overlay was built from those inputs plus verified current coordinator metadata/state. Its intended configuration preserves GCP metadata revision `064f892f95847ea2eed0eb684da36c6e1bb5fce6`, existing Linear default secret mappings/pins, and active ingress settings. That original baseline full plan proposed VM metadata/startup updates. The final snapshot plan described below preserves them.
- Exact existing coordinator service account (state output and current instance state): `rig-factory-coordinator@factory-511117.iam.gserviceaccount.com`.
- Active gcloud caller observed: `adam@rig.ai`. This identifies the planning caller; bucket IAM ownership was not independently established.
- Manual coordinator disk activation verified in DEV-235 was `3667340`; GCP boot metadata remains `064f892f...`. Worker control stays `064f892f...`; worker app remains `541279`. The overlay preserved these release selections as inputs. The original broad VM updates remain unapproved; the final snapshot plan does not propose them.

## Requested-scope preview

The targeted preview has exactly eight creates and no drift: four Secret Manager containers in `factory-511117` with automatic replication, and four secret-scoped `roles/secretmanager.secretAccessor` grants to the coordinator service account above. The four container IDs are:

- `linear-high-effort-client-secret`
- `linear-high-effort-webhook-signing`
- `linear-daybreak-client-secret`
- `linear-daybreak-webhook-signing`

The plan contains no secret payloads or secret versions. The intended first uploaded version remains numeric version `1`; values were not fetched, printed, or staged here. The targeted plan reports `applyable=true`, `complete=false` because it was produced with `-target`; it is an inspection preview only and is not an apply artifact.

## Previous baseline full-plan blocker (historical)

The complete full plan has 12 non-no-op actions and four drift entries. It is broader than the approved scope and must not be applied:

- Two VM updates: `module.pilot.google_compute_instance.coordinator["main"]` and `module.pilot.google_compute_instance.worker["0"]`. They include metadata/startup-related changes; the coordinator `factory-config` change is caused by current Terraform coupling of optional secret refs into VM metadata.
- Four requested Secret Manager container creates and four requested scoped IAM grant creates.
- Two release-reader IAM replacements: `module.pilot.google_storage_bucket_iam_member.coordinator_release_reader` and `module.pilot.google_storage_bucket_iam_member.worker_release_reader`.
- Four drift updates detected in the refreshed plan: `module.pilot.google_storage_bucket.releases`, `module.pilot.google_storage_bucket_iam_member.worker_host_key_creator`, `module.pilot.google_storage_bucket_iam_member.worker_host_key_reader`, and `module.pilot.google_storage_bucket_iam_member.worker_release_reader`.

The coordinator and worker updates violate the no-VM-metadata boundary; the release-reader replacements are also outside the requested IAM scope. No general drift repair was attempted. Before any later apply, refresh state and re-plan under the live-slot procedure because other work may have changed the deployment since this snapshot.

## Provenance and private artifacts

- Source repo commit: `1c602878879f96ca5a9fd37a65f79713a0f5905d`; staged Terraform/source set: 28 files, SHA-256 `e7aba5d07de4027eeffb617cd00099307d25b2032ae76e7a02614e945d6a88ba`.
- `dashboard.tfvars.json` SHA-256: `bc08891e662a0ddf0d7ace5b61086ceda217aaac4936ed7f5dc783234d9de0f9`.
- `pilot.tfvars.json` SHA-256: `690709b110b8519fcf2ce9c3195dd48e0d67f7a0e24e63675f10ed1312a383f8`.
- Private overlay SHA-256: `9f3091b4d506b066fa26af1ce66cc3a18f5ea259b1ca7e4bf2ee39e7d72d023c`.
- State snapshot SHA-256: `581e218efe65215a64f2afb3d164ef690ec6047b1cf731aeb5876bac28d81cad`.
- Full plan: `/private/tmp/dev235-terraform/full.tfplan`; JSON SHA-256 `2fe3d109df9cfe803ea841cca4aa6f2a67dbd4ae7142cc4632ee3baa7d4ac5e7`; binary SHA-256 `451c2a27f76c582e8966a33d52d3805df41b3668edcb87fda86c0dfe6bcaf51e`.
- Targeted inspection preview: `/private/tmp/dev235-terraform/targeted.tfplan`; JSON SHA-256 `ad770126f6018be72402766c55e0b0979663187b1bcae9cb9a8993455a6426d4`; binary SHA-256 `972a1eabc5006f678a7f1af5f997c73d3908900364689b2057d0f3208d4a3367`.

The full JSON plans, plan logs, private overlay, and source copy are sensitive and should remain private with file mode `0600` in directory mode `0700`. Temporary copied gcloud credentials and transient access tokens are absent; no credential store copy remains in this directory.


## Final staged pre-provision full plan

A second, full refresh-enabled plan was prepared from a private copy of source commit `1c602878879f96ca5a9fd37a65f79713a0f5905d`. No repository files were edited. The private staging copy adds a pre-provision snapshot input so the approved empty secret containers and scoped grants can be planned without activating new coordinator routes or changing existing VM metadata/IAM conditions:

- Root wiring: `infra/gcp/pilot/variables.tf` and `infra/gcp/pilot/main.tf` add and pass `dev235_preserve_preprovision_snapshot`.
- `infra/gcp/modules/pilot/variables.tf` declares the private snapshot object.
- `infra/gcp/modules/pilot/compute.tf` selects the snapshot's current coordinator and worker metadata maps when supplied; the existing generated metadata remains the fallback.
- `infra/gcp/modules/pilot/identity.tf` selects the snapshot's current coordinator and worker release-reader conditions when supplied; the existing generated conditions remain the fallback.

The snapshot was captured from a full Terraform plan with refresh enabled. Its four override fieldpaths are `dev235_preserve_preprovision_snapshot.coordinator_metadata` (6 metadata keys), `.worker_metadata` (6 keys), `.coordinator_release_condition` (3 fields), and `.worker_release_condition` (3 fields). Canonical JSON SHA-256 values are respectively `6a6d75a80f25b8f379e29abae77eb16974ce7365803c0e1cff8c97713e53f2d2`, `0571b22b001764d78d844777b582fc16a83bec3bb566d2c40fc182b099fe7e46`, `2cee0c27ad8d0d2343125462e7c54704780f5fb89e7f0863097646a1d36157eb`, and `33480440512ec1ce2f589928536bb63238b55bb2f3994835b5610f8820adfd9b`. The protected input file is `/private/tmp/dev235-terraform/preprovision-snapshot.tfvars.json`, SHA-256 `681791e69e83eb6d34153af72d2812be9ab19228a3ba29c6bc65b498c6f0dc0d`.

The coordinator snapshot retains GCP metadata `service_revision=064f892f95847ea2eed0eb684da36c6e1bb5fce6`; its current `factory-config` only maps the existing `LINEAR_API_KEY` and `LINEAR_API_TOKEN` entries, both pinned to version `1`. The worker snapshot retains metadata revision `541279aeb6a5366571e1ee8935e134c728d91c63`. The coordinator release-reader condition retains six current archive pins (`064f892f...`, `9c026077...`, `3e81a8a4...`, `a4c95481...`, `080c0484...`, and `541279ae...`) plus the worker host-key object; the worker release-reader remains pinned to `541279ae...`. The previously verified manual coordinator disk activation `3667340` is separate from these GCP metadata values. The full metadata and startup script contents stay only in the protected snapshot and plan artifacts.

The final plan used a normal full refresh, with no `-target`, `-refresh=false`, or lifecycle ignore rule. Its detailed exit status was `2` (successful plan with changes); Terraform reported `applyable=true`, `complete=true`, `errored=false`. The planned action set has exactly eight creates: the four named containers, each in project `factory-511117` with automatic replication, plus four secret-scoped `roles/secretmanager.secretAccessor` grants to `serviceAccount:rig-factory-coordinator@factory-511117.iam.gserviceaccount.com`. There are no VM changes, release-reader IAM changes, Secret Manager version resources, or secret payload fields. The existing metadata maps and both release-reader conditions compare equal before/after in the plan. New integrations remain absent from the coordinator metadata, so this is container-and-grant pre-provisioning only.

The refreshed plan still reports four drift entries: `module.pilot.google_storage_bucket.releases`, `module.pilot.google_storage_bucket_iam_member.worker_host_key_creator`, `module.pilot.google_storage_bucket_iam_member.worker_host_key_reader`, and `module.pilot.google_storage_bucket_iam_member.worker_release_reader`. These are visible in `resource_drift`; none is a non-no-op planned action. No broader drift repair was attempted. A later apply must use a new full refresh and a newly reviewed plan after DEV237 work; this saved plan is only a planning artifact and was not applied.

State pulled after the final plan remained serial `51`, lineage `c978a3aa-5fef-dd4a-0483-bef957afa575`, Terraform `1.13.5`, with 61 resources. The raw state file SHA-256 was `9916736d7a1a105f85e73e0180f34925c631d4eb678e36abd4f0b94b1da010b4`; normalized state-core SHA-256 (canonical JSON, sorting `check_results` and resource arrays) was `7de49fd0a07ba2eda97370f30d1bee2264aaf8ee6133c5352862447f3ee95956`. The normalized state-core digest matched the pre-plan pull; serial and lineage were unchanged.

The staged source set contains 28 files. Its path-NUL-content-NUL manifest SHA-256 is `10a1320cdfd23eae62c3f452853045723d3446910ea21771aeb3d0168f9667ad`. Changed staged-file SHA-256 values are:

- `infra/gcp/pilot/main.tf`: `89690894fc4d2e1d8bacfbb0725a911098c780fb57582087dca122e1f4409d32`
- `infra/gcp/pilot/variables.tf`: `d22668cf46419c4c818ff4baf4fd7b3a1594bb3a442998145061d152001a304a`
- `infra/gcp/modules/pilot/variables.tf`: `4384c3e4fb0229c4344296a79a37e2ede625c2a0e3b1ad38925096aa39d052b3`
- `infra/gcp/modules/pilot/compute.tf`: `40c2788d2b2c4ca5797225ba85eba8cdd516e6e961f170896d65614b7cc1dc3b`
- `infra/gcp/modules/pilot/identity.tf`: `f85476979d47439c1279d0f9800d66251e5da996fa5e2e766e152a9f6583a6a9`

The final private plan artifacts are `/private/tmp/dev235-terraform/final-full.tfplan` (SHA-256 `eefa10bcfb5d7b3eae1b28cedefd49b6f1b49b3b7586d5f21d88a22da757388b`), `/private/tmp/dev235-terraform/final-full-plan.json` (SHA-256 `3cf5c45d79bc62866a7a3a2444c86f42165697846fbc21e72c6da69277486604`), and `/private/tmp/dev235-terraform/final-full-plan.log` (SHA-256 `65766cf0ad626552a2f41fc7adb49121f3bb26c19e895dacf331f6605676460e`). `terraform validate` passed on the staged copy.

## Recreating after DEV237

The protected wrapper at `/private/tmp/dev235-terraform/tf-current.py` obtains the gcloud access token only in process memory and writes Terraform output to mode-600 files. After DEV237 releases the live slot, first run a full refresh-enabled observation plan, then regenerate the snapshot from its JSON with `refresh-preprovision-snapshot.py`, and run a second full plan. The helper writes only the four snapshot fields above and prints hashes, never metadata contents. Example commands:

```text
python3 /private/tmp/dev235-terraform/tf-current.py /private/tmp/dev235-terraform/replay-init.log init -reconfigure -input=false -backend-config=backend.hcl.example
python3 /private/tmp/dev235-terraform/tf-current.py /private/tmp/dev235-terraform/refresh-observation.log plan -input=false -lock-timeout=60s -detailed-exitcode -var-file=/private/tmp/dev235-terraform/pilot-review.tfvars.json -var-file=/private/tmp/dev235-terraform/preprovision-snapshot.tfvars.json -out=/private/tmp/dev235-terraform/refresh-observation.tfplan
python3 /private/tmp/dev235-terraform/tf-current.py /private/tmp/dev235-terraform/refresh-observation.json show -json /private/tmp/dev235-terraform/refresh-observation.tfplan
python3 /private/tmp/dev235-terraform/refresh-preprovision-snapshot.py /private/tmp/dev235-terraform/refresh-observation.json /private/tmp/dev235-terraform/preprovision-snapshot.tfvars.json
python3 /private/tmp/dev235-terraform/tf-current.py /private/tmp/dev235-terraform/reviewed-full-plan.log plan -input=false -lock-timeout=60s -detailed-exitcode -var-file=/private/tmp/dev235-terraform/pilot-review.tfvars.json -var-file=/private/tmp/dev235-terraform/preprovision-snapshot.tfvars.json -out=/private/tmp/dev235-terraform/reviewed-full.tfplan
python3 /private/tmp/dev235-terraform/tf-current.py /private/tmp/dev235-terraform/reviewed-full-plan.json show -json /private/tmp/dev235-terraform/reviewed-full.tfplan
```

Recheck the full action set, snapshot equality, drift, serial, and hashes before any later review. If any VM metadata or release-reader values changed after DEV237, use the fresh snapshot and stop if the full plan proposes anything beyond the eight approved creates. No apply, credential upload, browser authentication retry, app activation, or dispatch occurred in this preparation.
