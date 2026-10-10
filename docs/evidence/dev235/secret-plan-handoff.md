# DEV-235 Secret Manager Terraform handoff

This is a sanitized, read-only planning receipt. Terraform init/state reads/plans only were run. This planning task performed no apply, secret-version upload, application creation, or cloud resource mutation. Separate approved OAuth record creation is recorded in deployment-boundary.json. The raw plan files and staged inputs remain private in this directory; this report contains no secret contents.

## Existing deployment and state

- Project: `factory-511117`; region/zone: `us-central1` / `us-central1-a`.
- Existing Terraform backend: GCS bucket `factory-511117-symphony-tfstate`, prefix `dev-230/pilot` (`infra/gcp/pilot/backend.hcl.example`).
- State snapshot used for this plan: serial `51`; Terraform `1.13.5`.
- Existing inputs: `/private/tmp/dev233-ingress-plan/dashboard.tfvars.json` and `/private/tmp/dev230-private/pilot.tfvars.json`. The supplied pilot tfvars are older than live state; a private review overlay was built from those inputs plus verified current coordinator metadata/state. Its intended configuration preserves GCP metadata revision `064f892f95847ea2eed0eb684da36c6e1bb5fce6`, existing Linear default secret mappings/pins, and active ingress settings. The full plan still proposes VM metadata/startup updates, so the full plan cannot certify preservation of live metadata.
- Exact existing coordinator service account (state output and current instance state): `rig-factory-coordinator@factory-511117.iam.gserviceaccount.com`.
- Active gcloud caller observed: `adam@rig.ai`. This identifies the planning caller; bucket IAM ownership was not independently established.
- Manual coordinator disk candidate is `3667340`; GCP boot metadata remains `064f892f...`. Worker control stays `064f892f...`; worker app remains `541279`. The overlay preserved these release selections as inputs; the broad VM updates remain a blocker and are not approved.

## Requested-scope preview

The targeted preview has exactly eight creates and no drift: four Secret Manager containers in `factory-511117` with automatic replication, and four secret-scoped `roles/secretmanager.secretAccessor` grants to the coordinator service account above. The four container IDs are:

- `linear-high-effort-client-secret`
- `linear-high-effort-webhook-signing`
- `linear-daybreak-client-secret`
- `linear-daybreak-webhook-signing`

The plan contains no secret payloads or secret versions. The intended first uploaded version remains numeric version `1`; values were not fetched, printed, or staged here. The targeted plan reports `applyable=true`, `complete=false` because it was produced with `-target`; it is an inspection preview only and is not an apply artifact.

## Full-plan blocker

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
