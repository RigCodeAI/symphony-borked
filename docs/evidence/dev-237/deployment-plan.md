# DEV-237 prepared deployment plan

Prepared locally on 2026-10-10. Deployment, protected workflow installation,
service activation and the live retained-task test have not been performed.
The source package is a committed source archive with `RELEASE.json`, not a
precompiled production image. The existing deployment builds it as the service
user on the intended worker/coordinator before activation.

## Fixed inputs

- Project: `factory-511117`; numeric project: `292978199748`.
- Zone: `us-central1-a`; existing coordinator: `rig-factory-coordinator`;
  existing worker: `rig-factory-worker-01`.
- Viewer: `https://factory.rig.ai`; global backend:
  `rig-factory-coordinator-https`, ID `7237567286073591668`.
- Exact signed JWT audience:
  `/projects/292978199748/global/backendServices/7237567286073591668`.
- Read-only checks confirmed that backend has IAP enabled and its resource policy
  grants only the existing `domain:rig.ai` binding for
  `roles/iap.httpsResourceAccessor`. No binding was added. The separate Linear
  webhook backend remains outside this viewer route.
- Private root: `/srv/factory/evidence`, owned by `factory-coordinator`, mode 0700,
  outside every task workspace. Existing archive:
  `factory-511117-rig-factory-archive`; release bucket:
  `factory-511117-rig-factory-releases`. Existing budget and credentials stay pinned.

The numeric audience and policy above were refreshed with `gcloud projects
 describe`, `gcloud compute backend-services list`, and `gcloud iap web
 get-iam-policy`. No JWT, cookie or credential value was printed or retained.

## Operator sequence requiring deployment approval

1. Confirm the deployed source revision, current protected workflow, idle/active
   worker operations and the current retained-state schema through the existing
   operator route. DEV-235 is unmerged and must not be imported as a prerequisite.
   If its separate deployment is selected later, resolve the documented shared
   reassignment seam before building a combined release.
2. Stop coordinator dispatch deliberately, reconcile worker liveness, then take
   an online pre-migration backup of the durable run database and private index.
   Retain immutable bundles separately. Record active release/config pins without
   exposing credential values. Schema v3 is forward-only: a v2 release cannot
   reopen it. Rollback must use compatible source, or restore the pre-activation
   backup while stopped after reconciling later operations. A restored older
   backup loses later state; activation's link rollback alone is insufficient.
3. Verify the prepared package SHA, upload it to the existing immutable release
   prefix, extract it into the protected per-revision release directory, and
   build/check it using the existing service-user deployment path. Install a new
   protected `/etc/factory/workflows/<revision>.md` derived from the current
   reviewed workflow, adding only the `evidence` block below. Preserve current
   Linear routes, app/secret references and worker pins. Do not overwrite the old
   workflow or reload model authentication from its seed.
4. Create the private root safely if absent. Apply the new release through the
   existing root activation path and check localhost health. Enabling the store
   requires restart. Confirm retained baseline snapshots and outbox replay before
   dispatch. Verify Google key retrieval and an allowed signed-in request through
   IAP. Do not log assertion headers or authorize using unsigned email headers.
5. Run one explicitly disposable, retained native task containing an executable
   check failure, retry, a later stage and a human wait. Use an already authorized
   app/session; do not create another app. Confirm the same task URL while active
   and after completion, separate attempts, original pins and available trusted
   validation logs. Child links may use labeled contract data until composition
   exists. A new live task/agent turn remains part of the requested live approval.
6. Confirm unsigned/unauthorized access is denied. Submit only a fixture secret
   to prove pre-persistence redaction. Exercise a controlled archive failure and
   retry; failure must be visible and cannot permit cleanup. Run root maintenance
   from this verified release, read back every uploaded object, and inspect the
   exact current root-owned receipt. No synthetic receipt qualifies the pilot.
7. The actual cleanup owner is a later increment. Before any manual disposable
   workspace deletion, independently confirm GitHub merge authorization and
   trusted worker termination, then require the evidence receipt predicate.
   Reopen the same task URL and original artifact revision after deletion.
   If these prerequisites are unavailable, record that acceptance step as pending.

```yaml
evidence:
  root: /srv/factory/evidence
  base_url: https://factory.rig.ai
  iap_audience: /projects/292978199748/global/backendServices/7237567286073591668
  allowed_domains: [rig.ai]
  allowed_emails: []
```

## Approval boundary

Automatic review previously rejected DEV-235's remote source-staging action.
This local artifact does not bypass that decision. The coordinating chat will
request approval for the concrete package, protected per-revision configuration,
activation and disposable live test. No remote source upload, activation, IAM,
routing, new app or credential write is performed by this increment.

Package revision, digest and local path are recorded in the private packaging
handoff after committing the reviewed source. Live acceptance remains incomplete;
no PR is opened until the required live boundaries are satisfied.
