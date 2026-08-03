# GCS operator guide

`ManifestGCSDagBundle` gives Airflow versioned Dag bundles from a mutable GCS
folder. It keeps the Dag source read-only. An automatic dag processor or an
explicit publisher mirrors the current folder into local staging, then publishes
an immutable, content-addressed snapshot to `published_root`. Airflow parses and
executes only validated cache copies of those snapshots.

## Installation

Install the GCS extra on each automatic dag-processor or explicit publisher host.
Use the same explicit Airflow version as the deployment:

```bash
python -m pip install \
  "apache-airflow==3.3.0" \
  "airflow-manifest-bundle[gcs]==0.3.0"
```

The extra installs `apache-airflow-providers-google`. The minimum supported provider
version is 18.1.0, which introduced Airflow's stock `GCSDagBundle` used as the
configuration compatibility reference.

An explicit-mode dag processor only consumes releases from `published_root`, so it
can use the base package without the GCS extra.

## Configuration

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "my_dags",
      "classpath": "airflow_manifest_bundle.gcs.ManifestGCSDagBundle",
      "kwargs": {
        "bucket_name": "airflow-dags",
        "prefix": "production/",
        "gcp_conn_id": "google_cloud_default",
        "published_root": "/mnt/shared/airflow-bundles",
        "refresh_interval": 30,
        "source_stability_seconds": 30,
        "deployment_marker_key": ".ready"
      }
    }
  ]
```

`bucket_name` and `published_root` are required. `prefix` defaults to the bucket
root. `gcp_conn_id` defaults to `google_cloud_default`, which preserves stock
Airflow behavior and Application Default Credentials fallback.

The manifest extensions are:

- `auto_publish`: publish during each unpinned refresh. The default is `true`. Set it
  to `false` when a deployment pipeline runs the explicit publisher command.
- `source_stability_seconds`: how long one remote observation must remain unchanged.
  The default is `refresh_interval`. It applies only to automatic publication.
- `allow_empty_source`: permit a release with no included files. The default is
  `false`.
- `deployment_marker_key`: a relative object name below `prefix` that the deployment
  process writes last. When configured, the object is required.
- `max_file_count`: maximum included objects. The default is 10,000.
- `max_file_size_bytes`: maximum bytes in one included object. The default is
  104,857,600 (100 MiB).
- `max_total_size_bytes`: maximum bytes in all included objects. The default is
  1,073,741,824 (1 GiB).

`published_root` must be a durable shared filesystem path. Object-store published
roots (`s3://`, `gs://`) are not supported for this adapter yet, and the constructor
rejects them. GCS is the mutable source in this adapter; the published root is the
historical store that must retain every version Airflow can request.

A configured `prefix` with no objects raises a recoverable not-found error instead
of publishing, so a mistyped prefix cannot look like an emptied Dag source. Set
`allow_empty_source` to true only when an empty release is intended.

## What the bundle stores

Each publisher host has these disposable paths below Airflow's bundle base folder:

```text
_gcs_source/                 mutable GCS mirror
_gcs_source_state.json       object generations and local mirror hashes
versions/                    validated Airflow cache copies
```

The published root is authoritative:

```text
versions/<bundle>/sha256-<hex>/           immutable snapshots
refs/<bundle>/latest.json                 current release reference
_locks/<bundle>.lock                      publication lock
_state/<bundle>/auto-publish.json         shared stability candidate
```

The mirror can be deleted at any time. The next automatic refresh or explicit
publication rebuilds it. Do not back it up, mount it as a Dag folder, or let any
Airflow component execute from it.

The mutable GCS folder cannot recover a historical version after the local cache is
deleted. Back up or retain `published_root` for as long as retries, reruns, deferred
tasks, callbacks, or cleared Dag runs can request an old `DagRun.bundle_version`.

## Read-only IAM for the Dag source

An automatic dag processor or explicit publisher needs permission to:

```text
storage.buckets.get
storage.objects.list
storage.objects.get
```

Constrain object access to the configured bucket and prefix where possible. Add the
permissions required to decrypt customer-managed encryption keys when the source
uses them.

The adapter does not create, replace, or delete source objects. A deployment system
writes the Dag objects and optional marker.

Dag processors with `auto_publish=false` need no Google connection or GCS
permission. Workers that initialize pinned versions also need no GCS access. Those
processes need read access to `published_root` and write access to their local
Airflow bundle cache.

## Deployment boundary

The stability window filters short-lived changes, but it is not a transaction. A
partly uploaded folder can remain unchanged long enough to look stable.

`deployment_marker_key` is relative to `prefix`. For this configuration:

```ini
"bucket_name": "airflow-dags",
"prefix": "production/",
"deployment_marker_key": ".ready"
```

the marker object is:

```text
gs://airflow-dags/production/.ready
```

Give each deployment a new value, such as a Git commit SHA, CI run ID, or deployment
UUID. For a strong release boundary:

1. Upload or replace all Dag objects below the configured prefix.
2. Delete objects that are not part of the new release.
3. Write or replace `deployment_marker_key` last with a new value.

For example:

```bash
set -euo pipefail

BUCKET=airflow-dags
PREFIX=production
DEPLOYMENT_ID="${GIT_COMMIT_SHA:?GIT_COMMIT_SHA must be set}"

gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  --exclude='(^|/)\.ready$' \
  ./dags "gs://${BUCKET}/${PREFIX}/"
printf '%s\n' "${DEPLOYMENT_ID}" | \
  gcloud storage cp - "gs://${BUCKET}/${PREFIX}/.ready"

airflow-manifest-bundle publish-gcs my_dags --output json
```

Ensure the source sync does not delete or replace the marker before the final marker
write. The adapter reads the marker before and after each inventory. It excludes the
marker from the mirror and manifest. If Dag objects change without a new marker, it
keeps the current release.

## Explicit publication

Set `auto_publish` to `false` in the bundle configuration:

```ini
"kwargs": {
  "auto_publish": false,
  "bucket_name": "airflow-dags",
  "prefix": "production/",
  "gcp_conn_id": "google_cloud_default",
  "published_root": "/mnt/shared/airflow-bundles",
  "deployment_marker_key": ".ready"
}
```

After the deployment tool writes the Dag objects and optional marker, run:

```bash
airflow-manifest-bundle publish-gcs my_dags --output json
```

After the first release, use the current version as an ordering guard:

```bash
airflow-manifest-bundle publish-gcs my_dags \
  --expected-current-version sha256-<hex> \
  --output json
```

The command gets the bucket, prefix, connection, limits, and marker from Airflow
configuration. It holds the host's Airflow bundle lock while it synchronizes and
publishes the mirror. It confirms the local mirror and remote observation before it
updates the release reference.

The explicit command does not wait for `source_stability_seconds` and does not write
automatic candidate state. Run it only after source delivery is complete. The
command rejects a bundle that still has automatic publication enabled.

## Synchronization and integrity

The adapter validates every listed object name before a download. It rejects
absolute or parent-relative paths, empty components, backslashes, control characters,
duplicates, and file/directory collisions. It ignores GCS directory markers and the
same files that local publication ignores: `.git`, `__pycache__`, `.pyc`, and
`.airflow-bundle-manifest.json`.

For each object, mirror state stores the object generation, metageneration, size,
updated value, ETag, and local SHA-256. The generation is a remote change token. It
is not the artifact hash. Every download names the observed generation and uses a
generation-match precondition. A replacement cannot silently supply different bytes.

A new process verifies the SHA-256 of each reused mirror file. After the process
confirms one source observation and its published snapshot, unchanged refreshes use
the metadata-only fast path.

After a mirror change, the adapter lists GCS again and requires the same inventory.
Publication then uses SHA-256 from local bytes, verifies the immutable snapshot,
confirms the local mirror and remote observation again, and replaces the release
reference last.

If automatic publication fails after a release exists, the bundle keeps the current
release active. An explicit publication exits with an error and leaves the release
reference unchanged.

## Capacity and recovery

Plan space for one current GCS mirror on each publisher host, every retained snapshot
in `published_root`, and Airflow's local cache copies. Raise the default source limits
only after including all three areas in capacity planning.

To recover a broken mirror, stop concurrent publication on that host and remove only
the bundle's `_gcs_source` folder and `_gcs_source_state.json`. The next publication
downloads a fresh validated inventory.

To recover a cache, remove the affected cache version and validation marker. Pinned
or unpinned initialization rematerializes it from `published_root`.

If a published snapshot is missing, restore that exact `sha256-<hex>` directory from
backup. Copying the current GCS prefix is insufficient unless its bytes happen to
produce the requested historical version.
