# Design Document: airflow-manifest-bundle

## 1. Scope

This document gives the design of the airflow-manifest-bundle package. The package
supplies a Dag bundle for Apache Airflow. The bundle reads immutable Dag snapshots
from a shared filesystem. This document tells you what the parts are, how they
operate, and why the design is safe.

## 2. Problem

Airflow includes the `LocalDagBundle` class. This class reads Dag files from a
folder. The files in this folder can change at all times. The folder has no version.

When a task runs again, Airflow cannot find the files that the first run used. A
deployment on a shared filesystem cannot run a task again with the same files. Git is
one solution, but not all deployments can use Git.

## 3. Solution summary

The package divides "a change to a file" from "a deployment". A publisher command
writes an immutable snapshot of the Dag files. Airflow reads only published
snapshots. A change to a source file has no effect until the next publication.

The version of each snapshot is a hash of its content. Airflow keeps this version
with each Dag run. When a task runs again, the bundle finds the same snapshot from
the version.

## 4. Terms

This document uses each term below with one meaning only.

| Term | Meaning |
| --- | --- |
| Source tree | The folder that contains the Dag files. Only the publisher reads it. |
| Published root | The shared folder that holds all publications. Its path is the `published_root` option. |
| Snapshot | One immutable, read-only copy of the source tree in the published root. |
| Manifest | A JSON file that lists each file of a snapshot with its hash, size, and executable flag. |
| Release reference | The file `refs/<bundle>/latest.json`. It points to the current snapshot. |
| Version | The identity of a snapshot: `sha256-` plus the SHA-256 hash of the manifest entries. |
| Cache | Airflow's local bundle folder (`dag_bundle_storage_path`). Airflow can delete it at all times. |
| Cache copy | A validated copy of a snapshot in the cache. Airflow parses and runs Dags from it. |
| Publisher | The `airflow-manifest-bundle publish-local` command. |
| Marker | A file in the cache that records a passed validation of a cache copy on that host. |

## 5. Parts of the package

The package has four modules:

- `manifest.py` — makes and examines manifests. It computes hashes and versions.
- `local.py` — contains `ManifestLocalDagBundle` and the publication function for the
  local backend.
- `cli.py` — the console script. It supplies the `publish-local` subcommand.
- `_compat.py` — small helpers that keep the package compatible with more than one
  Airflow release.

Airflow finds the bundle through its configuration. The classpath is
`airflow_manifest_bundle.local.ManifestLocalDagBundle`. No plugin is necessary.

## 6. Storage layout

The publisher writes this structure in the published root:

```text
versions/<bundle>/sha256-<hex>/           one snapshot for each version
    <dag files>
    .airflow-bundle-manifest.json         the manifest of this snapshot
refs/<bundle>/latest.json                 the release reference
_locks/<bundle>.lock                      the publication lock
```

Airflow writes cache copies to `<dag_bundle_storage_path>/<bundle>/versions/<version>/`.

Keep the source tree, the published root, and the cache in different locations. The
bundle refuses a configuration in which these locations touch.

## 7. Version identity

The manifest records four values for each file: the relative path, the SHA-256 hash,
the size, and the executable flag. The version is the SHA-256 hash of these entries
in a canonical JSON form. Thus the version changes if, and only if, the content
changes.

The version string starts with `sha256-`. All characters in the string are safe in a
file name. Airflow makes cache paths and lock paths from the raw version string, so a
safe string is necessary.

The publisher ignores these items in the source tree: `.git`, `__pycache__`, files
with the `.pyc` extension, and the manifest file itself.

## 8. The publication procedure

The publisher does these steps:

1. It reads the source tree and makes the manifest.
2. It gets the publication lock. Other publishers wait.
3. If the operator gave `--expected-current-version`, it compares that value with the
   release reference. If the values are different, it stops with an error.
4. If the snapshot for this version exists, it validates the snapshot. If the
   snapshot does not exist, it copies each file into a temporary folder, checks each
   copy against the manifest, and then moves the folder into position with one
   atomic rename.
5. It reads the source metadata again. If the source changed during the operation,
   it stops with an error.
6. It writes the release reference to a temporary file, then replaces `latest.json`
   with one atomic rename.

These properties follow from the procedure:

- **Idempotent.** A second publication of the same content makes no new snapshot. It
  only confirms the reference.
- **Atomic.** The reference changes last. If the publisher stops at an earlier step,
  the previous release stays active.
- **Serialized.** The lock permits one publisher at a time for each bundle.
- **Ordered.** The `--expected-current-version` option prevents an old, slow
  deployment from a move of the reference backwards.

The publisher does not use the Airflow metadata database.

## 9. Runtime operation

### 9.1 Refresh

The Dag processor calls `refresh()` on an interval. The bundle reads the release
reference. If a validated cache copy of that version exists, the bundle uses it
without a lock. If the cache copy does not exist, the bundle makes one under the
bundle lock.

### 9.2 Creation of a cache copy

To make a cache copy, the bundle does these steps:

1. It removes unused temporary folders and unused markers from the cache.
2. It does a structural check of the published snapshot.
3. It copies the snapshot into a temporary folder in the cache.
4. It validates the copy against the manifest, with full hashes.
5. It sets the permissions of the copy (see section 11).
6. It moves the folder into position with one atomic rename.
7. It writes the marker for this version.

### 9.3 Pinned runs

Airflow keeps the bundle version with each Dag run. When a task runs again, Airflow
gives that version to the bundle. The bundle then uses the cache copy for that exact
version. If Airflow deleted the cache copy, the bundle makes it again from the
published root. The snapshot proves its own identity: its manifest must hash to the
pinned version.

### 9.4 Validation and markers

A full validation hashes each file. This is too costly for each task start. After
one passed full validation on a host, the bundle writes a marker. Later processes on
that host do only a structural check: the file set, the file types, and the absence
of symbolic links. The structural check finds a cut tree or an added file. To force
a full validation again, delete the marker.

If a validation fails, the bundle moves the bad cache copy aside, removes its marker
and its Airflow tracking file, and makes a new copy from the published root.

### 9.5 The path fallback

Airflow reads the `path` property of a bundle before initialization in two cases:
priority parse requests and callbacks without a version. Directly after a
publication, the new version has no cache copy. In that case, `path` points to the
newest validated cache copy. This keeps a callback alive; Airflow deletes callback
records before the parse, so a missed callback cannot come back.

## 10. Error contract

Airflow's Dag processor continues after an `AirflowException` from one bundle. Other
exception types can stop the processor. Thus:

- `BundleManifestError` is a subclass of `AirflowException`. All manifest and
  validation errors use it.
- `BundleManifestNotFoundError` is a subclass of `BundleManifestError` and of
  `FileNotFoundError`. The bundle uses it when a reference, a snapshot, or a manifest
  is not there. Code that catches `FileNotFoundError` continues to operate.
- The public entry points (`initialize`, `refresh`, `get_current_version`, `path`)
  change an unplanned `OSError` into a `BundleManifestError`.

The CLI catches these errors, writes one line to stderr, and stops with exit code 2.

## 11. Permissions and users

The publisher and the Airflow components can run as different OS users.

- Snapshots in the published root are read-only and readable by all users:
  directories `0555`, files `0444`, executable files `0555`.
- The release reference is `0644`. The publication lock is `0644`.
- Cache copies keep files read-only, but directories are `0755`. Airflow's stale-cache
  removal uses a plain `shutil.rmtree`, and that call fails on read-only directories.
  The writable directories make the removal possible. The structural check behind the
  marker finds a file that other code adds through a writable directory.

The publisher sets permissions only on directories that it makes. A published root
that an administrator made before keeps its permissions.

## 12. Compatibility

The package operates on Apache Airflow 3.1 and later. The package examines the
installed Airflow at import time:

- On Airflow 3.3 and later, `get_current_version()` returns a `BundleVersion` object.
- On Airflow 3.1 and 3.2, it returns the version as a string, because those releases
  know only strings.

The files on disk are the contract between the publisher and the runtime. Each file
contains a `schema_version` field. The two sides do not exchange Python objects, so
their patch versions can be different. A change to the file format that is not
compatible must use a new schema version.

## 13. Known limits

- Airflow's task start gets the version lock after `bundle.initialize()`. A stale-cache
  removal in that small window can delete the version. The bundle then makes the copy
  again from the published root; the task fails one time and is good on the retry.
- The structural check runs one time for each process. In one long process, a change
  to a cache copy after the check stays unseen until the next process.
- Airflow never deletes from the published root. The operator removes old snapshots.
  Keep each version that a Dag run can request again: retries, new runs of old Dag
  runs, deferred tasks, and callbacks point to versions.

## 14. Extension plan

The package is one family: manifest bundles. Each storage backend is one module.

- A new backend adds one module (for example, `gcs.py` with `ManifestGCSDagBundle`),
  one CLI subcommand (for example, `publish-gcs`), and one optional-dependency group
  in `pyproject.toml`.
- All backends share `manifest.py`. The version calculation stays the same for all
  backends.
- The release reference records the backend in its `backend.type` field. Schema
  checks keep the backends apart.

The local-only installation stays small: an optional-dependency group pulls a cloud
SDK only when the operator asks for that backend.
