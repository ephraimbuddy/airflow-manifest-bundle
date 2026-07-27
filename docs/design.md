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

The package divides "a change to a file" from "a deployment". A publisher writes an
immutable snapshot of the Dag files. The bundle can run the publisher during a
refresh. A command can also run the publisher. Airflow reads only published
snapshots. A change to a source file has no effect until a publication.

The version of each snapshot is a hash of its content. Airflow keeps this version
with each Dag run. When a task runs again, the bundle finds the same snapshot from
the version.

## 4. Terms

This document uses each term below with one meaning only.

| Term | Meaning |
| --- | --- |
| Source tree | The mutable folder that contains the Dag files. A publisher reads it. |
| Published root | The shared folder that holds all publications. Its path is the `published_root` option. |
| Snapshot | One immutable, read-only copy of the source tree in the published root. |
| Manifest | A JSON file that lists each file of a snapshot with its hash, size, and executable flag. |
| Release reference | The file `refs/<bundle>/latest.json`. It points to the current snapshot. |
| Version | The identity of a snapshot: `sha256-` plus the SHA-256 hash of the manifest entries. |
| Cache | Airflow's local bundle folder (`dag_bundle_storage_path`). Airflow can delete it at all times. |
| Cache copy | A validated copy of a snapshot in the cache. Airflow parses and runs Dags from it. |
| Publisher | The code that makes a snapshot and updates the release reference. |
| Marker | A file in the cache that records a passed validation of a cache copy on that host. |
| Candidate state | A shared JSON file that records the first observation of one source signature. |

## 5. Parts of the package

The package has four modules:

- `manifest.py` — makes and examines manifests. It computes hashes and versions.
- `local.py` — contains `ManifestLocalDagBundle` and the automatic and explicit
  publication code for the local backend.
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
_state/<bundle>/auto-publish.json         the candidate state for automatic publication
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

The command gets the publication lock first. Other publishers wait. It compares
`--expected-current-version` with the release reference when the operator gives this
option. If the values are different, it stops with an error. It then reads the
source tree and makes the manifest.

The automatic publisher makes the manifest after its source stability check. It then
gets the publication lock and reads the release reference again.

Both publisher modes then do these steps:

1. If the snapshot for this version exists, it validates the snapshot. If the
   snapshot does not exist, it copies each file into a temporary folder, checks each
   copy against the manifest, and then moves the folder into position with one
   atomic rename.
2. It reads the source metadata again. If the source changed during the operation,
   it stops with an error.
3. It writes the release reference to a temporary file, then replaces `latest.json`
   with one atomic rename.

These properties follow from the procedure:

- **Idempotent.** A second publication of the same content makes no new snapshot. It
  only confirms the reference.
- **Atomic.** The reference changes last. If the publisher stops at an earlier step,
  the previous release stays active.
- **Serialized.** The lock permits one publisher at a time for each bundle.
- **Ordered.** In explicit mode, the `--expected-current-version` option prevents an
  old, slow deployment from a move of the reference backwards.

The publisher does not use the Airflow metadata database.

## 9. Automatic publication

The `source_path` option enables automatic publication. An unpinned refresh does
these steps:

1. It reads the current release reference.
2. It reads the source metadata and makes a source signature.
3. It gets the publication lock and reads the candidate state.
4. If the candidate state has a different signature, it reads the source metadata
   again. It writes this current signature and the current UTC time. It stops this
   publication attempt if the current signature differs from the first signature.
   It then releases the lock and waits.
5. If the candidate state has the same signature, it waits until
   `source_stability_seconds` has elapsed. Its default value is `refresh_interval`.
   Any replica can complete this wait.
6. It makes the manifest after the wait.
7. It gets the publication lock and reads the candidate state and release reference
   again. It stops this publication attempt if the candidate signature changed.
8. If the reference has the same version, it records a confirmation in process
   memory. If the version is different, it runs the publication procedure.
9. It materializes the current release in the cache.

The source signature contains each relative path, size, modification time, change
time, and mode. A refresh reads file metadata in the steady state. It does not read
file content after it confirms a signature. A new process uses the shared stability
observation and hashes the stable source one time.

The candidate state is an atomic, schema-versioned coordination hint in the
published root. It does not select a release. The release reference is the only file
that selects a release. A malformed candidate state restarts the stability period.
An unsupported candidate-state schema stops automatic publication.

A delayed replica cannot replace candidate state with an old source observation.
A revert to a confirmed source also replaces a different candidate. Thus, a candidate
timestamp cannot stay valid across a source revert.

The source confirmation stays in process memory. The bundle does not write candidate
state or source confirmation to the cache. The cache is disposable and cannot hold
publisher correctness state.

The automatic publisher rejects an empty source tree by default. The
`allow_empty_source` option permits an empty publication. The automatic publisher
does not operate for a pinned bundle.

If automatic publication fails and a current release exists, the bundle logs the
error and uses the current release. If no release exists, initialization returns a
recoverable bundle error until publication succeeds.

All automatic publishers for one bundle must read the same source tree. This source
tree is authoritative. An operator must not use a different source or a manual
reference change for the same bundle. The automatic publisher can replace such a
reference with the version of its source.

A metadata stability period is a safeguard, not a transaction. A deployment tool
can still leave an incomplete source tree unchanged for that period. For the best
protection, the deployment tool must prepare a separate source tree and replace the
active source tree with one atomic rename.

All automatic-publisher hosts must have synchronized clocks. If a host reads a
candidate timestamp that is in the future, it waits and writes a warning.

## 10. Runtime operation

### 10.1 Refresh

The Dag processor calls `refresh()` on an interval. The bundle reads the release
reference. If `source_path` is set, it first runs the automatic publication procedure.
If a validated cache copy of the current version exists, the bundle uses it without
a lock. If the cache copy does not exist, the bundle makes one under the bundle lock.

### 10.2 Creation of a cache copy

To make a cache copy, the bundle does these steps:

1. It removes unused temporary folders and unused markers from the cache.
2. It does a structural check of the published snapshot.
3. It copies the snapshot into a temporary folder in the cache.
4. It validates the copy against the manifest, with full hashes.
5. It sets the permissions of the copy (see section 12).
6. It moves the folder into position with one atomic rename.
7. It writes the marker for this version.

### 10.3 Pinned runs

Airflow keeps the bundle version with each Dag run. When a task runs again, Airflow
gives that version to the bundle. The bundle then uses the cache copy for that exact
version. If Airflow deleted the cache copy, the bundle makes it again from the
published root. The snapshot proves its own identity: its manifest must hash to the
pinned version.

### 10.4 Validation and markers

A full validation hashes each file. This is too costly for each task start. After
one passed full validation on a host, the bundle writes a marker. Later processes on
that host do only a structural check: the file set, the file types, and the absence
of symbolic links. The structural check finds a cut tree or an added file. To force
a full validation again, delete the marker.

If a validation fails, the bundle moves the bad cache copy aside, removes its marker
and its Airflow tracking file, and makes a new copy from the published root.

### 10.5 The path fallback

Airflow reads the `path` property of a bundle before initialization in two cases:
priority parse requests and callbacks without a version. Directly after a
publication, the new version has no cache copy. In that case, `path` points to the
newest validated cache copy. This keeps a callback alive; Airflow deletes callback
records before the parse, so a missed callback cannot come back.

## 11. Error contract

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
During automatic publication, the bundle keeps the current release when these errors
occur. If no current release exists, the error stays visible to Airflow.

## 12. Permissions and users

The publisher and the Airflow components can run as different OS users.

- Snapshots in the published root are read-only and readable by all users:
  directories `0555`, files `0444`, executable files `0555`.
- The release reference is `0644`. The publication lock is `0644`.
- The candidate-state file is `0644`. Its directories are `0755`.
- Cache copies keep files read-only, but directories are `0755`. Airflow's stale-cache
  removal uses a plain `shutil.rmtree`, and that call fails on read-only directories.
  The writable directories make the removal possible. The structural check behind the
  marker finds a file that other code adds through a writable directory.

The publisher sets permissions only on directories that it makes. A published root
that an administrator made before keeps its permissions.

An automatic publisher must have read access to the source tree and write access to
the published root. A pinned bundle needs only read access to the published root.

## 13. Compatibility

The package operates on Apache Airflow 3.1 and later. The package examines the
installed Airflow at import time:

- On Airflow 3.3 and later, `get_current_version()` returns a `BundleVersion` object.
- On Airflow 3.1 and 3.2, it returns the version as a string, because those releases
  know only strings.

The files on disk are the contract between the publisher and the runtime. The
release reference, manifest, and candidate state contain a `schema_version` field.
The two sides do not exchange Python objects, so their patch versions can be
different. A change to a file format that is not compatible must use a new schema
version.

## 14. Known limits

- Airflow's task start gets the version lock after `bundle.initialize()`. A stale-cache
  removal in that small window can delete the version. The bundle then makes the copy
  again from the published root; the task fails one time and is good on the retry.
- The structural check runs one time for each process. In one long process, a change
  to a cache copy after the check stays unseen until the next process.
- Airflow never deletes from the published root. The operator removes old snapshots.
  Keep each version that a Dag run can request again: retries, new runs of old Dag
  runs, deferred tasks, and callbacks point to versions.
- An automatic refresh reads the metadata of each source file. A new process also
  waits for the stability period and hashes the unchanged source one time.
- The stability period cannot prove that a source delivery is complete. Atomic source
  replacement gives stronger protection.
- Clock differences between automatic-publisher hosts can delay publication. A host
  does not publish when the shared candidate timestamp is in its future.

## 15. Extension plan

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
