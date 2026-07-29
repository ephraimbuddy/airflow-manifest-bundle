# Design Document: airflow-manifest-bundle

## 1. Scope

This document gives the design of the airflow-manifest-bundle package. The package
supplies manifest Dag bundles for Apache Airflow. Each bundle reads immutable Dag
snapshots from a shared filesystem. A source adapter can read a local folder or an
S3 folder. This document tells you what the parts are, how they operate, and why the
design is safe.

## 2. Problem

Airflow includes the `LocalDagBundle` and `S3DagBundle` classes. These classes read
Dag files from mutable locations. The files can change at all times. These locations
have no recoverable version.

When a task runs again, Airflow cannot find the files that the first run used. A
deployment on a shared filesystem cannot run a task again with the same files. Git is
one solution, but not all deployments can use Git.

## 3. Solution summary

The package divides "a change to a file" from "a deployment". A source adapter
prepares one local source tree. A common publisher writes an immutable snapshot of
the Dag files. The bundle can run the publisher as part of each refresh. An operator
can also run a local or S3 publisher command. Airflow reads only published
snapshots. A change to a source file has no effect until a publication.

The version of each snapshot is a hash of its content. Airflow keeps this version
with each Dag run. When a task runs again, the bundle finds the same snapshot from
the version.

## 4. Terms

This document uses each term below with one meaning only.

| Term | Meaning |
| --- | --- |
| Source | The mutable location that contains the Dag files. |
| Prepared source | One local tree and one source observation from a source adapter. |
| Source tree | The mutable local folder that contains the Dag files. |
| S3 folder | The mutable set of S3 objects below one bucket and prefix. |
| S3 mirror | A disposable local copy of the current S3 folder. Airflow does not parse it. |
| Source observation | The identity of one source state. |
| Deployment marker | An optional S3 object below the source prefix. A deployment tool writes a new value to it last. |
| Published root | The location that holds all publications: a shared folder, or an S3 location (`s3://bucket/prefix`). Its value is the `published_root` option. |
| Artifact store | The code that reads and writes the published root. One implementation uses the filesystem. One implementation uses S3. |
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

The package has eight modules:

- `manifest.py` — makes and examines manifests. It computes hashes and versions.
- `store.py` — defines the artifact-store contract. All access to the published
  root goes through this contract.
- `bundle.py` — contains `ManifestDagBundleBase`, the common artifact lifecycle,
  and the filesystem artifact store.
- `local.py` — contains the local source adapter, `ManifestLocalDagBundle`.
- `s3.py` — contains the S3 source adapter, `ManifestS3DagBundle`.
- `s3_store.py` — contains the S3 artifact store for an `s3://` published root.
- `cli.py` — contains the explicit publisher commands.
- `_compat.py` — small helpers that keep the package compatible with more than one
  Airflow release.

Both source adapters inherit directly from `ManifestDagBundleBase`. Neither source
adapter inherits from the other. The common class does not import the Amazon
provider.

The source adapter and the artifact store are independent selections. The source
adapter supplies the Dag files. The artifact store keeps the published artifacts.
The `published_root` value selects the artifact store: a filesystem path selects
the filesystem store, and an `s3://` URL selects the S3 store. Each source adapter
can publish to each artifact store.

Airflow finds a bundle through its configuration. The classpath is
`airflow_manifest_bundle.local.ManifestLocalDagBundle` or
`airflow_manifest_bundle.s3.ManifestS3DagBundle`. No plugin is necessary.

## 6. Storage layout

The publisher writes this structure in the published root:

```text
versions/<bundle>/sha256-<hex>/           one snapshot for each version
    <dag files>
    .airflow-bundle-manifest.json         the manifest of this snapshot
refs/<bundle>/latest.json                 the release reference
_locks/<bundle>.lock                      the publication lock (filesystem roots only)
_state/<bundle>/auto-publish.json         the candidate state for automatic publication
```

An S3 published root holds the same structure below its prefix, without `_locks/`.
Conditional writes protect the release reference and the candidate state there. The
publisher writes the manifest object last. The manifest object commits the
snapshot. A version prefix without its manifest object is not a release.

Airflow writes cache copies to `<dag_bundle_storage_path>/<bundle>/versions/<version>/`.
The S3 adapter writes its mirror to
`<dag_bundle_storage_path>/<bundle>/_s3_source/`. It writes mirror state to
`<dag_bundle_storage_path>/<bundle>/_s3_source_state.json`.

Keep the source, the published root, the mirror, and the cache copies in different
locations. The bundle refuses a local configuration in which these locations touch.
The S3 mirror is a safe child of the Airflow bundle base folder.

## 7. Version identity

The manifest records four values for each file: the relative path, the SHA-256 hash,
the size, and the executable flag. The version is the SHA-256 hash of these entries
in a canonical JSON form. Thus the version changes if, and only if, the content
changes.

The version string starts with `sha256-`. All characters in the string are safe in a
file name. Airflow makes cache paths and lock paths from the raw version string, so a
safe string is necessary.

Each source adapter ignores these items: `.git`, `__pycache__`, files with the
`.pyc` extension, and the manifest file itself.

## 8. The publication procedure

A source adapter returns a prepared source. Automatic publication first completes
the source stability check. The explicit publisher commands do not do this check.
The common publisher makes the manifest from the prepared local tree. For a
filesystem published root, the publisher then gets the publication lock, and other
publishers wait for it. For an S3 published root, there is no lock; section 8.2
gives the differences. The publisher reads the release reference again. It then
does these steps:

1. If the snapshot for this version exists, it validates the snapshot. If the
   snapshot does not exist, it copies each file into a temporary folder, checks each
   copy against the manifest, and then moves the folder into position with one
   atomic rename.
2. It asks the source adapter to confirm the prepared source. The local adapter reads
   local metadata. The S3 adapter reads the remote observation and local mirror
   metadata. If the source changed, the publisher stops with an error.
3. It writes the release reference to a temporary file, then replaces `latest.json`
   with one atomic rename.

These properties follow from the procedure:

- **Idempotent.** A second publication of the same content makes no new snapshot. It
  only confirms the reference.
- **Atomic.** The reference changes last. If the publisher stops at an earlier step,
  the previous release stays active.
- **Serialized.** The lock permits one publisher at a time for each bundle.

The publisher does not use the Airflow metadata database.

### 8.1 Explicit publication

A local bundle can omit `source_path`. An operator can then run this command:

```text
airflow-manifest-bundle publish-local <bundle-name> <source-path>
```

An S3 bundle can set `auto_publish` to `false`. An operator can then run this command:

```text
airflow-manifest-bundle publish-s3 <bundle-name>
```

Each command reads the Airflow bundle configuration. The local command publishes
the specified source tree. The S3 command reads the configured bucket and prefix.
It writes a disposable mirror in the Airflow bundle cache. It holds the Airflow
bundle lock until the final source confirmation is complete.

The explicit command is the deployment boundary. It does not use candidate state or
the source stability period. The deployment tool must complete the source delivery
before it runs the command. An S3 deployment marker gives a stronger boundary.

The `--expected-current-version` option stops an old deployment from replacing a
newer release. Each command can write its result as text or JSON. Each command
rejects a bundle that has automatic publication enabled.

An explicit S3 publisher needs read access to the Dag source. It does not need
write access to the Dag source. For an S3 published root, it needs write access to
the releases prefix; section 12 gives the permissions.

### 8.2 Publication to an S3 published root

An S3 published root has no lock. The procedure keeps the same steps, with these
differences:

1. The store uploads each file to its final key below the version prefix. It hashes
   each file before the upload and compares the result with the manifest. It writes
   the manifest object last. The manifest object commits the snapshot. When the
   source and the published root use the same S3 endpoint, the store copies each
   object on the server side, with a condition on the observed object state. If a
   copy fails, the store uploads the prepared local file instead.
2. A write to the release reference or to the candidate state has a condition: the
   document must be unchanged since the last read by this store. When another
   publisher changed the document first, the store reports a conflict. A concurrent
   write of the same bytes is not a conflict; the result is already correct.
3. On a release-reference conflict, the publisher that lost the race follows the
   release of the winner. It does not overwrite that release. On a candidate-state
   conflict, the publisher adopts the winner only when the winner recorded the same
   source observation. The stability period never uses the timestamp of a different
   observation.

The same properties hold: idempotent, atomic, and safe with concurrent publishers.
Publication requires conditional writes (`If-Match` and `If-None-Match`) from the
object store. AWS S3 supports them. A store without them stops the first
publication with a clear error. Consumption does not need conditional writes.

## 9. Automatic publication

A local `source_path` enables automatic local publication. The default S3
configuration enables automatic S3 publication. An S3 bundle can set `auto_publish`
to `false` to disable it. An unpinned automatic refresh does these steps:

1. It reads the current release reference.
2. It asks the source adapter for a prepared source and a source signature.
3. It gets the publication lock and reads the candidate state.
4. If the candidate state has a different signature, it asks the source adapter to
   confirm the prepared source. It writes this current signature, source identity,
   source type, and current UTC time. It stops this publication attempt if the source
   differs from the prepared source. It then releases the lock and waits.
5. If the candidate state has the same signature, it waits until
   `source_stability_seconds` has elapsed. Its default value is `refresh_interval`.
   Any replica can complete this wait.
6. It makes the manifest from the prepared local tree after the wait.
7. It gets the publication lock and reads the candidate state and release reference
   again. It stops this publication attempt if the candidate signature changed.
8. If the reference has the same version, it records a confirmation in process
   memory. If the version is different, it runs the publication procedure.
9. It materializes the current release in the cache.

For a local source, the source signature contains each relative path, size,
modification time, change time, and mode. A refresh reads file metadata in the
steady state. It does not read file content after it confirms a signature. A new
process uses the shared stability observation and hashes the stable source one time.

For an S3 source, the source signature contains a canonical object inventory. Each
entry contains the relative path, object key, size, last-modified value, and ETag.
The ETag is a remote change token. It is not an artifact hash. The publisher always
uses its own SHA-256 value from the downloaded bytes for artifact identity.

The S3 adapter validates all object paths before a download. It rejects an unsafe
path, a duplicate path, and a file and directory collision. It downloads only the
validated inventory. It uses the Airflow bundle lock to protect one host's mirror.
It compares the remote inventory before and after a mirror change. It writes mirror
state only after this comparison and a structural check.

The adapter rejects a source that has more than 10,000 files. It rejects a file that
is larger than 100 MiB. It rejects a source that is larger than 1 GiB. The
`max_file_count`, `max_file_size_bytes`, and `max_total_size_bytes` options can
change these limits.

A new process checks the SHA-256 value of each reused mirror file. A mismatch makes
the adapter download the file again. After the process confirms the source and its
snapshot, an unchanged refresh checks metadata only.

The S3 source identity contains the endpoint identity, bucket, and normalized prefix.
It does not contain `aws_conn_id` or credentials. The shared candidate state rejects
a different source identity for the same bundle.

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

The automatic publisher and each explicit publisher reject an empty source tree by
default. The `allow_empty_source` option permits an empty publication. The automatic
publisher does not operate for a pinned bundle.

If automatic publication fails and a current release exists, the bundle logs the
error and uses the current release. If no release exists, `initialize()` waits for
the rest of the stability period. It makes one more publication attempt after this
wait. If the source changes during the wait, this attempt returns a recoverable
bundle error. Airflow can try again.

All automatic publishers for one bundle must read the same source. This source is
authoritative. An operator must not use a different source or a manual reference
change for the same bundle. The automatic publisher can replace such a reference
with the version of its source.

A metadata stability period is a safeguard, not a transaction. A deployment tool
can still leave an incomplete source unchanged for that period. For a local source,
the deployment tool can prepare a separate source tree and replace the active source
tree with one atomic rename.

For an S3 source, `deployment_marker_key` gives a stronger release boundary. The
deployment tool writes this object after all Dag objects are ready. The adapter reads
the marker before and after the object inventory. It excludes the marker from the
manifest. A changed Dag inventory cannot replace a current release until the marker
also changes. When the configuration sets this option, the object is required. The
deployment marker is not a Marker.

All automatic-publisher hosts must have synchronized clocks. If a host reads a
candidate timestamp that is in the future, it waits and writes a warning.

## 10. Runtime operation

### 10.1 Refresh

The Dag processor calls `refresh()` on an interval. The bundle reads the release
reference. If the bundle has an automatic publication source, it first runs the
automatic publication procedure. The automatic S3 adapter prepares its mirror under
the Airflow bundle lock. An explicit S3 bundle does not read S3 during refresh. If a
validated cache copy of the current version exists, the bundle uses it without a
lock. If the cache copy does not exist, the bundle makes one under the bundle lock.

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

A pinned S3 bundle does not make an S3 hook. It does not check a bucket, list a
prefix, read a mirror, read candidate state, or read the release reference. It needs
only the pinned version and the published root.

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
- The S3 adapter changes AWS credentials, access, endpoint, timeout, service, and
  download errors into `BundleManifestError`.
- The S3 adapter uses `BundleManifestNotFoundError` when a bucket or required
  deployment marker does not exist. It uses `BundleManifestSourceChangedError` when
  two source observations differ.

If these errors occur in automatic publication, the bundle keeps the current release.
If no current release exists, the error stays visible to Airflow.

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

An automatic publisher must have read access to its source. It must have write access
to the bundle folders in the published root. All automatic publishers for one bundle
must use the same OS user. Alternatively, an administrator must give write permission
to each publisher. This permission must apply to all child folders that an earlier
publisher created in the published root. A pinned bundle needs only read access to
the published root.

An explicit publisher must have write access to its bundle folders in the published
root. An S3 publisher also needs write access to its local mirror. It needs
`s3:ListBucket` and `s3:GetObject` for the source prefix. It does not need write
access to the Dag source. With a filesystem published root, an explicit-mode Dag
processor and a pinned S3 bundle need no S3 permission.

For an S3 published root, OS users and file modes do not apply. The permissions are:

- A publisher needs `s3:PutObject` and `s3:GetObject` on the releases prefix. The
  Dag source prefix stays read-only.
- A pinned bundle and a consume-only Dag processor need `s3:GetObject` on the
  releases prefix, and no other S3 permission.
- Read access to the source prefix by the store's principal permits server-side
  copies. Without it, the store uploads each file. Both paths give a correct
  snapshot.
- The optional `published_root_conn_id` option selects the AWS connection of the
  artifact store. Workers give the best results with credentials from the default
  AWS chain, because bundle initialization runs before task context.

## 13. Compatibility

The package operates on Apache Airflow 3.1 and later. The package examines the
installed Airflow at import time:

- On Airflow 3.3 and later, `get_current_version()` returns a `BundleVersion` object.
- On Airflow 3.1 and 3.2, it returns the version as a string, because those releases
  know only strings.

The S3 adapter requires `apache-airflow-providers-amazon` 9.10.0 or later. The
`s3` optional dependency supplies this provider. The base package and local adapter
can operate without it. An S3 class import also stays safe without the provider. An
unpinned S3 source operation gives an error that names the optional dependency.

The files on disk define compatibility between bundle processes. The release
reference, manifest, and candidate state contain a `schema_version` field. Bundle
processes do not exchange Python objects. Thus, the processes can use different patch
versions. An incompatible change to a file format must use a new schema version.

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
- An automatic S3 refresh reads the remote object inventory. An explicit S3 command
  does the same. A mirror change needs a second inventory and object downloads.
- The stability period cannot prove that a source delivery is complete. Atomic source
  replacement gives stronger local protection. An S3 deployment marker gives a
  stronger S3 boundary.
- The S3 mirror is not a historical store. The published root must retain every
  version that Airflow can request.
- Clock differences between automatic-publisher hosts can delay publication. A host
  does not publish when the shared candidate timestamp is in its future.
- Publication to an S3 published root requires conditional writes. Some
  S3-compatible stores do not have them. Consumption operates without them.
- A publication to an S3 published root can stop before the manifest write. This
  leaves an uncommitted version prefix. Consumers do not see it. A later
  publication of the same content completes it. An age-based lifecycle rule can
  remove abandoned prefixes.

## 15. Backend extension

The package is one family: manifest bundles. Each source adapter is one module.

- A new source adapter inherits from `ManifestDagBundleBase`. It prepares one local
  tree and confirms its source observation.
- A new adapter adds one module (for example, `gcs.py` with
  `ManifestGCSDagBundle`) and one optional-dependency group in `pyproject.toml`.
- All adapters share `bundle.py` and `manifest.py`. The version calculation and
  artifact lifecycle stay the same for all adapters.
- The release reference records the artifact backend in its `backend.type` field.
  Both current source adapters publish local filesystem artifacts. Optional source
  metadata records the source adapter type, source identity, source observation, and
  deployment marker observation.

The artifact store is a second extension point. `store.py` defines the contract.
The filesystem store and the S3 store implement it. A new artifact store implements
the same operations with the atomicity primitives of its backend. Two rules are
mandatory. First, writes to the release reference and to the candidate state need
protection from concurrent publishers: a lock, or conditional writes. Second, a
snapshot must not become visible before it is complete. Consumers hash each fetched
file against the manifest, so a store defect stops materialization; it cannot
produce a wrong cache copy.

The local-only installation stays small: an optional-dependency group pulls a cloud
SDK only when the operator asks for that backend.
