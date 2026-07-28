# airflow-manifest-bundle

Manifest-backed local and S3 Dag bundles for Apache Airflow — install the package,
point your bundle config at it, and it works with any standard Airflow 3.1+
installation.

The Airflow `LocalDagBundle` cannot identify the exact source files a task retry or rerun
used: files can change after a Dag run is created, so its bundle version resolves nothing.
`ManifestLocalDagBundle` and `ManifestS3DagBundle` give filesystem and S3 deployments
reproducible pinned execution without requiring Git. They work like `GitDagBundle`
does for commits:

- The bundle **publishes** an immutable, content-addressed snapshot automatically
  after the Dag source stays unchanged for a configured interval, or through an
  explicit local or S3 publisher command.
- The bundle version is a SHA-256 content hash (`sha256-<hex>`), and the release
  reference is updated atomically as the last publication step.
- Airflow **materializes** snapshots into its normal, disposable bundle cache before
  parsing or execution, validating path safety, integrity, and permissions.
- Task retries and reruns **pin** through the existing `DagRun.bundle_version` field and
  rematerialize from the published root if the local cache was cleaned.

## Install

Maintainers currently publish versioned wheels through
[GitHub Releases](https://github.com/ephraimbuddy/airflow-manifest-bundle/releases).
Install the wheel in the same environment as Airflow. Set `AIRFLOW_VERSION` to the
version in your deployment, and select a published bundle version:

```bash
AIRFLOW_VERSION=3.3.0
AIRFLOW_MANIFEST_BUNDLE_VERSION=0.1.0
BUNDLE_WHEEL_URL="https://github.com/ephraimbuddy/airflow-manifest-bundle/releases/download/v${AIRFLOW_MANIFEST_BUNDLE_VERSION}/airflow_manifest_bundle-${AIRFLOW_MANIFEST_BUNDLE_VERSION}-py3-none-any.whl"

python -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "airflow-manifest-bundle @ ${BUNDLE_WHEEL_URL}"
```

The explicit Airflow pin prevents package installation from changing the Airflow
version in your deployment. Install the same bundle wheel in each Airflow environment
that loads the bundle and on any host that runs the publisher command.

On each host that reads the S3 source, install the optional Amazon provider
dependency:

```bash
python -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "airflow-manifest-bundle[s3] @ ${BUNDLE_WHEEL_URL}"
```

The base package and the local bundle do not import the Amazon provider. An
explicit-mode S3 dag processor that only consumes published releases can also use
the base package; install the S3 extra on the explicit publisher host.

The bundle requires Apache Airflow 3.1 or newer, detected at import time with no
configuration: on Airflow 3.3+ `get_current_version` returns a `BundleVersion`; on
3.1/3.2 it returns the plain version string those releases expect.

## Configure

Bundles are discovered purely via Airflow config — no plugin registration:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "my_dags",
      "classpath": "airflow_manifest_bundle.local.ManifestLocalDagBundle",
      "kwargs": {
        "source_path": "/shared/dags",
        "published_root": "/shared/dag-releases",
        "refresh_interval": 30
      }
    }
  ]
```

For S3, keep the same authoritative local `published_root` and replace only the
source adapter:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "my_dags",
      "classpath": "airflow_manifest_bundle.s3.ManifestS3DagBundle",
      "kwargs": {
        "bucket_name": "airflow-dags",
        "prefix": "dags/",
        "published_root": "/shared/dag-releases",
        "refresh_interval": 30,
        "deployment_marker_key": ".ready"
      }
    }
  ]
```

`aws_conn_id` is optional and defaults to `aws_default`, as it does for Airflow's
stock `S3DagBundle`. The S3 adapter lists and reads objects. It never writes to S3.
It mirrors the folder into disposable local staging, but Airflow never parses or
executes that mirror. See the [S3 operator guide](docs/s3.md) for IAM, deployment
markers, storage, and recovery.

S3 automatic publication is enabled by default. Set `"auto_publish": false` when a
deployment pipeline runs `publish-s3` and dag processors must only consume explicit
releases.

The S3 adapter rejects more than 10,000 included objects, an object larger than
100 MiB, or more than 1 GiB in total by default. Configure `max_file_count`,
`max_file_size_bytes`, and `max_total_size_bytes` when a known Dag source needs
different bounds.

`dag_bundle_storage_path` is optional. If you do not set it, Airflow uses
`Path(tempfile.gettempdir()) / "airflow" / "dag_bundles"` (usually
`/tmp/airflow/dag_bundles`) for its disposable cache. Set it explicitly when you
want a predictable location where you can inspect materialized snapshots and their
embedded manifests:

```ini
[dag_processor]
dag_bundle_storage_path = /var/lib/airflow/dag-bundle-cache
```

The environment-variable equivalents are
`AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` and, for the optional cache-path
override, `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH`.

Keep these locations separate and non-overlapping:

- **the Dag source** — a mutable local tree or S3 folder, read by the selected publisher
- **the S3 mirror** — disposable per-host staging used only by the S3 adapter
- **`published_root`** — the authoritative publication area, a shared filesystem path
  writable by publishers and readable by all Airflow components
- **`dag_bundle_storage_path`** — Airflow's disposable per-host cache

Airflow always parses and executes the immutable snapshot, never the mutable source
or S3 mirror. `source_path` enables the automatic local publisher. An S3 bundle
publishes during refresh when `auto_publish` is true. A pinned bundle never
publishes.

For explicit local publication, omit `source_path` from the local bundle config. The
publisher command receives the source path at deployment time:

```ini
"kwargs": {
  "published_root": "/shared/dag-releases",
  "refresh_interval": 30
}
```

## Automatic publication

Each unpinned publisher checks source metadata during `refresh()`. A changed source
must remain unchanged for
`source_stability_seconds` before the bundle hashes and publishes it. The default
stability period is `refresh_interval`; set it explicitly when you need a different
delay:

```ini
"kwargs": {
  "source_path": "/shared/dags",
  "published_root": "/shared/dag-releases",
  "refresh_interval": 30,
  "source_stability_seconds": 60
}
```

The stability period uses elapsed time, not a count of refresh calls. Set
`source_stability_seconds` to `0` only when your deployment tool replaces the whole
source tree atomically. A metadata stability check cannot prove that a non-atomic
deployment has delivered every intended file. Use a staging directory plus atomic
rename for local sources. For S3, write `deployment_marker_key` last after all
objects are present.

When no release exists, such as on the first start, initialization waits for the
remaining stability period once and logs that normal wait at info level. If the source
changes during that wait, or if publication fails, initialization reports a
recoverable bundle error and Airflow retries. After a release exists, an unstable,
unreadable, empty, or failed publication leaves the current release active. Empty
sources are rejected by default; set `"allow_empty_source": true` only when publishing
an empty bundle is intentional.

Automatic publication has these operational requirements:

- Each unpinned dag processor must be able to read its source and write to the bundle's
  `refs`, `versions`, `_locks`, and `_state` paths under `published_root`. An S3
  publisher also needs write access to its local mirror. Workers that load pinned
  versions need only read `published_root`; they need no S3 credentials.
- All automatic publishers must use the same writer identity, or the administrator
  must grant them write access with ownership or ACLs. Access to `published_root`
  alone is not sufficient when an older publisher owns its existing child directories.
- All automatic publishers for one bundle must observe the same source. The
  shared publication lock makes those publishers safe and idempotent.
- Dag-processor hosts must keep their clocks synchronized. Replicas use one shared
  UTC timestamp to measure the source stability period; a host that sees a timestamp
  in the future waits instead of publishing early.
- The configured source is authoritative. Do not mix automatic publication with a
  different source or with manual reference changes for the same bundle. An automatic
  publisher can move the release reference back to the version of its source.
- Pinned task, retry, callback, and rerun bundles never publish. They materialize only
  the exact version Airflow gives them.

In steady state, a local refresh walks file metadata, and an S3 refresh lists remote
object metadata. Neither reads file contents. A process hashes the prepared local
tree once after it observes a new stable metadata signature.
Replicas share the first-observed signature and timestamp under `published_root`, so
any replica can complete the stability period. The full-hash confirmation remains a
process-local optimization: a restarted process hashes the stable source once, but
does not restart an already elapsed shared stability period. Airflow's disposable
cache stores no publisher state.

Automatic publication writes snapshots, one release reference, and one shared
candidate-state file:

```text
versions/my_dags/sha256-<hex>/            immutable snapshot (read-only, world-readable)
    <your dag files>
    .airflow-bundle-manifest.json         full manifest embedded in the snapshot
refs/my_dags/latest.json                  compact release reference — updated last, atomically
_locks/my_dags.lock                       cross-host publication lock
_state/my_dags/auto-publish.json          shared stability candidate (automatic mode)
```

The release reference changes only after the snapshot is complete and verified. If
automatic publication fails after an earlier release exists, the bundle logs the
error and continues to serve that release. The candidate state is an atomic,
schema-versioned coordination hint; it never selects the release that Airflow serves.

## Explicit publication

For a local bundle configured without `source_path`, publish a release directly:

```bash
airflow-manifest-bundle publish-local my_dags /path/to/dag/source --output json
```

For S3, disable automatic publication in the bundle configuration:

```ini
"kwargs": {
  "auto_publish": false,
  "bucket_name": "airflow-dags",
  "prefix": "dags/",
  "published_root": "/shared/dag-releases",
  "deployment_marker_key": ".ready"
}
```

Then publish the configured S3 source:

```bash
airflow-manifest-bundle publish-s3 my_dags --output json
```

Each command reads the named bundle from Airflow configuration, creates or validates
the content-addressed snapshot, and updates the release reference last. Neither needs
the Airflow metadata database. The local publishing host needs read access to the
source and write access to `published_root`. The S3 publishing host needs read-only
S3 access plus write access to its local mirror and `published_root`.

Use `--expected-current-version sha256-<hex>` after the first release to stop an
older deployment from replacing a newer one. Omit that option for the first release,
when no current version exists. The JSON result includes the version, snapshot and
reference paths, manifest hash, file count, total size, and whether it created the
snapshot.

The commands reject automatic bundles. Omit `source_path` for an explicit local
bundle, and set `auto_publish` to false for an explicit S3 bundle. In explicit S3
mode, dag-processor refreshes do not access S3.

## Deployment behavior

With a plain dags folder, Airflow can see a half-synced deployment and a retry can
read files that changed after the run started. Automatic publication waits for the
source stability period. Explicit publication waits for its command. Both workflows
publish an immutable snapshot and atomically move the release reference. Airflow
continues to serve the previous release until publication succeeds.

Publication is idempotent, atomic, and serialized by the shared lock under
`published_root`. A deployment pipeline can deliver the source tree for automatic
publication, or it can run an explicit publisher command. Neither workflow needs
access to the Airflow metadata database.

**Retention**: Airflow never deletes from `published_root` — pruning old
`versions/<bundle>/sha256-*` directories is a deliberate operation you schedule
yourself, and it must keep any version that a Dag run can still request (retries,
reruns, deferred tasks, callbacks pin by version).

## Verify a deployment

From any host with the Airflow config above and an initialized metadata database
(the Airflow CLI insists on `airflow db migrate` having run, even for local parsing):

```bash
airflow dags list --local --bundle-name my_dags --output table
airflow dags list-import-errors --local --bundle-name my_dags --output table
```

`--local` parses straight from the bundle (no scheduler needed) and reads the
materialized cache copy — never the source tree. A source edit never changes the
files that Airflow parses in place. Automatic publication waits for source stability;
explicit publication waits for its command. Persisted `DagVersion.bundle_version`
values match the published version string.

On the first automatic initialization, the bundle waits for source stability before
it creates the release reference. An explicit bundle needs a successful publisher
command before initialization. The per-host `.sha256-<hex>.validated` marker files
next to cache entries let repeat validations skip the checksum pass — delete them to
force a full re-validation.

The on-disk files are the compatibility contract between bundle processes. The
release reference, manifest, and candidate state carry `schema_version`; incompatible
format changes use a new schema version.

The full design — terms, storage layout, publication procedure, runtime operation,
error contract, and extension plan — is in [docs/design.md](docs/design.md).

## Design notes: coexisting with stock Airflow

An external bundle cannot change Airflow core, so several behaviors are designed around
what core actually does at runtime:

- **Filesystem-safe versions.** Versions are `sha256-<hex>` — safe as a raw path
  segment, so Airflow's cache paths, tracking files, and version locks work untouched.
- **Cleanup-compatible cache permissions.** Cache copies keep files read-only but
  directories writable (`0755`) so Airflow's stale-version cleanup (a plain
  `shutil.rmtree`) works; the published root stays fully read-only (`0555`/`0444`).
  Because an interrupted cleanup can leave a truncated tree behind and writable
  directories permit file injection, the per-host validation marker skips only the
  checksum pass — structure is re-verified once per process.
- **Self-managed cache metadata.** The bundle reaps its own orphaned validation markers
  during materialization, and removes Airflow's usage-tracking file when it moves a
  corrupt cached version aside (a tracking file pointing at a missing dir would crash
  Airflow's cleanup sweep).
- **AirflowException-based error contract.** Airflow's dag processor treats only
  `AirflowException` from a bundle as a recoverable per-bundle error, so
  `BundleManifestError` subclasses it; `initialize()`/`refresh()`/`get_current_version()`/
  `path` wrap incidental `OSError` into it, and deliberate missing-artifact errors are
  `BundleManifestNotFoundError` (both `BundleManifestError` and `FileNotFoundError`), so
  every entry point core calls degrades gracefully.
- **Callback safety.** For callbacks without a pinned version, `path` falls back to the
  newest validated cached version when the just-published release is not materialized
  yet (otherwise the callback would be lost — core deletes callback rows before parsing).
- **Automatic publication.** `source_path` lets the bundle publish during an unpinned
  local refresh. The S3 adapter does the same from its isolated mirror. Pinned bundle
  instances only materialize the requested version.
- **Standalone publisher CLI.** External packages cannot add `airflow` subcommands, so
  explicit local and S3 publication use the `airflow-manifest-bundle` console script.
- **Shared automatic-publisher coordination.** Airflow can run successive refreshes
  in different dag-processor replicas, so the stability candidate lives under
  `published_root`, protected by the publication lock. Only the confirmed-source
  hashing optimization remains process-local.

Known caveat: Airflow's task startup takes the bundle version lock only after
`bundle.initialize()`, so a stale-cleanup race can remove a version mid-initialization;
the bundle rematerializes from the published root, so the task fails once and heals on
retry.

`_compat.py` carries a small self-contained helper (`remove_bundle_tree_forcefully`)
rather than depending on Airflow internals that may change between releases.

## Development

```bash
pip install -e '.[dev]'
pytest
```

The test suite covers manifest hashing and determinism, local and S3 publication,
the publisher CLI, S3 mirror safety, validation, and cache materialization and
self-healing. It runs against an installed Airflow with no database required.

Maintainers currently publish wheels and source distributions through GitHub Releases.
See the [release process](docs/releasing.md) for the version, tag, build, publication,
and verification steps.
