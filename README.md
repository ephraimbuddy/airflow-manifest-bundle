# airflow-manifest-bundle

A manifest-backed local Dag bundle for Apache Airflow — pip-install it, point your
bundle config at it, and it works with any standard Airflow 3.1+ installation.

The Airflow `LocalDagBundle` cannot identify the exact source files a task retry or rerun
used: files can change after a Dag run is created, so its bundle version resolves nothing.
`ManifestLocalDagBundle` gives shared-filesystem deployments reproducible pinned execution
without requiring Git — it works like `GitDagBundle` does for commits:

- The bundle **publishes** an immutable, content-addressed snapshot automatically
  after the Dag source stays unchanged for a configured interval.
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
that loads the bundle.

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

Keep three locations separate and non-overlapping:

- **the Dag source tree** — mutable, read by the bundle's automatic publisher
- **`published_root`** — the authoritative publication area, a shared filesystem path
  writable by automatic publishers and readable by all Airflow components
- **`dag_bundle_storage_path`** — Airflow's disposable per-host cache

Airflow always parses and executes the immutable snapshot, never the mutable source
tree. The `source_path` option makes the unpinned bundle refresh act as the publisher.
A pinned bundle does not read or publish the source.

## Automatic publication

When `source_path` is configured, each unpinned bundle process checks source metadata
during `refresh()`. A changed source must remain unchanged for
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
source tree atomically. A metadata stability check cannot prove that a non-atomic sync
has delivered every intended file, so a staging directory plus atomic rename is the
safest source-delivery pattern.

When no release exists, such as on the first start, initialization waits for the
remaining stability period once and logs that normal wait at info level. If the source
changes during that wait, or if publication fails, initialization reports a
recoverable bundle error and Airflow retries. After a release exists, an unstable,
unreadable, empty, or failed publication leaves the current release active. Empty
sources are rejected by default; set `"allow_empty_source": true` only when publishing
an empty bundle is intentional.

Automatic publication has these operational requirements:

- Each unpinned dag processor that uses `source_path` must be able to read that path
  and write to the bundle's `refs`, `versions`, `_locks`, and `_state` paths under
  `published_root`. Workers that load pinned versions need only read `published_root`.
- All automatic publishers must use the same writer identity, or the administrator
  must grant them write access with ownership or ACLs. Access to `published_root`
  alone is not sufficient when an older publisher owns its existing child directories.
- All automatic publishers for one bundle must observe the same source tree. The
  shared publication lock makes those publishers safe and idempotent.
- Dag-processor hosts must keep their clocks synchronized. Replicas use one shared
  UTC timestamp to measure the source stability period; a host that sees a timestamp
  in the future waits instead of publishing early.
- `source_path` is authoritative. Do not mix automatic publication with a different
  source or with manual reference changes for the same bundle. An automatic publisher
  can move the release reference back to the version of its source.
- Pinned task, retry, callback, and rerun bundles never publish. They materialize only
  the exact version Airflow gives them.

In steady state, a refresh walks file metadata but does not read file contents. A
process hashes the source once after it observes a new stable metadata signature.
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

## Deployment behavior

With a plain dags folder, Airflow can see a half-synced deployment and a retry can
read files that changed after the run started. This bundle waits for the source
stability period, publishes an immutable snapshot, and atomically moves the release
reference. Airflow continues to serve the previous release until that sequence
succeeds.

Publication is idempotent, atomic, and serialized by the shared lock under
`published_root`. A deployment pipeline can deliver the source tree without access
to the Airflow metadata database. The dag processor publishes it after the configured
stability period.

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
files that Airflow parses in place: automatic publication waits for source stability
and creates a new snapshot. Persisted `DagVersion.bundle_version` values match the
published version string.

On the first initialization, the bundle waits for source stability before it creates
the release reference. The per-host `.sha256-<hex>.validated` marker files next to
cache entries let repeat validations skip the checksum pass — delete them to force
a full re-validation.

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
  refresh. Pinned bundle instances only materialize the requested version.
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

The test suite covers manifest hashing and determinism, automatic snapshot
publication, validation, and cache materialization and self-healing. It runs against
an installed Airflow with no database required.

Maintainers currently publish wheels and source distributions through GitHub Releases.
See the [release process](docs/releasing.md) for the version, tag, build, publication,
and verification steps.
