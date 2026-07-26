# airflow-manifest-bundle

A manifest-backed local Dag bundle for Apache Airflow — pip-install it, point your
bundle config at it, and it works with any standard Airflow 3.1+ installation.

The Airflow `LocalDagBundle` cannot identify the exact source files a task retry or rerun
used: files can change after a Dag run is created, so its bundle version resolves nothing.
`ManifestLocalDagBundle` gives shared-filesystem deployments reproducible pinned execution
without requiring Git — it works like `GitDagBundle` does for commits:

- A deploy step **publishes** an immutable, content-addressed snapshot of the Dag source
  under a shared `published_root`. The bundle version is a SHA-256 content hash
  (`sha256-<hex>`), and the release reference is updated atomically as the last step.
- Airflow **materializes** snapshots into its normal, disposable bundle cache before
  parsing or execution, validating path safety, integrity, and permissions.
- Task retries and reruns **pin** through the existing `DagRun.bundle_version` field and
  rematerialize from the published root if the local cache was cleaned.

## Install

```bash
pip install .
```

Requires Apache Airflow 3.1 or newer, detected at import time with no configuration:
on Airflow 3.3+ `get_current_version` returns a `BundleVersion`; on 3.1/3.2 it returns
the plain version string those releases expect.

## Configure

Bundles are discovered purely via Airflow config — no plugin registration:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "my_dags",
      "classpath": "airflow_manifest_bundle.local.ManifestLocalDagBundle",
      "kwargs": {"published_root": "/shared/dag-releases", "refresh_interval": 30}
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

- **the Dag source tree** — mutable, only ever read by the publisher command
- **`published_root`** — the authoritative publication area, a shared filesystem path
  readable by the dag processor, workers, and the publisher
- **`dag_bundle_storage_path`** — Airflow's disposable per-host cache

The bundle never scans a source directory at runtime, and editing source files changes
nothing until the next publish: Airflow only follows the release reference.

## Publish a release

The source directory is supplied only to the publisher:

```bash
airflow-manifest-bundle publish-local my_dags /path/to/dag/source --output json
```

The publisher builds a deterministic manifest of the source tree (ignoring `.git`,
`__pycache__`, and `*.pyc`), then writes under `published_root`:

```text
versions/my_dags/sha256-<hex>/            immutable snapshot (read-only, world-readable)
    <your dag files>
    .airflow-bundle-manifest.json         full manifest embedded in the snapshot
refs/my_dags/latest.json                  compact release reference — updated last, atomically
_locks/my_dags.lock                       cross-host publication lock
```

The version is the SHA-256 content hash of the manifest entries, and the `--output json`
result reports it along with `version_path`, `manifest_ref_path`, `file_count`,
`total_size`, and `created_snapshot`. Airflow validates and copies the referenced
snapshot into its own cache before parsing or execution; stale-cache cleanup may delete
that copy, and initialization rematerializes it from `published_root`.

The publisher and the Airflow runtime can run as different OS users: snapshots are
world-readable and read-only. The publisher chmods only directories it creates — a
pre-provisioned `published_root` keeps its permissions, so make it readable by the
Airflow components.

## Deploying: the publish command is the deploy

With a plain dags folder, "deploy" happens implicitly the moment files land in the
folder — Airflow can see half-synced state, and a retry can pick up files that changed
after the run started. With this bundle, copying files deploys nothing. Your existing
workflow stays the same — edit, push, let your deploy tool deliver the files — and
gains exactly one command at the end:

```bash
airflow-manifest-bundle publish-local my_dags ./dags
```

That command **is** the deploy: it snapshots the source and atomically flips the
release reference, and until it runs, Airflow keeps serving the previous release no
matter what is happening in the source directory. Anything can run it — a person over
ssh, a deploy script, a CI job — as long as the host has the package and Airflow
installed, the bundle config visible (env vars are enough), and `published_root`
mounted. **No metadata database access is required to publish.**

### Optional hardening for automated pipelines

Nothing below is required by the bundle. If deploys are automated and can overlap,
two additions make them safe; shown as a CI job sketch (any CI system works — the
runner just needs `/shared/dag-releases` mounted):

```yaml
deploy-dags:
  runs-on: [self-hosted, dag-deployer]
  env:
    AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST: >
      [{"name": "my_dags",
        "classpath": "airflow_manifest_bundle.local.ManifestLocalDagBundle",
        "kwargs": {"published_root": "/shared/dag-releases"}}]
  steps:
    - uses: actions/checkout@v4
    - run: pip install "apache-airflow==<your Airflow version>" airflow-manifest-bundle

    # Capture the released version FIRST: it is the optimistic-concurrency token that
    # stops an older pipeline run from clobbering a newer release at the end.
    - name: Capture current release
      run: echo "EXPECTED=$(jq -r .version /shared/dag-releases/refs/my_dags/latest.json)" >> "$GITHUB_ENV"

    - name: Test Dags before publishing
      run: pytest tests/dag_integrity/   # import checks, policy checks, etc.

    - name: Publish
      run: |
        airflow-manifest-bundle publish-local my_dags ./dags \
          --expected-current-version "$EXPECTED" --output json | tee release.json
```

Omit `--expected-current-version` only on the very first publication (with nothing to
compare against, the publisher refuses the flag with a clear error).

Properties that make publishing safe to automate:

- **Idempotent** — republishing identical content computes the same version, creates no
  new snapshot (`created_snapshot: false`), and simply confirms the reference.
- **Atomic and fail-safe** — the reference is replaced last via an atomic rename; a
  publish that dies at any earlier point leaves the previous release fully active.
- **Serialized** — concurrent publishers queue on the shared lock under `published_root`.
- **Ordered** — `--expected-current-version` makes a stale pipeline fail loudly instead
  of rolling the reference backward.

**Rollback** is just publishing the previous source again:

```bash
git checkout <last-good-ref>
airflow-manifest-bundle publish-local my_dags ./dags \
  --expected-current-version "$BAD_VERSION"
```

Content-addressing makes this instant when the old snapshot still exists under
`published_root`: nothing is copied, the reference moves back.

**Retention**: Airflow never deletes from `published_root` — pruning old
`versions/<bundle>/sha256-*` directories is a deliberate operation you schedule
yourself, and it must keep any version that a Dag run can still request (retries,
reruns, deferred tasks, callbacks pin by version).

## Verify a deployment

From any host with the Airflow config above and an initialized metadata database
(the CLI insists on `airflow db migrate` having run, even for local parsing):

```bash
airflow dags list --local --bundle-name my_dags --output table
airflow dags list-import-errors --local --bundle-name my_dags --output table
```

`--local` parses straight from the bundle (no scheduler needed) and reads the
materialized cache copy — never the source tree. Editing source files without
publishing changes nothing; after a publish, the same command shows the new Dags.
Persisted `DagVersion.bundle_version` values match the published version string.

Two operational notes: if the release reference is missing, initialization fails with
an error telling you to run the publisher; and the per-host `.sha256-<hex>.validated`
marker files next to cache entries let repeat validations skip the checksum pass —
delete them to force a full re-validation.

The on-disk files are the compatibility contract between publisher and runtime: both
sides implement the manifest `schema_version` they read and write, so their patch
versions need not match. Publish from an image aligned with your target Airflow
release rather than assuming an older CLI can write a newer schema.

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
- **Standalone publisher CLI.** External packages cannot add `airflow` subcommands, so
  publishing ships as the `airflow-manifest-bundle` console script.

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

The test suite covers manifest hashing and determinism, snapshot publication and
validation, cache materialization and self-healing, and the publisher CLI. It runs
against an installed Airflow with no database required.

Maintainers currently publish wheels and source distributions through GitHub Releases.
See the [release process](docs/releasing.md) for the version, tag, build, publication,
and verification steps.
