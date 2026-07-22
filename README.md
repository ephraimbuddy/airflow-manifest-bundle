# airflow-manifest-bundle

A manifest-backed local Dag bundle for Apache Airflow — a standalone, pip-installable
package that runs on unpatched Airflow.

The stock `LocalDagBundle` cannot identify the exact source files a task retry or rerun
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

Requires Airflow with `BundleVersion` / `version_data` support in `BaseDagBundle`
(Airflow main as of July 2026; not in 3.1.x).

## Configure

Bundles are discovered purely via Airflow config — no plugin registration:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "my_dags",
      "classpath": "airflow_manifest_bundle.bundle.ManifestLocalDagBundle",
      "kwargs": {"published_root": "/shared/dag-releases"}
    }
  ]
```

`published_root` must be a shared filesystem path visible to the dag processor, workers,
and the publisher, and must not overlap `dag_bundle_storage_path` (Airflow's cache).

## Publish a release

```bash
airflow-manifest-bundle publish-local my_dags /path/to/dag/source
```

- Publishing identical content is **idempotent** (same content hash, no new snapshot).
- Concurrent publishers serialize through a shared lock under `published_root`.
- Deploy systems that can finish out of order can pass
  `--expected-current-version sha256-<hex>` to refuse a stale update.
- A failed publication leaves the previous release active: the release reference
  (`refs/<bundle>/latest.json`) is updated atomically as the last step.

The publisher and the Airflow runtime can run as different OS users: snapshots are
world-readable and read-only.

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
