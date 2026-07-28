# AGENTS.md

Guidance for AI coding agents (and new contributors) working in this repository.

## What this is

`airflow-manifest-bundle` is a standalone pip package that provides local and S3
source adapters for manifest Dag bundles. Both adapters serve immutable,
content-addressed Dag snapshots from a shared filesystem. The bundle version is the
SHA-256 content hash of the snapshot's manifest, so snapshots are self-certifying.
Read `docs/design.md` before changing runtime behavior — it defines the terms and the
safety argument.

## Layout

- `src/airflow_manifest_bundle/manifest.py` — content-addressing core: hashing, manifest
  schema, validation. Shared by all (current and future) backends.
- `src/airflow_manifest_bundle/bundle.py` — common publication, reference, immutable
  snapshot, cache, and validation lifecycle.
- `src/airflow_manifest_bundle/local.py` — local source adapter.
- `src/airflow_manifest_bundle/s3.py` — read-only S3 source and local mirror adapter.
- `src/airflow_manifest_bundle/cli.py` — the `airflow-manifest-bundle` console script.
- `src/airflow_manifest_bundle/_compat.py` — the only place that handles differences
  between Airflow releases.
- `tests/` — pytest suite; no database, no scheduler, no network required.
- `docs/design.md` — the design document, written in ASD-STE100 Simplified Technical
  English. Edits to it must keep STE style (active voice, short sentences, no -ing verb
  forms, one meaning per term).

## Commands

```bash
uv venv && uv pip install "apache-airflow==3.3.0" pytest -e .   # or 3.1.8
.venv/bin/python -m pytest -q            # full suite, ~3s
uvx ruff==0.16.0 check src tests scripts # lint
uv build && uvx twine check dist/*       # packaging sanity
```

Install with `-e` (editable): the tests import the installed package, not `src/`, so a
non-editable install silently tests stale code after you edit `src/`. CI installs
non-editable on purpose — it validates the packaged wheel — so do not "fix" that.

## Invariants — do not break these

1. **Stock Airflow 3.1+ only.** Never import an Airflow symbol without confirming it
   exists in Airflow 3.1.8. Version-dependent behavior goes in `_compat.py`
   (see `make_bundle_version`: `BundleVersion` object on 3.3+, plain string earlier).
   Verify changes against both ends of the support range; CI runs 3.1.8 and 3.3.0.
2. **Version strings are `sha256-<hex>` and must stay filesystem-safe.** Airflow core
   builds cache, lock, and tracking paths from the raw version string.
3. **Error contract.** Every entry point Airflow calls (`initialize`, `refresh`,
   `get_current_version`, `path`) must only raise `AirflowException` subclasses.
   (One deliberate exception: `refresh()` raises `ValueError` on a pinned bundle —
   Airflow core never does that, and the guard marks a programming error. Keep it.)
   Manifest errors use `BundleManifestError`; missing artifacts use
   `BundleManifestNotFoundError` (also a `FileNotFoundError` — keep the dual
   inheritance); incidental `OSError` is wrapped via `_oserror_as_manifest_error`.
   A raw exception from a bundle entry point can crash Airflow's dag processor.
4. **Permissions are load-bearing.** Published snapshots: dirs `0555`, files
   `0444`/`0555` (multiple OS users read them). Cache copies: files read-only but dirs
   `0755` — Airflow's stale-cache cleanup uses plain `shutil.rmtree`, which fails on
   read-only dirs.
5. **Validation markers skip only the checksum pass.** The structural check (file set,
   types, no symlinks) must still run once per process; it is what detects truncated or
   mutated cache trees.
6. **The on-disk format is the compatibility contract.** `latest.json`, the embedded
   manifest, and automatic-publisher candidate state carry `schema_version`. Any
   incompatible format change requires a new schema version, never a silent change to
   schema 1.
7. **Publication ordering.** The release reference is written last, atomically, after
   the snapshot is fully materialized and verified. A failed publish must leave the
   previous release active.
8. **Automatic publication is fail-safe.** An automatic publication error must not
   disrupt an existing release. Pinned bundles never publish. Candidate readiness is
   shared under `published_root`; only the confirmed-source hashing hint stays in
   process memory. Airflow's disposable cache stores neither.
9. **One automatic source is authoritative.** All automatic publishers for one bundle
   must observe the same source. The shared publication lock makes that safe and
   idempotent; mixing automatic sources or manual reference changes is unsupported.

## Conventions

- No per-file license headers — licensing lives in `LICENSE` and pyproject metadata.
- Commit messages: imperative subject line, then prose paragraphs explaining motivation
  and verification (see `git log`).
- New manifest backends extend this package rather than becoming new packages: one
  module (e.g. `s3.py`) and one optional-dependency group in `pyproject.toml`. All
  backends share `manifest.py` and its version calculus.
- The README is conversational; `docs/design.md` is strict STE. Keep both in the shared
  vocabulary defined by the design doc's terms table (snapshot, manifest, release
  reference, cache copy, marker).

## Workflow

`main` is protected: no direct pushes. Branch, push, open a PR, and the four required
checks (`lint`, `build`, and the two test-matrix jobs) must pass before merge. Renaming
CI jobs requires updating the required-check contexts in the repository ruleset.
