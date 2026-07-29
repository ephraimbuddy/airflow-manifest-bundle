"""
Contract tests for ``ArtifactStore`` implementations.

Every implementation must satisfy these behaviors; the tests talk only to the
interface. When another backend lands (for example an S3 store), add it to the
``store`` fixture parametrization instead of writing a parallel suite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from airflow_manifest_bundle.bundle import FilesystemArtifactStore
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestNotFoundError,
    build_bundle_version_manifest_result,
)

BUNDLE_NAME = "contract"


@pytest.fixture(params=["filesystem"])
def store(request, tmp_path):
    if request.param == "filesystem":
        return FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=tmp_path / "published",
            cache_root=tmp_path / "cache",
        )
    raise ValueError(request.param)


def _write_source(root: Path) -> Path:
    (root / "dags").mkdir(parents=True, exist_ok=True)
    (root / "dags" / "example.py").write_text("print('dag')\n")
    return root


def _manifest_result(source: Path):
    return build_bundle_version_manifest_result(
        bundle_name=BUNDLE_NAME,
        root=source,
        backend_type="local",
    )


def _publish(store, source: Path):
    result = _manifest_result(source)
    store.prepare_publish_areas()
    created = store.publish_snapshot(
        result.version,
        manifest=result.manifest,
        source_root=source,
        validate_existing=lambda tree: None,
    )
    return result, created


class TestDocuments:
    def test_read_ref_missing_raises_not_found_with_message(self, store):
        with pytest.raises(BundleManifestNotFoundError, match="ref is gone"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")

    def test_ref_roundtrip(self, store):
        store.prepare_publish_areas()
        store.write_ref({"schema_version": 1, "bundle_name": BUNDLE_NAME})
        payload = store.read_ref(missing_message="missing", invalid_message="invalid")
        assert payload == {"schema_version": 1, "bundle_name": BUNDLE_NAME}

    def test_read_ref_invalid_json_raises_with_message(self, store):
        store.prepare_publish_areas()
        store.ref_path.parent.mkdir(parents=True, exist_ok=True)
        store.ref_path.write_text("{not json")
        with pytest.raises(BundleManifestError, match="ref is invalid"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")

    def test_state_roundtrip_and_replace(self, store):
        store.prepare_state_area()
        store.write_state({"first_observed_at": 1.0})
        store.write_state({"first_observed_at": 2.0})
        payload = store.read_state(missing_message="missing", invalid_message="invalid")
        assert payload == {"first_observed_at": 2.0}

    def test_read_state_missing_raises_not_found(self, store):
        with pytest.raises(BundleManifestNotFoundError, match="state is gone"):
            store.read_state(missing_message="state is gone", invalid_message="invalid")


class TestSnapshots:
    def test_publish_then_exists_then_fetch(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result, created = _publish(store, source)

        assert created is True
        assert store.snapshot_exists(result.version)

        seen_trees: list[Path] = []
        destination = tmp_path / "dest"
        destination.mkdir()
        store.fetch_snapshot(
            result.version,
            destination,
            structural_validator=seen_trees.append,
        )
        assert seen_trees, "structural validator must run before the transfer"
        assert (destination / "dags" / "example.py").read_text() == "print('dag')\n"
        embedded = json.loads((destination / MANIFEST_FILE_NAME).read_text())
        assert embedded["version"] == result.version

    def test_publish_is_idempotent_and_validates_existing(self, store, tmp_path):
        source = _write_source(tmp_path / "source")
        result, created = _publish(store, source)
        assert created is True

        validated: list[Path] = []
        created_again = store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=validated.append,
        )
        assert created_again is False
        assert validated, "an existing snapshot must be validated, not rewritten"

    def test_fetch_unpublished_version_raises_not_found(self, store, tmp_path):
        destination = tmp_path / "dest"
        destination.mkdir()
        version = "sha256-" + "0" * 64
        with pytest.raises(BundleManifestNotFoundError, match="is not published"):
            store.fetch_snapshot(
                version,
                destination,
                structural_validator=lambda tree: None,
            )

    def test_snapshot_exists_is_false_before_publication(self, store):
        assert store.snapshot_exists("sha256-" + "0" * 64) is False

    def test_sweep_publish_temps_removes_orphans(self, store):
        store.prepare_publish_areas()
        orphan_snapshot = store.snapshots_root / (".sha256-" + "0" * 64 + ".tmp123")
        orphan_snapshot.mkdir(parents=True)
        orphan_ref = store.ref_path.parent / f".{store.ref_path.name}.tmp123"
        orphan_ref.write_text("partial")

        store.sweep_publish_temps()

        assert not orphan_snapshot.exists()
        assert not orphan_ref.exists()


class TestCoordination:
    def test_publication_guard_enters_and_exits(self, store):
        with store.publication_guard():
            pass
        # Re-acquirable after release.
        with store.publication_guard():
            pass

    def test_validate_source_paths_rejects_overlap(self, store, tmp_path):
        with pytest.raises(ValueError):
            store.validate_source_paths(
                store.root / "source",
                cache_versions_dir=tmp_path / "cache" / "versions",
            )

    def test_validate_source_paths_accepts_disjoint_source(self, store, tmp_path):
        source = _write_source(tmp_path / "elsewhere")
        store.validate_source_paths(source, cache_versions_dir=tmp_path / "cache" / "versions")

    def test_locators_are_scoped_to_the_bundle(self, store):
        version = "sha256-" + "a" * 64
        assert store.snapshot_path(version).name == version
        assert str(store.snapshots_root).startswith(str(store.root))
        assert str(store.ref_path).startswith(str(store.root))
        assert str(store.state_path).startswith(str(store.root))


class TestFilesystemConstruction:
    def test_rejects_published_root_overlapping_cache(self, tmp_path):
        with pytest.raises(ValueError, match="must not overlap"):
            FilesystemArtifactStore(
                bundle_name=BUNDLE_NAME,
                published_root=tmp_path / "cache" / "published",
                cache_root=tmp_path / "cache",
            )
