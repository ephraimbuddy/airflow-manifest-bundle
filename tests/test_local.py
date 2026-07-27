from __future__ import annotations

import json
import os
import shutil
import stat
from pathlib import Path
from unittest import mock

import pytest
from _test_utils import conf_vars

from airflow_manifest_bundle import local as local_bundle_module
from airflow_manifest_bundle import manifest as manifest_module
from airflow_manifest_bundle._compat import remove_bundle_tree_forcefully
from airflow_manifest_bundle.local import (
    BundleManifestReferenceChangedError,
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    MANIFEST_SCHEMA_VERSION,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest_result,
    serialize_bundle_version_manifest,
)


def _write_file(root: Path, relative_path: str, content: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _version_string(result) -> str:
    # get_current_version returns BundleVersion on newer Airflow, a plain str on older.
    return getattr(result, "version", result)


def _write_manifest_ref(manifest_ref_path: Path, ref_payload: dict) -> None:
    manifest_ref_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_ref_path.write_bytes(serialize_bundle_version_manifest(ref_payload))


def _add_write_bits(path: Path) -> None:
    write_bits = 0o700 if path.is_dir() else 0o600
    path.chmod(stat.S_IMODE(path.stat().st_mode) | write_bits)


def _create_symlink_or_skip(target: Path, link: Path, *, target_is_directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")


def _publish_manifest_local_bundle(
    bundle: ManifestLocalDagBundle,
    source: Path,
    *,
    materialize_snapshot: bool = True,
):
    if materialize_snapshot:
        return publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

    result = build_bundle_version_manifest_result(
        bundle_name=bundle.name,
        root=source,
        backend_type="local",
    )
    _write_manifest_ref(bundle.manifest_ref_path, result.ref_payload)
    return result


class TestManifestLocalDagBundle:
    @pytest.fixture(autouse=True)
    def _clear_validated_version_paths(self):
        ManifestLocalDagBundle._validated_version_paths.clear()
        yield
        ManifestLocalDagBundle._validated_version_paths.clear()

    def test_supports_versioning(self):
        assert ManifestLocalDagBundle.supports_versioning is True

    def test_requires_published_root(self):
        # TypeError, not ValueError: stock prepare_callback_bundle swallows ValueError from
        # bundle construction as "Bundle no longer configured", silently dropping callbacks.
        with pytest.raises(TypeError, match="published_root must be provided"):
            ManifestLocalDagBundle(name="manifest-local")

        with pytest.raises(TypeError, match="published_root must be provided"):
            ManifestLocalDagBundle(name="manifest-local", published_root="")

    def test_published_root_derives_authoritative_paths(self, tmp_path):
        bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(tmp_path / "published"))

        assert bundle.manifest_ref_path == tmp_path / "published/refs/manifest-local/latest.json"
        assert bundle.published_versions_dir == tmp_path / "published/versions/manifest-local"
        assert bundle.publication_lock_path == tmp_path / "published/_locks/manifest-local.lock"

    @pytest.mark.parametrize("published_root_position", ["inside", "ancestor"])
    def test_rejects_published_root_overlapping_airflow_cache(self, tmp_path, published_root_position):
        cache_root = tmp_path / "cache"
        published_root = cache_root / "published" if published_root_position == "inside" else tmp_path

        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(cache_root)}),
            pytest.raises(ValueError, match="must not overlap Airflow's bundle cache"),
        ):
            ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
            )

    def test_get_current_version_returns_bundle_version_from_ref(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
        bundle_version = bundle.get_current_version()

        assert _version_string(bundle_version) == published.version
        # The version string is the whole contract: no metadata flows through Airflow.
        assert getattr(bundle_version, "data", None) is None

    def test_path_and_get_current_version_do_not_materialize_snapshots(self, tmp_path):
        missing_version = f"sha256-{'0' * 64}"
        manifest_ref_path = tmp_path / "published/refs/manifest-local/latest.json"
        _write_manifest_ref(
            manifest_ref_path,
            {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "bundle_name": "manifest-local",
                "version": missing_version,
                "backend": {"type": "local"},
                "manifest": {
                    "path": MANIFEST_FILE_NAME,
                    "sha256": f"sha256:{'1' * 64}",
                },
                "file_count": 0,
                "total_size": 0,
            },
        )

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            with mock.patch.object(
                ManifestLocalDagBundle, "_materialize_cached_version", autospec=True
            ) as mock_materialize:
                assert _version_string(bundle.get_current_version()) == missing_version
                assert bundle.path == bundle.versions_dir / (missing_version)
            mock_materialize.assert_not_called()

            with pytest.raises(BundleManifestError, match="not published"):
                bundle.initialize()

    def test_path_loads_materialized_snapshot_before_refresh(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)

            assert bundle._current_manifest_ref is None
            assert bundle.path == bundle.versions_dir / (published.version)

    def test_refresh_uses_materialized_snapshot_for_latest_path(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        _write_file(source, "dags/__pycache__/ignored.pyc", "compiled")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)

            bundle.refresh()

            assert bundle.path == bundle.versions_dir / (published.version)
            assert bundle.path != source
            assert (bundle.path / "dags/example.py").read_text() == "print('dag')"
            assert not (bundle.path / "dags/example.py").stat().st_mode & 0o200
            assert stat.S_IMODE(bundle.path.stat().st_mode) == 0o755
            assert stat.S_IMODE((bundle.path / "dags").stat().st_mode) == 0o755
            assert not (bundle.path / "dags/__pycache__/ignored.pyc").exists()
            snapshot_manifest = json.loads((bundle.path / MANIFEST_FILE_NAME).read_text())
            assert snapshot_manifest["version"] == published.version
            assert [file_info["path"] for file_info in snapshot_manifest["files"]] == ["dags/example.py"]

    def test_publish_and_refresh_support_nested_paths_before_root_files(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "z.py", "print('root')")
        _write_file(source, "a/example.py", "print('nested')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            published = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
            snapshot_manifest = json.loads(
                (published.version_path / MANIFEST_FILE_NAME).read_text(encoding="utf-8")
            )
            bundle.refresh()

            assert [file_info["path"] for file_info in snapshot_manifest["files"]] == [
                "a/example.py",
                "z.py",
            ]
            assert (bundle.path / "a/example.py").read_text() == "print('nested')"
            assert (bundle.path / "z.py").read_text() == "print('root')"

    def test_publish_manifest_local_dag_bundle_is_idempotent(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            first = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
            second = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert first.version == second.version
            assert first.created_snapshot is True
            assert second.created_snapshot is False
            assert json.loads(bundle.manifest_ref_path.read_text()) == second.ref_payload
            assert not stat.S_IMODE(second.version_path.stat().st_mode) & 0o222
            assert not (bundle.versions_dir / (second.version)).exists()

    def test_publish_creates_world_readable_artifacts(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        script = _write_file(source, "bin/task.sh", "#!/bin/sh\n")
        script.chmod(0o700)
        published_root = tmp_path / "share" / "nested" / "published"

        old_umask = os.umask(0o077)
        try:
            with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
                bundle = ManifestLocalDagBundle(
                    name="manifest-local",
                    published_root=str(published_root),
                )
                published = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
        finally:
            os.umask(old_umask)

        def get_mode(path: Path) -> int:
            return stat.S_IMODE(path.stat().st_mode)

        assert get_mode(bundle.manifest_ref_path) == 0o644
        assert get_mode(bundle.publication_lock_path) == 0o644
        # Ancestors the publisher created above published_root must also be traversable.
        assert get_mode(tmp_path / "share") == 0o755
        assert get_mode(tmp_path / "share" / "nested") == 0o755
        assert get_mode(bundle.published_root) & 0o055 == 0o055
        assert get_mode(bundle.manifest_ref_path.parent) & 0o055 == 0o055
        assert get_mode(bundle.published_versions_dir) & 0o055 == 0o055
        assert get_mode(published.version_path) == 0o555
        assert get_mode(published.version_path / "dags") == 0o555
        assert get_mode(published.version_path / "dags/example.py") == 0o444
        assert get_mode(published.version_path / "bin/task.sh") == 0o555
        assert get_mode(published.version_path / MANIFEST_FILE_NAME) == 0o444

    def test_publish_rejects_stale_expected_current_version(self, tmp_path):
        source = tmp_path / "source"
        dag_file = _write_file(source, "dags/example.py", "print('first')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            first = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
            dag_file.write_text("print('second')")
            second = publish_manifest_local_dag_bundle(
                bundle=bundle,
                source_path=source,
                expected_current_version=first.version,
            )
            dag_file.write_text("print('stale')")

            with pytest.raises(
                BundleManifestReferenceChangedError,
                match=f"expected {first.version!r}, found {second.version!r}",
            ):
                publish_manifest_local_dag_bundle(
                    bundle=bundle,
                    source_path=source,
                    expected_current_version=first.version,
                )

            assert json.loads(bundle.manifest_ref_path.read_text())["version"] == second.version

    def test_first_publish_rejects_expected_current_version_with_clear_error(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            with pytest.raises(
                BundleManifestReferenceChangedError,
                match="no published version to compare",
            ):
                publish_manifest_local_dag_bundle(
                    bundle=bundle,
                    source_path=source,
                    expected_current_version=f"sha256-{'0' * 64}",
                )

            assert not bundle.manifest_ref_path.exists()

    def test_publish_rejects_file_added_during_publication(self, tmp_path, monkeypatch):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        real_materialize = local_bundle_module._materialize_local_manifest_snapshot

        def materialize_and_add_file(**kwargs):
            real_materialize(**kwargs)
            _write_file(source, "dags/added.py", "print('added')")

        monkeypatch.setattr(
            local_bundle_module,
            "_materialize_local_manifest_snapshot",
            materialize_and_add_file,
        )

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            with pytest.raises(BundleManifestSourceChangedError, match="changed while publishing"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not bundle.manifest_ref_path.exists()

    def test_publish_manifest_local_dag_bundle_rejects_published_root_inside_source(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(source / "published"))

            with pytest.raises(ValueError, match="published_root must not be inside source_path"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not bundle.manifest_ref_path.exists()

    def test_publish_manifest_local_dag_bundle_rejects_source_inside_published_root(self, tmp_path):
        published_root = tmp_path / "published"
        source = published_root / "refs" / "manifest-local"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))

            with pytest.raises(ValueError, match="source_path must not be inside published_root"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not bundle.manifest_ref_path.exists()

    def test_publish_manifest_local_dag_bundle_rejects_versions_dir_inside_source(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        storage_path = source / ".airflow-bundles"

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(storage_path)}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            with pytest.raises(ValueError, match="source_path and versions_dir must not overlap"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not bundle.manifest_ref_path.exists()
            assert not storage_path.exists()

    def test_publish_manifest_local_dag_bundle_rejects_source_inside_versions_dir(self, tmp_path):
        storage_path = tmp_path / "bundles"

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(storage_path)}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            source = bundle.versions_dir / "publisher-source"
            dag_file = _write_file(source, "dags/example.py", "print('dag')")

            with pytest.raises(ValueError, match="source_path and versions_dir must not overlap"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert dag_file.exists()
            assert not bundle.manifest_ref_path.exists()

    def test_publish_manifest_local_dag_bundle_fsyncs_versions_dir_before_ref_write(
        self, tmp_path, monkeypatch
    ):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        calls = []
        real_write_manifest_ref_atomically = local_bundle_module._write_manifest_ref_atomically

        def record_fsync_directory(path):
            calls.append(("fsync_directory", Path(path)))

        def record_write_manifest_ref_atomically(manifest_ref_path, ref_payload):
            calls.append(("write_manifest_ref", Path(manifest_ref_path)))
            real_write_manifest_ref_atomically(manifest_ref_path, ref_payload)

        monkeypatch.setattr(local_bundle_module, "_fsync_directory", record_fsync_directory)
        monkeypatch.setattr(
            local_bundle_module,
            "_write_manifest_ref_atomically",
            record_write_manifest_ref_atomically,
        )

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            versions_dir_fsync = ("fsync_directory", bundle.published_versions_dir)
            ref_write = ("write_manifest_ref", bundle.manifest_ref_path)
            assert versions_dir_fsync in calls
            assert ref_write in calls
            assert calls.index(versions_dir_fsync) < calls.index(ref_write)

    def test_publish_manifest_local_dag_bundle_cleans_read_only_temp_snapshot_on_failure(
        self, tmp_path, monkeypatch
    ):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        def fail_fsync_tree_directories(root):
            raise RuntimeError(f"failed to sync {root}")

        monkeypatch.setattr(local_bundle_module, "_fsync_tree_directories", fail_fsync_tree_directories)

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )

            with pytest.raises(RuntimeError, match="failed to sync"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not bundle.manifest_ref_path.exists()
            assert list(bundle.published_versions_dir.iterdir()) == []

    def test_publish_manifest_local_dag_bundle_removes_orphaned_temp_snapshots(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            bundle.published_versions_dir.mkdir(parents=True)
            orphan = bundle.published_versions_dir / f".sha256-{'1' * 64}.abandoned"
            _write_file(orphan, "partial.py", "print('partial')")
            local_bundle_module._set_snapshot_permissions(orphan)
            unrelated_hidden_dir = bundle.published_versions_dir / ".not-a-publisher-temp"
            unrelated_hidden_dir.mkdir()

            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not orphan.exists()
            assert unrelated_hidden_dir.exists()

    def test_source_changes_without_ref_update_do_not_affect_get_current_version(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            published_root = tmp_path / "published"
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))
            published = _publish_manifest_local_bundle(bundle, source)

            _write_file(source, "dags/example.py", "print('changed')")
            _write_file(source, "dags/new.py", "print('new')")
            fresh_bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))

            assert _version_string(fresh_bundle.get_current_version()) == published.version
            fresh_bundle.refresh()
            assert fresh_bundle.path == fresh_bundle.versions_dir / (published.version)
            assert (fresh_bundle.path / "dags/example.py").read_text() == "print('dag')"
            assert not (fresh_bundle.path / "dags/new.py").exists()

    def test_ref_update_changes_bundle_version_when_snapshot_exists(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            first = _publish_manifest_local_bundle(bundle, source)
            bundle.refresh()

            _write_file(source, "dags/example.py", "print('changed')")
            second = _publish_manifest_local_bundle(bundle, source)
            bundle.refresh()

            assert second.version != first.version
            assert _version_string(bundle.get_current_version()) == second.version
            assert (bundle.path / "dags/example.py").read_text() == "print('changed')"

    def test_ref_update_before_snapshot_materialization_raises_clear_error(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source, materialize_snapshot=False)

            with pytest.raises(BundleManifestError, match="not published"):
                bundle.refresh()

            assert not (bundle.versions_dir / (published.version)).exists()

    def test_pinned_bundle_uses_materialized_snapshot(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            published_root = tmp_path / "published"
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))
            published = _publish_manifest_local_bundle(bundle, source)
            pinned_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
                version=published.version,
            )
            pinned_bundle.initialize()

            assert _version_string(pinned_bundle.get_current_version()) == published.version
            assert pinned_bundle.path == bundle.versions_dir / (published.version)
            assert (pinned_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_pinned_bundle_rematerializes_deleted_cache(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            published_root = tmp_path / "published"
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))
            published = _publish_manifest_local_bundle(bundle, source)
            pinned_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
                version=published.version,
            )
            pinned_bundle.initialize()
            cached_version_path = pinned_bundle.path
            remove_bundle_tree_forcefully(cached_version_path)

            fresh_pinned_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
                version=published.version,
            )
            fresh_pinned_bundle.initialize()

            assert published.version_path.exists()
            assert (fresh_pinned_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_pinned_bundle_rebuilds_corrupt_cache(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            published_root = tmp_path / "published"
            bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(published_root))
            published = _publish_manifest_local_bundle(bundle, source)
            pinned_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
                version=published.version,
            )
            pinned_bundle.initialize()
            cached_dag = pinned_bundle.path / "dags/example.py"
            _add_write_bits(cached_dag)
            cached_dag.write_text("print('corrupt')")

            # A host whose validation state was invalidated must detect the corruption.
            ManifestLocalDagBundle._validated_version_paths.clear()
            pinned_bundle._validation_marker_path(published.version).unlink()
            fresh_pinned_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
                version=published.version,
            )
            fresh_pinned_bundle.initialize()

            assert cached_dag.read_text() == "print('dag')"
            assert (published.version_path / "dags/example.py").read_text() == "print('dag')"

    def test_refresh_skips_revalidation_of_validated_cache(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            _publish_manifest_local_bundle(bundle, source)
            bundle.refresh()

            with mock.patch.object(
                ManifestLocalDagBundle, "_validate_snapshot_files", autospec=True
            ) as mock_validate:
                bundle.refresh()
            mock_validate.assert_not_called()

    def test_validation_marker_lets_fresh_process_skip_revalidation(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            bundle.refresh()
            assert bundle._validation_marker_path(published.version).exists()

            # Simulate a fresh process (e.g. a task runner) on the same host.
            ManifestLocalDagBundle._validated_version_paths.clear()
            fresh_bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                version=published.version,
            )
            # The marker skips only the hashing pass; the structural pass still runs so
            # that truncated or mutated cache trees are detected and rebuilt.
            with mock.patch(
                "airflow_manifest_bundle.local.compute_file_sha256",
                side_effect=AssertionError("marker must skip the hashing pass"),
            ):
                fresh_bundle.initialize()
            assert (fresh_bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_cache_tree_and_marker_are_fsynced_in_durability_order(self, tmp_path, monkeypatch):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            cached_version_path = bundle.versions_dir / published.version
            marker_path = bundle._validation_marker_path(published.version)
            calls: list[tuple[str, Path]] = []
            real_fsync_file = local_bundle_module._fsync_file
            real_fsync_directory = local_bundle_module._fsync_directory
            real_replace = local_bundle_module.os.replace

            def record_fsync_file(path):
                calls.append(("fsync_file", Path(path)))
                real_fsync_file(path)

            def record_fsync_directory(path):
                calls.append(("fsync_directory", Path(path)))
                real_fsync_directory(path)

            def record_replace(source_path, destination_path):
                destination_path = Path(destination_path)
                if destination_path == cached_version_path:
                    calls.append(("replace_cache", destination_path))
                real_replace(source_path, destination_path)

            monkeypatch.setattr(local_bundle_module, "_fsync_file", record_fsync_file)
            monkeypatch.setattr(local_bundle_module, "_fsync_directory", record_fsync_directory)
            monkeypatch.setattr(local_bundle_module.os, "replace", record_replace)

            bundle.refresh()

            replace_index = calls.index(("replace_cache", cached_version_path))
            marker_fsync_index = calls.index(("fsync_file", marker_path))
            cache_file_fsync_indices = [
                index
                for index, (operation, path) in enumerate(calls)
                if operation == "fsync_file" and path != marker_path
            ]
            versions_dir_fsync_indices = [
                index
                for index, call in enumerate(calls)
                if call == ("fsync_directory", bundle.versions_dir)
            ]

            assert cache_file_fsync_indices
            assert max(cache_file_fsync_indices) < replace_index
            assert any(replace_index < index < marker_fsync_index for index in versions_dir_fsync_indices)
            assert any(index > marker_fsync_index for index in versions_dir_fsync_indices)
            assert marker_path.read_text() == published.version

    def test_refresh_survives_undeletable_orphan_temp_snapshot(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            bundle.versions_dir.mkdir(parents=True, exist_ok=True)
            orphan = bundle.versions_dir / f".sha256-{'a' * 64}.orphan"
            _write_file(orphan, "dags/leftover.py", "print('leftover')")

            with mock.patch(
                "airflow_manifest_bundle.local.remove_bundle_tree_forcefully",
                autospec=True,
                side_effect=PermissionError("operation not permitted"),
            ):
                bundle.refresh()

            assert orphan.exists()
            assert (bundle.versions_dir / (published.version)).is_dir()

    def test_publish_removes_orphaned_ref_temp_files(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            leftover = bundle.manifest_ref_path.parent / ".latest.json.abc123"
            leftover.parent.mkdir(parents=True, exist_ok=True)
            leftover.write_text("crashed publish leftover")

            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not leftover.exists()
            assert bundle.manifest_ref_path.exists()

    def test_publish_repairs_owned_restrictive_lock_file(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            # Simulate a publisher crash that left the lock file owner-only.
            bundle.publication_lock_path.parent.mkdir(parents=True, exist_ok=True)
            bundle.publication_lock_path.touch()
            bundle.publication_lock_path.chmod(0o600)

            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert stat.S_IMODE(bundle.publication_lock_path.stat().st_mode) == 0o644

    def test_publish_does_not_chmod_pre_existing_published_root(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        published_root = tmp_path / "published"
        published_root.mkdir(mode=0o750)

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(published_root),
            )
            publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

        assert stat.S_IMODE(published_root.stat().st_mode) == 0o750

    def test_refresh_sweeps_orphaned_temp_snapshots_from_cache(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            _publish_manifest_local_bundle(bundle, source)
            bundle.versions_dir.mkdir(parents=True, exist_ok=True)
            orphan = bundle.versions_dir / f".sha256-{'a' * 64}.orphan"
            _write_file(orphan, "dags/leftover.py", "print('leftover')")

            bundle.refresh()

            assert not orphan.exists()
            assert (bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_missing_pinned_snapshot_raises_clear_error(self, tmp_path):
        missing_version = f"sha256-{'0' * 64}"
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                version=missing_version,
            )

            with pytest.raises(BundleManifestError, match="is not published"):
                bundle.initialize()

    def test_refresh_rejects_missing_manifest_ref(self, tmp_path):
        bundle = ManifestLocalDagBundle(
            name="manifest-local",
            published_root=str(tmp_path / "published"),
        )

        with pytest.raises(BundleManifestError, match="manifest reference file"):
            bundle.refresh()

    def test_refresh_rejects_invalid_json_ref(self, tmp_path):
        bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(tmp_path / "published"))
        bundle.manifest_ref_path.parent.mkdir(parents=True)
        bundle.manifest_ref_path.write_text("{not-json")

        with pytest.raises(BundleManifestError, match="not valid JSON"):
            bundle.refresh()

    def test_json_reader_uses_utf8_encoding(self, tmp_path, monkeypatch):
        path = tmp_path / "payload.json"
        path.write_bytes('{"value": "valid UTF-8: ☃"}'.encode())
        real_read_text = Path.read_text

        def require_utf8(path, encoding=None, errors=None):
            assert encoding == "utf-8"
            return real_read_text(path, encoding=encoding, errors=errors)

        monkeypatch.setattr(Path, "read_text", require_utf8)

        assert ManifestLocalDagBundle._read_json_file(
            path,
            missing_message="missing",
            invalid_message="invalid",
        ) == {"value": "valid UTF-8: \u2603"}

    @pytest.mark.parametrize("entry_point", ["path", "get_current_version", "refresh", "initialize"])
    def test_entry_points_reject_non_utf8_manifest_ref(self, tmp_path, entry_point):
        bundle = ManifestLocalDagBundle(name="manifest-local", published_root=str(tmp_path / "published"))
        bundle.manifest_ref_path.parent.mkdir(parents=True)
        bundle.manifest_ref_path.write_bytes(b"\xff")

        with pytest.raises(BundleManifestError, match="not valid JSON") as excinfo:
            if entry_point == "path":
                _ = bundle.path
            else:
                getattr(bundle, entry_point)()

        assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)

    @pytest.mark.parametrize(
        ("mutate", "match"),
        [
            (lambda payload: payload.update({"schema_version": 2}), "unsupported schema_version"),
            (lambda payload: payload.update({"bundle_name": "other"}), "expected 'manifest-local'"),
            (lambda payload: payload.update({"version": None}), "does not contain a version"),
            (lambda payload: payload.update({"version": "../outside"}), "valid sha256 version"),
            (lambda payload: payload.update({"version": "sha256-bad"}), "valid sha256 version"),
            (
                lambda payload: payload.update({"version": f"sha256-{'A' * 64}"}),
                "valid sha256 version",
            ),
            (lambda payload: payload.update({"backend": {"type": "s3"}}), "not for a local backend"),
            (
                lambda payload: payload.update({"manifest": {"path": "../manifest.json", "sha256": "x"}}),
                "unsafe",
            ),
            (
                lambda payload: payload.update({"manifest": {"path": "/tmp/manifest.json", "sha256": "x"}}),
                "unsafe",
            ),
            (
                lambda payload: payload.update(
                    {"manifest": {"path": "nested/.airflow-bundle-manifest.json", "sha256": "x"}}
                ),
                "must point",
            ),
            (lambda payload: payload.update({"file_count": -1}), "valid file_count"),
            (lambda payload: payload.update({"total_size": -1}), "valid total_size"),
        ],
    )
    def test_refresh_rejects_invalid_ref_payload(self, tmp_path, mutate, match):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            payload = dict(published.ref_payload)
            payload["backend"] = dict(payload["backend"])
            payload["manifest"] = dict(payload["manifest"])
            mutate(payload)
            _write_manifest_ref(bundle.manifest_ref_path, payload)

            with pytest.raises(BundleManifestError, match=match):
                bundle.refresh()

    def test_pinned_bundle_rejects_invalid_version_before_path_lookup(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                version="../outside",
            )

            with pytest.raises(BundleManifestError, match="valid sha256 version"):
                bundle.initialize()

    @pytest.mark.parametrize(
        ("mutate", "match"),
        [
            (lambda manifest: manifest.update({"schema_version": 2}), "unsupported schema_version"),
            (lambda manifest: manifest.update({"bundle_name": "other"}), "expected 'manifest-local'"),
            (lambda manifest: manifest.update({"backend": {"type": "s3"}}), "not for a local backend"),
            (lambda manifest: manifest.update({"file_count": 99}), "file_count"),
            (lambda manifest: manifest.update({"total_size": 99}), "total_size"),
            (
                lambda manifest: manifest["files"].append(dict(manifest["files"][0])),
                "duplicate path",
            ),
            (
                lambda manifest: manifest["files"][0].update({"path": "../evil.py"}),
                "unsafe relative path",
            ),
            (
                lambda manifest: manifest["files"][0].update({"path": "dags//example.py"}),
                "unsafe relative path",
            ),
            (
                lambda manifest: manifest["files"][0].update({"path": "dags/./example.py"}),
                "unsafe relative path",
            ),
        ],
    )
    def test_refresh_rejects_corrupt_snapshot_manifest(self, tmp_path, mutate, match):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            manifest_path = published.version_path / MANIFEST_FILE_NAME
            manifest = json.loads(manifest_path.read_text())
            mutate(manifest)
            _add_write_bits(manifest_path)
            manifest_path.write_text(json.dumps(manifest))

            with pytest.raises(BundleManifestError, match=match):
                bundle.refresh()

    def test_refresh_rejects_corrupt_snapshot_manifest_json(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            manifest_path = published.version_path / MANIFEST_FILE_NAME
            _add_write_bits(manifest_path)
            manifest_path.write_text("{not-json")

            with pytest.raises(BundleManifestError, match="snapshot manifest is not valid JSON"):
                bundle.refresh()

    def test_refresh_rejects_missing_snapshot_manifest(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            manifest_path = published.version_path / MANIFEST_FILE_NAME
            _add_write_bits(manifest_path.parent)
            manifest_path.unlink()

            with pytest.raises(BundleManifestError, match=MANIFEST_FILE_NAME):
                bundle.refresh()

    def test_refresh_rejects_snapshot_manifest_digest_mismatch(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            payload = dict(published.ref_payload)
            payload["manifest"] = dict(payload["manifest"])
            payload["manifest"]["sha256"] = "sha256:bad"
            _write_manifest_ref(bundle.manifest_ref_path, payload)

            with pytest.raises(BundleManifestError, match="digest mismatch"):
                bundle.refresh()

    def test_refresh_rejects_snapshot_file_content_drift(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            _add_write_bits(published.version_path / "dags/example.py")
            _write_file(published.version_path, "dags/example.py", "print('drift')")

            with pytest.raises(BundleManifestError, match="does not match local snapshot manifest"):
                bundle.refresh()

    def test_refresh_rejects_unexpected_snapshot_files(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            _add_write_bits(published.version_path / "dags")
            _write_file(published.version_path, "dags/unexpected.py", "print('extra')")

            with pytest.raises(BundleManifestError, match="files not present in the manifest"):
                bundle.refresh()

    def test_refresh_rejects_symlinked_snapshot_directory(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        symlink_target = tmp_path / "outside"
        _write_file(symlink_target, "outside.py", "print('outside')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            snapshot_dags_dir = published.version_path / "dags"
            _add_write_bits(snapshot_dags_dir)
            _create_symlink_or_skip(
                symlink_target,
                snapshot_dags_dir / "linked",
                target_is_directory=True,
            )

            with pytest.raises(BundleManifestError, match="symlinked directory"):
                bundle.refresh()

    def test_refresh_rejects_symlinked_snapshot_file(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        symlink_target = tmp_path / "outside.py"
        symlink_target.write_text("print('outside')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            snapshot_dags_dir = published.version_path / "dags"
            _add_write_bits(snapshot_dags_dir)
            _create_symlink_or_skip(symlink_target, snapshot_dags_dir / "linked.py")

            with pytest.raises(BundleManifestError, match="symlinked file"):
                bundle.refresh()

    def test_refresh_rejects_non_regular_snapshot_file(self, tmp_path):
        if not hasattr(os, "mkfifo"):
            pytest.skip("mkfifo is not supported in this test environment")

        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            snapshot_dags_dir = published.version_path / "dags"
            _add_write_bits(snapshot_dags_dir)
            fifo_path = snapshot_dags_dir / "fifo.py"
            try:
                os.mkfifo(fifo_path)
            except OSError as e:
                pytest.skip(f"mkfifo is not supported in this test environment: {e}")

            with pytest.raises(BundleManifestError, match="non-regular file"):
                bundle.refresh()

    def test_snapshot_validation_rejects_walk_error(self, tmp_path, monkeypatch):
        snapshot_root = tmp_path / "snapshot"
        snapshot_root.mkdir()
        bundle = ManifestLocalDagBundle(
            name="manifest-local",
            published_root=str(tmp_path / "published"),
        )

        def fail_walk(*args, onerror, **kwargs):
            onerror(PermissionError(13, "Permission denied", str(snapshot_root / "unreadable")))

        monkeypatch.setattr(local_bundle_module.os, "walk", fail_walk)

        with pytest.raises(BundleManifestError, match="changed or became unreadable"):
            list(bundle._iter_snapshot_source_files(snapshot_root))

    def test_refresh_rejects_unexpected_snapshot_file_under_ignored_directory(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            published = _publish_manifest_local_bundle(bundle, source)
            snapshot_dags_dir = published.version_path / "dags"
            _add_write_bits(snapshot_dags_dir)
            _write_file(snapshot_dags_dir, "__pycache__/unexpected.py", "print('extra')")

            with pytest.raises(BundleManifestError, match="files not present in the manifest"):
                bundle.refresh()



class TestReviewRegressions:
    """Regression tests for the code-review fixes (stock-Airflow interaction hardening)."""

    def _published_bundle(self, tmp_path, **bundle_kwargs):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('dag')")
        bundle = ManifestLocalDagBundle(
            name="manifest-local",
            published_root=str(tmp_path / "published"),
            **bundle_kwargs,
        )
        published = _publish_manifest_local_bundle(bundle, source)
        return bundle, source, published

    def test_marker_does_not_certify_truncated_cache_tree(self, tmp_path):
        # Simulates stock stale cleanup's shutil.rmtree being interrupted partway:
        # version dir still exists (truncated), marker survives. The structural pass
        # behind the marker must detect this and rebuild.
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.refresh()
            cached_dag = bundle.versions_dir / published.version / "dags/example.py"
            cached_dag.unlink()

            ManifestLocalDagBundle._validated_version_paths.clear()
            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )
            fresh.refresh()
            assert cached_dag.read_text() == "print('dag')"

    def test_marker_does_not_certify_mutated_cache_tree(self, tmp_path):
        # Cache dirs are owner-writable (stock rmtree requirement), so DAG code could
        # inject files; the structural pass behind the marker must catch it.
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.refresh()
            injected = bundle.versions_dir / published.version / "dags/generated.py"
            injected.write_text("print('injected')")

            ManifestLocalDagBundle._validated_version_paths.clear()
            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )
            fresh.refresh()
            assert not injected.exists()

    def test_path_raises_airflow_exception_when_ref_missing(self, tmp_path):
        # Stock core reads bundle.path on uninitialized bundles; a raw FileNotFoundError
        # there crashes the dag processor loop instead of degrading per-bundle.
        from airflow.exceptions import AirflowException

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )
            with pytest.raises(BundleManifestError, match="manifest reference file") as excinfo:
                _ = bundle.path
            assert isinstance(excinfo.value, AirflowException)

    def test_path_falls_back_to_newest_cached_version_before_materialization(self, tmp_path):
        # A callback without bundle_version reads path before the new release is
        # materialized; serving the previous cached version beats losing the callback.
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source, first = self._published_bundle(tmp_path)
            bundle.refresh()
            _write_file(source, "dags/example.py", "print('dag v2')")
            second = _publish_manifest_local_bundle(bundle, source)
            assert second.version != first.version

            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )
            assert fresh.path == fresh.versions_dir / first.version

            fresh.refresh()
            assert fresh.path == fresh.versions_dir / second.version

    def test_path_rejects_truncated_current_cache(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.refresh()
            (bundle.versions_dir / published.version / "dags/example.py").unlink()

            ManifestLocalDagBundle._validated_version_paths.clear()
            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )

            with pytest.raises(BundleManifestError, match="structurally invalid cache copy"):
                _ = fresh.path

    def test_path_fallback_skips_structurally_invalid_newest_cache(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source, first = self._published_bundle(tmp_path)
            bundle.refresh()

            _write_file(source, "dags/example.py", "print('dag v2')")
            second = _publish_manifest_local_bundle(bundle, source)
            bundle.refresh()
            (bundle.versions_dir / second.version / "dags/example.py").unlink()

            os.utime(bundle._validation_marker_path(first.version), (1, 1))
            os.utime(bundle._validation_marker_path(second.version), (2, 2))
            ManifestLocalDagBundle._validated_version_paths.clear()

            _write_file(source, "dags/example.py", "print('dag v3')")
            third = _publish_manifest_local_bundle(bundle, source)
            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )

            assert fresh.path == fresh.versions_dir / first.version
            assert not (fresh.versions_dir / third.version).exists()

    def test_move_aside_removes_stock_tracking_file(self, tmp_path):
        # A tracking file pointing at a version dir the package removed would crash stock
        # stale cleanup (plain rmtree, only BlockingIOError caught) on every future sweep.
        from airflow.dag_processing.bundles.base import get_bundle_tracking_file

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.refresh()

            tracking_file = get_bundle_tracking_file(
                bundle_name=bundle.name, version=published.version
            )
            tracking_file.parent.mkdir(parents=True, exist_ok=True)
            tracking_file.write_text("2026-07-22T00:00:00+00:00")

            cached_dag = bundle.versions_dir / published.version / "dags/example.py"
            _add_write_bits(cached_dag)
            cached_dag.write_text("corrupted!!")
            bundle._validation_marker_path(published.version).unlink()
            ManifestLocalDagBundle._validated_version_paths.clear()

            fresh = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )
            fresh.refresh()
            assert not tracking_file.exists()
            assert (fresh.versions_dir / published.version / "dags/example.py").read_text() == "print('dag')"

    def test_publish_rejects_executable_bit_flip_before_snapshot_is_published(self, tmp_path, monkeypatch):
        # The exec bit is part of the content address; failing only after os.replace would
        # permanently poison the published snapshot for that content version.
        source = tmp_path / "source"
        dag_file = _write_file(source, "dags/example.py", "print('dag')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="manifest-local", published_root=str(tmp_path / "published")
            )

            real_copy2 = shutil.copy2

            def chmod_after_copy(src, dst, **kwargs):
                result = real_copy2(src, dst, **kwargs)
                if Path(src) == dag_file:
                    os.chmod(dst, 0o755)
                return result

            monkeypatch.setattr(local_bundle_module.shutil, "copy2", chmod_after_copy)
            with pytest.raises(BundleManifestSourceChangedError, match="source changed"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

            assert not any(bundle.published_versions_dir.glob("sha256-*"))
            assert not bundle.manifest_ref_path.exists()

    def test_orphaned_validation_markers_are_reaped(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.versions_dir.mkdir(parents=True, exist_ok=True)
            orphan_marker = bundle.versions_dir / f".sha256-{'b' * 64}.validated"
            orphan_marker.write_text(f"sha256-{'b' * 64}")

            bundle.refresh()
            assert not orphan_marker.exists()
            # The live version's marker must survive the sweep.
            assert bundle._validation_marker_path(published.version).exists()

    @pytest.mark.skipif(os.geteuid() == 0, reason="permission checks are bypassed as root")
    def test_pinned_initialize_wraps_permission_error(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _, published = self._published_bundle(tmp_path)
            bundle.refresh()

            ManifestLocalDagBundle._validated_version_paths.clear()
            pinned = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                version=published.version,
            )
            storage_root = tmp_path / "bundles"
            storage_root.chmod(0o000)
            try:
                with pytest.raises(BundleManifestError):
                    pinned.initialize()
            finally:
                storage_root.chmod(0o755)


class TestManifestLocalDagBundleAutoPublish:
    @pytest.fixture(autouse=True)
    def _clear_validated_version_paths(self):
        ManifestLocalDagBundle._validated_version_paths.clear()
        yield
        ManifestLocalDagBundle._validated_version_paths.clear()

    def _bundle(self, tmp_path, *, stability=0, source=None, **kwargs):
        source = source or tmp_path / "source"
        source.mkdir(parents=True, exist_ok=True)
        bundle = ManifestLocalDagBundle(
            name="manifest-local",
            published_root=str(tmp_path / "published"),
            source_path=str(source),
            source_stability_seconds=stability,
            **kwargs,
        )
        return bundle, source

    def test_source_stability_defaults_to_refresh_interval(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(
                tmp_path,
                stability=None,
                refresh_interval=42,
            )

        assert bundle.source_path == source
        assert bundle.source_stability_seconds == 42

    @pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), True, "30"])
    def test_rejects_invalid_source_stability_seconds(self, tmp_path, value):
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="source_stability_seconds"),
        ):
            ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                source_path=str(tmp_path / "source"),
                source_stability_seconds=value,
            )

    def test_rejects_non_boolean_allow_empty_source(self, tmp_path):
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="allow_empty_source"),
        ):
            ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                source_path=str(tmp_path / "source"),
                allow_empty_source="false",
            )

    def test_bootstrap_waits_for_elapsed_stability_time(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path, stability=30)
            _write_file(source, "dags/example.py", "print('dag')")

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                with pytest.raises(BundleManifestError, match="has not remained unchanged"):
                    bundle.initialize()
                assert not bundle.manifest_ref_path.exists()

                clock.return_value = 129
                with pytest.raises(BundleManifestError, match="has not remained unchanged"):
                    bundle.initialize()
                assert not bundle.manifest_ref_path.exists()

                clock.return_value = 130
                bundle.initialize()

            assert bundle.manifest_ref_path.is_file()
            assert (bundle.path / "dags/example.py").read_text() == "print('dag')"

    def test_fresh_instance_uses_shared_bootstrap_stability_candidate(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            first, source = self._bundle(tmp_path, stability=30)
            _write_file(source, "dags/example.py", "print('dag')")

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                with pytest.raises(BundleManifestError, match="has not remained unchanged"):
                    first.initialize()

                state_payload = json.loads(first.auto_publish_state_path.read_text())
                assert set(state_payload) == {
                    "schema_version",
                    "bundle_name",
                    "source_signature",
                    "first_observed_at",
                }
                assert (
                    state_payload["schema_version"]
                    == local_bundle_module.AUTO_PUBLISH_STATE_SCHEMA_VERSION
                )
                assert state_payload["bundle_name"] == first.name
                assert state_payload["source_signature"].startswith("sha256:")
                assert state_payload["first_observed_at"] == 100
                assert stat.S_IMODE(first.auto_publish_state_path.stat().st_mode) == 0o644
                assert stat.S_IMODE(first.auto_publish_state_path.parent.stat().st_mode) == 0o755

                second, _ = self._bundle(tmp_path, stability=30, source=source)
                clock.return_value = 130
                second.initialize()

            assert second.manifest_ref_path.is_file()
            assert (second.path / "dags/example.py").read_text() == "print('dag')"

    def test_different_instance_can_publish_shared_stable_candidate(self, tmp_path):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                first.refresh()
                assert _version_string(first.get_current_version()) == old.version

                clock.return_value = 130
                second.refresh()

            assert _version_string(second.get_current_version()) != old.version
            assert (second.path / "dags/example.py").read_text() == "print('new')"

    def test_immediate_refresh_after_initialize_does_not_satisfy_stability_window(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            publisher = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=publisher, source_path=source)
            _write_file(source, "dags/example.py", "print('new')")
            bundle, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                bundle.initialize()
                bundle.refresh()
                assert _version_string(bundle.get_current_version()) == old.version

                clock.return_value = 130
                bundle.refresh()

            assert _version_string(bundle.get_current_version()) != old.version
            assert (bundle.path / "dags/example.py").read_text() == "print('new')"

    def test_changed_candidate_restarts_stability_timer(self, tmp_path):
        source = tmp_path / "source"
        _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            publisher = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=publisher, source_path=source)
            bundle, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                _write_file(source, "dags/example.py", "print('candidate one')")
                bundle.refresh()

                clock.return_value = 120
                _write_file(source, "dags/example.py", "print('candidate two')")
                bundle.refresh()

                clock.return_value = 149
                bundle.refresh()
                assert _version_string(bundle.get_current_version()) == old.version

                clock.return_value = 150
                bundle.refresh()

            assert _version_string(bundle.get_current_version()) != old.version
            assert (bundle.path / "dags/example.py").read_text() == "print('candidate two')"

    def test_different_instances_share_candidate_reset(self, tmp_path):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)
            third, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                source_file.write_text("print('candidate one')")
                first.refresh()

                clock.return_value = 120
                source_file.write_text("print('candidate two')")
                second.refresh()

                clock.return_value = 149
                first.refresh()
                assert _version_string(first.get_current_version()) == old.version

                clock.return_value = 150
                third.refresh()

            assert _version_string(third.get_current_version()) != old.version
            assert (third.path / "dags/example.py").read_text() == "print('candidate two')"

    def test_revert_to_confirmed_source_resets_shared_candidate(self, tmp_path):
        source_a = tmp_path / "source-a"
        source_b = tmp_path / "source-b"
        source_link = tmp_path / "source"
        _write_file(source_a, "dags/example.py", "print('source a')")
        _write_file(source_b, "dags/example.py", "print('source b')")
        _create_symlink_or_skip(source_a, source_link, target_is_directory=True)

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _ = self._bundle(tmp_path, stability=30, source=source_link)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                with pytest.raises(BundleManifestError, match="has not remained unchanged"):
                    bundle.initialize()

                clock.return_value = 130
                bundle.initialize()
                confirmed_version = _version_string(bundle.get_current_version())

                source_link.unlink()
                _create_symlink_or_skip(source_b, source_link, target_is_directory=True)
                clock.return_value = 140
                bundle.refresh()

                source_link.unlink()
                _create_symlink_or_skip(source_a, source_link, target_is_directory=True)
                clock.return_value = 150
                bundle.refresh()
                reverted_state = json.loads(bundle.auto_publish_state_path.read_text())
                assert reverted_state["first_observed_at"] == 150

                source_link.unlink()
                _create_symlink_or_skip(source_b, source_link, target_is_directory=True)
                clock.return_value = 170
                bundle.refresh()

            candidate_state = json.loads(bundle.auto_publish_state_path.read_text())
            assert candidate_state["first_observed_at"] == 170
            assert _version_string(bundle.get_current_version()) == confirmed_version

    def test_delayed_observation_cannot_overwrite_newer_candidate(self, tmp_path):
        source_a = tmp_path / "source-a"
        source_b = tmp_path / "source-b"
        source_link = tmp_path / "source"
        _write_file(source_a, "dags/example.py", "print('source a')")
        _write_file(source_b, "dags/example.py", "print('source b')")
        _create_symlink_or_skip(source_a, source_link, target_is_directory=True)

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _ = self._bundle(tmp_path, stability=30, source=source_link)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                with pytest.raises(BundleManifestError, match="has not remained unchanged"):
                    bundle.initialize()
                original_state = json.loads(bundle.auto_publish_state_path.read_text())

                source_link.unlink()
                _create_symlink_or_skip(source_b, source_link, target_is_directory=True)
                delayed_signature = local_bundle_module.collect_bundle_source_snapshot(
                    source_link
                ).signature

                source_link.unlink()
                _create_symlink_or_skip(source_a, source_link, target_is_directory=True)
                clock.return_value = 110
                with pytest.raises(BundleManifestSourceChangedError, match="candidate was recorded"):
                    bundle._auto_publish_candidate_is_ready(delayed_signature)

            assert json.loads(bundle.auto_publish_state_path.read_text()) == original_state

    def test_missing_reference_is_restored_from_confirmed_source(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            _write_file(source, "dags/example.py", "print('dag')")
            bundle.initialize()
            version = _version_string(bundle.get_current_version())

            bundle.manifest_ref_path.unlink()
            bundle.refresh()

            assert json.loads(bundle.manifest_ref_path.read_text())["version"] == version
            assert _version_string(bundle.get_current_version()) == version

    def test_missing_published_snapshot_is_restored_from_confirmed_source(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            _write_file(source, "dags/example.py", "print('dag')")
            bundle.initialize()
            version = _version_string(bundle.get_current_version())
            published_version_path = bundle.published_versions_dir / version

            remove_bundle_tree_forcefully(published_version_path)
            bundle.refresh()

            assert published_version_path.is_dir()
            assert json.loads(bundle.manifest_ref_path.read_text())["version"] == version

    def test_publish_failure_keeps_serving_current_release(self, tmp_path, caplog):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            _write_file(source, "dags/example.py", "print('old')")
            bundle.initialize()
            version = _version_string(bundle.get_current_version())
            _write_file(source, "dags/example.py", "print('new')")

            with (
                caplog.at_level("WARNING", logger="airflow_manifest_bundle.local"),
                mock.patch.object(
                    local_bundle_module,
                    "_publish_manifest_result",
                    side_effect=BundleManifestError("publish failed"),
                ),
            ):
                bundle.refresh()

            assert "keeping the current release" in caplog.text
            assert _version_string(bundle.get_current_version()) == version
            assert (bundle.path / "dags/example.py").read_text() == "print('old')"

    def test_another_instance_retries_failed_ready_candidate_without_delay(self, tmp_path):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)
            third, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                first.refresh()

                clock.return_value = 130
                with mock.patch.object(
                    local_bundle_module,
                    "_publish_manifest_result",
                    side_effect=BundleManifestError("publish failed"),
                ):
                    second.refresh()
                assert _version_string(second.get_current_version()) == old.version

                clock.return_value = 131
                third.refresh()

            assert _version_string(third.get_current_version()) != old.version
            assert (third.path / "dags/example.py").read_text() == "print('new')"

    def test_candidate_is_rechecked_before_publication(self, tmp_path):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)
            build_manifest = local_bundle_module.build_bundle_version_manifest_result

            def build_manifest_then_replace_candidate(**kwargs):
                result = build_manifest(**kwargs)
                state_payload = json.loads(second.auto_publish_state_path.read_text())
                state_payload["source_signature"] = "sha256:another-candidate"
                second.auto_publish_state_path.write_text(json.dumps(state_payload))
                return result

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                first.refresh()

                clock.return_value = 130
                with mock.patch.object(
                    local_bundle_module,
                    "build_bundle_version_manifest_result",
                    side_effect=build_manifest_then_replace_candidate,
                ):
                    second.refresh()

            assert _version_string(second.get_current_version()) == old.version
            assert json.loads(second.auto_publish_state_path.read_text())["source_signature"] == (
                "sha256:another-candidate"
            )

    @pytest.mark.parametrize("state_content", ["{", "{}"])
    def test_malformed_shared_state_restarts_stability_period(self, tmp_path, state_content):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=100) as clock:
                first.refresh()
                first.auto_publish_state_path.write_text(state_content)

                clock.return_value = 130
                second.refresh()

            state_payload = json.loads(second.auto_publish_state_path.read_text())
            assert state_payload["first_observed_at"] == 130
            assert _version_string(second.get_current_version()) == old.version

    def test_unsupported_shared_state_schema_keeps_current_release(self, tmp_path, caplog):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            bundle, _ = self._bundle(tmp_path, stability=30, source=source)
            bundle.auto_publish_state_path.parent.mkdir(parents=True)
            bundle.auto_publish_state_path.write_text(
                json.dumps(
                    {
                        "schema_version": local_bundle_module.AUTO_PUBLISH_STATE_SCHEMA_VERSION + 1,
                        "bundle_name": bundle.name,
                        "source_signature": "sha256:future",
                        "first_observed_at": 100,
                    }
                )
            )

            with (
                caplog.at_level("WARNING", logger="airflow_manifest_bundle.local"),
                mock.patch.object(local_bundle_module.time, "time", return_value=130),
            ):
                bundle.refresh()

            assert "unsupported schema_version" in caplog.text
            assert json.loads(bundle.auto_publish_state_path.read_text())["schema_version"] == 2
            assert _version_string(bundle.get_current_version()) == old.version

    def test_future_shared_timestamp_does_not_publish_early(self, tmp_path, caplog):
        source = tmp_path / "source"
        source_file = _write_file(source, "dags/example.py", "print('old')")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            explicit = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
            )
            old = publish_manifest_local_dag_bundle(bundle=explicit, source_path=source)
            source_file.write_text("print('new')")
            first, _ = self._bundle(tmp_path, stability=30, source=source)
            second, _ = self._bundle(tmp_path, stability=30, source=source)

            with mock.patch.object(local_bundle_module.time, "time", return_value=200):
                first.refresh()

            with (
                caplog.at_level("WARNING", logger="airflow_manifest_bundle.local"),
                mock.patch.object(local_bundle_module.time, "time", return_value=130),
            ):
                second.refresh()

            assert "timestamp is in the future" in caplog.text
            assert _version_string(second.get_current_version()) == old.version
            assert json.loads(second.auto_publish_state_path.read_text())["first_observed_at"] == 200

    def test_source_change_during_publication_keeps_serving_current_release(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            source_file = _write_file(source, "dags/example.py", "print('old')")
            bundle.initialize()
            version = _version_string(bundle.get_current_version())
            source_file.write_text("print('candidate')")
            materialize = local_bundle_module._materialize_local_manifest_snapshot

            def materialize_then_change_source(**kwargs):
                materialize(**kwargs)
                source_file.write_text("print('changed again')")

            with mock.patch.object(
                local_bundle_module,
                "_materialize_local_manifest_snapshot",
                side_effect=materialize_then_change_source,
            ):
                bundle.refresh()

            assert _version_string(bundle.get_current_version()) == version
            assert json.loads(bundle.manifest_ref_path.read_text())["version"] == version
            assert (bundle.path / "dags/example.py").read_text() == "print('old')"

    def test_confirmed_source_uses_metadata_only_until_it_changes(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            source_file = _write_file(source, "dags/example.py", "print('dag')")
            bundle.initialize()
            source_stat = source_file.stat()

            with mock.patch.object(
                manifest_module,
                "compute_file_sha256",
                wraps=manifest_module.compute_file_sha256,
            ) as compute_sha256:
                bundle.refresh()
                assert compute_sha256.call_count == 0

                os.utime(
                    source_file,
                    ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns + 1_000_000_000),
                )
                bundle.refresh()
                assert compute_sha256.call_count == 1

                bundle.refresh()
                assert compute_sha256.call_count == 1

    def test_pinned_bundle_never_publishes_dirty_source(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, source = self._bundle(tmp_path)
            _write_file(source, "dags/example.py", "print('old')")
            bundle.initialize()
            version = _version_string(bundle.get_current_version())
            _write_file(source, "dags/new.py", "print('new')")

            pinned = ManifestLocalDagBundle(
                name="manifest-local",
                published_root=str(tmp_path / "published"),
                source_path=str(source),
                source_stability_seconds=0,
                version=version,
            )
            with mock.patch.object(
                ManifestLocalDagBundle,
                "_maybe_publish_from_source",
                autospec=True,
            ) as publish:
                pinned.initialize()

            publish.assert_not_called()
            assert not (pinned.path / "dags/new.py").exists()
            assert not pinned.auto_publish_state_path.exists()

    def test_empty_source_requires_explicit_opt_in(self, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _ = self._bundle(tmp_path)
            with pytest.raises(BundleManifestError, match="Refusing to auto-publish empty"):
                bundle.initialize()
            assert not bundle.manifest_ref_path.exists()

            allowed, _ = self._bundle(
                tmp_path,
                source=tmp_path / "allowed-source",
                allow_empty_source=True,
            )
            allowed.initialize()
            assert json.loads(allowed.manifest_ref_path.read_text())["file_count"] == 0

    def test_retargeted_source_overlap_keeps_current_release(self, tmp_path, caplog):
        real_source = tmp_path / "real-source"
        source_link = tmp_path / "source-link"
        _write_file(real_source, "dags/example.py", "print('dag')")
        _create_symlink_or_skip(real_source, source_link, target_is_directory=True)

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, _ = self._bundle(tmp_path, source=source_link)
            bundle.initialize()
            version = _version_string(bundle.get_current_version())

            source_link.unlink()
            _create_symlink_or_skip(
                bundle.published_root,
                source_link,
                target_is_directory=True,
            )
            with caplog.at_level("WARNING", logger="airflow_manifest_bundle.local"):
                bundle.refresh()

            assert "keeping the current release" in caplog.text
            assert _version_string(bundle.get_current_version()) == version
            assert json.loads(bundle.manifest_ref_path.read_text())["version"] == version
