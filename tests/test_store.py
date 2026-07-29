"""
Contract tests for ``ArtifactStore`` implementations.

Every implementation must satisfy these behaviors; the tests talk only to the
interface. When another backend lands (for example an S3 store), add it to the
``store`` fixture parametrization instead of writing a parallel suite.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from _test_utils import conf_vars

from airflow_manifest_bundle import s3_store as s3_store_module
from airflow_manifest_bundle.bundle import FilesystemArtifactStore
from airflow_manifest_bundle.local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestNotFoundError,
    build_bundle_version_manifest_result,
    serialize_bundle_version_manifest,
)
from airflow_manifest_bundle.s3_store import S3ArtifactStore, parse_s3_published_root

BUNDLE_NAME = "contract"
BUCKET = "dag-bucket"


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


class FakeStoreClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code, "Message": code}}


class FakeStoreS3Client:
    """Just enough of the S3 API for the artifact store's read path."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self.objects[key] = body

    def get_object(self, *, Bucket: str, Key: str):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise FakeStoreClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key]), "ETag": '"fake-etag"'}

    def head_object(self, *, Bucket: str, Key: str):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise FakeStoreClientError("404")
        return {"ContentLength": len(self.objects[Key]), "ETag": '"fake-etag"'}


@pytest.fixture
def fake_s3(monkeypatch):
    client = FakeStoreS3Client()
    seen_conn_ids: list[str] = []

    class FakeHook:
        default_conn_name = "aws_default"

        def __init__(self, *, aws_conn_id: str) -> None:
            seen_conn_ids.append(aws_conn_id)

        def get_conn(self):
            return client

    monkeypatch.setattr(s3_store_module, "S3Hook", FakeHook)
    client.seen_conn_ids = seen_conn_ids
    return client


def _publish_to_fake_s3(client: FakeStoreS3Client, *, prefix: str, bundle_name: str, source: Path):
    """Populate the fake object store the way a (future) publisher would: manifest last."""
    result = build_bundle_version_manifest_result(
        bundle_name=bundle_name,
        root=source,
        backend_type="local",
    )
    base = f"{prefix}/versions/{bundle_name}/{result.version}" if prefix else (
        f"versions/{bundle_name}/{result.version}"
    )
    for file_info in result.manifest["files"]:
        client.put(f"{base}/{file_info['path']}", (source / file_info["path"]).read_bytes())
    client.put(f"{base}/{MANIFEST_FILE_NAME}", serialize_bundle_version_manifest(result.manifest))
    ref_key = f"{prefix}/refs/{bundle_name}/latest.json" if prefix else f"refs/{bundle_name}/latest.json"
    client.put(ref_key, serialize_bundle_version_manifest(result.ref_payload))
    return result


def _s3_artifact_store(prefix: str = "releases") -> S3ArtifactStore:
    root = f"s3://{BUCKET}/{prefix}" if prefix else f"s3://{BUCKET}"
    return S3ArtifactStore(bundle_name=BUNDLE_NAME, published_root=root)


class TestParsePublishedRoot:
    def test_bucket_and_prefix(self):
        assert parse_s3_published_root("s3://bucket/some/prefix/") == ("bucket", "some/prefix")

    def test_bucket_only(self):
        assert parse_s3_published_root("s3://bucket") == ("bucket", "")

    @pytest.mark.parametrize(
        "url",
        ["http://bucket/prefix", "s3://", "s3:///prefix", "s3://bucket/prefix?versionId=1"],
    )
    def test_rejects_invalid_urls(self, url):
        with pytest.raises(TypeError):
            parse_s3_published_root(url)


class TestS3StoreReadPath:
    def test_requires_amazon_provider(self, monkeypatch):
        monkeypatch.setattr(s3_store_module, "S3Hook", None)
        with pytest.raises(TypeError, match="requires the Amazon provider"):
            _s3_artifact_store()

    def test_read_ref_missing_and_invalid(self, fake_s3):
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestNotFoundError, match="ref is gone"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")
        fake_s3.put(f"releases/refs/{BUNDLE_NAME}/latest.json", b"{not json")
        with pytest.raises(BundleManifestError, match="ref is invalid"):
            store.read_ref(missing_message="ref is gone", invalid_message="ref is invalid")

    def test_snapshot_exists_requires_the_manifest_object(self, fake_s3, tmp_path):
        store = _s3_artifact_store()
        source = _write_source(tmp_path / "source")
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name=BUNDLE_NAME, source=source)
        assert store.snapshot_exists(result.version) is True

        # Without the manifest object the version prefix is an uncommitted upload.
        fake_s3.objects.pop(
            f"releases/versions/{BUNDLE_NAME}/{result.version}/{MANIFEST_FILE_NAME}"
        )
        assert store.snapshot_exists(result.version) is False

    def test_fetch_snapshot_downloads_verifies_and_applies_modes(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        script = source / "dags" / "task.sh"
        script.write_text("#!/bin/sh\n")
        script.chmod(0o755)
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name=BUNDLE_NAME, source=source)

        store = _s3_artifact_store()
        destination = tmp_path / "dest"
        destination.mkdir()
        store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)

        assert (destination / "dags" / "example.py").read_text() == "print('dag')\n"
        assert (destination / "dags" / "task.sh").stat().st_mode & 0o111
        assert not (destination / "dags" / "example.py").stat().st_mode & 0o111
        embedded = json.loads((destination / MANIFEST_FILE_NAME).read_text())
        assert embedded["version"] == result.version

    def test_fetch_snapshot_missing_manifest_raises_not_found(self, fake_s3, tmp_path):
        store = _s3_artifact_store()
        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestNotFoundError, match="is not published"):
            store.fetch_snapshot(
                "sha256-" + "0" * 64, destination, structural_validator=lambda tree: None
            )

    def test_fetch_snapshot_rejects_corrupted_object(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name=BUNDLE_NAME, source=source)
        key = f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py"
        fake_s3.put(key, b"tampered")

        store = _s3_artifact_store()
        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestError, match="does not match the snapshot manifest"):
            store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)

    def test_fetch_snapshot_missing_file_object(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name=BUNDLE_NAME, source=source)
        fake_s3.objects.pop(f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py")

        store = _s3_artifact_store()
        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestError, match="is missing manifest entry"):
            store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)

    def test_fetch_snapshot_rejects_unsafe_manifest_paths(self, fake_s3, tmp_path):
        version = "sha256-" + "0" * 64
        evil_manifest = {
            "files": [{"path": "../evil.py", "sha256": "0" * 64, "size": 1, "executable": False}],
        }
        fake_s3.put(
            f"releases/versions/{BUNDLE_NAME}/{version}/{MANIFEST_FILE_NAME}",
            json.dumps(evil_manifest).encode(),
        )
        store = _s3_artifact_store()
        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestError, match="unsafe relative path"):
            store.fetch_snapshot(version, destination, structural_validator=lambda tree: None)
        assert not (tmp_path / "evil.py").exists()

    def test_publication_operations_are_unsupported(self, fake_s3, tmp_path):
        store = _s3_artifact_store()
        operations = [
            lambda: store.write_ref({}),
            lambda: store.write_state({}),
            lambda: store.publication_guard(),
            lambda: store.prepare_publish_areas(),
            lambda: store.prepare_state_area(),
            lambda: store.validate_source_paths(tmp_path, cache_versions_dir=tmp_path / "v"),
            lambda: store.publish_snapshot(
                "sha256-" + "0" * 64,
                manifest={},
                source_root=tmp_path,
                validate_existing=lambda tree: None,
            ),
            lambda: store.sweep_publish_temps(),
        ]
        for operation in operations:
            with pytest.raises(BundleManifestError, match="does not support publication"):
                operation()

    def test_locators_are_urls_scoped_to_the_root(self, fake_s3):
        store = _s3_artifact_store()
        version = "sha256-" + "a" * 64
        assert store.root == f"s3://{BUCKET}/releases"
        assert store.ref_path == f"s3://{BUCKET}/releases/refs/{BUNDLE_NAME}/latest.json"
        assert store.snapshot_path(version).endswith(f"/versions/{BUNDLE_NAME}/{version}")
        assert str(store.state_path).startswith(store.root)


def _version_string(version) -> str:
    return getattr(version, "version", version)


class TestBundleWithObjectStoreRoot:
    def test_consume_only_refresh_and_parse_path(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
            )
            bundle.initialize()
            assert _version_string(bundle.get_current_version()) == result.version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('dag')\n"

    def test_pinned_initialize_materializes_from_object_store(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                version=result.version,
            )
            bundle.initialize()
            assert (bundle.path / "dags" / "example.py").read_text() == "print('dag')\n"

    def test_source_path_rejected_with_object_store_root(self, fake_s3, tmp_path):
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="consume-only"),
        ):
            ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                source_path=str(tmp_path / "source"),
            )

    def test_s3_bundle_auto_publish_rejected_with_object_store_root(self, fake_s3, tmp_path):
        from airflow_manifest_bundle.s3 import ManifestS3DagBundle

        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            with pytest.raises(TypeError, match="consume-only"):
                ManifestS3DagBundle(
                    name="my-dags",
                    bucket_name="source-bucket",
                    published_root=f"s3://{BUCKET}/releases",
                )
            bundle = ManifestS3DagBundle(
                name="my-dags",
                bucket_name="source-bucket",
                published_root=f"s3://{BUCKET}/releases",
                auto_publish=False,
            )
            assert bundle._has_publish_source is False

    def test_explicit_publish_rejected_with_object_store_root(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
            )
            with pytest.raises(BundleManifestError, match="does not support publication"):
                publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)

    def test_published_root_conn_id_reaches_the_hook(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                published_root_conn_id="aws_publishing",
            )
            bundle.refresh()
            assert fake_s3.seen_conn_ids == ["aws_publishing"]

    def test_conn_id_rejected_for_filesystem_root(self, tmp_path):
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="only valid with an object-store"),
        ):
            ManifestLocalDagBundle(
                name="my-dags",
                published_root=str(tmp_path / "published"),
                published_root_conn_id="aws_publishing",
            )
