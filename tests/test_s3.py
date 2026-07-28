from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from _test_utils import conf_vars

from airflow_manifest_bundle import ManifestDagBundleBase
from airflow_manifest_bundle import s3 as s3_module
from airflow_manifest_bundle.local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestNotFoundError,
    BundleManifestSourceChangedError,
)
from airflow_manifest_bundle.s3 import ManifestS3DagBundle


class FakeS3Client:
    def __init__(self, *, endpoint_url: str = "https://s3.example.test") -> None:
        self.meta = SimpleNamespace(endpoint_url=endpoint_url)
        self.objects: dict[str, dict[str, object]] = {}
        self.downloads: list[str] = []
        self.list_calls = 0
        self.bucket_exists = True
        self.bucket_error: Exception | None = None
        self.fail_download = False
        self.mutate_on_list: tuple[int, str, bytes, str] | None = None
        self.mutate_after_download: tuple[str, bytes, str] | None = None

    def put(self, key: str, body: bytes, *, etag: str | None = None) -> None:
        self.objects[key] = {
            "body": body,
            "etag": etag or f'"etag-{len(body)}-{body[:8].hex()}"',
            "last_modified": f"2026-07-28T12:00:{len(self.objects):02d}+00:00",
        }

    def delete(self, key: str) -> None:
        del self.objects[key]

    def get_paginator(self, operation: str):
        assert operation == "list_objects_v2"
        return self

    def head_object(self, *, Bucket: str, Key: str):
        assert Bucket == "dag-bucket"
        try:
            data = self.objects[Key]
        except KeyError as e:
            raise FileNotFoundError(Key) from e
        return {
            "ContentLength": len(data["body"]),
            "ETag": data["etag"],
            "LastModified": data["last_modified"],
        }

    def head_bucket(self, *, Bucket: str):
        assert Bucket == "dag-bucket"
        if self.bucket_error is not None:
            raise self.bucket_error
        if not self.bucket_exists:
            raise FileNotFoundError(Bucket)
        return {}

    def paginate(self, *, Bucket: str, Prefix: str):
        assert Bucket == "dag-bucket"
        self.list_calls += 1
        mutation = self.mutate_on_list
        if mutation is not None and mutation[0] == self.list_calls:
            _, key, body, etag = mutation
            self.put(key, body, etag=etag)
            self.mutate_on_list = None
        contents = []
        for key, data in sorted(self.objects.items()):
            if not key.startswith(Prefix):
                continue
            contents.append(
                {
                    "Key": key,
                    "Size": len(data["body"]),
                    "ETag": data["etag"],
                    "LastModified": data["last_modified"],
                }
            )
        return [{"Contents": contents}]

    def download_file(self, bucket_name: str, key: str, destination: str) -> None:
        assert bucket_name == "dag-bucket"
        if self.fail_download:
            raise RuntimeError("download failed")
        Path(destination).write_bytes(self.objects[key]["body"])
        self.downloads.append(key)
        mutation = self.mutate_after_download
        if mutation is not None:
            changed_key, body, etag = mutation
            self.put(changed_key, body, etag=etag)
            self.mutate_after_download = None


def _install_fake_hook(monkeypatch, client: FakeS3Client, *, bucket_exists: bool = True):
    constructed: list[str] = []
    client.bucket_exists = bucket_exists

    class FakeS3Hook:
        def __init__(self, *, aws_conn_id: str) -> None:
            constructed.append(aws_conn_id)
            self.region_name = "us-east-2"

        def get_conn(self):
            return client

        def check_for_prefix(self, *, bucket_name: str, prefix: str, delimiter: str) -> bool:
            assert bucket_name == "dag-bucket"
            assert delimiter == "/"
            return any(key.startswith(prefix) for key in client.objects)

    monkeypatch.setattr(s3_module, "S3Hook", FakeS3Hook)
    return constructed


def _bundle(tmp_path: Path, **kwargs) -> ManifestS3DagBundle:
    return ManifestS3DagBundle(
        name="manifest-s3",
        bucket_name="dag-bucket",
        prefix="dags/",
        published_root=str(tmp_path / "published"),
        source_stability_seconds=0,
        **kwargs,
    )


def _version_string(version) -> str:
    return getattr(version, "version", version)


@pytest.fixture(autouse=True)
def _clear_validated_version_paths():
    ManifestDagBundleBase._validated_version_paths.clear()
    yield
    ManifestDagBundleBase._validated_version_paths.clear()


def test_common_base_and_concrete_bundles_are_siblings():
    assert inspect.isabstract(ManifestDagBundleBase)
    assert ManifestLocalDagBundle.__bases__ == (ManifestDagBundleBase,)
    assert ManifestS3DagBundle.__bases__ == (ManifestDagBundleBase,)


def test_constructor_matches_oss_defaults_and_constructs_hook_lazily(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"print('dag')")
    constructed = _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        custom = _bundle(tmp_path, aws_conn_id="custom")

        assert bundle.aws_conn_id == "aws_default"
        assert bundle.bucket_name == "dag-bucket"
        assert bundle.prefix == "dags/"
        assert bundle.max_file_count == s3_module.DEFAULT_MAX_FILE_COUNT
        assert bundle.max_file_size_bytes == s3_module.DEFAULT_MAX_FILE_SIZE_BYTES
        assert bundle.max_total_size_bytes == s3_module.DEFAULT_MAX_TOTAL_SIZE_BYTES
        assert bundle.s3_dags_dir == bundle.base_dir / "_s3_source"
        assert bundle.s3_dags_dir != bundle.versions_dir
        assert constructed == []
        assert custom.s3_hook is not None
        assert constructed == ["custom"]
        assert custom.view_url_template() == "https://dag-bucket.s3.us-east-2.amazonaws.com/dags/"


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("max_file_count", 0),
        ("max_file_count", True),
        ("max_file_size_bytes", -1),
        ("max_total_size_bytes", 1.5),
    ],
)
def test_constructor_rejects_invalid_source_limits(tmp_path, option, value):
    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(TypeError, match=option),
    ):
        _bundle(tmp_path, **{option: value})


def test_missing_extra_fails_only_when_unpinned_source_is_used(tmp_path, monkeypatch):
    monkeypatch.setattr(s3_module, "S3Hook", None)
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with pytest.raises(BundleManifestError, match=r"airflow-manifest-bundle\[s3\]"):
            bundle.refresh()


def test_first_refresh_downloads_and_publishes_local_artifact(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"print('dag')")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())

        assert client.downloads == ["dags/example.py"]
        assert bundle.path == bundle.versions_dir / version
        assert (bundle.path / "example.py").read_bytes() == b"print('dag')"
        ref = json.loads(bundle.manifest_ref_path.read_text())
        assert ref["source"]["type"] == "s3"
        assert ref["source"]["identity"].startswith("sha256:")
        assert ref["source"]["observation"].startswith("sha256:")
        assert (bundle.published_versions_dir / version / MANIFEST_FILE_NAME).is_file()


def test_publish_boundary_acquires_bundle_lock_once(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"print('dag')")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with mock.patch.object(bundle, "lock", wraps=bundle.lock) as bundle_lock:
            manifest_ref = bundle._publish_from_source_if_ready(None)

        assert manifest_ref is not None
        assert bundle_lock.call_count == 1


def test_sync_normalizes_mirror_directories_once_before_and_after_downloads(
    tmp_path,
    monkeypatch,
):
    client = FakeS3Client()
    client.put("dags/one/example.py", b"one")
    client.put("dags/two/example.py", b"two")
    client.put("dags/three/example.py", b"three")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with mock.patch.object(
            bundle,
            "_normalize_mirror_directories",
            wraps=bundle._normalize_mirror_directories,
        ) as normalize_directories:
            bundle.refresh()

        assert normalize_directories.call_count == 2
        for directory in (bundle.s3_dags_dir, *(bundle.s3_dags_dir.iterdir())):
            assert directory.stat().st_mode & 0o777 == 0o755


def test_unchanged_inventory_reuses_mirror_and_release(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"print('dag')")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.downloads.clear()
        with mock.patch.object(
            s3_module,
            "compute_file_sha256",
            side_effect=AssertionError("unchanged mirror must not be rehashed"),
        ):
            bundle.refresh()

        assert client.downloads == []
        assert _version_string(bundle.get_current_version()) == first_version


def test_changed_same_size_object_and_deleted_object_repair_mirror(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"old", etag='"old"')
    client.put("dags/deleted.py", b"delete")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.downloads.clear()
        client.put("dags/example.py", b"new", etag='"new"')
        client.delete("dags/deleted.py")
        bundle.refresh()

        assert client.downloads == ["dags/example.py"]
        assert (bundle.s3_dags_dir / "example.py").read_bytes() == b"new"
        assert not (bundle.s3_dags_dir / "deleted.py").exists()
        assert _version_string(bundle.get_current_version()) != first_version


def test_corrupt_mirror_and_malformed_state_force_repair(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"remote")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        client.downloads.clear()
        (bundle.s3_dags_dir / "example.py").write_bytes(b"broken")
        bundle.s3_mirror_state_path.write_text("{")
        bundle.refresh()

        assert client.downloads == ["dags/example.py"]
        assert (bundle.s3_dags_dir / "example.py").read_bytes() == b"remote"


def test_marker_change_repairs_same_size_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"remote", etag='"object-v1"')
    client.put("dags/.ready", b"release-1", etag='"marker-v1"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        mirror_file = bundle.s3_dags_dir / "example.py"
        original_stat = mirror_file.stat()

        mirror_file.write_bytes(b"broken")
        os.utime(
            mirror_file,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        client.put("dags/.ready", b"release-2", etag='"marker-v2"')
        client.downloads.clear()
        bundle.refresh()

        assert client.downloads == ["dags/example.py"]
        assert mirror_file.read_bytes() == b"remote"
        assert (bundle.path / "example.py").read_bytes() == b"remote"
        assert _version_string(bundle.get_current_version()) == first_version


def test_new_process_repairs_same_size_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"remote", etag='"object-v1"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        mirror_file = bundle.s3_dags_dir / "example.py"
        original_stat = mirror_file.stat()

        mirror_file.write_bytes(b"broken")
        os.utime(
            mirror_file,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        client.downloads.clear()
        fresh_bundle = _bundle(tmp_path)
        fresh_bundle.refresh()

        assert client.downloads == ["dags/example.py"]
        assert mirror_file.read_bytes() == b"remote"
        assert (fresh_bundle.path / "example.py").read_bytes() == b"remote"
        assert _version_string(fresh_bundle.get_current_version()) == first_version


@pytest.mark.parametrize(
    ("objects", "limits", "message"),
    [
        (
            [("dags/one.py", b"1"), ("dags/two.py", b"2")],
            {"max_file_count": 1},
            "max_file_count=1",
        ),
        (
            [("dags/large.py", b"12")],
            {"max_file_size_bytes": 1},
            "max_file_size_bytes=1",
        ),
        (
            [("dags/one.py", b"12"), ("dags/two.py", b"34")],
            {"max_total_size_bytes": 3},
            "max_total_size_bytes=3",
        ),
    ],
)
def test_source_limits_are_enforced_before_download(
    tmp_path,
    monkeypatch,
    objects,
    limits,
    message,
):
    client = FakeS3Client()
    for key, body in objects:
        client.put(key, body)
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match=message),
    ):
        _bundle(tmp_path, **limits).refresh()
    assert client.downloads == []


@pytest.mark.parametrize(
    "unsafe_key",
    [
        "dags/../escape.py",
        "dags//empty.py",
        "dags/back\\slash.py",
        "dags/control\x7f.py",
    ],
)
def test_unsafe_s3_key_is_rejected_before_download(tmp_path, monkeypatch, unsafe_key):
    client = FakeS3Client()
    client.put(unsafe_key, b"unsafe")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with pytest.raises(BundleManifestError, match="unsafe"):
            bundle.refresh()
    assert client.downloads == []


def test_file_directory_collision_is_rejected(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/a", b"file")
    client.put("dags/a/b.py", b"nested")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match="collides"),
    ):
        _bundle(tmp_path).refresh()
    assert client.downloads == []


def test_file_directory_collision_is_rejected_with_interleaved_sibling(
    tmp_path,
    monkeypatch,
):
    client = FakeS3Client()
    client.put("dags/a", b"file")
    client.put("dags/a-sibling.py", b"sibling")
    client.put("dags/a/b.py", b"nested")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match="collides"),
    ):
        _bundle(tmp_path).refresh()
    assert client.downloads == []


def test_directory_markers_and_manifest_ignored_paths_are_not_mirrored(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/folder/", b"")
    client.put("dags/folder/example.py", b"print('dag')")
    client.put("dags/.git/config", b"ignored")
    client.put("dags/__pycache__/example.pyc", b"ignored")
    client.put(f"dags/{MANIFEST_FILE_NAME}", b"ignored")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        manifest = json.loads(
            (bundle.published_versions_dir / _version_string(bundle.get_current_version()) / MANIFEST_FILE_NAME)
            .read_text()
        )

        assert [entry["path"] for entry in manifest["files"]] == ["folder/example.py"]
        assert client.downloads == ["dags/folder/example.py"]


def test_inventory_change_during_download_does_not_advance_state(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"old", etag='"old"')
    client.mutate_after_download = ("dags/example.py", b"new", '"new"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        with pytest.raises(BundleManifestSourceChangedError, match="changed while synchronizing"):
            bundle.refresh()
        assert not bundle.s3_mirror_state_path.exists()
        assert not bundle.manifest_ref_path.exists()


def test_sync_failure_keeps_current_release(tmp_path, monkeypatch, caplog):
    client = FakeS3Client()
    client.put("dags/example.py", b"old", etag='"old"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"new", etag='"new"')
        client.fail_download = True
        with caplog.at_level("WARNING", logger="airflow_manifest_bundle.bundle"):
            bundle.refresh()

        assert _version_string(bundle.get_current_version()) == version
        assert (bundle.path / "example.py").read_bytes() == b"old"
        assert "keeping the current release" in caplog.text


def test_missing_bucket_is_a_recoverable_not_found_error(tmp_path, monkeypatch):
    client = FakeS3Client()
    _install_fake_hook(monkeypatch, client, bucket_exists=False)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestNotFoundError, match="does not exist"),
    ):
        _bundle(tmp_path).refresh()


def test_access_denied_is_not_reported_as_a_missing_bucket(tmp_path, monkeypatch):
    class AccessDeniedError(Exception):
        def __init__(self, message: str) -> None:
            super().__init__(message)
            self.response = {"Error": {"Code": "403"}}

    client = FakeS3Client()
    client.bucket_error = AccessDeniedError("denied")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
        pytest.raises(BundleManifestError, match="Could not access"),
    ):
        _bundle(tmp_path).refresh()


def test_deployment_marker_is_required_excluded_and_defines_next_release(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"old", etag='"old"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        with pytest.raises(BundleManifestNotFoundError, match="deployment marker"):
            bundle.refresh()

        client.put("dags/.ready", b"release-1", etag='"marker-1"')
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        first_ref = json.loads(bundle.manifest_ref_path.read_text())
        assert first_ref["source"]["deployment_marker"].startswith("sha256:")
        assert not (bundle.s3_dags_dir / ".ready").exists()

        client.put("dags/example.py", b"new", etag='"new"')
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) == first_version

        client.put("dags/.ready", b"release-2", etag='"marker-2"')
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) != first_version
        assert (bundle.path / "example.py").read_bytes() == b"new"


def test_remote_change_before_reference_write_keeps_previous_release(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"old", etag='"old"')
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"candidate", etag='"candidate"')
        client.mutate_on_list = (
            client.list_calls + 3,
            "dags/example.py",
            b"changed-again",
            '"changed-again"',
        )
        bundle.refresh()

        assert _version_string(bundle.get_current_version()) == first_version
        assert json.loads(bundle.manifest_ref_path.read_text())["version"] == first_version


def test_pinned_initialization_does_not_construct_s3_hook_or_read_latest(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_text("print('dag')")
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        local_bundle = ManifestLocalDagBundle(
            name="manifest-s3",
            published_root=str(tmp_path / "published"),
        )
        published = publish_manifest_local_dag_bundle(bundle=local_bundle, source_path=source)
        local_bundle.manifest_ref_path.unlink()

        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"S3 hook must not be constructed: {kwargs}")

        monkeypatch.setattr(s3_module, "S3Hook", ForbiddenHook)
        pinned = _bundle(tmp_path, version=published.version)
        pinned.initialize()

        assert (pinned.path / "example.py").read_text() == "print('dag')"
        assert not pinned.s3_dags_dir.exists()


def test_source_identity_does_not_include_connection_id(tmp_path, monkeypatch):
    client = FakeS3Client()
    client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        first = _bundle(tmp_path, aws_conn_id="first")
        second = _bundle(tmp_path, aws_conn_id="second")
        assert first._source_identity(first._get_s3_client()) == second._source_identity(
            second._get_s3_client()
        )


def test_current_release_rejects_a_different_s3_source_identity(tmp_path, monkeypatch):
    first_client = FakeS3Client(endpoint_url="https://first.example.test")
    first_client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, first_client)

    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
        first = _bundle(tmp_path)
        first.refresh()
        original_ref = json.loads(first.manifest_ref_path.read_text())

        second_client = FakeS3Client(endpoint_url="https://second.example.test")
        second_client.put("dags/example.py", b"dag")
        _install_fake_hook(monkeypatch, second_client)
        second = _bundle(tmp_path)
        second.refresh()

        assert json.loads(second.manifest_ref_path.read_text()) == original_ref
