from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from _test_utils import conf_vars, published_payload

from airflow_manifest_bundle import ManifestDagBundleBase, cli
from airflow_manifest_bundle import gcs as gcs_module
from airflow_manifest_bundle.bundle import BundleManifestReferenceChangedError
from airflow_manifest_bundle.gcs import (
    ManifestGCSDagBundle,
    publish_manifest_gcs_dag_bundle,
)
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
from airflow_manifest_bundle.object_source import ObjectStoreSourceDagBundleBase
from airflow_manifest_bundle.s3 import ManifestS3DagBundle


class FakeGCSError(Exception):
    def __init__(self, code: int, operation: str) -> None:
        super().__init__(f"{operation}: {code}")
        self.code = code


class FakeGCSBlob:
    def __init__(
        self,
        client: FakeGCSClient,
        name: str,
        *,
        generation: int | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        self._client = client
        self.name = name
        self._requested_generation = generation
        data = metadata if metadata is not None else client.objects.get(name)
        if data is None:
            self.size = None
            self.generation = generation
            self.metageneration = None
            self.updated = None
            self.etag = None
        else:
            self.size = len(data["body"])
            self.generation = data["generation"]
            self.metageneration = data["metageneration"]
            self.updated = data["updated"]
            self.etag = data["etag"]

    def download_to_filename(self, filename: str, *, if_generation_match: int) -> None:
        if self._client.fail_download:
            raise RuntimeError("download failed")
        mutation = self._client.mutate_before_download
        if mutation is not None:
            name, body = mutation
            self._client.put(name, body)
            self._client.mutate_before_download = None
        data = self._client.objects.get(self.name)
        if data is None:
            raise FakeGCSError(404, "Download")
        if (
            data["generation"] != if_generation_match
            or self._requested_generation != if_generation_match
        ):
            raise FakeGCSError(412, "Download")
        Path(filename).write_bytes(data["body"])
        self._client.downloads.append((self.name, if_generation_match))
        mutation = self._client.mutate_after_download
        if mutation is not None:
            name, body = mutation
            self._client.put(name, body)
            self._client.mutate_after_download = None


class FakeGCSBucket:
    def __init__(self, client: FakeGCSClient, name: str) -> None:
        self._client = client
        self.name = name

    def get_blob(self, name: str) -> FakeGCSBlob | None:
        data = self._client.objects.get(name)
        return (
            None
            if data is None
            else FakeGCSBlob(self._client, name, metadata=dict(data))
        )

    def blob(self, name: str, *, generation: int | None = None) -> FakeGCSBlob:
        return FakeGCSBlob(self._client, name, generation=generation)


class FakeGCSClient:
    def __init__(self, *, endpoint: str = "https://storage.example.test") -> None:
        self._connection = SimpleNamespace(API_BASE_URL=endpoint)
        self.objects: dict[str, dict[str, object]] = {}
        self.downloads: list[tuple[str, int]] = []
        self.list_calls = 0
        self.bucket_exists = True
        self.bucket_error: Exception | None = None
        self.fail_download = False
        self.mutate_on_list: tuple[int, str, bytes] | None = None
        self.mutate_before_download: tuple[str, bytes] | None = None
        self.mutate_after_download: tuple[str, bytes] | None = None
        self._next_generation = 1

    def put(self, name: str, body: bytes) -> None:
        generation = self._next_generation
        self._next_generation += 1
        self.objects[name] = {
            "body": body,
            "generation": generation,
            "metageneration": 1,
            "updated": f"2026-08-03T12:00:{generation:02d}+00:00",
            "etag": f"etag-{generation}",
        }

    def delete(self, name: str) -> None:
        del self.objects[name]

    def get_bucket(self, name: str) -> FakeGCSBucket:
        assert name == "dag-bucket"
        if self.bucket_error is not None:
            raise self.bucket_error
        if not self.bucket_exists:
            raise FakeGCSError(404, "GetBucket")
        return FakeGCSBucket(self, name)

    def bucket(self, name: str) -> FakeGCSBucket:
        assert name == "dag-bucket"
        return FakeGCSBucket(self, name)

    def list_blobs(self, bucket_name: str, *, prefix: str, max_results: int | None = None):
        assert bucket_name == "dag-bucket"
        self.list_calls += 1
        mutation = self.mutate_on_list
        if mutation is not None and mutation[0] == self.list_calls:
            _, name, body = mutation
            self.put(name, body)
            self.mutate_on_list = None
        blobs = [
            FakeGCSBlob(self, name, metadata=dict(data))
            for name, data in sorted(self.objects.items())
            if name.startswith(prefix)
        ]
        return blobs if max_results is None else blobs[:max_results]


def _install_fake_hook(monkeypatch, client: FakeGCSClient):
    constructed: list[str] = []

    class FakeGCSHook:
        def __init__(self, *, gcp_conn_id: str) -> None:
            constructed.append(gcp_conn_id)

        def get_conn(self):
            return client

    monkeypatch.setattr(gcs_module, "GCSHook", FakeGCSHook)
    return constructed


def _bundle(tmp_path: Path, **kwargs) -> ManifestGCSDagBundle:
    prefix = kwargs.pop("prefix", "dags/")
    return ManifestGCSDagBundle(
        name="manifest-gcs",
        bucket_name="dag-bucket",
        prefix=prefix,
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
    assert inspect.isabstract(ObjectStoreSourceDagBundleBase)
    assert ManifestLocalDagBundle.__bases__ == (ManifestDagBundleBase,)
    assert ManifestS3DagBundle.__bases__ == (ObjectStoreSourceDagBundleBase,)
    assert ManifestGCSDagBundle.__bases__ == (ObjectStoreSourceDagBundleBase,)
    assert not issubclass(ManifestS3DagBundle, ManifestGCSDagBundle)
    assert not issubclass(ManifestGCSDagBundle, ManifestS3DagBundle)


def test_constructor_matches_stock_defaults_and_constructs_hook_lazily(
    tmp_path, monkeypatch
):
    client = FakeGCSClient()
    constructed = _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        custom = _bundle(tmp_path, gcp_conn_id="custom")

        assert bundle.gcp_conn_id == "google_cloud_default"
        assert bundle.bucket_name == "dag-bucket"
        assert bundle.prefix == "dags/"
        assert bundle.max_file_count == gcs_module.DEFAULT_MAX_FILE_COUNT
        assert bundle.max_file_size_bytes == gcs_module.DEFAULT_MAX_FILE_SIZE_BYTES
        assert bundle.max_total_size_bytes == gcs_module.DEFAULT_MAX_TOTAL_SIZE_BYTES
        assert bundle.auto_publish is True
        assert bundle.gcs_dags_dir == bundle.base_dir / "_gcs_source"
        assert bundle.gcs_dags_dir != bundle.versions_dir
        assert bundle.view_url_template() == (
            "https://console.cloud.google.com/storage/browser/dag-bucket/dags/"
        )
        assert constructed == []
        assert custom.gcs_hook is not None
        assert constructed == ["custom"]


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("auto_publish", 1),
        ("max_file_count", 0),
        ("max_file_size_bytes", -1),
        ("max_total_size_bytes", 1.5),
    ],
)
def test_constructor_rejects_invalid_options(tmp_path, option, value):
    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(TypeError, match=option),
    ):
        _bundle(tmp_path, **{option: value})


def test_constructor_rejects_object_store_published_root(tmp_path, monkeypatch):
    from airflow_manifest_bundle import s3_store

    # The store itself must construct so the test exercises the GCS adapter's
    # restriction, not a missing Amazon provider.
    monkeypatch.setattr(
        s3_store, "S3Hook", SimpleNamespace(default_conn_name="aws_default")
    )
    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(TypeError, match="filesystem published_root"),
    ):
        ManifestGCSDagBundle(
            name="manifest-gcs",
            bucket_name="dag-bucket",
            prefix="dags/",
            published_root="s3://dag-releases/team",
        )


def test_missing_extra_fails_only_when_unpinned_source_is_used(tmp_path, monkeypatch):
    monkeypatch.setattr(gcs_module, "GCSHook", None)
    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        with pytest.raises(
            BundleManifestError, match=r"airflow-manifest-bundle\[gcs\]"
        ):
            bundle.refresh()


def test_first_refresh_downloads_and_publishes_version(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"print('dag')")
    generation = client.objects["dags/example.py"]["generation"]
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())

        assert client.downloads == [("dags/example.py", generation)]
        assert bundle.path == bundle.versions_dir / version
        assert (bundle.path / "example.py").read_bytes() == b"print('dag')"
        ref = json.loads(bundle.manifest_ref_path.read_text())
        assert ref["source"]["type"] == "gcs"
        assert ref["source"]["identity"].startswith("sha256:")
        assert ref["source"]["observation"].startswith("sha256:")
        assert (bundle.published_versions_dir / version / MANIFEST_FILE_NAME).is_file()


def test_explicit_mode_refresh_uses_release_without_accessing_gcs(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_text("print('dag')")

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        local_bundle = ManifestLocalDagBundle(
            name="manifest-gcs",
            published_root=str(tmp_path / "published"),
        )
        published = publish_manifest_local_dag_bundle(
            bundle=local_bundle, source_path=source
        )

        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"GCS hook must not be constructed: {kwargs}")

        monkeypatch.setattr(gcs_module, "GCSHook", ForbiddenHook)
        explicit = _bundle(tmp_path, auto_publish=False)
        explicit.refresh()

        assert _version_string(explicit.get_current_version()) == published.version
        assert (explicit.path / "example.py").read_text() == "print('dag')"
        assert explicit._gcs_hook is None
        assert not explicit.gcs_dags_dir.exists()


def test_explicit_publisher_guards_modes_before_gcs_access(tmp_path, monkeypatch):
    monkeypatch.setattr(gcs_module, "GCSHook", None)
    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        with pytest.raises(BundleManifestError, match="auto_publish enabled"):
            publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path))
        with pytest.raises(
            BundleManifestError, match="Cannot explicitly publish pinned bundle"
        ):
            publish_manifest_gcs_dag_bundle(
                bundle=_bundle(
                    tmp_path,
                    auto_publish=False,
                    version=f"sha256-{'a' * 64}",
                )
            )


def test_explicit_publisher_records_source_and_expected_version(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"first")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path, auto_publish=False)
        with mock.patch.object(bundle, "lock", wraps=bundle.lock) as bundle_lock:
            first = publish_manifest_gcs_dag_bundle(bundle=bundle)

        assert bundle_lock.call_count == 1
        assert first.version.startswith("sha256-")
        assert first.ref_payload["source"]["type"] == "gcs"
        assert not bundle.auto_publish_state_path.exists()
        repeated = publish_manifest_gcs_dag_bundle(bundle=bundle)
        assert repeated.version == first.version
        assert repeated.created_snapshot is False

        client.put("dags/example.py", b"second")
        second = publish_manifest_gcs_dag_bundle(
            bundle=bundle,
            expected_current_version=first.version,
        )
        client.put("dags/example.py", b"third")
        with pytest.raises(
            BundleManifestReferenceChangedError, match="manifest reference changed"
        ):
            publish_manifest_gcs_dag_bundle(
                bundle=bundle,
                expected_current_version=first.version,
            )
        assert (
            json.loads(bundle.manifest_ref_path.read_text())["version"]
            == second.version
        )


def test_publish_gcs_command(tmp_path, monkeypatch, capsys):
    client = FakeGCSClient()
    client.put("dags/example.py", b"print('dag')")
    _install_fake_hook(monkeypatch, client)
    published_root = tmp_path / "published"
    config = [
        {
            "name": "manifest-gcs",
            "classpath": "airflow_manifest_bundle.gcs.ManifestGCSDagBundle",
            "kwargs": {
                "auto_publish": False,
                "bucket_name": "dag-bucket",
                "prefix": "dags/",
                "published_root": str(published_root),
            },
        }
    ]

    with conf_vars(
        {
            ("core", "load_examples"): "False",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles"),
        }
    ):
        cli.main(["publish-gcs", "manifest-gcs", "--output", "json"])

    published = published_payload(capsys.readouterr().out)
    assert published["bundle_name"] == "manifest-gcs"
    assert published["version"].startswith("sha256-")
    assert published["file_count"] == 1
    ref = json.loads((published_root / "refs/manifest-gcs/latest.json").read_text())
    assert ref["source"]["type"] == "gcs"


def test_same_size_generation_change_and_delete_repair_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    client.put("dags/delete.py", b"gone")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"new")
        new_generation = client.objects["dags/example.py"]["generation"]
        client.delete("dags/delete.py")
        bundle.refresh()

        assert _version_string(bundle.get_current_version()) != first_version
        assert (bundle.gcs_dags_dir / "example.py").read_bytes() == b"new"
        assert not (bundle.gcs_dags_dir / "delete.py").exists()
        assert ("dags/example.py", new_generation) in client.downloads


def test_generation_precondition_stops_changed_download(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"candidate")
    client.mutate_before_download = ("dags/example.py", b"changed")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(
            BundleManifestSourceChangedError,
            match="changed before it could be downloaded",
        ),
    ):
        publish_manifest_gcs_dag_bundle(bundle=_bundle(tmp_path, auto_publish=False))

    assert not (tmp_path / "published/refs/manifest-gcs/latest.json").exists()


def test_new_process_repairs_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"good")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        first = _bundle(tmp_path)
        first.refresh()
        first.gcs_dags_dir.joinpath("example.py").write_bytes(b"evil")
        downloads_before = len(client.downloads)

        fresh = _bundle(tmp_path)
        fresh.refresh()

        assert len(client.downloads) == downloads_before + 1
        assert (fresh.gcs_dags_dir / "example.py").read_bytes() == b"good"


def test_marker_change_repairs_same_size_corrupt_reused_mirror(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"remote")
    client.put("dags/.ready", b"release-1")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        mirror_file = bundle.gcs_dags_dir / "example.py"
        original_stat = mirror_file.stat()

        mirror_file.write_bytes(b"broken")
        os.utime(
            mirror_file,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        client.put("dags/.ready", b"release-2")
        client.downloads.clear()
        bundle.refresh()

        assert [name for name, _ in client.downloads] == ["dags/example.py"]
        assert mirror_file.read_bytes() == b"remote"
        assert (bundle.path / "example.py").read_bytes() == b"remote"
        assert _version_string(bundle.get_current_version()) == first_version


@pytest.mark.parametrize(
    "unsafe_name",
    ["dags/../escape.py", "dags//empty.py", "dags/back\\slash.py", "dags/bad\x00.py"],
)
def test_unsafe_gcs_name_is_rejected_before_download(
    tmp_path, monkeypatch, unsafe_name
):
    client = FakeGCSClient()
    client.put(unsafe_name, b"dag")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(BundleManifestError, match="unsafe"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_file_directory_collision_is_rejected(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/collision", b"file")
    client.put("dags/collision/child.py", b"child")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(BundleManifestError, match="collides"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_prefix_boundary_directory_markers_and_ignored_files(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/example.py", b"dag")
    client.put("dags/subdir/", b"")
    client.put("dags/.git/config", b"ignored")
    client.put("dags/cache.pyc", b"ignored")
    client.put("dags-archive/old.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path, prefix="dags")
        bundle.refresh()

        assert [name for name, _ in client.downloads] == ["dags/example.py"]
        assert (bundle.path / "example.py").is_file()
        assert not (bundle.path / "old.py").exists()


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("max_file_count", 1, "max_file_count"),
        ("max_file_size_bytes", 2, "max_file_size_bytes"),
        ("max_total_size_bytes", 4, "max_total_size_bytes"),
    ],
)
def test_source_limits_are_enforced_before_download(
    tmp_path, monkeypatch, option, value, message
):
    client = FakeGCSClient()
    client.put("dags/first.py", b"abc")
    client.put("dags/second.py", b"def")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(BundleManifestError, match=message),
    ):
        _bundle(tmp_path, **{option: value}).refresh()

    assert client.downloads == []


def test_changed_inventory_does_not_advance_mirror_state_or_release(
    tmp_path, monkeypatch
):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        original_ref = json.loads(bundle.manifest_ref_path.read_text())
        original_state = bundle.gcs_mirror_state_path.read_text()

        client.put("dags/example.py", b"candidate")
        client.mutate_after_download = ("dags/example.py", b"changed-again")
        bundle.refresh()

        assert json.loads(bundle.manifest_ref_path.read_text()) == original_ref
        assert bundle.gcs_mirror_state_path.read_text() == original_state


def test_sync_failure_keeps_current_release(tmp_path, monkeypatch, caplog):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"new")
        client.fail_download = True

        with caplog.at_level("WARNING", logger="airflow_manifest_bundle.bundle"):
            bundle.refresh()

        assert _version_string(bundle.get_current_version()) == version
        assert (bundle.path / "example.py").read_bytes() == b"old"
        assert "keeping the current release" in caplog.text


def test_deployment_marker_is_required_excluded_and_defines_release(
    tmp_path, monkeypatch
):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path, deployment_marker_key=".ready")
        with pytest.raises(BundleManifestNotFoundError, match="deployment marker"):
            bundle.refresh()

        client.put("dags/.ready", b"release-1")
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        assert not (bundle.gcs_dags_dir / ".ready").exists()

        client.put("dags/example.py", b"new")
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) == first_version

        client.put("dags/.ready", b"release-2")
        bundle.refresh()
        assert _version_string(bundle.get_current_version()) != first_version
        assert (bundle.path / "example.py").read_bytes() == b"new"


def test_remote_change_before_reference_write_keeps_previous_release(
    tmp_path, monkeypatch
):
    client = FakeGCSClient()
    client.put("dags/example.py", b"old")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        bundle.refresh()
        first_version = _version_string(bundle.get_current_version())
        client.put("dags/example.py", b"candidate")
        client.mutate_on_list = (
            client.list_calls + 3,
            "dags/example.py",
            b"changed-again",
        )
        bundle.refresh()

        assert _version_string(bundle.get_current_version()) == first_version
        assert (
            json.loads(bundle.manifest_ref_path.read_text())["version"] == first_version
        )


def test_empty_source_requires_explicit_opt_in(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags/.git/config", b"ignored")
    _install_fake_hook(monkeypatch, client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        with pytest.raises(
            BundleManifestError, match="explicitly publish empty source tree"
        ):
            publish_manifest_gcs_dag_bundle(
                bundle=_bundle(tmp_path, auto_publish=False)
            )

        client.delete("dags/.git/config")
        published = publish_manifest_gcs_dag_bundle(
            bundle=_bundle(tmp_path, auto_publish=False, allow_empty_source=True)
        )
        assert published.file_count == 0


def test_bucket_errors_use_the_bundle_error_contract(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.bucket_exists = False
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(BundleManifestNotFoundError, match="does not exist"),
    ):
        _bundle(tmp_path).refresh()

    client.bucket_exists = True
    client.bucket_error = FakeGCSError(403, "GetBucket")
    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles2")}
        ),
        pytest.raises(BundleManifestError, match="Could not access"),
    ):
        _bundle(tmp_path).refresh()


def test_missing_prefix_is_a_recoverable_not_found_error(tmp_path, monkeypatch):
    client = FakeGCSClient()
    client.put("dags-archive/example.py", b"dag")
    _install_fake_hook(monkeypatch, client)

    with (
        conf_vars(
            {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
        ),
        pytest.raises(BundleManifestNotFoundError, match="prefix .* does not exist"),
    ):
        _bundle(tmp_path).refresh()

    assert client.downloads == []


def test_pinned_initialization_does_not_construct_gcs_hook_or_read_latest(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    (source / "example.py").write_text("print('dag')")
    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        local_bundle = ManifestLocalDagBundle(
            name="manifest-gcs",
            published_root=str(tmp_path / "published"),
        )
        published = publish_manifest_local_dag_bundle(
            bundle=local_bundle, source_path=source
        )
        local_bundle.manifest_ref_path.unlink()

        class ForbiddenHook:
            def __init__(self, **kwargs):
                raise AssertionError(f"GCS hook must not be constructed: {kwargs}")

        monkeypatch.setattr(gcs_module, "GCSHook", ForbiddenHook)
        pinned = _bundle(tmp_path, version=published.version)
        assert pinned.view_url() is None
        assert pinned.view_url_template() is None
        pinned.initialize()

        assert (pinned.path / "example.py").read_text() == "print('dag')"
        assert not pinned.gcs_dags_dir.exists()


def test_source_identity_excludes_connection_and_rejects_endpoint_change(
    tmp_path, monkeypatch
):
    first_client = FakeGCSClient(endpoint="https://first.example.test")
    first_client.put("dags/example.py", b"dag")
    _install_fake_hook(monkeypatch, first_client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        first = _bundle(tmp_path, gcp_conn_id="first")
        same = _bundle(tmp_path, gcp_conn_id="second")
        assert first._source_identity(first._get_gcs_client()) == same._source_identity(
            same._get_gcs_client()
        )
        first.refresh()
        original_ref = json.loads(first.manifest_ref_path.read_text())

        second_client = FakeGCSClient(endpoint="https://second.example.test")
        second_client.put("dags/example.py", b"dag")
        _install_fake_hook(monkeypatch, second_client)
        second = _bundle(tmp_path)
        second.refresh()

        assert json.loads(second.manifest_ref_path.read_text()) == original_ref


def test_source_identity_normalizes_the_default_public_endpoint(tmp_path, monkeypatch):
    default_client = FakeGCSClient(endpoint="https://storage.googleapis.com")
    _install_fake_hook(monkeypatch, default_client)

    with conf_vars(
        {("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}
    ):
        bundle = _bundle(tmp_path)
        default_identity = bundle._source_identity(bundle._get_gcs_client())

        # A library upgrade that hides the private endpoint attributes must not
        # change the identity of a default deployment.
        stripped_client = FakeGCSClient()
        stripped_client._connection = SimpleNamespace()
        _install_fake_hook(monkeypatch, stripped_client)
        stripped = _bundle(tmp_path)
        assert (
            stripped._source_identity(stripped._get_gcs_client()) == default_identity
        )

        custom_client = FakeGCSClient(endpoint="https://custom.example.test")
        _install_fake_hook(monkeypatch, custom_client)
        custom = _bundle(tmp_path)
        assert custom._source_identity(custom._get_gcs_client()) != default_identity
