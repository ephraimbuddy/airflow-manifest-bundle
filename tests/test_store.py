"""
Contract tests for ``ArtifactStore`` implementations.

Every implementation must satisfy these behaviors; the tests talk only to the
interface, and the ``store`` fixture parametrizes them over every backend. Backend
additions join that parametrization instead of writing a parallel suite; the few
store-kind conditionals mark contract points that are deliberately backend-specific.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from _test_utils import conf_vars

from airflow_manifest_bundle import bundle as bundle_module
from airflow_manifest_bundle import s3_store as s3_store_module
from airflow_manifest_bundle.bundle import (
    BundleManifestReferenceChangedError,
    FilesystemArtifactStore,
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
    build_bundle_version_manifest_result,
    serialize_bundle_version_manifest,
)
from airflow_manifest_bundle.s3_store import S3ArtifactStore, parse_s3_published_root
from airflow_manifest_bundle.store import ArtifactStoreConflictError

BUNDLE_NAME = "contract"
BUCKET = "dag-bucket"


@pytest.fixture(params=["filesystem", "s3"])
def store(request, tmp_path, fake_s3):
    if request.param == "filesystem":
        return FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=tmp_path / "published",
            cache_root=tmp_path / "cache",
        )
    if request.param == "s3":
        return S3ArtifactStore(
            bundle_name=BUNDLE_NAME, published_root=f"s3://{BUCKET}/releases"
        )
    raise ValueError(request.param)


def _is_filesystem(store) -> bool:
    return isinstance(store, FilesystemArtifactStore)


def _corrupt_ref(store, fake_s3) -> None:
    if _is_filesystem(store):
        store.ref_path.parent.mkdir(parents=True, exist_ok=True)
        store.ref_path.write_text("{not json")
    else:
        fake_s3.put(store._ref_key, b"{not json")


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

    def test_read_ref_invalid_json_raises_with_message(self, store, fake_s3):
        store.prepare_publish_areas()
        _corrupt_ref(store, fake_s3)
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
        if _is_filesystem(store):
            # Only backends with a walkable published tree run the pre-transfer pass.
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
        if _is_filesystem(store):
            # The object store validates by comparing committed manifest bytes instead.
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

    def test_sweep_publish_temps_removes_filesystem_orphans(self, tmp_path):
        # Filesystem-specific: object-store publication writes no temporary artifacts,
        # so its sweep is a documented no-op.
        store = FilesystemArtifactStore(
            bundle_name=BUNDLE_NAME,
            published_root=tmp_path / "published",
            cache_root=tmp_path / "cache",
        )
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
        cache_versions_dir = tmp_path / "cache" / "versions"
        if _is_filesystem(store):
            source = store.root / "source"
        else:
            # The published root is remote, so the cache overlap is the only local one.
            source = cache_versions_dir / "source"
        with pytest.raises(ValueError):
            store.validate_source_paths(source, cache_versions_dir=cache_versions_dir)

    def test_validate_source_paths_accepts_disjoint_source(self, store, tmp_path):
        source = _write_source(tmp_path / "elsewhere")
        store.validate_source_paths(source, cache_versions_dir=tmp_path / "cache" / "versions")

    def test_locators_are_scoped_to_the_bundle(self, store):
        version = "sha256-" + "a" * 64
        assert str(store.snapshot_path(version)).endswith(f"/{version}")
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
    """Just enough of the S3 API for the artifact store, with conditional-write semantics."""

    def __init__(self) -> None:
        self.meta = SimpleNamespace(endpoint_url="https://store.example.test")
        self.objects: dict[str, bytes] = {}
        self.etags: dict[str, str] = {}
        self.put_sequence: list[str] = []
        self.put_conditions: list[tuple[str, str | None, str | None]] = []
        self.fail_conditional_writes_with: str | None = None
        self.source_objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.copies: list[tuple[str, str, str]] = []
        self.copy_attempts = 0
        self.fail_copy_with: str | None = None
        self._etag_counter = 0

    def add_source_object(self, bucket: str, key: str, body: bytes, *, etag: str) -> None:
        self.source_objects[(bucket, key)] = (body, etag)

    def copy_object(self, *, Bucket: str, Key: str, CopySource, CopySourceIfMatch=None):
        assert Bucket == BUCKET
        self.copy_attempts += 1
        if self.fail_copy_with:
            raise FakeStoreClientError(self.fail_copy_with)
        source = (CopySource["Bucket"], CopySource["Key"])
        if source not in self.source_objects:
            raise FakeStoreClientError("NoSuchKey")
        body, etag = self.source_objects[source]
        if CopySourceIfMatch is not None and etag != CopySourceIfMatch:
            raise FakeStoreClientError("PreconditionFailed")
        self.objects[Key] = body
        self.etags[Key] = self._next_etag()
        self.copies.append((source[0], source[1], Key))
        return {"CopyObjectResult": {"ETag": self.etags[Key]}}

    def _next_etag(self) -> str:
        self._etag_counter += 1
        return f'"etag-{self._etag_counter}"'

    def put(self, key: str, body: bytes) -> None:
        """Test helper: an external writer that bypasses this client's bookkeeping."""
        self.objects[key] = body
        self.etags[key] = self._next_etag()

    def get_object(self, *, Bucket: str, Key: str):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise FakeStoreClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key]), "ETag": self.etags.get(Key, '"fake-etag"')}

    def head_object(self, *, Bucket: str, Key: str):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise FakeStoreClientError("404")
        return {"ContentLength": len(self.objects[Key]), "ETag": self.etags.get(Key, '"fake-etag"')}

    def put_object(self, *, Bucket: str, Key: str, Body, IfMatch=None, IfNoneMatch=None):
        assert Bucket == BUCKET
        if (IfMatch or IfNoneMatch) and self.fail_conditional_writes_with:
            raise FakeStoreClientError(self.fail_conditional_writes_with)
        exists = Key in self.objects
        if IfNoneMatch == "*" and exists:
            raise FakeStoreClientError("PreconditionFailed")
        if IfMatch is not None and (not exists or self.etags.get(Key) != IfMatch):
            raise FakeStoreClientError("PreconditionFailed")
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()
        etag = self._next_etag()
        self.etags[Key] = etag
        self.put_sequence.append(Key)
        self.put_conditions.append((Key, IfMatch, IfNoneMatch))
        return {"ETag": etag}


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

    def test_locators_are_urls_scoped_to_the_root(self, fake_s3):
        store = _s3_artifact_store()
        version = "sha256-" + "a" * 64
        assert store.root == f"s3://{BUCKET}/releases"
        assert store.ref_path == f"s3://{BUCKET}/releases/refs/{BUNDLE_NAME}/latest.json"
        assert store.snapshot_path(version).endswith(f"/versions/{BUNDLE_NAME}/{version}")
        assert str(store.state_path).startswith(store.root)


class TestS3StoreDocumentCAS:
    def _ref_key(self) -> str:
        return f"releases/refs/{BUNDLE_NAME}/latest.json"

    def test_first_write_conditions_on_absence(self, fake_s3):
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref(missing_message="missing", invalid_message="invalid")
        store.write_ref({"schema_version": 1})
        assert fake_s3.put_conditions == [(self._ref_key(), None, "*")]

    def test_write_without_prior_read_establishes_a_baseline(self, fake_s3):
        store = _s3_artifact_store()
        store.write_ref({"schema_version": 1})
        assert fake_s3.put_conditions == [(self._ref_key(), None, "*")]

    def test_replacement_conditions_on_the_read_etag(self, fake_s3):
        fake_s3.put(self._ref_key(), b"{}")
        expected_etag = fake_s3.etags[self._ref_key()]
        store = _s3_artifact_store()
        store.read_ref(missing_message="missing", invalid_message="invalid")
        store.write_ref({"schema_version": 1})
        assert fake_s3.put_conditions == [(self._ref_key(), expected_etag, None)]

    def test_lost_create_race_with_different_content_conflicts(self, fake_s3):
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref(missing_message="missing", invalid_message="invalid")
        fake_s3.put(self._ref_key(), b'{"winner":true}')
        with pytest.raises(ArtifactStoreConflictError):
            store.write_ref({"schema_version": 1})
        # The conflict refreshed the baseline, so a retry replaces the winner cleanly.
        store.write_ref({"schema_version": 2})
        assert json.loads(fake_s3.objects[self._ref_key()]) == {"schema_version": 2}

    def test_lost_race_with_identical_content_is_an_idempotent_win(self, fake_s3):
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref(missing_message="missing", invalid_message="invalid")
        payload = {"schema_version": 1, "bundle_name": BUNDLE_NAME}
        fake_s3.put(self._ref_key(), serialize_bundle_version_manifest(payload))
        store.write_ref(payload)

    def test_stale_baseline_conflicts_when_another_publisher_won(self, fake_s3):
        fake_s3.put(self._ref_key(), b"{}")
        store = _s3_artifact_store()
        store.read_ref(missing_message="missing", invalid_message="invalid")
        fake_s3.put(self._ref_key(), b'{"winner":true}')
        with pytest.raises(ArtifactStoreConflictError):
            store.write_ref({"schema_version": 1})

    def test_missing_conditional_write_support_is_a_clear_error(self, fake_s3):
        fake_s3.fail_conditional_writes_with = "NotImplemented"
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestError, match="does not support conditional writes"):
            store.write_ref({"schema_version": 1})


class TestS3StorePublishSnapshot:
    def test_manifest_object_is_committed_last(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        store = _s3_artifact_store()
        created = store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
        )
        assert created is True
        snapshot_puts = [key for key in fake_s3.put_sequence if f"/{result.version}/" in key]
        assert snapshot_puts[-1].endswith(f"/{MANIFEST_FILE_NAME}")
        assert len(snapshot_puts) == len(result.manifest["files"]) + 1

    def test_republish_of_committed_version_uploads_nothing(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        store = _s3_artifact_store()
        store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
        )
        puts_after_first = len(fake_s3.put_sequence)
        created = store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
        )
        assert created is False
        assert len(fake_s3.put_sequence) == puts_after_first

    def test_tampered_committed_manifest_is_refused(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        fake_s3.put(
            f"releases/versions/{BUNDLE_NAME}/{result.version}/{MANIFEST_FILE_NAME}",
            b'{"tampered": true}',
        )
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestError, match="refusing to overwrite"):
            store.publish_snapshot(
                result.version,
                manifest=result.manifest,
                source_root=source,
                validate_existing=lambda tree: None,
            )

    def test_source_drift_aborts_before_the_manifest_commits(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        (source / "dags" / "example.py").write_text("print('drifted')\n")
        store = _s3_artifact_store()
        with pytest.raises(BundleManifestSourceChangedError):
            store.publish_snapshot(
                result.version,
                manifest=result.manifest,
                source_root=source,
                validate_existing=lambda tree: None,
            )
        assert store.snapshot_exists(result.version) is False


def _two_file_source(root: Path) -> Path:
    _write_source(root)
    (root / "dags" / "second.py").write_text("print('second')\n")
    return root


def _copy_hints_for(source: Path, result, *, endpoint="https://store.example.test"):
    return {
        file_info["path"]: {
            "type": "s3",
            "endpoint": endpoint,
            "bucket": "source-bucket",
            "key": f"dags-src/{file_info['path']}",
            "etag": f'"src-{file_info["path"]}"',
        }
        for file_info in result.manifest["files"]
    }


def _seed_copy_sources(fake_s3, source: Path, hints) -> None:
    for path, hint in hints.items():
        fake_s3.add_source_object(
            hint["bucket"], hint["key"], (source / path).read_bytes(), etag=hint["etag"]
        )


class TestS3StoreServerSideCopy:
    def _publish_with_hints(self, fake_s3, source, hints):
        result = _manifest_result(source)
        store = _s3_artifact_store()
        created = store.publish_snapshot(
            result.version,
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
            copy_hints=hints,
        )
        return store, result, created

    def test_matching_endpoint_copies_instead_of_uploading(self, fake_s3, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _copy_hints_for(source, result)
        _seed_copy_sources(fake_s3, source, hints)

        store, result, created = self._publish_with_hints(fake_s3, source, hints)

        assert created is True
        assert len(fake_s3.copies) == len(result.manifest["files"])
        snapshot_puts = [key for key in fake_s3.put_sequence if f"/{result.version}/" in key]
        assert snapshot_puts == [
            f"releases/versions/{BUNDLE_NAME}/{result.version}/{MANIFEST_FILE_NAME}"
        ]
        destination = tmp_path / "dest"
        destination.mkdir()
        store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)
        assert (destination / "dags" / "second.py").read_text() == "print('second')\n"

    def test_endpoint_mismatch_uploads_everything(self, fake_s3, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _copy_hints_for(source, result, endpoint="https://elsewhere.example.test")
        _seed_copy_sources(fake_s3, source, hints)

        store, result, created = self._publish_with_hints(fake_s3, source, hints)

        assert created is True
        assert fake_s3.copies == []
        assert store.snapshot_exists(result.version)

    def test_stale_source_etag_falls_back_for_that_file_only(self, fake_s3, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _copy_hints_for(source, result)
        _seed_copy_sources(fake_s3, source, hints)
        stale = hints["dags/example.py"]
        fake_s3.add_source_object(
            stale["bucket"], stale["key"], b"moved on", etag='"a-newer-etag"'
        )

        _store, result, created = self._publish_with_hints(fake_s3, source, hints)

        assert created is True
        assert len(fake_s3.copies) == 1
        uploaded = [
            key
            for key in fake_s3.put_sequence
            if f"/{result.version}/" in key and not key.endswith(MANIFEST_FILE_NAME)
        ]
        assert uploaded == [
            f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py"
        ]
        # The published object carries the manifest's bytes, not the moved-on source.
        assert (
            fake_s3.objects[f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py"]
            == b"print('dag')\n"
        )

    def test_systemic_copy_failure_disables_further_attempts(self, fake_s3, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _copy_hints_for(source, result)
        _seed_copy_sources(fake_s3, source, hints)
        fake_s3.fail_copy_with = "AccessDenied"

        store, result, created = self._publish_with_hints(fake_s3, source, hints)

        assert created is True
        assert fake_s3.copy_attempts == 1
        assert fake_s3.copies == []
        assert store.snapshot_exists(result.version)

    def test_lying_copy_source_is_detected_at_fetch(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _copy_hints_for(source, result)
        hint = hints["dags/example.py"]
        # The remote object claims the observed ETag but carries different bytes.
        fake_s3.add_source_object(
            hint["bucket"], hint["key"], b"print('corrupted')\n", etag=hint["etag"]
        )

        store, result, created = self._publish_with_hints(fake_s3, source, hints)
        assert created is True

        destination = tmp_path / "dest"
        destination.mkdir()
        with pytest.raises(BundleManifestError, match="does not match the snapshot manifest"):
            store.fetch_snapshot(
                result.version, destination, structural_validator=lambda tree: None
            )


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

    def test_refresh_follows_ref_updates_and_keeps_old_versions(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        first = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
            )
            bundle.initialize()
            assert _version_string(bundle.get_current_version()) == first.version

            (source / "dags" / "example.py").write_text("print('v2')\n")
            second = _publish_to_fake_s3(
                fake_s3, prefix="releases", bundle_name="my-dags", source=source
            )
            bundle.refresh()

            assert _version_string(bundle.get_current_version()) == second.version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('v2')\n"
            # The previous version stays materialized for pinned work.
            old_copy = bundle.versions_dir / first.version / "dags" / "example.py"
            assert old_copy.read_text() == "print('dag')\n"

    def test_missing_bucket_is_a_configuration_error_not_a_missing_release(self, fake_s3):
        store = _s3_artifact_store()

        def missing_bucket(**kwargs):
            raise FakeStoreClientError("NoSuchBucket")

        fake_s3.get_object = missing_bucket
        with pytest.raises(BundleManifestError, match="does not exist. Fix the published_root"):
            store.read_ref(missing_message="ref is gone", invalid_message="invalid")

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

    def test_auto_publish_local_source_to_object_store(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                source_path=str(source),
                source_stability_seconds=0,
            )
            bundle.initialize()
            first_version = _version_string(bundle.get_current_version())
            assert (bundle.path / "dags" / "example.py").read_text() == "print('dag')\n"
            ref = json.loads(fake_s3.objects["releases/refs/my-dags/latest.json"])
            assert ref["version"] == first_version

            (source / "dags" / "example.py").write_text("print('v2')\n")
            bundle.refresh()
            second_version = _version_string(bundle.get_current_version())
            assert second_version != first_version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('v2')\n"
            # The first release's objects stay published for pinned work.
            assert f"releases/versions/my-dags/{first_version}/{MANIFEST_FILE_NAME}" in fake_s3.objects

    def test_auto_publish_waits_for_shared_stability_over_object_store(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        initial = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        (source / "dags" / "example.py").write_text("print('v2')\n")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                source_path=str(source),
                source_stability_seconds=30,
            )
            with mock.patch.object(bundle_module.time, "time", return_value=100):
                bundle.refresh()
            # Not stable yet: the current release stays active, the candidate is shared.
            assert _version_string(bundle.get_current_version()) == initial.version
            state = json.loads(fake_s3.objects["releases/_state/my-dags/auto-publish.json"])
            assert state["first_observed_at"] == 100

            with mock.patch.object(bundle_module.time, "time", return_value=130):
                bundle.refresh()
            assert _version_string(bundle.get_current_version()) != initial.version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('v2')\n"

    def _external_candidate(self, source: Path, *, signature: str | None = None) -> bytes:
        from airflow_manifest_bundle.local import _local_source_identity
        from airflow_manifest_bundle.manifest import collect_bundle_source_snapshot

        return json.dumps(
            {
                "schema_version": 2,
                "bundle_name": "my-dags",
                "source_type": "local",
                "source_identity": _local_source_identity(source),
                "source_signature": signature or collect_bundle_source_snapshot(source).signature,
                "first_observed_at": 50.0,
            }
        ).encode()

    def _bundle_losing_candidate_race(self, fake_s3, tmp_path):
        """A bundle whose changed source needs 30s of stability, plus its initial release."""
        source = _write_source(tmp_path / "source")
        initial = _publish_to_fake_s3(fake_s3, prefix="releases", bundle_name="my-dags", source=source)
        (source / "dags" / "example.py").write_text("print('v2')\n")
        bundle = ManifestLocalDagBundle(
            name="my-dags",
            published_root=f"s3://{BUCKET}/releases",
            source_path=str(source),
            source_stability_seconds=30,
        )
        return bundle, initial, source

    def _inject_candidate_race(self, bundle, fake_s3, monkeypatch, candidate: bytes) -> None:
        """Write an external candidate between the bundle's state read and its CAS write."""
        raced = {}
        original_confirm = bundle._confirm_publish_source

        def confirm_then_lose_the_race(prepared):
            original_confirm(prepared)
            if not raced:
                raced["done"] = True
                fake_s3.put("releases/_state/my-dags/auto-publish.json", candidate)

        monkeypatch.setattr(bundle, "_confirm_publish_source", confirm_then_lose_the_race)

    def test_lost_candidate_race_adopts_a_matching_winner(self, fake_s3, tmp_path, monkeypatch):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, initial, source = self._bundle_losing_candidate_race(fake_s3, tmp_path)
            # The winner observed the same source 50 seconds before our clock reads 100.
            self._inject_candidate_race(
                bundle, fake_s3, monkeypatch, self._external_candidate(source)
            )
            with mock.patch.object(bundle_module.time, "time", return_value=100):
                bundle.refresh()

            # The adopted shared timestamp already satisfied the window: one refresh
            # published without restarting the stability period.
            assert _version_string(bundle.get_current_version()) != initial.version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('v2')\n"

    def test_lost_candidate_race_to_a_different_observation_stays_unpublished(
        self, fake_s3, tmp_path, monkeypatch
    ):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle, initial, source = self._bundle_losing_candidate_race(fake_s3, tmp_path)
            self._inject_candidate_race(
                bundle,
                fake_s3,
                monkeypatch,
                self._external_candidate(source, signature="sha256:someone-elses-view"),
            )
            with mock.patch.object(bundle_module.time, "time", return_value=100):
                bundle.refresh()

            # A winner with a different observation must not satisfy our window: using
            # its timestamp would publish a source that never proved stable.
            assert _version_string(bundle.get_current_version()) == initial.version

            # The next quiet cycles record a fresh candidate and publish normally.
            with mock.patch.object(bundle_module.time, "time", return_value=200):
                bundle.refresh()
            assert _version_string(bundle.get_current_version()) == initial.version
            with mock.patch.object(bundle_module.time, "time", return_value=230):
                bundle.refresh()
            assert _version_string(bundle.get_current_version()) != initial.version

    def test_lost_release_race_follows_the_winner(self, fake_s3, tmp_path, monkeypatch):
        source = _write_source(tmp_path / "source")
        other_source = _write_source(tmp_path / "other")
        (other_source / "dags" / "example.py").write_text("print('winner')\n")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                source_path=str(source),
                source_stability_seconds=0,
            )
            winner = {}
            original_confirm = bundle._confirm_publish_source

            def confirm_then_lose_the_race(prepared):
                original_confirm(prepared)
                # Another publisher commits a different release between this bundle's
                # reference read and its conditional write.
                if not winner:
                    winner["result"] = _publish_to_fake_s3(
                        fake_s3, prefix="releases", bundle_name="my-dags", source=other_source
                    )

            monkeypatch.setattr(bundle, "_confirm_publish_source", confirm_then_lose_the_race)
            bundle.initialize()

            assert _version_string(bundle.get_current_version()) == winner["result"].version
            assert (bundle.path / "dags" / "example.py").read_text() == "print('winner')\n"

    def test_explicit_publish_to_object_store(self, fake_s3, tmp_path):
        source = _write_source(tmp_path / "source")
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
            )
            result = publish_manifest_local_dag_bundle(bundle=bundle, source_path=source)
            assert result.created_snapshot is True
            assert str(result.manifest_ref_path) == f"s3://{BUCKET}/releases/refs/my-dags/latest.json"

            bundle.refresh()
            assert _version_string(bundle.get_current_version()) == result.version

            with pytest.raises(BundleManifestReferenceChangedError):
                publish_manifest_local_dag_bundle(
                    bundle=bundle,
                    source_path=source,
                    expected_current_version="sha256-" + "0" * 64,
                )

    def test_object_store_root_for_s3_source_logs_no_fallback_hint(
        self, fake_s3, tmp_path, caplog
    ):
        from airflow_manifest_bundle.s3 import ManifestS3DagBundle

        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            caplog.at_level("INFO", logger="airflow_manifest_bundle.s3"),
        ):
            ManifestS3DagBundle(
                name="my-dags",
                bucket_name="source-bucket",
                published_root=f"s3://{BUCKET}/releases",
            )
        assert "removes the shared filesystem" not in caplog.text

    def test_consume_only_guard_still_protects_against_non_publishing_stores(
        self, fake_s3, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(S3ArtifactStore, "supports_publication", False)
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="consume-only"),
        ):
            ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
                source_path=str(tmp_path / "source"),
            )

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

    def test_uppercase_scheme_selects_the_object_store(self, fake_s3, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"S3://{BUCKET}/releases",
            )
            assert bundle.published_root == f"s3://{BUCKET}/releases"

    @pytest.mark.parametrize("root", ["s3:/bucket/releases", "gcs://bucket/releases", "s3:releases"])
    def test_url_shaped_roots_with_unsupported_schemes_are_rejected(self, root, tmp_path):
        with (
            conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}),
            pytest.raises(TypeError, match="Unsupported published_root scheme"),
        ):
            ManifestLocalDagBundle(name="my-dags", published_root=root)

    def test_publication_lock_path_explains_object_store_roots(self, fake_s3, tmp_path):
        with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles")}):
            bundle = ManifestLocalDagBundle(
                name="my-dags",
                published_root=f"s3://{BUCKET}/releases",
            )
            with pytest.raises(AttributeError, match="no publication lock file"):
                _ = bundle.publication_lock_path

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
