"""Read-only S3 source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle._compat import remove_bundle_tree_forcefully
from airflow_manifest_bundle.bundle import (
    BundleManifestRef,
    FilesystemArtifactStore,
    ManifestDagBundleBase,
    PreparedPublishSource,
    _write_json_atomically,
    publish_prepared_manifest_dag_bundle,
)
from airflow_manifest_bundle.manifest import (
    BundleManifestError,
    BundleManifestNotFoundError,
    BundleManifestSourceChangedError,
    BundleSourceSnapshot,
    collect_bundle_source_snapshot,
    compute_file_sha256,
    is_ignored_bundle_relative_path,
)

if TYPE_CHECKING:
    from airflow_manifest_bundle.bundle import BundlePublishResult

try:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ModuleNotFoundError:
    AwsBaseHook = None
    S3Hook = None

DEFAULT_AWS_CONN_ID = (
    AwsBaseHook.default_conn_name if AwsBaseHook is not None else "aws_default"
)
DEFAULT_MAX_FILE_COUNT = 10_000
DEFAULT_MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_TOTAL_SIZE_BYTES = 1024 * 1024 * 1024
S3_SOURCE_OBSERVATION_SCHEMA_VERSION = 1
S3_MIRROR_STATE_SCHEMA_VERSION = 1
S3_MIRROR_STATE_FILE_NAME = "_s3_source_state.json"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3ObjectObservation:
    """One safe S3 object and its remote change token."""

    relative_path: str
    key: str
    size: int
    last_modified: str
    etag: str

    def signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "key": self.key,
            "size": self.size,
            "last_modified": self.last_modified,
            "etag": self.etag,
        }


@dataclass(frozen=True)
class S3SourceObservation:
    """One canonical observation of an S3 source folder."""

    entries: tuple[S3ObjectObservation, ...]
    inventory_signature: str
    candidate_signature: str
    marker_signature: str | None


class ManifestS3DagBundle(ManifestDagBundleBase):
    """
    Mirror an S3 folder into local staging and publish immutable snapshots.

    The S3 folder is the mutable source, never parsed or executed from directly.
    Releases go to ``published_root``: an ``s3://`` prefix (recommended — pinned
    execution then reads the releases prefix with the artifact store's S3 client),
    or a shared filesystem path (pinned execution then needs no S3 access at all).

    :param max_file_count: Maximum number of included objects in one source observation.
    :param max_file_size_bytes: Maximum size of one included object.
    :param max_total_size_bytes: Maximum total size of all included objects.
    :param auto_publish: Publish source changes during unpinned refreshes. Disable this
        option when the standalone publisher command controls releases.
    """

    def __init__(
        self,
        *,
        aws_conn_id: str = DEFAULT_AWS_CONN_ID,
        bucket_name: str,
        prefix: str = "",
        deployment_marker_key: str | None = None,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        max_total_size_bytes: int = DEFAULT_MAX_TOTAL_SIZE_BYTES,
        auto_publish: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not isinstance(aws_conn_id, str) or not aws_conn_id:
            raise TypeError("aws_conn_id must be a non-empty string")
        if not isinstance(bucket_name, str) or not bucket_name:
            raise TypeError("bucket_name must be a non-empty string")
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")
        if deployment_marker_key is not None:
            if not isinstance(deployment_marker_key, str) or not deployment_marker_key:
                raise TypeError("deployment_marker_key must be a non-empty relative S3 key")
            _validate_marker_relative_key(deployment_marker_key)
        _validate_positive_limit("max_file_count", max_file_count)
        _validate_positive_limit("max_file_size_bytes", max_file_size_bytes)
        _validate_positive_limit("max_total_size_bytes", max_total_size_bytes)
        if not isinstance(auto_publish, bool):
            raise TypeError("auto_publish must be a boolean")

        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.deployment_marker_key = deployment_marker_key
        self.max_file_count = max_file_count
        self.max_file_size_bytes = max_file_size_bytes
        self.max_total_size_bytes = max_total_size_bytes
        self.auto_publish = auto_publish
        if auto_publish and not self._store.supports_publication:
            raise TypeError(
                "auto_publish requires a published_root that supports publication; the "
                "configured published_root is consume-only. Set auto_publish=False or use "
                "a published_root that publishers can write."
            )
        # Advisory for dag processors and publisher hosts only: pinned bundles are
        # constructed for every task, and workers never publish.
        if self.version is None and isinstance(self._store, FilesystemArtifactStore):
            log.info(
                "Bundle '%s' reads its Dag source from S3 but publishes releases to the "
                "filesystem published_root %s. An s3:// published_root removes the shared "
                "filesystem; see the S3 operator guide.",
                self.name,
                self.published_root,
            )
        self._normalized_prefix = prefix.rstrip("/")
        self._marker_object_key = (
            _join_s3_key(self._normalized_prefix, deployment_marker_key)
            if deployment_marker_key is not None
            else None
        )
        self.s3_dags_dir = self.base_dir / "_s3_source"
        self.s3_mirror_state_path = self.base_dir / S3_MIRROR_STATE_FILE_NAME
        self._s3_hook: Any | None = None
        self._s3_configuration_checked = False

    @property
    def _has_publish_source(self) -> bool:
        return self.auto_publish

    @property
    def _publish_source_description(self) -> str:
        suffix = f"/{self.prefix}" if self.prefix else ""
        return f"s3://{self.bucket_name}{suffix}"

    @property
    def s3_hook(self) -> Any:
        """Construct the provider hook only when an unpinned source operation needs it."""
        if self._s3_hook is None:
            if S3Hook is None:
                raise BundleManifestError(
                    "ManifestS3DagBundle requires the Amazon provider. Install "
                    "'airflow-manifest-bundle[s3]'."
                )
            try:
                self._s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            except Exception as e:
                raise BundleManifestError(
                    f"Could not create an S3 client for bucket {self.bucket_name!r}"
                ) from e
        return self._s3_hook

    def __repr__(self) -> str:
        return (
            f"<ManifestS3DagBundle(name={self.name!r}, bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, version={self.version!r})>"
        )

    def view_url(self, version: str | None = None) -> str | None:
        """Return the mutable S3 source URL for Airflow releases that use this method."""
        if version is not None:
            return None
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        if self.version:
            return None
        # getattr: Airflow 3.0's BaseDagBundle does not set _view_url_template
        # (the attribute arrived in 3.1.0).
        configured_template = getattr(self, "_view_url_template", None)
        if configured_template:
            return configured_template
        url = f"https://{self.bucket_name}.s3"
        region_name = None
        if self.auto_publish:
            try:
                region_name = self.s3_hook.region_name
            except Exception:  # noqa: BLE001 - the optional UI URL must not block bundle use
                region_name = None
        if region_name:
            url += f".{region_name}"
        url += ".amazonaws.com"
        if self.prefix:
            url += f"/{self.prefix}"
        return url

    def _prepare_publish_source(self) -> PreparedPublishSource:
        self._check_s3_configuration()
        client = self._get_s3_client()
        observation = self._collect_source_observation(client)
        source_identity = self._source_identity(client)
        snapshot, file_sha256_by_path = self._synchronize_mirror(
            client=client,
            source_identity=source_identity,
            observation=observation,
        )
        release_source_metadata: dict[str, Any] = {
            "type": "s3",
            "identity": source_identity,
            "observation": observation.inventory_signature,
        }
        if observation.marker_signature is not None:
            release_source_metadata["deployment_marker"] = observation.marker_signature
        # Transport hints for an object-store published_root: each observed object,
        # pinned to its ETag, so the store can server-side copy instead of uploading
        # the mirror bytes. Optimization only — any hint may fail back to upload.
        endpoint = getattr(getattr(client, "meta", None), "endpoint_url", None)
        copy_hints = {
            entry.relative_path: {
                "type": "s3",
                "endpoint": endpoint if isinstance(endpoint, str) else None,
                "bucket": self.bucket_name,
                "key": entry.key,
                "etag": entry.etag,
            }
            for entry in observation.entries
        }
        return PreparedPublishSource(
            root=self.s3_dags_dir,
            source_snapshot=snapshot,
            source_type="s3",
            source_identity=source_identity,
            source_signature=observation.candidate_signature,
            file_sha256_by_path=file_sha256_by_path,
            confirmation_data=observation,
            release_source_metadata=release_source_metadata,
            copy_hints=copy_hints,
        )

    def _publish_from_source_if_ready(
        self,
        current_ref: BundleManifestRef | None,
    ) -> BundleManifestRef | None:
        # Keep one host's mutable mirror stable through hashing, snapshot copy, and
        # the final remote confirmation. The shared publication lock still protects
        # cross-host snapshot and reference updates.
        with self.lock():
            return super()._publish_from_source_if_ready(current_ref)

    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        expected = prepared.confirmation_data
        if not isinstance(expected, S3SourceObservation):
            raise BundleManifestError("Prepared S3 source does not contain a remote observation")
        current_snapshot = collect_bundle_source_snapshot(prepared.root)
        if current_snapshot.signature != prepared.source_snapshot.signature:
            raise BundleManifestSourceChangedError(
                f"S3 mirror changed while publishing bundle '{self.name}'"
            )
        current = self._collect_source_observation(self._get_s3_client())
        if current != expected:
            raise BundleManifestSourceChangedError(
                f"S3 source changed while publishing bundle '{self.name}'"
            )

    def _validate_release_transition(
        self,
        *,
        current_ref: BundleManifestRef | None,
        prepared: PreparedPublishSource,
        target_version: str,
    ) -> None:
        if current_ref is None:
            return
        current_source = current_ref.ref_payload.get("source")
        prepared_source = prepared.release_source_metadata
        if not isinstance(current_source, dict) or not isinstance(prepared_source, dict):
            return
        if (
            current_source.get("type") == "s3"
            and current_source.get("identity") != prepared_source.get("identity")
        ):
            raise BundleManifestError(
                f"Current release for bundle '{self.name}' records a different S3 source"
            )
        if self.deployment_marker_key is None:
            return
        if (
            current_source.get("observation") != prepared_source.get("observation")
            and current_source.get("deployment_marker")
            == prepared_source.get("deployment_marker")
        ):
            raise BundleManifestSourceChangedError(
                f"S3 objects for bundle '{self.name}' changed without a new deployment marker"
            )

    def _current_release_matches_source(
        self,
        *,
        current_ref: BundleManifestRef,
        prepared: PreparedPublishSource,
    ) -> bool:
        return current_ref.ref_payload.get("source") == prepared.release_source_metadata

    def _check_s3_configuration(self) -> None:
        if self._s3_configuration_checked:
            return
        client = self._get_s3_client()
        try:
            client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            if _is_missing_s3_object_error(e):
                raise BundleManifestNotFoundError(
                    f"S3 bucket {self.bucket_name!r} does not exist"
                ) from e
            raise BundleManifestError(
                f"Could not access S3 bucket {self.bucket_name!r}"
            ) from e
        with self._translate_s3_error("validate source"):
            if (
                self.prefix
                and not self.allow_empty_source
                and not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name,
                    prefix=self.prefix,
                    delimiter="/",
                )
            ):
                raise BundleManifestNotFoundError(
                    f"S3 prefix {self._publish_source_description!r} does not exist"
                )
        self._s3_configuration_checked = True

    def _get_s3_client(self) -> Any:
        with self._translate_s3_error("create client"):
            return self.s3_hook.get_conn()

    def _source_identity(self, client: Any) -> str:
        endpoint = getattr(getattr(client, "meta", None), "endpoint_url", None)
        payload = {
            "schema_version": S3_SOURCE_OBSERVATION_SCHEMA_VERSION,
            "endpoint": endpoint if isinstance(endpoint, str) else None,
            "bucket": self.bucket_name,
            "prefix": self._normalized_prefix,
        }
        return _sha256_json(payload)

    def _collect_source_observation(self, client: Any) -> S3SourceObservation:
        entries: list[S3ObjectObservation] = []
        total_size = 0
        marker_before = self._read_deployment_marker(client)
        with self._translate_s3_error("list source"):
            paginator = client.get_paginator("list_objects_v2")
            listing_prefix = (
                f"{self._normalized_prefix}/" if self._normalized_prefix else ""
            )
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=listing_prefix,
            )
            for page in pages:
                contents = page.get("Contents", [])
                if not isinstance(contents, list):
                    raise BundleManifestError("S3 listing returned invalid object metadata")
                for object_data in contents:
                    observation = self._object_observation(object_data)
                    if observation is None:
                        continue
                    if observation.key == self._marker_object_key:
                        continue
                    if is_ignored_bundle_relative_path(observation.relative_path):
                        continue
                    if observation.size > self.max_file_size_bytes:
                        raise BundleManifestError(
                            f"S3 object {observation.key!r} has size {observation.size}, "
                            f"which exceeds max_file_size_bytes={self.max_file_size_bytes}"
                        )
                    if len(entries) >= self.max_file_count:
                        raise BundleManifestError(
                            f"S3 source for bundle '{self.name}' exceeds "
                            f"max_file_count={self.max_file_count}"
                        )
                    total_size += observation.size
                    if total_size > self.max_total_size_bytes:
                        raise BundleManifestError(
                            f"S3 source for bundle '{self.name}' has total size "
                            f"{total_size}, which exceeds "
                            f"max_total_size_bytes={self.max_total_size_bytes}"
                        )
                    entries.append(observation)

        entries.sort(key=lambda entry: entry.relative_path)
        _validate_s3_inventory_paths(entries)
        marker_after = self._read_deployment_marker(client)
        if marker_before != marker_after:
            raise BundleManifestSourceChangedError(
                f"S3 deployment marker changed while observing bundle '{self.name}'"
            )
        marker = marker_after

        inventory_signature = _sha256_json(
            {
                "schema_version": S3_SOURCE_OBSERVATION_SCHEMA_VERSION,
                "files": [entry.signature_record() for entry in entries],
            }
        )
        marker_signature = (
            _sha256_json(
                {
                    "schema_version": S3_SOURCE_OBSERVATION_SCHEMA_VERSION,
                    "marker": marker.signature_record(),
                }
            )
            if marker is not None
            else None
        )
        candidate_signature = _sha256_json(
            {
                "schema_version": S3_SOURCE_OBSERVATION_SCHEMA_VERSION,
                "inventory": inventory_signature,
                "deployment_marker": marker_signature,
            }
        )
        return S3SourceObservation(
            entries=tuple(entries),
            inventory_signature=inventory_signature,
            candidate_signature=candidate_signature,
            marker_signature=marker_signature,
        )

    def _read_deployment_marker(self, client: Any) -> S3ObjectObservation | None:
        marker_key = self._marker_object_key
        if marker_key is None:
            return None
        try:
            response = client.head_object(Bucket=self.bucket_name, Key=marker_key)
        except Exception as e:
            if _is_missing_s3_object_error(e):
                raise BundleManifestNotFoundError(
                    f"Required S3 deployment marker {marker_key!r} is missing "
                    f"from bucket {self.bucket_name!r}"
                ) from e
            raise BundleManifestError(
                f"Could not read the deployment marker for S3 source "
                f"{self._publish_source_description}"
            ) from e
        return self._object_observation(
            {
                "Key": marker_key,
                "Size": response.get("ContentLength"),
                "ETag": response.get("ETag"),
                "LastModified": response.get("LastModified"),
            }
        )

    def _object_observation(self, object_data: Any) -> S3ObjectObservation | None:
        if not isinstance(object_data, dict):
            raise BundleManifestError("S3 listing returned invalid object metadata")
        key = object_data.get("Key")
        size = object_data.get("Size")
        etag = object_data.get("ETag")
        last_modified = object_data.get("LastModified")
        if (
            not isinstance(key, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(etag, str)
            or not etag
        ):
            raise BundleManifestError("S3 listing returned invalid object metadata")
        if hasattr(last_modified, "isoformat"):
            last_modified = last_modified.isoformat()
        if not isinstance(last_modified, str) or not last_modified:
            raise BundleManifestError("S3 listing returned invalid object metadata")

        relative_path = _relative_s3_object_path(
            key=key,
            normalized_prefix=self._normalized_prefix,
        )
        if relative_path is None:
            return None
        return S3ObjectObservation(
            relative_path=relative_path,
            key=key,
            size=size,
            last_modified=last_modified,
            etag=etag,
        )

    def _synchronize_mirror(
        self,
        *,
        client: Any,
        source_identity: str,
        observation: S3SourceObservation,
    ) -> tuple[BundleSourceSnapshot, dict[str, str]]:
        state = self._read_mirror_state()
        state_entries = _state_entries_by_path(state, source_identity=source_identity)
        mirror_changed = False
        if state_entries is None or not self._mirror_tree_is_safe():
            self._reset_mirror()
            state_entries = {}
            mirror_changed = True
        else:
            self.s3_dags_dir.mkdir(parents=True, exist_ok=True)

        verify_reused_content = not self._can_skip_reused_content_check(
            source_identity=source_identity,
            observation=observation,
        )
        expected_paths = {entry.relative_path for entry in observation.entries}
        observed_paths = self._observed_mirror_files()
        self._normalize_mirror_directories()
        for stale_path in sorted(observed_paths - expected_paths, reverse=True):
            (self.s3_dags_dir / stale_path).unlink()
            mirror_changed = True
        self._remove_empty_mirror_directories()

        local_records: list[dict[str, Any]] = []
        file_sha256_by_path: dict[str, str] = {}
        downloaded_files = 0
        downloaded_bytes = 0
        for entry in observation.entries:
            destination = self.s3_dags_dir / entry.relative_path
            self._ensure_destination_is_within_root(self.s3_dags_dir, destination)
            old_record = state_entries.get(entry.relative_path)
            reused = _can_reuse_mirror_file(destination, entry=entry, state=old_record)
            verified_reused_file: tuple[str, int] | None = None
            if reused and verify_reused_content:
                verified_reused_file = compute_file_sha256(destination)
                expected_sha256 = old_record["local_sha256"].removeprefix("sha256:")
                if verified_reused_file[0] != expected_sha256:
                    reused = False
                    verified_reused_file = None
            if not reused:
                self._download_object(client, entry=entry, destination=destination)
                downloaded_files += 1
                downloaded_bytes += entry.size
                mirror_changed = True
            destination.chmod(0o644)
            if reused:
                if verified_reused_file is not None:
                    file_sha256, actual_size = verified_reused_file
                else:
                    file_sha256 = old_record["local_sha256"].removeprefix("sha256:")
                    actual_size = destination.stat().st_size
            else:
                file_sha256, actual_size = compute_file_sha256(destination)
            if actual_size != entry.size:
                raise BundleManifestSourceChangedError(
                    f"Downloaded S3 object {entry.key!r} has size {actual_size}, "
                    f"expected {entry.size}"
                )
            local_records.append(
                {
                    **entry.signature_record(),
                    "local_mtime_ns": destination.stat().st_mtime_ns,
                    "local_sha256": f"sha256:{file_sha256}",
                }
            )
            file_sha256_by_path[entry.relative_path] = file_sha256

        self._normalize_mirror_directories()
        if mirror_changed:
            confirmed = self._collect_source_observation(client)
            if confirmed != observation:
                raise BundleManifestSourceChangedError(
                    f"S3 source changed while synchronizing bundle '{self.name}'"
                )
        self._validate_mirror_against_inventory(observation)
        snapshot = collect_bundle_source_snapshot(self.s3_dags_dir)
        _write_json_atomically(
            self.s3_mirror_state_path,
            {
                "schema_version": S3_MIRROR_STATE_SCHEMA_VERSION,
                "source_identity": source_identity,
                "source_signature": observation.candidate_signature,
                "files": local_records,
            },
        )
        if downloaded_files:
            log.info(
                "Synchronized S3 bundle mirror. bundle=%s downloaded_files=%d downloaded_bytes=%d",
                self.name,
                downloaded_files,
                downloaded_bytes,
            )
        return snapshot, file_sha256_by_path

    def _can_skip_reused_content_check(
        self,
        *,
        source_identity: str,
        observation: S3SourceObservation,
    ) -> bool:
        confirmed_version = self._confirmed_source_version
        return (
            self._confirmed_source_key
            == ("s3", source_identity, observation.candidate_signature)
            and confirmed_version is not None
            and self._published_snapshot_exists(confirmed_version)
        )

    def _read_mirror_state(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.s3_mirror_state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError, OSError):
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _reset_mirror(self) -> None:
        if self.s3_dags_dir.exists() or self.s3_dags_dir.is_symlink():
            remove_bundle_tree_forcefully(self.s3_dags_dir)
        self.s3_dags_dir.mkdir(parents=True, mode=0o755)

    def _mirror_tree_is_safe(self) -> bool:
        if not self.s3_dags_dir.exists():
            return False
        if self.s3_dags_dir.is_symlink() or not self.s3_dags_dir.is_dir():
            return False
        try:
            self._observed_mirror_files()
        except BundleManifestError:
            return False
        return True

    def _observed_mirror_files(self) -> set[str]:
        observed: set[str] = set()
        if not self.s3_dags_dir.exists():
            return observed
        for dirpath, dirnames, filenames in os.walk(self.s3_dags_dir, followlinks=False):
            root = Path(dirpath)
            for dirname in dirnames:
                path = root / dirname
                if path.is_symlink() or not path.is_dir():
                    raise BundleManifestError(f"S3 mirror contains unsafe directory {path}")
            for filename in filenames:
                path = root / filename
                if path.is_symlink():
                    raise BundleManifestError(f"S3 mirror contains symlink {path}")
                try:
                    file_mode = path.stat().st_mode
                except FileNotFoundError as e:
                    raise BundleManifestSourceChangedError(
                        f"S3 mirror file disappeared during validation: {path}"
                    ) from e
                if not stat.S_ISREG(file_mode):
                    raise BundleManifestError(f"S3 mirror contains non-regular file {path}")
                observed.add(path.relative_to(self.s3_dags_dir).as_posix())
        return observed

    def _download_object(
        self,
        client: Any,
        *,
        entry: S3ObjectObservation,
        destination: Path,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            dir=destination.parent,
        )
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            with self._translate_s3_error("download object"):
                client.download_file(self.bucket_name, entry.key, str(tmp_path))
            if tmp_path.is_symlink() or not tmp_path.is_file():
                raise BundleManifestError(
                    f"S3 download for object {entry.key!r} did not produce a regular file"
                )
            os.replace(tmp_path, destination)
        finally:
            with suppress(OSError):
                tmp_path.unlink(missing_ok=True)

    def _validate_mirror_against_inventory(self, observation: S3SourceObservation) -> None:
        expected = {entry.relative_path: entry.size for entry in observation.entries}
        observed = self._observed_mirror_files()
        if observed != set(expected):
            raise BundleManifestSourceChangedError(
                f"S3 mirror file set does not match the remote inventory for bundle '{self.name}'"
            )
        for relative_path, expected_size in expected.items():
            if (self.s3_dags_dir / relative_path).stat().st_size != expected_size:
                raise BundleManifestSourceChangedError(
                    f"S3 mirror file {relative_path!r} has the wrong size"
                )

    def _remove_empty_mirror_directories(self) -> None:
        for dirpath, _, _ in os.walk(self.s3_dags_dir, topdown=False):
            path = Path(dirpath)
            if path == self.s3_dags_dir:
                continue
            with suppress(OSError):
                path.rmdir()

    def _normalize_mirror_directories(self) -> None:
        self._normalize_mirror_directory(self.s3_dags_dir)
        for dirpath, dirnames, _ in os.walk(self.s3_dags_dir, followlinks=False):
            self._normalize_mirror_directory(Path(dirpath))
            for dirname in dirnames:
                self._normalize_mirror_directory(Path(dirpath) / dirname)

    @staticmethod
    def _normalize_mirror_directory(path: Path) -> None:
        try:
            mode = path.lstat().st_mode
        except OSError as e:
            raise BundleManifestError(f"Could not inspect S3 mirror directory {path}") from e
        if not stat.S_ISDIR(mode):
            raise BundleManifestError(f"S3 mirror contains unsafe directory {path}")

        try:
            directory_fd = os.open(
                path,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
        except OSError as e:
            raise BundleManifestError(
                f"Could not open S3 mirror directory safely: {path}"
            ) from e
        try:
            if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
                raise BundleManifestError(f"S3 mirror contains unsafe directory {path}")
            os.fchmod(directory_fd, 0o755)
        except OSError as e:
            raise BundleManifestError(
                f"Could not normalize S3 mirror directory permissions: {path}"
            ) from e
        finally:
            os.close(directory_fd)

    @contextmanager
    def _translate_s3_error(self, operation: str) -> Iterator[None]:
        try:
            yield
        except BundleManifestError:
            raise
        except Exception as e:
            raise BundleManifestError(
                f"Could not {operation} for S3 source {self._publish_source_description}"
            ) from e


def publish_manifest_s3_dag_bundle(
    *,
    bundle: ManifestS3DagBundle,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish the configured S3 source as an immutable manifest-backed snapshot."""
    if bundle.auto_publish:
        raise BundleManifestError(
            f"Bundle '{bundle.name}' has auto_publish enabled. Set auto_publish=False "
            "before using the explicit S3 publisher."
        )
    if bundle.version:
        raise BundleManifestError(
            f"Cannot explicitly publish pinned bundle '{bundle.name}' at version "
            f"{bundle.version!r}"
        )

    # The mirror is mutable host-local staging. Keep the Airflow bundle lock until
    # publication finishes its final local and remote source confirmation.
    with bundle.lock():
        prepared = bundle._prepare_publish_source()
        return publish_prepared_manifest_dag_bundle(
            bundle=bundle,
            prepared_source=prepared,
            expected_current_version=expected_current_version,
        )


def _validate_marker_relative_key(key: str) -> None:
    if key.startswith("/") or key.endswith("/") or "\\" in key:
        raise TypeError("deployment_marker_key must be a safe relative S3 object key")
    parts = key.split("/")
    if any(
        not part
        or part in {".", ".."}
        or any(ord(character) < 32 or ord(character) == 127 for character in part)
        for part in parts
    ):
        raise TypeError("deployment_marker_key must be a safe relative S3 object key")


def _validate_positive_limit(name: str, value: Any) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TypeError(f"{name} must be a positive integer")


def _join_s3_key(prefix: str, key: str) -> str:
    return f"{prefix}/{key}" if prefix else key


def _relative_s3_object_path(*, key: str, normalized_prefix: str) -> str | None:
    if "\x00" in key or "\\" in key or any(
        ord(character) < 32 or ord(character) == 127 for character in key
    ):
        raise BundleManifestError(f"S3 source contains unsafe object key {key!r}")
    prefix_with_separator = f"{normalized_prefix}/" if normalized_prefix else ""
    if normalized_prefix:
        if key == normalized_prefix or key == prefix_with_separator:
            return None
        if not key.startswith(prefix_with_separator):
            raise BundleManifestError(
                f"S3 object key {key!r} is outside normalized prefix {normalized_prefix!r}"
            )
        relative_path = key[len(prefix_with_separator) :]
    else:
        relative_path = key
    if relative_path.endswith("/"):
        directory_path = relative_path[:-1]
        if not directory_path:
            return None
        parts = directory_path.split("/")
        if any(not part or part in {".", ".."} for part in parts):
            raise BundleManifestError(f"S3 source contains unsafe directory marker {key!r}")
        return None
    parts = relative_path.split("/")
    if (
        not relative_path
        or relative_path.startswith("/")
        or any(not part or part in {".", ".."} for part in parts)
    ):
        raise BundleManifestError(f"S3 source contains unsafe object key {key!r}")
    return "/".join(parts)


def _validate_s3_inventory_paths(entries: list[S3ObjectObservation]) -> None:
    seen: set[str] = set()
    for path in sorted(entry.relative_path for entry in entries):
        if path in seen:
            raise BundleManifestError(f"S3 source contains duplicate path {path!r}")
        parts = path.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            if parent in seen:
                raise BundleManifestError(
                    f"S3 source path {path!r} collides with file {parent!r}"
                )
        seen.add(path)


def _state_entries_by_path(
    state: dict[str, Any] | None,
    *,
    source_identity: str,
) -> dict[str, dict[str, Any]] | None:
    if (
        state is None
        or state.get("schema_version") != S3_MIRROR_STATE_SCHEMA_VERSION
        or state.get("source_identity") != source_identity
        or not isinstance(state.get("source_signature"), str)
        or not isinstance(state.get("files"), list)
    ):
        return None
    result: dict[str, dict[str, Any]] = {}
    for item in state["files"]:
        if not isinstance(item, dict):
            return None
        path = item.get("path")
        if not isinstance(path, str) or not path or path in result:
            return None
        result[path] = item
    return result


def _can_reuse_mirror_file(
    path: Path,
    *,
    entry: S3ObjectObservation,
    state: dict[str, Any] | None,
) -> bool:
    if state is None or path.is_symlink():
        return False
    try:
        file_stat = path.stat()
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(file_stat.st_mode):
        return False
    local_sha256 = state.get("local_sha256")
    return (
        state.get("key") == entry.key
        and state.get("size") == entry.size == file_stat.st_size
        and state.get("etag") == entry.etag
        and state.get("last_modified") == entry.last_modified
        and state.get("local_mtime_ns") == file_stat.st_mtime_ns
        and isinstance(local_sha256, str)
        and len(local_sha256) == 71
        and local_sha256.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in local_sha256[7:])
    )


def _sha256_json(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(serialized).hexdigest()}"


def _is_missing_s3_object_error(error: Exception) -> bool:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    error_data = response.get("Error")
    if not isinstance(error_data, dict):
        return False
    return str(error_data.get("Code")) in {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}


__all__ = [
    "DEFAULT_AWS_CONN_ID",
    "DEFAULT_MAX_FILE_COUNT",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "DEFAULT_MAX_TOTAL_SIZE_BYTES",
    "ManifestS3DagBundle",
    "S3ObjectObservation",
    "S3SourceObservation",
    "publish_manifest_s3_dag_bundle",
]
