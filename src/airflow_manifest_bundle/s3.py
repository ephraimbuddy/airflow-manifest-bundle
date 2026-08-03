"""Read-only S3 source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.bundle import FilesystemArtifactStore
from airflow_manifest_bundle.manifest import (
    BundleManifestError,
    BundleManifestNotFoundError,
)
from airflow_manifest_bundle.object_source import (
    DEFAULT_MAX_FILE_COUNT,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    DEFAULT_MAX_TOTAL_SIZE_BYTES,
    ObjectSourceObservation,
    ObjectStoreSourceDagBundleBase,
    _relative_object_path,
    publish_manifest_object_store_dag_bundle,
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

    @property
    def remote_name(self) -> str:
        return self.key

    def signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "key": self.key,
            "size": self.size,
            "last_modified": self.last_modified,
            "etag": self.etag,
        }


@dataclass(frozen=True)
class S3SourceObservation(ObjectSourceObservation):
    """One canonical observation of an S3 source folder."""


class ManifestS3DagBundle(ObjectStoreSourceDagBundleBase):
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

    _source_type = "s3"
    _source_label = "S3"
    _remote_name_noun = "object key"
    _source_url_scheme = "s3"
    _mirror_dir_name = "_s3_source"
    _mirror_state_file_name = S3_MIRROR_STATE_FILE_NAME
    _observation_schema_version = S3_SOURCE_OBSERVATION_SCHEMA_VERSION
    _mirror_state_schema_version = S3_MIRROR_STATE_SCHEMA_VERSION
    _observation_class = S3SourceObservation
    _marker_key_requirement = "a non-empty relative S3 key"
    _marker_key_safe_requirement = "a safe relative S3 object key"

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
        if not isinstance(aws_conn_id, str) or not aws_conn_id:
            raise TypeError("aws_conn_id must be a non-empty string")
        super().__init__(
            bucket_name=bucket_name,
            prefix=prefix,
            deployment_marker_key=deployment_marker_key,
            max_file_count=max_file_count,
            max_file_size_bytes=max_file_size_bytes,
            max_total_size_bytes=max_total_size_bytes,
            auto_publish=auto_publish,
            **kwargs,
        )
        self.aws_conn_id = aws_conn_id
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
        self._s3_hook: Any | None = None

    @property
    def s3_dags_dir(self) -> Path:
        return self._source_mirror_dir

    @property
    def s3_mirror_state_path(self) -> Path:
        return self._mirror_state_path

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

    def _publish_copy_hints(
        self, client: Any, observation: ObjectSourceObservation
    ) -> Mapping[str, dict[str, Any]] | None:
        # Transport hints for an object-store published_root: each observed object,
        # pinned to its ETag, so the store can server-side copy instead of uploading
        # the mirror bytes. Optimization only — any hint may fail back to upload.
        endpoint = self._source_endpoint(client)
        return {
            entry.relative_path: {
                "type": "s3",
                "endpoint": endpoint,
                "bucket": self.bucket_name,
                "key": entry.key,
                "etag": entry.etag,
            }
            for entry in observation.entries
        }

    def _validate_source_configuration(self, client: Any) -> None:
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
        with self._translate_source_error("validate source"):
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

    def _get_source_client(self) -> Any:
        with self._translate_source_error("create client"):
            return self.s3_hook.get_conn()

    # Kept under its historical name as well: external tooling and tests reach
    # for the adapter-specific client accessor.
    _get_s3_client = _get_source_client

    def _source_endpoint(self, client: Any) -> str | None:
        endpoint = getattr(getattr(client, "meta", None), "endpoint_url", None)
        return endpoint if isinstance(endpoint, str) else None

    def _list_source_objects(self, client: Any) -> Iterator[S3ObjectObservation]:
        with self._translate_source_error("list source"):
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
                    if observation is not None:
                        yield observation

    def _read_deployment_marker(self, client: Any) -> S3ObjectObservation | None:
        marker_key = self._marker_remote_name
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

        relative_path = _relative_object_path(
            name=key,
            normalized_prefix=self._normalized_prefix,
            label=self._source_label,
            noun=self._remote_name_noun,
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

    def _download_entry(self, client: Any, *, entry: Any, tmp_path: Path) -> None:
        with self._translate_source_error("download object"):
            client.download_file(self.bucket_name, entry.key, str(tmp_path))


def publish_manifest_s3_dag_bundle(
    *,
    bundle: ManifestS3DagBundle,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish the configured S3 source as an immutable manifest-backed snapshot."""
    return publish_manifest_object_store_dag_bundle(
        bundle=bundle,
        expected_current_version=expected_current_version,
    )


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
