"""Read-only GCS source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.bundle import FilesystemArtifactStore
from airflow_manifest_bundle.manifest import (
    BundleManifestError,
    BundleManifestNotFoundError,
    BundleManifestSourceChangedError,
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
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
except ModuleNotFoundError:
    GCSHook = None
    GoogleBaseHook = None

DEFAULT_GCP_CONN_ID = (
    GoogleBaseHook.default_conn_name
    if GoogleBaseHook is not None
    else "google_cloud_default"
)
GCS_SOURCE_OBSERVATION_SCHEMA_VERSION = 1
GCS_MIRROR_STATE_SCHEMA_VERSION = 1
GCS_MIRROR_STATE_FILE_NAME = "_gcs_source_state.json"
#: The library-default public endpoint. It is normalized out of the source
#: identity so the identity of a default deployment never depends on how a
#: particular google-cloud-storage release spells its internal endpoint
#: attributes; only an explicitly configured custom endpoint differentiates.
GCS_PUBLIC_API_ENDPOINT = "https://storage.googleapis.com"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GCSObjectObservation:
    """One safe GCS object and the metadata that identifies its generation."""

    relative_path: str
    name: str
    size: int
    generation: int
    metageneration: int
    updated: str
    etag: str

    @property
    def remote_name(self) -> str:
        return self.name

    def signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "name": self.name,
            "size": self.size,
            "generation": self.generation,
            "metageneration": self.metageneration,
            "updated": self.updated,
            "etag": self.etag,
        }


@dataclass(frozen=True)
class GCSSourceObservation(ObjectSourceObservation):
    """One canonical observation of a GCS source folder."""


class ManifestGCSDagBundle(ObjectStoreSourceDagBundleBase):
    """
    Mirror a GCS folder into local staging and publish immutable snapshots.

    The GCS folder is the mutable source, never parsed or executed from directly.
    Releases go to ``published_root``, which must be a durable shared filesystem
    path: this adapter does not support object-store published roots yet.

    :param max_file_count: Maximum number of included objects in one source observation.
    :param max_file_size_bytes: Maximum size of one included object.
    :param max_total_size_bytes: Maximum total size of all included objects.
    :param auto_publish: Publish source changes during unpinned refreshes. Disable this
        option when the standalone publisher command controls releases.
    """

    _source_type = "gcs"
    _source_label = "GCS"
    _remote_name_noun = "object name"
    _source_url_scheme = "gs"
    _mirror_dir_name = "_gcs_source"
    _mirror_state_file_name = GCS_MIRROR_STATE_FILE_NAME
    _observation_schema_version = GCS_SOURCE_OBSERVATION_SCHEMA_VERSION
    _mirror_state_schema_version = GCS_MIRROR_STATE_SCHEMA_VERSION
    _observation_class = GCSSourceObservation
    _marker_key_requirement = "a non-empty relative GCS object name"
    _marker_key_safe_requirement = "a safe relative GCS object name"

    def __init__(
        self,
        *,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        bucket_name: str,
        prefix: str = "",
        deployment_marker_key: str | None = None,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        max_total_size_bytes: int = DEFAULT_MAX_TOTAL_SIZE_BYTES,
        auto_publish: bool = True,
        **kwargs: Any,
    ) -> None:
        if not isinstance(gcp_conn_id, str) or not gcp_conn_id:
            raise TypeError("gcp_conn_id must be a non-empty string")
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
        if not isinstance(self._store, FilesystemArtifactStore):
            raise TypeError(
                "ManifestGCSDagBundle supports only a filesystem published_root; got "
                f"{self.published_root!r}. Object-store published roots are not "
                "supported for the GCS adapter yet."
            )
        self.gcp_conn_id = gcp_conn_id
        self._gcs_hook: Any | None = None

    @property
    def gcs_dags_dir(self) -> Path:
        return self._source_mirror_dir

    @property
    def gcs_mirror_state_path(self) -> Path:
        return self._mirror_state_path

    @property
    def gcs_hook(self) -> Any:
        """Construct the provider hook only when an unpinned source operation needs it."""
        if self._gcs_hook is None:
            if GCSHook is None:
                raise BundleManifestError(
                    "ManifestGCSDagBundle requires the Google provider. Install "
                    "'airflow-manifest-bundle[gcs]'."
                )
            try:
                self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            except Exception as e:
                raise BundleManifestError(
                    f"Could not create a GCS client for bucket {self.bucket_name!r}"
                ) from e
        return self._gcs_hook

    def view_url_template(self) -> str | None:
        if self.version:
            return None
        # getattr: Airflow 3.0's BaseDagBundle does not set _view_url_template
        # (the attribute arrived in 3.1.0).
        configured_template = getattr(self, "_view_url_template", None)
        if configured_template:
            return configured_template
        url = f"https://console.cloud.google.com/storage/browser/{self.bucket_name}"
        if self.prefix:
            url += f"/{self.prefix}"
        return url

    def _validate_source_configuration(self, client: Any) -> None:
        try:
            client.get_bucket(self.bucket_name)
        except Exception as e:
            if _is_missing_gcs_error(e):
                raise BundleManifestNotFoundError(
                    f"GCS bucket {self.bucket_name!r} does not exist"
                ) from e
            raise BundleManifestError(
                f"Could not access GCS bucket {self.bucket_name!r}"
            ) from e
        with self._translate_source_error("validate source"):
            if self.prefix and not self.allow_empty_source:
                probe = client.list_blobs(
                    self.bucket_name,
                    prefix=f"{self._normalized_prefix}/",
                    max_results=1,
                )
                if next(iter(probe), None) is None:
                    raise BundleManifestNotFoundError(
                        f"GCS prefix {self._publish_source_description!r} does not exist"
                    )

    def _get_source_client(self) -> Any:
        with self._translate_source_error("create client"):
            return self.gcs_hook.get_conn()

    # Kept under its historical name as well: external tooling and tests reach
    # for the adapter-specific client accessor.
    _get_gcs_client = _get_source_client

    def _source_endpoint(self, client: Any) -> str | None:
        # Best effort only: these attributes are library internals, so the default
        # endpoint must map to a stable value. A missing attribute and the public
        # endpoint both become None; a configured custom endpoint stays visible.
        connection = getattr(client, "_connection", None)
        endpoint = getattr(connection, "API_BASE_URL", None)
        if not isinstance(endpoint, str):
            client_options = getattr(client, "_client_options", None)
            endpoint = getattr(client_options, "api_endpoint", None)
        if not isinstance(endpoint, str) or not endpoint:
            return None
        endpoint = endpoint.rstrip("/")
        if endpoint == GCS_PUBLIC_API_ENDPOINT:
            return None
        return endpoint

    def _list_source_objects(self, client: Any) -> Iterator[GCSObjectObservation]:
        listing_prefix = f"{self._normalized_prefix}/" if self._normalized_prefix else ""
        with self._translate_source_error("list source"):
            for blob in client.list_blobs(self.bucket_name, prefix=listing_prefix):
                observation = self._object_observation(blob)
                if observation is not None:
                    yield observation

    def _read_deployment_marker(self, client: Any) -> GCSObjectObservation | None:
        marker_name = self._marker_remote_name
        if marker_name is None:
            return None
        try:
            blob = client.bucket(self.bucket_name).get_blob(marker_name)
        except Exception as e:
            if _is_missing_gcs_error(e):
                blob = None
            else:
                raise BundleManifestError(
                    f"Could not read the deployment marker for GCS source "
                    f"{self._publish_source_description}"
                ) from e
        if blob is None:
            raise BundleManifestNotFoundError(
                f"Required GCS deployment marker {marker_name!r} is missing "
                f"from bucket {self.bucket_name!r}"
            )
        return self._object_observation(blob)

    def _object_observation(self, blob: Any) -> GCSObjectObservation | None:
        name = getattr(blob, "name", None)
        size = getattr(blob, "size", None)
        generation = getattr(blob, "generation", None)
        metageneration = getattr(blob, "metageneration", None)
        updated = getattr(blob, "updated", None)
        etag = getattr(blob, "etag", None)
        if hasattr(updated, "isoformat"):
            updated = updated.isoformat()
        if (
            not isinstance(name, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 1
            or not isinstance(metageneration, int)
            or isinstance(metageneration, bool)
            or metageneration < 1
            or not isinstance(updated, str)
            or not updated
            or not isinstance(etag, str)
            or not etag
        ):
            raise BundleManifestError("GCS listing returned invalid object metadata")

        relative_path = _relative_object_path(
            name=name,
            normalized_prefix=self._normalized_prefix,
            label=self._source_label,
            noun=self._remote_name_noun,
        )
        if relative_path is None:
            return None
        return GCSObjectObservation(
            relative_path=relative_path,
            name=name,
            size=size,
            generation=generation,
            metageneration=metageneration,
            updated=updated,
            etag=etag,
        )

    def _download_entry(self, client: Any, *, entry: Any, tmp_path: Path) -> None:
        blob = client.bucket(self.bucket_name).blob(
            entry.name, generation=entry.generation
        )
        try:
            blob.download_to_filename(
                str(tmp_path),
                if_generation_match=entry.generation,
            )
        except Exception as e:
            if _is_missing_gcs_error(e) or _is_gcs_precondition_error(e):
                raise BundleManifestSourceChangedError(
                    f"GCS object {entry.name!r} changed before it could be downloaded"
                ) from e
            raise BundleManifestError(
                f"Could not download object {entry.name!r} from GCS source "
                f"{self._publish_source_description}"
            ) from e


def publish_manifest_gcs_dag_bundle(
    *,
    bundle: ManifestGCSDagBundle,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish the configured GCS source as an immutable manifest-backed snapshot."""
    return publish_manifest_object_store_dag_bundle(
        bundle=bundle,
        expected_current_version=expected_current_version,
    )


def _gcs_error_code(error: Exception) -> int | None:
    code = getattr(error, "code", None)
    if callable(code):
        code = code()
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def _is_missing_gcs_error(error: Exception) -> bool:
    return _gcs_error_code(error) == 404 or type(error).__name__ == "NotFound"


def _is_gcs_precondition_error(error: Exception) -> bool:
    return _gcs_error_code(error) == 412 or type(error).__name__ in {
        "FailedPrecondition",
        "PreconditionFailed",
    }


__all__ = [
    "DEFAULT_GCP_CONN_ID",
    "DEFAULT_MAX_FILE_COUNT",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "DEFAULT_MAX_TOTAL_SIZE_BYTES",
    "GCSObjectObservation",
    "GCSSourceObservation",
    "ManifestGCSDagBundle",
    "publish_manifest_gcs_dag_bundle",
]
