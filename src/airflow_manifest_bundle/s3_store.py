"""
Read-only S3 artifact store: consume published bundle artifacts from an object store.

An S3 ``published_root`` (``s3://bucket/prefix``) holds the same logical layout as a
filesystem one — ``versions/<bundle>/<version>/``, ``refs/<bundle>/latest.json``,
``_state/<bundle>/auto-publish.json`` — with one object-store-specific rule: the
embedded snapshot manifest object is written **last** during publication, so its
presence is the commit marker. A version prefix without its manifest is invisible.

This store currently implements only the consume side (reference reads and snapshot
materialization). Publication — conditional-write reference updates and manifest-last
snapshot commits — lands separately; until then ``supports_publication`` stays False
and adapters reject publish configurations against an object-store root.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.bundle import (
    AUTO_PUBLISH_STATE_FILE_NAME,
    ManifestDagBundleBase,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestNotFoundError,
)
from airflow_manifest_bundle.store import ArtifactStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ModuleNotFoundError:
    S3Hook = None

log = logging.getLogger(__name__)

S3_PUBLISHED_ROOT_SCHEME = "s3://"

_PUBLICATION_UNSUPPORTED_MESSAGE = (
    "An object-store published_root does not support publication yet. Publish through "
    "a filesystem published_root, or configure this bundle as consume-only."
)


def _is_missing_s3_object_error(error: Exception) -> bool:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    error_data = response.get("Error")
    if not isinstance(error_data, dict):
        return False
    return str(error_data.get("Code")) in {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}


def parse_s3_published_root(published_root: str) -> tuple[str, str]:
    """Split ``s3://bucket[/prefix]`` into (bucket, normalized prefix without slashes)."""
    if not published_root.startswith(S3_PUBLISHED_ROOT_SCHEME):
        raise TypeError(f"published_root {published_root!r} is not an s3:// URL")
    remainder = published_root[len(S3_PUBLISHED_ROOT_SCHEME) :]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        raise TypeError(f"published_root {published_root!r} does not contain a bucket name")
    if "?" in remainder or "#" in remainder:
        raise TypeError(f"published_root {published_root!r} must not contain a query or fragment")
    return bucket, prefix.strip("/")


class S3ArtifactStore(ArtifactStore):
    """Object-store artifact backend; consume-only until publication support lands."""

    supports_publication = False

    def __init__(
        self,
        *,
        bundle_name: str,
        published_root: str,
        aws_conn_id: str | None = None,
    ) -> None:
        if S3Hook is None:
            # TypeError for the same reason bundle constructors use it: stock callback
            # preparation swallows ValueError from bundle construction as "bundle no
            # longer configured".
            raise TypeError(
                "An s3:// published_root requires the Amazon provider. Install "
                "'airflow-manifest-bundle[s3]' on every host that reads it."
            )
        if aws_conn_id is not None and (not isinstance(aws_conn_id, str) or not aws_conn_id):
            raise TypeError("published_root_conn_id must be a non-empty string")
        self.bundle_name = bundle_name
        self.bucket_name, self._prefix = parse_s3_published_root(published_root)
        self.aws_conn_id = aws_conn_id if aws_conn_id is not None else S3Hook.default_conn_name
        self._s3_hook: Any | None = None

    # --- locators -------------------------------------------------------------

    def _key(self, *parts: str) -> str:
        segments = [self._prefix, *parts] if self._prefix else list(parts)
        return "/".join(segments)

    def _url(self, key: str) -> str:
        return f"{S3_PUBLISHED_ROOT_SCHEME}{self.bucket_name}/{key}" if key else self.root

    @property
    def root(self) -> str:
        suffix = f"/{self._prefix}" if self._prefix else ""
        return f"{S3_PUBLISHED_ROOT_SCHEME}{self.bucket_name}{suffix}"

    @property
    def ref_path(self) -> str:
        return self._url(self._ref_key)

    @property
    def state_path(self) -> str:
        return self._url(self._state_key)

    @property
    def snapshots_root(self) -> str:
        return self._url(self._key("versions", self.bundle_name))

    def snapshot_path(self, version: str) -> str:
        return self._url(self._snapshot_key(version))

    @property
    def _ref_key(self) -> str:
        return self._key("refs", self.bundle_name, "latest.json")

    @property
    def _state_key(self) -> str:
        return self._key("_state", self.bundle_name, AUTO_PUBLISH_STATE_FILE_NAME)

    def _snapshot_key(self, version: str, *parts: str) -> str:
        return self._key("versions", self.bundle_name, version, *parts)

    # --- client ---------------------------------------------------------------

    @property
    def s3_hook(self) -> Any:
        if self._s3_hook is None:
            try:
                self._s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            except Exception as e:
                raise BundleManifestError(
                    f"Could not create an S3 client for published_root {self.root}"
                ) from e
        return self._s3_hook

    def _get_client(self) -> Any:
        try:
            return self.s3_hook.get_conn()
        except BundleManifestError:
            raise
        except Exception as e:
            raise BundleManifestError(
                f"Could not create an S3 client for published_root {self.root}"
            ) from e

    def _get_object_bytes(self, key: str) -> bytes | None:
        """Return the object body, or None when the object does not exist."""
        client = self._get_client()
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"]
            return b"".join(iter(lambda: body.read(1024 * 1024), b""))
        except Exception as e:
            if _is_missing_s3_object_error(e):
                return None
            raise BundleManifestError(
                f"Could not read {self._url(key)} from the object-store published_root"
            ) from e

    def _object_exists(self, key: str) -> bool:
        client = self._get_client()
        try:
            client.head_object(Bucket=self.bucket_name, Key=key)
        except Exception as e:
            if _is_missing_s3_object_error(e):
                return False
            raise BundleManifestError(
                f"Could not read {self._url(key)} from the object-store published_root"
            ) from e
        return True

    # --- mutable documents ----------------------------------------------------

    def _read_json_object(self, key: str, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        data = self._get_object_bytes(key)
        if data is None:
            raise BundleManifestNotFoundError(missing_message)
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(invalid_message) from e
        if not isinstance(payload, dict):
            raise BundleManifestError(invalid_message)
        return payload

    def read_ref(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        return self._read_json_object(
            self._ref_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def read_state(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        return self._read_json_object(
            self._state_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def write_ref(self, payload: dict[str, Any]) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    def write_state(self, payload: dict[str, Any]) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    # --- coordination ---------------------------------------------------------

    def publication_guard(self):
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    def prepare_publish_areas(self) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    def prepare_state_area(self) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    def validate_source_paths(self, source_path: Path, *, cache_versions_dir: Path) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    # --- snapshots ------------------------------------------------------------

    def snapshot_exists(self, version: str) -> bool:
        # The embedded manifest is written last during publication, so its presence is
        # the commit marker; a version prefix without it is an incomplete upload.
        return self._object_exists(self._snapshot_key(version, MANIFEST_FILE_NAME))

    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        # There is no remote tree to structurally pre-validate; the manifest is the
        # authority, every downloaded byte is hash-verified below, and the caller runs
        # the full validation pass on the materialized copy afterwards.
        manifest_bytes = self._get_object_bytes(self._snapshot_key(version, MANIFEST_FILE_NAME))
        if manifest_bytes is None:
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' version '{version}' is not published at "
                f"{self.snapshot_path(version)}. Publish or restore the immutable snapshot before "
                "running pinned work."
            )
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(
                f"Bundle snapshot manifest is not valid JSON: "
                f"{self._url(self._snapshot_key(version, MANIFEST_FILE_NAME))}"
            ) from e
        if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
            raise BundleManifestError(
                f"Bundle snapshot manifest at {self.snapshot_path(version)} does not list its files"
            )

        (destination / MANIFEST_FILE_NAME).write_bytes(manifest_bytes)
        for file_info in manifest["files"]:
            # Path safety must hold before anything is written to disk; the full
            # manifest/ref consistency checks run in the caller's validation pass.
            relative_path = ManifestDagBundleBase._validate_manifest_file_info(file_info)
            file_destination = destination / relative_path
            ManifestDagBundleBase._ensure_destination_is_within_root(destination, file_destination)
            file_destination.parent.mkdir(parents=True, exist_ok=True)
            self._download_snapshot_file(
                key=self._snapshot_key(version, *relative_path.split("/")),
                destination=file_destination,
                file_info=file_info,
            )

    def _download_snapshot_file(self, *, key: str, destination: Path, file_info: dict[str, Any]) -> None:
        client = self._get_client()
        digest = hashlib.sha256()
        size = 0
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"]
            with destination.open("wb") as file:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    file.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        except Exception as e:
            if _is_missing_s3_object_error(e):
                raise BundleManifestError(
                    f"Bundle snapshot at {self._url(key)} is missing manifest entry "
                    f"{file_info['path']!r}"
                ) from e
            raise BundleManifestError(
                f"Could not read {self._url(key)} from the object-store published_root"
            ) from e
        if size != file_info["size"] or digest.hexdigest() != file_info["sha256"]:
            raise BundleManifestError(
                f"Bundle snapshot file {file_info['path']!r} does not match the snapshot manifest"
            )
        destination.chmod(0o755 if file_info["executable"] else 0o644)

    def publish_snapshot(
        self,
        version: str,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> bool:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)

    def sweep_publish_temps(self) -> None:
        raise BundleManifestError(_PUBLICATION_UNSUPPORTED_MESSAGE)


__all__ = [
    "S3_PUBLISHED_ROOT_SCHEME",
    "S3ArtifactStore",
    "parse_s3_published_root",
]
