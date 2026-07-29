"""
S3 artifact store: publish and consume bundle artifacts in an object store.

An S3 ``published_root`` (``s3://bucket/prefix``) holds the same logical layout as a
filesystem one — ``versions/<bundle>/<version>/``, ``refs/<bundle>/latest.json``,
``_state/<bundle>/auto-publish.json`` — with two object-store-specific rules:

- The embedded snapshot manifest object is written **last** during publication, so
  its presence is the commit marker. A version prefix without its manifest is
  invisible, and re-publishing the same content completes it.
- There is no cross-host lock. The two mutable documents are protected by
  conditional writes (``If-Match``/``If-None-Match``); a lost race surfaces as
  ``ArtifactStoreConflictError`` unless the winner wrote identical bytes, which is an
  idempotent success. Snapshot uploads need no protection: content-addressed keys
  make concurrent same-content publications write identical objects.

Publication therefore requires an object store with conditional-write support (AWS
S3 has it; some S3-compatible stores do not — those fail with a clear error on the
first conditional write).
"""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.bundle import (
    AUTO_PUBLISH_STATE_FILE_NAME,
    ManifestDagBundleBase,
    _paths_overlap,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestNotFoundError,
    BundleManifestSourceChangedError,
    serialize_bundle_version_manifest,
)
from airflow_manifest_bundle.store import ArtifactStore, ArtifactStoreConflictError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ModuleNotFoundError:
    S3Hook = None

log = logging.getLogger(__name__)

S3_PUBLISHED_ROOT_SCHEME = "s3://"

# HTTP 412 rejects a failed precondition; S3 returns 409 ConditionalRequestConflict
# when concurrent conditional writes on one key race each other.
_CONDITIONAL_WRITE_CONFLICT_CODES = frozenset(
    {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}
)
_CONDITIONAL_WRITE_UNSUPPORTED_CODES = frozenset({"NotImplemented", "501"})

#: CAS baseline recording that the document did not exist when last read.
_MISSING = object()


def _s3_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_data = response.get("Error")
    if not isinstance(error_data, dict):
        return None
    return str(error_data.get("Code"))


def _is_missing_s3_object_error(error: Exception) -> bool:
    # NoSuchBucket is deliberately not in this set: a missing bucket is a
    # configuration problem and must not read as "the artifact is not published".
    return _s3_error_code(error) in {"404", "NoSuchKey", "NotFound"}


def _is_missing_s3_bucket_error(error: Exception) -> bool:
    return _s3_error_code(error) == "NoSuchBucket"


def parse_s3_published_root(published_root: str) -> tuple[str, str]:
    """Split ``s3://bucket[/prefix]`` into (bucket, normalized prefix without slashes)."""
    if not published_root[: len(S3_PUBLISHED_ROOT_SCHEME)].lower() == S3_PUBLISHED_ROOT_SCHEME:
        raise TypeError(f"published_root {published_root!r} is not an s3:// URL")
    remainder = published_root[len(S3_PUBLISHED_ROOT_SCHEME) :]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        raise TypeError(f"published_root {published_root!r} does not contain a bucket name")
    if "?" in remainder or "#" in remainder:
        raise TypeError(f"published_root {published_root!r} must not contain a query or fragment")
    return bucket, prefix.strip("/")


class S3ArtifactStore(ArtifactStore):
    """Object-store artifact backend: CAS-coordinated publication, manifest-last commits."""

    supports_publication = True

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
        # CAS baselines for the mutable documents: key -> ETag from the last read or
        # write, or _MISSING when the last read found no document. Absent means no
        # baseline; the next write re-reads to establish one.
        self._document_tokens: dict[str, Any] = {}

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

    def _get_object(self, key: str) -> tuple[bytes, str | None] | None:
        """Return (body, ETag) for the object, or None when it does not exist."""
        client = self._get_client()
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"]
            data = b"".join(iter(lambda: body.read(1024 * 1024), b""))
        except Exception as e:
            if _is_missing_s3_object_error(e):
                return None
            raise self._read_error(key, e) from e
        etag = response.get("ETag")
        return data, (str(etag) if etag else None)

    def _get_object_bytes(self, key: str) -> bytes | None:
        """Return the object body, or None when the object does not exist."""
        result = self._get_object(key)
        return None if result is None else result[0]

    def _object_exists(self, key: str) -> bool:
        client = self._get_client()
        try:
            client.head_object(Bucket=self.bucket_name, Key=key)
        except Exception as e:
            if _is_missing_s3_object_error(e):
                return False
            raise self._read_error(key, e) from e
        return True

    def _read_error(self, key: str, error: Exception) -> BundleManifestError:
        if _is_missing_s3_bucket_error(error):
            return BundleManifestError(
                f"S3 bucket {self.bucket_name!r} for published_root {self.root} does not "
                "exist. Fix the published_root configuration before refreshing this bundle."
            )
        return BundleManifestError(
            f"Could not read {self._url(key)} from the object-store published_root"
        )

    # --- mutable documents ----------------------------------------------------

    def _read_json_object(self, key: str, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        result = self._get_object(key)
        if result is None:
            self._document_tokens[key] = _MISSING
            raise BundleManifestNotFoundError(missing_message)
        data, etag = result
        # Record the CAS baseline before parsing: a later write may deliberately
        # replace a corrupt document.
        self._record_document_token(key, etag)
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(invalid_message) from e
        if not isinstance(payload, dict):
            raise BundleManifestError(invalid_message)
        return payload

    def _record_document_token(self, key: str, etag: str | None) -> None:
        if etag:
            self._document_tokens[key] = etag
        else:
            self._document_tokens.pop(key, None)

    def read_ref(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        return self._read_json_object(
            self._ref_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def read_state(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        return self._read_json_object(
            self._state_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def write_ref(self, payload: dict[str, Any]) -> None:
        self._write_json_document(self._ref_key, payload)

    def write_state(self, payload: dict[str, Any]) -> None:
        self._write_json_document(self._state_key, payload)

    def _write_json_document(self, key: str, payload: dict[str, Any]) -> None:
        token = self._document_tokens.get(key)
        if token is None:
            # No baseline from this session; establish one so the write stays CAS.
            result = self._get_object(key)
            token = result[1] if result is not None and result[1] else _MISSING
            self._document_tokens[key] = token
        body = serialize_bundle_version_manifest(payload)
        conditional = {"IfNoneMatch": "*"} if token is _MISSING else {"IfMatch": token}
        client = self._get_client()
        try:
            response = client.put_object(
                Bucket=self.bucket_name, Key=key, Body=body, **conditional
            )
        except Exception as e:
            code = _s3_error_code(e)
            if code in _CONDITIONAL_WRITE_CONFLICT_CODES:
                current = self._get_object(key)
                if current is None:
                    self._document_tokens[key] = _MISSING
                else:
                    self._record_document_token(key, current[1])
                    if current[0] == body:
                        # A concurrent publisher wrote identical bytes: idempotent win.
                        return
                raise ArtifactStoreConflictError(
                    f"Another publisher updated {self._url(key)} concurrently"
                ) from e
            if code in _CONDITIONAL_WRITE_UNSUPPORTED_CODES:
                raise BundleManifestError(
                    f"The object store for published_root {self.root} does not support "
                    "conditional writes, which publication requires. Use a store with "
                    "If-Match support or publish through a filesystem published_root."
                ) from e
            raise self._write_error(key, e) from e
        self._record_document_token(key, response.get("ETag"))

    # --- coordination ---------------------------------------------------------

    @contextmanager
    def publication_guard(self):
        # No cross-host lock exists for an object store. The mutable documents are
        # compare-and-swap protected, and snapshot uploads are idempotent
        # (content-addressed keys, manifest-last commit), so the guard is a no-op.
        yield

    def prepare_publish_areas(self) -> None:
        """Nothing to create: object-store prefixes appear with their first object."""

    def prepare_state_area(self) -> None:
        """Nothing to create: object-store prefixes appear with their first object."""

    def validate_source_paths(self, source_path: Path, *, cache_versions_dir: Path) -> None:
        # The published root is remote, so only the local-cache overlap can occur.
        if _paths_overlap(source_path, cache_versions_dir):
            raise ValueError(
                "source_path and Airflow's bundle cache must not overlap. Keep the Dag "
                "source tree outside dag_bundle_storage_path."
            )

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
            raise self._read_error(key, e) from e
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
        # ``validate_existing`` walks a filesystem tree; the object-store equivalent is
        # comparing the committed manifest bytes — the manifest is content-addressed, so
        # byte equality certifies the whole snapshot (consumers hash every file on fetch).
        manifest_bytes = serialize_bundle_version_manifest(manifest)
        manifest_key = self._snapshot_key(version, MANIFEST_FILE_NAME)
        existing = self._get_object_bytes(manifest_key)
        if existing is not None:
            if existing != manifest_bytes:
                raise BundleManifestError(
                    f"Bundle snapshot manifest at {self._url(manifest_key)} does not match "
                    f"the content-addressed version {version!r}; refusing to overwrite it"
                )
            return False

        for file_info in manifest["files"]:
            relative_path = ManifestDagBundleBase._validate_manifest_file_info(file_info)
            source = source_root / relative_path
            try:
                data = source.read_bytes()
            except FileNotFoundError as e:
                raise BundleManifestSourceChangedError(
                    f"Bundle source file disappeared while publishing bundle version "
                    f"{version}: {relative_path}"
                ) from e
            # Hash what is actually uploaded so source drift between manifest
            # construction and upload cannot commit a snapshot that contradicts its
            # own content address (mirrors the filesystem store's inline check).
            if (
                len(data) != file_info["size"]
                or hashlib.sha256(data).hexdigest() != file_info["sha256"]
            ):
                raise BundleManifestSourceChangedError(
                    f"Bundle source changed while publishing bundle version {version}"
                )
            self._put_object(self._snapshot_key(version, *relative_path.split("/")), data)

        # The manifest goes last: its presence commits the snapshot. Unconditional on
        # purpose — same-content publishers write identical bytes, and overwriting a
        # corrupt manifest with the correct one is self-healing.
        self._put_object(manifest_key, manifest_bytes)
        return True

    def _put_object(self, key: str, body: bytes) -> None:
        client = self._get_client()
        try:
            client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
        except Exception as e:
            raise self._write_error(key, e) from e

    def _write_error(self, key: str, error: Exception) -> BundleManifestError:
        if _is_missing_s3_bucket_error(error):
            return BundleManifestError(
                f"S3 bucket {self.bucket_name!r} for published_root {self.root} does not "
                "exist. Fix the published_root configuration before publishing."
            )
        return BundleManifestError(
            f"Could not write {self._url(key)} to the object-store published_root"
        )

    def sweep_publish_temps(self) -> None:
        """
        Nothing to sweep: object-store publication writes no temporary artifacts.

        Uploads land at their final content-addressed keys and the manifest object
        commits the snapshot. A crash before the manifest PUT leaves an uncommitted
        prefix that a later publication of the same content completes; reclaiming
        abandoned prefixes is a deployment-side lifecycle concern.
        """


__all__ = [
    "S3_PUBLISHED_ROOT_SCHEME",
    "S3ArtifactStore",
    "parse_s3_published_root",
]
