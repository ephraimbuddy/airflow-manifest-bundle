"""Shared lifecycle for immutable, content-addressed manifest Dag bundles."""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import stat
import tempfile
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from typing import Any, ClassVar

from airflow.dag_processing.bundles.base import (
    BaseDagBundle,
    get_bundle_storage_root_path,
    get_bundle_tracking_file,
)

from airflow_manifest_bundle._compat import (
    make_bundle_version,
    remove_bundle_tree_forcefully,
)
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    MANIFEST_SCHEMA_VERSION,
    BundleManifestError,
    BundleManifestNotFoundError,
    BundleManifestSourceChangedError,
    BundleSourceSnapshot,
    BundleVersionManifest,
    build_bundle_version_manifest_result,
    build_ref_payload,
    compute_bundle_version,
    compute_file_sha256,
    serialize_bundle_version_manifest,
    verify_bundle_version_manifest,
)

log = logging.getLogger(__name__)

FILESYSTEM_ARTIFACT_BACKEND_TYPE = "local"
SHA256_VERSION_PREFIX = "sha256-"
AUTO_PUBLISH_STATE_SCHEMA_VERSION = 2
AUTO_PUBLISH_STATE_FILE_NAME = "auto-publish.json"


@contextmanager
def _oserror_as_manifest_error():
    """
    Re-raise OSError as BundleManifestError (an AirflowException).

    Stock Airflow's dag processor only treats AirflowException from bundle initialization
    as a recoverable per-bundle error; a raw OSError (e.g. a missing manifest reference or
    published snapshot) would crash its refresh loop.
    """
    try:
        yield
    except BundleManifestError:
        raise
    except OSError as e:
        raise BundleManifestError(str(e)) from e


class BundleManifestReferenceChangedError(BundleManifestError):
    """Raised when a publisher's expected manifest reference is stale."""


class _AutoPublishNotReadyError(BundleManifestError):
    """Raised when no release is available and the source still needs stability time."""

    def __init__(self, message: str, *, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class BundleManifestRef:
    """Compact pointer to an immutable manifest bundle artifact."""

    version: str
    ref_payload: dict[str, Any]
    manifest_sha256: str


@dataclass(frozen=True)
class BundlePublishResult:
    """Result of publishing an immutable manifest bundle snapshot."""

    bundle_name: str
    version: str
    ref_payload: dict[str, Any]
    version_path: Path
    manifest_ref_path: Path
    manifest_sha256: str
    file_count: int
    total_size: int
    created_snapshot: bool


@dataclass(frozen=True)
class _AutoPublishCandidateState:
    """Shared observation of one automatic-publication source signature."""

    source_type: str
    source_identity: str
    source_signature: str
    first_observed_at: float


@dataclass(frozen=True)
class PreparedPublishSource:
    """One materialized source observation prepared by a concrete adapter."""

    root: Path
    source_snapshot: BundleSourceSnapshot
    source_type: str
    source_identity: str
    source_signature: str
    file_sha256_by_path: dict[str, str] | None = None
    confirmation_data: Any = None
    release_source_metadata: dict[str, Any] | None = None


class ManifestDagBundleBase(BaseDagBundle, ABC):
    """
    Shared runtime for filesystem-published content-addressed bundle manifests.

    Concrete adapters prepare a local source tree for automatic publication. Airflow
    only parses immutable, validated copies from ``versions_dir``.

    :param published_root: Shared root containing published snapshots and the current release reference.
    :param source_stability_seconds: How long source metadata must remain unchanged
        before automatic publication. Defaults to ``refresh_interval``. Set to zero
        only when the deployment process updates the source atomically.
    :param allow_empty_source: Permit publication of a source tree with no files. Off
        by default to prevent a failed or incomplete sync from deleting all Dags from
        the current release.
    """

    supports_versioning = True

    # Per-process record of fully validated cache trees, keyed by
    # (bundle name, version, cache path). Safe because snapshots are immutable.
    _validated_version_paths: ClassVar[set[tuple[str, str, str]]] = set()

    def __init__(
        self,
        *,
        published_root: str | None = None,
        source_stability_seconds: float | None = None,
        allow_empty_source: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not published_root:
            # TypeError, not ValueError: stock prepare_callback_bundle swallows ValueError
            # from bundle construction as "Bundle no longer configured", silently dropping
            # callbacks with a misleading log. TypeError matches how any bundle class fails
            # on a bad config kwarg.
            raise TypeError("published_root must be provided")
        self.published_root = Path(published_root)
        self.manifest_ref_path = self.published_root / "refs" / self.name / "latest.json"

        if _paths_overlap(self.published_root, get_bundle_storage_root_path()):
            raise ValueError(
                "published_root must not overlap Airflow's bundle cache. Configure separate "
                "authoritative publication and dag_bundle_storage_path locations."
            )

        self.published_versions_dir = self.published_root / "versions" / self.name
        self.publication_lock_path = self.published_root / "_locks" / f"{self.name}.lock"
        self.auto_publish_state_path = (
            self.published_root / "_state" / self.name / AUTO_PUBLISH_STATE_FILE_NAME
        )
        if source_stability_seconds is None:
            source_stability_seconds = float(self.refresh_interval)
        if (
            isinstance(source_stability_seconds, bool)
            or not isinstance(source_stability_seconds, (int, float))
            or not math.isfinite(source_stability_seconds)
            or source_stability_seconds < 0
        ):
            raise TypeError("source_stability_seconds must be a finite number greater than or equal to zero")
        self.source_stability_seconds = float(source_stability_seconds)
        if not isinstance(allow_empty_source, bool):
            raise TypeError("allow_empty_source must be a boolean")
        self.allow_empty_source = allow_empty_source
        self._current_manifest_ref: BundleManifestRef | None = None
        # This is only a per-process hashing optimization. Source readiness is shared
        # under published_root because Airflow can run successive refreshes in different
        # dag-processor replicas. The disposable Airflow cache stores neither.
        self._confirmed_source_key: tuple[str, str, str] | None = None
        self._confirmed_source_version: str | None = None

    @property
    @abstractmethod
    def _has_publish_source(self) -> bool:
        """Whether this bundle has a source for automatic publication."""

    @property
    @abstractmethod
    def _publish_source_description(self) -> str:
        """Safe source description for logs and errors."""

    @abstractmethod
    def _prepare_publish_source(self) -> PreparedPublishSource:
        """Materialize and observe the concrete source."""

    @abstractmethod
    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        """Raise if the concrete source no longer matches ``prepared``."""

    def _validate_release_transition(
        self,
        *,
        current_ref: BundleManifestRef | None,
        prepared: PreparedPublishSource,
        target_version: str,
    ) -> None:
        """Validate adapter-specific rules before a prepared source can become current."""

    def _current_release_matches_source(
        self,
        *,
        current_ref: BundleManifestRef,
        prepared: PreparedPublishSource,
    ) -> bool:
        """Return whether the current reference already records this source observation."""
        return True

    def get_current_version(self) -> Any:
        """
        Current version: a ``BundleVersion`` on Airflow 3.3+, a plain string on 3.1/3.2.

        The version string alone is the whole contract: it is the content hash of the
        snapshot's manifest entries, so a pinned snapshot is self-certifying and no
        side-channel metadata needs to flow through Airflow.
        """
        if self.version:
            return make_bundle_version(self.version)
        with _oserror_as_manifest_error():
            manifest_ref = self._ensure_current_manifest_ref()
        return make_bundle_version(manifest_ref.version)

    def initialize(self) -> None:
        if self.version:
            version = _validate_manifest_version(self.version, source="pinned bundle version")
            with _oserror_as_manifest_error():
                if not self._has_validated_cache(version, manifest_ref=None):
                    with self.lock():
                        self._materialize_cached_version(version=version, manifest_ref=None)
        else:
            with _oserror_as_manifest_error():
                try:
                    self.refresh()
                except _AutoPublishNotReadyError as e:
                    log.info(
                        "No bundle release is available yet; waiting %.3g seconds for source "
                        "stability. bundle=%s source=%s",
                        e.retry_after,
                        self.name,
                        self._publish_source_description,
                    )
                    time.sleep(e.retry_after)
                    self.refresh()
        super().initialize()

    def refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific bundle version is not supported")

        with _oserror_as_manifest_error():
            manifest_ref = self._read_current_manifest_ref_or_none()
            if self._has_publish_source:
                manifest_ref = self._maybe_publish_from_source(manifest_ref)
            if manifest_ref is None:
                # Use the established missing-reference error and message.
                manifest_ref = self._read_current_manifest_ref()
            # Lock-free fast path: a validated cache tree is immutable, so the steady state
            # must not contend with another process's materialization of a different version.
            # Known limitation: a concurrent corrupt-cache rebuild can move the tree aside
            # right after this check; the affected process fails once and heals on retry.
            if self._has_validated_cache(manifest_ref.version, manifest_ref=manifest_ref):
                self._current_manifest_ref = manifest_ref
                return
            with self.lock():
                self._materialize_cached_version(version=manifest_ref.version, manifest_ref=manifest_ref)
                self._current_manifest_ref = manifest_ref

    def _maybe_publish_from_source(
        self,
        current_ref: BundleManifestRef | None,
    ) -> BundleManifestRef | None:
        """Publish a stable source change without disrupting an existing release."""
        try:
            return self._publish_from_source_if_ready(current_ref)
        except BundleManifestSourceChangedError:
            fallback_ref = current_ref or self._read_current_manifest_ref_or_none()
            if fallback_ref is None:
                raise
            log.debug(
                "Bundle source changed while auto-publishing; keeping the current release. "
                "bundle=%s source=%s",
                self.name,
                self._publish_source_description,
            )
            return fallback_ref
        except (BundleManifestError, OSError):
            fallback_ref = current_ref or self._read_current_manifest_ref_or_none()
            if fallback_ref is None:
                raise
            log.warning(
                "Auto-publishing the bundle source failed; keeping the current release. "
                "bundle=%s source=%s",
                self.name,
                self._publish_source_description,
                exc_info=True,
            )
            return fallback_ref

    def _publish_from_source_if_ready(
        self,
        current_ref: BundleManifestRef | None,
    ) -> BundleManifestRef | None:
        if not self._has_publish_source:
            return current_ref
        prepared = self._prepare_publish_source()
        self._validate_publish_source_paths(prepared.root)
        snapshot = prepared.source_snapshot
        source_key = (
            prepared.source_type,
            prepared.source_identity,
            prepared.source_signature,
        )
        source_is_confirmed = source_key == self._confirmed_source_key
        if source_is_confirmed and self.source_stability_seconds > 0:
            self._ensure_auto_publish_candidate_matches(prepared)

        if (
            source_is_confirmed
            and current_ref is not None
            and current_ref.version == self._confirmed_source_version
            and self._published_snapshot_exists(current_ref.version)
            and self._current_release_matches_source(
                current_ref=current_ref,
                prepared=prepared,
            )
        ):
            return current_ref

        if not source_is_confirmed and self.source_stability_seconds > 0:
            candidate_is_ready, retry_after = self._auto_publish_candidate_readiness(prepared)
        else:
            candidate_is_ready, retry_after = True, 0.0
        if not candidate_is_ready:
            if current_ref is None:
                raise _AutoPublishNotReadyError(
                    f"Bundle source for '{self.name}' has not remained unchanged for "
                    f"{self.source_stability_seconds:g} seconds; no release is available yet",
                    retry_after=retry_after,
                )
            log.info(
                "Bundle source changed; waiting until it has remained unchanged for %g seconds. "
                "bundle=%s source=%s",
                self.source_stability_seconds,
                self.name,
                self._publish_source_description,
            )
            return current_ref

        if not snapshot.files and not self.allow_empty_source:
            raise BundleManifestError(
                f"Refusing to auto-publish empty source tree {self._publish_source_description} for bundle "
                f"'{self.name}'; set allow_empty_source=True if removing every Dag is intended"
            )

        manifest_result = build_bundle_version_manifest_result(
            bundle_name=self.name,
            root=prepared.root,
            backend_type=FILESYSTEM_ARTIFACT_BACKEND_TYPE,
            source_snapshot=snapshot,
            precomputed_file_sha256=prepared.file_sha256_by_path,
        )
        self._validate_release_transition(
            current_ref=current_ref,
            prepared=prepared,
            target_version=manifest_result.version,
        )
        if (
            current_ref is not None
            and current_ref.version == manifest_result.version
            and self._published_snapshot_exists(manifest_result.version)
            and self._current_release_matches_source(
                current_ref=current_ref,
                prepared=prepared,
            )
        ):
            self._record_confirmed_source(prepared, manifest_result.version)
            return current_ref

        self._validate_publish_source_paths(prepared.root)
        _ensure_public_dir(self.published_versions_dir)
        _ensure_public_dir(self.manifest_ref_path.parent)
        with self.acquire_publication_lock():
            self._validate_publish_source_paths(prepared.root)
            if (
                not source_is_confirmed
                and self.source_stability_seconds > 0
                and not self._auto_publish_candidate_remains_ready_locked(prepared)
            ):
                raise BundleManifestSourceChangedError(
                    "Bundle source stability candidate changed before automatic publication"
                )
            latest_ref = self._read_current_manifest_ref_or_none()
            self._validate_release_transition(
                current_ref=latest_ref,
                prepared=prepared,
                target_version=manifest_result.version,
            )
            if (
                latest_ref is not None
                and latest_ref.version == manifest_result.version
                and self._published_snapshot_exists(manifest_result.version)
                and self._current_release_matches_source(
                    current_ref=latest_ref,
                    prepared=prepared,
                )
            ):
                self._record_confirmed_source(prepared, manifest_result.version)
                return latest_ref
            result = _publish_manifest_result(
                bundle=self,
                manifest_result=manifest_result,
                prepared_source=prepared,
            )

        self._record_confirmed_source(prepared, result.version)
        log.info(
            "Auto-published bundle source. bundle=%s version=%s file_count=%d total_size=%d "
            "created_snapshot=%s",
            self.name,
            result.version,
            result.file_count,
            result.total_size,
            result.created_snapshot,
        )
        return self._manifest_ref_from_payload(result.ref_payload, source="auto-publisher")

    def _validate_publish_source_paths(self, source_path: Path) -> None:
        try:
            _validate_publish_paths(
                source_path=source_path,
                published_root=self.published_root,
                versions_dir=self.versions_dir,
            )
        except ValueError as e:
            raise BundleManifestError(str(e)) from e

    def _record_confirmed_source(self, prepared: PreparedPublishSource, version: str) -> None:
        self._confirmed_source_key = (
            prepared.source_type,
            prepared.source_identity,
            prepared.source_signature,
        )
        self._confirmed_source_version = version

    def _published_snapshot_exists(self, version: str) -> bool:
        return (self.published_versions_dir / version).is_dir()

    @staticmethod
    def _candidate_matches(
        state: _AutoPublishCandidateState,
        prepared: PreparedPublishSource,
    ) -> bool:
        return (
            state.source_type == prepared.source_type
            and state.source_identity == prepared.source_identity
            and state.source_signature == prepared.source_signature
        )

    def _auto_publish_candidate_is_ready(self, prepared: PreparedPublishSource) -> bool:
        """Record or check shared source readiness under the publication lock."""
        is_ready, _ = self._auto_publish_candidate_readiness(prepared)
        return is_ready

    def _auto_publish_candidate_readiness(
        self,
        prepared: PreparedPublishSource,
    ) -> tuple[bool, float]:
        """Return source readiness and the bounded time until another check."""
        _ensure_public_dir(self.auto_publish_state_path.parent)
        with self.acquire_publication_lock():
            state = self._synchronize_auto_publish_candidate_locked(prepared)
            now = time.time()
            if self._auto_publish_candidate_elapsed(state, now=now):
                return True, 0.0
            elapsed = now - state.first_observed_at
            if elapsed < 0:
                return False, self.source_stability_seconds
            return False, max(0.0, self.source_stability_seconds - elapsed)

    def _ensure_auto_publish_candidate_matches(self, prepared: PreparedPublishSource) -> None:
        """Reset a stale shared candidate before taking the confirmed-source fast path."""
        state = self._read_auto_publish_candidate_state_or_none()
        if state is not None and self._candidate_matches(state, prepared):
            return
        _ensure_public_dir(self.auto_publish_state_path.parent)
        with self.acquire_publication_lock():
            self._synchronize_auto_publish_candidate_locked(prepared)

    def _synchronize_auto_publish_candidate_locked(
        self,
        prepared: PreparedPublishSource,
    ) -> _AutoPublishCandidateState:
        """
        Reconcile shared state with a source observation while holding the publication lock.

        A replica can wait between its metadata walk and lock acquisition. Re-read the
        source before replacing a different candidate so that an old observation cannot
        overwrite a newer signature another replica recorded.
        """
        state = self._read_auto_publish_candidate_state_or_none()
        if state is not None and self._candidate_matches(state, prepared):
            return state

        if state is not None and (
            state.source_type != prepared.source_type
            or state.source_identity != prepared.source_identity
        ):
            raise BundleManifestError(
                f"Automatic publishers for bundle '{self.name}' do not use the same "
                f"{prepared.source_type} source"
            )

        self._confirm_publish_source(prepared)
        now = time.time()
        if state is None or not self._candidate_matches(state, prepared):
            state = _AutoPublishCandidateState(
                source_type=prepared.source_type,
                source_identity=prepared.source_identity,
                source_signature=prepared.source_signature,
                first_observed_at=now,
            )
            self._write_auto_publish_candidate_state(state)
        return state

    def _auto_publish_candidate_remains_ready_locked(
        self,
        prepared: PreparedPublishSource,
    ) -> bool:
        """Check readiness again without replacing another publisher's candidate."""
        state = self._read_auto_publish_candidate_state_or_none()
        if state is None or not self._candidate_matches(state, prepared):
            return False
        return self._auto_publish_candidate_elapsed(state, now=time.time())

    def _auto_publish_candidate_elapsed(
        self,
        state: _AutoPublishCandidateState,
        *,
        now: float,
    ) -> bool:
        elapsed = now - state.first_observed_at
        if elapsed < 0:
            log.warning(
                "Automatic-publish candidate timestamp is in the future; waiting for clocks "
                "to converge. bundle=%s candidate_timestamp=%s current_timestamp=%s",
                self.name,
                state.first_observed_at,
                now,
            )
            return False
        return elapsed >= self.source_stability_seconds

    def _read_auto_publish_candidate_state_or_none(self) -> _AutoPublishCandidateState | None:
        try:
            payload = self._read_json_file(
                self.auto_publish_state_path,
                missing_message=f"Automatic-publish state is missing: {self.auto_publish_state_path}",
                invalid_message=f"Automatic-publish state is not valid JSON: {self.auto_publish_state_path}",
            )
        except BundleManifestNotFoundError:
            return None
        except BundleManifestError:
            log.warning(
                "Automatic-publish state is malformed; restarting the source stability period. "
                "bundle=%s path=%s",
                self.name,
                self.auto_publish_state_path,
                exc_info=True,
            )
            return None

        schema_version = payload.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version < 1
        ):
            log.warning(
                "Automatic-publish state has an invalid schema version; restarting the source "
                "stability period. bundle=%s path=%s",
                self.name,
                self.auto_publish_state_path,
            )
            return None
        if schema_version == 1:
            log.info(
                "Automatic-publish state uses schema version 1; restarting the source "
                "stability period with schema version 2. bundle=%s path=%s",
                self.name,
                self.auto_publish_state_path,
            )
            return None
        if schema_version != AUTO_PUBLISH_STATE_SCHEMA_VERSION:
            raise BundleManifestError(
                f"Automatic-publish state {self.auto_publish_state_path} has unsupported "
                f"schema_version {schema_version!r}"
            )
        if payload.get("bundle_name") != self.name:
            log.warning(
                "Automatic-publish state has the wrong bundle name; restarting the source "
                "stability period. bundle=%s path=%s",
                self.name,
                self.auto_publish_state_path,
            )
            return None
        source_type = payload.get("source_type")
        source_identity = payload.get("source_identity")
        source_signature = payload.get("source_signature")
        first_observed_at = payload.get("first_observed_at")
        if (
            not isinstance(source_type, str)
            or not source_type
            or not isinstance(source_identity, str)
            or not source_identity
            or not isinstance(source_signature, str)
            or not source_signature
            or isinstance(first_observed_at, bool)
            or not isinstance(first_observed_at, (int, float))
            or not math.isfinite(first_observed_at)
            or first_observed_at < 0
        ):
            log.warning(
                "Automatic-publish state has invalid candidate data; restarting the source "
                "stability period. bundle=%s path=%s",
                self.name,
                self.auto_publish_state_path,
            )
            return None
        return _AutoPublishCandidateState(
            source_type=source_type,
            source_identity=source_identity,
            source_signature=source_signature,
            first_observed_at=float(first_observed_at),
        )

    def _write_auto_publish_candidate_state(self, state: _AutoPublishCandidateState) -> None:
        _remove_orphaned_atomic_temp_files(self.auto_publish_state_path)
        _write_json_atomically(
            self.auto_publish_state_path,
            {
                "schema_version": AUTO_PUBLISH_STATE_SCHEMA_VERSION,
                "bundle_name": self.name,
                "source_type": state.source_type,
                "source_identity": state.source_identity,
                "source_signature": state.source_signature,
                "first_observed_at": state.first_observed_at,
            },
        )

    def _read_current_manifest_ref_or_none(self) -> BundleManifestRef | None:
        try:
            return self._read_current_manifest_ref()
        except BundleManifestNotFoundError:
            return None

    def _validation_marker_path(self, version: str) -> Path:
        # A file, not a directory: the orphan sweep only matches temp snapshot directories.
        # Stock Airflow's stale-version cleanup does not know about the marker; a marker
        # without its version dir is reaped by _remove_orphaned_validation_markers.
        return self.versions_dir / f".{version}.validated"

    def _has_validated_cache(self, version: str, *, manifest_ref: BundleManifestRef | None) -> bool:
        """
        Return whether a validated cache tree exists for the version.

        The persisted marker skips only the per-file hashing pass, not validation entirely:
        stock Airflow's stale cleanup can leave a truncated tree behind (interrupted plain
        rmtree that knows nothing about the marker), and cache directories are owner-writable
        (a stock-cleanup requirement), so structure — file set, types, no symlinks — is
        re-checked once per process even when the marker vouches for the content hashes.
        """
        cached_version_path = self.versions_dir / version
        if not cached_version_path.exists():
            return False
        cache_key = (self.name, version, str(cached_version_path))
        if cache_key in self._validated_version_paths:
            return True
        if not self._validation_marker_path(version).exists():
            return False
        try:
            self._validate_materialized_version(
                version_path=cached_version_path,
                version=version,
                manifest_ref=manifest_ref,
                check_content=False,
            )
        except (BundleManifestError, OSError):
            log.warning(
                "Cached snapshot has a validation marker but failed the structural check; "
                "it must be rebuilt. bundle=%s version=%s",
                self.name,
                version,
                exc_info=True,
            )
            return False
        self._validated_version_paths.add(cache_key)
        return True

    def _record_validated_cache(self, version: str, cache_key: tuple[str, str, str]) -> None:
        """Persist a marker after the cache tree and its parent entry are durable."""
        marker_path = self._validation_marker_path(version)
        marker_path.write_text(version)
        with suppress(OSError):
            marker_path.chmod(0o644)
        _fsync_file(marker_path)
        _fsync_directory(marker_path.parent)
        self._validated_version_paths.add(cache_key)

    @contextmanager
    def acquire_publication_lock(self):
        """Serialize publishers through a lock stored with the authoritative snapshots."""
        _ensure_public_dir(self.publication_lock_path.parent)
        # flock works on a read-only descriptor, so publishers running as different OS
        # users can all acquire the lock created by whichever user published first.
        try:
            fd = os.open(self.publication_lock_path, os.O_CREAT | os.O_RDONLY, 0o644)
        except PermissionError as e:
            raise BundleManifestError(
                f"Cannot open publication lock {self.publication_lock_path}: another OS user "
                "created it with restrictive permissions. Make it world-readable (chmod 0644) "
                "and retry."
            ) from e
        with os.fdopen(fd, "r") as lock_file:
            # os.open masks the mode with the umask; repair it whenever this publisher
            # owns the file so a crash cannot leave it unreadable for other publishers.
            with suppress(OSError):
                os.fchmod(fd, 0o644)
            flock(lock_file, LOCK_EX)
            try:
                yield
            finally:
                flock(lock_file, LOCK_UN)

    def _materialize_cached_version(
        self,
        *,
        version: str,
        manifest_ref: BundleManifestRef | None,
    ) -> None:
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        _remove_orphaned_manifest_temp_snapshots(self.versions_dir)
        self._remove_orphaned_validation_markers()
        cached_version_path = self.versions_dir / version
        cache_key = (self.name, version, str(cached_version_path))
        if cached_version_path.exists():
            # Cache files are read-only, so one full checksum validation per host is
            # enough; later calls need only the marker plus a structural re-check.
            if self._has_validated_cache(version, manifest_ref=manifest_ref):
                return
            try:
                self._validate_materialized_version(
                    version_path=cached_version_path,
                    version=version,
                    manifest_ref=manifest_ref,
                )
            except (BundleManifestError, OSError):
                log.warning(
                    "Cached snapshot failed validation and will be rebuilt. bundle=%s version=%s",
                    self.name,
                    version,
                    exc_info=True,
                )
                self._move_cached_version_aside(cached_version_path, version=version)
            else:
                _fsync_tree(cached_version_path)
                _fsync_directory(self.versions_dir)
                self._record_validated_cache(version, cache_key)
                return

        published_version_path = self.published_versions_dir / version
        if not published_version_path.is_dir():
            raise BundleManifestNotFoundError(
                f"Bundle '{self.name}' version '{version}' is not published at {published_version_path}. "
                "Publish or restore the immutable snapshot before running pinned work."
            )

        # Structural pass only (no hashing): reject symlinks, special files, and
        # unexpected entries before copytree touches them.
        self._validate_materialized_version(
            version_path=published_version_path,
            version=version,
            manifest_ref=manifest_ref,
            check_content=False,
        )

        tmp_path = Path(
            tempfile.mkdtemp(
                prefix=_get_manifest_temp_snapshot_prefix(version),
                dir=self.versions_dir,
            )
        )
        try:
            shutil.copytree(published_version_path, tmp_path, copy_function=shutil.copy2, dirs_exist_ok=True)
            # Validating the copy also validates the published source in one read pass.
            self._validate_materialized_version(
                version_path=tmp_path,
                version=version,
                manifest_ref=manifest_ref,
            )
            _set_cache_tree_permissions(tmp_path)
            _fsync_tree(tmp_path)
            os.replace(tmp_path, cached_version_path)
            _fsync_directory(self.versions_dir)
        except Exception:
            # Best effort only: a cleanup failure must not mask the original error.
            with suppress(OSError):
                remove_bundle_tree_forcefully(tmp_path)
            raise
        self._record_validated_cache(version, cache_key)

    def _move_cached_version_aside(self, cached_version_path: Path, *, version: str) -> None:
        # Rename instead of delete: a concurrent task may still read the old tree, and a
        # POSIX rename keeps its already-open files valid. The orphan sweep removes it later.
        self._validation_marker_path(version).unlink(missing_ok=True)
        # Also drop stock Airflow's usage-tracking file: its stale cleanup rmtree's the
        # tracked path catching only BlockingIOError, so a tracking file pointing at a
        # version dir this rename removes would crash every future cleanup sweep (and, on
        # a scheduler with a local executor, the scheduler itself). The next task run's
        # BundleVersionLock recreates the file.
        with suppress(OSError):
            get_bundle_tracking_file(bundle_name=self.name, version=version).unlink(missing_ok=True)
        cache_key = (self.name, version, str(cached_version_path))
        self._validated_version_paths.discard(cache_key)
        aside_dir = Path(
            tempfile.mkdtemp(
                prefix=_get_manifest_temp_snapshot_prefix(version),
                dir=self.versions_dir,
            )
        )
        with suppress(OSError):
            cached_version_path.chmod(stat.S_IMODE(cached_version_path.stat().st_mode) | 0o700)
        # FileNotFoundError: a concurrent stale-version cleanup already removed the tree;
        # the rebuild that follows is all that is needed.
        with suppress(FileNotFoundError):
            os.rename(cached_version_path, aside_dir / "snapshot")

    def _validate_materialized_version(
        self,
        *,
        version_path: Path,
        version: str,
        manifest_ref: BundleManifestRef | None,
        check_content: bool = True,
    ) -> None:
        if manifest_ref:
            if manifest_ref.version != version:
                raise BundleManifestError(
                    f"Bundle manifest reference contains version {manifest_ref.version!r}, "
                    f"expected {version!r}"
                )
            self._validate_snapshot_for_ref(manifest_ref, version_path, check_content=check_content)
            return

        snapshot_manifest = self._read_local_snapshot_manifest(version_path)
        self._validate_snapshot_manifest(snapshot_manifest, expected_version=version)
        self._validate_snapshot_files(snapshot_manifest, version_path, check_content=check_content)

    def _ensure_current_manifest_ref(self) -> BundleManifestRef:
        # Only a cheap read of the release reference. Snapshot materialization and full
        # checksum validation happen in initialize()/refresh(); ``path`` can still do the
        # required structural check of a cache copy that has a validation marker.
        if self._current_manifest_ref is None:
            self._current_manifest_ref = self._read_current_manifest_ref()
        return self._current_manifest_ref

    def _read_current_manifest_ref(self) -> BundleManifestRef:
        payload = self._read_json_file(
            self.manifest_ref_path,
            missing_message=(
                f"Bundle '{self.name}' manifest reference file {self.manifest_ref_path} is missing. "
                "Configure an automatic publication source, run the explicit publisher, or "
                "restore the release reference before refreshing this bundle."
            ),
            invalid_message=f"Bundle manifest reference is not valid JSON: {self.manifest_ref_path}",
        )
        return self._manifest_ref_from_payload(payload, source=str(self.manifest_ref_path))

    def _manifest_ref_from_payload(self, payload: dict[str, Any], source: str) -> BundleManifestRef:
        schema_version = payload.get("schema_version")
        if schema_version != MANIFEST_SCHEMA_VERSION:
            raise BundleManifestError(
                f"Bundle manifest reference {source} has unsupported schema_version {schema_version!r}"
            )

        bundle_name = payload.get("bundle_name")
        if bundle_name != self.name:
            raise BundleManifestError(
                f"Bundle manifest reference {source} is for bundle {bundle_name!r}, "
                f"expected {self.name!r}"
            )

        version = payload.get("version")
        if not isinstance(version, str) or not version:
            raise BundleManifestError(f"Bundle manifest reference {source} does not contain a version")
        version = _validate_manifest_version(version, source=source)

        backend = payload.get("backend")
        if not isinstance(backend, dict) or backend.get("type") != FILESYSTEM_ARTIFACT_BACKEND_TYPE:
            raise BundleManifestError(f"Bundle manifest reference {source} is not for a local backend")

        manifest_data = payload.get("manifest")
        if not isinstance(manifest_data, dict):
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain manifest data"
            )

        manifest_path = manifest_data.get("path")
        manifest_sha256 = manifest_data.get("sha256")
        if not isinstance(manifest_path, str):
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid manifest path"
            )
        if not isinstance(manifest_sha256, str) or not manifest_sha256:
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain manifest sha256"
            )
        self._validate_manifest_ref_path(manifest_path)

        file_count = payload.get("file_count")
        total_size = payload.get("total_size")
        # bool is an int subclass and True == 1, so it would slip through every check below.
        if not isinstance(file_count, int) or isinstance(file_count, bool) or file_count < 0:
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid file_count"
            )
        if not isinstance(total_size, int) or isinstance(total_size, bool) or total_size < 0:
            raise BundleManifestError(
                f"Bundle manifest reference {source} does not contain a valid total_size"
            )

        ref_payload = build_ref_payload(
            bundle_name=bundle_name,
            version=version,
            backend={"type": FILESYSTEM_ARTIFACT_BACKEND_TYPE},
            manifest_sha256=manifest_sha256,
            file_count=file_count,
            total_size=total_size,
        )
        source_metadata = payload.get("source")
        if source_metadata is not None:
            self._validate_release_source_metadata(source_metadata, source=source)
            ref_payload["source"] = dict(source_metadata)
        return BundleManifestRef(
            version=version,
            ref_payload=ref_payload,
            manifest_sha256=manifest_sha256,
        )

    @staticmethod
    def _validate_release_source_metadata(source_metadata: Any, *, source: str) -> None:
        if not isinstance(source_metadata, dict):
            raise BundleManifestError(
                f"Bundle manifest reference {source} contains invalid source metadata"
            )
        source_type = source_metadata.get("type")
        source_identity = source_metadata.get("identity")
        source_observation = source_metadata.get("observation")
        if (
            not isinstance(source_type, str)
            or not source_type
            or not isinstance(source_identity, str)
            or not source_identity.startswith("sha256:")
            or not isinstance(source_observation, str)
            or not source_observation.startswith("sha256:")
        ):
            raise BundleManifestError(
                f"Bundle manifest reference {source} contains invalid source metadata"
            )
        deployment_marker = source_metadata.get("deployment_marker")
        if deployment_marker is not None and (
            not isinstance(deployment_marker, str)
            or not deployment_marker.startswith("sha256:")
        ):
            raise BundleManifestError(
                f"Bundle manifest reference {source} contains invalid source metadata"
            )

    @staticmethod
    def _validate_manifest_ref_path(manifest_path: str) -> None:
        relative_path = _validate_relative_path(manifest_path)
        if relative_path.as_posix() != MANIFEST_FILE_NAME:
            raise BundleManifestError(
                f"Bundle manifest reference must point to {MANIFEST_FILE_NAME!r}, got {manifest_path!r}"
            )

    def _validate_snapshot_for_ref(
        self,
        manifest_ref: BundleManifestRef,
        version_path: Path,
        *,
        check_content: bool = True,
    ) -> None:
        snapshot_manifest = self._read_local_snapshot_manifest(version_path)
        self._validate_snapshot_manifest(
            snapshot_manifest,
            expected_version=manifest_ref.version,
            expected_file_count=manifest_ref.ref_payload["file_count"],
            expected_total_size=manifest_ref.ref_payload["total_size"],
        )
        verify_bundle_version_manifest(snapshot_manifest, manifest_ref.manifest_sha256)
        self._validate_snapshot_files(snapshot_manifest, version_path, check_content=check_content)

    def _validate_snapshot_manifest(
        self,
        manifest: dict[str, Any],
        *,
        expected_version: str | None,
        expected_file_count: int | None = None,
        expected_total_size: int | None = None,
    ) -> None:
        schema_version = manifest.get("schema_version")
        if schema_version != MANIFEST_SCHEMA_VERSION:
            raise BundleManifestError(
                f"Bundle manifest has unsupported schema_version {schema_version!r}"
            )
        if manifest.get("bundle_name") != self.name:
            raise BundleManifestError(
                f"Bundle manifest is for bundle {manifest.get('bundle_name')!r}, expected {self.name!r}"
            )
        if expected_version and manifest.get("version") != expected_version:
            raise BundleManifestError(
                f"Bundle manifest contains version {manifest.get('version')!r}, "
                f"expected {expected_version!r}"
            )

        backend = manifest.get("backend")
        if not isinstance(backend, dict) or backend.get("type") != FILESYSTEM_ARTIFACT_BACKEND_TYPE:
            raise BundleManifestError("Bundle manifest is not for a local backend")

        files = manifest.get("files")
        if not isinstance(files, list):
            raise BundleManifestError("Bundle manifest files must be a list")

        seen_paths: set[str] = set()
        path_order: list[str] = []
        total_size = 0
        for file_info in files:
            relative_path = self._validate_manifest_file_info(file_info)
            if relative_path in seen_paths:
                raise BundleManifestError(f"Bundle manifest contains duplicate path {relative_path!r}")
            seen_paths.add(relative_path)
            path_order.append(relative_path)
            total_size += file_info["size"]

        if path_order != sorted(path_order):
            raise BundleManifestError("Bundle manifest paths must be sorted")

        if manifest.get("file_count") != len(files):
            raise BundleManifestError(
                f"Bundle manifest file_count {manifest.get('file_count')!r} does not match "
                f"{len(files)} files"
            )
        if manifest.get("total_size") != total_size:
            raise BundleManifestError(
                f"Bundle manifest total_size {manifest.get('total_size')!r} does not match "
                f"{total_size} bytes"
            )
        if expected_file_count is not None and manifest["file_count"] != expected_file_count:
            raise BundleManifestError(
                f"Bundle manifest file_count {manifest['file_count']!r} does not match "
                f"manifest reference {expected_file_count!r}"
            )
        if expected_total_size is not None and manifest["total_size"] != expected_total_size:
            raise BundleManifestError(
                f"Bundle manifest total_size {manifest['total_size']!r} does not match "
                f"manifest reference {expected_total_size!r}"
            )

        computed_version = compute_bundle_version(files)
        if manifest.get("version") != computed_version:
            raise BundleManifestError(
                f"Bundle manifest version {manifest.get('version')!r} does not match "
                f"computed content version {computed_version!r}"
            )

    @staticmethod
    def _validate_manifest_file_info(file_info: Any) -> str:
        if not isinstance(file_info, dict):
            raise BundleManifestError("Bundle manifest file entries must be objects")
        relative_path = file_info.get("path")
        if not isinstance(relative_path, str):
            raise BundleManifestError("Bundle manifest file entry does not contain a valid path")
        _validate_relative_path(relative_path)
        if relative_path == MANIFEST_FILE_NAME:
            raise BundleManifestError(
                f"Bundle manifest must not list {MANIFEST_FILE_NAME!r} as a bundle file"
            )

        digest = file_info.get("sha256")
        if not isinstance(digest, str) or not digest:
            raise BundleManifestError(
                f"Bundle manifest entry {relative_path!r} does not contain a valid sha256"
            )
        size = file_info.get("size")
        if not isinstance(size, int) or size < 0:
            raise BundleManifestError(
                f"Bundle manifest entry {relative_path!r} does not contain a valid size"
            )
        if not isinstance(file_info.get("executable"), bool):
            raise BundleManifestError(
                f"Bundle manifest entry {relative_path!r} does not contain a valid executable flag"
            )
        return relative_path

    def _validate_snapshot_files(
        self,
        snapshot_manifest: dict[str, Any],
        version_path: Path,
        *,
        check_content: bool = True,
    ) -> None:
        # check_content=False keeps the structural checks (path safety, symlinks, file
        # types, unexpected files) but skips the per-file hashing pass.
        expected_paths = {file_info["path"] for file_info in snapshot_manifest["files"]}
        observed_paths = {
            path.relative_to(version_path).as_posix()
            for path in self._iter_snapshot_source_files(version_path)
        }
        unexpected_paths = sorted(observed_paths - expected_paths)
        if unexpected_paths:
            raise BundleManifestError(
                f"Bundle snapshot {version_path} contains files not present in the manifest: "
                f"{', '.join(unexpected_paths)}"
            )

        for file_info in snapshot_manifest["files"]:
            relative_path = _validate_relative_path(file_info["path"])
            source = version_path / relative_path
            self._ensure_destination_is_within_root(version_path, source)
            if source.is_symlink():
                raise BundleManifestError(
                    f"Bundle snapshot file {file_info['path']!r} is a symlink; symlinks are not "
                    "supported in manifest bundle snapshots"
                )
            try:
                file_stat = source.stat()
            except FileNotFoundError:
                raise BundleManifestError(
                    f"Bundle snapshot {version_path} is missing manifest entry {file_info['path']!r}"
                ) from None
            if not stat.S_ISREG(file_stat.st_mode):
                raise BundleManifestError(f"Bundle snapshot file {file_info['path']!r} is not a regular file")
            if not check_content:
                continue
            file_sha256, size = compute_file_sha256(source)
            executable = bool(stat.S_IMODE(file_stat.st_mode) & 0o111)
            if (
                size != file_info["size"]
                or file_sha256 != file_info["sha256"]
                or executable != file_info["executable"]
            ):
                raise BundleManifestError(
                    f"Bundle snapshot file {file_info['path']!r} does not match local snapshot manifest"
                )

    @staticmethod
    def _iter_snapshot_source_files(snapshot_root: Path):
        def raise_walk_error(error: OSError) -> None:
            raise BundleManifestError(
                f"Bundle snapshot {snapshot_root} changed or became unreadable while validating: "
                f"{error.filename}"
            ) from error

        for dirpath, dirnames, filenames in os.walk(
            snapshot_root,
            followlinks=False,
            onerror=raise_walk_error,
        ):
            dirpath_path = Path(dirpath)
            relative_dirpath = dirpath_path.relative_to(snapshot_root).as_posix()
            if dirpath_path.is_symlink():
                raise BundleManifestError(
                    f"Bundle snapshot {snapshot_root} contains symlinked directory "
                    f"{relative_dirpath!r}; symlinks are not supported in manifest bundle snapshots"
                )

            for dirname in dirnames:
                path = dirpath_path / dirname
                if path.is_symlink():
                    relative_path = path.relative_to(snapshot_root).as_posix()
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains symlinked directory "
                        f"{relative_path!r}; symlinks are not supported in manifest bundle snapshots"
                    )
                if not path.is_dir():
                    relative_path = path.relative_to(snapshot_root).as_posix()
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains non-directory entry "
                        f"{relative_path!r} where a directory was expected"
                    )
            dirnames[:] = sorted(dirnames)
            for filename in sorted(filenames):
                path = Path(dirpath) / filename
                relative_path = path.relative_to(snapshot_root).as_posix()
                if path.is_symlink():
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains symlinked file "
                        f"{relative_path!r}; symlinks are not supported in manifest bundle snapshots"
                    )
                try:
                    file_stat = path.stat()
                except FileNotFoundError as e:
                    raise BundleManifestSourceChangedError(
                        f"Bundle snapshot file disappeared during validation: "
                        f"{relative_path}"
                    ) from e
                if not stat.S_ISREG(file_stat.st_mode):
                    raise BundleManifestError(
                        f"Bundle snapshot {snapshot_root} contains non-regular file {relative_path!r}"
                    )
                if relative_path == MANIFEST_FILE_NAME:
                    continue
                yield path

    @staticmethod
    def _read_local_snapshot_manifest(version_path: Path) -> dict[str, Any]:
        manifest_file = version_path / MANIFEST_FILE_NAME
        if not manifest_file.exists():
            raise FileNotFoundError(
                f"Bundle snapshot at {version_path} does not contain {MANIFEST_FILE_NAME}. "
                "Publish or restore the immutable snapshot before updating the manifest reference."
            )
        if manifest_file.is_symlink():
            raise BundleManifestError(
                f"Bundle snapshot manifest {manifest_file} is a symlink; symlinks are not supported "
                "in manifest bundle snapshots"
            )
        if not manifest_file.is_file():
            raise BundleManifestError(f"Bundle snapshot manifest {manifest_file} is not a regular file")
        return ManifestDagBundleBase._read_json_file(
            manifest_file,
            missing_message=(
                f"Bundle snapshot at {version_path} does not contain {MANIFEST_FILE_NAME}. "
                "Publish or restore the immutable snapshot before updating the manifest reference."
            ),
            invalid_message=f"Bundle snapshot manifest is not valid JSON: {manifest_file}",
        )

    @staticmethod
    def _read_json_file(path: Path, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise BundleManifestNotFoundError(missing_message) from e
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(invalid_message) from e

        if not isinstance(payload, dict):
            raise BundleManifestError(invalid_message)
        return payload

    @staticmethod
    def _ensure_destination_is_within_root(root: Path, destination: Path) -> None:
        try:
            destination.resolve().relative_to(root.resolve())
        except ValueError:
            raise BundleManifestError(
                f"Bundle manifest path {destination} resolves outside snapshot root {root}"
            ) from None

    @property
    def path(self) -> Path:
        if self.version:
            version = _validate_manifest_version(self.version, source="pinned bundle version")
            return self.versions_dir / version
        with _oserror_as_manifest_error():
            manifest_ref = self._ensure_current_manifest_ref()
            version = manifest_ref.version
            current_version_path = self.versions_dir / version
            if self._has_validated_cache(version, manifest_ref=manifest_ref):
                return current_version_path

            # Stock core reads `path` on bundles it never initializes (callbacks without a
            # bundle_version, priority parse requests before the first refresh tick). A just-
            # published version is not materialized yet at that point — and callback rows are
            # already deleted from the DB, so a nonexistent path loses the callback outright.
            # Serving the newest validated cached version is strictly better than that.
            fallback = self._latest_validated_cached_version_path(exclude_version=version)
            if fallback is not None:
                log.warning(
                    "Bundle version is not materialized yet; falling back to the newest cached "
                    "version. bundle=%s version=%s fallback=%s",
                    self.name,
                    version,
                    fallback.name,
                )
                return fallback

            if current_version_path.exists():
                raise BundleManifestError(
                    f"Bundle '{self.name}' version '{version}' has an unvalidated or structurally "
                    f"invalid cache copy at {current_version_path}. Initialize or refresh the bundle "
                    "to rebuild it before use."
                )
            return current_version_path

    def _latest_validated_cached_version_path(self, *, exclude_version: str) -> Path | None:
        """Newest marked cache version that passes this process's structural check."""
        candidates: list[tuple[float, str]] = []
        try:
            entries = list(self.versions_dir.iterdir())
        except OSError:
            return None
        for entry in entries:
            version = _marker_file_version(entry)
            if version is None or version == exclude_version:
                continue
            with suppress(OSError):
                candidates.append((entry.stat().st_mtime, version))

        for _, version in sorted(candidates, reverse=True):
            if self._has_validated_cache(version, manifest_ref=None):
                return self.versions_dir / version
        return None

    def _remove_orphaned_validation_markers(self) -> None:
        """
        Reap validation markers whose version dir is gone.

        Stock Airflow's stale-version cleanup removes only the version dir and its tracking
        file; without this sweep every version ever materialized would leak one marker file.
        """
        try:
            entries = list(self.versions_dir.iterdir())
        except OSError:
            return
        for entry in entries:
            version = _marker_file_version(entry)
            if version is None:
                continue
            if not (self.versions_dir / version).exists():
                with suppress(OSError):
                    entry.unlink()


def publish_prepared_manifest_dag_bundle(
    *,
    bundle: ManifestDagBundleBase,
    prepared_source: PreparedPublishSource,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish one prepared local tree as an immutable manifest-backed snapshot."""
    source_path = prepared_source.root
    _validate_publish_paths(
        source_path=source_path,
        published_root=bundle.published_root,
        versions_dir=bundle.versions_dir,
    )
    if not prepared_source.source_snapshot.files and not bundle.allow_empty_source:
        raise BundleManifestError(
            f"Refusing to explicitly publish empty source tree {source_path} for bundle "
            f"'{bundle.name}'; set allow_empty_source=True if removing every Dag is intended"
        )
    _ensure_public_dir(bundle.published_versions_dir)
    _ensure_public_dir(bundle.manifest_ref_path.parent)
    with bundle.acquire_publication_lock():
        if expected_current_version is not None:
            expected_current_version = _validate_manifest_version(
                expected_current_version,
                source="expected current bundle version",
            )
            try:
                current_version = bundle._read_current_manifest_ref().version
            except FileNotFoundError as e:
                raise BundleManifestReferenceChangedError(
                    f"Bundle '{bundle.name}' has no published version to compare with "
                    "--expected-current-version. Omit the option for the initial publication."
                ) from e
            if current_version != expected_current_version:
                raise BundleManifestReferenceChangedError(
                    f"Bundle '{bundle.name}' manifest reference changed: expected "
                    f"{expected_current_version!r}, found {current_version!r}"
                )

        manifest_result = build_bundle_version_manifest_result(
            bundle_name=bundle.name,
            root=source_path,
            backend_type=FILESYSTEM_ARTIFACT_BACKEND_TYPE,
            source_snapshot=prepared_source.source_snapshot,
            precomputed_file_sha256=prepared_source.file_sha256_by_path,
        )
        current_ref = bundle._read_current_manifest_ref_or_none()
        bundle._validate_release_transition(
            current_ref=current_ref,
            prepared=prepared_source,
            target_version=manifest_result.version,
        )
        return _publish_manifest_result(
            bundle=bundle,
            manifest_result=manifest_result,
            prepared_source=prepared_source,
        )


def _publish_manifest_result(
    *,
    bundle: ManifestDagBundleBase,
    manifest_result: BundleVersionManifest,
    prepared_source: PreparedPublishSource,
) -> BundlePublishResult:
    """Publish a precomputed manifest while the caller holds the publication lock."""
    ref_payload = dict(manifest_result.ref_payload)
    if prepared_source.release_source_metadata is not None:
        ref_payload["source"] = prepared_source.release_source_metadata
    manifest_ref = bundle._manifest_ref_from_payload(ref_payload, source="publisher")
    version_path = bundle.published_versions_dir / manifest_result.version

    _remove_orphaned_manifest_temp_snapshots(bundle.published_versions_dir)
    _remove_orphaned_atomic_temp_files(bundle.manifest_ref_path)
    created_snapshot = False
    if version_path.exists():
        if not version_path.is_dir():
            raise FileExistsError(f"Bundle snapshot path exists but is not a directory: {version_path}")
        bundle._validate_snapshot_for_ref(manifest_ref, version_path)
    else:
        _materialize_manifest_snapshot(
            source_path=prepared_source.root,
            manifest=manifest_result.manifest,
            versions_dir=bundle.published_versions_dir,
            version_path=version_path,
        )
        created_snapshot = True

    bundle._confirm_publish_source(prepared_source)
    _write_manifest_ref_atomically(bundle.manifest_ref_path, ref_payload)

    return BundlePublishResult(
        bundle_name=bundle.name,
        version=manifest_result.version,
        ref_payload=ref_payload,
        version_path=version_path,
        manifest_ref_path=bundle.manifest_ref_path,
        manifest_sha256=manifest_ref.manifest_sha256,
        file_count=ref_payload["file_count"],
        total_size=ref_payload["total_size"],
        created_snapshot=created_snapshot,
    )


def _materialize_manifest_snapshot(
    *,
    source_path: Path,
    manifest: dict[str, Any],
    versions_dir: Path,
    version_path: Path,
) -> None:
    version = manifest["version"]
    tmp_path = Path(
        tempfile.mkdtemp(
            prefix=_get_manifest_temp_snapshot_prefix(version),
            dir=versions_dir,
        )
    )
    try:
        for file_info in manifest["files"]:
            relative_path = _validate_relative_path(file_info["path"])
            source = source_path / relative_path
            destination = tmp_path / relative_path
            ManifestDagBundleBase._ensure_destination_is_within_root(tmp_path, destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            _fsync_file(destination)
            # Hash the copy inline: this verifies both the copy and the source against
            # the manifest without a second full walk over the materialized tree. The
            # executable bit is part of the content address, so it must be checked here
            # too — failing after os.replace would leave a published snapshot whose modes
            # contradict its own manifest, permanently blocking that content version.
            destination_sha256, destination_size = compute_file_sha256(destination)
            destination_executable = bool(stat.S_IMODE(destination.stat().st_mode) & 0o111)
            if (
                destination_sha256 != file_info["sha256"]
                or destination_size != file_info["size"]
                or destination_executable != file_info["executable"]
            ):
                raise BundleManifestSourceChangedError(
                    f"Bundle source changed while materializing bundle version {version}"
                )

        _write_manifest_file(manifest, tmp_path / MANIFEST_FILE_NAME)
        _set_snapshot_permissions(tmp_path)
        _fsync_tree_directories(tmp_path)
        os.replace(tmp_path, version_path)
        _fsync_directory(versions_dir)
    except Exception:
        # Best effort only: a cleanup failure must not mask the original error.
        with suppress(OSError):
            remove_bundle_tree_forcefully(tmp_path)
        raise


def _get_manifest_temp_snapshot_prefix(version: str) -> str:
    return f".{version}."


def _remove_orphaned_manifest_temp_snapshots(versions_dir: Path) -> None:
    if not versions_dir.exists():
        return

    removed_orphans = False
    for path in versions_dir.iterdir():
        if not _is_manifest_temp_snapshot_dir(path):
            continue
        try:
            remove_bundle_tree_forcefully(path)
        except OSError:
            # One undeletable orphan (e.g. owned by another OS user) must not block
            # materialization of valid versions.
            log.warning("Could not remove orphaned temp snapshot %s", path, exc_info=True)
            continue
        removed_orphans = True

    if removed_orphans:
        _fsync_directory(versions_dir)


def _is_manifest_temp_snapshot_dir(path: Path) -> bool:
    if path.is_symlink() or not path.is_dir():
        return False

    name = path.name
    if not name.startswith(".sha256-"):
        return False

    digest, separator, suffix = name.removeprefix(".sha256-").partition(".")
    return (
        separator == "."
        and bool(suffix)
        and len(digest) == 64
        and all(char in "0123456789abcdef" for char in digest)
    )


def _remove_orphaned_atomic_temp_files(destination_path: Path) -> None:
    if not destination_path.parent.exists():
        return
    prefix = f".{destination_path.name}."
    for path in destination_path.parent.iterdir():
        if not path.name.startswith(prefix) or path.is_symlink() or not path.is_file():
            continue
        with suppress(OSError):
            path.unlink()


def _write_manifest_ref_atomically(manifest_ref_path: Path, ref_payload: dict[str, Any]) -> None:
    _write_json_atomically(manifest_ref_path, ref_payload)


def _write_json_atomically(destination_path: Path, payload: dict[str, Any]) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{destination_path.name}.",
        dir=destination_path.parent,
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(serialize_bundle_version_manifest(payload))
            file.flush()
            # mkstemp creates the file owner-only; consumers can run as a different OS user.
            os.fchmod(file.fileno(), 0o644)
            os.fsync(file.fileno())
        os.replace(tmp_path, destination_path)
        _fsync_directory(destination_path.parent)
    except Exception:
        # missing_ok: an exists()/unlink() pair could itself raise here (e.g. an external
        # tmp sweeper won the race) and mask the original publish error.
        with suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


def _ensure_public_dir(path: Path) -> None:
    """
    Create ``path`` with world-traversable permissions.

    Every directory this call creates — including ancestors above ``published_root`` — is
    chmodded 0755. Pre-existing directories (for example an admin-provisioned
    ``published_root`` the publisher does not own) keep their permissions and remain the
    deployer's contract.
    """
    missing: list[Path] = []
    current = Path(os.path.abspath(path))
    while not current.exists() and current.parent != current:
        missing.append(current)
        current = current.parent
    path.mkdir(parents=True, exist_ok=True)
    for created in missing:
        # A concurrent publisher may have created (and own) the directory; it applies
        # the same permissions itself.
        with suppress(OSError):
            created.chmod(0o755)


def _validate_publish_paths(
    *,
    source_path: Path,
    published_root: Path,
    versions_dir: Path,
) -> None:
    if _is_same_or_nested_path(published_root, source_path):
        raise ValueError(
            "published_root must not be inside source_path. Keep published snapshots "
            "outside the Dag source tree before publishing a manifest bundle."
        )

    if _is_same_or_nested_path(source_path, published_root):
        raise ValueError(
            "source_path must not be inside published_root. Keep the Dag source tree "
            "outside the published snapshots before publishing a manifest bundle."
        )

    if _paths_overlap(source_path, versions_dir):
        raise ValueError(
            "source_path and versions_dir must not overlap. Keep Airflow's bundle cache outside "
            "the Dag source tree before publishing a manifest bundle."
        )


def _is_same_or_nested_path(path: Path, root: Path) -> bool:
    absolute_path = Path(os.path.abspath(path))
    absolute_root = Path(os.path.abspath(root))
    return absolute_path.is_relative_to(absolute_root) or path.resolve(strict=False).is_relative_to(
        root.resolve(strict=False)
    )


def _paths_overlap(first: Path, second: Path) -> bool:
    return _is_same_or_nested_path(first, second) or _is_same_or_nested_path(second, first)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as file:
        os.fsync(file.fileno())


def _fsync_directory(path: Path) -> None:
    flags = getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, os.O_RDONLY | flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree_directories(root: Path) -> None:
    directories = [Path(dirpath) for dirpath, _, _ in os.walk(root)]
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        _fsync_directory(directory)


def _fsync_tree(root: Path) -> None:
    """Make all regular files and directory entries in a tree durable."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            if not path.is_symlink():
                _fsync_file(path)
    _fsync_tree_directories(root)


def _write_manifest_file(manifest: dict[str, Any], manifest_file: Path) -> None:
    with manifest_file.open("wb") as file:
        file.write(serialize_bundle_version_manifest(manifest))
        file.flush()
        os.fsync(file.fileno())


def _set_snapshot_permissions(snapshot_path: Path) -> None:
    # Snapshots must be read-only and world-readable: the publisher and the Airflow
    # components reading published_root may run as different OS users.
    directories: list[Path] = []
    for dirpath, _, filenames in os.walk(snapshot_path):
        directories.append(Path(dirpath))
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.is_symlink():
                continue
            executable = stat.S_IMODE(path.stat().st_mode) & 0o111
            path.chmod(0o555 if executable else 0o444)
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(0o555)


def _set_cache_tree_permissions(cache_path: Path) -> None:
    # Cache copies keep files read-only (content drift stays detectable) but leave
    # directories writable: stock Airflow's stale-bundle cleanup removes cached versions
    # with a plain shutil.rmtree, which fails on read-only directories.
    for dirpath, _, filenames in os.walk(cache_path):
        directory = Path(dirpath)
        for filename in filenames:
            path = directory / filename
            if path.is_symlink():
                continue
            executable = stat.S_IMODE(path.stat().st_mode) & 0o111
            path.chmod(0o555 if executable else 0o444)
        directory.chmod(0o755)


def _is_valid_manifest_version(version: str) -> bool:
    digest = version.removeprefix(SHA256_VERSION_PREFIX)
    return digest != version and len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _validate_manifest_version(version: str, *, source: str) -> str:
    if not _is_valid_manifest_version(version):
        raise BundleManifestError(
            f"Bundle manifest reference {source} does not contain a valid sha256 version"
        )
    return version


def _marker_file_version(path: Path) -> str | None:
    """Return the version a ``.{version}.validated`` marker file certifies, or None."""
    name = path.name
    if not (name.startswith(".") and name.endswith(".validated")):
        return None
    version = name[1 : -len(".validated")]
    if not _is_valid_manifest_version(version):
        return None
    if path.is_symlink() or not path.is_file():
        return None
    return version


def _validate_relative_path(relative_path: str) -> Path:
    if "\\" in relative_path:
        raise BundleManifestError(f"Bundle manifest contains unsafe relative path: {relative_path!r}")
    path = Path(relative_path)
    if (
        path.is_absolute()
        or any(segment in {"", ".", ".."} for segment in relative_path.split("/"))
        or path.as_posix() != relative_path
    ):
        raise BundleManifestError(f"Bundle manifest contains unsafe relative path: {relative_path!r}")
    return path
