"""
Contract for the published-artifact backend of manifest Dag bundles.

An artifact store keeps everything that lives under a bundle's ``published_root``:

- the **release reference** — one mutable document naming the current version;
- the **auto-publish state** — one mutable document holding the shared
  source-stability candidate;
- the **immutable snapshots** — one content-addressed tree per published version.

``ManifestDagBundleBase`` performs all published-artifact access through this
interface. The filesystem implementation lives in ``bundle.py`` next to the module
helpers it wraps; object-store implementations (for example S3) provide the same
operations with backend-native atomicity.

Contract notes for implementations:

- The two documents are the only mutable artifacts. ``publication_guard`` must make
  the read-reconcile-write sections that ``ManifestDagBundleBase`` runs inside it
  safe against concurrent publishers (a filesystem lock, or compare-and-swap
  semantics that surface conflicts as ``BundleManifestError``).
- Snapshots are immutable once committed and must not become visible half-written.
- Locator properties return ``Path`` for the filesystem store and string URLs for
  object stores; callers must not apply Path-only operations to them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.manifest import BundleManifestError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from contextlib import AbstractContextManager
    from pathlib import Path


class ArtifactStoreConflictError(BundleManifestError):
    """
    A concurrent publisher changed a mutable document between this store's read and write.

    Raised by stores whose ``publication_guard`` is not a mutual-exclusion lock (their
    document writes are compare-and-swap instead). Callers reconcile by re-reading:
    the losing publisher follows the winning release rather than overwriting it.
    """


class ArtifactStore(ABC):
    """Storage backend for a bundle's published references, state, and snapshots."""

    bundle_name: str

    #: Whether publishers can write releases through this store. A consume-only store
    #: keeps this False so adapters can reject publish configurations at construction
    #: time instead of failing on the first refresh.
    supports_publication: bool = True

    # --- locators -------------------------------------------------------------

    @property
    @abstractmethod
    def root(self) -> Path | str:
        """The published root this store manages."""

    @property
    @abstractmethod
    def ref_path(self) -> Path | str:
        """Locator of the release reference document."""

    @property
    @abstractmethod
    def state_path(self) -> Path | str:
        """Locator of the auto-publish state document."""

    @property
    @abstractmethod
    def snapshots_root(self) -> Path | str:
        """Locator of the published snapshots area for this bundle."""

    @abstractmethod
    def snapshot_path(self, version: str) -> Path | str:
        """Locator of one published snapshot."""

    # --- mutable documents ----------------------------------------------------

    @abstractmethod
    def read_ref(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        """Read the release reference payload."""

    @abstractmethod
    def write_ref(self, payload: dict[str, Any]) -> None:
        """
        Atomically replace the release reference. The last step of a publication.

        A compare-and-swap store conditions the write on the version it last read and
        raises ``ArtifactStoreConflictError`` when a concurrent publisher won — unless
        the winning document is byte-identical, which reports success (idempotent
        same-content publications must not conflict).
        """

    @abstractmethod
    def read_state(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        """Read the auto-publish state payload."""

    @abstractmethod
    def write_state(self, payload: dict[str, Any]) -> None:
        """Atomically replace the auto-publish state. Conflict semantics as ``write_ref``."""

    # --- coordination ---------------------------------------------------------

    @abstractmethod
    def publication_guard(self) -> AbstractContextManager[None]:
        """
        Serialize publishers for this bundle across hosts.

        Either a real mutual-exclusion lock (the filesystem store's flock), or a no-op
        for stores whose document writes are compare-and-swap — the caller's
        read-reconcile-write steps inside the guard plus ``ArtifactStoreConflictError``
        provide the equivalent safety.
        """

    @abstractmethod
    def prepare_publish_areas(self) -> None:
        """Create the snapshot and reference areas publishers write to."""

    @abstractmethod
    def prepare_state_area(self) -> None:
        """Create the area holding the auto-publish state document."""

    @abstractmethod
    def validate_source_paths(self, source_path: Path, *, cache_versions_dir: Path) -> None:
        """
        Reject a publish source that overlaps this store or the local cache.

        Raises ``ValueError`` for an overlap; a store with ``supports_publication``
        False raises ``BundleManifestError`` instead, like its other publish
        operations.
        """

    # --- snapshots ------------------------------------------------------------

    @abstractmethod
    def snapshot_exists(self, version: str) -> bool:
        """Whether a committed snapshot exists for the version."""

    @abstractmethod
    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        """
        Transfer a published snapshot into ``destination``.

        ``structural_validator`` runs against the published tree before any bytes are
        transferred when the backend exposes one; the caller fully validates the
        transferred copy afterwards either way. Raises
        ``BundleManifestNotFoundError`` when the version is not published.
        """

    @abstractmethod
    def publish_snapshot(
        self,
        version: str,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
        copy_hints: Mapping[str, dict[str, Any]] | None = None,
    ) -> bool:
        """
        Commit an immutable snapshot for ``version`` from a prepared local tree.

        Idempotent: when the version already exists, ``validate_existing`` checks it
        against the manifest reference and nothing is written. Returns whether a new
        snapshot was created.

        ``copy_hints`` (relative path -> backend-specific locator) may let the store
        move bytes without routing them through this host. They are an optimization
        only: any hint may be ignored or fail, in which case the store publishes from
        ``source_root``; correctness never depends on a hint because consumers verify
        every fetched file against the manifest.
        """

    @abstractmethod
    def sweep_publish_temps(self) -> None:
        """Remove leftover temporary artifacts from earlier failed publications."""


__all__ = ["ArtifactStore", "ArtifactStoreConflictError"]
