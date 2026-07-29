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
- Locator properties currently return ``Path`` because the only implementation is
  filesystem-backed; they will widen to string locators when an object store lands.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from pathlib import Path


class ArtifactStore(ABC):
    """Storage backend for a bundle's published references, state, and snapshots."""

    bundle_name: str

    # --- locators -------------------------------------------------------------

    @property
    @abstractmethod
    def root(self) -> Path:
        """The published root this store manages."""

    @property
    @abstractmethod
    def ref_path(self) -> Path:
        """Locator of the release reference document."""

    @property
    @abstractmethod
    def state_path(self) -> Path:
        """Locator of the auto-publish state document."""

    @property
    @abstractmethod
    def snapshots_root(self) -> Path:
        """Locator of the published snapshots area for this bundle."""

    @abstractmethod
    def snapshot_path(self, version: str) -> Path:
        """Locator of one published snapshot."""

    # --- mutable documents ----------------------------------------------------

    @abstractmethod
    def read_ref(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        """Read the release reference payload."""

    @abstractmethod
    def write_ref(self, payload: dict[str, Any]) -> None:
        """Atomically replace the release reference. The last step of a publication."""

    @abstractmethod
    def read_state(self, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        """Read the auto-publish state payload."""

    @abstractmethod
    def write_state(self, payload: dict[str, Any]) -> None:
        """Atomically replace the auto-publish state."""

    # --- coordination ---------------------------------------------------------

    @abstractmethod
    def publication_guard(self) -> AbstractContextManager[None]:
        """Serialize publishers for this bundle across hosts."""

    @abstractmethod
    def prepare_publish_areas(self) -> None:
        """Create the snapshot and reference areas publishers write to."""

    @abstractmethod
    def prepare_state_area(self) -> None:
        """Create the area holding the auto-publish state document."""

    @abstractmethod
    def validate_source_paths(self, source_path: Path, *, cache_versions_dir: Path) -> None:
        """Reject a publish source that overlaps this store or the local cache."""

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
    ) -> bool:
        """
        Commit an immutable snapshot for ``version`` from a prepared local tree.

        Idempotent: when the version already exists, ``validate_existing`` checks it
        against the manifest reference and nothing is written. Returns whether a new
        snapshot was created.
        """

    @abstractmethod
    def sweep_publish_temps(self) -> None:
        """Remove leftover temporary artifacts from earlier failed publications."""


__all__ = ["ArtifactStore"]
