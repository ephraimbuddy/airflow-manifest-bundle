"""Local source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow_manifest_bundle.bundle import (
    ManifestDagBundleBase,
    PreparedPublishSource,
    _validate_publish_paths,
    publish_prepared_manifest_dag_bundle,
)
from airflow_manifest_bundle.manifest import (
    BundleManifestError,
    BundleManifestSourceChangedError,
    collect_bundle_source_snapshot,
)

if TYPE_CHECKING:
    from airflow_manifest_bundle.bundle import BundlePublishResult


class ManifestLocalDagBundle(ManifestDagBundleBase):
    """Manifest bundle that publishes an optional mutable local source directory."""

    def __init__(self, *, source_path: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.source_path = Path(source_path) if source_path else None
        if self.source_path is not None:
            try:
                _validate_publish_paths(
                    source_path=self.source_path,
                    published_root=self.published_root,
                    versions_dir=self.versions_dir,
                )
            except ValueError as e:
                # Stock callback preparation treats ValueError as "bundle removed".
                raise TypeError(str(e)) from e

    @property
    def _has_publish_source(self) -> bool:
        return self.source_path is not None

    @property
    def _publish_source_description(self) -> str:
        return str(self.source_path) if self.source_path is not None else "<none>"

    def _prepare_publish_source(self) -> PreparedPublishSource:
        source_path = self.source_path
        if source_path is None:
            raise BundleManifestError("Automatic local publication requires source_path")
        snapshot = collect_bundle_source_snapshot(source_path)
        return _prepare_local_source(source_path, snapshot=snapshot)

    def _confirm_publish_source(self, prepared: PreparedPublishSource) -> None:
        current_snapshot = collect_bundle_source_snapshot(prepared.root)
        if current_snapshot.signature != prepared.source_signature:
            raise BundleManifestSourceChangedError(
                "Bundle source changed while publishing the bundle snapshot"
            )


def _local_source_identity(source_path: Path) -> str:
    normalized = os.path.abspath(source_path)
    return f"sha256:{hashlib.sha256(normalized.encode()).hexdigest()}"


def _prepare_local_source(
    source_path: Path,
    *,
    snapshot: Any | None = None,
) -> PreparedPublishSource:
    snapshot = snapshot or collect_bundle_source_snapshot(source_path)
    return PreparedPublishSource(
        root=source_path,
        source_snapshot=snapshot,
        source_type="local",
        source_identity=_local_source_identity(source_path),
        source_signature=snapshot.signature,
    )


def publish_manifest_local_dag_bundle(
    *,
    bundle: ManifestLocalDagBundle,
    source_path: str | Path,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish a local source tree as an immutable manifest-backed snapshot."""
    source_path = Path(source_path)
    return publish_prepared_manifest_dag_bundle(
        bundle=bundle,
        prepared_source=_prepare_local_source(source_path),
        expected_current_version=expected_current_version,
    )


__all__ = [
    "ManifestLocalDagBundle",
    "publish_manifest_local_dag_bundle",
]
