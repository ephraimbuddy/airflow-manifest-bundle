"""Manifest-backed Dag bundles for Apache Airflow."""

from __future__ import annotations

__version__ = "0.3.0"

from airflow_manifest_bundle.bundle import ManifestDagBundleBase
from airflow_manifest_bundle.local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)

__all__ = [
    "ManifestDagBundleBase",
    "ManifestLocalDagBundle",
    "publish_manifest_local_dag_bundle",
]
