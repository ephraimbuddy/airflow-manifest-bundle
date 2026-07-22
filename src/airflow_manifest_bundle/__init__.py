"""Manifest-backed local Dag bundle for Apache Airflow."""

from __future__ import annotations

__version__ = "0.1.0"

from airflow_manifest_bundle.local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)

__all__ = ["ManifestLocalDagBundle", "publish_manifest_local_dag_bundle"]
