"""
Standalone publisher CLI.

External packages cannot add ``airflow`` subcommands, so publishing ships as its own
console script:

    airflow-manifest-bundle publish-local <bundle-name> <source-path>
    airflow-manifest-bundle publish-s3 <bundle-name>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_manifest_bundle.bundle import BundlePublishResult


def _get_manifest_local_bundle(bundle_name: str, *, action: str):
    from airflow.dag_processing.bundles.manager import DagBundlesManager

    from airflow_manifest_bundle.local import ManifestLocalDagBundle

    bundle = DagBundlesManager().get_bundle(bundle_name)
    if not isinstance(bundle, ManifestLocalDagBundle):
        raise SystemExit(
            f"Bundle {bundle_name!r} is not configured as a ManifestLocalDagBundle. "
            f"Only manifest-backed local bundles can be {action} with this command."
        )
    if bundle.source_path is not None:
        raise SystemExit(
            f"Bundle {bundle_name!r} has source_path configured for automatic publication. "
            "Remove source_path before using the explicit publisher command."
        )
    return bundle


def _get_manifest_s3_bundle(bundle_name: str, *, action: str):
    from airflow.dag_processing.bundles.manager import DagBundlesManager

    from airflow_manifest_bundle.s3 import ManifestS3DagBundle

    bundle = DagBundlesManager().get_bundle(bundle_name)
    if not isinstance(bundle, ManifestS3DagBundle):
        raise SystemExit(
            f"Bundle {bundle_name!r} is not configured as a ManifestS3DagBundle. "
            f"Only manifest-backed S3 bundles can be {action} with this command."
        )
    if bundle.auto_publish:
        raise SystemExit(
            f"Bundle {bundle_name!r} has auto_publish enabled for automatic publication. "
            "Set auto_publish=False before using the explicit publisher command."
        )
    return bundle


def _print_publish_result(result: BundlePublishResult, *, output: str) -> None:
    payload = {
        "bundle_name": result.bundle_name,
        "version": result.version,
        "version_path": str(result.version_path),
        "manifest_ref_path": str(result.manifest_ref_path),
        "manifest_sha256": result.manifest_sha256,
        "file_count": result.file_count,
        "total_size": result.total_size,
        "created_snapshot": result.created_snapshot,
    }
    if output == "json":
        print(json.dumps(payload, indent=2))
    else:
        for key, value in payload.items():
            print(f"{key}: {value}")


def publish_local(args: argparse.Namespace) -> None:
    """Publish an immutable local snapshot for a manifest-backed local Dag bundle."""
    from airflow_manifest_bundle.local import publish_manifest_local_dag_bundle

    bundle = _get_manifest_local_bundle(args.bundle_name, action="published")
    result = publish_manifest_local_dag_bundle(
        bundle=bundle,
        source_path=Path(args.source_path),
        expected_current_version=args.expected_current_version,
    )
    _print_publish_result(result, output=args.output)


def publish_s3(args: argparse.Namespace) -> None:
    """Publish the configured S3 source for a manifest-backed S3 Dag bundle."""
    from airflow_manifest_bundle.s3 import publish_manifest_s3_dag_bundle

    bundle = _get_manifest_s3_bundle(args.bundle_name, action="published")
    result = publish_manifest_s3_dag_bundle(
        bundle=bundle,
        expected_current_version=args.expected_current_version,
    )
    _print_publish_result(result, output=args.output)


def _add_common_publish_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--expected-current-version",
        default=None,
        help=(
            "Refuse to publish unless the currently released version matches. "
            "Protects out-of-order deploys from clobbering a newer release."
        ),
    )
    parser.add_argument("--output", choices=("table", "json"), default="table")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airflow-manifest-bundle",
        description="Manage manifest-backed Dag bundles.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser(
        "publish-local",
        help="Publish an immutable local snapshot and atomically update the release reference.",
    )
    publish.add_argument("bundle_name", help="Name of the configured ManifestLocalDagBundle")
    publish.add_argument("source_path", help="Local directory containing the Dag files to publish")
    _add_common_publish_options(publish)
    publish.set_defaults(func=publish_local)

    publish_s3_parser = subparsers.add_parser(
        "publish-s3",
        help="Publish the configured S3 source and atomically update the release reference.",
    )
    publish_s3_parser.add_argument(
        "bundle_name",
        help="Name of the configured ManifestS3DagBundle",
    )
    _add_common_publish_options(publish_s3_parser)
    publish_s3_parser.set_defaults(func=publish_s3)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    from airflow.exceptions import AirflowException

    try:
        args.func(args)
    # ValueError/TypeError cover the common misconfigurations: unknown bundle name from
    # DagBundlesManager.get_bundle, bad bundle kwargs, and overlapping publish paths.
    except (OSError, RuntimeError, ValueError, TypeError, AirflowException) as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
