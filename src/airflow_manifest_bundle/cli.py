# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Standalone publisher CLI.

External packages cannot add ``airflow`` subcommands, so publishing ships as its own
console script:

    airflow-manifest-bundle publish-local <bundle-name> <source-path>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _get_manifest_local_bundle(bundle_name: str, *, action: str):
    from airflow.dag_processing.bundles.manager import DagBundlesManager

    from airflow_manifest_bundle.local import ManifestLocalDagBundle

    bundle = DagBundlesManager().get_bundle(bundle_name)
    if not isinstance(bundle, ManifestLocalDagBundle):
        raise SystemExit(
            f"Bundle {bundle_name!r} is not configured as a ManifestLocalDagBundle. "
            f"Only manifest-backed local bundles can be {action} with this command."
        )
    return bundle


def publish_local(args: argparse.Namespace) -> None:
    """Publish an immutable local snapshot for a manifest-backed local Dag bundle."""
    from airflow_manifest_bundle.local import publish_manifest_local_dag_bundle

    bundle = _get_manifest_local_bundle(args.bundle_name, action="published")
    result = publish_manifest_local_dag_bundle(
        bundle=bundle,
        source_path=Path(args.source_path),
        expected_current_version=args.expected_current_version,
    )
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
    if args.output == "json":
        print(json.dumps(payload, indent=2))
    else:
        for key, value in payload.items():
            print(f"{key}: {value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airflow-manifest-bundle",
        description="Manage manifest-backed local Dag bundles.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser(
        "publish-local",
        help="Publish an immutable local snapshot and atomically update the release reference.",
    )
    publish.add_argument("bundle_name", help="Name of the configured ManifestLocalDagBundle")
    publish.add_argument("source_path", help="Local directory containing the Dag files to publish")
    publish.add_argument(
        "--expected-current-version",
        default=None,
        help=(
            "Refuse to publish unless the currently released version matches. "
            "Protects out-of-order deploys from clobbering a newer release."
        ),
    )
    publish.add_argument("--output", choices=("table", "json"), default="table")
    publish.set_defaults(func=publish_local)
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
