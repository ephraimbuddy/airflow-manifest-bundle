# Copyright 2026 Ephraim Anierobi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import json

import pytest

from airflow_manifest_bundle import cli
from airflow_manifest_bundle.manifest import MANIFEST_FILE_NAME

from _test_utils import conf_vars


def test_publish_local_command(tmp_path, capsys):
    source = tmp_path / "source"
    dag_file = source / "example.py"
    dag_file.parent.mkdir(parents=True)
    dag_file.write_text("print('dag')")
    published_root = tmp_path / "published"
    bundle_storage_path = tmp_path / "bundles"
    config = [
        {
            "name": "manifest-local",
            "classpath": "airflow_manifest_bundle.local.ManifestLocalDagBundle",
            "kwargs": {"published_root": str(published_root)},
        }
    ]

    with conf_vars(
        {
            ("core", "load_examples"): "False",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(bundle_storage_path),
        }
    ):
        cli.main(["publish-local", "manifest-local", str(source), "--output", "json"])

    # DagBundlesManager may log to stdout before the command prints its JSON payload.
    out = capsys.readouterr().out
    published = json.loads(out[out.index("{") :])
    assert published["bundle_name"] == "manifest-local"
    assert published["version"].startswith("sha256-")
    manifest_ref_path = published_root / "refs/manifest-local/latest.json"
    assert published["manifest_ref_path"] == str(manifest_ref_path)
    assert published["version_path"] == str(
        published_root / "versions/manifest-local" / published["version"]
    )
    assert published["file_count"] == 1
    assert published["total_size"] == len("print('dag')")
    assert published["created_snapshot"] is True

    snapshot_path = published_root / "versions/manifest-local" / published["version"]
    assert (snapshot_path / "example.py").read_text() == "print('dag')"
    assert not (bundle_storage_path / "manifest-local/versions" / published["version"]).exists()
    manifest_ref = json.loads(manifest_ref_path.read_text())
    assert manifest_ref["version"] == published["version"]
    assert manifest_ref["file_count"] == 1
    assert manifest_ref["total_size"] == len("print('dag')")
    assert json.loads((snapshot_path / MANIFEST_FILE_NAME).read_text())["version"] == published["version"]


def test_publish_local_command_parses_expected_current_version():
    expected_version = f"sha256-{'a' * 64}"

    args = cli.build_parser().parse_args(
        [
            "publish-local",
            "manifest-local",
            "/tmp/source",
            "--expected-current-version",
            expected_version,
        ]
    )

    assert args.expected_current_version == expected_version


def test_publish_local_command_rejects_non_manifest_bundle(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    config = [
        {
            "name": "local",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(source)},
        }
    ]

    with conf_vars(
        {
            ("core", "load_examples"): "False",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
        }
    ):
        with pytest.raises(SystemExit, match="not configured as a ManifestLocalDagBundle"):
            cli.main(["publish-local", "local", str(source)])


def test_publish_local_command_reports_stale_expected_version(tmp_path, capsys):
    source = tmp_path / "source"
    (source / "example.py").parent.mkdir(parents=True)
    (source / "example.py").write_text("print('dag')")
    published_root = tmp_path / "published"
    config = [
        {
            "name": "manifest-local",
            "classpath": "airflow_manifest_bundle.local.ManifestLocalDagBundle",
            "kwargs": {"published_root": str(published_root)},
        }
    ]

    with conf_vars(
        {
            ("core", "load_examples"): "False",
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(tmp_path / "bundles"),
        }
    ):
        cli.main(["publish-local", "manifest-local", str(source)])
        capsys.readouterr()
        with pytest.raises(SystemExit) as excinfo:
            cli.main(
                [
                    "publish-local",
                    "manifest-local",
                    str(source),
                    "--expected-current-version",
                    f"sha256-{'0' * 64}",
                ]
            )

    assert excinfo.value.code == 2
    assert "manifest reference changed" in capsys.readouterr().err
