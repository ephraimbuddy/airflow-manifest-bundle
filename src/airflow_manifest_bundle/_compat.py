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
"""
Self-contained filesystem helpers.

``airflow.dag_processing.bundles.base`` does not provide these; they live here so the
package depends only on stable Airflow APIs. If equivalents ever ship in Airflow core,
this module can re-export them instead.
"""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path
from typing import Any

try:
    from airflow.dag_processing.bundles.base import BundleVersion
except ImportError:  # Airflow 3.1/3.2: get_current_version returns a plain string
    BundleVersion = None  # type: ignore[assignment,misc]


def make_bundle_version(version: str) -> Any:
    """
    Return ``version`` in the form the installed Airflow expects.

    Airflow 3.3+ expects a ``BundleVersion`` (a bare string from a versioned bundle
    triggers a deprecation warning); 3.1/3.2 only know plain strings. The ``data``
    field stays None — the content-hash version string is the whole contract.
    """
    if BundleVersion is not None:
        return BundleVersion(version=version, data=None)
    return version


def _make_tree_writable(path: Path) -> None:
    directories: list[Path] = []
    for dirpath, _, filenames in os.walk(path):
        directory = Path(dirpath)
        directories.append(directory)
        for filename in filenames:
            child = directory / filename
            if child.is_symlink():
                continue
            try:
                child.chmod(stat.S_IMODE(child.stat().st_mode) | 0o200)
            except OSError:
                continue

    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        try:
            directory.chmod(stat.S_IMODE(directory.stat().st_mode) | 0o700)
        except OSError:
            continue


def remove_bundle_tree_forcefully(path: Path) -> None:
    """Remove a bundle tree, adding write permissions first when a read-only snapshot blocks removal."""
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
        return
    try:
        shutil.rmtree(path)
    except OSError:
        _make_tree_writable(path)
        shutil.rmtree(path)
