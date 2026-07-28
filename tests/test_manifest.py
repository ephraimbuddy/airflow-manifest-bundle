from __future__ import annotations

import hashlib
import json
import stat

import pytest

from airflow_manifest_bundle import manifest as manifest_module
from airflow_manifest_bundle.manifest import (
    MANIFEST_FILE_NAME,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest_result,
    collect_bundle_source_snapshot,
)


def build_bundle_version_manifest(**kwargs):
    return build_bundle_version_manifest_result(**kwargs).manifest


def _write_file(root, relative_path, content):
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_manifest_hash_is_stable_across_filesystem_creation_order(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"

    _write_file(first, "dags/a.py", "print('a')")
    _write_file(first, "dags/nested/b.py", "print('b')")
    _write_file(second, "dags/nested/b.py", "print('b')")
    _write_file(second, "dags/a.py", "print('a')")

    first_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=first, backend_type="local"
    )
    second_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=second, backend_type="local"
    )

    assert first_manifest["version"] == second_manifest["version"]
    assert first_manifest["files"] == second_manifest["files"]


def test_manifest_hash_changes_when_file_content_changes(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('first')")
    first_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    _write_file(source, "dags/example.py", "print('second')")
    second_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert first_manifest["version"] != second_manifest["version"]


def test_manifest_uses_complete_precomputed_file_hashes(tmp_path, monkeypatch):
    source = tmp_path / "source"
    content = "print('dag')"
    _write_file(source, "dags/example.py", content)
    source_snapshot = collect_bundle_source_snapshot(source)
    expected_digest = hashlib.sha256(content.encode()).hexdigest()

    def fail_hash(_):
        raise AssertionError("precomputed hashes must skip a second source read")

    monkeypatch.setattr(manifest_module, "compute_file_sha256", fail_hash)
    result = build_bundle_version_manifest_result(
        bundle_name="manifest-s3",
        root=source,
        backend_type="local",
        source_snapshot=source_snapshot,
        precomputed_file_sha256={"dags/example.py": expected_digest},
    )

    assert result.manifest["files"][0]["sha256"] == expected_digest


@pytest.mark.parametrize(
    "precomputed_hashes",
    [
        {},
        {"other.py": "0" * 64},
        {"dags/example.py": "not-a-sha256"},
    ],
)
def test_manifest_rejects_invalid_precomputed_file_hashes(tmp_path, precomputed_hashes):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")

    with pytest.raises(BundleManifestError, match="Precomputed file hash"):
        build_bundle_version_manifest_result(
            bundle_name="manifest-s3",
            root=source,
            backend_type="local",
            precomputed_file_sha256=precomputed_hashes,
        )


def test_manifest_hash_changes_when_executable_bit_changes(tmp_path):
    source = tmp_path / "source"
    script = _write_file(source, "scripts/run.sh", "#!/bin/sh\n")
    first_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    script.chmod(stat.S_IMODE(script.stat().st_mode) | stat.S_IXUSR)
    second_manifest = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert first_manifest["version"] != second_manifest["version"]
    assert first_manifest["files"][0]["executable"] is False
    assert second_manifest["files"][0]["executable"] is True


def test_manifest_rejects_source_symlink(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "outside.py"
    target.write_text("print('outside')")
    try:
        (source / "linked.py").symlink_to(target)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")

    with pytest.raises(BundleManifestError, match="symlinked file"):
        build_bundle_version_manifest(bundle_name="manifest-local", root=source, backend_type="local")


def test_manifest_rejects_source_walk_error(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()

    def fail_walk(*args, onerror, **kwargs):
        onerror(PermissionError(13, "Permission denied", str(source / "unreadable")))

    monkeypatch.setattr(manifest_module.os, "walk", fail_walk)

    with pytest.raises(BundleManifestSourceChangedError, match="changed or became unreadable"):
        build_bundle_version_manifest(bundle_name="manifest-local", root=source, backend_type="local")


def test_manifest_ignores_cache_and_vcs_files(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    _write_file(source, ".git/config", "[core]")
    _write_file(source, MANIFEST_FILE_NAME, "{}")
    _write_file(source, "dags/__pycache__/example.cpython-312.pyc", "compiled")
    manifest_after = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert manifest_after["version"] == manifest_before["version"]
    assert [file_info["path"] for file_info in manifest_after["files"]] == ["dags/example.py"]


def test_manifest_ignores_symlinked_ignored_directory(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    real_git_dir = tmp_path / "real-git"
    real_git_dir.mkdir()
    try:
        (source / ".git").symlink_to(real_git_dir, target_is_directory=True)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")
    manifest_after = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert manifest_after["version"] == manifest_before["version"]


def test_manifest_ignores_git_worktree_pointer_file(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    _write_file(source, ".git", "gitdir: /repos/main/.git/worktrees/dags")
    manifest_after = build_bundle_version_manifest(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert manifest_after["version"] == manifest_before["version"]
    assert [file_info["path"] for file_info in manifest_after["files"]] == ["dags/example.py"]


def test_manifest_paths_are_relative_and_sorted(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "z.py", "print('z')")
    _write_file(source, "a/nested.py", "print('nested')")
    _write_file(source, "a.py", "print('a')")

    manifest = build_bundle_version_manifest(bundle_name="manifest-local", root=source, backend_type="local")
    paths = [file_info["path"] for file_info in manifest["files"]]

    assert paths == ["a.py", "a/nested.py", "z.py"]
    assert str(source) not in json.dumps(manifest)


def test_ref_payload_is_compact_and_points_to_manifest(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")

    result = build_bundle_version_manifest_result(
        bundle_name="manifest-local", root=source, backend_type="local"
    )

    assert result.ref_payload == {
        "schema_version": 1,
        "bundle_name": "manifest-local",
        "version": result.version,
        "backend": {"type": "local"},
        "manifest": {
            "path": MANIFEST_FILE_NAME,
            "sha256": result.ref_payload["manifest"]["sha256"],
        },
        "file_count": 1,
        "total_size": len("print('dag')"),
    }
    assert result.ref_payload["manifest"]["sha256"].startswith("sha256:")
    assert "files" not in result.ref_payload
    assert str(source) not in json.dumps(result.ref_payload)
