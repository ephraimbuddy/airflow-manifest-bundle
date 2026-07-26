from __future__ import annotations

import importlib.util
import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

if sys.version_info < (3, 11):
    pytest.skip(
        "the release verifier requires Python 3.11 or newer",
        allow_module_level=True,
    )


def _load_verify_release_module():
    script_path = Path(__file__).parents[1] / "scripts" / "verify_release.py"
    spec = importlib.util.spec_from_file_location("verify_release", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


verify_release = _load_verify_release_module()


def test_load_project_metadata_reads_static_name_and_version(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "example-project"
version = "1.2.3"
""".lstrip()
    )

    metadata = verify_release.load_project_metadata(pyproject)

    assert metadata == verify_release.ProjectMetadata(name="example-project", version="1.2.3")


def test_create_artifact_directory_refuses_to_reuse_existing_directory(tmp_path):
    output_dir = tmp_path / "release"
    output_dir.mkdir()

    with pytest.raises(verify_release.ReleaseVerificationError, match="already exists"):
        verify_release.create_artifact_directory(output_dir, "1.2.3")


def test_verify_git_state_requires_main(monkeypatch):
    monkeypatch.setattr(verify_release, "_require_clean_tree", lambda: None)
    monkeypatch.setattr(verify_release, "capture_command", lambda command: "feature-branch")

    with pytest.raises(verify_release.ReleaseVerificationError, match="'main'"):
        verify_release.verify_git_state("v1.2.3")


def test_verify_git_state_requires_signing_key(monkeypatch):
    monkeypatch.setattr(verify_release, "_require_clean_tree", lambda: None)

    def capture(command):
        if command == ["git", "branch", "--show-current"]:
            return "main"
        if command in (["git", "rev-parse", "HEAD"], ["git", "rev-parse", "origin/main"]):
            return "abc123"
        raise AssertionError(f"Unexpected captured command: {command}")

    def run(command, **kwargs):
        if command[:2] == ["git", "fetch"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:3] == ["git", "show-ref", "--verify"]:
            return subprocess.CompletedProcess(command, 1, "", "")
        if command == ["git", "config", "--get", "user.signingkey"]:
            return subprocess.CompletedProcess(command, 1, "", "")
        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(verify_release, "capture_command", capture)
    monkeypatch.setattr(verify_release, "run_command", run)

    with pytest.raises(verify_release.ReleaseVerificationError, match="user.signingkey"):
        verify_release.verify_git_state("v1.2.3")


def test_verify_release_commit_rejects_changed_head(monkeypatch):
    monkeypatch.setattr(verify_release, "_require_clean_tree", lambda: None)

    def capture(command):
        if command == ["git", "branch", "--show-current"]:
            return "main"
        if command == ["git", "rev-parse", "HEAD"]:
            return "changed456"
        raise AssertionError(f"Unexpected captured command: {command}")

    monkeypatch.setattr(verify_release, "capture_command", capture)

    with pytest.raises(verify_release.ReleaseVerificationError, match="HEAD changed"):
        verify_release.verify_release_commit("verified123")


def test_find_distribution_artifacts_requires_exact_pair(tmp_path):
    (tmp_path / ".gitignore").write_text("*")
    (tmp_path / "example-1.2.3-py3-none-any.whl").touch()
    (tmp_path / "example-1.2.3.tar.gz").touch()

    artifacts = verify_release.find_distribution_artifacts(tmp_path)

    assert artifacts.wheel.name == "example-1.2.3-py3-none-any.whl"
    assert artifacts.sdist.name == "example-1.2.3.tar.gz"

    (tmp_path / "unexpected.txt").touch()
    with pytest.raises(verify_release.ReleaseVerificationError, match="exactly one wheel"):
        verify_release.find_distribution_artifacts(tmp_path)


def test_validate_distribution_metadata_checks_wheel_and_sdist(tmp_path):
    wheel = tmp_path / "example_project-1.2.3-py3-none-any.whl"
    sdist = tmp_path / "example_project-1.2.3.tar.gz"
    metadata = b"Metadata-Version: 2.4\nName: example-project\nVersion: 1.2.3\n"

    with zipfile.ZipFile(wheel, mode="w") as archive:
        archive.writestr("example_project-1.2.3.dist-info/METADATA", metadata)

    with tarfile.open(sdist, mode="w:gz") as archive:
        info = tarfile.TarInfo("example_project-1.2.3/PKG-INFO")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))

    verify_release.validate_distribution_metadata(
        verify_release.DistributionArtifacts(wheel=wheel, sdist=sdist),
        verify_release.ProjectMetadata(name="example-project", version="1.2.3"),
    )


def test_validate_distribution_metadata_rejects_wrong_version(tmp_path):
    wheel = tmp_path / "example_project-1.2.3-py3-none-any.whl"
    sdist = tmp_path / "example_project-1.2.3.tar.gz"
    wrong_metadata = b"Metadata-Version: 2.4\nName: example-project\nVersion: 9.9.9\n"

    with zipfile.ZipFile(wheel, mode="w") as archive:
        archive.writestr("example_project-1.2.3.dist-info/METADATA", wrong_metadata)

    with tarfile.open(sdist, mode="w:gz") as archive:
        info = tarfile.TarInfo("example_project-1.2.3/PKG-INFO")
        info.size = len(wrong_metadata)
        archive.addfile(info, io.BytesIO(wrong_metadata))

    with pytest.raises(verify_release.ReleaseVerificationError, match="version '9.9.9'"):
        verify_release.validate_distribution_metadata(
            verify_release.DistributionArtifacts(wheel=wheel, sdist=sdist),
            verify_release.ProjectMetadata(name="example-project", version="1.2.3"),
        )


def test_run_lint_uses_pinned_ruff(monkeypatch):
    commands = []
    monkeypatch.setattr(verify_release, "run_command", lambda command: commands.append(command))

    verify_release.run_lint()

    assert commands == [["uvx", "ruff==0.16.0", "check", "src", "tests", "scripts"]]


def test_build_and_validate_runs_full_suite_against_wheel(tmp_path, monkeypatch):
    wheel = tmp_path / "example_project-1.2.3-py3-none-any.whl"
    sdist = tmp_path / "example_project-1.2.3.tar.gz"
    artifacts = verify_release.DistributionArtifacts(wheel=wheel, sdist=sdist)
    commands = []

    monkeypatch.setattr(verify_release, "run_command", lambda command, **kwargs: commands.append(command))
    monkeypatch.setattr(verify_release, "find_distribution_artifacts", lambda output_dir: artifacts)
    monkeypatch.setattr(verify_release, "validate_distribution_metadata", lambda result, project: None)

    verify_release.build_and_validate(
        project=verify_release.ProjectMetadata(name="example-project", version="1.2.3"),
        output_dir=tmp_path,
        python="python3.12",
        airflow_version="3.3.0",
    )

    install_command = next(command for command in commands if command[:3] == ["uv", "pip", "install"])
    assert wheel in install_command
    assert "pytest" in install_command
    assert "--editable" not in install_command
    assert any(command[-3:] == ["-m", "pytest", "-q"] for command in commands)


def test_print_release_commands_binds_tag_to_verified_commit(tmp_path, capsys):
    verify_release.print_release_commands(
        project=verify_release.ProjectMetadata(name="example-project", version="1.2.3"),
        tag="v1.2.3",
        commit="verified123",
        artifacts=verify_release.DistributionArtifacts(
            wheel=tmp_path / "example_project-1.2.3-py3-none-any.whl",
            sdist=tmp_path / "example_project-1.2.3.tar.gz",
        ),
    )

    output = capsys.readouterr().out
    assert "git tag -s -m 'Release v1.2.3' v1.2.3 verified123" in output
