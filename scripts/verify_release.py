#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# ///
"""Verify and build a release without creating a tag or publishing it."""

from __future__ import annotations

import argparse
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AIRFLOW_VERSION = "3.3.0"
RELEASE_BRANCH = "main"
RUFF_VERSION = "0.16.0"


class ReleaseVerificationError(RuntimeError):
    """A release prerequisite or verification step failed."""


@dataclass(frozen=True)
class ProjectMetadata:
    """Project identity read from pyproject.toml."""

    name: str
    version: str


@dataclass(frozen=True)
class DistributionArtifacts:
    """The wheel and source distribution produced by the release build."""

    wheel: Path
    sdist: Path


def _command_text(command: Sequence[str | Path]) -> str:
    return shlex.join(str(part) for part in command)


def run_command(
    command: Sequence[str | Path],
    *,
    capture_output: bool = False,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one command from the repository root and report it first."""
    argv = [str(part) for part in command]
    print(f"+ {_command_text(command)}", flush=True)
    result = subprocess.run(
        argv,
        cwd=REPOSITORY_ROOT,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if result.returncode and not allow_failure:
        detail = ""
        if capture_output:
            detail = (result.stderr or result.stdout).strip()
        message = f"Command failed with exit code {result.returncode}: {_command_text(command)}"
        if detail:
            message = f"{message}\n{detail}"
        raise ReleaseVerificationError(message)
    return result


def capture_command(command: Sequence[str | Path]) -> str:
    """Run a command and return its stripped standard output."""
    return run_command(command, capture_output=True).stdout.strip()


def load_project_metadata(pyproject_path: Path) -> ProjectMetadata:
    """Read the static project name and version from pyproject.toml."""
    with pyproject_path.open("rb") as file:
        payload = tomllib.load(file)
    project = payload.get("project")
    if not isinstance(project, dict):
        raise ReleaseVerificationError(f"{pyproject_path} does not contain a [project] table")
    name = project.get("name")
    version = project.get("version")
    if not isinstance(name, str) or not name:
        raise ReleaseVerificationError(f"{pyproject_path} does not contain a static project name")
    if not isinstance(version, str) or not version:
        raise ReleaseVerificationError(f"{pyproject_path} does not contain a static project version")
    return ProjectMetadata(name=name, version=version)


def require_tools() -> None:
    """Require every external command used by verification and publication."""
    missing = [command for command in ("git", "gh", "uv", "uvx") if shutil.which(command) is None]
    if missing:
        raise ReleaseVerificationError(f"Required commands are missing: {', '.join(missing)}")


def _require_clean_tree() -> None:
    status = capture_command(["git", "status", "--porcelain=v1", "--untracked-files=all"])
    if status:
        raise ReleaseVerificationError(
            "The working tree is not clean. Commit or remove these changes before release:\n"
            f"{status}"
        )


def verify_git_state(tag: str) -> str:
    """Verify the release commit, tag availability, authentication, and signing setup."""
    _require_clean_tree()
    branch = capture_command(["git", "branch", "--show-current"])
    if branch != RELEASE_BRANCH:
        raise ReleaseVerificationError(
            f"Release verification must run on {RELEASE_BRANCH!r}, not {branch!r}"
        )

    run_command(["git", "fetch", "--quiet", "--tags", "origin"])
    head = capture_command(["git", "rev-parse", "HEAD"])
    origin_head = capture_command(["git", "rev-parse", f"origin/{RELEASE_BRANCH}"])
    if head != origin_head:
        raise ReleaseVerificationError(
            f"HEAD {head} does not match origin/{RELEASE_BRANCH} {origin_head}. "
            "Update the local branch before release."
        )

    tag_lookup = run_command(
        ["git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}"],
        capture_output=True,
        allow_failure=True,
    )
    if tag_lookup.returncode == 0:
        raise ReleaseVerificationError(f"Release tag {tag!r} already exists")
    if tag_lookup.returncode != 1:
        raise ReleaseVerificationError(f"Could not determine whether release tag {tag!r} exists")

    signing_key = run_command(
        ["git", "config", "--get", "user.signingkey"],
        capture_output=True,
        allow_failure=True,
    )
    if signing_key.returncode != 0 or not signing_key.stdout.strip():
        raise ReleaseVerificationError(
            "Git user.signingkey is not configured. Release tags must be signed."
        )

    run_command(["gh", "auth", "status"])
    _require_clean_tree()
    return head


def verify_release_commit(commit: str) -> None:
    """Require the repository to remain on the verified release commit."""
    _require_clean_tree()
    branch = capture_command(["git", "branch", "--show-current"])
    if branch != RELEASE_BRANCH:
        raise ReleaseVerificationError(
            f"Release verification started on {RELEASE_BRANCH!r}, but the current branch is "
            f"{branch!r}"
        )
    head = capture_command(["git", "rev-parse", "HEAD"])
    if head != commit:
        raise ReleaseVerificationError(
            f"HEAD changed during release verification: expected {commit}, found {head}"
        )


def create_artifact_directory(requested: Path | None, version: str) -> Path:
    """Create a new output directory without overwriting an earlier build."""
    output_dir = requested or REPOSITORY_ROOT / "dist" / f"release-{version}"
    if not output_dir.is_absolute():
        output_dir = REPOSITORY_ROOT / output_dir
    output_dir = output_dir.resolve()
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as e:
        raise ReleaseVerificationError(
            f"Release artifact directory already exists: {output_dir}. "
            "Preserve it or choose a new empty path with --output-dir."
        ) from e
    return output_dir


def find_distribution_artifacts(output_dir: Path) -> DistributionArtifacts:
    """Require one wheel, one source distribution, and no unexpected build output."""
    entries = sorted(path for path in output_dir.iterdir() if path.name != ".gitignore")
    wheels = [path for path in entries if path.is_file() and path.suffix == ".whl"]
    sdists = [path for path in entries if path.is_file() and path.name.endswith(".tar.gz")]
    if len(entries) != 2 or len(wheels) != 1 or len(sdists) != 1:
        names = ", ".join(path.name for path in entries) or "<empty>"
        raise ReleaseVerificationError(
            "Release build must produce exactly one wheel and one .tar.gz source distribution; "
            f"found: {names}"
        )
    if not wheels[0].name.endswith("-py3-none-any.whl"):
        raise ReleaseVerificationError(f"Release wheel is not platform-independent: {wheels[0].name}")
    return DistributionArtifacts(wheel=wheels[0], sdist=sdists[0])


def _canonical_project_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _validate_metadata_payload(
    payload: bytes,
    *,
    source: str,
    expected_project: ProjectMetadata,
) -> None:
    metadata = BytesParser().parsebytes(payload)
    actual_name = metadata.get("Name")
    actual_version = metadata.get("Version")
    if not actual_name or _canonical_project_name(actual_name) != _canonical_project_name(
        expected_project.name
    ):
        raise ReleaseVerificationError(
            f"{source} contains project name {actual_name!r}, expected {expected_project.name!r}"
        )
    if actual_version != expected_project.version:
        raise ReleaseVerificationError(
            f"{source} contains version {actual_version!r}, expected {expected_project.version!r}"
        )


def validate_distribution_metadata(
    artifacts: DistributionArtifacts,
    project: ProjectMetadata,
) -> None:
    """Confirm that both built distributions carry the intended name and version."""
    with zipfile.ZipFile(artifacts.wheel) as archive:
        metadata_names = [
            name
            for name in archive.namelist()
            if name.count("/") == 1 and name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise ReleaseVerificationError(
                f"{artifacts.wheel} must contain exactly one .dist-info/METADATA file"
            )
        _validate_metadata_payload(
            archive.read(metadata_names[0]),
            source=str(artifacts.wheel),
            expected_project=project,
        )

    with tarfile.open(artifacts.sdist, mode="r:gz") as archive:
        metadata_members = [
            member
            for member in archive.getmembers()
            if member.isfile() and member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        ]
        if len(metadata_members) != 1:
            raise ReleaseVerificationError(
                f"{artifacts.sdist} must contain exactly one top-level PKG-INFO file"
            )
        metadata_file = archive.extractfile(metadata_members[0])
        if metadata_file is None:
            raise ReleaseVerificationError(f"Could not read PKG-INFO from {artifacts.sdist}")
        _validate_metadata_payload(
            metadata_file.read(),
            source=str(artifacts.sdist),
            expected_project=project,
        )


def run_lint() -> None:
    """Run static checks against every Python source directory."""
    run_command(["uvx", f"ruff=={RUFF_VERSION}", "check", "src", "tests", "scripts"])


def build_and_validate(
    *,
    project: ProjectMetadata,
    output_dir: Path,
    python: str,
    airflow_version: str,
) -> DistributionArtifacts:
    """Build, inspect, install, and exercise the exact release wheel."""
    run_command(["uv", "build", "--out-dir", output_dir])
    artifacts = find_distribution_artifacts(output_dir)
    run_command(["uvx", "twine", "check", artifacts.wheel, artifacts.sdist])
    validate_distribution_metadata(artifacts, project)

    with tempfile.TemporaryDirectory(prefix="airflow-manifest-bundle-release-wheel-") as temp_dir:
        venv_dir = Path(temp_dir) / "venv"
        venv_python = venv_dir / "bin" / "python"
        console_script = venv_dir / "bin" / "airflow-manifest-bundle"
        run_command(["uv", "venv", "--python", python, venv_dir])
        run_command(
            [
                "uv",
                "pip",
                "install",
                "--python",
                venv_python,
                f"apache-airflow=={airflow_version}",
                artifacts.wheel,
                "pytest",
            ]
        )
        run_command([venv_python, "-m", "pytest", "-q"])
        run_command(
            [
                venv_python,
                "-c",
                (
                    "from airflow_manifest_bundle import ManifestDagBundleBase; "
                    "from airflow_manifest_bundle.gcs import ManifestGCSDagBundle; "
                    "from airflow_manifest_bundle.local import ManifestLocalDagBundle; "
                    "from airflow_manifest_bundle.s3 import ManifestS3DagBundle"
                ),
            ]
        )
        run_command([console_script, "publish-local", "--help"])
        run_command([console_script, "publish-s3", "--help"])
        run_command([console_script, "publish-gcs", "--help"])
    return artifacts


def print_release_commands(
    *,
    project: ProjectMetadata,
    tag: str,
    commit: str,
    artifacts: DistributionArtifacts,
) -> None:
    """Print the signed-tag and GitHub publication commands for the verified artifacts."""
    print("\nRelease verification passed.")
    print(f"Project: {project.name} {project.version}")
    print(f"Commit:  {commit}")
    print(f"Wheel:   {artifacts.wheel}")
    print(f"Source:  {artifacts.sdist}")
    print("\nRun these commands to publish the verified artifacts:")
    commands = [
        ["git", "tag", "-s", "-m", f"Release {tag}", tag, commit],
        ["git", "tag", "-v", tag],
        ["git", "push", "origin", tag],
        [
            "gh",
            "release",
            "create",
            tag,
            artifacts.wheel,
            artifacts.sdist,
            "--verify-tag",
            "--generate-notes",
            "--title",
            tag,
        ],
    ]
    for command in commands:
        print(_command_text(command))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify main and build the exact artifacts for a GitHub release."
    )
    parser.add_argument(
        "--airflow-version",
        default=DEFAULT_AIRFLOW_VERSION,
        help=f"Airflow version for local tests and wheel smoke tests (default: {DEFAULT_AIRFLOW_VERSION})",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter used for disposable test environments",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="New directory for release artifacts (default: dist/release-<version>)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        require_tools()
        project = load_project_metadata(REPOSITORY_ROOT / "pyproject.toml")
        tag = f"v{project.version}"
        commit = verify_git_state(tag)
        run_lint()
        output_dir = create_artifact_directory(args.output_dir, project.version)
        artifacts = build_and_validate(
            project=project,
            output_dir=output_dir,
            python=args.python,
            airflow_version=args.airflow_version,
        )
        verify_release_commit(commit)
    except (OSError, ReleaseVerificationError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print_release_commands(
        project=project,
        tag=tag,
        commit=commit,
        artifacts=artifacts,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
