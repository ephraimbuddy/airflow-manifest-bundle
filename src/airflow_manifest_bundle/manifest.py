from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException

MANIFEST_FILE_NAME = ".airflow-bundle-manifest.json"
IGNORED_DIR_NAMES = frozenset({".git", "__pycache__"})
# ".git" as a file name covers git-worktree checkouts, where .git is a pointer file.
IGNORED_FILE_NAMES = frozenset({MANIFEST_FILE_NAME, ".git"})
IGNORED_FILE_SUFFIXES = frozenset({".pyc"})
MANIFEST_SCHEMA_VERSION = 1


class BundleManifestError(AirflowException):
    """
    Base class for bundle manifest errors.

    Subclasses ``AirflowException`` so that stock Airflow's dag processor treats a bundle
    that fails to initialize as a per-bundle refresh error instead of crashing the loop.
    """


class BundleManifestSourceChangedError(BundleManifestError):
    """Raised when bundle source files change while a manifest is being built."""


class BundleManifestNotFoundError(BundleManifestError, FileNotFoundError):
    """
    A required manifest artifact (release reference, snapshot, or snapshot manifest) is missing.

    Dual-inherits ``FileNotFoundError`` so publisher code that distinguishes the missing-ref
    case keeps working, while stock Airflow's ``except AirflowException`` handling still
    treats it as a recoverable per-bundle error from any entry point — including ones
    (like the ``path`` property) that core calls outside the wrapped initialize/refresh flow.
    """


@dataclass(frozen=True)
class BundleSourceFile:
    """A source file and the stat metadata used to detect source changes during publishing."""

    path: Path
    relative_path: str
    size: int
    mtime_ns: int
    ctime_ns: int
    mode: int

    def build_signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "mode": self.mode,
        }


@dataclass(frozen=True)
class BundleSourceSnapshot:
    """A deterministic snapshot of the source tree metadata."""

    root: Path
    files: tuple[BundleSourceFile, ...]
    signature: str


@dataclass(frozen=True)
class BundleVersionManifest:
    """Full manifest plus the compact release-reference payload written to latest.json."""

    version: str
    manifest: dict[str, Any]
    ref_payload: dict[str, Any]
    source_snapshot: BundleSourceSnapshot


def _raise_source_walk_error(error: OSError) -> None:
    raise BundleManifestSourceChangedError(
        f"Bundle source changed or became unreadable while collecting manifest metadata: {error.filename}"
    ) from error


def _iter_manifest_file_paths(root: Path):
    for dirpath, dirnames, filenames in os.walk(
        root,
        followlinks=False,
        onerror=_raise_source_walk_error,
    ):
        retained_dirnames: list[str] = []
        for dirname in sorted(dirnames):
            if dirname in IGNORED_DIR_NAMES:
                continue
            path = Path(dirpath) / dirname
            try:
                file_stat = path.lstat()
            except FileNotFoundError as e:
                raise BundleManifestSourceChangedError(
                    f"Bundle source directory disappeared while collecting manifest metadata: {path}"
                ) from e
            if stat.S_ISLNK(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains symlinked directory: {path}")
            if not stat.S_ISDIR(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains non-directory entry: {path}")
            retained_dirnames.append(dirname)
        dirnames[:] = retained_dirnames

        for filename in sorted(filenames):
            if filename in IGNORED_FILE_NAMES or Path(filename).suffix in IGNORED_FILE_SUFFIXES:
                continue
            path = Path(dirpath) / filename
            try:
                file_stat = path.lstat()
            except FileNotFoundError as e:
                raise BundleManifestSourceChangedError(
                    f"Bundle source file disappeared while collecting manifest metadata: {path}"
                ) from e
            if stat.S_ISLNK(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains symlinked file: {path}")
            if not stat.S_ISREG(file_stat.st_mode):
                raise BundleManifestError(f"Bundle source contains non-regular file: {path}")
            yield path


def _build_source_file(root: Path, path: Path) -> BundleSourceFile:
    try:
        file_stat = path.lstat()
    except FileNotFoundError as e:
        raise BundleManifestSourceChangedError(
            f"Bundle source file disappeared while collecting manifest metadata: {path}"
        ) from e

    if stat.S_ISLNK(file_stat.st_mode):
        raise BundleManifestError(f"Bundle source contains symlinked file: {path}")
    if not stat.S_ISREG(file_stat.st_mode):
        raise BundleManifestError(f"Bundle source contains non-regular file: {path}")

    return BundleSourceFile(
        path=path,
        relative_path=path.relative_to(root).as_posix(),
        size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
        ctime_ns=file_stat.st_ctime_ns,
        mode=stat.S_IMODE(file_stat.st_mode),
    )


def compute_file_sha256(path: Path) -> tuple[str, int]:
    """Compute the sha256 hex digest and byte size of a file."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def compute_bundle_version(files: list[dict[str, Any]]) -> str:
    """
    Compute the content-addressed bundle version from manifest file entries.

    Only the identity fields (path, sha256, size, executable) participate, so the
    version stays stable if file entries ever gain extra metadata.
    """
    payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "files": [
            {
                "path": file_info["path"],
                "sha256": file_info["sha256"],
                "size": file_info["size"],
                "executable": file_info["executable"],
            }
            for file_info in files
        ],
    }
    # "sha256-" (not "sha256:") so the version string is filesystem-safe as-is: stock
    # Airflow builds cache and tracking paths from the raw version string.
    return f"sha256-{hashlib.sha256(_serialize_manifest_payload(payload)).hexdigest()}"


def _serialize_manifest_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def serialize_bundle_version_manifest(manifest: dict[str, Any]) -> bytes:
    """Serialize a bundle manifest deterministically for storage and verification."""
    return _serialize_manifest_payload(manifest)


def _validate_bundle_root(root: Path) -> Path:
    root = Path(root)
    if not root.exists():
        raise FileNotFoundError(f"Bundle source path does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Bundle source path is not a directory: {root}")
    return root


def collect_bundle_source_snapshot(root: Path) -> BundleSourceSnapshot:
    """Collect deterministic source-file metadata without reading file contents."""
    root = _validate_bundle_root(root)
    files = [_build_source_file(root, path) for path in _iter_manifest_file_paths(root)]
    files.sort(key=lambda source_file: source_file.relative_path)
    signature_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "files": [file.build_signature_record() for file in files],
    }
    signature = f"sha256:{hashlib.sha256(_serialize_manifest_payload(signature_payload)).hexdigest()}"
    return BundleSourceSnapshot(root=root, files=tuple(files), signature=signature)


def _ensure_source_file_unchanged(source_file: BundleSourceFile) -> None:
    try:
        current_stat = source_file.path.stat()
    except FileNotFoundError as e:
        raise BundleManifestSourceChangedError(
            f"Bundle source file disappeared while building manifest: {source_file.relative_path}"
        ) from e

    current_metadata = (
        current_stat.st_size,
        current_stat.st_mtime_ns,
        current_stat.st_ctime_ns,
        stat.S_IMODE(current_stat.st_mode),
    )
    expected_metadata = (
        source_file.size,
        source_file.mtime_ns,
        source_file.ctime_ns,
        source_file.mode,
    )
    if current_metadata != expected_metadata:
        raise BundleManifestSourceChangedError(
            f"Bundle source file changed while building manifest: {source_file.relative_path}"
        )


def build_ref_payload(
    *,
    bundle_name: str,
    version: str,
    backend: dict[str, Any],
    manifest_sha256: str,
    file_count: int,
    total_size: int,
) -> dict[str, Any]:
    """
    Build the compact release-reference payload; the one place that defines its schema.

    This is the package's own latest.json format, independent of any Airflow plumbing.
    """
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "version": version,
        "backend": backend,
        "manifest": {
            "path": MANIFEST_FILE_NAME,
            "sha256": manifest_sha256,
        },
        "file_count": file_count,
        "total_size": total_size,
    }


def _build_ref_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    manifest_sha256 = hashlib.sha256(serialize_bundle_version_manifest(manifest)).hexdigest()
    return build_ref_payload(
        bundle_name=manifest["bundle_name"],
        version=manifest["version"],
        backend=manifest["backend"],
        manifest_sha256=f"sha256:{manifest_sha256}",
        file_count=manifest["file_count"],
        total_size=manifest["total_size"],
    )


def build_bundle_version_manifest_result(
    *,
    bundle_name: str,
    root: Path,
    backend_type: str,
    source_snapshot: BundleSourceSnapshot | None = None,
) -> BundleVersionManifest:
    """Build a deterministic content manifest for a materialized Dag bundle root."""
    source_snapshot = source_snapshot or collect_bundle_source_snapshot(root)
    root = _validate_bundle_root(root)
    if source_snapshot.root != root:
        raise ValueError("source_snapshot root does not match manifest root")

    files: list[dict[str, Any]] = []
    total_size = 0
    for source_file in source_snapshot.files:
        try:
            file_digest, _ = compute_file_sha256(source_file.path)
        except FileNotFoundError as e:
            raise BundleManifestSourceChangedError(
                f"Bundle source file disappeared while building manifest: {source_file.relative_path}"
            ) from e
        _ensure_source_file_unchanged(source_file)
        files.append(
            {
                "path": source_file.relative_path,
                "sha256": file_digest,
                "size": source_file.size,
                "executable": bool(source_file.mode & 0o111),
            }
        )
        total_size += source_file.size

    version = compute_bundle_version(files)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "bundle_name": bundle_name,
        "version": version,
        "backend": {"type": backend_type},
        "file_count": len(files),
        "total_size": total_size,
        "files": files,
    }
    return BundleVersionManifest(
        version=version,
        manifest=manifest,
        ref_payload=_build_ref_payload(manifest),
        source_snapshot=source_snapshot,
    )


def verify_bundle_version_manifest(manifest: dict[str, Any], expected_sha256: str) -> None:
    """Verify serialized manifest content against an expected sha256 digest."""
    actual_sha256 = hashlib.sha256(serialize_bundle_version_manifest(manifest)).hexdigest()
    expected_sha256 = expected_sha256.removeprefix("sha256:")
    if actual_sha256 != expected_sha256:
        raise BundleManifestError(
            f"Bundle manifest digest mismatch: expected sha256:{expected_sha256}, got sha256:{actual_sha256}"
        )
