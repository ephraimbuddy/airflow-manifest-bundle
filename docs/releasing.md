# Release process

This project currently distributes releases through GitHub Releases. Each release
contains a wheel and a source distribution that were built from the tagged commit.
The project can add other release channels, such as PyPI, in the future.

GitHub is not a Python package index. Users must install a release with its direct
wheel URL or install from a Git tag. A GitHub release does not reserve the project
name on PyPI.

## Prerequisites

Before a release:

- Install `uv` and the GitHub CLI (`gh`).
- Authenticate the GitHub CLI with an account that has write access to the repository.
- Merge the version change into `main`.
- Wait for all required CI checks on that commit to pass.
- Configure Git to sign tags with the maintainer's signing key.
- Use a signed release tag with the form `v<project-version>`.

The tag and `[project].version` in `pyproject.toml` must match. For example, project
version `0.1.0` uses tag `v0.1.0`.

## Prepare the release

Check out the exact `main` commit that passed CI:

```bash
git switch main
git pull --ff-only
uv run scripts/verify_release.py
```

The verification script:

- reads the project name and version from `pyproject.toml`;
- requires a clean `main` at the same commit as `origin/main`;
- fetches tags and refuses to reuse an existing release tag;
- checks Git tag-signing configuration and GitHub CLI authentication;
- runs Ruff;
- builds into a new `dist/release-<version>` directory;
- checks the wheel and source distribution metadata;
- installs the exact wheel that will become the release asset in a disposable
  environment;
- runs the full test suite and an import smoke test against that wheel; and
- confirms that the branch and commit did not change during verification.

CI also tests the oldest supported Airflow version. Do not release unless the complete
CI matrix has passed. The script refuses to overwrite an existing artifact directory;
preserve that directory or pass a new empty path with `--output-dir`.

## Tag and publish

The verification script prints the exact signed-tag and GitHub release commands for the
version, commit, and artifact paths it checked. The tag command names the verified
commit explicitly. Run the commands in order.

Do not push the tag unless `git tag -v` reports a valid signature. If Git cannot find
the correct signing key, configure `user.signingkey` or create the tag with an explicit
key:

```bash
git tag -u <key-id> -m "Release v<version>" v<version> <verified-commit>
```

The printed `gh release create` command uses `--verify-tag`, which prevents GitHub CLI
from silently creating a tag at the wrong commit.

## Verify the published release

Inspect the release and its assets:

```bash
gh release view v<version> --web
gh release view v<version> --json tagName,url,assets
```

Test the wheel from its version-specific URL in a disposable environment:

```bash
PROJECT_VERSION=<version>
PUBLISHED_RELEASE_CHECK_DIR="$(mktemp -d)"
uv venv "${PUBLISHED_RELEASE_CHECK_DIR}/venv"
uv pip install \
  --python "${PUBLISHED_RELEASE_CHECK_DIR}/venv/bin/python" \
  "apache-airflow==3.3.0" \
  "airflow-manifest-bundle @ https://github.com/ephraimbuddy/airflow-manifest-bundle/releases/download/v${PROJECT_VERSION}/airflow_manifest_bundle-${PROJECT_VERSION}-py3-none-any.whl"

"${PUBLISHED_RELEASE_CHECK_DIR}/venv/bin/python" -c \
  "from airflow_manifest_bundle.local import ManifestLocalDagBundle"
```

Users can use the same direct-reference form with `pip install`. Installing the wheel
is preferable to installing from Git because it uses the exact artifact that the
release process verified.

## Corrections after publication

Do not move a published tag or replace a published wheel. Those changes make an
existing installation URL return content different from the original release.

If a published release contains a defect, fix it on a branch, increment the project
version, and publish a new release.

The GitHub CLI creates a draft, uploads the assets, and then publishes the release. If
`gh release create` fails, first check whether it left a draft:

```bash
gh release view v<version> --json isDraft,assets,url
```

If no release exists, retry `gh release create` with the same verified artifacts. If a
draft exists, inspect its `assets` output and upload only each missing artifact:

```bash
gh release upload v<version> <wheel-path>
gh release upload v<version> <sdist-path>
```

Do not run an upload command for an asset that the draft already contains. After both
assets are present, publish the draft:

```bash
gh release edit v<version> --draft=false
```
