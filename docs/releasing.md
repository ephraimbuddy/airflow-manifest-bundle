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
- Use an annotated or signed tag. Release tags use the form `v<project-version>`.

The tag and `[project].version` in `pyproject.toml` must match. For example, project
version `0.1.0` uses tag `v0.1.0`.

## Prepare the release

Check out the exact `main` commit that passed CI:

```bash
git switch main
git pull --ff-only
git status --short
```

The status output must be empty. Confirm the project version in `pyproject.toml`, then
run the release checks:

```bash
uvx ruff check src tests
uv venv
uv pip install "apache-airflow==3.3.0" pytest -e .
.venv/bin/python -m pytest -q
```

CI also tests the oldest supported Airflow version. Do not release unless the complete
CI matrix has passed.

Build into a new temporary directory so that files from an older build cannot become
release assets:

```bash
PROJECT_VERSION="$(
  .venv/bin/python -c \
    'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])'
)"
RELEASE_TAG="v${PROJECT_VERSION}"
RELEASE_ARTIFACT_DIR="$(mktemp -d)"
WHEEL_PATH="${RELEASE_ARTIFACT_DIR}/airflow_manifest_bundle-${PROJECT_VERSION}-py3-none-any.whl"
SDIST_PATH="${RELEASE_ARTIFACT_DIR}/airflow_manifest_bundle-${PROJECT_VERSION}.tar.gz"

uv build --out-dir "${RELEASE_ARTIFACT_DIR}"
uvx twine check "${WHEEL_PATH}" "${SDIST_PATH}"
ls -l "${RELEASE_ARTIFACT_DIR}"
```

The directory must contain exactly these two distributions, with the selected version:

```text
airflow_manifest_bundle-<version>-py3-none-any.whl
airflow_manifest_bundle-<version>.tar.gz
```

Install and exercise the exact wheel that will become the release asset:

```bash
LOCAL_RELEASE_CHECK_DIR="$(mktemp -d)"
uv venv "${LOCAL_RELEASE_CHECK_DIR}/venv"
uv pip install \
  --python "${LOCAL_RELEASE_CHECK_DIR}/venv/bin/python" \
  "apache-airflow==3.3.0" \
  "${WHEEL_PATH}"

"${LOCAL_RELEASE_CHECK_DIR}/venv/bin/airflow-manifest-bundle" --help
```

This check must happen before publication. `twine check` validates distribution
metadata, but it does not prove that the wheel installs or that its console script
starts.

## Tag and publish

Confirm that the checks and build did not change tracked files:

```bash
git diff --exit-code
git status --short
```

The status output must still be empty. Then create the tag:

```bash
git tag -a "${RELEASE_TAG}" -m "Release ${RELEASE_TAG}"
git push origin "${RELEASE_TAG}"
```

If Git tag signing is configured, use `git tag -s` instead of `git tag -a`.

Create the GitHub release and attach only the two verified distributions:

```bash
gh release create "${RELEASE_TAG}" \
  "${WHEEL_PATH}" \
  "${SDIST_PATH}" \
  --verify-tag \
  --generate-notes \
  --title "${RELEASE_TAG}"
```

`--verify-tag` prevents the release command from silently creating a tag at the wrong
commit.

## Verify the published release

Inspect the release and its assets:

```bash
gh release view "${RELEASE_TAG}" --web
gh release view "${RELEASE_TAG}" --json tagName,url,assets
```

Test the wheel from its version-specific URL in a disposable environment:

```bash
PUBLISHED_RELEASE_CHECK_DIR="$(mktemp -d)"
uv venv "${PUBLISHED_RELEASE_CHECK_DIR}/venv"
uv pip install \
  --python "${PUBLISHED_RELEASE_CHECK_DIR}/venv/bin/python" \
  "airflow-manifest-bundle @ https://github.com/ephraimbuddy/airflow-manifest-bundle/releases/download/v${PROJECT_VERSION}/airflow_manifest_bundle-${PROJECT_VERSION}-py3-none-any.whl"

"${PUBLISHED_RELEASE_CHECK_DIR}/venv/bin/airflow-manifest-bundle" --help
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
gh release view "${RELEASE_TAG}" --json isDraft,assets,url
```

If no release exists, retry `gh release create` with the same verified artifacts. If a
draft exists, inspect its `assets` output and upload only each missing artifact:

```bash
gh release upload "${RELEASE_TAG}" "${WHEEL_PATH}"
gh release upload "${RELEASE_TAG}" "${SDIST_PATH}"
```

Do not run an upload command for an asset that the draft already contains. After both
assets are present, publish the draft:

```bash
gh release edit "${RELEASE_TAG}" --draft=false
```
