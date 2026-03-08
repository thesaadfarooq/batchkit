# Releasing

`batchkit-ai` uses a two-workflow release flow:

1. `Release` runs quality checks on `main`, then bumps the version in `pyproject.toml`, commits it to `main`, and creates a version tag.
2. `Publish` runs from that tag, publishes to PyPI, and creates the GitHub Release with generated notes.

## Maintainer flow

1. Go to GitHub Actions.
2. Run the `Release` workflow manually.
3. Choose `patch`, `minor`, or `major`.
4. Let the workflow push the version-bump commit and tag.
5. Approve the `pypi` environment in the `Publish` workflow if prompted.
6. After PyPI publish succeeds, GitHub creates the release notes automatically.

## Notes

- PyPI distribution name: `batchkit-ai`
- Python import package: `batchkit`
- Version numbers are sourced from `pyproject.toml`
- GitHub Releases are created only after successful PyPI publishing
