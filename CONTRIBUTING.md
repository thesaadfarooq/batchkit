# Contributing

## Workflow

- Open an issue for features, changes, and design discussions.
- Keep pull requests focused and reviewable.
- Add or update tests before implementation changes.
- Keep the public API simple and documented.

## Local setup

```bash
uv venv --python 3.11
uv pip install -e ".[dev]"
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy src
```

## Review expectations

- new features should have tests
- behavior changes should update docs
- PRs should explain user-visible impact

## Releases

- PyPI distribution name: `batchkit-ai`
- import package: `batchkit`
- publishing runs from Git tags like `v0.1.0`
- GitHub Actions publishes through the `pypi` environment
