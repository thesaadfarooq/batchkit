# batchkit

`batchkit` is a thin Python wrapper around the OpenAI Batch API.

Install from PyPI:

```bash
pip install batchkit-ai
```

Import as:

```python
import batchkit
```

It is designed to remove the repetitive parts of batch usage:

- building JSONL request files
- uploading files and creating batches
- polling batch status
- downloading output and error artifacts
- reconciling results back to the original inputs
- retrying failed rows

## Goals

- keep the official `openai` SDK under the hood
- make the happy path one normal Python flow
- persist inspectable local manifests under `.batchkit/`
- support both sync and async usage

## Example

```python
from openai import OpenAI
from batchkit import BatchClient


sdk = OpenAI()
client = BatchClient(sdk)

job = client.map(
    name="movie-classification",
    items=movies,
    model="gpt-4.1-mini",
    build_request=lambda movie: {
        "input": movie["overview"],
    },
)

results = job.wait(progress=True)

for row in results.rows:
    if row.ok:
        print(row.custom_id, row.response)
    else:
        print(row.custom_id, row.error)
```

## Development

This project is built test-first.

```bash
uv venv --python 3.11
uv pip install -e ".[dev]"
.venv/bin/python -m pytest
```

## Release

Publishing is handled by GitHub Actions.

- create a version tag like `v0.1.0`
- push the tag
- approve the `pypi` environment job
- the `Publish` workflow uploads `batchkit-ai` to PyPI
