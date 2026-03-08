# batchkit

`batchkit` is a thin Python wrapper around the OpenAI Batch API.

It turns the low-level Batch workflow into a simpler Python flow:

- map source items into batch requests
- submit through the official `openai` SDK
- wait for completion with status updates
- fetch parsed results
- retry failed rows without rebuilding the whole job by hand

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
        print(row.custom_id, row.response_body)
    else:
        print(row.custom_id, row.error)
```

## Result helpers and row statuses

`BatchResults` keeps the raw `rows` list, but also exposes helpers that are easier to consume in
application code:

- `results.successes()` / `results.successful()`
- `results.failures()` / `results.failed()`
- `results.retryables()` / `results.retryable()`
- `results.incomplete()` for rows affected by cancellation, expiration, or missing terminal rows
- `results.errors()` for normalized `BatchError` instances
- `results.get(custom_id)` / `results.by_custom_id()`

Each `BatchRow` exposes:

- `row.response_body` for the response payload body when a row succeeded
- `row.error` for normalized error metadata (`code`, `error_type`, `param`, `line`, `payload`)

Documented row statuses:

- `succeeded`: row completed successfully
- `failed_validation`: the row failed with a non-retryable request or validation problem
- `failed_execution`: the row failed after submission and may be retried
- `expired`: the batch expired before the row completed
- `cancelled`: the batch was cancelled before the row completed
- `incomplete`: the batch reached a terminal state but no output or error row was returned for the request

`incomplete`, `expired`, and `cancelled` rows are surfaced as actionable failures with normalized
`BatchError` values so downstream code can inspect or retry them instead of treating them as silent
validation failures.

## What It Handles

- request JSONL generation
- batch file upload and creation
- polling and terminal-state handling
- local manifests under `.batchkit/`
- output and error artifact download
- result reconciliation by `custom_id`
- retry job creation for retryable rows

## Scope

Current scope:

- OpenAI only
- `responses` endpoint first
- sync and async clients

Contributor setup and release workflow live in [CONTRIBUTING.md](CONTRIBUTING.md).
