# batchkit

`batchkit` is a thin Python wrapper around the OpenAI Batch API.
It still uses the official `openai` SDK, but it removes most of the manual batch plumbing you
would otherwise write around it.

If you use the Batch API directly through the raw SDK, you usually have to:

- build JSONL request files yourself
- upload the input file and create the batch
- poll until the batch reaches a terminal state
- download output and error artifacts
- reconcile result rows back to the original inputs
- decide which failures are retryable and assemble a retry batch

`batchkit` turns that low-level workflow into a smaller Python flow:

- map source items into batch requests
- submit through the official `openai` SDK
- wait for completion with status updates
- fetch parsed results
- retry failed rows without rebuilding the whole job by hand

## Why use this instead of the raw OpenAI SDK?

- Smaller API surface: the happy path is `client.map(...)`, `job.wait(...)`, and `job.retry_failed()`.
- Less file handling: request JSONL, manifests, and downloaded artifacts are written for you.
- Easier result handling: rows come back with consistent success, failure, and retryable metadata.
- Easier recovery: retryable rows can be resubmitted without manually rebuilding the next batch file.
- Easier inspection: jobs persist under `.batchkit/jobs`, so they can be resumed and audited later.

Install from PyPI:

```bash
pip install batchkit-ai
```

Import as:

```python
import batchkit
```

It is designed to remove the repetitive parts of batch usage while keeping the underlying OpenAI
Batch model intact:

- building JSONL request files
- uploading files and creating batches
- polling batch status
- downloading output and error artifacts
- reconciling results back to the original inputs
- retrying failed rows

## Quick Start

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

Under the raw SDK, that same flow usually requires you to manage the JSONL payload, file upload,
batch creation, polling, artifact download, and row reconciliation as separate steps. Here, the
README example stays focused on your inputs and outputs instead of the transport details.

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
- `failed_execution`: the row or batch failed after submission; retryability depends on `row.retryable`
- `expired`: the batch expired before the row completed
- `cancelled`: the batch was cancelled before the row completed
- `incomplete`: the batch reached a terminal state but no output or error row was returned for the request

`incomplete`, `expired`, and `cancelled` rows are surfaced as actionable failures with normalized
`BatchError` values so downstream code can inspect or retry them instead of treating them as silent
validation failures.

## Retry policies and reports

`job.preview_retry()` lets you inspect retry decisions before you submit a follow-up batch:

```python
from batchkit import RetryPolicy


plan = job.preview_retry(policy=RetryPolicy.execution_only())

print(plan.summary.selected_rows)
print(plan.summary.skipped_by_reason)
```

Built-in retry policy helpers:

- `RetryPolicy.all_retryable()` retries all rows currently marked retryable
- `RetryPolicy.execution_only()` retries only `failed_execution` rows
- `RetryPolicy.incomplete_only()` retries only `incomplete`, `expired`, and `cancelled` rows

You can also filter by error code:

```python
policy = RetryPolicy(
    include_error_codes={"rate_limit_exceeded", "server_error"},
    exclude_error_codes={"batch_failed"},
)

retry_job = job.retry_failed(policy=policy)
```

Each retry job persists:

- the retry policy that was used
- a summary of selected and skipped rows
- per-row retry decisions in `retry_report.json`
- retry lineage in the child manifest so multiple retry attempts remain inspectable

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
