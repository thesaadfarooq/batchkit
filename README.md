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
        print(row.custom_id, row.response)
    else:
        print(row.custom_id, row.error)
```

Under the raw SDK, that same flow usually requires you to manage the JSONL payload, file upload,
batch creation, polling, artifact download, and row reconciliation as separate steps. Here, the
README example stays focused on your inputs and outputs instead of the transport details.

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
