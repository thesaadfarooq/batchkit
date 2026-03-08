# OpenAI Batch API Python Ergonomics Analysis

Last updated: March 8, 2026

## Executive summary

Yes, there is room for a Python package here.

The OpenAI Batch API is operationally useful, but the Python developer experience is still fairly low-level. The official flow is intentionally minimal: create a JSONL file, upload it, create a batch, poll until terminal state, download output and error files, parse JSONL again, then reconcile results back to the original inputs using `custom_id`.

That design is fine as an API primitive. It is not fine as an application-facing programming model.

The biggest pain is not any single API call. It is the orchestration burden around the calls:

1. Request shaping is manual and repetitive.
2. Status handling encourages ad hoc polling loops.
3. Results are file-based and unordered, so reconciliation is always user code.
4. Error handling and retries are batch-specific and easy to get wrong.
5. There is no strong notion of a local job manifest, resumability, or typed outputs.

That creates a clear product gap for a wrapper package that sits on top of the official `openai` client and turns Batch into a high-level job abstraction.

## Scope of this analysis

This document is based on the official OpenAI Batch API reference, the official batch guide and cookbook example, current model pages, and the official webhooks documentation.

The goal is not to replace the official SDK. The goal is to identify where a companion package can remove repetitive code while staying aligned with the official API.

## What the current workflow looks like

A typical Python flow today looks like this:

1. Build one JSON object per request.
2. Serialize those objects into a `.jsonl` file.
3. Upload the file with `purpose="batch"`.
4. Create a batch with `input_file_id`, `endpoint`, and `completion_window="24h"`.
5. Poll `batches.retrieve(...)` until the batch reaches a terminal status.
6. Download the `output_file_id` and possibly the `error_file_id`.
7. Parse both files line by line.
8. Join results back to original inputs with `custom_id`.
9. Decide what to retry, resubmit, or ignore.

The cookbook example makes the ergonomics problem obvious: there is custom JSONL building, manual file I/O, manual polling, manual output download, and manual result matching because results are not returned in input order.

## What is a true API constraint vs. what is an SDK ergonomics problem

### Real API constraints

These are constraints a wrapper must respect, not hide by pretending they do not exist:

1. Batches are asynchronous and complete within a `24h` completion window.
2. Input is JSONL, one request per line.
3. Only `POST` requests are supported inside a batch.
4. Supported endpoints are limited to a small set, including `/v1/responses`, `/v1/chat/completions`, `/v1/embeddings`, `/v1/completions`, and `/v1/moderations`.
5. Each request must have a unique `custom_id`.
6. A single input file can only target one model.
7. Output is delivered via file IDs, not inline in the batch object.
8. Results are not guaranteed to be returned in the same order as inputs.
9. Batches can end as `completed`, `expired`, `failed`, `cancelled`, etc., and partial completion is normal.

### SDK ergonomics problems

These are the places where a Python package can materially improve the experience:

1. Developers must hand-roll the JSONL writer almost every time.
2. Developers often build fragile `while True: sleep(); retrieve()` loops.
3. Developers must remember terminal states and special handling for `expired` and `error_file_id`.
4. Developers have to parse output files and rebuild input-to-output mappings themselves.
5. Developers usually reimplement retries for failed or expired request rows.
6. Developers rarely get a durable local state model for resume/recovery.
7. Type safety is weak unless users add their own schema layer.

## Biggest bottlenecks and pain points

### 1. JSONL request construction is too manual

This is the first major friction point.

The official shape is simple, but actual app code becomes repetitive fast:

- generate a unique `custom_id`
- choose endpoint
- choose model
- wrap body in batch line format
- serialize line-by-line
- ensure no malformed row slips in

For dataset-driven use cases, users also need to map rows to prompts and preserve enough metadata to reconstruct outputs later.

This is busywork, not business logic.

### 2. Polling is the default UX, even though it is the wrong abstraction

The official docs and cookbook naturally push users toward repeated `retrieve()` calls until status becomes `completed`.

That leads to:

- copy-pasted polling loops
- inconsistent sleep intervals
- missing terminal-state handling
- no jitter or backoff
- no timeout policy
- no persistence across process restarts

This is exactly the kind of boilerplate that should be centralized.

Important nuance: this is not purely an API problem. OpenAI also supports webhooks for events such as batch completion. A good package should make polling optional, not mandatory.

### 3. Result reconciliation is always custom work

This is probably the single most annoying day-to-day pain.

The output file is not guaranteed to match input order, so users must reconcile by `custom_id`. That is fine at protocol level, but painful at application level.

Typical consequences:

- users encode row indexes into `custom_id`
- users maintain sidecar maps from `custom_id` to source records
- users parse JSONL output into lists and do manual joins
- users write one-off logic for success rows vs. error rows

A wrapper should treat this as a first-class operation, not as post-processing trivia.

### 4. Error handling is split across multiple places

There are at least three distinct failure surfaces:

1. Validation-time or batch-level errors.
2. Per-request errors inside the output or error files.
3. Expiration or cancellation that leaves a mix of completed and incomplete rows.

That means the developer has to inspect:

- `batch.status`
- `batch.errors`
- `batch.error_file_id`
- per-line `error`
- HTTP status codes inside per-line responses

This is powerful, but it is not ergonomic. A high-level package should collapse these into a clearer result model like:

- `succeeded`
- `failed_validation`
- `failed_execution`
- `expired`
- `cancelled`
- `retryable`

### 5. There is no strong resumability model

If a script crashes after upload but before local state is written, developers often lose the easy path back to the job.

Common operational needs:

- resume by batch ID
- rebuild local manifest from remote batch metadata
- cache input/output/error file locations
- avoid duplicate submission
- safely re-download results

The official SDK exposes the primitives, but not the workflow.

This gap matters a lot for real usage because Batch is explicitly long-running and often used in offline pipelines.

### 6. Batch sharding is left to the user

The batch guide describes important limits, including a maximum of 50,000 requests per batch and a maximum input file size of 200 MB. Embeddings batches have an additional cap on total embedding inputs.

Those limits are reasonable, but users should not have to hand-build chunking logic every time.

A package should be able to:

- estimate payload size
- split work into multiple batches automatically
- keep stable manifests across shards
- aggregate results across shard boundaries

This is a high-value abstraction because it removes both boilerplate and failure risk.

### 7. The file lifecycle is too exposed

The current flow makes developers think about temporary files more than they should:

- where to write the input JSONL
- whether to keep it
- where to store downloaded output
- how to parse it later
- whether to clean up remote and local artifacts

For many users, local file management is accidental complexity. A wrapper should support:

- in-memory staging when practical
- managed temp directories
- explicit artifact retention policies
- automatic naming and manifests

### 8. Typed output extraction is still userland code

Even if the prompt requests JSON or structured output, the developer still has to:

- parse the response body
- validate the parsed shape
- associate validation errors with the correct input row

This gets worse when partial failure is involved.

There is a strong opportunity for schema-first helpers here:

- Pydantic model per output row
- parsed result object with `.parsed`, `.raw_text`, `.error`
- bulk validation report

### 9. Observability is under-modeled

The Batch object exposes useful status and count information, and newer batches can expose usage data, but application developers still have to assemble their own dashboards and summaries.

Useful missing abstractions:

- percent-complete reporting
- aggregate token/cost summaries
- batch health summary
- shard-level rollups
- retry report generation

This is especially valuable for notebook, ETL, and scheduled-job use cases.

## Where a Python package can create the most value

The best package is not “another SDK.” It is an orchestration layer with good defaults.

### 1. High-level job abstraction

Core idea:

```python
job = batcher.run(
    name="movie-classification",
    items=df.to_dict("records"),
    endpoint="responses",
    model="gpt-4o-mini",
    build_request=build_request,
    output_schema=MovieLabels,
)
```

Instead of exposing raw files and raw batch IDs everywhere, expose a `BatchJob` object:

- `job.submit()`
- `job.wait()`
- `job.refresh()`
- `job.results()`
- `job.failures()`
- `job.retry_failed()`
- `job.to_dataframe()`

### 2. Request builder abstraction

Developers should provide the business-level request logic only:

```python
def build_request(row: dict) -> dict:
    return {
        "input": [
            {"role": "system", "content": "Classify this movie."},
            {"role": "user", "content": row["overview"]},
        ],
        "response_format": MovieLabels,
    }
```

The wrapper should inject:

- endpoint path
- model
- `custom_id`
- request envelope
- serialization

### 3. Managed polling and webhook integration

The package should support both:

- `wait(poll_interval=..., timeout=...)`
- `wait_via_webhook(...)`

For polling, the package should own:

- terminal-state detection
- backoff and jitter
- timeout behavior
- log/progress hooks

For webhooks, it should provide:

- signature verification helpers
- event-to-job lookup
- idempotent completion handling

### 4. Automatic result reconciliation

This should be a headline feature.

The wrapper should maintain a manifest that maps:

- source item
- `custom_id`
- shard ID
- remote batch ID
- final output row
- parse status

That lets users ask for:

- ordered results in original input order
- only successes
- only retryable failures
- outputs joined back to original records

### 5. Retry planner

Retries are where most batch wrappers either become useful or become shallow.

The package should distinguish:

- batch-level failure
- retryable line error
- permanent validation error
- expiration-derived retry candidates

Then provide:

```python
retry_job = job.retry_failed(reason_filter={"batch_expired", "request_timeout"})
```

That retry flow should rebuild a fresh JSONL from only retryable rows.

### 6. Sharding and aggregation

Users should be able to submit 200k source rows and let the package decide how many batches to create.

Needed features:

- size-aware sharding
- request-count-aware sharding
- embeddings-specific sharding rules
- aggregate status across shards
- aggregate result download and parsing

### 7. Typed result layer

Ideal output model:

```python
result = job.results()

for row in result.rows:
    if row.ok:
        print(row.input_item, row.parsed)
    else:
        print(row.custom_id, row.error, row.retryable)
```

This is much better than forcing every user to inspect nested dictionaries like:

- `response.body.choices[0].message.content`
- per-line `error`
- raw JSONL text

### 8. Local manifest and resumability

Every job should create a small local manifest, likely JSON or SQLite-backed:

- job name
- creation timestamp
- config hash
- remote file IDs
- remote batch IDs
- shard metadata
- local artifact paths
- final summary

Then users can do:

```python
job = batcher.resume("movie-classification")
job.wait()
```

This is one of the most practically valuable features for offline pipelines.

## Proposed package shape

### Guiding principle

Wrap the official `openai` Python client. Do not fork API semantics. Do not hide core batch realities. Normalize them.

### Suggested modules

- `batchkit.client`
  - main entrypoint, wraps `OpenAI`
- `batchkit.job`
  - `BatchJob`, `BatchShard`, manifests, lifecycle methods
- `batchkit.builders`
  - request builders for `responses`, `chat.completions`, `embeddings`, `moderations`
- `batchkit.polling`
  - waiters, backoff, terminal-state logic
- `batchkit.results`
  - parsing, reconciliation, ordered joins, dataframe export
- `batchkit.retry`
  - retry planning and resubmission
- `batchkit.schemas`
  - Pydantic integration and validation reports
- `batchkit.webhooks`
  - webhook verification and event handling
- `batchkit.storage`
  - local manifest store, temp artifacts, retention policies

### Suggested core classes

- `BatchClient`
- `BatchJob`
- `BatchResult`
- `BatchRowResult`
- `RetryPlan`
- `BatchManifest`

## Example of the target user experience

### Today

Today, a user usually writes:

- row iteration
- JSONL writing
- file upload
- batch creation
- polling loop
- output download
- JSONL parse loop
- `custom_id` join logic
- retry logic

### Desired

The package should reduce that to:

```python
from batchkit import BatchClient
from pydantic import BaseModel


class MovieLabels(BaseModel):
    categories: list[str]
    summary: str


client = BatchClient()

job = client.map(
    name="movie-classification",
    items=movies,
    endpoint="chat.completions",
    model="gpt-4o-mini",
    output_schema=MovieLabels,
    build_messages=lambda movie: [
        {"role": "system", "content": "Classify and summarize this movie."},
        {"role": "user", "content": movie["overview"]},
    ],
)

results = job.wait().ordered_results()
failures = job.failures()
retry_job = job.retry_failed()
```

That is a meaningfully different abstraction level.

## Recommended MVP

If building this package, I would not start with every possible feature.

### Phase 1: strong MVP

Build these first:

1. Request builder for `/v1/responses` and `/v1/chat/completions`.
2. Managed JSONL staging and upload.
3. `BatchJob.wait()` with good terminal-state handling.
4. Output and error file download + parse.
5. Result reconciliation by `custom_id`.
6. Ordered result restoration.
7. Local manifest/resume support.
8. Retry of failed or expired rows.

This is enough to remove most of the annoying code.

### Phase 2: high-leverage additions

1. Pydantic output validation.
2. DataFrame and Polars helpers.
3. Multi-batch sharding.
4. CLI for submit/wait/download/retry.
5. Progress hooks and usage summaries.

### Phase 3: advanced operational features

1. Webhook-driven completion flow.
2. SQLite-backed job store.
3. Batch dashboards or HTML reports.
4. Airflow, Prefect, Dagster integrations.

## Design choices I would make

### Use `/v1/responses` as the default path going forward

The package should support both `/v1/responses` and `/v1/chat/completions`, but I would bias the package API toward `responses` unless a team has a hard dependency on the older shape.

Reason:

- it aligns better with current OpenAI platform direction
- it is a better long-term default for new work
- it reduces future migration pressure

### Keep artifacts inspectable

Do not make the package too magical.

Users should be able to inspect:

- generated JSONL
- uploaded file IDs
- batch IDs
- downloaded output and error artifacts
- retry manifests

The wrapper should simplify, not obscure.

### Prefer explicit retry policies

Avoid “retry everything automatically” defaults.

Batch workflows are often large-scale and expensive. Retrying should be visible, reviewable, and policy-driven.

### Treat manifests as product, not implementation detail

The manifest is what turns a script helper into a real workflow tool.

Without manifests, users still end up rebuilding state by hand after interruptions.

## Risks and caveats

### 1. Some friction is structural

Because the API is file-based and asynchronous, no wrapper can make Batch feel fully synchronous without lying to the user.

The package should improve the workflow honestly, not pretend the underlying mechanics do not exist.

### 2. Model and endpoint constraints still matter

A wrapper cannot remove constraints like:

- one-model-per-input-file
- endpoint restrictions
- batch size and file size caps
- completion within the allowed batch window

It can only pre-validate and automate around them.

### 3. Webhooks are not a universal replacement for polling

Webhooks are great for production systems, but many notebook or local-script users will still prefer polling. The package should support both cleanly.

### 4. Too much abstraction can become leaky

If the package tries to flatten every endpoint into one generic interface, it may become harder to debug than the raw SDK.

The wrapper should keep endpoint-specific concepts visible when they matter.

## Bottom line

There is a real opportunity for a Python package here.

The Batch API itself is not the main problem. The problem is that the official developer workflow is still at the “transport primitive” layer. A good package can add a higher-level “batch job” layer that handles:

- request building
- sharding
- polling or webhook waiting
- result reconciliation
- typed parsing
- manifesting
- retries

If built well, this would not just save a few lines of code. It would turn Batch from an awkward low-level utility into a reliable workflow primitive for ETL, offline inference, evals, enrichment pipelines, and scheduled background jobs.

## Most promising package positioning

If I were naming and positioning it, I would pitch it as:

> A workflow-oriented Python wrapper for OpenAI Batch jobs, focused on submit, wait, reconcile, validate, and retry.

That is narrow enough to stay coherent and broad enough to be useful.

## Sources

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch/batch-api)
- [OpenAI Batch API reference](https://platform.openai.com/docs/api-reference/batch/retrieve)
- [OpenAI cookbook: Batch processing with the Batch API](https://cookbook.openai.com/examples/batch_processing)
- [OpenAI webhooks guide](https://platform.openai.com/docs/webhooks)
- [OpenAI cost optimization guide](https://platform.openai.com/docs/guides/cost-optimization)
- [OpenAI model pages](https://developers.openai.com/api/docs/models)

## Key source-backed facts used here

- Batch jobs complete within a `24h` completion window and are designed for lower-cost asynchronous processing.
- Batch supports a constrained set of endpoints and currently uses JSONL request files.
- Each request line needs a unique `custom_id`.
- Result rows are not guaranteed to come back in input order, so `custom_id` is required for matching.
- Batch status includes states such as `validating`, `in_progress`, `finalizing`, `completed`, `expired`, `cancelling`, and `cancelled`.
- Output and error artifacts are provided separately via file IDs.
- The docs describe limits of up to 50,000 requests and 200 MB per input file, with extra embeddings-specific restrictions.
- OpenAI webhooks can notify clients when a batch completes, which means polling does not have to be the only completion strategy.
