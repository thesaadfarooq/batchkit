# OpenAI Batch API: Where a Python Wrapper Can Actually Improve the Workflow

Last updated: March 8, 2026

## Executive Summary

Yes. There is enough friction in the current OpenAI Batch workflow to justify a focused Python package.

The main issue is not that the Batch API is weak. It is that the official Python experience still sits at the transport primitive layer:

1. Build JSONL manually.
2. Upload the file manually.
3. Create the batch manually.
4. Poll status manually.
5. Download output and error artifacts manually.
6. Reconcile unordered results manually with `custom_id`.
7. Rebuild failed subsets manually for retries.

That is acceptable as a low-level API. It is not a good application-facing programming model.

The best opportunity is not "another SDK." The opportunity is a workflow layer on top of the official `openai` client that treats a batch as a durable job with builders, manifests, results, retries, and resumability.

## Recommendation

`batchkit` is worth building if it focuses on six things:

1. Request building and validation.
2. Submission orchestration.
3. Polling and lifecycle management.
4. Result parsing and reconciliation.
5. Retry and recovery.
6. Local manifests and resumability.

If it does those well, it removes most of the repetitive code users currently write around the Batch API.

## What I Analyzed

This analysis is based on the current OpenAI Batch docs, the cookbook batch example, the official webhook docs, the cost-optimization guide, and the public `openai-python` SDK source.

The local repository did not contain a concrete implementation to review, so the analysis is focused on:

- the current official workflow,
- the friction it creates in Python,
- and where a wrapper package can add leverage without fighting the underlying API design.

## Current Reality: What the Official Workflow Looks Like

Today, a typical Python flow looks roughly like this:

1. Build one request object per input row.
2. Serialize the requests as `.jsonl`.
3. Upload the file with `purpose="batch"`.
4. Create a batch from that uploaded file.
5. Poll `batches.retrieve(...)` until the batch reaches a terminal state.
6. Download the output file and possibly the error file.
7. Parse both files line by line.
8. Rejoin results with the original inputs using `custom_id`.
9. Decide what to retry.

That flow exposes several real platform constraints:

- Batch is asynchronous.
- The completion window is currently `24h`.
- Input is JSONL.
- Each request line must include a unique `custom_id`.
- Results are not guaranteed to arrive in input order.
- Output and errors are delivered through file IDs.
- Partial completion is normal.
- Batch size limits still matter.

Those are honest platform constraints. A wrapper should not pretend they do not exist.

## The Key Distinction: API Constraints vs SDK Ergonomics

This distinction matters because some pain is unavoidable and some pain is self-inflicted by the current level of abstraction.

### Real API constraints

These cannot be removed, only managed better:

- asynchronous execution,
- file-based input and output artifacts,
- unordered result rows,
- endpoint restrictions,
- partial completion and expiration,
- per-batch request and file-size limits,
- and one-model-per-input-file constraints.

### SDK and workflow ergonomics gaps

These are exactly where a package can help:

- manual JSONL authoring,
- fragmented submission across `files` and `batches`,
- repeated polling loops,
- raw output parsing,
- custom result reconciliation,
- no durable local job state,
- and no first-class retry planning.

The wrapper should target the second list while staying honest about the first.

## The Biggest Bottlenecks, Ranked

### 1. Input construction is too low-level

Severity: Very high

This is the first pain users hit and they hit it every time.

Today, callers usually have to:

- loop through source records,
- generate `custom_id` values,
- build request envelopes by hand,
- ensure endpoint-specific body shapes are correct,
- write line-delimited JSON,
- and retain enough side metadata to reconnect outputs later.

This is repetitive and error-prone. It is not business logic.

Why it matters:

- malformed request rows are easy to create,
- `custom_id` discipline becomes ad hoc,
- endpoint-specific body shapes leak into all calling code,
- and every team writes its own tiny JSONL generator.

High-value abstraction:

- endpoint-aware builders,
- automatic `custom_id` generation,
- schema validation before upload,
- iterable-to-JSONL serialization,
- and stable source-row mapping.

### 2. Submission is fragmented across multiple APIs

Severity: Very high

A simple batch job requires coordinating at least two separate resource types:

- `files`
- `batches`

That means even the happy path requires the caller to manage:

- temporary input artifacts,
- uploaded file IDs,
- batch IDs,
- and metadata handoff between steps.

Why it matters:

- the workflow is easy to get half-right,
- callers must understand file lifecycle details that are incidental,
- and restart/resume becomes harder because local state is usually scattered.

High-value abstraction:

- one `submit()` path that stages input, uploads it, creates the batch, and persists a local manifest.

### 3. Polling is the default experience and it is the wrong abstraction for most users

Severity: Very high

Most examples push users toward some version of:

```python
while True:
    batch = client.batches.retrieve(batch_id)
    if batch.status in terminal_states:
        break
    time.sleep(30)
```

That pattern is simple, but it scales badly across real usage.

Problems it creates:

- copied polling loops,
- inconsistent terminal-state handling,
- no backoff or jitter,
- no progress normalization,
- no timeout policy,
- and no process-resume story.

Important nuance:

This is not just a polling problem. OpenAI also documents webhook-based event delivery. A good wrapper should support polling well and make webhook-driven completion possible for users who want that model.

High-value abstraction:

- `wait()`,
- `wait_async()`,
- progress callbacks,
- normalized terminal statuses,
- and optional webhook integration.

### 4. Result reconciliation is always user code

Severity: Very high

The docs explicitly note that output order is not guaranteed to match input order, so users must reconcile via `custom_id`.

In practice, that means every serious caller ends up implementing:

- a `custom_id` mapping strategy,
- a request manifest,
- a parser for output lines,
- a parser for error lines,
- and a join operation back to the original records.

Why it matters:

- this is the most annoying recurring part of the workflow,
- many users will accidentally assume positional order,
- and the downstream code becomes tightly coupled to raw wire-format dictionaries.

High-value abstraction:

- parsed result rows,
- ordered restoration in original input order,
- joined source-plus-result views,
- and convenience filters such as `successful()`, `failed()`, and `retryable()`.

### 5. Error handling is split across too many surfaces

Severity: High

There is not one failure mode. There are multiple:

1. Validation-time or batch-level errors.
2. Per-request execution errors.
3. Lifecycle outcomes like `expired` or `cancelled`.
4. Partial completion with a mix of success and failure.

That forces the caller to reason across:

- `batch.status`,
- `batch.errors`,
- `output_file_id`,
- `error_file_id`,
- and per-row response/error payloads.

Why it matters:

- retry logic becomes inconsistent,
- failure classification is easy to get wrong,
- and users often over-retry or under-retry.

High-value abstraction:

- a normalized row result model with explicit status classes such as:
  - `succeeded`
  - `failed_validation`
  - `failed_execution`
  - `expired`
  - `cancelled`
  - `retryable`

### 6. There is no strong resumability model

Severity: High

Batch is explicitly long-running, but the official Python flow does not give users a clear durable local state model.

Typical real-world needs:

- resume by batch ID,
- recover after a crash,
- re-download output artifacts later,
- avoid duplicate submission,
- and inspect lineage across retries.

Without a manifest, users usually end up with:

- loose file IDs in notebooks,
- ad hoc local JSON,
- or no durable record at all.

High-value abstraction:

- manifest-backed jobs,
- `resume(...)`,
- artifact caching,
- and rehydration of job state from remote metadata plus local lineage.

### 7. Preflight validation and sharding are mostly left to the caller

Severity: High

The API enforces real limits, including request-count and file-size caps. Those limits are reasonable, but it is poor ergonomics to discover them too late or to force every user to hand-roll chunking.

Why it matters:

- a large submission can fail after significant local preparation,
- sharding logic gets copied across teams,
- and retry subsets need the same machinery all over again.

High-value abstraction:

- request counting,
- payload-size estimation,
- automatic sharding,
- per-shard manifests,
- and aggregated result views across shards.

### 8. Typed outputs remain entirely userland

Severity: Medium-high

Even when users ask for structured output, they still have to:

- parse model output,
- validate each row,
- attach validation failures to the right `custom_id`,
- and carry raw plus parsed representations.

Why it matters:

- structured output pipelines are one of the most common batch use cases,
- and validation is especially painful when only some rows succeed.

High-value abstraction:

- optional Pydantic schemas,
- row objects exposing `.parsed`, `.raw`, and `.error`,
- and bulk validation summaries.

### 9. Observability exists, but it is under-modeled

Severity: Medium

The API exposes useful metadata such as request counts, timestamps, statuses, and artifact IDs. But users still have to assemble their own progress and reporting layer.

High-value abstraction:

- progress summaries,
- health snapshots,
- shard rollups,
- token or cost summaries when available,
- and audit-friendly job reports.

## A Concrete Signal From the SDK: There Is a Missing Helper Tier

One useful signal from the public `openai-python` source is that some other resources already expose higher-level helper methods such as:

- `create_and_poll`
- `upload_and_poll`
- `poll`

That helper tier does not appear to exist for Batch in the same way.

That matters because it shows two things:

1. The SDK already recognizes that some workflows are too tedious if exposed only as raw CRUD calls.
2. Batch is currently a good candidate for a dedicated helper layer above the official client.

This makes `batchkit` feel like a natural extension, not an artificial wrapper.

## Where a Wrapper Package Can Add the Most Value

The best package is not "a nicer dict interface." It is a job orchestration layer.

The right mental model is:

- official SDK = transport and raw resource access
- `batchkit` = request builder, job runner, parser, manifest store, retry planner

That division keeps the package honest and maintainable.

## Recommended Package Shape

### Layer 1: Request builders

Purpose:

- convert Python items into valid batch request lines,
- normalize endpoint-specific request bodies,
- and preserve source-to-request lineage.

Core responsibilities:

- endpoint-aware body builders,
- `custom_id` generation,
- validation,
- JSONL serialization,
- and preflight sizing.

Target API:

```python
job = (
    batchkit.responses()
    .from_iterable(records)
    .custom_id(lambda row: row["id"])
    .body(lambda row: {
        "model": "gpt-5-mini",
        "input": row["prompt"],
    })
    .submit()
)
```

### Layer 2: Submission orchestration

Purpose:

- hide the file-upload choreography,
- persist enough local state to recover later,
- and keep remote IDs inspectable.

Core responsibilities:

- stage JSONL,
- upload with `purpose="batch"`,
- create the batch,
- persist manifests,
- and expose batch plus file IDs without forcing callers to manage them manually.

Target API:

```python
job = builder.submit(metadata={"pipeline": "nightly-eval"})
```

### Layer 3: Lifecycle and waiting

Purpose:

- centralize status handling,
- normalize terminal states,
- and support both polling and event-driven completion.

Core responsibilities:

- `wait()` and `wait_async()`,
- backoff and jitter,
- timeout policies,
- progress callbacks,
- and webhook integration hooks.

Target API:

```python
job.wait(progress=True, timeout=3600)
```

or

```python
await job.wait_async(on_update=print_progress)
```

### Layer 4: Results and reconciliation

Purpose:

- remove raw JSONL parsing from application code,
- and present stable, joined results.

Core responsibilities:

- download output and error artifacts,
- parse rows,
- join by `custom_id`,
- restore original ordering,
- and provide tabular exports.

Target API:

```python
results = job.fetch_results()

for row in results.rows:
    if row.ok:
        handle(row.custom_id, row.response_body)
    else:
        log_error(row.custom_id, row.error)
```

### Layer 5: Retry and recovery

Purpose:

- turn partial completion into a normal workflow rather than a manual cleanup step.

Core responsibilities:

- classify retryable failures,
- rebuild failed subsets,
- preserve lineage between original and retry jobs,
- and merge results across reruns.

Target API:

```python
retry_job = job.retry_failed()
retry_job.wait()
merged = retry_job.merge_back()
```

### Layer 6: Manifest and storage layer

Purpose:

- make jobs durable.

Core responsibilities:

- store local manifests,
- cache artifacts,
- rehydrate prior jobs,
- and support clean retention policies.

Target API:

```python
job = batchkit.resume("movie-classification")
```

## Proposed Core Objects

The package likely only needs a small set of explicit domain objects:

- `BatchKit`
- `BatchBuilder`
- `BatchJob`
- `BatchManifest`
- `BatchResultSet`
- `BatchRowResult`
- `BatchPlan`
- `RetryPlan`

Suggested ownership:

- `BatchBuilder` owns request generation.
- `BatchPlan` owns preflight sizing and sharding.
- `BatchJob` owns remote lifecycle and local persistence.
- `BatchResultSet` owns parsing, filtering, and exports.
- `RetryPlan` owns resubmission strategy.

## The Best Initial Surface Area

If you want to simplify the common case without overbuilding, the package should start with:

- `/v1/responses`
- `/v1/chat/completions`
- `/v1/embeddings`

Those cover most serious batch use cases:

- offline inference,
- classification and enrichment,
- evals,
- dataset transformations,
- and bulk embeddings generation.

Other endpoints can come later once the core abstractions are stable.

## A Good Target User Experience

### What users write today

They usually end up writing:

- row iteration,
- JSONL writing,
- temp file management,
- upload code,
- batch creation,
- polling loops,
- file download logic,
- JSONL parsing,
- `custom_id` joins,
- and retry logic.

### What they should be able to write instead

```python
from openai import OpenAI
from batchkit import BatchKit
from pydantic import BaseModel


class MovieLabels(BaseModel):
    categories: list[str]
    summary: str


client = OpenAI()
bk = BatchKit(client)

job = (
    bk.responses()
    .from_iterable(movies)
    .custom_id(lambda row: row["id"])
    .body(lambda row: {
        "model": "gpt-5-mini",
        "input": row["overview"],
    })
    .submit(name="movie-classification")
)

results = (
    job.wait(progress=True)
    .fetch_results(schema=MovieLabels)
    .ordered()
)

retry_job = job.retry_failed()
```

That is a meaningfully better abstraction level without hiding the underlying OpenAI primitives.

## Recommended MVP

If `batchkit` is going to exist, the MVP should be tight and high leverage.

Build these first:

1. Request builders for `responses` and `chat.completions`.
2. Managed JSONL staging and upload.
3. `submit()` returning a durable `BatchJob`.
4. `wait()` with sane polling and terminal-state handling.
5. Output and error download plus parsing.
6. Result reconciliation by `custom_id`.
7. Ordered result restoration.
8. Manifest-backed resume and recovery.
9. `retry_failed()` for retryable subsets.

That is enough to remove most of the annoying code.

## Strong V2 Features

After the MVP, the most valuable additions are:

- automatic sharding across size and count limits,
- Pydantic-backed parsing and validation,
- Pandas and Polars adapters,
- async API parity,
- a CLI for submit, wait, fetch, retry, and inspect,
- webhook-driven completion support,
- and richer progress and reporting hooks.

## Design Principles

### Keep the official client visible

Do not fork or replace the OpenAI client. Compose it.

That keeps transport concerns delegated to the official SDK and keeps `batchkit` focused on orchestration.

### Keep raw artifacts inspectable

Do not make the package too magical. Users should still be able to inspect:

- generated JSONL,
- local manifests,
- uploaded file IDs,
- batch IDs,
- output files,
- error files,
- and retry manifests.

### Treat manifests as a product feature

The manifest is what turns a convenience helper into a real workflow tool.

Without it, resume, retry, audit, and lineage all become fragile.

### Make retry explicit

Batch jobs can be large and expensive. Retry behavior should be visible and policy-driven, not automatic and opaque.

### Stay endpoint-aware

Do not flatten every endpoint into one shapeless dict interface. The wrapper should normalize workflow, not erase meaningful endpoint differences.

## Risks and Caveats

### Some friction is structural

Batch is asynchronous and file-based. No wrapper can honestly make that fully disappear.

### Too much abstraction can become leaky

If the package tries to universalize every endpoint behind one generic model, it will become harder to debug than the raw SDK.

### Webhooks are not a complete replacement for polling

Local scripts and notebooks will still want polling. Production services may want webhooks. The package should support both without forcing either.

### Sharding and retries can get complicated fast

These features are high value, but they create lineage and manifest complexity. They should be added with discipline rather than rushed into the first version.

## Bottom Line

There is a real product gap here.

The Batch API already does the hard platform work. The missing layer is the Python workflow layer that turns low-level batch primitives into a durable job abstraction.

If `batchkit` focuses on:

- builders,
- submission,
- waiting,
- result reconciliation,
- manifests,
- and retry planning,

then it will solve the exact pain that makes current Batch usage feel repetitive and awkward.

That is enough to justify building it.

## Sources

- [OpenAI Batch guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI Batch API reference](https://platform.openai.com/docs/api-reference/batch)
- [OpenAI Batch request input reference](https://platform.openai.com/docs/api-reference/batch/request-input)
- [OpenAI cookbook: batch processing example](https://cookbook.openai.com/examples/batch_processing)
- [OpenAI webhooks guide](https://platform.openai.com/docs/guides/webhooks)
- [OpenAI cost optimization guide](https://platform.openai.com/docs/guides/cost-optimization)
- [openai-python `batches.py`](https://raw.githubusercontent.com/openai/openai-python/main/src/openai/resources/batches.py)
- [openai-python `vector_stores/file_batches.py`](https://raw.githubusercontent.com/openai/openai-python/main/src/openai/resources/vector_stores/file_batches.py)

## Source-Backed Facts Used Here

- Batch jobs are asynchronous and use a `24h` completion window.
- Batch input is JSONL.
- Each batch line includes a `custom_id`.
- Results are not guaranteed to be returned in input order.
- Output and errors are delivered through separate file artifacts.
- The workflow requires coordinating the Files API and Batch API.
- OpenAI documents webhook support, so polling does not have to be the only completion path.
- The SDK exposes helper methods in some other resources, which highlights the lack of a comparable helper tier for Batch.
