from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import BaseModel

from batchkit import (
    AsyncBatchClient,
    BatchClient,
    BatchNotReadyError,
    DuplicateCustomIDError,
    RetryPolicy,
    RetryUnavailableError,
)


@dataclass
class FakeBatch:
    id: str
    status: str
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    request_counts: dict[str, int] | None = None


@dataclass
class FakeRequestCounts:
    total: int
    completed: int
    failed: int


class FakeFilesAPI:
    def __init__(
        self,
        *,
        output_payload: bytes | None = None,
        error_payload: bytes | None = None,
    ) -> None:
        self.created_files: list[dict[str, str]] = []
        self.output_payload = output_payload or b""
        self.error_payload = error_payload or b""

    def create(self, *, file, purpose: str):  # noqa: ANN001
        payload = file.read().decode("utf-8")
        self.created_files.append({"purpose": purpose, "payload": payload})
        return {"id": "file-input-123"}

    def content(self, file_id: str) -> bytes:
        if file_id == "file-output-123":
            return self.output_payload
        if file_id == "file-error-123":
            return self.error_payload
        raise KeyError(file_id)


class FakeBatchesAPI:
    def __init__(self) -> None:
        self.created_with: list[dict[str, object]] = []
        self.retrieve_calls = 0
        self.current = FakeBatch(
            id="batch-123",
            status="in_progress",
            input_file_id="file-input-123",
            request_counts={"total": 2, "completed": 0, "failed": 0},
        )

    def create(self, **kwargs):  # noqa: ANN003
        self.created_with.append(kwargs)
        return self.current

    def retrieve(self, batch_id: str) -> FakeBatch:
        self.retrieve_calls += 1
        if self.retrieve_calls >= 1:
            self.current = FakeBatch(
                id=batch_id,
                status="completed",
                input_file_id="file-input-123",
                output_file_id="file-output-123",
                error_file_id="file-error-123",
                request_counts={"total": 2, "completed": 1, "failed": 1},
            )
        return self.current

    def cancel(self, batch_id: str) -> FakeBatch:
        self.current = FakeBatch(id=batch_id, status="cancelled", input_file_id="file-input-123")
        return self.current


class FakeSDK:
    def __init__(
        self,
        *,
        output_payload: bytes | None = None,
        error_payload: bytes | None = None,
    ) -> None:
        self.files = FakeFilesAPI(output_payload=output_payload, error_payload=error_payload)
        self.batches = FakeBatchesAPI()


class AsyncFakeFilesAPI(FakeFilesAPI):
    async def create(self, *, file, purpose: str):  # type: ignore[override]  # noqa: ANN001
        return super().create(file=file, purpose=purpose)

    async def content(self, file_id: str) -> bytes:  # type: ignore[override]
        return super().content(file_id)


class AsyncFakeBatchesAPI(FakeBatchesAPI):
    async def create(self, **kwargs):  # type: ignore[override]  # noqa: ANN003
        return super().create(**kwargs)

    async def retrieve(self, batch_id: str) -> FakeBatch:  # type: ignore[override]
        return super().retrieve(batch_id)

    async def cancel(self, batch_id: str) -> FakeBatch:  # type: ignore[override]
        return super().cancel(batch_id)


class AsyncFakeSDK:
    def __init__(
        self,
        *,
        output_payload: bytes | None = None,
        error_payload: bytes | None = None,
    ) -> None:
        self.files = AsyncFakeFilesAPI(output_payload=output_payload, error_payload=error_payload)
        self.batches = AsyncFakeBatchesAPI()


class ParsedMovie(BaseModel):
    id: str
    output_text: str


class InvalidMovie(BaseModel):
    count: int


def _output_rows() -> bytes:
    row = {
        "custom_id": "movies-0",
        "response": {"body": {"id": "resp_1", "output_text": "The Matrix"}},
    }
    return (json.dumps(row) + "\n").encode("utf-8")


def _error_rows() -> bytes:
    row = {
        "custom_id": "movies-1",
        "error": {
            "message": "rate limited",
            "type": "rate_limit_exceeded",
            "param": "input",
            "line": 2,
        },
    }
    return (json.dumps(row) + "\n").encode("utf-8")


def test_map_writes_request_artifacts_and_submits_batch(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)

    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    manifest = json.loads((tmp_path / "job" / "manifest.json").read_text(encoding="utf-8"))
    request_lines = (
        (tmp_path / "job" / "requests.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )

    assert job.batch_id == "batch-123"
    assert manifest["endpoint"] == "responses"
    assert len(request_lines) == 2
    assert json.loads(request_lines[0])["body"]["model"] == "gpt-4.1-mini"
    assert sdk.batches.created_with[0]["endpoint"] == "/v1/responses"
    assert job.input_file_id == "file-input-123"
    assert job.output_file_id is None
    assert job.error_file_id is None


def test_map_rejects_duplicate_custom_ids(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)

    with pytest.raises(DuplicateCustomIDError):
        client.map(
            name="movies",
            items=[{"id": 1}, {"id": 1}],
            model="gpt-4.1-mini",
            build_request=lambda item: {"input": str(item["id"])},
            custom_id=lambda item: "same-id",
            storage_dir=tmp_path / "job",
        )


def test_map_rejects_empty_items(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)

    with pytest.raises(ValueError, match="items must not be empty"):
        client.map(
            name="movies",
            items=[],
            model="gpt-4.1-mini",
            build_request=lambda item: {"input": item["prompt"]},
            storage_dir=tmp_path / "job",
        )


def test_wait_returns_results_and_retry_job(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    results = job.wait(poll_interval=0)
    success = results.successes()[0]
    failure = results.failures()[0]

    assert results.counts.total == 2
    assert results.counts.succeeded == 1
    assert results.counts.retryable == 1
    assert success.custom_id == "movies-0"
    assert success.response_body == {"id": "resp_1", "output_text": "The Matrix"}
    assert results.get("movies-0") is success
    assert results.get("missing-id") is None
    assert failure.custom_id == "movies-1"
    assert failure.status == "failed_execution"
    assert failure.error is not None
    assert failure.error.code == "rate_limit_exceeded"
    assert failure.error.error_type == "rate_limit_exceeded"
    assert failure.error.param == "input"
    assert failure.error.line == 2
    assert results.errors() == [failure.error]

    retry_job = job.retry_failed()
    retry_requests = (
        (retry_job.storage_dir / "requests.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )

    assert len(retry_requests) == 1
    assert json.loads(retry_requests[0])["custom_id"] == "movies-1"
    assert retry_job.storage_dir.parent == tmp_path


def test_wait_marks_missing_rows_as_incomplete_and_retryable(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    results = job.wait(poll_interval=0)
    incomplete = results.incomplete()

    assert len(incomplete) == 1
    assert incomplete[0].custom_id == "movies-1"
    assert incomplete[0].status == "incomplete"
    assert incomplete[0].retryable is True
    assert incomplete[0].error is not None
    assert incomplete[0].error.code == "missing_result_row"
    assert incomplete[0].error.payload["batch_status"] == "completed"


def test_refresh_serializes_request_counts_objects(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    sdk.batches.current.request_counts = FakeRequestCounts(total=2, completed=0, failed=0)  # type: ignore[assignment]
    client = BatchClient(sdk)

    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job.refresh()

    manifest = json.loads((tmp_path / "job" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["request_counts"] == {"total": 2, "completed": 1, "failed": 1}
    assert manifest["updated_at"] != manifest["created_at"]


def test_resume_supports_manifest_path_directory_and_name(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk, storage_root=tmp_path)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
    )

    resumed_from_dir = client.resume(job.storage_dir)
    resumed_from_manifest = client.resume(job.storage_dir / "manifest.json")
    resumed_from_name = client.resume("movies")

    assert resumed_from_dir.id == job.id
    assert resumed_from_manifest.id == job.id
    assert resumed_from_name.id == job.id


def test_resume_raises_for_missing_job(tmp_path: Path) -> None:
    client = BatchClient(FakeSDK(), storage_root=tmp_path)

    with pytest.raises(FileNotFoundError, match="Could not find batch job 'missing'"):
        client.resume("missing")


def test_wait_prints_progress_and_results_support_schema_parsing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    job.wait(progress=True, poll_interval=0)
    parsed_results = job.results(schema=ParsedMovie)
    output = capsys.readouterr().out

    assert "[batchkit] movies: in_progress -> completed" in output
    assert parsed_results.successful()[0].response == {"id": "resp_1", "output_text": "The Matrix"}


def test_results_with_invalid_schema_marks_rows_as_validation_failures(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job.wait(poll_interval=0)

    results = job.results(schema=InvalidMovie)
    row = results.rows[0]

    assert row.ok is False
    assert row.status == "failed_validation"
    assert row.retryable is False
    assert row.error is not None
    assert row.error.code == "schema_validation_error"


def test_results_raise_when_batch_never_reaches_terminal_state(tmp_path: Path) -> None:
    sdk = FakeSDK()
    sdk.batches.retrieve = lambda batch_id: sdk.batches.current  # type: ignore[method-assign]
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(BatchNotReadyError):
        job.results()


def test_wait_times_out_when_batch_never_completes(tmp_path: Path) -> None:
    sdk = FakeSDK()
    sdk.batches.retrieve = lambda batch_id: sdk.batches.current  # type: ignore[method-assign]
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(TimeoutError, match="Timed out waiting for batch batch-123"):
        job.wait(timeout=0, poll_interval=0)


def test_wait_calls_sleep_before_timing_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = FakeSDK()
    sdk.batches.retrieve = lambda batch_id: sdk.batches.current  # type: ignore[method-assign]
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    monotonic_values = iter([0.0, 0.0, 2.0])
    sleep_calls: list[float] = []

    monkeypatch.setattr("batchkit.jobs.time.monotonic", lambda: next(monotonic_values, 2.0))
    monkeypatch.setattr("batchkit.jobs.time.sleep", lambda interval: sleep_calls.append(interval))

    with pytest.raises(TimeoutError):
        job.wait(timeout=1, poll_interval=0.5)

    assert sleep_calls == [0.5]


def test_refresh_and_cancel_are_noops_without_batch_id(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job._manifest["batch_id"] = None

    assert job.refresh() is job
    assert job.cancel() is job


def test_results_raise_runtime_error_without_pydantic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job.wait(poll_interval=0)
    real_import = __import__

    def fake_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> object:
        if name == "pydantic":
            raise ImportError("missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", fake_import)

    with pytest.raises(
        RuntimeError,
        match="Schema parsing requires the optional 'pydantic' dependency",
    ):
        job.results(schema=ParsedMovie)


def test_retry_failed_raises_public_retry_error(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(RetryUnavailableError):
        job.retry_failed()


def test_preview_retry_reports_selected_and_skipped_rows(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    plan = job.preview_retry()

    assert plan.source_job_id == job.id
    assert plan.root_job_id == job.id
    assert plan.attempt == 1
    assert plan.lineage_job_ids == [job.id]
    assert plan.summary.total_rows == 2
    assert plan.summary.selected_rows == 1
    assert plan.summary.skipped_rows == 1
    assert plan.summary.selected_by_status == {"failed_execution": 1}
    assert plan.summary.skipped_by_reason == {"non_retryable": 1}
    assert [decision.custom_id for decision in plan.selected] == ["movies-1"]
    assert [decision.reason_code for decision in plan.skipped] == ["non_retryable"]


def test_preview_retry_can_filter_by_status_and_error_code(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    filtered_by_code = job.preview_retry(
        policy=RetryPolicy(include_error_codes={"api_connection_error"})
    )

    assert filtered_by_code.summary.selected_rows == 0
    assert filtered_by_code.summary.skipped_by_reason == {
        "error_code_not_included": 1,
        "non_retryable": 1,
    }

    sdk = FakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = BatchClient(sdk)
    incomplete_job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "incomplete-job",
    )

    filtered_by_status = incomplete_job.preview_retry(policy=RetryPolicy.execution_only())

    assert filtered_by_status.summary.selected_rows == 0
    assert filtered_by_status.summary.skipped_by_reason == {
        "non_retryable": 1,
        "status_filtered": 1,
    }


def test_preview_retry_handles_invalid_lineage_and_excluded_error_codes(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job._manifest["retry"] = {"lineage_job_ids": "invalid"}

    filtered = job.preview_retry(policy=RetryPolicy.incomplete_only())

    assert filtered.lineage_job_ids == [job.id]
    assert filtered.summary.skipped_by_reason == {
        "non_retryable": 1,
        "status_filtered": 1,
    }

    excluded = job.preview_retry(policy=RetryPolicy(exclude_error_codes={"rate_limit_exceeded"}))
    assert excluded.summary.skipped_by_reason == {
        "error_code_excluded": 1,
        "non_retryable": 1,
    }


def test_retry_failed_persists_retry_report_and_lineage(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    retry_job = job.retry_failed(policy=RetryPolicy.execution_only())
    retry_manifest = json.loads(
        (retry_job.storage_dir / "manifest.json").read_text(encoding="utf-8")
    )
    retry_report = json.loads(
        (retry_job.storage_dir / "retry_report.json").read_text(encoding="utf-8")
    )

    assert retry_manifest["retry"]["source_job_id"] == job.id
    assert retry_manifest["retry"]["root_job_id"] == job.id
    assert retry_manifest["retry"]["attempt"] == 1
    assert retry_manifest["retry"]["lineage_job_ids"] == [job.id]
    assert retry_manifest["retry"]["summary"]["selected_rows"] == 1
    assert retry_manifest["paths"]["retry_report"].endswith("retry_report.json")
    assert retry_report["policy"]["statuses"] == ["failed_execution"]
    assert retry_report["summary"]["selected_rows"] == 1
    assert sdk.batches.created_with[1]["metadata"] == {
        "retry_attempt": "1",
        "retry_of": job.id,
        "retry_root": job.id,
        "retry_selected": "1",
    }

    retry_job_two = retry_job.retry_failed(
        name="movies-retry-2",
        policy=RetryPolicy.execution_only(),
    )
    retry_manifest_two = json.loads(
        (retry_job_two.storage_dir / "manifest.json").read_text(encoding="utf-8")
    )

    assert retry_manifest_two["retry"]["root_job_id"] == job.id
    assert retry_manifest_two["retry"]["attempt"] == 2
    assert retry_manifest_two["retry"]["lineage_job_ids"] == [job.id, retry_job.id]
    assert sdk.batches.created_with[2]["metadata"]["retry_of"] == retry_job.id
    assert sdk.batches.created_with[2]["metadata"]["retry_root"] == job.id


def test_retry_failed_with_filtered_policy_raises_clear_error(tmp_path: Path) -> None:
    sdk = FakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(
        RetryUnavailableError,
        match="No rows matched retry policy",
    ):
        job.retry_failed(policy=RetryPolicy(include_error_codes={"api_connection_error"}))


def test_cancelled_batches_surface_cancelled_rows(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    results = job.cancel().results()

    assert [row.status for row in results.rows] == ["cancelled", "cancelled"]
    assert len(results.incomplete()) == 2
    assert all(row.retryable for row in results.rows)
    assert all(row.error is not None for row in results.rows)
    assert results.errors()[0].code == "batch_cancelled"


def test_expired_batches_surface_expired_rows(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job._manifest["status"] = "expired"
    job._manifest["output_file_id"] = None
    job._manifest["error_file_id"] = None

    results = job.results()

    assert [row.status for row in results.rows] == ["expired", "expired"]
    assert len(results.incomplete()) == 2
    assert all(row.retryable is True for row in results.rows)
    assert all(row.error is not None for row in results.rows)
    assert results.errors()[0].code == "batch_expired"


def test_failed_batches_do_not_mark_missing_rows_retryable(tmp_path: Path) -> None:
    sdk = FakeSDK()
    client = BatchClient(sdk)
    job = client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job._manifest["status"] = "failed"
    job._manifest["output_file_id"] = None
    job._manifest["error_file_id"] = None

    results = job.results()

    assert [row.status for row in results.rows] == ["failed_execution", "failed_execution"]
    assert results.incomplete() == []
    assert all(row.retryable is False for row in results.rows)
    assert all(row.error is not None for row in results.rows)
    assert results.errors()[0].code == "batch_failed"


@pytest.mark.asyncio
async def test_async_map_and_wait(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = AsyncBatchClient(sdk)

    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    results = await job.wait(poll_interval=0)

    assert results.counts.total == 2
    assert results.successful()[0].custom_id == "movies-0"


@pytest.mark.asyncio
async def test_async_resume_supports_manifest_path_directory_and_name(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = AsyncBatchClient(sdk, storage_root=tmp_path)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
    )

    resumed_from_dir = await client.resume(job.storage_dir)
    resumed_from_manifest = await client.resume(job.storage_dir / "manifest.json")
    resumed_from_name = await client.resume("movies")

    assert resumed_from_dir.id == job.id
    assert resumed_from_manifest.id == job.id
    assert resumed_from_name.id == job.id


@pytest.mark.asyncio
async def test_async_resume_raises_for_missing_job(tmp_path: Path) -> None:
    client = AsyncBatchClient(AsyncFakeSDK(), storage_root=tmp_path)

    with pytest.raises(FileNotFoundError, match="Could not find batch job 'missing'"):
        await client.resume("missing")


@pytest.mark.asyncio
async def test_async_map_rejects_duplicate_custom_ids(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()
    client = AsyncBatchClient(sdk)

    with pytest.raises(DuplicateCustomIDError):
        await client.map(
            name="movies",
            items=[{"id": 1}, {"id": 1}],
            model="gpt-4.1-mini",
            build_request=lambda item: {"input": str(item["id"])},
            custom_id=lambda item: "same-id",
            storage_dir=tmp_path / "job",
        )


@pytest.mark.asyncio
async def test_async_wait_marks_missing_rows_as_incomplete_and_retryable(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = AsyncBatchClient(sdk)

    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    results = await job.wait(poll_interval=0)
    success = results.successes()[0]
    incomplete = results.incomplete()

    assert success.custom_id == "movies-0"
    assert success.response_body == {"id": "resp_1", "output_text": "The Matrix"}
    assert results.get("movies-0") is success
    assert len(incomplete) == 1
    assert incomplete[0].custom_id == "movies-1"
    assert incomplete[0].status == "incomplete"
    assert incomplete[0].retryable is True
    assert incomplete[0].error is not None
    assert incomplete[0].error.code == "missing_result_row"
    assert results.errors() == [incomplete[0].error]


@pytest.mark.asyncio
async def test_async_results_support_schema_parsing_and_retry_failed(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    await job.wait(progress=True, poll_interval=0)
    parsed_results = await job.results(schema=ParsedMovie)
    retry_job = await job.retry_failed()

    assert parsed_results.successful()[0].response == {"id": "resp_1", "output_text": "The Matrix"}
    retry_requests = (
        (retry_job.storage_dir / "requests.jsonl")
        .read_text(encoding="utf-8")
        .strip()
        .splitlines()
    )
    assert len(retry_requests) == 1


@pytest.mark.asyncio
async def test_async_preview_retry_and_retry_report(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    plan = await job.preview_retry(policy=RetryPolicy.execution_only())
    retry_job = await job.retry_failed(policy=RetryPolicy.execution_only())
    retry_manifest = json.loads(
        (retry_job.storage_dir / "manifest.json").read_text(encoding="utf-8")
    )

    assert plan.summary.selected_rows == 1
    assert plan.summary.selected_by_status == {"failed_execution": 1}
    assert retry_manifest["retry"]["attempt"] == 1
    assert retry_manifest["retry"]["lineage_job_ids"] == [job.id]
    assert retry_manifest["retry"]["policy"]["statuses"] == ["failed_execution"]


@pytest.mark.asyncio
async def test_async_results_raise_when_batch_never_reaches_terminal_state(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()

    async def stuck_retrieve(batch_id: str) -> FakeBatch:
        return sdk.batches.current

    sdk.batches.retrieve = stuck_retrieve  # type: ignore[method-assign]
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(BatchNotReadyError):
        await job.results()


@pytest.mark.asyncio
async def test_async_wait_times_out_when_batch_never_completes(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()

    async def stuck_retrieve(batch_id: str) -> FakeBatch:
        return sdk.batches.current

    sdk.batches.retrieve = stuck_retrieve  # type: ignore[method-assign]
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(TimeoutError, match="Timed out waiting for batch batch-123"):
        await job.wait(timeout=0, poll_interval=0)


@pytest.mark.asyncio
async def test_async_wait_calls_sleep_before_timing_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = AsyncFakeSDK()

    async def stuck_retrieve(batch_id: str) -> FakeBatch:
        return sdk.batches.current

    async def fake_sleep(interval: float) -> None:
        sleep_calls.append(interval)

    sdk.batches.retrieve = stuck_retrieve  # type: ignore[method-assign]
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    monotonic_values = iter([0.0, 0.0, 2.0])
    sleep_calls: list[float] = []

    monkeypatch.setattr("batchkit.jobs.time.monotonic", lambda: next(monotonic_values, 2.0))
    monkeypatch.setattr("batchkit.jobs.asyncio.sleep", fake_sleep)

    with pytest.raises(TimeoutError):
        await job.wait(timeout=1, poll_interval=0.5)

    assert sleep_calls == [0.5]


@pytest.mark.asyncio
async def test_async_cancel_and_refresh_are_noops_without_batch_id(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )
    job._manifest["batch_id"] = None

    assert await job.refresh() is job
    assert await job.cancel() is job


@pytest.mark.asyncio
async def test_async_cancel_surfaces_cancelled_rows(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    results = await (await job.cancel()).results()

    assert [row.status for row in results.rows] == ["cancelled", "cancelled"]
    assert len(results.incomplete()) == 2
    assert results.errors()[0].code == "batch_cancelled"


@pytest.mark.asyncio
async def test_async_retry_failed_raises_public_retry_error(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=b"")
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(RetryUnavailableError):
        await job.retry_failed()


@pytest.mark.asyncio
async def test_async_retry_failed_with_filtered_policy_raises_clear_error(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK(output_payload=_output_rows(), error_payload=_error_rows())
    client = AsyncBatchClient(sdk)
    job = await client.map(
        name="movies",
        items=[{"prompt": "a"}, {"prompt": "b"}],
        model="gpt-4.1-mini",
        build_request=lambda item: {"input": item["prompt"]},
        storage_dir=tmp_path / "job",
    )

    with pytest.raises(
        RetryUnavailableError,
        match="No rows matched retry policy",
    ):
        await job.retry_failed(policy=RetryPolicy(include_error_codes={"api_connection_error"}))


@pytest.mark.asyncio
async def test_async_map_rejects_empty_items(tmp_path: Path) -> None:
    sdk = AsyncFakeSDK()
    client = AsyncBatchClient(sdk)

    with pytest.raises(ValueError, match="items must not be empty"):
        await client.map(
            name="movies",
            items=[],
            model="gpt-4.1-mini",
            build_request=lambda item: {"input": item["prompt"]},
            storage_dir=tmp_path / "job",
        )
