from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from batchkit import AsyncBatchClient, BatchClient, DuplicateCustomIDError, RetryUnavailableError


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


def _output_rows() -> bytes:
    row = {
        "custom_id": "movies-0",
        "response": {"body": {"id": "resp_1", "output_text": "The Matrix"}},
    }
    return (json.dumps(row) + "\n").encode("utf-8")


def _error_rows() -> bytes:
    row = {
        "custom_id": "movies-1",
        "error": {"message": "rate limited", "code": "rate_limit_exceeded"},
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

    assert results.counts.total == 2
    assert results.counts.succeeded == 1
    assert results.counts.retryable == 1

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
