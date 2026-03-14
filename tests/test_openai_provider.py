from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from batchkit.openai_provider import AsyncOpenAIProvider, OpenAIProvider, _coerce_batch


class BytesContentResponse:
    def __init__(self, payload: bytes) -> None:
        self.content = payload


class ReadContentResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class AsyncReadContentResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def read(self) -> bytes:
        return self._payload


class AsyncCallableReadContentResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self):
        return lambda: self._payload


class FilesAPI:
    def __init__(self, created: object, content: object) -> None:
        self._created = created
        self._content = content

    def create(self, *, file, purpose: str):  # noqa: ANN001
        return self._created

    def content(self, file_id: str):  # noqa: ANN001
        return self._content


class AsyncFilesAPI(FilesAPI):
    async def create(self, *, file, purpose: str):  # type: ignore[override]  # noqa: ANN001
        return self._created

    async def content(self, file_id: str):  # type: ignore[override]  # noqa: ANN001
        return self._content


@dataclass
class RequestCountsObject:
    total: int
    completed: int
    failed: int


@dataclass
class BatchObject:
    id: str
    status: str
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    request_counts: object | None = None


class SDK:
    def __init__(self, files: object) -> None:
        self.files = files


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (b"payload", b"payload"),
        (BytesContentResponse(b"payload"), b"payload"),
        (ReadContentResponse(b"payload"), b"payload"),
    ],
)
def test_sync_provider_download_file_supports_common_response_shapes(
    tmp_path: Path,
    response: object,
    expected: bytes,
) -> None:
    request_file = tmp_path / "request.jsonl"
    request_file.write_text("{}\n", encoding="utf-8")
    provider = OpenAIProvider(SDK(FilesAPI({"id": "file-123"}, response)))

    assert provider.upload_batch_file(request_file) == "file-123"
    assert provider.download_file("file-123") == expected


def test_sync_provider_upload_and_download_raise_for_invalid_sdk_shapes(tmp_path: Path) -> None:
    request_file = tmp_path / "request.jsonl"
    request_file.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="SDK did not return a file id"):
        OpenAIProvider(SDK(FilesAPI({}, b"payload"))).upload_batch_file(request_file)

    with pytest.raises(TypeError, match="Unsupported file content response from SDK"):
        OpenAIProvider(SDK(FilesAPI({"id": "file-123"}, object()))).download_file("file-123")


@pytest.mark.asyncio
async def test_async_provider_download_file_supports_common_response_shapes(tmp_path: Path) -> None:
    request_file = tmp_path / "request.jsonl"
    request_file.write_text("{}\n", encoding="utf-8")

    provider = AsyncOpenAIProvider(SDK(AsyncFilesAPI({"id": "file-123"}, b"payload")))
    assert await provider.upload_batch_file(request_file) == "file-123"
    assert await provider.download_file("file-123") == b"payload"

    provider = AsyncOpenAIProvider(
        SDK(AsyncFilesAPI({"id": "file-123"}, BytesContentResponse(b"payload")))
    )
    assert await provider.download_file("file-123") == b"payload"

    provider = AsyncOpenAIProvider(
        SDK(AsyncFilesAPI({"id": "file-123"}, AsyncReadContentResponse(b"payload")))
    )
    assert await provider.download_file("file-123") == b"payload"

    provider = AsyncOpenAIProvider(
        SDK(AsyncFilesAPI({"id": "file-123"}, AsyncCallableReadContentResponse(b"payload")))
    )
    assert await provider.download_file("file-123") == b"payload"


@pytest.mark.asyncio
async def test_async_provider_upload_and_download_raise_for_invalid_sdk_shapes(
    tmp_path: Path,
) -> None:
    request_file = tmp_path / "request.jsonl"
    request_file.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="SDK did not return a file id"):
        await AsyncOpenAIProvider(
            SDK(AsyncFilesAPI({}, b"payload"))
        ).upload_batch_file(request_file)

    with pytest.raises(TypeError, match="Unsupported file content response from SDK"):
        await AsyncOpenAIProvider(SDK(AsyncFilesAPI({"id": "file-123"}, object()))).download_file(
            "file-123"
        )


def test_coerce_batch_handles_dicts_objects_and_missing_request_counts() -> None:
    dict_batch = _coerce_batch(
        {
            "id": "batch-1",
            "status": "completed",
            "input_file_id": "input-1",
            "output_file_id": "output-1",
            "error_file_id": "error-1",
            "request_counts": {"total": "2", "completed": 1, "failed": 1},
        }
    )
    object_batch = _coerce_batch(
        BatchObject(
            id="batch-2",
            status="failed",
            request_counts=RequestCountsObject(total=3, completed=2, failed=1),
        )
    )
    empty_batch = _coerce_batch(BatchObject(id="batch-3", status="in_progress"))

    assert dict_batch.request_counts == {"total": 2, "completed": 1, "failed": 1}
    assert dict_batch.raw is not None
    assert object_batch.request_counts == {"total": 3, "completed": 2, "failed": 1}
    assert object_batch.raw is None
    assert empty_batch.request_counts == {}
