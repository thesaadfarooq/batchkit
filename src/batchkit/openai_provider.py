from __future__ import annotations

from pathlib import Path
from typing import Any

from .types import RemoteBatch


def _get_value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


class OpenAIProvider:
    """Thin adapter over the official OpenAI SDK."""

    endpoint = "/v1/responses"

    def __init__(self, sdk: Any) -> None:
        self.sdk = sdk

    def upload_batch_file(self, path: Path) -> str:
        with path.open("rb") as handle:
            created = self.sdk.files.create(file=handle, purpose="batch")
        file_id = _get_value(created, "id")
        if not file_id:
            raise ValueError("SDK did not return a file id")
        return str(file_id)

    def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> RemoteBatch:
        batch = self.sdk.batches.create(
            input_file_id=input_file_id,
            endpoint=self.endpoint,
            completion_window="24h",
            metadata=metadata or {},
        )
        return _coerce_batch(batch)

    def get_batch(self, batch_id: str) -> RemoteBatch:
        return _coerce_batch(self.sdk.batches.retrieve(batch_id))

    def cancel_batch(self, batch_id: str) -> RemoteBatch:
        return _coerce_batch(self.sdk.batches.cancel(batch_id))

    def download_file(self, file_id: str) -> bytes:
        response = self.sdk.files.content(file_id)
        if isinstance(response, bytes):
            return response
        if hasattr(response, "content"):
            return bytes(response.content)
        if hasattr(response, "read"):
            return bytes(response.read())
        raise TypeError("Unsupported file content response from SDK")


class AsyncOpenAIProvider:
    endpoint = "/v1/responses"

    def __init__(self, sdk: Any) -> None:
        self.sdk = sdk

    async def upload_batch_file(self, path: Path) -> str:
        with path.open("rb") as handle:
            created = await self.sdk.files.create(file=handle, purpose="batch")
        file_id = _get_value(created, "id")
        if not file_id:
            raise ValueError("SDK did not return a file id")
        return str(file_id)

    async def create_batch(
        self,
        *,
        input_file_id: str,
        metadata: dict[str, str] | None = None,
    ) -> RemoteBatch:
        batch = await self.sdk.batches.create(
            input_file_id=input_file_id,
            endpoint=self.endpoint,
            completion_window="24h",
            metadata=metadata or {},
        )
        return _coerce_batch(batch)

    async def get_batch(self, batch_id: str) -> RemoteBatch:
        return _coerce_batch(await self.sdk.batches.retrieve(batch_id))

    async def cancel_batch(self, batch_id: str) -> RemoteBatch:
        return _coerce_batch(await self.sdk.batches.cancel(batch_id))

    async def download_file(self, file_id: str) -> bytes:
        response = await self.sdk.files.content(file_id)
        if isinstance(response, bytes):
            return response
        if hasattr(response, "content"):
            return bytes(response.content)
        if hasattr(response, "read"):
            data = response.read()
            if callable(data):
                data = data()
            if hasattr(data, "__await__"):
                data = await data
            return bytes(data)
        raise TypeError("Unsupported file content response from SDK")


def _coerce_batch(batch: Any) -> RemoteBatch:
    raw_request_counts = _get_value(batch, "request_counts")
    if isinstance(raw_request_counts, dict):
        request_counts = {key: int(value) for key, value in raw_request_counts.items()}
    elif raw_request_counts is None:
        request_counts = {}
    else:
        request_counts = {}
        for key in ("total", "completed", "failed"):
            if hasattr(raw_request_counts, key):
                value = getattr(raw_request_counts, key)
                if value is not None:
                    request_counts[key] = int(value)

    return RemoteBatch(
        id=str(_get_value(batch, "id")),
        status=str(_get_value(batch, "status")),
        input_file_id=_get_value(batch, "input_file_id"),
        output_file_id=_get_value(batch, "output_file_id"),
        error_file_id=_get_value(batch, "error_file_id"),
        request_counts=request_counts,
        raw=batch if isinstance(batch, dict) else None,
    )
