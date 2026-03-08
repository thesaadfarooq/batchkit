from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, cast

from .errors import BatchError, BatchNotReadyError, RetryUnavailableError
from .manifests import read_json, read_jsonl, utc_now_iso, write_json
from .results import BatchResults, build_results

TERMINAL_STATUSES = {"completed", "failed", "expired", "cancelled"}


class BatchJob:
    def __init__(self, client: Any, manifest_file: Path) -> None:
        self._client = client
        self._manifest_file = manifest_file
        self._manifest = read_json(manifest_file)

    @property
    def id(self) -> str:
        return str(self._manifest["local_job_id"])

    @property
    def name(self) -> str:
        return str(self._manifest["name"])

    @property
    def status(self) -> str:
        return str(self._manifest["status"])

    @property
    def batch_id(self) -> str | None:
        return self._manifest.get("batch_id")

    @property
    def input_file_id(self) -> str | None:
        return self._manifest.get("input_file_id")

    @property
    def output_file_id(self) -> str | None:
        return self._manifest.get("output_file_id")

    @property
    def error_file_id(self) -> str | None:
        return self._manifest.get("error_file_id")

    @property
    def storage_dir(self) -> Path:
        return self._manifest_file.parent

    def refresh(self) -> BatchJob:
        if not self.batch_id:
            return self
        remote = self._client.provider.get_batch(self.batch_id)
        self._update_from_remote(remote)
        return self

    def wait(
        self,
        timeout: float | None = None,
        poll_interval: float = 15.0,
        progress: bool = False,
    ) -> BatchResults:
        started = time.monotonic()
        last_status = self.status

        while True:
            self.refresh()
            if progress and self.status != last_status:
                print(f"[batchkit] {self.name}: {last_status} -> {self.status}")
                last_status = self.status
            if self.status in TERMINAL_STATUSES:
                return self.results()
            if timeout is not None and (time.monotonic() - started) >= timeout:
                raise TimeoutError(f"Timed out waiting for batch {self.batch_id}")
            time.sleep(poll_interval)

    def results(self, schema: Any | None = None) -> BatchResults:
        if self.status not in TERMINAL_STATUSES:
            self.refresh()
        if self.status not in TERMINAL_STATUSES:
            raise BatchNotReadyError(f"Batch {self.batch_id} is not in a terminal state")

        request_index = read_json(self.storage_dir / "request_index.json")["requests"]
        output_rows = self._load_artifact_rows("output_file_id", self.storage_dir / "output.jsonl")
        error_rows = self._load_artifact_rows("error_file_id", self.storage_dir / "errors.jsonl")
        results = build_results(
            job=self,
            request_index=request_index,
            output_rows=output_rows,
            error_rows=error_rows,
        )
        write_json(
            self.storage_dir / "results.json",
            {
                "rows": [
                    {
                        "custom_id": row.custom_id,
                        "ok": row.ok,
                        "status": row.status,
                        "retryable": row.retryable,
                    }
                    for row in results.rows
                ]
            },
        )
        if schema is not None:
            results = self._apply_schema(results, schema)
        return results

    def retry_failed(self, name: str | None = None) -> BatchJob:
        results = self.results()
        retry_records = [row for row in results.rows if row.retryable]
        if not retry_records:
            raise RetryUnavailableError("No retryable rows found")
        retry_name = name or f"{self.name}-retry"
        request_rows = [row.request_line for row in retry_records]
        request_index = [
            {
                "index": index,
                "custom_id": row.custom_id,
                "request": row.request,
                "request_line": row.request_line,
                "source_item": row.source_item,
            }
            for index, row in enumerate(retry_records)
        ]
        return cast(
            BatchJob,
            self._client._submit_request_rows(
                name=retry_name,
                request_rows=request_rows,
                request_index=request_index,
                metadata={"retry_of": self.id},
                parent_job_id=self.id,
                storage_root=self.storage_dir.parent,
            ),
        )

    def cancel(self) -> BatchJob:
        if not self.batch_id:
            return self
        remote = self._client.provider.cancel_batch(self.batch_id)
        self._update_from_remote(remote)
        return self

    def _update_from_remote(self, remote: Any) -> None:
        self._manifest["batch_id"] = remote.id
        self._manifest["status"] = remote.status
        self._manifest["input_file_id"] = remote.input_file_id
        self._manifest["output_file_id"] = remote.output_file_id
        self._manifest["error_file_id"] = remote.error_file_id
        self._manifest["request_counts"] = remote.request_counts
        self._manifest["updated_at"] = utc_now_iso()
        write_json(self._manifest_file, self._manifest)

    def _load_artifact_rows(self, manifest_key: str, artifact_path: Path) -> list[dict[str, Any]]:
        file_id = self._manifest.get(manifest_key)
        if not file_id:
            return []
        if not artifact_path.exists():
            artifact_path.write_bytes(self._client.provider.download_file(file_id))
        return read_jsonl(artifact_path)

    def _apply_schema(self, results: BatchResults, schema: Any) -> BatchResults:
        try:
            from pydantic import ValidationError
        except ImportError as exc:
            raise RuntimeError(
                "Schema parsing requires the optional 'pydantic' dependency"
            ) from exc

        rows = []
        for row in results.rows:
            if row.ok and row.response is not None:
                try:
                    payload = row.response.get("body", row.response)
                    row.response = schema.model_validate(payload).model_dump()
                except ValidationError as exc:
                    row.ok = False
                    row.status = "failed_validation"
                    row.retryable = False
                    row.error = BatchError(
                        "Schema validation failed",
                        code="schema_validation_error",
                        payload={"errors": exc.errors()},
                    )
            rows.append(row)
        return BatchResults(job=results.job, rows=rows)


class AsyncBatchJob(BatchJob):
    async def refresh(self) -> AsyncBatchJob:  # type: ignore[override]
        if not self.batch_id:
            return self
        remote = await self._client.provider.get_batch(self.batch_id)
        self._update_from_remote(remote)
        return self

    async def wait(  # type: ignore[override]
        self,
        timeout: float | None = None,
        poll_interval: float = 15.0,
        progress: bool = False,
    ) -> BatchResults:
        started = time.monotonic()
        last_status = self.status

        while True:
            await self.refresh()
            if progress and self.status != last_status:
                print(f"[batchkit] {self.name}: {last_status} -> {self.status}")
                last_status = self.status
            if self.status in TERMINAL_STATUSES:
                return await self.results()
            if timeout is not None and (time.monotonic() - started) >= timeout:
                raise TimeoutError(f"Timed out waiting for batch {self.batch_id}")
            await asyncio.sleep(poll_interval)

    async def results(self, schema: Any | None = None) -> BatchResults:  # type: ignore[override]
        if self.status not in TERMINAL_STATUSES:
            await self.refresh()
        if self.status not in TERMINAL_STATUSES:
            raise BatchNotReadyError(f"Batch {self.batch_id} is not in a terminal state")

        request_index = read_json(self.storage_dir / "request_index.json")["requests"]
        output_rows = await self._load_artifact_rows(
            "output_file_id",
            self.storage_dir / "output.jsonl",
        )
        error_rows = await self._load_artifact_rows(
            "error_file_id",
            self.storage_dir / "errors.jsonl",
        )
        results = build_results(
            job=self,
            request_index=request_index,
            output_rows=output_rows,
            error_rows=error_rows,
        )
        write_json(
            self.storage_dir / "results.json",
            {
                "rows": [
                    {
                        "custom_id": row.custom_id,
                        "ok": row.ok,
                        "status": row.status,
                        "retryable": row.retryable,
                    }
                    for row in results.rows
                ]
            },
        )
        if schema is not None:
            results = self._apply_schema(results, schema)
        return results

    async def retry_failed(self, name: str | None = None) -> AsyncBatchJob:  # type: ignore[override]
        results = await self.results()
        retry_records = [row for row in results.rows if row.retryable]
        if not retry_records:
            raise RetryUnavailableError("No retryable rows found")
        retry_name = name or f"{self.name}-retry"
        request_rows = [row.request_line for row in retry_records]
        request_index = [
            {
                "index": index,
                "custom_id": row.custom_id,
                "request": row.request,
                "request_line": row.request_line,
                "source_item": row.source_item,
            }
            for index, row in enumerate(retry_records)
        ]
        return cast(
            AsyncBatchJob,
            await self._client._submit_request_rows(
                name=retry_name,
                request_rows=request_rows,
                request_index=request_index,
                metadata={"retry_of": self.id},
                parent_job_id=self.id,
                storage_root=self.storage_dir.parent,
            ),
        )

    async def cancel(self) -> AsyncBatchJob:  # type: ignore[override]
        if not self.batch_id:
            return self
        remote = await self._client.provider.cancel_batch(self.batch_id)
        self._update_from_remote(remote)
        return self

    async def _load_artifact_rows(  # type: ignore[override]
        self,
        manifest_key: str,
        artifact_path: Path,
    ) -> list[dict[str, Any]]:
        file_id = self._manifest.get(manifest_key)
        if not file_id:
            return []
        if not artifact_path.exists():
            artifact_path.write_bytes(await self._client.provider.download_file(file_id))
        return read_jsonl(artifact_path)
