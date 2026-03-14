from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, cast

from .errors import BatchError, BatchNotReadyError, RetryUnavailableError
from .manifests import read_json, read_jsonl, utc_now_iso, write_json
from .results import BatchResults, build_results
from .retry import RetryPlan, RetryPolicy, build_retry_plan

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

    def retry_failed(
        self,
        name: str | None = None,
        *,
        policy: RetryPolicy | None = None,
    ) -> BatchJob:
        return self.retry_failed_with_policy(name=name, policy=policy)

    def preview_retry(self, policy: RetryPolicy | None = None) -> RetryPlan:
        results = self.results()
        lineage_job_ids = self._retry_lineage_for_child()
        return build_retry_plan(
            source_job_id=self.id,
            source_job_name=self.name,
            lineage_job_ids=lineage_job_ids,
            results=results,
            policy=policy,
        )

    def retry_failed_with_policy(
        self,
        *,
        name: str | None = None,
        policy: RetryPolicy | None = None,
    ) -> BatchJob:
        plan = self.preview_retry(policy=policy)
        retry_records = plan.selected_rows
        if not retry_records:
            skipped = ", ".join(
                f"{reason}={count}" for reason, count in plan.summary.skipped_by_reason.items()
            )
            detail = f" ({skipped})" if skipped else ""
            raise RetryUnavailableError(f"No rows matched retry policy{detail}")
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
        return self._finalize_retry_job(
            plan=plan,
            child=cast(
                BatchJob,
                self._client._submit_request_rows(
                    name=retry_name,
                    request_rows=request_rows,
                    request_index=request_index,
                    metadata=self._retry_metadata(plan),
                    parent_job_id=self.id,
                    storage_root=self.storage_dir.parent,
                ),
            ),
        )

    def _retry_metadata(self, plan: RetryPlan) -> dict[str, str]:
        return {
            "retry_of": self.id,
            "retry_root": plan.root_job_id,
            "retry_attempt": str(plan.attempt),
            "retry_selected": str(plan.summary.selected_rows),
        }

    def _retry_lineage_for_child(self) -> list[str]:
        retry_info = self._manifest.get("retry")
        if not isinstance(retry_info, dict):
            return [self.id]
        lineage = retry_info.get("lineage_job_ids")
        if not isinstance(lineage, list):
            return [self.id]
        lineage_ids = [value for value in lineage if isinstance(value, str)]
        return [*lineage_ids, self.id]

    def _finalize_retry_job(self, *, plan: RetryPlan, child: BatchJob) -> BatchJob:
        report_path = child.storage_dir / "retry_report.json"
        write_json(report_path, plan.to_payload())
        child._manifest["paths"]["retry_report"] = str(report_path)
        child._manifest["retry"] = {
            "source_job_id": self.id,
            "source_job_name": self.name,
            "root_job_id": plan.root_job_id,
            "attempt": plan.attempt,
            "lineage_job_ids": plan.lineage_job_ids,
            "policy": plan.policy.to_payload(),
            "summary": plan.summary.to_payload(),
            "report_path": str(report_path),
        }
        write_json(child._manifest_file, child._manifest)
        return child

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

    async def retry_failed(  # type: ignore[override]
        self,
        name: str | None = None,
        *,
        policy: RetryPolicy | None = None,
    ) -> AsyncBatchJob:
        return await self.retry_failed_with_policy(name=name, policy=policy)

    async def preview_retry(self, policy: RetryPolicy | None = None) -> RetryPlan:  # type: ignore[override]
        results = await self.results()
        lineage_job_ids = self._retry_lineage_for_child()
        return build_retry_plan(
            source_job_id=self.id,
            source_job_name=self.name,
            lineage_job_ids=lineage_job_ids,
            results=results,
            policy=policy,
        )

    async def retry_failed_with_policy(  # type: ignore[override]
        self,
        *,
        name: str | None = None,
        policy: RetryPolicy | None = None,
    ) -> AsyncBatchJob:
        plan = await self.preview_retry(policy=policy)
        retry_records = plan.selected_rows
        if not retry_records:
            skipped = ", ".join(
                f"{reason}={count}" for reason, count in plan.summary.skipped_by_reason.items()
            )
            detail = f" ({skipped})" if skipped else ""
            raise RetryUnavailableError(f"No rows matched retry policy{detail}")
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
        child = cast(
            AsyncBatchJob,
            await self._client._submit_request_rows(
                name=retry_name,
                request_rows=request_rows,
                request_index=request_index,
                metadata=self._retry_metadata(plan),
                parent_job_id=self.id,
                storage_root=self.storage_dir.parent,
            ),
        )
        return cast(AsyncBatchJob, self._finalize_retry_job(plan=plan, child=child))

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
