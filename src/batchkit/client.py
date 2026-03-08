from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, TypeVar

from .errors import DuplicateCustomIDError
from .jobs import BatchJob
from .manifests import (
    create_job_dir,
    generate_local_job_id,
    manifest_path,
    serialize_source_item,
    utc_now_iso,
    write_json,
    write_jsonl,
)
from .openai_provider import OpenAIProvider

T = TypeVar("T")


class BatchClient:
    def __init__(self, sdk: Any, *, storage_root: str | Path | None = None) -> None:
        self.provider = OpenAIProvider(sdk)
        self.storage_root = Path(storage_root) if storage_root is not None else None

    def map(
        self,
        *,
        name: str,
        items: Iterable[T],
        model: str,
        build_request: Callable[[T], dict[str, Any]],
        custom_id: Callable[[T], str] | None = None,
        metadata: dict[str, str] | None = None,
        storage_dir: str | Path | None = None,
    ) -> BatchJob:
        request_rows: list[dict[str, Any]] = []
        request_index: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for index, item in enumerate(items):
            row_custom_id = custom_id(item) if custom_id is not None else f"{name}-{index}"
            if row_custom_id in seen_ids:
                raise DuplicateCustomIDError(f"Duplicate custom_id generated: {row_custom_id}")
            seen_ids.add(row_custom_id)

            body = dict(build_request(item))
            body["model"] = model
            request_line = {
                "custom_id": row_custom_id,
                "method": "POST",
                "url": self.provider.endpoint,
                "body": body,
            }
            request_rows.append(request_line)
            request_index.append(
                {
                    "index": index,
                    "custom_id": row_custom_id,
                    "request": body,
                    "request_line": request_line,
                    "source_item": serialize_source_item(item),
                }
            )

        return self._submit_request_rows(
            name=name,
            request_rows=request_rows,
            request_index=request_index,
            metadata=metadata,
            storage_dir=storage_dir,
        )

    def resume(self, path_or_name: str | Path) -> BatchJob:
        path = Path(path_or_name)
        if path.exists():
            manifest_file = path if path.name == "manifest.json" else manifest_path(path)
            return BatchJob(self, manifest_file)

        root = self.storage_root or Path(".batchkit") / "jobs"
        matches = sorted(root.glob(f"{path_or_name}*/manifest.json"))
        if not matches:
            raise FileNotFoundError(f"Could not find batch job '{path_or_name}'")
        return BatchJob(self, matches[-1])

    def _submit_request_rows(
        self,
        *,
        name: str,
        request_rows: list[dict[str, Any]],
        request_index: list[dict[str, Any]],
        metadata: dict[str, str] | None = None,
        storage_dir: str | Path | None = None,
        parent_job_id: str | None = None,
    ) -> BatchJob:
        job_dir = create_job_dir(
            name,
            storage_dir=storage_dir or (self.storage_root / name if self.storage_root else None),
        )
        requests_file = job_dir / "requests.jsonl"
        write_jsonl(requests_file, request_rows)
        write_json(job_dir / "request_index.json", {"requests": request_index})

        local_job_id = generate_local_job_id()
        file_id = self.provider.upload_batch_file(requests_file)
        remote_batch = self.provider.create_batch(input_file_id=file_id, metadata=metadata)

        manifest = {
            "local_job_id": local_job_id,
            "name": name,
            "provider": "openai",
            "endpoint": "responses",
            "model": request_rows[0]["body"]["model"] if request_rows else None,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "status": remote_batch.status,
            "batch_id": remote_batch.id,
            "input_file_id": remote_batch.input_file_id or file_id,
            "output_file_id": remote_batch.output_file_id,
            "error_file_id": remote_batch.error_file_id,
            "request_counts": remote_batch.request_counts,
            "paths": {
                "requests": str(requests_file),
                "request_index": str(job_dir / "request_index.json"),
                "output": str(job_dir / "output.jsonl"),
                "errors": str(job_dir / "errors.jsonl"),
                "results": str(job_dir / "results.json"),
            },
            "parent_job_id": parent_job_id,
        }
        manifest_file = manifest_path(job_dir)
        write_json(manifest_file, manifest)
        return BatchJob(self, manifest_file)
