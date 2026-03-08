from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import BatchError
from .types import BatchResultCounts

if TYPE_CHECKING:
    from .jobs import BatchJob


NON_RETRYABLE_ERROR_CODES = {"invalid_request_error", "validation_error"}


@dataclass(slots=True)
class BatchRow:
    custom_id: str
    ok: bool
    status: str
    retryable: bool
    request: dict[str, Any]
    request_line: dict[str, Any]
    response: dict[str, Any] | None
    error: BatchError | None
    source_item: Any | None
    order: int


@dataclass(slots=True)
class BatchResults:
    job: BatchJob
    rows: list[BatchRow]

    @property
    def counts(self) -> BatchResultCounts:
        succeeded = sum(1 for row in self.rows if row.ok)
        failed = len(self.rows) - succeeded
        retryable = sum(1 for row in self.rows if row.retryable)
        return BatchResultCounts(
            total=len(self.rows),
            succeeded=succeeded,
            failed=failed,
            retryable=retryable,
        )

    def successful(self) -> list[BatchRow]:
        return [row for row in self.rows if row.ok]

    def failed(self) -> list[BatchRow]:
        return [row for row in self.rows if not row.ok]

    def retryable(self) -> list[BatchRow]:
        return [row for row in self.rows if row.retryable]

    def by_custom_id(self) -> dict[str, BatchRow]:
        return {row.custom_id: row for row in self.rows}

    def ordered(self) -> BatchResults:
        return BatchResults(job=self.job, rows=sorted(self.rows, key=lambda row: row.order))


def build_results(
    *,
    job: BatchJob,
    request_index: list[dict[str, Any]],
    output_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
) -> BatchResults:
    output_by_id = {row["custom_id"]: row for row in output_rows}
    error_by_id = {row["custom_id"]: row for row in error_rows}
    rows: list[BatchRow] = []
    batch_status = job.status

    for record in request_index:
        custom_id = record["custom_id"]
        output_row = output_by_id.get(custom_id)
        error_row = error_by_id.get(custom_id)
        row = _build_row(
            batch_status=batch_status,
            record=record,
            output_row=output_row,
            error_row=error_row,
        )
        rows.append(row)

    return BatchResults(job=job, rows=rows)


def _build_row(
    *,
    batch_status: str,
    record: dict[str, Any],
    output_row: dict[str, Any] | None,
    error_row: dict[str, Any] | None,
) -> BatchRow:
    if output_row is not None:
        return BatchRow(
            custom_id=record["custom_id"],
            ok=True,
            status="succeeded",
            retryable=False,
            request=record["request"],
            request_line=record["request_line"],
            response=output_row.get("response"),
            error=None,
            source_item=record.get("source_item"),
            order=record["index"],
        )

    if error_row is not None:
        payload = error_row.get("error") or {}
        message = payload.get("message", "Batch request failed")
        code = payload.get("code") or payload.get("type")
        retryable = code not in NON_RETRYABLE_ERROR_CODES
        status = "failed_validation" if not retryable else "failed_execution"
        return BatchRow(
            custom_id=record["custom_id"],
            ok=False,
            status=status,
            retryable=retryable,
            request=record["request"],
            request_line=record["request_line"],
            response=None,
            error=BatchError(message, code=code, payload=payload),
            source_item=record.get("source_item"),
            order=record["index"],
        )

    if batch_status == "expired":
        status = "expired"
        retryable = True
    elif batch_status == "cancelled":
        status = "cancelled"
        retryable = True
    else:
        status = "failed_validation"
        retryable = False

    return BatchRow(
        custom_id=record["custom_id"],
        ok=False,
        status=status,
        retryable=retryable,
        request=record["request"],
        request_line=record["request_line"],
        response=None,
        error=None,
        source_item=record.get("source_item"),
        order=record["index"],
    )
