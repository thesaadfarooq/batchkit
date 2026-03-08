from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import BatchError
from .types import BatchResultCounts

if TYPE_CHECKING:
    from .jobs import BatchJob


NON_RETRYABLE_ERROR_CODES = {"invalid_request_error", "validation_error"}
PARTIAL_RESULT_ERROR_CODE = "missing_result_row"


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

    @property
    def failed(self) -> bool:
        return not self.ok

    @property
    def response_body(self) -> dict[str, Any] | None:
        """Return the nested response body when present, otherwise the raw response mapping."""
        if self.response is None:
            return None
        body = self.response.get("body")
        if isinstance(body, dict):
            return body
        return self.response


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

    def successes(self) -> list[BatchRow]:
        return self.successful()

    def failed(self) -> list[BatchRow]:
        return [row for row in self.rows if not row.ok]

    def failures(self) -> list[BatchRow]:
        return self.failed()

    def retryable(self) -> list[BatchRow]:
        return [row for row in self.rows if row.retryable]

    def retryables(self) -> list[BatchRow]:
        return self.retryable()

    def incomplete(self) -> list[BatchRow]:
        """Return rows that did not finish cleanly, including expired or cancelled requests."""
        return [row for row in self.rows if row.status in {"incomplete", "expired", "cancelled"}]

    def errors(self) -> list[BatchError]:
        return [row.error for row in self.rows if row.error is not None]

    def by_custom_id(self) -> dict[str, BatchRow]:
        return {row.custom_id: row for row in self.rows}

    def get(self, custom_id: str) -> BatchRow | None:
        for row in self.rows:
            if row.custom_id == custom_id:
                return row
        return None

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
        error = BatchError.from_payload(payload, default_message="Batch request failed")
        retryable = error.code not in NON_RETRYABLE_ERROR_CODES
        status = "failed_validation" if not retryable else "failed_execution"
        return BatchRow(
            custom_id=record["custom_id"],
            ok=False,
            status=status,
            retryable=retryable,
            request=record["request"],
            request_line=record["request_line"],
            response=None,
            error=error,
            source_item=record.get("source_item"),
            order=record["index"],
        )

    if batch_status == "expired":
        status = "expired"
        retryable = True
        error = BatchError.from_payload(
            {
                "message": "Batch expired before this request completed",
                "code": "batch_expired",
                "batch_status": batch_status,
            },
            default_message="Batch expired before this request completed",
        )
    elif batch_status == "cancelled":
        status = "cancelled"
        retryable = True
        error = BatchError.from_payload(
            {
                "message": "Batch was cancelled before this request completed",
                "code": "batch_cancelled",
                "batch_status": batch_status,
            },
            default_message="Batch was cancelled before this request completed",
        )
    else:
        status = "incomplete"
        retryable = True
        error = BatchError.from_payload(
            {
                "message": (
                    "Batch reached a terminal state without an output or error row for this request"
                ),
                "code": PARTIAL_RESULT_ERROR_CODE,
                "batch_status": batch_status,
            },
            default_message=(
                "Batch reached a terminal state without an output or error row for this request"
            ),
        )

    return BatchRow(
        custom_id=record["custom_id"],
        ok=False,
        status=status,
        retryable=retryable,
        request=record["request"],
        request_line=record["request_line"],
        response=None,
        error=error,
        source_item=record.get("source_item"),
        order=record["index"],
    )
