from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .results import BatchResults, BatchRow

DEFAULT_RETRY_STATUSES = frozenset({"failed_execution", "expired", "cancelled", "incomplete"})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    statuses: frozenset[str] = field(default_factory=lambda: DEFAULT_RETRY_STATUSES)
    include_error_codes: frozenset[str] | None = None
    exclude_error_codes: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "statuses", frozenset(self.statuses))
        if self.include_error_codes is not None:
            object.__setattr__(self, "include_error_codes", frozenset(self.include_error_codes))
        object.__setattr__(self, "exclude_error_codes", frozenset(self.exclude_error_codes))

    @classmethod
    def all_retryable(cls) -> RetryPolicy:
        return cls()

    @classmethod
    def execution_only(cls) -> RetryPolicy:
        return cls(statuses=frozenset({"failed_execution"}))

    @classmethod
    def incomplete_only(cls) -> RetryPolicy:
        return cls(statuses=frozenset({"incomplete", "expired", "cancelled"}))

    def to_payload(self) -> dict[str, Any]:
        return {
            "statuses": sorted(self.statuses),
            "include_error_codes": (
                sorted(self.include_error_codes) if self.include_error_codes is not None else None
            ),
            "exclude_error_codes": sorted(self.exclude_error_codes),
        }


@dataclass(slots=True)
class RetryDecision:
    row: BatchRow
    selected: bool
    reason_code: str
    reason: str

    @property
    def custom_id(self) -> str:
        return self.row.custom_id

    @property
    def status(self) -> str:
        return self.row.status

    @property
    def retryable(self) -> bool:
        return self.row.retryable

    @property
    def error_code(self) -> str | None:
        if self.row.error is None:
            return None
        return self.row.error.code

    def to_payload(self) -> dict[str, Any]:
        return {
            "custom_id": self.custom_id,
            "status": self.status,
            "retryable": self.retryable,
            "selected": self.selected,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "error_code": self.error_code,
        }


@dataclass(slots=True)
class RetrySummary:
    total_rows: int
    selected_rows: int
    skipped_rows: int
    selected_by_status: dict[str, int]
    skipped_by_reason: dict[str, int]

    def to_payload(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "selected_rows": self.selected_rows,
            "skipped_rows": self.skipped_rows,
            "selected_by_status": dict(self.selected_by_status),
            "skipped_by_reason": dict(self.skipped_by_reason),
        }


@dataclass(slots=True)
class RetryPlan:
    source_job_id: str
    source_job_name: str
    root_job_id: str
    attempt: int
    lineage_job_ids: list[str]
    policy: RetryPolicy
    decisions: list[RetryDecision]
    _summary: RetrySummary | None = field(default=None, init=False, repr=False)

    @property
    def selected(self) -> list[RetryDecision]:
        return [decision for decision in self.decisions if decision.selected]

    @property
    def skipped(self) -> list[RetryDecision]:
        return [decision for decision in self.decisions if not decision.selected]

    @property
    def selected_rows(self) -> list[BatchRow]:
        return [decision.row for decision in self.selected]

    @property
    def summary(self) -> RetrySummary:
        if self._summary is None:
            selected = self.selected
            skipped = self.skipped
            selected_by_status = Counter(
                decision.status for decision in selected if decision.status
            )
            skipped_by_reason = Counter(decision.reason_code for decision in skipped)
            self._summary = RetrySummary(
                total_rows=len(self.decisions),
                selected_rows=len(selected),
                skipped_rows=len(skipped),
                selected_by_status=dict(sorted(selected_by_status.items())),
                skipped_by_reason=dict(sorted(skipped_by_reason.items())),
            )
        return self._summary

    def to_payload(self) -> dict[str, Any]:
        return {
            "source_job_id": self.source_job_id,
            "source_job_name": self.source_job_name,
            "root_job_id": self.root_job_id,
            "attempt": self.attempt,
            "lineage_job_ids": list(self.lineage_job_ids),
            "policy": self.policy.to_payload(),
            "summary": self.summary.to_payload(),
            "decisions": [decision.to_payload() for decision in self.decisions],
        }


def build_retry_plan(
    *,
    source_job_id: str,
    source_job_name: str,
    lineage_job_ids: list[str],
    results: BatchResults,
    policy: RetryPolicy | None = None,
) -> RetryPlan:
    active_policy = policy or RetryPolicy.all_retryable()
    decisions = [_build_retry_decision(row=row, policy=active_policy) for row in results.rows]
    root_job_id = lineage_job_ids[0] if lineage_job_ids else source_job_id
    return RetryPlan(
        source_job_id=source_job_id,
        source_job_name=source_job_name,
        root_job_id=root_job_id,
        attempt=len(lineage_job_ids),
        lineage_job_ids=list(lineage_job_ids),
        policy=active_policy,
        decisions=decisions,
    )


def _build_retry_decision(*, row: BatchRow, policy: RetryPolicy) -> RetryDecision:
    if not row.retryable:
        return RetryDecision(
            row=row,
            selected=False,
            reason_code="non_retryable",
            reason="Row is marked non-retryable",
        )

    if row.status not in policy.statuses:
        return RetryDecision(
            row=row,
            selected=False,
            reason_code="status_filtered",
            reason=f"Row status '{row.status}' is excluded by the retry policy",
        )

    error_code = row.error.code if row.error is not None else None
    if policy.include_error_codes is not None and error_code not in policy.include_error_codes:
        return RetryDecision(
            row=row,
            selected=False,
            reason_code="error_code_not_included",
            reason="Row error code is not included by the retry policy",
        )

    if error_code is not None and error_code in policy.exclude_error_codes:
        return RetryDecision(
            row=row,
            selected=False,
            reason_code="error_code_excluded",
            reason="Row error code is excluded by the retry policy",
        )

    return RetryDecision(
        row=row,
        selected=True,
        reason_code="selected",
        reason="Row matched the retry policy",
    )
