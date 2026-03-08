from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RemoteBatch:
    id: str
    status: str
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    request_counts: dict[str, int] = field(default_factory=dict)
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class BatchResultCounts:
    total: int
    succeeded: int
    failed: int
    retryable: int
