from __future__ import annotations

from typing import Any


class BatchKitError(Exception):
    """Base error for batchkit."""


class DuplicateCustomIDError(BatchKitError):
    """Raised when generated custom IDs are not unique."""


class BatchNotReadyError(BatchKitError):
    """Raised when results are requested before artifacts are ready."""


class RetryUnavailableError(BatchKitError):
    """Raised when no retryable rows exist."""


class BatchError(BatchKitError):
    """Normalized row-level error."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        error_type: str | None = None,
        param: str | None = None,
        line: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.error_type = error_type
        self.param = param
        self.line = line
        self.payload = dict(payload or {})

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any] | None,
        *,
        default_message: str,
        default_code: str | None = None,
    ) -> BatchError:
        normalized_payload = dict(payload or {})
        message = _as_text(normalized_payload.get("message")) or default_message
        code = (
            _as_text(normalized_payload.get("code"))
            or _as_text(normalized_payload.get("type"))
            or default_code
        )
        error_type = _as_text(normalized_payload.get("type")) or code
        param = _as_text(normalized_payload.get("param"))
        line_value = normalized_payload.get("line")
        line = line_value if isinstance(line_value, int) else None
        return cls(
            message,
            code=code,
            error_type=error_type,
            param=param,
            line=line,
            payload=normalized_payload,
        )

    def __str__(self) -> str:
        label = self.code or self.error_type
        if label:
            return f"{label}: {self.message}"
        return self.message


def _as_text(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
