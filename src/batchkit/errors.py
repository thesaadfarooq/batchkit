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
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.payload = payload or {}

    def __str__(self) -> str:
        if self.code:
            return f"{self.code}: {self.message}"
        return self.message
