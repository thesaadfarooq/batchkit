from __future__ import annotations

from batchkit import BatchError, BatchResults, BatchRow


def test_result_helpers_cover_aliases_lookup_and_ordering() -> None:
    success = BatchRow(
        custom_id="b",
        ok=True,
        status="succeeded",
        retryable=False,
        request={},
        request_line={},
        response={"body": {"value": 1}},
        error=None,
        source_item=None,
        order=2,
    )
    failure = BatchRow(
        custom_id="a",
        ok=False,
        status="failed_execution",
        retryable=True,
        request={},
        request_line={},
        response=None,
        error=BatchError("boom", code="rate_limit"),
        source_item=None,
        order=1,
    )
    raw_response = BatchRow(
        custom_id="c",
        ok=True,
        status="succeeded",
        retryable=False,
        request={},
        request_line={},
        response={"id": "resp_1"},
        error=None,
        source_item=None,
        order=3,
    )
    results = BatchResults(job=object(), rows=[success, failure, raw_response])  # type: ignore[arg-type]

    assert success.failed is False
    assert failure.failed is True
    assert success.response_body == {"value": 1}
    assert failure.response_body is None
    assert raw_response.response_body == {"id": "resp_1"}
    assert results.successes() == results.successful()
    assert results.failures() == results.failed()
    assert results.retryable() == [failure]
    assert results.retryables() == [failure]
    assert results.by_custom_id() == {"b": success, "a": failure, "c": raw_response}
    assert [row.custom_id for row in results.ordered().rows] == ["a", "b", "c"]
