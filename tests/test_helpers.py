from __future__ import annotations

from pathlib import Path

from batchkit.errors import BatchError
from batchkit.manifests import (
    default_jobs_root,
    generate_local_job_id,
    manifest_path,
    read_jsonl,
    serialize_source_item,
    slugify,
)


class Unserializable:
    pass


def test_batch_error_from_payload_and_string_representations() -> None:
    error = BatchError.from_payload(
        {"message": "boom", "type": "rate_limit", "param": "input", "line": 3},
        default_message="default",
        default_code="fallback",
    )
    fallback = BatchError.from_payload({}, default_message="default")

    assert error.code == "rate_limit"
    assert error.error_type == "rate_limit"
    assert error.param == "input"
    assert error.line == 3
    assert str(error) == "rate_limit: boom"
    assert str(fallback) == "default"


def test_manifest_helpers_cover_default_paths_and_unserializable_inputs(tmp_path: Path) -> None:
    missing_rows = read_jsonl(tmp_path / "missing.jsonl")
    local_job_id = generate_local_job_id()

    assert slugify("  Batch Job !!! ") == "batch-job"
    assert slugify("$$$") == "batch-job"
    assert default_jobs_root() == Path(".batchkit") / "jobs"
    assert missing_rows == []
    assert serialize_source_item({"ok": True}) == {"ok": True}
    assert serialize_source_item(Unserializable()) is None
    assert manifest_path(tmp_path) == tmp_path / "manifest.json"
    assert len(local_job_id) == 32
