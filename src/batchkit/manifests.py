from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return cleaned or "batch-job"


def default_jobs_root() -> Path:
    return Path(".batchkit") / "jobs"


def create_job_dir(name: str, storage_dir: str | Path | None = None) -> Path:
    if storage_dir is not None:
        job_dir = Path(storage_dir)
    else:
        slug = slugify(name)
        suffix = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        job_dir = default_jobs_root() / f"{slug}-{suffix}"
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return dict(data)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def serialize_source_item(value: Any) -> Any | None:
    try:
        json.dumps(value)
    except TypeError:
        return None
    return value


def manifest_path(job_dir: Path) -> Path:
    return job_dir / "manifest.json"


def generate_local_job_id() -> str:
    return uuid4().hex
