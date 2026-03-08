from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "bump_version.py"
SPEC = importlib.util.spec_from_file_location("batchkit_bump_version", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
BUMP_VERSION_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUMP_VERSION_MODULE)
update_project_version = BUMP_VERSION_MODULE.update_project_version


def _run_bump(tmp_path: Path, release_type: str) -> tuple[str, str]:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "batchkit-ai"\nversion = "0.1.0"\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(pyproject), release_type],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip(), pyproject.read_text(encoding="utf-8")


def test_patch_release_bumps_patch(tmp_path: Path) -> None:
    version, contents = _run_bump(tmp_path, "patch")
    assert version == "0.1.1"
    assert 'version = "0.1.1"' in contents


def test_minor_release_bumps_minor(tmp_path: Path) -> None:
    version, contents = _run_bump(tmp_path, "minor")
    assert version == "0.2.0"
    assert 'version = "0.2.0"' in contents


def test_major_release_bumps_major(tmp_path: Path) -> None:
    version, contents = _run_bump(tmp_path, "major")
    assert version == "1.0.0"
    assert 'version = "1.0.0"' in contents


def test_preserves_crlf_line_endings(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\r\nname = "batchkit-ai"\r\nversion = "0.1.0"\r\n',
        encoding="utf-8",
        newline="",
    )

    subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(pyproject), "patch"],
        capture_output=True,
        text=True,
        check=True,
    )

    contents = pyproject.read_bytes()
    assert b'\r\nversion = "0.1.1"\r\n' in contents


def test_raises_when_project_section_is_missing() -> None:
    with pytest.raises(ValueError, match=r"Could not find \[project\] in pyproject.toml"):
        update_project_version('version = "0.1.0"\n', "patch")


def test_raises_when_project_version_is_missing() -> None:
    with pytest.raises(ValueError, match=r"Could not find \[project\]\.version in pyproject.toml"):
        update_project_version('[project]\nname = "batchkit-ai"\n', "patch")
