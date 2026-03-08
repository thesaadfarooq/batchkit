from __future__ import annotations

import argparse
import re
from pathlib import Path

PROJECT_SECTION_PATTERN = re.compile(
    r"(?ms)^\[project\]\s*(?:\r?\n)(?P<body>.*?)(?=^\[|\Z)"
)
VERSION_PATTERN = re.compile(
    r'^(?P<prefix>\s*version\s*=\s*")'
    r'(?P<version>\d+\.\d+\.\d+)'
    r'(?P<suffix>")'
    r'(?P<trailing>[ \t]*)'
    r'(?P<line_ending>\r?\n|$)',
    re.MULTILINE,
)


def bump_version(version: str, release_type: str) -> str:
    major, minor, patch = (int(part) for part in version.split("."))
    if release_type == "patch":
        patch += 1
    elif release_type == "minor":
        minor += 1
        patch = 0
    elif release_type == "major":
        major += 1
        minor = 0
        patch = 0
    else:
        raise ValueError(f"Unsupported release type: {release_type}")
    return f"{major}.{minor}.{patch}"


def update_project_version(text: str, release_type: str) -> tuple[str, str]:
    project_match = PROJECT_SECTION_PATTERN.search(text)
    if project_match is None:
        raise ValueError("Could not find [project] in pyproject.toml")

    body = project_match.group("body")
    version_match = VERSION_PATTERN.search(body)
    if version_match is None:
        raise ValueError("Could not find [project].version in pyproject.toml")

    current_version = version_match.group("version")
    new_version = bump_version(current_version, release_type)
    updated_body = VERSION_PATTERN.sub(
        rf"\g<prefix>{new_version}\g<suffix>\g<trailing>\g<line_ending>",
        body,
        count=1,
    )
    updated_text = (
        text[: project_match.start("body")]
        + updated_body
        + text[project_match.end("body") :]
    )
    return updated_text, new_version


def main() -> int:
    parser = argparse.ArgumentParser(description="Bump the package version in pyproject.toml.")
    parser.add_argument("path", type=Path, help="Path to pyproject.toml")
    parser.add_argument(
        "release_type",
        choices=("patch", "minor", "major"),
        help="Which part of the version to bump",
    )
    args = parser.parse_args()

    with args.path.open("r", encoding="utf-8", newline="") as file:
        original = file.read()
    updated, version = update_project_version(original, args.release_type)
    with args.path.open("w", encoding="utf-8", newline="") as file:
        file.write(updated)
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
