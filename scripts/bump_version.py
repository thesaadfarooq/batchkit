from __future__ import annotations

import argparse
import re
from pathlib import Path

VERSION_PATTERN = re.compile(r'^(version\s*=\s*")(\d+)\.(\d+)\.(\d+)(")\s*$')


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
    in_project = False
    lines = text.splitlines()

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[project]":
            in_project = True
            continue
        if in_project and stripped.startswith("[") and stripped.endswith("]"):
            break
        if in_project:
            match = VERSION_PATTERN.match(line)
            if match is not None:
                current_version = ".".join(match.groups()[1:4])
                new_version = bump_version(current_version, release_type)
                lines[index] = f'{match.group(1)}{new_version}{match.group(5)}'
                return "\n".join(lines) + "\n", new_version

    raise ValueError("Could not find [project].version in pyproject.toml")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bump the package version in pyproject.toml.")
    parser.add_argument("path", type=Path, help="Path to pyproject.toml")
    parser.add_argument(
        "release_type",
        choices=("patch", "minor", "major"),
        help="Which part of the version to bump",
    )
    args = parser.parse_args()

    original = args.path.read_text(encoding="utf-8")
    updated, version = update_project_version(original, args.release_type)
    args.path.write_text(updated, encoding="utf-8")
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
