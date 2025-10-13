#!/usr/bin/env python3
"""Synchronise project metadata files with the VCS-derived version."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
MESON_BUILD = ROOT / "meson.build"

sys.path.insert(0, str(ROOT / "scripts"))
from get_version import detect_version, FALLBACK_VERSION  # noqa: E402


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument(
        "--version",
        help="Override the detected version (leading 'v' is stripped automatically).",
    )
    return parser.parse_args(argv)


def _normalise(version: str) -> str:
    return version[1:] if version.startswith("v") else version


def _replace_once(path: Path, pattern: str, repl: str) -> None:
    regex = re.compile(pattern, flags=re.MULTILINE)
    original = path.read_text()
    updated, count = regex.subn(repl, original)
    if count != 1:
        raise RuntimeError(f"Could not update version in {path}: expected 1 match, got {count}")
    path.write_text(updated)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    if args.version:
        version = _normalise(args.version)
    else:
        version = detect_version()
        if version == FALLBACK_VERSION:
            print(
                "[sync_version] Warning: using fallback version. Provide --version or create tags.",
                file=sys.stderr,
            )

    _replace_once(
        PYPROJECT,
        r'^(version\s*=\s*)"[^"]+"',
        rf'\1"{version}"',
    )
    _replace_once(
        MESON_BUILD,
        r"^(\s*version:\s*)'[^']+'",
        rf"\1'{version}'",
    )
    print(f"Updated project version to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
