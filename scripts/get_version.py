#!/usr/bin/env python3
"""Return the project version derived from Git tags via setuptools-scm.

The script prints the detected version to stdout so it can be reused in
Meson configuration, release tooling, or other automation.
"""

from __future__ import annotations

from pathlib import Path
import sys

FALLBACK_VERSION = "0.0.0"
REPO_ROOT = Path(__file__).resolve().parents[1]


def detect_version() -> str:
    """Return the version from VCS tags, falling back if unavailable."""
    try:
        from setuptools_scm import get_version
    except ModuleNotFoundError:
        return FALLBACK_VERSION

    try:
        return get_version(root=str(REPO_ROOT), fallback_version=FALLBACK_VERSION)
    except Exception as exc:  # pragma: no cover - best effort logging for tooling
        print(f"[get_version] falling back to {FALLBACK_VERSION}: {exc}", file=sys.stderr)
        return FALLBACK_VERSION


def main() -> int:
    print(detect_version())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
