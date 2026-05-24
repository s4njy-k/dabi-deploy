"""Shared secret-reading helper. /run/secrets/* is populated by scripts/pull-secrets.sh."""
from __future__ import annotations

from pathlib import Path

SECRET_DIR = Path("/run/secrets")


def get(name: str) -> str:
    """Read a secret value. Raises FileNotFoundError if not present."""
    return (SECRET_DIR / name).read_text().strip()


def try_get(name: str) -> str | None:
    """Return secret if present, None otherwise."""
    try:
        return get(name)
    except FileNotFoundError:
        return None
