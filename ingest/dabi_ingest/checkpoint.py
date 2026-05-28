"""Per-pipeline idempotency checkpoint stored as sqlite at /checkpoints/<pipeline>.sqlite.

Schema (one row per (partition_date, attempt)):
    runs(partition_date TEXT, attempt INT, status TEXT,
         started_at TEXT, completed_at TEXT, output_rows INT, error TEXT,
         PRIMARY KEY (partition_date, attempt))

Contract:
    1. On entry, mark status='running' for (date, max(attempt)+1).
    2. If a prior row for (date, *) already exists with status='ok', skip.
    3. On success: status='ok', completed_at=now, output_rows=<count>.
    4. On failure: status='failed', error=<reason>.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

CHECKPOINT_ROOT = Path("/checkpoints")


def _conn(pipeline: str) -> sqlite3.Connection:
    CHECKPOINT_ROOT.mkdir(parents=True, exist_ok=True)
    path = CHECKPOINT_ROOT / f"{pipeline}.sqlite"
    cx = sqlite3.connect(str(path))
    cx.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            partition_date TEXT NOT NULL,
            attempt        INTEGER NOT NULL,
            status         TEXT NOT NULL,
            started_at     TEXT NOT NULL,
            completed_at   TEXT,
            output_rows    INTEGER,
            error          TEXT,
            PRIMARY KEY (partition_date, attempt)
        )
    """)
    return cx


def is_done(pipeline: str, partition_date: str) -> bool:
    with _conn(pipeline) as cx:
        cur = cx.execute(
            "SELECT 1 FROM runs WHERE partition_date=? AND status='ok' LIMIT 1",
            (partition_date,),
        )
        return cur.fetchone() is not None


@contextmanager
def run(pipeline: str, partition_date: str):
    """Yield a logger-friendly run context; auto-marks status on exit.

        with checkpoint.run("czds", "2026-05-14") as cp:
            ... do work ...
            cp.set_rows(15_234)
    """
    started = datetime.now(UTC).isoformat()
    with _conn(pipeline) as cx:
        cur = cx.execute(
            "SELECT COALESCE(MAX(attempt),0)+1 FROM runs WHERE partition_date=?",
            (partition_date,),
        )
        attempt = int(cur.fetchone()[0])
        cx.execute(
            "INSERT INTO runs (partition_date, attempt, status, started_at) "
            "VALUES (?, ?, 'running', ?)",
            (partition_date, attempt, started),
        )
        cx.commit()

    class _CP:
        def __init__(self):
            self.output_rows: int = 0

        def set_rows(self, n: int) -> None:
            self.output_rows = int(n)

    cp = _CP()
    try:
        yield cp
    except Exception as exc:
        with _conn(pipeline) as cx:
            cx.execute(
                "UPDATE runs SET status='failed', error=?, completed_at=? "
                "WHERE partition_date=? AND attempt=?",
                (repr(exc)[:500], datetime.now(UTC).isoformat(),
                 partition_date, attempt),
            )
            cx.commit()
        raise
    else:
        with _conn(pipeline) as cx:
            cx.execute(
                "UPDATE runs SET status='ok', completed_at=?, output_rows=? "
                "WHERE partition_date=? AND attempt=?",
                (datetime.now(UTC).isoformat(),
                 cp.output_rows, partition_date, attempt),
            )
            cx.commit()
