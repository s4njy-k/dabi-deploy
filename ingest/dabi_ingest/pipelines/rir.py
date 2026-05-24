"""RIR-level data from rir-data.org — WHOIS + reverse-DNS zones; bzip2 JSON Lines; populates CH dabi.rir_whois + OS dabi-rir-whois cache.

TODO: implement fetch → transform → load.
The skeleton below establishes the argparse contract, logging, checkpoint use,
and exit code semantics so systemd timers can be wired immediately while the
real logic is filled in.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import structlog

from dabi_ingest import checkpoint

PIPELINE = "rir"
DESCRIPTION = "RIR-level data from rir-data.org — WHOIS + reverse-DNS zones; bzip2 JSON Lines; populates CH dabi.rir_whois + OS dabi-rir-whois cache."

log = structlog.get_logger(PIPELINE)


def _default_partition_date() -> str:
    """Default to yesterday (UTC); most OpenINTEL/CZDS publish previous-day data."""
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--partition-date",
        default=_default_partition_date(),
        help="ISO date (YYYY-MM-DD) of the partition to ingest. Default: yesterday UTC.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore checkpoint and re-run even if already marked done.",
    )


def run(args: argparse.Namespace) -> int:
    log_ = log.bind(partition_date=args.partition_date, force=args.force)

    if not args.force and checkpoint.is_done(PIPELINE, args.partition_date):
        log_.info("checkpoint.skip", reason="already done")
        return 0

    with checkpoint.run(PIPELINE, args.partition_date) as cp:
        log_.info("step.fetch", todo=True)
        # TODO: fetch source data into /scratch (Local SSD)

        log_.info("step.transform", todo=True)
        # TODO: DuckDB / Python transformation

        log_.info("step.load", todo=True)
        # TODO: bulk-load into OpenSearch (api host) or ClickHouse (analytics host)
        rows_loaded = 0

        cp.set_rows(rows_loaded)
        log_.info("done", rows_loaded=rows_loaded)

    return 0
