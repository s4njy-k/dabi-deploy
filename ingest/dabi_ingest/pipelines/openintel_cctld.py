"""OpenINTEL ccTLD apex domain lists — fetched from OpenINTEL public object storage.

Source:      https://object.openintel.nl/seeseetld/lists/
Format:      gzip-compressed CSV, one apex domain per line, no header
Attribution: OpenINTEL (University of Twente) — CC BY-NC-SA 4.0
             https://openintel.nl/

URL template:
  {BASE}/tld={tld}/year={YYYY}/month={MM}/day={DD}/
  ccTLD-domain-names-list.{tld}.{YYYY-MM-DD}.csv.gz

Output: dabi.openintel_cctld (ReplacingMergeTree, partitioned by tld)
Timer:  weekly Monday 23:00 UTC via dabi-ingest-openintel-cctld.timer
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import io
from datetime import timezone

import requests
import structlog

from dabi_ingest import checkpoint, clients

PIPELINE = "openintel-cctld"
DESCRIPTION = "OpenINTEL ccTLD apex lists — top 10 ccTLDs, ~30M domains/week."

log = structlog.get_logger(PIPELINE)

_BASE = "https://object.openintel.nl/seeseetld/lists"
TOP_10_CCTLDS = ["de", "uk", "nl", "ru", "br", "eu", "cn", "au", "it", "fr"]

# Rows to accumulate before each ClickHouse bulk insert
_CHUNK = 200_000

_DDL = """
CREATE TABLE IF NOT EXISTS dabi.openintel_cctld
(
    apex          String,
    tld           LowCardinality(String),
    snapshot_date Date
)
ENGINE = ReplacingMergeTree(snapshot_date)
PARTITION BY tld
ORDER BY (tld, apex)
SETTINGS index_granularity = 8192
"""


def _default_partition_date() -> str:
    return (datetime.datetime.now(timezone.utc).date() - datetime.timedelta(days=1)).isoformat()


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--partition-date",
        default=_default_partition_date(),
        help="Snapshot date recorded in ClickHouse (YYYY-MM-DD). Default: yesterday UTC.",
    )
    parser.add_argument(
        "--tlds",
        nargs="+",
        default=TOP_10_CCTLDS,
        metavar="TLD",
        help=f"ccTLDs to ingest. Default: all top 10 ({' '.join(TOP_10_CCTLDS)}).",
    )
    parser.add_argument(
        "--look-back",
        type=int,
        default=7,
        metavar="DAYS",
        help="Days to look back for the most recent OpenINTEL publish (default: 7).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if checkpoint already marks this date done.",
    )


def run(args: argparse.Namespace) -> int:
    log_ = log.bind(partition_date=args.partition_date, tlds=args.tlds)

    if not args.force and checkpoint.is_done(PIPELINE, args.partition_date):
        log_.info("checkpoint.skip", reason="already done")
        return 0

    ch = clients.clickhouse()

    # Schema: if the table already exists with extra columns from the old Tranco-based
    # schema, DROP and recreate — we're replacing with the leaner OpenINTEL schema.
    ch.command("DROP TABLE IF EXISTS dabi.openintel_cctld")
    ch.command(_DDL)
    log_.info("schema.ensured")

    snap_date = datetime.date.fromisoformat(args.partition_date)
    target = snap_date  # look back from here to find OpenINTEL data

    with checkpoint.run(PIPELINE, args.partition_date) as cp:
        total = 0
        for tld in args.tlds:
            n = _ingest_tld(ch, tld, snap_date, target, args.look_back, log_)
            total += n
        cp.set_rows(total)
        log_.info("done", total_rows=total)

    return 0


def _ingest_tld(
    ch,
    tld: str,
    snap_date: datetime.date,
    target: datetime.date,
    look_back: int,
    log_,
) -> int:
    log_ = log_.bind(tld=tld)

    result = _find_url(tld, target, look_back, log_)
    if result is None:
        log_.warning("step.no_data", look_back=look_back)
        return 0
    url, data_date = result
    log_.info("step.download", url=url, data_date=data_date.isoformat())

    try:
        resp = requests.get(url, timeout=600)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log_.error("step.download.failed", error=str(exc))
        raise

    compressed_bytes = len(resp.content)
    log_.info("step.download.done", compressed_mb=round(compressed_bytes / 1_048_576, 1))

    total = 0
    batch: list[tuple[str, str, datetime.date]] = []

    with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8", errors="replace") as gz:
        for raw_line in gz:
            domain = raw_line.strip()
            if not domain:
                continue
            batch.append((domain, tld, snap_date))
            if len(batch) >= _CHUNK:
                ch.insert(
                    "dabi.openintel_cctld",
                    batch,
                    column_names=["apex", "tld", "snapshot_date"],
                )
                total += len(batch)
                log_.info("step.insert.progress", rows_so_far=total)
                batch = []

    if batch:
        ch.insert(
            "dabi.openintel_cctld",
            batch,
            column_names=["apex", "tld", "snapshot_date"],
        )
        total += len(batch)

    log_.info("step.tld.done", rows=total)
    return total


def _find_url(
    tld: str,
    target: datetime.date,
    look_back: int,
    log_,
) -> tuple[str, datetime.date] | None:
    """Return (url, publish_date) for the most recent available OpenINTEL file."""
    for offset in range(look_back):
        date = target - datetime.timedelta(days=offset)
        url = (
            f"{_BASE}/tld={tld}"
            f"/year={date.year:04d}"
            f"/month={date.month:02d}"
            f"/day={date.day:02d}"
            f"/ccTLD-domain-names-list.{tld}.{date.isoformat()}.csv.gz"
        )
        try:
            r = requests.head(url, timeout=15)
            if r.status_code == 200:
                log_.debug("step.url_found", date=date.isoformat(), offset=offset)
                return url, date
        except requests.RequestException:
            continue
    return None
