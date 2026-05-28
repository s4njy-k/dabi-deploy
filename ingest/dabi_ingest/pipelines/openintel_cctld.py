"""OpenINTEL ccTLD apex lists — CT-derived + Tranco apex inventory for top 10 ccTLDs.

Derives an apex domain inventory for each ccTLD by merging two locally-available
sources that approximate the OpenINTEL B3 ccTLD measurement target lists:

  dabi.ct_fqdn_observations  — Certificate Transparency records (grows daily via ctlog timer)
  dabi.tranco_top1m          — Tranco popularity ranking (latest partition)

When the B3 OpenINTEL SFTP/HTTPS data-share is approved, replace the fetch step
with a direct Parquet download from data.openintel.nl and keep the ClickHouse load
step as-is.

Output: dabi.openintel_cctld (ReplacingMergeTree, keyed on snapshot_date, partitioned by tld).
Timer:  dabi-ingest-openintel-cctld.timer — weekly Monday 23:00 UTC.
"""
from __future__ import annotations

import argparse
import datetime
from datetime import timezone

import structlog

from dabi_ingest import checkpoint, clients

PIPELINE = "openintel-cctld"
DESCRIPTION = "OpenINTEL ccTLD apex lists — CT + Tranco inventory for top 10 ccTLDs."

log = structlog.get_logger(PIPELINE)

TOP_10_CCTLDS = ["de", "uk", "nl", "ru", "br", "eu", "cn", "au", "it", "fr"]

_DDL = """
CREATE TABLE IF NOT EXISTS dabi.openintel_cctld
(
    apex          String,
    tld           LowCardinality(String),
    in_ct         UInt8   DEFAULT 0,
    in_tranco     UInt8   DEFAULT 0,
    ct_count      UInt32  DEFAULT 0,
    tranco_rank   UInt32  DEFAULT 0,
    first_seen    Date,
    last_seen     Date,
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
        help="Snapshot date (YYYY-MM-DD). Default: yesterday UTC.",
    )
    parser.add_argument(
        "--tlds",
        nargs="+",
        default=TOP_10_CCTLDS,
        metavar="TLD",
        help=f"ccTLDs to ingest. Default: all top 10 ({' '.join(TOP_10_CCTLDS)}).",
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
    ch.command(_DDL)
    log_.info("schema.ensured")

    snap_date = datetime.date.fromisoformat(args.partition_date)

    with checkpoint.run(PIPELINE, args.partition_date) as cp:
        total = 0
        for tld in args.tlds:
            n = _ingest_tld(ch, tld, snap_date, log_)
            total += n
        cp.set_rows(total)
        log_.info("done", total_rows=total)

    return 0


def _ingest_tld(ch, tld: str, snap_date: datetime.date, log_) -> int:
    log_ = log_.bind(tld=tld)
    tld_suffix = f".{tld}"

    # --- Certificate Transparency observations ---
    log_.info("step.ct_query")
    ct_result = ch.query(
        """
        SELECT
            apex,
            toUInt32(count())   AS ct_count,
            min(observed_date)  AS first_seen,
            max(observed_date)  AS last_seen
        FROM dabi.ct_fqdn_observations
        WHERE endsWith(apex, {suffix:String})
        GROUP BY apex
        """,
        parameters={"suffix": tld_suffix},
    )
    # map: apex → (ct_count, first_seen_date, last_seen_date)
    ct_map: dict[str, tuple[int, datetime.date, datetime.date]] = {
        r[0]: (int(r[1]), r[2], r[3]) for r in ct_result.result_rows
    }
    log_.info("step.ct_query.done", count=len(ct_map))

    # --- Tranco top-1M (latest snapshot only) ---
    log_.info("step.tranco_query")
    tranco_result = ch.query(
        """
        SELECT domain, toUInt32(min(rank)) AS best_rank
        FROM dabi.tranco_top1m
        WHERE endsWith(domain, {suffix:String})
          AND observed_date = (SELECT max(observed_date) FROM dabi.tranco_top1m)
        GROUP BY domain
        """,
        parameters={"suffix": tld_suffix},
    )
    # map: domain → best_rank
    tranco_map: dict[str, int] = {r[0]: int(r[1]) for r in tranco_result.result_rows}
    log_.info("step.tranco_query.done", count=len(tranco_map))

    all_apexes = set(ct_map) | set(tranco_map)
    if not all_apexes:
        log_.info("step.empty_tld")
        return 0

    rows = []
    for apex in all_apexes:
        in_ct = 1 if apex in ct_map else 0
        in_tranco = 1 if apex in tranco_map else 0
        ct_count, first_seen, last_seen = ct_map.get(apex, (0, snap_date, snap_date))
        tranco_rank = tranco_map.get(apex, 0)
        rows.append((
            apex, tld,
            in_ct, in_tranco,
            ct_count, tranco_rank,
            first_seen, last_seen,
            snap_date,
        ))

    ch.insert(
        "dabi.openintel_cctld",
        rows,
        column_names=[
            "apex", "tld",
            "in_ct", "in_tranco",
            "ct_count", "tranco_rank",
            "first_seen", "last_seen",
            "snapshot_date",
        ],
    )
    log_.info("step.insert.done", rows=len(rows))
    return len(rows)
