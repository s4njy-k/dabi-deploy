"""OpenINTEL ccTLD apex domain lists — fetched from OpenINTEL public object storage.

Source:      https://object.openintel.nl/seeseetld/lists/
Format:      gzip-compressed CSV, one apex domain per line, no header
Attribution: OpenINTEL (University of Twente) — CC BY-NC-SA 4.0

Pipeline:
  1. Download gzipped CSV from object.openintel.nl (public, no auth)
  2. Bulk-insert apex domains into dabi.openintel_cctld (ClickHouse)
  3. Bulk-index into domains-{tld}-{date} (OpenSearch) with computed features
  4. Update domains-{tld} alias + dabi-domains global alias

Timer:  weekly Monday 23:00 UTC via dabi-ingest-openintel-cctld.timer
"""

from __future__ import annotations

import argparse
import datetime
import gzip
import io
import re

import requests
import structlog
from opensearchpy.helpers import parallel_bulk

from dabi_ingest import checkpoint, clients

PIPELINE = "openintel-cctld"
DESCRIPTION = "OpenINTEL ccTLD apex lists — top 10 ccTLDs, ~38M domains/week."

log = structlog.get_logger(PIPELINE)

_BASE = "https://object.openintel.nl/seeseetld/lists"
TOP_10_CCTLDS = ["de", "uk", "nl", "ru", "br", "eu", "cn", "au", "it", "fr"]

_CHUNK_CH = 200_000
_CHUNK_OS = 25_000  # docs per parallel_bulk request
_OS_THREADS = 8  # concurrent bulk requests

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

_VOWELS = frozenset("aeiou")
_YEAR_RE = re.compile(r"(19|20)\d{2}")


def _default_partition_date() -> str:
    return (datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=1)).isoformat()


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
        "--look-back",
        type=int,
        default=7,
        metavar="DAYS",
        help="Days to look back for the most recent OpenINTEL publish (default: 7).",
    )
    parser.add_argument(
        "--skip-opensearch",
        action="store_true",
        help="Load ClickHouse only; skip OpenSearch indexing.",
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
    os_client = None if args.skip_opensearch else clients.opensearch()

    # Migrate schema: drop old 9-column table if exists, recreate lean 3-column
    ch.command("DROP TABLE IF EXISTS dabi.openintel_cctld")
    ch.command(_DDL)
    log_.info("schema.ensured")

    snap_date = datetime.date.fromisoformat(args.partition_date)

    with checkpoint.run(PIPELINE, args.partition_date) as cp:
        total_ch = 0
        total_os = 0

        for tld in args.tlds:
            n_ch = _fetch_and_load_ch(ch, tld, snap_date, snap_date, args.look_back, log_)
            total_ch += n_ch

            if os_client is not None and n_ch > 0:
                n_os = _index_tld_to_os(os_client, ch, tld, snap_date, log_)
                total_os += n_os

        cp.set_rows(total_ch)
        log_.info("done", total_ch=total_ch, total_os=total_os)

    return 0


# ── ClickHouse load ───────────────────────────────────────────────────────────


def _fetch_and_load_ch(ch, tld, snap_date, target, look_back, log_):
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

    log_.info("step.download.done", compressed_mb=round(len(resp.content) / 1_048_576, 1))

    total = 0
    batch: list[tuple[str, str, datetime.date]] = []

    with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8", errors="replace") as gz:
        for raw_line in gz:
            domain = raw_line.strip()
            if not domain:
                continue
            batch.append((domain, tld, snap_date))
            if len(batch) >= _CHUNK_CH:
                ch.insert("dabi.openintel_cctld", batch, column_names=["apex", "tld", "snapshot_date"])
                total += len(batch)
                log_.info("step.ch_insert.progress", rows_so_far=total)
                batch = []

    if batch:
        ch.insert("dabi.openintel_cctld", batch, column_names=["apex", "tld", "snapshot_date"])
        total += len(batch)

    log_.info("step.ch_insert.done", rows=total)
    return total


def _find_url(tld, target, look_back, log_):
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
                return url, date
        except requests.RequestException:
            continue
    return None


# ── OpenSearch index ──────────────────────────────────────────────────────────


def _index_tld_to_os(os_client, ch, tld, snap_date, log_):
    log_ = log_.bind(tld=tld)
    snap_str = snap_date.isoformat()
    index_name = f"domains-{tld}-{snap_str.replace('-', '')}"

    log_.info("step.os_index.start", index=index_name)

    mapping = _get_domain_mapping(os_client)
    if os_client.indices.exists(index=index_name):
        os_client.indices.delete(index=index_name)
    os_client.indices.create(index=index_name, body={"mappings": mapping})

    result = ch.query(
        "SELECT apex FROM dabi.openintel_cctld WHERE tld = {tld:String} AND snapshot_date = {snap:String}",
        parameters={"tld": tld, "snap": snap_str},
    )
    domains = [row[0] for row in result.result_rows]
    log_.info("step.os_index.fetched", count=len(domains))

    def _gen_docs():
        for fqdn in domains:
            yield {"_index": index_name, "_id": fqdn, "_source": _compute_doc(fqdn, tld, snap_str)}

    success = 0
    error_count = 0
    for ok, _info in parallel_bulk(
        os_client,
        _gen_docs(),
        chunk_size=_CHUNK_OS,
        thread_count=_OS_THREADS,
        request_timeout=120,
        raise_on_error=False,
    ):
        if ok:
            success += 1
        else:
            error_count += 1  # noqa: SIM115 — intentional sequential counter
    if error_count:
        log_.warning("step.os_index.errors", count=error_count)

    _update_aliases(os_client, tld, index_name, log_)
    log_.info("step.os_index.done", indexed=success)
    return success


def _compute_doc(fqdn: str, tld: str, snap_str: str) -> dict:
    sld = fqdn[: -(len(tld) + 1)] if fqdn.endswith(f".{tld}") else fqdn
    label = sld.split(".")[0].lower()
    label_len = len(label)
    hyphen_count = label.count("-")
    digit_count = sum(1 for c in label if c.isdigit())
    alpha_count = sum(1 for c in label if c.isalpha())
    vowel_count = sum(1 for c in label if c in _VOWELS)
    vowel_ratio = round(vowel_count / max(label_len, 1), 4)
    digit_ratio = round(digit_count / max(label_len, 1), 4)
    has_year_token = bool(_YEAR_RE.search(label))
    collapsed = re.sub(r"^\d+|\d+$", "", label) or label
    fqdn_reversed = ".".join(reversed(fqdn.split(".")))

    return {
        "fqdn": fqdn,
        "fqdn_reversed": fqdn_reversed,
        "tld": tld,
        "sld": sld,
        "label": label,
        "collapsed": collapsed,
        "label_len": label_len,
        "hyphen_count": hyphen_count,
        "digit_count": digit_count,
        "alpha_count": alpha_count,
        "has_year_token": has_year_token,
        "vowel_ratio": vowel_ratio,
        "digit_ratio": digit_ratio,
        "nameservers": [],
        "ns_apex": "",
        "a_records": [],
        "aaaa_records": [],
        "has_dnssec": False,
        "ns_ttl": 0,
        "record_count": 0,
        "risk_score": 0.0,
        "risk_band": "clean",
        "signals": {},
        "categories": [],
        "brand_matches": [],
        "is_new": False,
        "snapshot_date": snap_str,
    }


def _get_domain_mapping(os_client) -> dict:
    try:
        existing = os_client.indices.get(index="domains-online-*")
        if existing:
            idx = sorted(existing.keys())[-1]
            return existing[idx]["mappings"]
    except Exception:
        pass
    return {
        "properties": {
            "fqdn": {"type": "keyword"},
            "fqdn_reversed": {"type": "keyword"},
            "tld": {"type": "keyword"},
            "sld": {"type": "keyword"},
            "label": {"type": "keyword"},
            "collapsed": {"type": "keyword"},
            "label_len": {"type": "short"},
            "hyphen_count": {"type": "short"},
            "digit_count": {"type": "short"},
            "alpha_count": {"type": "short"},
            "has_year_token": {"type": "boolean"},
            "vowel_ratio": {"type": "float"},
            "digit_ratio": {"type": "float"},
            "nameservers": {"type": "keyword"},
            "ns_apex": {"type": "keyword"},
            "a_records": {"type": "keyword"},
            "aaaa_records": {"type": "keyword"},
            "has_dnssec": {"type": "boolean"},
            "ns_ttl": {"type": "integer"},
            "record_count": {"type": "long"},
            "risk_score": {"type": "float"},
            "risk_band": {"type": "keyword"},
            "signals": {"type": "object"},
            "categories": {"type": "keyword"},
            "brand_matches": {"type": "keyword"},
            "is_new": {"type": "boolean"},
            "snapshot_date": {"type": "date"},
        }
    }


def _update_aliases(os_client, tld: str, new_index: str, log_) -> None:
    alias_tld = f"domains-{tld}"
    alias_global = "dabi-domains"
    actions: list[dict] = []

    try:
        existing = os_client.indices.get_alias(name=alias_tld)
        for old_idx in existing:
            if old_idx != new_index:
                actions.append({"remove": {"index": old_idx, "alias": alias_tld}})
    except Exception:
        pass

    actions.append({"add": {"index": new_index, "alias": alias_tld}})
    actions.append({"add": {"index": new_index, "alias": alias_global}})
    os_client.indices.update_aliases(body={"actions": actions})
    log_.info("step.aliases.updated", tld_alias=alias_tld)
