"""Shared logic for the OpenINTEL forward-DNS pipelines (toplist + zonefile).

Source:      https://object.openintel.nl/openintel-public/fdns/basis=<basis>/source=<source>/...
Format:      gzip-compressed Parquet, one row per DNS response (OpenINTEL "big table" schema)
Attribution: OpenINTEL (Univ. of Twente, SIDN, NLnet Labs, SURF) — CC BY-NC-SA 4.0

Pipeline (per source, per day):
  1. Discover the latest published Parquet part URLs (terms-cookie + listing navigation).
  2. ClickHouse reads each Parquet server-side via url(..., Parquet) → dabi.dns_records.
  3. Upsert distinct (apex, query_type, response) into dabi.dns_current.
  4. Enrich matching OpenSearch domain docs with a_records/aaaa_records/nameservers/etc.
"""

from __future__ import annotations

import os

import structlog

OBJECT_BASE = "https://object.openintel.nl/openintel-public/fdns"
LISTING_BASE = "https://openintel.nl/download/forward-dns"
TERMS_URL = "https://openintel.nl/download/terms/"

# Record types we ingest (response_type values). RRSIG/NSEC/CDS/CDNSKEY/NSEC3* are skipped.
RECORD_TYPES = ("A", "AAAA", "NS", "MX", "CNAME", "DNAME", "TXT", "SOA", "DS", "DNSKEY")

RETAIN_DAYS = int(os.environ.get("DABI_DNS_RETAIN_DAYS", "180"))

log = structlog.get_logger("openintel-fdns")

DDL_RECORDS = f"""
CREATE TABLE IF NOT EXISTS dabi.dns_records
(
    apex          String,
    query_name    String,
    query_type    LowCardinality(String),
    response      String,
    ttl           UInt32,
    country       LowCardinality(String),
    asn           UInt32,
    asn_name      String,
    ip_prefix     String,
    basis         LowCardinality(String),
    source        LowCardinality(String),
    observed_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_date)
ORDER BY (apex, query_type, response)
TTL observed_date + INTERVAL {RETAIN_DAYS} DAY
SETTINGS storage_policy = 'scratch', index_granularity = 8192
"""

DDL_PROJECTION = """
ALTER TABLE dabi.dns_records
ADD PROJECTION IF NOT EXISTS proj_by_response
( SELECT * ORDER BY (response, query_type, apex) )
"""

DDL_CURRENT = """
CREATE TABLE IF NOT EXISTS dabi.dns_current
(
    apex        String,
    query_type  LowCardinality(String),
    response    String,
    ttl         UInt32,
    source      LowCardinality(String),
    first_seen  Date,
    last_seen   Date
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (apex, query_type, response)
SETTINGS storage_policy = 'scratch'
"""


def ensure_schema(ch) -> None:
    """Create dns_records (+ reverse-pivot projection) and dns_current if absent."""
    ch.command(DDL_RECORDS)
    ch.command(DDL_PROJECTION)
    ch.command(DDL_CURRENT)
    log.info("schema.ensured")


# ── Discovery ───────────────────────────────────────────────────────────────

import datetime  # noqa: E402
import re  # noqa: E402

import requests  # noqa: E402

_PART_RE = re.compile(r'https://object\.openintel\.nl/openintel-public/fdns/[^"\'<> )]+\.parquet')


def parse_sources(html: str) -> list[str]:
    """Source names linked on a basis listing page (deduped, sorted)."""
    found = re.findall(r'source=([a-z0-9.\-]+)"', html)
    return sorted(set(found))


def parse_parts(html: str) -> list[str]:
    """Direct object.openintel.nl Parquet URLs on a leaf (day) page."""
    return sorted(set(_PART_RE.findall(html)))


def listing_url_for(basis: str, source: str, year: int, month: int, day: int) -> str:
    """Website listing URL for a given day (path segments are %3D-encoded, per OpenINTEL)."""
    return (
        f"{LISTING_BASE}/basis={basis}/source={source}/"
        f"year%3D{year:04d}/month%3D{month:02d}/day%3D{day:02d}/"
    )


def make_session() -> requests.Session:
    """Session with the CC BY-NC-SA terms cookie accepted (required before listing pages)."""
    sess = requests.Session()
    sess.post(TERMS_URL, params={"redirect_uri": "/download/forward-dns/"}, timeout=20)
    return sess


def discover_sources(sess: requests.Session, basis: str) -> list[str]:
    resp = sess.get(f"{LISTING_BASE}/basis={basis}/", timeout=20)
    resp.raise_for_status()
    return parse_sources(resp.text)


def discover_parts(
    sess: requests.Session, basis: str, source: str, target: datetime.date, look_back: int
) -> tuple[list[str], datetime.date] | None:
    """Walk back up to look_back days; return (part_urls, data_date) for the newest day with files."""
    for offset in range(look_back):
        d = target - datetime.timedelta(days=offset)
        try:
            r = sess.get(listing_url_for(basis, source, d.year, d.month, d.day), timeout=20)
            parts = parse_parts(r.text)
            if parts:
                return parts, d
        except requests.RequestException:
            continue
    return None


# ── Load SQL (ClickHouse reads Parquet server-side via url()) ─────────────────

_TYPES_SQL = "(" + ",".join(f"'{t}'" for t in RECORD_TYPES) + ")"


def build_insert_sql(part_url: str, basis: str, source: str, date: str) -> str:
    """INSERT … SELECT that reads one OpenINTEL Parquet part server-side and normalizes it.

    response is the type-appropriate value column; host-valued columns have their trailing
    dot stripped. DS/DNSKEY rows carry a key-tag/algorithm marker so has_dnssec can be derived.
    `as` is back-quoted (ClickHouse keyword).
    """
    return f"""
INSERT INTO dabi.dns_records
    (apex, query_name, query_type, response, ttl, country, asn, asn_name, ip_prefix, basis, source, observed_date)
WITH trimRight(query_name, '.') AS qn
SELECT
    cutToFirstSignificantSubdomain(qn) AS apex,
    qn AS query_name,
    response_type AS query_type,
    multiIf(
        response_type = 'A',      ifNull(ip4_address, ''),
        response_type = 'AAAA',   ifNull(ip6_address, ''),
        response_type = 'NS',     trimRight(ifNull(ns_address, ''), '.'),
        response_type = 'MX',     trimRight(ifNull(mx_address, ''), '.'),
        response_type = 'CNAME',  trimRight(ifNull(cname_name, ''), '.'),
        response_type = 'DNAME',  trimRight(ifNull(dname_name, ''), '.'),
        response_type = 'TXT',    ifNull(txt_text, ''),
        response_type = 'SOA',    trimRight(ifNull(soa_mname, ''), '.'),
        response_type = 'DS',     toString(ifNull(ds_key_tag, 0)),
        response_type = 'DNSKEY', toString(ifNull(dnskey_algorithm, 0)),
        ''
    ) AS response,
    toUInt32(ifNull(response_ttl, 0)) AS ttl,
    ifNull(country, '') AS country,
    toUInt32OrZero(ifNull(`as`, '')) AS asn,
    ifNull(as_full, '') AS asn_name,
    ifNull(ip_prefix, '') AS ip_prefix,
    '{basis}' AS basis,
    '{source}' AS source,
    toDate('{date}') AS observed_date
FROM url('{part_url}', Parquet)
WHERE response_type IN {_TYPES_SQL}
  AND response != ''
SETTINGS max_http_get_redirects = 5
"""


def build_current_upsert_sql(date: str) -> str:
    """Roll the day's rows into dns_current (ReplacingMergeTree collapses on (apex,type,response))."""
    return f"""
INSERT INTO dabi.dns_current (apex, query_type, response, ttl, source, first_seen, last_seen)
SELECT
    apex, query_type, response,
    any(ttl) AS ttl,
    any(source) AS source,
    min(observed_date) AS first_seen,
    max(observed_date) AS last_seen
FROM dabi.dns_records
WHERE observed_date = toDate('{date}')
GROUP BY apex, query_type, response
"""


# ── OpenSearch enrichment ─────────────────────────────────────────────────────

from opensearchpy.helpers import parallel_bulk  # noqa: E402


def build_enrich_doc(apex: str, rows: list[tuple[str, str]], snapshot_date: str) -> dict:
    """Group (query_type, response) rows for one apex into the OS partial-update source."""
    a, aaaa, ns, mx = [], [], [], []
    has_dnssec = False
    for qtype, resp in rows:
        if qtype == "A":
            a.append(resp)
        elif qtype == "AAAA":
            aaaa.append(resp)
        elif qtype == "NS":
            ns.append(resp)
        elif qtype == "MX":
            mx.append(resp)
        elif qtype in ("DS", "DNSKEY"):
            has_dnssec = True
    ns_apex = _registered_apex(ns[0]) if ns else ""
    return {
        "a_records": a,
        "aaaa_records": aaaa,
        "nameservers": ns,
        "mx_records": mx,
        "ns_apex": ns_apex,
        "has_dnssec": has_dnssec,
        "record_count": len(rows),
        "snapshot_date": snapshot_date,
    }


def _registered_apex(host: str) -> str:
    """Last two labels (sufficient for the single-/two-label NS hosts OpenINTEL returns)."""
    labels = host.strip(".").split(".")
    return ".".join(labels[-2:]) if len(labels) >= 2 else host


def enrich_opensearch(os_client, ch, date: str, log_, limit: int | None = None) -> int:
    """Push the day's dns_current state into existing OS domain docs (partial updates).

    Only apexes already present in OpenSearch are touched (no doc_as_upsert, so new domains
    are not created here — name indexing is the cctld/czds pipelines' job).
    """
    import itertools

    q = (
        "SELECT apex, query_type, response FROM dabi.dns_records "
        "WHERE observed_date = toDate({d:String}) "
        "AND query_type IN ('A','AAAA','NS','MX','DS','DNSKEY') "
        "ORDER BY apex"
    )
    if limit:
        q += f" LIMIT {int(limit)}"
    result = ch.query(q, parameters={"d": date})

    def _actions():
        for apex, grp in itertools.groupby(result.result_rows, key=lambda r: r[0]):
            rows = [(r[1], r[2]) for r in grp]
            yield {
                "_op_type": "update",
                "_index": "dabi-domains",
                "_id": apex,
                "doc": build_enrich_doc(apex, rows, date),
            }

    updated = 0
    for ok, _info in parallel_bulk(
        os_client, _actions(), chunk_size=5000, thread_count=8,
        request_timeout=120, raise_on_error=False,
    ):
        if ok:
            updated += 1
    log_.info("enrich.done", updated=updated)
    return updated
