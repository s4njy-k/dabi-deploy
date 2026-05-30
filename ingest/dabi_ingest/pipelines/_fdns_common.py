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
