-- DABI ClickHouse schema — runs ONLY on fresh /var/lib/clickhouse (docker-entrypoint-initdb.d
-- behavior). All CREATE IF NOT EXISTS, so manual replay via scripts/replay-init-sql.sh is safe
-- for container restarts that retained state.
-- Documented in Plan v8 §13 (carried over from v6 §14.3)

CREATE DATABASE IF NOT EXISTS dabi;

-- Schema-migration tracker (manual but recorded)
CREATE TABLE IF NOT EXISTS dabi.schema_migrations
(
    version     UInt32,
    description String,
    applied_at  DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY version;

-- ============================================================
-- B3 — IPv4 reverse DNS (the killer ClickHouse workload)
-- ============================================================
CREATE TABLE IF NOT EXISTS dabi.rdns
(
    ip4            UInt32 CODEC(T64, LZ4),
    ptr            String CODEC(ZSTD(3)),
    ptr_apex       LowCardinality(String) CODEC(ZSTD(3)),
    asn            UInt32 CODEC(T64, LZ4),
    country        FixedString(2),
    observed_date  Date,
    last_seen      DateTime CODEC(DoubleDelta, LZ4)
)
ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY toYYYYMM(observed_date)
ORDER BY (ip4, observed_date)
SETTINGS index_granularity = 8192;

-- ============================================================
-- A7 — raw CT-log FQDN observations (fact table)
-- ============================================================
CREATE TABLE IF NOT EXISTS dabi.ct_fqdn_observations
(
    fqdn           String CODEC(ZSTD(3)),
    apex           LowCardinality(String) CODEC(ZSTD(3)),
    observed_date  Date,
    ct_log_source  LowCardinality(String),
    cert_index     UInt64,
    has_wildcard   UInt8,
    resolved_ip4   IPv4,
    resolved_ip6   IPv6,
    asn            UInt32,
    country        FixedString(2),
    dnssec_ad      UInt8,
    ttl            UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_date)
ORDER BY (apex, fqdn, observed_date);

-- Materialized view: per-apex rollup (read by api for /trends)
CREATE MATERIALIZED VIEW IF NOT EXISTS dabi.apex_ct_aggregates_mv
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(observed_date)
ORDER BY (apex, observed_date)
AS SELECT
    apex,
    observed_date,
    uniqState(fqdn)         AS fqdn_count_state,
    minState(observed_date) AS first_seen_state,
    maxState(observed_date) AS last_seen_state,
    uniqState(asn)          AS asn_count_state,
    uniqState(country)      AS country_count_state,
    uniqState(resolved_ip4) AS ip4_count_state,
    maxState(has_wildcard)  AS has_wildcard_state,
    avgState(dnssec_ad)     AS dnssec_ratio_state
FROM dabi.ct_fqdn_observations
GROUP BY apex, observed_date;

-- ============================================================
-- A6 — RIR WHOIS (CIDR overlap lookups)
-- ============================================================
CREATE TABLE IF NOT EXISTS dabi.rir_whois
(
    prefix_start   UInt32 CODEC(T64),
    prefix_end     UInt32 CODEC(T64),
    prefix_len     UInt8,
    netname        String,
    country        FixedString(2),
    org            String,
    source         LowCardinality(String),
    last_modified  Date
)
ENGINE = ReplacingMergeTree(last_modified)
ORDER BY (prefix_start, prefix_end);

-- ============================================================
-- External archive table — GCS Parquet read in-place via S3 protocol
-- HMAC creds are injected at query time by the api via SETTINGS
-- ============================================================
-- (uncomment + fill in HMAC after Phase 2 GCS HMAC creation)
-- CREATE TABLE IF NOT EXISTS dabi.archive_ctlog
-- (
--     ...same columns as ct_fqdn_observations...
-- )
-- ENGINE = S3(
--   'https://storage.googleapis.com/dabi-prod-archive/openintel/ctlog/y={year}/m={month}/d={day}/*.parquet',
--   '<HMAC_KEY>', '<HMAC_SECRET>', 'Parquet'
-- );


-- ============================================================
-- Tranco daily top-1M list (OpenINTEL toplist equivalent — public source)
-- ============================================================
CREATE TABLE IF NOT EXISTS dabi.tranco_top1m
(
    rank          UInt32 CODEC(T64, LZ4),
    domain        LowCardinality(String) CODEC(ZSTD(3)),
    apex          LowCardinality(String) CODEC(ZSTD(3)),
    observed_date Date
)
ENGINE = ReplacingMergeTree(observed_date)
PARTITION BY observed_date
ORDER BY (rank, domain);

-- Record this baseline migration
INSERT INTO dabi.schema_migrations (version, description)
SELECT 1, 'v8 baseline: rdns, ct_fqdn_observations, apex_ct_aggregates_mv, rir_whois'
WHERE NOT EXISTS (SELECT 1 FROM dabi.schema_migrations WHERE version = 1);
