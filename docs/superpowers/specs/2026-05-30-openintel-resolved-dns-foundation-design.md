# OpenINTEL Resolved-DNS Foundation — Design

**Date:** 2026-05-30
**Repo:** `s4njy-k/dabi-deploy` (ingest package at `ingest/dabi_ingest/`)
**Status:** Approved in principle — pending written-spec review
**Author:** Claude (DABI session)

## 1. Problem & Goal

DABI today indexes **domain names** (147.5M docs across CZDS gTLDs + OpenINTEL ccTLD
name-lists) but has almost no **resolved DNS** depth. The improvised `dns_observations`
table only resolves ~10K Tranco domains/day. For a cybercrime investigator the missing
capability is **infrastructure pivoting**: given a domain, what are its current/historical
A / AAAA / MX / NS / TXT / DNSKEY / CAA records; and inversely, given an IP / nameserver /
mail host, which domains share it.

OpenINTEL publishes exactly this as daily **forward-DNS Parquet** measurements, in a
**public** bucket (`object.openintel.nl/openintel-public/fdns/...`) gated only by a
one-click CC BY-NC-SA 4.0 license acceptance — **no approval required** (verified 2026-05-30:
a forward-dns parquet downloads HTTP 200 with no cookie/auth). The six `openintel-*` ingest
pipelines were never blocked on licensing — they are unimplemented 63-line stubs with
disabled timers.

**Goal of this sub-project:** land OpenINTEL resolved DNS into ClickHouse with a
pivot-optimized schema, and enrich the existing OpenSearch domain docs with live
infrastructure fields. This is the data foundation; API pivots and UI are follow-on specs.

## 2. Scope

**In scope (sub-project 1):**
- `openintel-toplist` pipeline — resolved DNS for all 6 public toplists
  (`alexa, crux, majestic, radar, tranco, umbrella`).
- `openintel-zonefile` pipeline — resolved DNS for all public zone-based sources
  (`ch, ee, fr, se, sk, li, nu, gov, fed.us, root`).
- ClickHouse schema: `dabi.dns_records` (history) + `dabi.dns_current` (latest state),
  hosted on the 2 TB `/mnt/scratch` disk via a ClickHouse storage policy.
- OpenSearch enrichment step: fill the already-present-but-empty `a_records`,
  `aaaa_records`, `nameservers`, `ns_apex`, `has_dnssec` fields on domain docs.
- Infra: bind-mount + `config.d` storage policy; dedicated systemd timers; CI rebuild.
- Tests: unit (URL discovery, parquet→CH mapping, enrichment doc-builder) + live smoke
  on one small zone (`.li`).

**Out of scope (future specs):**
- SP2: pivot API endpoints + `/api/v1/search` 503 fix (repo `domain-search-pro`).
- SP3: investigator web UI (pivot views, DNS-history timeline, infra clustering).
- SP4: remaining stubs `openintel-infra`, `openintel-ctlog`, `openintel-rdns`,
  `openintel-prefix`.
- Deprecating the improvised `dns_observations`/`rdns`/`ctlog` pipelines (revisit once
  `openintel-toplist` is verified to supersede them).

## 3. Architecture (Approach A — ClickHouse-centric)

Resolved DNS lives in **ClickHouse** (columnar, ~10× compression, built for the
`GROUP BY ip / ns / mx` pivots). **OpenSearch** stays the fuzzy name-search front and is
*enriched* with each domain's latest infra so search results immediately show A/NS/MX.
Rejected alternatives: OpenSearch-centric (overflows the 450 GB disk, slow cross-doc
aggregations) and a dedicated graph DB (YAGNI for v1).

```
object.openintel.nl/openintel-public/fdns/basis={toplist|zonefile}/source=<s>/year=/month=/day=/*.parquet
      │  (terms-cookie discovery, like openintel-cctld)
      ▼
ClickHouse  url('<parquet>', Parquet)  ──► dabi.dns_records  (append, history, TTL 180d)
      │                                          │ MV / INSERT-time roll
      │                                          ▼
      │                                    dabi.dns_current  (ReplacingMergeTree, latest)
      ▼                                          │
enrich step reads dns_current ──────────────────┘
      ▼
OpenSearch domain docs: a_records / aaaa_records / nameservers / ns_apex / has_dnssec
```

### 3.1 Storage placement

Both data disks are durable Persistent Disks (verified 2026-05-30). The 2 TB
`/mnt/scratch` (`google-scratch`, currently 22 GB used) is already provisioned —
hosting the heavy resolved-DNS tables there adds **no cost** and keeps the 450 GB
OpenSearch disk uncrowded.

- `docker-compose.yml` `analytics` service: add bind mount
  `/mnt/scratch/clickhouse:/var/lib/clickhouse-scratch`.
- `config/clickhouse/config.d/storage.xml`: define disk `scratch`
  (`<path>/var/lib/clickhouse-scratch/</path>`) and storage policy `scratch`
  (single volume on that disk).
- New heavy tables created with `SETTINGS storage_policy = 'scratch'`. Existing tables
  on `/srv/dabi` are untouched — no migration, no risk.

## 4. Data Model (ClickHouse)

Column mapping is **finalized** (verified 2026-05-30 via `DESCRIBE url('<li parquet>', Parquet)`
against the real OpenINTEL forward-DNS schema). The parquet carries one row per DNS
response with columns including `query_name`, `query_type`, `response_type`,
`response_name`, `response_ttl`, `ip4_address`, `ip6_address`, `country`, `as`,
`as_full`, `ip_prefix`, `cname_name`, `dname_name`, `mx_address`, `mx_preference`,
`ns_address`, `txt_text`, `soa_mname`, `ds_key_tag`, `dnskey_algorithm` (plus RRSIG/NSEC/
CDS/CDNSKEY detail we don't ingest). Two facts that shape the load:
- `query_name` is FQDN-with-trailing-dot (`coinstrader24.li.`); strip the dot and derive
  apex with `cutToFirstSignificantSubdomain()` (ClickHouse built-in PSL).
- A/AAAA rows ship **pre-resolved IP attribution** (`country`, `as`=ASN, `as_full`=org,
  `ip_prefix`) — we keep these for free domain→IP→ASN→country pivots.

The load SELECT normalizes the value column matching `response_type` into a single
`response String` (see §5.1).

```sql
CREATE TABLE IF NOT EXISTS dabi.dns_records
(
    apex          String,                       -- cutToFirstSignificantSubdomain(query_name)
    query_name    String,                       -- full queried name, trailing dot stripped
    query_type    LowCardinality(String),       -- = response_type: A,AAAA,NS,MX,CNAME,DNAME,TXT,SOA,DS,DNSKEY
    response      String,                        -- normalized value (IP / host / text / key-tag)
    ttl           UInt32,
    country       LowCardinality(String),        -- IP geo (A/AAAA), '' otherwise
    asn           UInt32,                         -- 0 if none
    asn_name      String,                         -- '' if none
    ip_prefix     String,                         -- '' if none
    basis         LowCardinality(String),        -- toplist | zonefile
    source        LowCardinality(String),        -- tranco, se, ch, li, ...
    observed_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_date)
ORDER BY (apex, query_type, response)
TTL observed_date + INTERVAL 180 DAY           -- tunable via DABI_DNS_RETAIN_DAYS
SETTINGS storage_policy = 'scratch', index_granularity = 8192;

-- Reverse-pivot index: IP→domain, NS→domain, MX→domain become index-fast
ALTER TABLE dabi.dns_records
    ADD PROJECTION IF NOT EXISTS proj_by_response
    ( SELECT * ORDER BY (response, query_type, apex) );

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
SETTINGS storage_policy = 'scratch';
```

`dns_current` is refreshed per run by upserting the day's distinct
`(apex, query_type, response)` with `last_seen = observed_date` and
`first_seen = min(existing, observed_date)` (ReplacingMergeTree collapses on merge;
`FINAL` or argMax used at read time).

## 5. Pipelines

Both pipelines replace the existing stub modules and follow the proven
`openintel_cctld.py` template (terms-cookie discovery → object-storage direct download
→ load), reusing `dabi_ingest.checkpoint`, `dabi_ingest.clients`, and the `DiskGuard`
pattern. Dispatcher (`__main__.py`) already registers both names.

### 5.1 Ingest path

ClickHouse reads the Parquet **server-side** via the `url()` table function — no
pandas/pyarrow needed (sidesteps the py3.14 ARM64 wheel gap; only DuckDB 1.5.3 is
present, used for schema discovery and as fallback):

```sql
INSERT INTO dabi.dns_records (apex, query_name, query_type, response, ttl, basis, source, observed_date)
SELECT <apex_expr>, query_name, query_type, <response_expr>, ttl, '<basis>', '<source>', toDate('<date>')
FROM url('https://object.openintel.nl/openintel-public/fdns/basis=<basis>/source=<source>/year=YYYY/month=MM/day=DD/<part>.parquet', Parquet)
WHERE query_type IN ('A','AAAA','NS','MX','TXT','SOA','DNSKEY','DS','CAA','TLSA','CNAME');
```

`apex_expr` derives the registered apex from `query_name` (for zonefile basis the
query_name is already apex-level; for toplist it may include `www.` — strip to apex).
If a `source=<s>` partition has multiple `part-*.parquet` files, iterate them (the
download/listing page enumerates parts, as the cctld discovery does for days).

### 5.2 CLI args (both pipelines)

`--partition-date` (default yesterday UTC), `--sources` (default: auto-discover all
available), `--look-back` (days, default 7), `--skip-opensearch`, `--force`,
`--disk-max-pct` (default 85, via DiskGuard).

### 5.3 OpenSearch enrichment step

After CH load, for each apex with new records, update the OS domain doc
(`dabi-domains` alias, `_id = fqdn`) via partial-update bulk, setting:
`a_records` (query_type=A), `aaaa_records` (AAAA), `nameservers` (NS),
`ns_apex` (apex of first NS), `has_dnssec` (any DNSKEY/DS present),
`record_count`, and refreshed `snapshot_date`. The OS mapping already declares all
these fields (currently empty), so no mapping migration is required. Enrichment only
touches docs whose apex exists in OS; it does not create new docs.

## 6. Operational

- **Timers:** enable `dabi-ingest-openintel-toplist.timer` and
  `dabi-ingest-openintel-zonefile.timer` (currently installed-but-disabled). Stagger:
  toplist ~04:30 UTC, zonefile ~05:30 UTC (after the existing 01:30–03:30 batch, before
  the weekly cctld). Both `OnCalendar` daily.
- **Image:** rebuilt via the existing `ci-ingest.yml` (builds `dabi-ingest:<sha>` from
  `ingest/`); for local/manual runs `dabi-ingest:local` is rebuilt with
  `docker build -t dabi-ingest:local /srv/dabi/deploy/ingest/`.
- **Egress:** parquet pulled via `url()` from the `analytics` container through Cloud NAT
  (`dabi-nat-egress`). OpenINTEL limits downloads to once/day — our daily cadence fits.
- **DiskGuard:** refuses to start / continue mid-batch if `/mnt/scratch` exceeds
  `--disk-max-pct` (default 85).
- **Attribution:** OpenINTEL (Univ. of Twente, SIDN, NLnet Labs, SURF), CC BY-NC-SA 4.0 —
  already surfaced in nginx headers + UI footer per directives; extend footer text to name
  the forward-DNS dataset.

## 7. Testing

- **Unit** (`ingest/tests/`, pytest, following `test_smoke.py`):
  - URL discovery: given a fixed `target` date + look-back, builds correct
    `object.openintel.nl/.../part-*.parquet` URLs per source.
  - Parquet→CH SELECT: column-mapping expression produces expected `(apex, query_type,
    response)` rows from a tiny fixture parquet (generated via DuckDB in the test).
  - Enrichment doc-builder: given `dns_current` rows for an apex, emits the correct OS
    partial-update body (a_records/nameservers/ns_apex/has_dnssec).
- **Live smoke** (manual, before enabling timers): run `openintel-zonefile --sources li`
  end-to-end; assert `dabi.dns_records` row count > 0, `dns_current` populated, and a
  spot-checked `.li` domain's OS doc now shows non-empty `a_records`/`nameservers`.
- **Verification before "done":** quote actual CH counts + an OS doc before claiming
  success (per verification-before-completion).

## 8. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| OpenINTEL parquet schema differs from assumptions | Step 0 inspects real schema with DuckDB before writing the load SELECT; mapping is data-driven. |
| Zone files very large (`.fr`, `.ch`) blow RAM | `url()` streams server-side in ClickHouse (bounded memory); per-source, per-part processing; DiskGuard on `/mnt/scratch`. |
| ClickHouse `url()` can't reach OpenINTEL / TLS issue | Fallback: download parquet to `/mnt/scratch` then `INSERT ... FROM file(...)`; DuckDB httpfs as third option. |
| Enrichment storms OpenSearch (100M+ partial updates) | Only enrich apexes present in OS; batch via `parallel_bulk`; cap per-run via `--enrich-limit`; can `--skip-opensearch` and enrich separately. |
| `/mnt/scratch` perf tier unknown for CH scans | PD throughput scales with size (2 TB); workload is append + scan, not high-IOPS OLTP; monitor first runs. |
| Storage policy misconfig corrupts CH startup | New disk/policy added additively; existing default policy/tables unchanged; smoke `docker compose up analytics` before loading. |

## 9. Definition of Done (this sub-project)

1. `openintel-toplist` + `openintel-zonefile` implemented, replacing the stubs.
2. `dabi.dns_records` (+ projection) and `dabi.dns_current` exist on `storage_policy=scratch`.
3. A live `.li` smoke run shows non-zero `dns_records`, populated `dns_current`, and an
   enriched OS doc (non-empty `a_records`/`nameservers`).
4. Unit tests pass under the existing CI gate.
5. Both timers enabled; first scheduled run succeeds (checkpoint `status='ok'`).
6. README + this spec updated; attribution footer extended.

## 10. Roadmap (subsequent specs)

- **SP2 — Pivot API** (`domain-search-pro`): `/api/v1/pivot/ip/{ip}`,
  `/pivot/ns/{host}`, `/pivot/mx/{host}`, `/domain/{apex}/dns-history`; fix
  `/api/v1/search` 503 by wiring it to the enriched corpus.
- **SP3 — Investigator UI:** pivot panels, DNS-history timeline, shared-infrastructure
  clustering, export.
- **SP4 — Remaining OpenINTEL stubs:** `infrastructure`, `ctlog`, `rdns`, `prefix`.
