# OpenINTEL Resolved-DNS Foundation — Implementation Plan

> **✅ STATUS: SHIPPED & DEPLOYED (2026-05-31).** All 8 tasks complete; merged to
> `main` via **PR #20** (squash `6c833fb`). Task 7 live `.li` smoke passed (17.4M
> records loaded, ~1.05M OpenSearch docs enriched, IP/NS reverse pivots verified).
> Final pre-merge refinement: OpenSearch enrichment groups per-apex **server-side
> in ClickHouse** (`arrayDistinct(groupArrayIf(...))` + `basis` filter +
> `query_row_block_stream`) to bound ingest-container memory on large zones.
> Downstream app integration shipped separately in domain-search-pro **PR #86**
> (IP co-host pivot → `dns_records`, MX surfaced, NS-set cohort). Checkboxes below
> ticked to reflect completion.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `openintel-toplist` + `openintel-zonefile` ingest pipelines to land OpenINTEL resolved DNS (A/AAAA/NS/MX/CNAME/TXT/SOA + DNSSEC presence, with IP→ASN→country attribution) into ClickHouse on the 2 TB scratch disk, and enrich OpenSearch domain docs with live infrastructure fields.

**Architecture:** ClickHouse-centric. ClickHouse reads OpenINTEL Parquet server-side via the `url(…, Parquet)` table function into `dabi.dns_records` (history, on a `scratch` storage policy backed by `/mnt/scratch`) with a reverse-pivot projection; a `dabi.dns_current` ReplacingMergeTree holds latest state and drives OpenSearch enrichment. The two stub pipelines are replaced by thin wrappers over a shared `_fdns_common` module.

**Tech Stack:** Python 3.14 (argparse dispatcher `python -m dabi_ingest <name>`), `clickhouse-connect` (HTTP client, port 8123), `opensearch-py` (`parallel_bulk`), `requests`, `structlog`, pytest. ClickHouse 25.8, OpenSearch 3. Repo `s4njy-k/dabi-deploy`, package `ingest/dabi_ingest/`.

**Spec:** `docs/superpowers/specs/2026-05-30-openintel-resolved-dns-foundation-design.md`

**Test environment:** Pure-function unit tests run in a venv:
`python3 -m venv /tmp/oi-venv && /tmp/oi-venv/bin/pip install -r ingest/requirements.txt pytest` then
`cd ingest && /tmp/oi-venv/bin/python -m pytest tests/ -v`. Integration ("live smoke") runs the built image against the live stack on the VM and is called out explicitly where used.

**Branch:** `feat/openintel-resolved-dns` (already created; spec committed).

---

## File Structure

| File | Responsibility |
|---|---|
| `config/clickhouse/config.d/storage.xml` (create) | Define ClickHouse disk `scratch` (`/var/lib/clickhouse-scratch`) + storage policy `scratch`. |
| `docker-compose.yml` (modify, `analytics` service) | Bind-mount `/mnt/scratch/clickhouse:/var/lib/clickhouse-scratch`. |
| `ingest/dabi_ingest/pipelines/_fdns_common.py` (create) | Shared forward-DNS logic: terms-cookie session, source/part discovery, schema DDL, load-SQL builder, `dns_current` upsert, OS enrichment, `run_fdns()` orchestrator. |
| `ingest/dabi_ingest/pipelines/openintel_zonefile.py` (replace stub) | Thin wrapper: `basis="zonefile"`, default zone sources, calls `run_fdns`. |
| `ingest/dabi_ingest/pipelines/openintel_toplist.py` (replace stub) | Thin wrapper: `basis="toplist"`, default toplist sources, calls `run_fdns`. |
| `ingest/tests/test_fdns_common.py` (create) | Unit tests for the pure functions in `_fdns_common`. |
| `scripts/run-openintel-toplist-pipeline.sh` (create) | systemd wrapper: `docker run dabi-ingest:local openintel-toplist`. |
| `scripts/run-openintel-zonefile-pipeline.sh` (create) | systemd wrapper for zonefile. |
| `systemd/dabi-ingest-openintel-toplist.{service,timer}` (verify/fix) | Dedicated service + daily 04:30 UTC timer. |
| `systemd/dabi-ingest-openintel-zonefile.{service,timer}` (verify/fix) | Dedicated service + daily 05:30 UTC timer. |
| `scripts/install-systemd.sh` (modify) | Add both timers to `PRODUCTION_TIMERS`. |
| `README.md` (modify) | Document the two pipelines + attribution. |

---

## Task 1: ClickHouse `scratch` storage policy on the 2 TB disk

**Files:**
- Create: `config/clickhouse/config.d/storage.xml`
- Modify: `docker-compose.yml` (`analytics` service `volumes:`)

- [x] **Step 1: Create the host directory for the scratch ClickHouse data**

Run (on VM):
```bash
sudo mkdir -p /mnt/scratch/clickhouse
sudo chown 101:101 /mnt/scratch/clickhouse   # clickhouse UID:GID per bootstrap (dabi_clickhouse=101)
```
Expected: directory exists, owned by 101:101.

- [x] **Step 2: Write the storage policy config**

Create `config/clickhouse/config.d/storage.xml`:
```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <scratch>
                <path>/var/lib/clickhouse-scratch/</path>
            </scratch>
        </disks>
        <policies>
            <scratch>
                <volumes>
                    <main>
                        <disk>scratch</disk>
                    </main>
                </volumes>
            </scratch>
        </policies>
    </storage_configuration>
</clickhouse>
```

- [x] **Step 3: Add the bind mount to the analytics service**

In `docker-compose.yml`, under the `analytics` service `volumes:` list (alongside the existing `/srv/dabi/clickhouse:/var/lib/clickhouse` bind), add:
```yaml
      - /mnt/scratch/clickhouse:/var/lib/clickhouse-scratch
```
(The `config.d` directory is already bind-mounted, so `storage.xml` is picked up automatically.)

- [x] **Step 4: Recreate analytics and verify the disk is registered**

Run (on VM):
```bash
cd /srv/dabi/deploy && docker compose up -d analytics
sleep 8
docker exec dabi-analytics clickhouse-client -q "SELECT name, path, free_space>0 FROM system.disks FORMAT TabSeparated"
```
Expected: rows include `default` AND `scratch  /var/lib/clickhouse-scratch/  1`. Existing data layer stays healthy (`docker compose ps` shows analytics healthy).

- [x] **Step 5: Commit**

```bash
git add config/clickhouse/config.d/storage.xml docker-compose.yml
git commit -m "feat(analytics): add scratch storage policy on 2TB disk for resolved-DNS tables"
```

---

## Task 2: Forward-DNS schema DDL (`ensure_schema`)

**Files:**
- Create: `ingest/dabi_ingest/pipelines/_fdns_common.py` (start the module)
- Create: `ingest/tests/test_fdns_common.py`

- [x] **Step 1: Write the failing test for the DDL constants**

Create `ingest/tests/test_fdns_common.py`:
```python
from dabi_ingest.pipelines import _fdns_common as fc


def test_ddl_records_targets_scratch_and_partitions_monthly():
    ddl = fc.DDL_RECORDS
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_records" in ddl
    assert "storage_policy = 'scratch'" in ddl
    assert "PARTITION BY toYYYYMM(observed_date)" in ddl
    assert "ORDER BY (apex, query_type, response)" in ddl
    assert "TTL observed_date + INTERVAL" in ddl


def test_ddl_projection_orders_by_response_for_reverse_pivots():
    assert "proj_by_response" in fc.DDL_PROJECTION
    assert "ORDER BY (response, query_type, apex)" in fc.DDL_PROJECTION


def test_ddl_current_is_replacing_mergetree():
    ddl = fc.DDL_CURRENT
    assert "CREATE TABLE IF NOT EXISTS dabi.dns_current" in ddl
    assert "ReplacingMergeTree(last_seen)" in ddl
    assert "storage_policy = 'scratch'" in ddl
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dabi_ingest.pipelines._fdns_common'`.

- [x] **Step 3: Write the module with DDL constants**

Create `ingest/dabi_ingest/pipelines/_fdns_common.py`:
```python
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
```

- [x] **Step 4: Run test to verify it passes**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -v`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
git add ingest/dabi_ingest/pipelines/_fdns_common.py ingest/tests/test_fdns_common.py
git commit -m "feat(fdns): resolved-DNS ClickHouse schema (dns_records + projection + dns_current)"
```

---

## Task 3: Source & part discovery

**Files:**
- Modify: `ingest/dabi_ingest/pipelines/_fdns_common.py`
- Modify: `ingest/tests/test_fdns_common.py`

- [x] **Step 1: Write failing tests for the pure parsers**

Append to `ingest/tests/test_fdns_common.py`:
```python
def test_parse_sources_from_listing_html():
    html = '<a href="/download/forward-dns/basis=toplist/source=tranco">x</a>' \
           '<a href="/download/forward-dns/basis=toplist/source=umbrella">y</a>' \
           '<a href="/download/forward-dns/basis=toplist">parent</a>'
    assert fc.parse_sources(html) == ["tranco", "umbrella"]


def test_parse_parts_from_leaf_html_keeps_only_object_parquet():
    html = (
        'junk <a href="https://object.openintel.nl/openintel-public/fdns/'
        'basis=zonefile/source=li/year=2026/month=05/day=29/part-00002-abc.c000.gz.parquet">f</a> '
        '<a href="/download/forward-dns/basis=zonefile/source=li">nav</a>'
    )
    parts = fc.parse_parts(html)
    assert parts == [
        "https://object.openintel.nl/openintel-public/fdns/basis=zonefile/"
        "source=li/year=2026/month=05/day=29/part-00002-abc.c000.gz.parquet"
    ]


def test_listing_url_for_builds_encoded_path():
    url = fc.listing_url_for("zonefile", "li", 2026, 5, 29)
    assert url == (
        "https://openintel.nl/download/forward-dns/basis=zonefile/source=li/"
        "year%3D2026/month%3D05/day%3D29/"
    )
```

- [x] **Step 2: Run to verify failure**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k "parse or listing_url" -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'parse_sources'`.

- [x] **Step 3: Implement the discovery helpers**

Append to `ingest/dabi_ingest/pipelines/_fdns_common.py`:
```python
import datetime
import re

import requests

_SOURCE_RE_TMPL = r'href="/download/forward-dns/basis={basis}/source=([a-z0-9.\-]+)"'
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
```

- [x] **Step 4: Run to verify pass**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k "parse or listing_url" -v`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add ingest/dabi_ingest/pipelines/_fdns_common.py ingest/tests/test_fdns_common.py
git commit -m "feat(fdns): OpenINTEL source/part discovery (terms cookie + listing nav)"
```

---

## Task 4: Load-SQL builder and `dns_current` upsert

**Files:**
- Modify: `ingest/dabi_ingest/pipelines/_fdns_common.py`
- Modify: `ingest/tests/test_fdns_common.py`

- [x] **Step 1: Write failing tests for the SQL builders**

Append to `ingest/tests/test_fdns_common.py`:
```python
def test_build_insert_sql_maps_value_columns_and_filters():
    sql = fc.build_insert_sql(
        part_url="https://object.openintel.nl/openintel-public/fdns/basis=zonefile/"
                 "source=li/year=2026/month=05/day=29/part-x.gz.parquet",
        basis="zonefile",
        source="li",
        date="2026-05-29",
    )
    # apex derivation + trailing-dot strip
    assert "cutToFirstSignificantSubdomain" in sql
    # value normalization per response_type
    assert "response_type = 'A'" in sql and "ip4_address" in sql
    assert "ns_address" in sql and "mx_address" in sql
    # only the record types we care about
    assert "response_type IN ('A','AAAA','NS','MX','CNAME','DNAME','TXT','SOA','DS','DNSKEY')" in sql
    # drops empty-value rows
    assert "response != ''" in sql
    # literal partition columns
    assert "'zonefile'" in sql and "'li'" in sql and "toDate('2026-05-29')" in sql
    assert "url('https://object.openintel.nl/openintel-public/fdns/basis=zonefile/" in sql


def test_build_current_upsert_sql_uses_date_and_min_first_seen():
    sql = fc.build_current_upsert_sql("2026-05-29")
    assert "INSERT INTO dabi.dns_current" in sql
    assert "FROM dabi.dns_records" in sql
    assert "observed_date = toDate('2026-05-29')" in sql
    assert "GROUP BY apex, query_type, response" in sql
```

- [x] **Step 2: Run to verify failure**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k "build_insert or build_current" -v`
Expected: FAIL — `AttributeError: ... 'build_insert_sql'`.

- [x] **Step 3: Implement the SQL builders**

Append to `ingest/dabi_ingest/pipelines/_fdns_common.py`:
```python
_TYPES_SQL = "(" + ",".join(f"'{t}'" for t in RECORD_TYPES) + ")"


def build_insert_sql(part_url: str, basis: str, source: str, date: str) -> str:
    """INSERT … SELECT that reads one OpenINTEL Parquet part server-side and normalizes it.

    response is the type-appropriate value column; host-valued columns have their trailing
    dot stripped. DS/DNSKEY rows carry a key-tag/algorithm marker so has_dnssec can be derived.
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
```

Note: `as` is a ClickHouse keyword, so it is back-quoted as `` `as` ``.

- [x] **Step 4: Run to verify pass**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k "build_insert or build_current" -v`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add ingest/dabi_ingest/pipelines/_fdns_common.py ingest/tests/test_fdns_common.py
git commit -m "feat(fdns): server-side Parquet load SQL + dns_current upsert builders"
```

---

## Task 5: OpenSearch enrichment body builder

**Files:**
- Modify: `ingest/dabi_ingest/pipelines/_fdns_common.py`
- Modify: `ingest/tests/test_fdns_common.py`

- [x] **Step 1: Write failing test for the enrichment doc builder**

Append to `ingest/tests/test_fdns_common.py`:
```python
def test_enrich_doc_groups_records_into_os_fields():
    rows = [
        # (query_type, response)
        ("A", "64.190.63.222"),
        ("A", "64.190.63.223"),
        ("AAAA", "2001:db8::1"),
        ("NS", "ns1.sedoparking.com"),
        ("NS", "ns2.sedoparking.com"),
        ("MX", "mail.example.li"),
        ("DNSKEY", "8"),
    ]
    doc = fc.build_enrich_doc("coinstrader24.li", rows, "2026-05-29")
    assert sorted(doc["a_records"]) == ["64.190.63.222", "64.190.63.223"]
    assert doc["aaaa_records"] == ["2001:db8::1"]
    assert sorted(doc["nameservers"]) == ["ns1.sedoparking.com", "ns2.sedoparking.com"]
    assert doc["ns_apex"] == "sedoparking.com"
    assert doc["has_dnssec"] is True
    assert doc["mx_records"] == ["mail.example.li"]
    assert doc["record_count"] == 7
    assert doc["snapshot_date"] == "2026-05-29"


def test_enrich_doc_no_dnssec_when_absent():
    doc = fc.build_enrich_doc("x.li", [("A", "1.2.3.4")], "2026-05-29")
    assert doc["has_dnssec"] is False
    assert doc["nameservers"] == []
    assert doc["ns_apex"] == ""
```

- [x] **Step 2: Run to verify failure**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k enrich -v`
Expected: FAIL — `AttributeError: ... 'build_enrich_doc'`.

- [x] **Step 3: Implement the enrichment builder + applier**

Append to `ingest/dabi_ingest/pipelines/_fdns_common.py`:
```python
from opensearchpy.helpers import parallel_bulk


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
    # apex of the first nameserver (PSL), e.g. ns1.sedoparking.com -> sedoparking.com
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

    Only apexes already present in OpenSearch are touched (doc_as_upsert is NOT used, so
    new domains are not created here — name indexing is the cctld/czds pipelines' job).
    """
    q = (
        "SELECT apex, query_type, response FROM dabi.dns_records "
        "WHERE observed_date = toDate({d:String}) "
        "AND query_type IN ('A','AAAA','NS','MX','DS','DNSKEY') "
        "ORDER BY apex"
    )
    if limit:
        q += f" LIMIT {int(limit)}"
    result = ch.query(q, parameters={"d": date})

    # group consecutive rows by apex
    import itertools

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
```

Note: `parallel_bulk` `update` against a missing `_id` returns a (benign) document-missing error and is counted as not-ok; that is the intended "don't create new docs" behavior.

- [x] **Step 4: Run to verify pass**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k enrich -v`
Expected: PASS.

- [x] **Step 5: Add `mx_records` to the OpenSearch mapping (so enrichment field is typed)**

The cctld/czds mapping has `a_records`, `aaaa_records`, `nameservers`, `ns_apex`, `has_dnssec` but no `mx_records`. Because OpenSearch dynamic mapping would infer `keyword` anyway, this is safe, but make it explicit on the live cluster:
```bash
docker exec dabi-search curl -s -X PUT "http://localhost:9200/dabi-domains/_mapping" \
  -H 'Content-Type: application/json' \
  -d '{"properties":{"mx_records":{"type":"keyword"}}}'
```
Expected: `{"acknowledged":true}` (run after Task 8 indices exist; harmless to re-run).

- [x] **Step 6: Commit**

```bash
git add ingest/dabi_ingest/pipelines/_fdns_common.py ingest/tests/test_fdns_common.py
git commit -m "feat(fdns): OpenSearch infra enrichment (a/aaaa/ns/mx/ns_apex/has_dnssec)"
```

---

## Task 6: `run_fdns` orchestrator + the two pipeline wrappers

**Files:**
- Modify: `ingest/dabi_ingest/pipelines/_fdns_common.py`
- Modify: `ingest/tests/test_fdns_common.py`
- Replace: `ingest/dabi_ingest/pipelines/openintel_zonefile.py`
- Replace: `ingest/dabi_ingest/pipelines/openintel_toplist.py`

- [x] **Step 1: Write failing test for `add_fdns_args` defaults**

Append to `ingest/tests/test_fdns_common.py`:
```python
import argparse


def test_add_fdns_args_sets_expected_defaults():
    p = argparse.ArgumentParser()
    fc.add_fdns_args(p, default_sources=["li", "se"])
    ns = p.parse_args([])
    assert ns.sources is None              # None => auto-discover
    assert ns.look_back == 7
    assert ns.disk_max_pct == 85
    assert ns.skip_opensearch is False
    assert ns.force is False
    ns2 = p.parse_args(["--sources", "li", "--skip-opensearch"])
    assert ns2.sources == ["li"]
    assert ns2.skip_opensearch is True
```

- [x] **Step 2: Run to verify failure**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k add_fdns_args -v`
Expected: FAIL — `AttributeError: ... 'add_fdns_args'`.

- [x] **Step 3: Implement `add_fdns_args` + `run_fdns`**

Append to `ingest/dabi_ingest/pipelines/_fdns_common.py`:
```python
import argparse

from dabi_ingest import checkpoint, clients


def _default_partition_date() -> str:
    return (datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=1)).isoformat()


def add_fdns_args(parser: argparse.ArgumentParser, default_sources: list[str]) -> None:
    parser.add_argument("--partition-date", default=_default_partition_date(),
                        help="Snapshot date YYYY-MM-DD. Default: yesterday UTC.")
    parser.add_argument("--sources", nargs="+", default=None, metavar="SRC",
                        help=f"Sources to ingest. Default: auto-discover (fallback {default_sources}).")
    parser.add_argument("--look-back", type=int, default=7, metavar="DAYS")
    parser.add_argument("--disk-max-pct", type=int, default=85, metavar="PCT",
                        help="Abort if /mnt/scratch exceeds this %% used.")
    parser.add_argument("--enrich-limit", type=int, default=None, metavar="N",
                        help="Cap OS enrichment rows (debug).")
    parser.add_argument("--skip-opensearch", action="store_true")
    parser.add_argument("--force", action="store_true")


def _scratch_pct(ch) -> int:
    """Percent used of the ClickHouse `scratch` disk (queried from CH, not the local FS —
    the ingest container does not mount /mnt/scratch; only the analytics container does)."""
    rows = ch.query(
        "SELECT total_space, free_space FROM system.disks WHERE name = 'scratch'"
    ).result_rows
    if not rows or not rows[0][0]:
        return 0
    total, free = rows[0]
    return round((total - free) / total * 100)


def run_fdns(basis: str, default_sources: list[str], args: argparse.Namespace) -> int:
    date = args.partition_date
    target = datetime.date.fromisoformat(date)
    log_ = log.bind(basis=basis, partition_date=date)

    if not args.force and checkpoint.is_done(f"openintel-{basis}", date):
        log_.info("checkpoint.skip", reason="already done")
        return 0

    ch = clients.clickhouse()
    ensure_schema(ch)

    pct = _scratch_pct(ch)
    if pct >= args.disk_max_pct:
        log_.error("diskguard.abort", scratch_pct=pct, limit=args.disk_max_pct)
        return 1

    sess = make_session()

    sources = args.sources or discover_sources(sess, basis) or default_sources
    log_.info("sources.resolved", sources=sources)

    with checkpoint.run(f"openintel-{basis}", date) as cp:
        total = 0
        for source in sources:
            slog = log_.bind(source=source)
            found = discover_parts(sess, basis, source, target, args.look_back)
            if not found:
                slog.warning("no_data", look_back=args.look_back)
                continue
            parts, data_date = found
            slog.info("download.start", parts=len(parts), data_date=data_date.isoformat())
            for part_url in parts:
                ch.command(build_insert_sql(part_url, basis, source, date))
            mid = _scratch_pct(ch)
            if mid >= args.disk_max_pct:
                slog.error("diskguard.abort_mid_batch", scratch_pct=mid)
                return 1
        # roll current-state once for the whole day
        ch.command(build_current_upsert_sql(date))
        total = ch.query(
            "SELECT count() FROM dabi.dns_records WHERE observed_date = toDate({d:String}) AND basis = {b:String}",
            parameters={"d": date, "b": basis},
        ).result_rows[0][0]
        cp.set_rows(total)
        log_.info("load.done", rows=total)

        if not args.skip_opensearch:
            os_client = clients.opensearch()
            enrich_opensearch(os_client, ch, date, log_, limit=args.enrich_limit)

    return 0
```

- [x] **Step 4: Run to verify pass**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/test_fdns_common.py -k add_fdns_args -v`
Expected: PASS.

- [x] **Step 5: Replace the zonefile stub with the wrapper**

Replace the entire contents of `ingest/dabi_ingest/pipelines/openintel_zonefile.py`:
```python
"""OpenINTEL forward-DNS zone-based — daily resolved records for whole public TLD zones."""

from __future__ import annotations

import argparse

from dabi_ingest.pipelines import _fdns_common as fc

PIPELINE = "openintel-zonefile"
DESCRIPTION = "OpenINTEL forward-DNS zone-based — resolved A/AAAA/NS/MX/CNAME/TXT/SOA per apex for public zones."

# Public zone-based sources on object.openintel.nl (auto-discovery is preferred; this is the fallback).
DEFAULT_SOURCES = ["ch", "ee", "fr", "se", "sk", "li", "nu", "gov", "fed.us", "root"]


def add_args(parser: argparse.ArgumentParser) -> None:
    fc.add_fdns_args(parser, DEFAULT_SOURCES)


def run(args: argparse.Namespace) -> int:
    return fc.run_fdns("zonefile", DEFAULT_SOURCES, args)
```

- [x] **Step 6: Replace the toplist stub with the wrapper**

Replace the entire contents of `ingest/dabi_ingest/pipelines/openintel_toplist.py`:
```python
"""OpenINTEL forward-DNS top-lists — daily resolved records for ranked popular-domain lists."""

from __future__ import annotations

import argparse

from dabi_ingest.pipelines import _fdns_common as fc

PIPELINE = "openintel-toplist"
DESCRIPTION = "OpenINTEL forward-DNS top-lists — resolved records for alexa/crux/majestic/radar/tranco/umbrella."

DEFAULT_SOURCES = ["alexa", "crux", "majestic", "radar", "tranco", "umbrella"]


def add_args(parser: argparse.ArgumentParser) -> None:
    fc.add_fdns_args(parser, DEFAULT_SOURCES)


def run(args: argparse.Namespace) -> int:
    return fc.run_fdns("toplist", DEFAULT_SOURCES, args)
```

- [x] **Step 7: Verify the dispatcher still imports every pipeline**

Run: `cd ingest && /tmp/oi-venv/bin/python -c "from dabi_ingest.__main__ import _build_parser; _build_parser(); print('ok')"`
Expected: prints `ok` (no ImportError; both modules expose `add_args`/`run`/`PIPELINE`).

- [x] **Step 8: Run the full unit suite**

Run: `cd ingest && /tmp/oi-venv/bin/python -m pytest tests/ -v`
Expected: all PASS.

- [x] **Step 9: Commit**

```bash
git add ingest/dabi_ingest/pipelines/_fdns_common.py ingest/dabi_ingest/pipelines/openintel_zonefile.py ingest/dabi_ingest/pipelines/openintel_toplist.py ingest/tests/test_fdns_common.py
git commit -m "feat(fdns): run_fdns orchestrator + openintel-zonefile/-toplist pipelines"
```

---

## Task 7: Build image, live smoke on `.li`

**Files:** none (verification task on VM).

- [x] **Step 1: Build the local ingest image**

Run (on VM):
```bash
docker build -t dabi-ingest:local /srv/dabi/deploy/ingest/
```
Expected: build succeeds (duckdb/clickhouse-connect wheels resolve on py3.14 ARM64 per requirements.txt).

- [x] **Step 2: Run zonefile for the single small `.li` source**

The compose `ingest` service is the *domain-search-pro* image (`${INGEST_SHA}`), which does
NOT contain these pipelines — so run the locally-built image directly with `docker run`,
mirroring `scripts/run-openintel-cctld-pipeline.sh` (network `deploy_dabi-net`, checkpoints
at `/srv/dabi/checkpoints`, secrets at `/run/dabi/secrets`):
```bash
docker run --rm \
  --network deploy_dabi-net \
  -v /srv/dabi/checkpoints:/checkpoints \
  -v /run/dabi/secrets:/run/secrets:ro \
  -e DABI_CH_URL=http://analytics:8123 \
  -e DABI_OS_URL=http://search:9200 \
  dabi-ingest:local \
  openintel-zonefile --sources li --partition-date 2026-05-29 --force
```
Expected: JSON logs end with `load.done` (rows > 0) then `enrich.done`. Exit 0.

- [x] **Step 3: Verify ClickHouse rows landed on the scratch disk**

Run:
```bash
docker exec dabi-analytics clickhouse-client -q "
SELECT
  (SELECT count() FROM dabi.dns_records WHERE source='li') AS records,
  (SELECT count() FROM dabi.dns_current WHERE apex LIKE '%.li') AS current_li,
  (SELECT count() FROM dabi.dns_records WHERE source='li' AND query_type='A') AS a_records,
  (SELECT countDistinct(asn) FROM dabi.dns_records WHERE source='li' AND asn>0) AS distinct_asns
FORMAT Vertical"
```
Expected: `records` in the ~10^5–10^6 range, `current_li` > 0, `a_records` > 0, `distinct_asns` > 0.

- [x] **Step 4: Verify a reverse pivot works (IP → domains)**

Run:
```bash
docker exec dabi-analytics clickhouse-client -q "
SELECT response AS ip, count() AS domains
FROM dabi.dns_records
WHERE source='li' AND query_type='A'
GROUP BY response ORDER BY domains DESC LIMIT 5 FORMAT TabSeparated"
```
Expected: a small table of shared-hosting IPs with their domain counts (proves the projection path / pivot capability).

- [x] **Step 5: Verify OpenSearch enrichment (only if a `.li` doc exists)**

Run:
```bash
# pick an apex that exists in OS (li domains may not be in dabi-domains yet; this is best-effort)
docker exec dabi-search curl -s "http://localhost:9200/dabi-domains/_search" -H 'Content-Type: application/json' \
  -d '{"size":1,"query":{"bool":{"filter":[{"term":{"tld":"li"}},{"exists":{"field":"a_records"}}]}},"_source":["fqdn","a_records","nameservers","ns_apex","has_dnssec"]}' | head -c 800
```
Expected: if any `.li` doc exists in OS, it now shows non-empty `a_records`/`nameservers`. If `.li` is not in the name corpus, enrichment is a no-op (expected) — note this and rely on Step 3/4 as the smoke pass.

- [x] **Step 6: No commit** (verification only). If any step fails, fix the relevant Task 2–6 module before proceeding.

---

## Task 8: systemd wiring, CI, README, attribution

**Files:**
- Create: `scripts/run-openintel-toplist-pipeline.sh`, `scripts/run-openintel-zonefile-pipeline.sh`
- Verify/fix: `systemd/dabi-ingest-openintel-toplist.{service,timer}`, `systemd/dabi-ingest-openintel-zonefile.{service,timer}`
- Modify: `scripts/install-systemd.sh`, `README.md`

- [x] **Step 1: Write the two wrapper scripts**

These mirror `scripts/run-openintel-cctld-pipeline.sh` exactly (network `deploy_dabi-net`,
checkpoints `/srv/dabi/checkpoints`, secrets `/run/dabi/secrets`, sources `.env`).

Create `scripts/run-openintel-zonefile-pipeline.sh`:
```bash
#!/bin/bash
# run-openintel-zonefile-pipeline.sh — OpenINTEL forward-DNS zone-based daily ingest.
# Uses dabi-ingest:local (the dabi-deploy/ingest/ image), NOT INGEST_SHA. Rebuild after
# code changes: docker build -t dabi-ingest:local /srv/dabi/deploy/ingest/
set -euo pipefail

if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

TODAY=$(date -u +%Y-%m-%d)
echo "[openintel-zonefile] === ingesting resolved DNS for public zones, ${TODAY} ==="
docker run --rm \
  --network deploy_dabi-net \
  -v /srv/dabi/checkpoints:/checkpoints \
  -v /run/dabi/secrets:/run/secrets:ro \
  -e DABI_CH_URL=http://analytics:8123 \
  -e DABI_OS_URL=http://search:9200 \
  -e DABI_DNS_RETAIN_DAYS="${DABI_DNS_RETAIN_DAYS:-180}" \
  dabi-ingest:local \
  openintel-zonefile --partition-date "${TODAY}"
echo "[openintel-zonefile] done."
```
Create `scripts/run-openintel-toplist-pipeline.sh` — identical except the echo label and the
final subcommand `openintel-toplist`.
Then: `chmod +x scripts/run-openintel-*.sh`.

- [x] **Step 2: Ensure the dedicated service units call the wrappers**

Confirm `systemd/dabi-ingest-openintel-zonefile.service` `ExecStart` is
`/srv/dabi/deploy/scripts/run-openintel-zonefile-pipeline.sh` (Type=oneshot), and the
`.timer` `OnCalendar=*-*-* 05:30:00` (UTC). Same for toplist at `04:30:00`. If they
still use the generic `dabi-ingest@.service` template, replace with dedicated units
modeled on `dabi-ingest-openintel-cctld.{service,timer}`.

- [x] **Step 3: Add both timers to install-systemd.sh PRODUCTION_TIMERS**

In `scripts/install-systemd.sh`, add to the `PRODUCTION_TIMERS` array:
```bash
  dabi-ingest-openintel-toplist.timer
  dabi-ingest-openintel-zonefile.timer
```

- [x] **Step 4: Install + enable the timers**

Run (on VM):
```bash
sudo /srv/dabi/deploy/scripts/install-systemd.sh
systemctl list-timers 'dabi-ingest-openintel-*' --all --no-pager
```
Expected: `dabi-ingest-openintel-toplist.timer` and `-zonefile.timer` now `enabled` with NEXT times at 04:30 / 05:30 UTC.

- [x] **Step 5: Update CI trigger (already covers `ingest/**`)**

Confirm `.github/workflows/ci-ingest.yml` triggers on `ingest/**` (it does). No change needed unless tests aren't run there — if the workflow only builds, add a step before build:
```yaml
      - name: Unit tests
        run: |
          python -m pip install -r ingest/requirements.txt pytest
          cd ingest && python -m pytest tests/ -v
```

- [x] **Step 6: Update README + attribution**

In `README.md`, under the ingest-pipelines section, add rows for `openintel-toplist`
(daily 04:30 UTC, 6 toplists) and `openintel-zonefile` (daily 05:30 UTC, 10 public zones),
their ClickHouse tables (`dabi.dns_records`, `dabi.dns_current`) and the env knob
`DABI_DNS_RETAIN_DAYS`. Extend the OpenINTEL attribution note to mention the forward-DNS
datasets (CC BY-NC-SA 4.0, Univ. of Twente / SIDN / NLnet Labs / SURF).

- [x] **Step 7: Commit**

```bash
git add scripts/run-openintel-toplist-pipeline.sh scripts/run-openintel-zonefile-pipeline.sh \
        systemd/dabi-ingest-openintel-toplist.service systemd/dabi-ingest-openintel-toplist.timer \
        systemd/dabi-ingest-openintel-zonefile.service systemd/dabi-ingest-openintel-zonefile.timer \
        scripts/install-systemd.sh README.md .github/workflows/ci-ingest.yml
git commit -m "feat(fdns): systemd timers + wrappers + CI tests + docs for openintel toplist/zonefile"
```

- [x] **Step 8: Push branch + open PR**

```bash
git push -u origin feat/openintel-resolved-dns
gh pr create --fill --base main
```
(Per repo convention, push from the VM via the configured SSH remote. CI builds + pushes the new `dabi-ingest:<sha>` image on merge to main.)

---

## Self-Review Notes (author)

- **Spec coverage:** §3 storage→Task 1; §4 schema→Task 2; §5.1 load→Task 4; §5.2 args→Task 6; §5.3 enrichment→Task 5; §6 operational→Tasks 7–8; §7 testing→unit tests in Tasks 2–6 + live smoke Task 7. All covered.
- **No placeholders:** all code/SQL is concrete; the only `<…>` are inside f-string templates that the builders fill at runtime, tested in Task 4.
- **Type/name consistency:** `build_insert_sql`, `build_current_upsert_sql`, `build_enrich_doc`, `enrich_opensearch`, `add_fdns_args`, `run_fdns`, `ensure_schema`, `make_session`, `discover_sources`, `discover_parts`, `parse_sources`, `parse_parts`, `listing_url_for` — referenced consistently across tasks. Column set (`apex, query_name, query_type, response, ttl, country, asn, asn_name, ip_prefix, basis, source, observed_date`) identical in DDL (Task 2) and INSERT (Task 4).
- **Known soft spots flagged in-plan:** compose `ingest` service image vs `dabi-ingest:local` (Task 7 Step 2 note); `.li` may be absent from the OS name corpus so enrichment can be a legitimate no-op (Task 7 Step 5).
