# DABI — Domain Abuse Intelligence

Production deployment of a threat-intelligence platform that surfaces malicious
and impersonation domains in near-real time. Targets the Indian banking and
critical-infrastructure threat surface. Built and operated by a single
maintainer on Google Cloud (asia-south1).

- **Live:** https://34-180-2-141.sslip.io
- **Status:** Production (single-tenant; auth-gated)
- **Plan v8 / Runbook v5** are the authoritative specifications. This document
  describes the *implemented* system as of the date in the header of each
  section.

---

## Table of contents

1. [Architecture](#architecture)
2. [Repository layout](#repository-layout)
3. [Tech stack](#tech-stack)
4. [Quick start (fresh VM bootstrap)](#quick-start-fresh-vm-bootstrap)
5. [Development workflow](#development-workflow)
6. [Data pipelines](#data-pipelines)
7. [Operations](#operations)
8. [Security model](#security-model)
9. [Runbooks](#runbooks)
10. [Decision log](#decision-log)
11. [Keeping this document current](#keeping-this-document-current)
12. [License & attribution](#license--attribution)

---

## Architecture

DABI is a single-VM stack. All services run as containers under one Docker
Compose project on a GCP C4A (ARM64) instance with a Hyperdisk Balanced data
volume.

```
                       Internet
                          │
                          ▼  (Cloud NAT for egress; firewall allows 0.0.0.0/0 :80/:443)
                  ┌───────────────┐
                  │  nginx :443   │  ◀── Let's Encrypt cert (90-day, weekly renewal)
                  └───────┬───────┘
                          │ proxy_pass /api/  →  api:8000
                          │ static /          →  /var/www/dabi/current/dist/
                          ▼
                  ┌───────────────┐
                  │  api :8000    │   FastAPI on gunicorn, Python 3.14
                  └────┬────┬─────┘
                       │    │
        ┌──────────────┘    └────────────────────┐
        ▼                                        ▼
┌────────────────┐                       ┌──────────────────┐
│  search :9200  │                       │  analytics :8123 │
│  OpenSearch 3  │                       │  ClickHouse 25.8 │
└────────────────┘                       └──────────────────┘
        ▲                                        ▲
        │   bulk_index                           │   INSERT FORMAT CSV
        │                                        │
        │   ┌──────────────────────────────┐     │
        └───┤  ingest (oneshot containers) ├─────┘
            │  Python 3.14 + dabi-ingest   │
            └─────────────┬────────────────┘
                          ▲
                          │ systemd timers (host)
                          │   czds 01:30, tranco 02:00, dns 02:30,
                          │   rdns 02:50, ctlog 03:00, rir 03:30 UTC
```

**Services**

| Service     | Container    | Image                                   | Role                                    |
|-------------|--------------|-----------------------------------------|-----------------------------------------|
| search      | dabi-search  | `dabi-opensearch:3` (built locally)     | Full-text index, snapshot source        |
| analytics   | dabi-analytics | `clickhouse/clickhouse-server:25.8`   | Analytical store, archive sink          |
| redis       | dabi-redis   | `redis:8.6`                             | Cache, session store, queue             |
| api         | dabi-api     | `dabi/api:<sha>` from Artifact Registry | FastAPI app, gunicorn workers           |
| nginx       | dabi-nginx   | `nginx:1.30`                            | TLS termination + SPA static + reverse-proxy |
| ingest      | (oneshot)    | `dabi/ingest:<sha>` from Artifact Registry | Data extractors, invoked by timers   |

**Storage layout (on the VM)**

| Path                          | Purpose                                       |
|-------------------------------|-----------------------------------------------|
| `/srv/dabi/deploy/`           | This repository (config, scripts, compose)    |
| `/srv/dabi/opensearch/`       | OpenSearch data volume                        |
| `/srv/dabi/clickhouse/`       | ClickHouse data volume                        |
| `/srv/dabi/parquet/`          | Per-TLD parquet snapshots + extractor state  |
| `/srv/dabi/auth/`             | api auth.db (sqlite) + admin bootstrap output |
| `/run/dabi/secrets/`          | tmpfs, populated from Secret Manager          |
| `/var/www/dabi/current/dist/` | Web SPA static assets                         |

**External dependencies**

| External service           | Purpose                                       |
|----------------------------|-----------------------------------------------|
| GCP Secret Manager         | Source of truth for all secrets               |
| Artifact Registry          | api + ingest container images                 |
| GCS `dabi-prod-backup`     | OpenSearch + ClickHouse + Redis + auth.db backups |
| GCS `dabi-prod-backup/web/`| Web SPA tarball builds                        |
| Cloud Monitoring           | Uptime check, alert policies, Ops Agent metrics |
| Let's Encrypt              | TLS cert for the sslip.io hostname            |
| ICANN CZDS                 | Zone-file downloads (per-TLD approval)        |
| `ftp.{arin,ripe,apnic,lacnic,afrinic}.net` | Delegated-extended stats     |
| `tranco-list.eu`           | Daily top-1M domain ranking                   |
| `ct.googleapis.com/logs/us1/argon2026h1/` | Direct CT log polling          |

---

## Repository layout

DABI is split across two repositories. Both are owned by GitHub user `s4njy-k`.

### `s4njy-k/dabi-deploy` — orchestration (this repo)

```
.
├── README.md                       # this file
├── docker-compose.yml              # service definitions; SHAs from .env
├── .env.example                    # template for .env (gitignored)
├── config/
│   ├── opensearch/
│   │   ├── Dockerfile              # bakes repository-gcs into opensearch:3
│   │   └── opensearch.yml          # single-node, bind localhost
│   ├── clickhouse/
│   │   ├── config.d/               # cluster, S3 endpoint
│   │   ├── users.d/                # api + ingest user definitions
│   │   └── init.sql                # schema (runs on fresh data dir only)
│   ├── redis/redis.conf
│   └── nginx/dabi.conf             # reverse-proxy + SPA + HTTPS
├── scripts/
│   ├── bootstrap.sh                # one-shot VM bring-up
│   ├── pull-secrets.sh             # SM → /run/dabi/secrets
│   ├── install-systemd.sh          # selective timer install + enable
│   ├── smoke.sh                    # 10-check stack health probe
│   ├── backup.sh                   # OS snapshot + CH BACKUP + Redis BGSAVE + auth.db
│   ├── pull-web.sh                 # GCS tarball → /var/www/dabi/current/
│   ├── register-snapshot-repo.sh   # one-time OpenSearch repo registration
│   ├── run-czds-pipeline.sh        # wrappers invoked by timers
│   ├── run-tranco-pipeline.sh
│   ├── run-dns-pipeline.sh
│   ├── run-rdns-pipeline.sh
│   ├── run-ctlog-pipeline.sh
│   └── run-rir-pipeline.sh
└── systemd/
    ├── dabi-cert-renew.{service,timer}
    ├── dabi-backup.{service,timer}
    ├── dabi-ingest-czds.{service,timer}
    ├── dabi-ingest-tranco.{service,timer}
    ├── dabi-ingest-dns.{service,timer}
    ├── dabi-ingest-rdns.{service,timer}
    ├── dabi-ingest-ctlog.{service,timer}
    └── dabi-ingest-rir.{service,timer}
```

### `s4njy-k/domain-search-pro` — application + ingest code

```
.
├── api/                            # FastAPI service
│   ├── Dockerfile                  # python:3.14-slim
│   ├── pyproject.toml              # dabi-api + dabi-admin entrypoints
│   └── dabi_api/                   # app code, models, routes, auth
├── ingest/                         # ingest container
│   ├── Dockerfile                  # python:3.14-slim
│   ├── pyproject.toml              # dabi-ingest entrypoint
│   └── dabi_ingest/
│       ├── cli.py                  # Typer dispatcher (run / stage / fetch / …)
│       ├── extract.py              # all extractors (CZDS, Tranco, DNS, rdns, CT, RIR)
│       ├── stages/                 # Stages 2-9 (parse, rollup, enrich, diff, index, swap, stats, archive)
│       ├── rules/                  # scoring + structural signals
│       └── seed/                   # brand reference data
├── web/                            # Vite + React SPA
│   ├── package.json
│   └── src/
└── .github/workflows/
    ├── ci-api.yml                  # ruff + mypy + pytest + build-and-push (main)
    ├── ci-ingest.yml               # same shape for ingest
    └── ci-web.yml                  # npm build + GCS tarball upload (main)
```

CI publishes container images to Artifact Registry tagged with the full
40-character GitHub commit SHA on every push to `main`. Web tarballs go to
`gs://dabi-prod-backup/web/web-<sha>.tar.gz`.

---

## Tech stack

| Layer       | Component        | Pinned version                                  |
|-------------|------------------|-------------------------------------------------|
| OS          | Ubuntu LTS ARM64 | 26.04 LTS (Resolute) on the live VM             |
| Runtime     | Python (api + ingest) | 3.14-slim                                  |
| Web build   | Node.js          | LTS in CI; output is static                     |
| Search      | OpenSearch       | 3.6.x (`opensearchproject/opensearch:3`)         |
| Analytics   | ClickHouse       | 25.8.x                                          |
| Cache       | Redis            | 8.6.x                                           |
| Proxy       | nginx            | 1.30.x                                          |
| Container   | Docker Compose   | v2 (host-installed)                             |
| Cloud       | GCP              | project `dabi-prod-01`, region `asia-south1`     |
| VM shape    | C4A              | `c4a-standard-16-lssd`, 16 vCPU, 64 GiB RAM, ARM64 |
| Data disk   | Hyperdisk Balanced | one volume mounted at `/srv/dabi/`            |

The OpenSearch image is built locally as `dabi-opensearch:3` and extends the
upstream with the `repository-gcs` plugin. Every other container uses upstream
images directly.

---

## Quick start (fresh VM bootstrap)

These steps assume a freshly provisioned C4A VM with the data disk attached at
`/srv/dabi/` and Docker + Google Cloud Ops Agent already installed (see Runbook
v5 Phases 0-4 for the GCP infrastructure setup).

```bash
# 1. Clone this repo
sudo mkdir -p /srv/dabi/deploy && sudo chown $USER:$USER /srv/dabi/deploy
git clone https://github.com/s4njy-k/dabi-deploy.git /srv/dabi/deploy
cd /srv/dabi/deploy

# 2. Bootstrap secrets, env, disks
sudo ./scripts/bootstrap.sh

# 3. Pull images and build the custom OpenSearch image
sudo docker compose pull
sudo docker compose build search

# 4. Bring up the stack
sudo docker compose up -d

# 5. Verify
./scripts/smoke.sh

# 6. Register the OpenSearch snapshot repo (one-time, idempotent)
sudo ./scripts/register-snapshot-repo.sh

# 7. Enable the production timers
sudo ./scripts/install-systemd.sh

# 8. Bootstrap an admin user (interactive password set)
sudo docker compose exec api dabi-admin init-admin <Firstname_Department>
```

After step 7 the data pipelines start running on their daily schedule. The
first 24 hours populate the indices; `/api/v1/search` returns results once the
first CZDS run completes.

---

## Development workflow

### WSL vs VM responsibilities

The repository is developed in **WSL** (Ubuntu) and deployed on the **VM**.
The two environments have different responsibilities and access scopes.

| Operation                                | Where     |
|------------------------------------------|-----------|
| `git push` / `gh pr` / branch work       | WSL       |
| `gcloud` mutations (IAM, monitoring, SM) | WSL (admin auth) |
| `docker compose` / `systemctl`           | VM        |
| Anything touching `/srv/dabi/` or `/run/dabi/` | VM  |
| Reading container logs                    | VM       |
| Image builds (custom OpenSearch)         | VM        |

The VM service account `sa-dabi-vm-runtime@dabi-prod-01.iam.gserviceaccount.com`
is intentionally read-only on Secret Manager and lacks
`compute.instances.get` — it cannot mutate GCP-level resources. All such
mutations come from WSL where `gcloud auth login` carries a human admin
identity.

### Pull-request flow

The two repositories follow identical PR conventions:

1. From WSL clone, branch off `main`: `git checkout -b feat/<short-slug>`
2. Make the change. Run `ruff check`, `ruff format`, and any local tests.
3. `git commit` with a Conventional Commits-style header
   (`feat:`, `fix:`, `style:`, `chore:`, …).
4. `git push -u origin <branch>` and `gh pr create --fill --base main`.
5. CI runs ruff/mypy/pytest. The build-and-push job is `skipped` on PRs
   and runs only on merge to `main`.
6. Branch protection on `domain-search-pro/main` requires the
   *Lint, type-check, test* check from BOTH `ci-api.yml` and `ci-ingest.yml`.
   PRs that touch only one (e.g. ingest-only) trigger only one check; merge
   with `gh pr merge <n> --squash --delete-branch --admin` in that case.
7. On merge, CI builds and pushes the new image to Artifact Registry tagged
   with the merge commit SHA.

### Adding a new ingest extractor

The canonical pattern lives in `domain-search-pro/ingest/dabi_ingest/`. To
add a new source:

1. **Extractor logic** in `extract.py` — add a `fetch_and_load_<source>()`
   function. End-to-end shape: download → parse → bulk-load via
   `_ch_insert_csv` (for ClickHouse) or stage write to parquet (for the
   OpenSearch pipeline).
2. **CLI branch** in `cli.py` inside `cmd_fetch()` — add the source string to
   the guard tuple, read any env knobs, call the extractor, echo a summary.
3. **ClickHouse schema** in `dabi-deploy/config/clickhouse/init.sql` if the
   source needs a new table. `ReplacingMergeTree(observed_date)` with
   `ORDER BY` on the dedup key is the established pattern.
4. **Pipeline wrapper** in `dabi-deploy/scripts/run-<source>-pipeline.sh` —
   sources `.env`, pulls the image, runs the container with profile=scheduled.
5. **systemd units** `dabi-deploy/systemd/dabi-ingest-<source>.{service,timer}`.
   Pick a 30-minute slot in the daily schedule that doesn't conflict.
6. **Timer activation** — add the timer to `PRODUCTION_TIMERS` in
   `scripts/install-systemd.sh`.

Both repos need a PR (one in `domain-search-pro` for the code, one in
`dabi-deploy` for the wrapper/timer). After both merge, on the VM:

```bash
cd /srv/dabi/deploy
sudo git pull
sed -i "s/^INGEST_SHA=.*/INGEST_SHA=<new>/" .env
sudo docker compose pull ingest
sudo ./scripts/install-systemd.sh
sudo /srv/dabi/deploy/scripts/run-<source>-pipeline.sh   # one-shot smoke test
```

### Image SHA management

Two SHAs travel through `.env` on the VM:

```
API_SHA=<full-40-char-merge-commit-sha-on-domain-search-pro>
INGEST_SHA=<full-40-char-merge-commit-sha-on-domain-search-pro>
WEB_SHA=<full-40-char-merge-commit-sha-on-domain-search-pro>
```

When a PR is merged to `main`, the build-and-push CI job tags the new image
with the merge commit's full SHA. To roll out, edit `.env` on the VM,
`docker compose pull <service>`, and `docker compose up -d <service>`. The
custom `dabi-opensearch:3` image is built locally and not tracked by SHA.

### CI gates

| Check                          | Tool             | Workflow file         | Pre-push command (WSL)                |
|--------------------------------|------------------|-----------------------|---------------------------------------|
| Lint                           | ruff             | ci-{api,ingest}.yml   | `ruff check ingest/dabi_ingest`       |
| Format                         | ruff format      | ci-{api,ingest}.yml   | `ruff format --check ingest/dabi_ingest` |
| Type checking                  | mypy `--strict`  | ci-{api,ingest}.yml   | `mypy --strict ingest/dabi_ingest`    |
| Tests                          | pytest           | ci-{api,ingest}.yml   | `pytest ingest/tests`                 |
| Image build + push (main only) | docker buildx    | ci-{api,ingest}.yml   | n/a                                   |
| Web tarball (main only)        | npm build + gsutil | ci-web.yml          | n/a                                   |

`ruff format` operates on its own opinions; pre-running it locally avoids the
"would reformat 1 file" failure that's common after editing in-place. On WSL,
either install ruff via `pipx install ruff` or use a venv
(`python3 -m venv .venv && .venv/bin/pip install ruff`); `pip --user` is
blocked by PEP 668 on modern Ubuntu.

---

## Data pipelines

### Schedule overview (all times UTC)

| Time  | Timer                       | Source / target                                         |
|-------|-----------------------------|---------------------------------------------------------|
| 01:30 | `dabi-ingest-czds.timer`    | ICANN CZDS zone files → OpenSearch `dabi-domains`      |
| 02:00 | `dabi-ingest-tranco.timer`  | `tranco-list.eu` → ClickHouse `dabi.tranco_top1m`      |
| 02:30 | `dabi-ingest-dns.timer`     | dnspython forward resolve → `dabi.dns_observations`    |
| 02:50 | `dabi-ingest-rdns.timer`    | PTR lookup of A-record IPs → `dabi.rdns`                |
| 03:00 | `dabi-ingest-ctlog.timer`   | Google Argon2026h1 CT log → `dabi.ct_fqdn_observations` |
| 03:30 | `dabi-ingest-rir.timer`     | 5 RIRs delegated-extended → `dabi.rir_whois`            |
| hourly | `dabi-backup.timer`        | OS snapshot + CH backup + Redis + auth.db → GCS         |
| Sun 04:00 | `dabi-cert-renew.timer` | certbot webroot renewal                                 |

Each timer is `Persistent=true` with `RandomizedDelaySec=180-300`. A missed
fire triggers immediately on next boot; the random delay spreads load when
multiple timers wake at the same instant.

### Source detail

#### CZDS (zone files)

- **Source:** `https://czds-api.icann.org/czds/downloads/links` — gated by
  per-TLD ICANN approval. Username/password stored in Secret Manager.
- **Approved TLDs:** managed via `DABI_CZDS_TLDS` in `.env` (space-separated).
  Current production set: `online xyz`. Adding a newly-approved TLD is a
  one-line `.env` edit; next 01:30 fire picks it up.
- **Pipeline:** download → BIND parse → label features → parquet at
  `/srv/dabi/parquet/zones/parquet/tld=<tld>/snapshot_date=<date>/records.parquet`
  → Stages 2-9 (parse, rollup, enrich, diff, bulk_index, swap_alias,
  stats_refresh, archive).
- **Output:** OpenSearch index `domains-<tld>-<yyyymmdd>` aliased to
  `domains-<tld>` and the global alias `dabi-domains`.
- **Volume:** `.online` produces ~3.4M unique apex domains; `.xyz` similar.

#### Tranco top-1M

- **Source:** `https://tranco-list.eu/top-1m.csv.zip` — daily snapshot of the
  Tranco research ranking. No auth.
- **Pipeline:** download → CSV parse → bulk insert via ClickHouse HTTP CSV.
- **Output:** `dabi.tranco_top1m (rank, domain, apex, observed_date)`.
  ReplacingMergeTree partitioned by `observed_date`.
- **Volume:** 1,000,000 rows per day. Run time ~3s.

#### DNS forward resolve

- **Source:** Top-N domains from `dabi.tranco_top1m` (latest snapshot).
- **Resolvers:** Cloudflare, Google, Quad9 round-robin via dnspython
  async resolver. Concurrency knob `DABI_DNS_CONCURRENCY` (default 100),
  batch size `DABI_DNS_RESOLVE_LIMIT` (default 10000).
- **Records:** A, AAAA, MX, NS, TXT per FQDN.
- **Output:** `dabi.dns_observations (fqdn, apex, record_type, value, ttl,
  observed_at, observed_date)`. ReplacingMergeTree partitioned by `toYYYYMM`.
- **Volume:** ~20k observations from top-1k Tranco; scales linearly.

#### Reverse DNS

- **Source:** distinct A-record `value`s from the latest `dns_observations`
  snapshot. Reads via parameterized ClickHouse query — no SQLi vector.
- **Pipeline:** dnspython async PTR resolution with semaphore-bounded
  concurrency. Concurrency knob `DABI_RDNS_CONCURRENCY`, limit
  `DABI_RDNS_LIMIT`.
- **Output:** `dabi.rdns (ip4, ptr, ptr_apex, asn, country, snapshot_date,
  observed_at)`. ReplacingMergeTree.
- **Notes:** PTR hit rate is ~30-60% depending on what's in the input
  IPs (residential and CDN ranges return more NXDOMAIN).

#### CT log (Argon2026h1, direct poll)

- **Source:** `https://ct.googleapis.com/logs/us1/argon2026h1/`.
- **Pipeline:**
  1. GET `/ct/v1/get-sth` for current `tree_size`.
  2. Resume from persisted `cert_index` at
     `/srv/dabi/parquet/state/ctlog_last_index.txt`.
  3. Loop `/ct/v1/get-entries?start=N&end=N+batch_count` until batch
     filled or response empty (Google caps batches at ≤1024 entries).
  4. For each MerkleTreeLeaf, parse the certificate (asn1crypto) and
     extract `dNSName` SAN entries.
- **Output:** `dabi.ct_fqdn_observations` raw rows + materialized view
  `dabi.apex_ct_aggregates_mv` rolling them up per apex.
- **State:** persisted index file ensures incremental polling across runs.

#### RIR delegated-extended

- **Sources:** 5 RIRs — URLs hardcoded in `extract._RIR_URL_REGISTRY`:
  - ARIN: `https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest`
  - RIPE: `https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest`
  - APNIC: `https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest`
  - LACNIC: `https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest`
  - AFRINIC: `https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest`
- **Pipeline:** for each source, download → parse pipe-delimited records →
  filter `allocated`/`assigned` → split IPv4 ranges into exact CIDR blocks
  via `ipaddress.summarize_address_range()` → bulk insert.
- **Output:** `dabi.rir_whois (prefix_start, prefix_end, prefix_len, netname,
  country, org, source, last_modified)`. ReplacingMergeTree.
- **Volume:** ~258k CIDR rows total across the 5 RIRs.
- **Env knob:** `DABI_RIR_SOURCES` (space-separated; default
  `"arin ripe apnic lacnic afrinic"`). Per-RIR failures are caught and
  logged — one slow registry does not block the others.

### Schema reference

```sql
-- ClickHouse: all tables under the `dabi.` schema, defined in
-- config/clickhouse/init.sql. Excerpt:

CREATE TABLE dabi.tranco_top1m (
    rank          UInt32 CODEC(T64, LZ4),
    domain        LowCardinality(String) CODEC(ZSTD(3)),
    apex          LowCardinality(String) CODEC(ZSTD(3)),
    observed_date Date
) ENGINE = ReplacingMergeTree(observed_date)
  PARTITION BY observed_date
  ORDER BY (rank, domain);

CREATE TABLE dabi.dns_observations (
    fqdn          String CODEC(ZSTD(3)),
    apex          LowCardinality(String) CODEC(ZSTD(3)),
    record_type   LowCardinality(String),
    value         String CODEC(ZSTD(3)),
    ttl           UInt32,
    observed_at   DateTime,
    observed_date Date
) ENGINE = ReplacingMergeTree(observed_at)
  PARTITION BY toYYYYMM(observed_date)
  ORDER BY (fqdn, record_type, value);

CREATE TABLE dabi.rdns (
    ip4           UInt32 CODEC(T64),
    ptr           String,
    ptr_apex      LowCardinality(String),
    asn           UInt32,
    country       FixedString(2),
    snapshot_date Date,
    observed_at   DateTime
) ENGINE = ReplacingMergeTree(observed_at)
  ORDER BY (ip4, snapshot_date);

CREATE TABLE dabi.rir_whois (
    prefix_start  UInt32 CODEC(T64),
    prefix_end    UInt32 CODEC(T64),
    prefix_len    UInt8,
    netname       String,
    country       FixedString(2),
    org           String,
    source        LowCardinality(String),
    last_modified Date
) ENGINE = ReplacingMergeTree(last_modified)
  ORDER BY (prefix_start, prefix_end);

CREATE TABLE dabi.ct_fqdn_observations (
    fqdn          String CODEC(ZSTD(3)),
    apex          LowCardinality(String),
    cert_index    UInt64,
    has_wildcard  UInt8,
    -- … additional fields per record
) ENGINE = MergeTree
  PARTITION BY toYYYYMM(observed_at);
```

```
# OpenSearch: indices follow the rolling alias pattern
domains-<tld>-<yyyymmdd>     # per-TLD per-day write index
domains-<tld>                # alias → latest write index
dabi-domains                 # global alias → union of all domains-<tld>
```

---

## Operations

### Daily ops checklist

1. `systemctl list-timers 'dabi-*' --no-pager` — confirm all 8 timers are
   scheduled with non-stale `LAST` times.
2. `journalctl -u 'dabi-ingest-*' --since=24h --no-pager | grep -E 'FAILED|Exception'`
   — expect no output.
3. `docker compose ps` — all services `(healthy)` except `dabi-search` which
   may show `(unhealthy)` cosmetically when `curl` isn't in the base image
   (real health: `_cluster/health` JSON).
4. `gsutil du gs://dabi-prod-backup` — total backup size growing as expected.
5. Cloud Monitoring dashboard at
   `https://console.cloud.google.com/monitoring/alerting?project=dabi-prod-01`
   — no open alerts.

### Deploying a new image

```bash
# On VM:
cd /srv/dabi/deploy
sudo git pull
sudo sed -i 's|^INGEST_SHA=.*|INGEST_SHA=<new-40-char-sha>|' .env
# or API_SHA, WEB_SHA depending on what changed
sudo docker compose pull ingest      # or api, or all
sudo docker compose up -d            # restart only changed services
./scripts/smoke.sh                   # confirm no regression
```

For the custom search image:

```bash
sudo docker compose build search
sudo docker compose up -d --force-recreate search
```

Repository registration survives container recreation because the
snapshot repo's state lives in the GCS bucket, not in the container.

### Backups

Backups are taken by `dabi-backup.service` invoked hourly via
`dabi-backup.timer`. The service runs `scripts/backup.sh` which performs:

1. **OpenSearch snapshot** via the `dabi-backup` repo (type `gcs`, base path
   `opensearch`). Uses Application Default Credentials from the VM metadata
   service — no HMAC needed for OpenSearch.
2. **ClickHouse BACKUP TO S3** to the same bucket under `clickhouse/`. Uses
   the GCS HMAC creds at `/run/dabi/secrets/dabi-ch-s3-{key,secret}`.
3. **Redis BGSAVE** + `docker cp` of the dump.rdb to a timestamped path,
   uploaded to GCS.
4. **auth.db** sqlite file copied and uploaded.

Total run time ~10-15s. Backups beyond 14 days are pruned by the GCS bucket
lifecycle policy.

To restore: see `scripts/restore-*.sh` in the next major-version update.
Manual restore for OpenSearch:
`POST /_snapshot/dabi-backup/<snapshot-name>/_restore`.

### Monitoring & alerting

| Resource                                         | Notes                                                  |
|--------------------------------------------------|--------------------------------------------------------|
| Uptime check `dabi-https-healthz`                | 1-minute cadence on `https://.../healthz`              |
| Alert `dabi-https-uptime-fail` (ERROR)           | Fires if `_check_passed` is false for 5 min            |
| Alert `dabi-disk-usage-high` (WARNING)           | Fires if `agent.googleapis.com/disk/percent_used` > 80%|
| Alert `dabi-container-down` (WARNING)            | Log match on `dockerd … level=info msg="container died"` |
| Notification channel `dabi-ops-email`            | Email to the maintainer; click verify link on first use |
| Google Cloud Ops Agent                           | Installed via apt; pinned to `noble` repo on Resolute   |

### Cert renewal

`dabi-cert-renew.timer` fires every Sunday at 04:00 UTC. The service runs
`certbot renew` against the webroot at `/var/www/dabi/current/`. The cert is
valid 90 days; weekly renewal gives plenty of margin.

Manual renewal:

```bash
sudo systemctl start dabi-cert-renew.service
journalctl -u dabi-cert-renew.service -n 50 --no-pager
sudo docker compose restart nginx     # pick up the new cert chain
```

---

## Security model

### Identity & access

| Principal                                              | Role(s)                                       | Scope                                  |
|--------------------------------------------------------|-----------------------------------------------|----------------------------------------|
| `sa-dabi-vm-runtime@dabi-prod-01.iam.gserviceaccount.com` | `secretmanager.secretAccessor` (on each secret), `storage.objectAdmin` (on `gs://dabi-prod-backup`), HMAC owner | Runtime access on the VM      |
| `sa-dabi-ci@dabi-prod-01.iam.gserviceaccount.com`     | `artifactregistry.writer` (project-wide), `storage.objectAdmin` (on `gs://dabi-prod-backup`) | GitHub Actions via WIF       |
| WIF provider `projects/<projnum>/locations/global/workloadIdentityPools/dabi-ci/providers/github` | attribute condition `assertion.repository_owner=='s4njy-k'` | All s4njy-k repos             |

### Secrets

All secrets live in GCP Secret Manager under project `dabi-prod-01`. The full
list is declared in `scripts/pull-secrets.sh` `SECRETS=(…)`. On the VM:

- `pull-secrets.sh` fetches the `latest` version of each secret to a
  tmpfs-backed `/run/dabi/secrets/<name>` file. Mode 444 (containers need
  cross-uid read).
- The tmpfs is recreated on VM reboot; `pull-secrets.sh` runs at bootstrap
  and may be re-run any time. The VM SA has only `secretAccessor`; writes
  require admin gcloud from WSL.

| Secret                                | Used by                                          |
|---------------------------------------|--------------------------------------------------|
| `dabi-czds-username`, `dabi-czds-password` | CZDS extractor (api auth to ICANN)          |
| `dabi-ch-api-password`, `dabi-ch-ingest-password` | ClickHouse user passwords             |
| `dabi-ch-api-password-sha256`, `…-ingest-password-sha256` | ClickHouse user definitions  |
| `dabi-ch-s3-key`, `dabi-ch-s3-secret` | ClickHouse `BACKUP TO S3` (GCS HMAC)             |
| `dabi-os-admin-password`              | OpenSearch admin user (security plugin disabled but still set) |
| `dabi-jwt-key`                        | api JWT signing                                  |
| `dabi-openphish-token`, `dabi-spamhaus-token` | api integrations (optional)              |
| `dabi-gemini-key`                     | api integration (optional)                       |

### HMAC key rotation

GCS HMAC keys for ClickHouse → GCS backups are tied to the VM service account.
To rotate (do not pipe through `jq` if `jq` isn't installed — the secret will
print to stdout):

```bash
# On WSL admin gcloud
PROJECT=dabi-prod-01
VM_SA=sa-dabi-vm-runtime@dabi-prod-01.iam.gserviceaccount.com

HMAC_JSON=$(gcloud storage hmac create "$VM_SA" --project="$PROJECT" --format=json)
ACCESS_ID=$(python3 -c "import sys,json; print(json.loads(sys.stdin.read())['metadata']['accessId'])" <<<"$HMAC_JSON")
SECRET=$(python3 -c "import sys,json; print(json.loads(sys.stdin.read())['secret'])" <<<"$HMAC_JSON")
unset HMAC_JSON
printf '%s' "$ACCESS_ID" | gcloud secrets versions add dabi-ch-s3-key --data-file=- --project="$PROJECT"
printf '%s' "$SECRET"    | gcloud secrets versions add dabi-ch-s3-secret --data-file=- --project="$PROJECT"
unset ACCESS_ID SECRET

# On VM: pick up the new version
sudo /srv/dabi/deploy/scripts/pull-secrets.sh
# Restart any container that holds the cred in-process (ClickHouse re-reads on the next BACKUP call).
```

### Network

- The VM has a public IP (`34.180.2.141`); the resource record
  `34-180-2-141.sslip.io` resolves to it via sslip.io's wildcard service.
- Firewall: `dabi-allow-web` opens `:80` and `:443` to `0.0.0.0/0`. All
  application routes are authentication-gated. Bare `/healthz`, `/`, and
  `/.well-known/acme-challenge/` are intentionally public.
- All other ports are blocked by GCP's default-deny posture.
- SSH access is via IAP-only on the management firewall rule (`dabi-allow-ssh`).
- Cloud NAT handles outbound traffic.

### TLS

- Let's Encrypt (ISRG Root X1 chain via E8) issues a 90-day cert for
  `34-180-2-141.sslip.io`.
- HSTS, X-Content-Type-Options, X-Frame-Options, and Referrer-Policy
  are set at the nginx server level. The HSTS directive is included into
  every location via `include /etc/nginx/security-headers.conf` to avoid
  the well-known nginx inheritance gotcha.
- HTTP→HTTPS 301 redirect except `/healthz` and ACME challenge paths.
- ssl_protocols TLSv1.2 TLSv1.3; modern cipher list; HTTP/2 enabled via the
  separate `http2 on;` directive (not the deprecated `listen … http2`).

---

## Runbooks

### "Search container is unhealthy"

```bash
sudo docker compose ps search
sudo docker compose logs --tail=200 search
sudo docker exec dabi-search curl -sf http://localhost:9200/_cluster/health
```

If the cluster JSON shows `status: red`: usually a corrupted shard after an
unclean shutdown. Try `POST /_cluster/reroute?retry_failed=true`.

If the container won't start: the most common cause is a plugin install that
no longer matches the upstream image. The OpenSearch image is built locally
as `dabi-opensearch:3` and bakes `repository-gcs` via
`config/opensearch/Dockerfile`. Rebuild:

```bash
sudo docker compose build search
sudo docker compose up -d --force-recreate search
```

### "Backup fails"

```bash
sudo systemctl status dabi-backup.service
journalctl -u dabi-backup.service -n 50 --no-pager
```

If the OpenSearch step fails: re-verify the snapshot repo is reachable.

```bash
sudo docker compose exec -T search curl -fsS -XPOST "http://localhost:9200/_snapshot/dabi-backup/_verify?pretty"
```

If `_verify` returns `repository_missing_exception`: re-register with
`sudo ./scripts/register-snapshot-repo.sh`. The repo state lives in GCS so
re-registration is idempotent.

If the ClickHouse step fails with an S3 error: HMAC creds have probably
rotated out of sync. Re-pull and confirm:

```bash
sudo /srv/dabi/deploy/scripts/pull-secrets.sh
sudo wc -c /run/dabi/secrets/dabi-ch-s3-{key,secret}   # 61 bytes + 40 bytes
sudo head -c 6 /run/dabi/secrets/dabi-ch-s3-key && echo   # starts with GOOG1E
```

### "Ingest timer X failed"

```bash
journalctl -u dabi-ingest-<source>.service -n 100 --no-pager
sudo /srv/dabi/deploy/scripts/run-<source>-pipeline.sh   # manual re-run
```

The multi-RIR fetcher isolates per-RIR errors — one slow registry doesn't
stop the others. If `fetch rir` reports `0/5 sources`, the issue is more
likely ClickHouse-side (check `docker compose logs analytics`).

### "/healthz returns non-200"

```bash
curl -fv https://34-180-2-141.sslip.io/healthz
sudo docker compose ps nginx api
sudo docker exec dabi-nginx nginx -t
```

If `nginx -t` reports cert errors: cert renewal may have failed. Manually
renew and reload (see [Cert renewal](#cert-renewal)).

If `/api/v1/healthz` returns 401 (auth-protected): that is correct — only
the bare `/healthz` proxied from nginx is open.

### "Disk usage > 80%"

Largest consumers, in typical order:
1. OpenSearch indices at `/srv/dabi/opensearch/` — pruned by Stage 9 archive,
   but accumulated daily snapshots can grow. Inspect with
   `du -sh /srv/dabi/opensearch/nodes/*/indices`.
2. Old parquet snapshots at `/srv/dabi/parquet/zones/parquet/`. Safe to
   delete anything older than 30 days.
3. Docker image cache. `sudo docker system prune -a --filter "until=168h"`.

### "VM rebooted"

`/run/dabi/secrets/` is tmpfs and gets wiped. `bootstrap.sh` and most boot
sequences run `pull-secrets.sh` automatically; if you suspect it didn't,
run manually:

```bash
sudo /srv/dabi/deploy/scripts/pull-secrets.sh
sudo docker compose restart   # services re-read /run/dabi/secrets
```

### "GitHub Actions can't push to Artifact Registry"

Symptom: CI passes lint/test but the `build-and-push` job fails on
`google-github-actions/auth` or the `docker push` step.

Check:
- WIF provider attribute condition is `assertion.repository_owner=='s4njy-k'`.
- `sa-dabi-ci` has `roles/artifactregistry.writer` project-wide.
- The workflow has `permissions: { id-token: write }`.

---

## Decision log

Architecture decisions that aren't obvious from the code:

| # | Decision                                                                       | Rationale                                                                                              |
|---|--------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| 1 | OpenSearch for full-text + ClickHouse for analytics (not one engine for both)  | Different access patterns: free-text scoring on names vs aggregations on observations. Cheaper at scale to keep them separate. |
| 2 | Direct polling of Google's Argon CT log instead of certstream                   | Certstream is community-maintained and has had multiple multi-day outages. Direct polling has its own state file and resumes cleanly. |
| 3 | `repository-gcs` plugin for OpenSearch backups (not `repository-s3`)            | OpenSearch 3.x bundles AWS SDK v2 which sends checksum headers GCS S3-interop rejects. `repository-gcs` talks the native Google API and uses ADC from the VM metadata service. |
| 4 | Custom `dabi-opensearch:3` image baked locally                                  | The upstream image doesn't include `repository-gcs`. Installing at runtime works but is lost on `docker compose down`. Baking it into the image survives any container lifecycle. |
| 5 | Ops Agent apt repo pinned to `noble` on Ubuntu Resolute                         | Google hasn't published a `resolute` build yet. The `noble` arm64 build is ABI-compatible.            |
| 6 | GCS HMAC for ClickHouse backups; ADC for OpenSearch backups                     | ClickHouse's S3 client requires explicit key/secret. OpenSearch's `repository-gcs` plugin reads ADC from the VM metadata service. Different mechanisms, same destination. |
| 7 | Per-RIR error isolation in `fetch_and_load_rirs`                                | One slow or down registry shouldn't block the daily run. Caller-side reporting shows which RIRs succeeded and which didn't. |
| 8 | Hourly backup cadence (vs daily)                                                | RPO target is 1 hour. GCS storage is cheap relative to the loss exposure on a single-VM deployment.    |
| 9 | Self-hosted extractors instead of OpenINTEL B3                                   | OpenINTEL B3 requires per-case external approval that takes weeks. Self-hosted equivalents (Tranco + CT + DNS + rDNS + RIR) cover 80% of the value without the blocker. If/when B3 approval lands, the existing tables can host their data via a parallel pipeline. |
| 10 | Firewall open to `0.0.0.0/0` for `:80`/`:443`                                   | All application routes are auth-gated. The threat model is "open internet exposure with login-protected endpoints", which matches typical SaaS posture. |

---

## Keeping this document current

This file is the source of truth for "how DABI works in production". It does
not auto-update; every architectural change to the deployment should include
an update to this README in the same PR.

When you change…

- **Add or remove a data pipeline** → update
  [Data pipelines](#data-pipelines): schedule table, source detail, schema.
- **Add a systemd timer / service** → update the schedule table and
  [Operations](#operations).
- **Change image versions** → update [Tech stack](#tech-stack).
- **Mutate IAM / SAs / secrets** → update [Security model](#security-model).
- **Change the alert policy set** → update Monitoring & alerting under
  [Operations](#operations).
- **Make a non-obvious architectural choice** → add a row to the
  [Decision log](#decision-log).
- **Encounter a recurring incident** → add to [Runbooks](#runbooks).

PRs that touch `docker-compose.yml`, `scripts/`, `systemd/`, or
`config/clickhouse/init.sql` should include a README update; CI does not
enforce this, but reviewers should bounce PRs that don't.

For application-level changes (api routes, ingest logic, web UI),
keep this README focused on the deployment surface. Code-level docs belong
in the `domain-search-pro` repo.

---

## License & attribution

DABI deployment configuration: MIT.

The deployed system ingests data under:

- **ICANN CZDS** — per-registry terms; zone-file data is not redistributed.
- **Tranco** — research use; <https://tranco-list.eu/>.
- **OpenINTEL** — CC BY-NC-SA 4.0 (UTwente / SIDN / NLnet Labs / SURF).
  Used only via public-equivalent extractors (CT logs, RIR delegated stats,
  forward DNS) until B3 approval lands.
- **rir-data.org** — CC BY-NC-SA 4.0 (CAIDA × OpenINTEL).

Operated non-commercially as Indian cyber-intelligence research. Attribution
strings appear in `config/nginx/dabi.conf` and the web SPA footer.
