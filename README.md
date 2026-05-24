# dabi-deploy

Production deployment of DABI (Domain Abuse Intelligence) on Google Cloud
(asia-south1, Mumbai) per **Plan v8** and **Runbook v5**.

This repository contains EVERYTHING needed to stand up DABI from a blank GCP VM:
Docker Compose orchestration, service configs, systemd timers, operational
scripts, ingest container source, and CI workflows.

The DABI application code (FastAPI api + React web SPA) lives in a **separate
repository** (`domain-search-pro`). Its CI builds and pushes the `dabi/api` and
`dabi/web` images to Artifact Registry. This repository consumes them by SHA in
`docker-compose.yml`.

## Quickstart (production)

```bash
# on a freshly-provisioned C4A VM (see Runbook v5 Phases 0-4)
git clone git@github.com:<org>/dabi-deploy.git /srv/dabi/deploy
cd /srv/dabi/deploy
sudo ./scripts/bootstrap.sh         # disks, sysctls, Docker, secrets
docker compose pull
docker compose up -d
./scripts/smoke.sh                  # 10-check stack health
sudo ./scripts/install-systemd.sh   # enable ingest timers
```

## Quickstart (local laptop dev)

```bash
cp .env.example .env                # edit OS_HEAP, CH_MAX, etc. for smaller envelope
docker compose build ingest         # build local ingest image
INGEST_SHA=dev docker compose up -d search analytics redis  # skip api/web for now
docker compose run --rm ingest --help
```

## Repository layout

```
.
├── docker-compose.yml         # single Compose file; .env handles env-specific values
├── config/                    # all non-Dockerfile service configs
│   ├── opensearch/            # opensearch.yml (single-node, bind 127.0.0.1)
│   ├── clickhouse/            # config.d/, users.d/, init.sql
│   ├── redis/                 # redis.conf
│   └── nginx/                 # dabi.conf (reverse-proxies api, serves web SPA)
├── ingest/                    # the ONE custom image
│   ├── Dockerfile             # multi-stage Python 3.14 ARM64-native
│   └── dabi_ingest/           # argparse-dispatched subcommands per pipeline
├── scripts/                   # idempotent ops: bootstrap, backup, restore, smoke
├── systemd/                   # host-level timers wrapping `docker compose run`
└── .github/workflows/         # CI: build ingest image, push to Artifact Registry via WIF
```

## Sources

- **Plan v8** — `DABI-Plan-v8-Expert-Verified-Corrections.docx`
- **Runbook v5** — `DABI-Runbook-v5-Verified-Commands-C4A-lssd.docx`

Both archived under `~/dabi-archive/` once superseded.

## Versions pinned (verified against official sources, 2026-05)

| Component         | Tag       | Verified at |
|-------------------|-----------|-------------|
| OpenSearch        | `:3`      | hub.docker.com/r/opensearchproject/opensearch/tags |
| ClickHouse        | `:25.8`   | hub.docker.com/r/clickhouse/clickhouse-server/tags |
| Redis             | `:8.6`    | hub.docker.com/_/redis |
| nginx             | `:1.30`   | hub.docker.com/_/nginx/tags |
| Python (api+ingest) | `:3.14-slim` | hub.docker.com/_/python |
| Ubuntu (host)     | 26.04 LTS ARM64 | canonical.com (fallback: 24.04) |

## License & attribution

DABI ingests data under:
- **OpenINTEL** — CC BY-NC-SA 4.0 (UTwente/SIDN/NLnet Labs/SURF)
- **ICANN CZDS** — per-registry terms; no zone-file redistribution
- **rir-data.org** — CC BY-NC-SA 4.0 (CAIDA × OpenINTEL)

This deployment is non-commercial (Indian cyber-intelligence). See attribution
strings in `config/nginx/dabi.conf` and the application footer.
