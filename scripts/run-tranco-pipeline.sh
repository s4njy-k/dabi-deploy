#!/bin/bash
# run-tranco-pipeline.sh — fetch + load Tranco daily top-1M into ClickHouse.
# Called by dabi-ingest-tranco.timer.
set -euo pipefail

if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

cd /srv/dabi/deploy
echo "[tranco-pipeline] === fetching Tranco top-1M ==="
docker compose --profile scheduled pull ingest
docker compose --profile scheduled run --rm ingest fetch tranco
echo "[tranco-pipeline] done."
