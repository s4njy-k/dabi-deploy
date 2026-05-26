#!/bin/bash
# run-dns-pipeline.sh — resolve top-N Tranco domains, insert forward DNS records.
set -euo pipefail
if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi
cd /srv/dabi/deploy
echo "[dns-pipeline] === resolving Tranco top domains ==="
docker compose --profile scheduled pull ingest
docker compose --profile scheduled run --rm ingest fetch dns
echo "[dns-pipeline] done."
