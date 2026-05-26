#!/bin/bash
# run-ctlog-pipeline.sh — stream certstream for N seconds, insert into ClickHouse.
# Called by dabi-ingest-ctlog.timer.
set -euo pipefail

if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

cd /srv/dabi/deploy
echo "[ctlog-pipeline] === streaming certstream ==="
docker compose --profile scheduled pull ingest
docker compose --profile scheduled run --rm ingest fetch ctlog
echo "[ctlog-pipeline] done."
