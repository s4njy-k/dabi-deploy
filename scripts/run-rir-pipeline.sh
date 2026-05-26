#!/bin/bash
# run-rir-pipeline.sh — fetch RIR delegated-extended stats, insert dabi.rir_whois.
set -euo pipefail
if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi
cd /srv/dabi/deploy
echo "[rir-pipeline] === fetching RIR delegated-extended stats ==="
docker compose --profile scheduled pull ingest
docker compose --profile scheduled run --rm ingest fetch rir
echo "[rir-pipeline] done."
