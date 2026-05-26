#!/bin/bash
# run-rdns-pipeline.sh — reverse-resolve A-record IPs from dns_observations,
# insert PTR rows into dabi.rdns. Depends on dabi-ingest-dns.timer having
# run first (same day) — produces the dns_observations snapshot we read.
set -euo pipefail
if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi
cd /srv/dabi/deploy
echo "[rdns-pipeline] === reverse-resolving A-record IPs ==="
docker compose --profile scheduled pull ingest
docker compose --profile scheduled run --rm ingest fetch rdns
echo "[rdns-pipeline] done."
