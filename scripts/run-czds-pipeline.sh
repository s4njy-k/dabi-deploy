#!/bin/bash
# run-czds-pipeline.sh — fetch + index for a list of approved CZDS TLDs.
# Called by dabi-ingest-czds.timer (per the systemd dabi-ingest@.service template).
# TLDs are space-separated; provide them via the DABI_CZDS_TLDS env var (set in .env).
set -euo pipefail

# Read DABI_CZDS_TLDS (and any other DABI_* vars) from .env so systemd-triggered
# runs see them. The wrapper is invoked both manually and by
# dabi-ingest-czds.service which has no EnvironmentFile= directive.
if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

DABI_CZDS_TLDS="${DABI_CZDS_TLDS:-online}"

cd /srv/dabi/deploy

for tld in $DABI_CZDS_TLDS; do
  echo "[czds-pipeline] === TLD: $tld ==="
  # Pull (NOT pull+up — just refresh the image cache)
  docker compose --profile scheduled pull ingest
  # Fetch zone file -> records.parquet
  docker compose --profile scheduled run --rm ingest fetch czds --tld "$tld"
  # Run Stages 2-9 -> OpenSearch
  docker compose --profile scheduled run --rm ingest run --tld "$tld"
done

echo "[czds-pipeline] done."
