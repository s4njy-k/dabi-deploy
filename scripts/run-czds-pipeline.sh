#!/bin/bash
# run-czds-pipeline.sh — auto-discover all approved CZDS TLDs and ingest each.
# Called by dabi-ingest-czds.timer (per the systemd dabi-ingest@.service template).
#
# This is now a thin shim — all the TLD discovery, filtering, checkpointing,
# disk-safety, and per-TLD failure isolation lives in `dabi-ingest czds-all`
# inside the ingest container. Override behaviour via .env knobs:
#
#   DABI_CZDS_EXCLUDE        — space-separated TLDs to skip (e.g. "com net")
#   DABI_CZDS_MAX_SIZE_MB    — skip zones larger than this (default 2000)
#   DABI_CZDS_DISK_MAX_PCT   — refuse to start if disks >this% full (default 85)
#
set -euo pipefail

# Source .env so DABI_CZDS_EXCLUDE etc reach docker compose run via -e flags
if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

MAX_SIZE_MB="${DABI_CZDS_MAX_SIZE_MB:-2000}"
DISK_MAX_PCT="${DABI_CZDS_DISK_MAX_PCT:-85}"

cd /srv/dabi/deploy

# Refresh image cache once (no-op if already at latest INGEST_SHA)
docker compose --profile scheduled pull ingest

# Single batch invocation — czds-all auto-discovers, checkpoints, isolates failures
docker compose --profile scheduled run --rm ingest czds-all \
  --max-size-mb "$MAX_SIZE_MB" \
  --disk-max-pct "$DISK_MAX_PCT"

echo "[czds-pipeline] done."