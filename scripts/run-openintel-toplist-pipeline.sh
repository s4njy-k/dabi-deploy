#!/bin/bash
# run-openintel-toplist-pipeline.sh — OpenINTEL forward-DNS top-list daily ingest.
#
# Uses dabi-ingest:local (the dabi-deploy/ingest/ image), NOT INGEST_SHA. Rebuild the
# local image after any change to dabi-deploy/ingest/:
#   docker build -t dabi-ingest:local /srv/dabi/deploy/ingest/
set -euo pipefail

if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

TODAY=$(date -u +%Y-%m-%d)
echo "[openintel-toplist] === ingesting resolved DNS for top-lists, ${TODAY} ==="
docker run --rm \
  --network deploy_dabi-net \
  -v /srv/dabi/checkpoints:/checkpoints \
  -v /run/dabi/secrets:/run/secrets:ro \
  -e DABI_CH_URL=http://analytics:8123 \
  -e DABI_OS_URL=http://search:9200 \
  -e DABI_DNS_RETAIN_DAYS="${DABI_DNS_RETAIN_DAYS:-180}" \
  dabi-ingest:local \
  openintel-toplist --partition-date "${TODAY}"
echo "[openintel-toplist] done."
