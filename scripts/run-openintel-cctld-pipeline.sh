#!/bin/bash
# run-openintel-cctld-pipeline.sh — ingest top-10 ccTLD apex lists (CT + Tranco sources).
# Uses dabi-ingest:local image built from dabi-deploy/ingest/ via:
#   docker compose build ingest
set -euo pipefail

if [ -f /srv/dabi/deploy/.env ]; then
  set -a; . /srv/dabi/deploy/.env; set +a
fi

TODAY=$(date -u +%Y-%m-%d)
echo "[openintel-cctld] === ingesting top-10 ccTLD apex lists for ${TODAY} ==="
docker run --rm \
  --network deploy_dabi-net \
  -v /srv/dabi/checkpoints:/checkpoints \
  -v /run/dabi/secrets:/run/secrets:ro \
  -e DABI_CH_URL=http://analytics:8123 \
  -e OPENSEARCH_URL=http://search:9200 \
  dabi-ingest:local \
  openintel-cctld --partition-date "${TODAY}"
echo "[openintel-cctld] done."
