#!/bin/bash
# run-openintel-cctld-pipeline.sh — ingest top-10 ccTLD apex lists from OpenINTEL.
#
# Uses dabi-ingest:local — the dabi-deploy/ingest/ image built separately from the
# domain-search-pro ingest image (INGEST_SHA). The two images coexist:
#   INGEST_SHA  → domain-search-pro image  (czds-all, fetch rir/tranco/dns/rdns/ctlog)
#   dabi-ingest:local → dabi-deploy image  (openintel-cctld + future new pipelines)
#
# Rebuild the local image after any change to dabi-deploy/ingest/:
#   docker build -t dabi-ingest:local /srv/dabi/deploy/ingest/
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
