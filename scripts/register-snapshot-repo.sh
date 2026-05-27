#!/bin/bash
# register-snapshot-repo.sh — register the OpenSearch GCS snapshot repo.
# Run ONCE after a fresh search cluster (state in /srv/dabi/opensearch reset).
# Uses application-default credentials from the VM's metadata service —
# sa-dabi-vm-runtime needs roles/storage.objectAdmin on gs://dabi-prod-backup.
set -euo pipefail
cd /srv/dabi/deploy
docker compose exec -T search curl -fsS -XPUT http://localhost:9200/_snapshot/dabi-backup \
  -H 'Content-Type: application/json' \
  -d '{"type":"gcs","settings":{"bucket":"dabi-prod-backup","base_path":"opensearch","client":"default"}}'
echo
docker compose exec -T search curl -fsS -XPOST "http://localhost:9200/_snapshot/dabi-backup/_verify?pretty"
