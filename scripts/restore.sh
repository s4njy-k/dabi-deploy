#!/bin/bash
# restore.sh — selective restore per-component
# Usage: ./scripts/restore.sh <snapshot-id> <component>
#   component ∈ { os | ch | redis | auth | all }
set -euo pipefail

SNAPSHOT="${1:?usage: $0 <snapshot-id> <component>}"
COMPONENT="${2:?usage: $0 <snapshot-id> <component>}"

cd "$(dirname "$0")/.."

case "$COMPONENT" in
  os|all)
    echo "[restore] OpenSearch from $SNAPSHOT..."
    docker compose exec -T search \
      curl -sf -XPOST "http://localhost:9200/_snapshot/dabi-backup/$SNAPSHOT/_restore?wait_for_completion=true" \
      -H 'Content-Type: application/json' -d '{"indices":"dabi-*"}'
    ;;& # fall through
  ch|all)
    if [ -f /run/dabi/secrets/dabi-ch-s3-key ]; then
      echo "[restore] ClickHouse from $SNAPSHOT..."
      CH_S3_KEY=$(cat /run/dabi/secrets/dabi-ch-s3-key)
      CH_S3_SECRET=$(cat /run/dabi/secrets/dabi-ch-s3-secret)
      docker compose exec -T analytics clickhouse-client --query "
        RESTORE DATABASE dabi
        FROM S3('https://storage.googleapis.com/dabi-prod-backup/clickhouse/$SNAPSHOT/',
                '$CH_S3_KEY', '$CH_S3_SECRET')"
    fi
    ;;& # fall through
  redis|all)
    echo "[restore] Redis from $SNAPSHOT..."
    docker compose stop redis
    gcloud storage cp "gs://dabi-prod-backup/redis/$SNAPSHOT.rdb" /srv/dabi/redis/dump.rdb
    docker compose start redis
    ;;& # fall through
  auth|all)
    echo "[restore] auth.db from $SNAPSHOT..."
    gcloud storage cp "gs://dabi-prod-backup/auth/$SNAPSHOT.db" /srv/dabi/auth/auth.db
    docker compose restart api
    ;;
  *)
    echo "Unknown component: $COMPONENT"
    echo "Valid: os | ch | redis | auth | all"
    exit 1
    ;;
esac

echo "[restore] $COMPONENT from $SNAPSHOT complete."
"$(dirname "$0")/smoke.sh"
