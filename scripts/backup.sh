#!/bin/bash
# backup.sh — on-demand backup of all stateful volumes
# Called hourly by dabi-backup.timer; also runs from CLI for manual snapshots
set -euo pipefail

DATE=${1:-$(date -u +%Y%m%d-%H%M)}
BUCKET=gs://dabi-prod-backup

cd "$(dirname "$0")/.."

echo "[backup $DATE] OpenSearch snapshot..."
docker compose exec -T search \
  curl -sf -XPUT "http://localhost:9200/_snapshot/dabi-backup/auto-$DATE?wait_for_completion=false" \
  -H 'Content-Type: application/json' -d '{}'

if [ -f /run/dabi/secrets/dabi-ch-s3-key ]; then
  echo "[backup $DATE] ClickHouse BACKUP TO S3..."
  CH_S3_KEY=$(cat /run/dabi/secrets/dabi-ch-s3-key)
  CH_S3_SECRET=$(cat /run/dabi/secrets/dabi-ch-s3-secret)
  docker compose exec -T analytics clickhouse-client --query "
    BACKUP DATABASE dabi
    TO S3('https://storage.googleapis.com/dabi-prod-backup/clickhouse/auto-$DATE/',
          '$CH_S3_KEY', '$CH_S3_SECRET')
    SETTINGS compression_method='zstd', compression_level=3"
fi

echo "[backup $DATE] Redis BGSAVE..."
docker compose exec -T redis redis-cli BGSAVE
sleep 5
docker compose cp redis:/data/dump.rdb /tmp/dump-$DATE.rdb
gcloud storage cp /tmp/dump-$DATE.rdb "$BUCKET/redis/auto-$DATE.rdb"
rm -f /tmp/dump-$DATE.rdb

echo "[backup $DATE] auth.db..."
gcloud storage cp /srv/dabi/auth/auth.db "$BUCKET/auth/auto-$DATE.db" 2>/dev/null || \
  echo "  (auth.db not yet present)"

echo "[backup $DATE] DONE"
