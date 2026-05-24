#!/usr/bin/env bash
# replay-init-sql.sh — manually replay config/clickhouse/init.sql against running analytics.
# Use when /var/lib/clickhouse already has state (so the docker-entrypoint-initdb.d auto-run
# was skipped) but you've added new CREATE statements to init.sql that need to be applied.
# Idempotent: all statements use CREATE IF NOT EXISTS / ALTER IF EXISTS.
set -euo pipefail

COMPOSE_DIR=/srv/dabi/deploy
INIT_SQL="${COMPOSE_DIR}/config/clickhouse/init.sql"

[ -f "$INIT_SQL" ] || { echo "FATAL: $INIT_SQL not found"; exit 1; }

echo "[*] Replaying $INIT_SQL against analytics (default user, localhost-only)..."
docker compose -f "${COMPOSE_DIR}/docker-compose.yml" exec -T analytics \
  clickhouse-client --multiquery < "$INIT_SQL"

echo "[*] Done. Verify with:"
echo "    docker compose -f ${COMPOSE_DIR}/docker-compose.yml exec analytics clickhouse-client --query 'SHOW TABLES FROM dabi'"
