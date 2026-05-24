#!/bin/bash
# bootstrap.sh — idempotent VM bring-up
# Run once as root after first IAP SSH into a fresh VM.
# Safe to re-run (everything is idempotent).
set -euo pipefail

DABI_ROOT=/srv/dabi
SECRET_DIR=/run/dabi/secrets

echo "[*] Creating dummy users for container UIDs..."
groupadd -g 101 dabi_clickhouse || true
useradd -u 101 -g 101 -M -s /bin/false dabi_clickhouse || true
groupadd -g 999 dabi_redis || true
useradd -u 999 -g 999 -M -s /bin/false dabi_redis || true

echo "[*] Creating /srv/dabi subdirs with container-expected ownership..."
install -d -o 1000 -g 0    "$DABI_ROOT/opensearch"
install -d -o 101  -g 101  "$DABI_ROOT/clickhouse"
install -d -o 999  -g 999  "$DABI_ROOT/redis"
install -d -o 0    -g 0    "$DABI_ROOT/auth"
install -d -o 0    -g 0    "$DABI_ROOT/letsencrypt"
install -d -o 1000 -g 1000 "$DABI_ROOT/parquet"
install -d -o 1000 -g 1000 "$DABI_ROOT/checkpoints"
install -d -o 0    -g 0    "$DABI_ROOT/logs"
install -d -o 0    -g 0    "/var/www/dabi/current/dist"

echo "[*] Verifying sysctls..."
[[ "$(sysctl -n vm.max_map_count)" -ge 262144 ]] || { echo "FAIL: vm.max_map_count < 262144"; exit 1; }
[[ "$(sysctl -n vm.swappiness)" -le 10 ]] || echo "WARN: vm.swappiness is high"

echo "[*] Verifying Docker..."
docker info >/dev/null || { echo "FAIL: docker daemon not running"; exit 1; }

echo "[*] Logging Docker in to Artifact Registry..."
gcloud auth configure-docker asia-south1-docker.pkg.dev --quiet

echo "[*] Mounting tmpfs at $SECRET_DIR..."
mkdir -p "$SECRET_DIR"
chmod 700 "$SECRET_DIR"
mountpoint -q "$SECRET_DIR" || mount -t tmpfs -o size=1m,mode=700 tmpfs "$SECRET_DIR"

echo "[*] Pulling secrets from Secret Manager..."
"$(dirname "$0")/pull-secrets.sh"

echo "[*] Rendering .env with PROJECT, SHAs, CH memory, and CH password SHA256s..."
API_SHA=${API_SHA:-latest}
INGEST_SHA=${INGEST_SHA:-latest}
PROJECT=$(gcloud config get-value project)
CLICKHOUSE_MAX_MEMORY=${CLICKHOUSE_MAX_MEMORY:-12884901888}    # 12 GiB per Plan v8 RAM budget

# Source CH password SHA256s from tmpfs (populated by pull-secrets.sh above).
# Hard fail if missing OR still 'placeholder' — analytics will not start otherwise (closes #1).
DABI_CH_API_PW_SHA256=$(cat "$SECRET_DIR/dabi-ch-api-password-sha256" 2>/dev/null) || { echo "FATAL: $SECRET_DIR/dabi-ch-api-password-sha256 missing"; exit 1; }
DABI_CH_INGEST_PW_SHA256=$(cat "$SECRET_DIR/dabi-ch-ingest-password-sha256" 2>/dev/null) || { echo "FATAL: $SECRET_DIR/dabi-ch-ingest-password-sha256 missing"; exit 1; }
[[ "$DABI_CH_API_PW_SHA256"    == "placeholder" ]] && { echo "FATAL: dabi-ch-api-password-sha256 still 'placeholder' in Secret Manager"; exit 1; }
[[ "$DABI_CH_INGEST_PW_SHA256" == "placeholder" ]] && { echo "FATAL: dabi-ch-ingest-password-sha256 still 'placeholder' in Secret Manager"; exit 1; }

cat > "$DABI_ROOT/deploy/.env" <<EOF
PROJECT=${PROJECT}
API_SHA=${API_SHA}
INGEST_SHA=${INGEST_SHA}
CLICKHOUSE_MAX_MEMORY=${CLICKHOUSE_MAX_MEMORY}
DABI_CH_API_PW_SHA256=${DABI_CH_API_PW_SHA256}
DABI_CH_INGEST_PW_SHA256=${DABI_CH_INGEST_PW_SHA256}
EOF

echo "[*] Done. Next: docker compose pull && docker compose up -d"
