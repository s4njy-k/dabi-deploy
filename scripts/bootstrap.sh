#!/bin/bash
# bootstrap.sh — idempotent VM bring-up
# Run once as root after first IAP SSH into a fresh VM.
# Safe to re-run (everything is idempotent).
set -euo pipefail

DABI_ROOT=/srv/dabi
SECRET_DIR=/run/dabi/secrets

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

echo "[*] Done. Next: docker compose pull && docker compose up -d"
