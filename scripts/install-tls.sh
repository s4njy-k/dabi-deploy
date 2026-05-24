#!/usr/bin/env bash
# install-tls.sh — first-time issuance of Let's Encrypt cert via certbot --standalone.
# Also creates the `_default_` symlink that config/nginx/dabi.conf expects, closing
# the gap where its cert paths are hardcoded to /etc/letsencrypt/live/_default_/ but
# certbot writes to /etc/letsencrypt/live/<TLS_DOMAIN>/.
#
# For RENEWALS (after nginx is serving), use a separate cron/timer with --webroot
# mode against /var/www/dabi/current/.well-known/acme-challenge/. This script is
# first-issuance only.
#
# Usage:
#   TLS_DOMAIN=dabi.example.com TLS_EMAIL=you@example.com sudo -E ./scripts/install-tls.sh
set -euo pipefail

: "${TLS_DOMAIN:?need TLS_DOMAIN env (e.g. dabi.example.com)}"
: "${TLS_EMAIL:?need TLS_EMAIL env (admin contact for renewal warnings)}"

LE_DIR=/srv/dabi/letsencrypt
COMPOSE_DIR=/srv/dabi/deploy

echo "[*] Ensuring nginx is NOT running (certbot --standalone needs port 80)..."
if ( cd "$COMPOSE_DIR" && docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -qx nginx ); then
  echo "    nginx is running — stopping for first issuance..."
  ( cd "$COMPOSE_DIR" && docker compose stop nginx )
fi

echo "[*] Issuing cert for $TLS_DOMAIN via certbot --standalone..."
sudo mkdir -p "$LE_DIR"
sudo docker run --rm \
  -p 80:80 \
  -v "$LE_DIR:/etc/letsencrypt" \
  certbot/certbot:arm64v8-latest certonly \
    --standalone \
    -d "$TLS_DOMAIN" \
    --non-interactive --agree-tos -m "$TLS_EMAIL" \
    --rsa-key-size 4096

echo "[*] Creating _default_ symlink (relative target — resolves on host AND inside nginx container)..."
sudo ln -sfn "$TLS_DOMAIN" "$LE_DIR/live/_default_"

echo "[*] Verifying symlink resolves..."
sudo test -f "$LE_DIR/live/_default_/fullchain.pem" || { echo "FATAL: $LE_DIR/live/_default_/fullchain.pem missing"; exit 1; }
sudo test -f "$LE_DIR/live/_default_/privkey.pem"   || { echo "FATAL: $LE_DIR/live/_default_/privkey.pem missing"; exit 1; }

echo "[*] Starting nginx with TLS now configured..."
( cd "$COMPOSE_DIR" && docker compose up -d nginx )

echo "[*] Done. Verify externally:"
echo "      curl -I https://$TLS_DOMAIN"
echo "      curl -fsSL https://$TLS_DOMAIN/api/healthz"
