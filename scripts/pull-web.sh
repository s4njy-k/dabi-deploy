#!/usr/bin/env bash
# pull-web.sh — fetch web bundle from GCS and install into /var/www/dabi/current/dist/
# Usage:  pull-web.sh <WEB_SHA>     (positional)
#         WEB_SHA=<sha> pull-web.sh (env)
# Requires: gsutil/gcloud, rsync, sudo (or run as root)
set -euo pipefail

WEB_SHA="${1:-${WEB_SHA:?need WEB_SHA as positional arg or env var}}"
TARBALL="web-${WEB_SHA}.tar.gz"
GCS_URI="gs://dabi-prod-backup/web/${TARBALL}"
DIST_DIR=/var/www/dabi/current/dist

TMP=$(mktemp -d)
trap "rm -rf '$TMP'" EXIT

echo "[*] Fetching $GCS_URI..."
gsutil cp "$GCS_URI" "$TMP/$TARBALL"

echo "[*] Extracting to staging..."
mkdir -p "$TMP/dist"
tar -xzf "$TMP/$TARBALL" -C "$TMP/dist/"

echo "[*] Syncing into $DIST_DIR (atomic per-file via rsync --delete)..."
sudo mkdir -p "$DIST_DIR"
sudo rsync -a --delete "$TMP/dist/" "$DIST_DIR/"

echo "[*] Done. web ${WEB_SHA} installed to $DIST_DIR"
ls -la "$DIST_DIR" | head -5
