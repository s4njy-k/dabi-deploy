#!/bin/bash
# pull-secrets.sh — fetch all DABI secrets into /run/dabi/secrets tmpfs.
# Re-run after rotating a secret in Secret Manager.
#
# Permission model (closes #4):
#   - Parent dir: 711 root  → any UID can traverse to known filenames, none can ls.
#   - Files:      444 root  → any UID can read; mount :ro in compose prevents writes.
# This is sufficient for single-tenant DABI VM (IAP-only SSH, no untrusted local users).
set -euo pipefail

SECRET_DIR=/run/dabi/secrets
mkdir -p "$SECRET_DIR"
chmod 711 "$SECRET_DIR"

# All secrets DABI knows about (mirror with Phase 2 of Runbook v5).
# IMPORTANT: keep in sync with what bootstrap.sh and the compose env vars expect.
SECRETS=(
  dabi-os-admin-password
  dabi-jwt-key
  dabi-openphish-token
  dabi-spamhaus-token
  dabi-gemini-key
  dabi-czds-username
  dabi-czds-password
  dabi-zonestream-token
  dabi-ch-api-password-sha256
  dabi-ch-ingest-password-sha256
  dabi-ch-api-password         # plaintext, for admin CH client connections (added 2026-05-24)
  dabi-ch-ingest-password      # plaintext (added 2026-05-24)
  dabi-ch-s3-key
  dabi-ch-s3-secret
)

for s in "${SECRETS[@]}"; do
  if gcloud secrets versions access latest --secret="$s" \
        --out-file="$SECRET_DIR/$s" 2>/dev/null; then
    chmod 444 "$SECRET_DIR/$s"
    echo "OK   $s"
  else
    echo "SKIP $s (not in Secret Manager yet)"
  fi
done
