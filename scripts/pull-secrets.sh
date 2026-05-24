#!/bin/bash
# pull-secrets.sh — fetch all DABI secrets into /run/dabi/secrets tmpfs.
# Re-run after rotating a secret in Secret Manager.
set -euo pipefail

SECRET_DIR=/run/dabi/secrets
mkdir -p "$SECRET_DIR"; chmod 700 "$SECRET_DIR"

# All secrets DABI knows about (mirror with Phase 1.6 of Runbook v5)
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
  dabi-ch-s3-key
  dabi-ch-s3-secret
)

for s in "${SECRETS[@]}"; do
  if gcloud secrets versions access latest --secret="$s" \
        --out-file="$SECRET_DIR/$s" 2>/dev/null; then
    chmod 400 "$SECRET_DIR/$s"
    echo "OK   $s"
  else
    echo "SKIP $s (not in Secret Manager yet)"
  fi
done
