#!/bin/bash
# install-systemd.sh — installs all DABI systemd units and enables timers
set -euo pipefail

UNIT_DIR=/etc/systemd/system
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)/systemd"

echo "[*] Copying units from $SRC_DIR to $UNIT_DIR..."
install -m 0644 "$SRC_DIR"/*.service "$UNIT_DIR/"
install -m 0644 "$SRC_DIR"/*.timer   "$UNIT_DIR/"

# Place the parameterized template
install -m 0644 "$SRC_DIR"/dabi-ingest@.service "$UNIT_DIR/"

echo "[*] systemctl daemon-reload"
systemctl daemon-reload

echo "[*] Enabling timers..."
for t in "$SRC_DIR"/*.timer; do
  name=$(basename "$t")
  systemctl enable --now "$name"
  echo "  enabled $name"
done

echo "[*] Enabling long-running services..."
for s in dabi-zonestream.service; do
  systemctl enable --now "$s"
  echo "  enabled $s"
done

echo "[*] Done. Status:"
systemctl list-timers 'dabi-*'
