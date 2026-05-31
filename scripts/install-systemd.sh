#!/bin/bash
# install-systemd.sh — install all DABI systemd units, enable only production-ready ones
#
# All units are copied to /etc/systemd/system/ (callable on demand), but
# only PRODUCTION_TIMERS / PRODUCTION_SERVICES are auto-enabled and started.
# Other units stay installed-but-disabled until their blocking gaps are
# resolved (see project_dabi_stack memory for the deferred list).
set -euo pipefail

UNIT_DIR=/etc/systemd/system
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)/systemd"

echo "[*] Copying units from $SRC_DIR to $UNIT_DIR..."
install -m 0644 "$SRC_DIR"/*.service "$UNIT_DIR/"
install -m 0644 "$SRC_DIR"/*.timer   "$UNIT_DIR/"

echo "[*] systemctl daemon-reload"
systemctl daemon-reload

# Production-ready units (auto-enable + start)
PRODUCTION_TIMERS=(
  dabi-cert-renew.timer
  dabi-ingest-czds.timer
  dabi-ingest-tranco.timer
  dabi-ingest-ctlog.timer
  dabi-ingest-dns.timer
  dabi-ingest-rir.timer
  dabi-ingest-rdns.timer
  dabi-ingest-openintel-cctld.timer
  dabi-ingest-openintel-toplist.timer
  dabi-ingest-openintel-zonefile.timer
)
PRODUCTION_SERVICES=()

# Deferred (installed-but-NOT-enabled) — re-enable individually as gaps close:
#   dabi-backup.timer            — needs OpenSearch snapshot repo 'dabi-backup' (Day-2)
#   dabi-ingest-*.timer          — dabi_ingest CLI subcommands not implemented yet
#                                  (archive, consolidate, openintel-ctlog/infra/prefix/rdns)
#   dabi-zonestream.service      — same: 'zonestream' CLI subcommand missing

echo "[*] Enabling production-ready timers..."
for t in "${PRODUCTION_TIMERS[@]}"; do
  systemctl enable --now "$t"
  echo "  enabled $t"
done

if [ ${#PRODUCTION_SERVICES[@]} -gt 0 ]; then
  echo "[*] Enabling production-ready services..."
  for s in "${PRODUCTION_SERVICES[@]}"; do
    systemctl enable --now "$s"
    echo "  enabled $s"
  done
fi

echo "[*] Done. Active dabi-* timers:"
systemctl list-timers 'dabi-*' --no-pager 2>&1 || true
