#!/bin/bash
# smoke.sh — 10 quick health checks against the running stack.
# Exit 0 if all pass; non-zero on first failure.
set -euo pipefail

cd "$(dirname "$0")/.."

pass() { printf "  PASS  %s\n" "$1"; }
fail() { printf "  FAIL  %s\n" "$1"; exit 1; }

echo "[smoke] running 10 checks..."

# 1. nginx
curl -fsk https://localhost/healthz >/dev/null && pass "nginx :443 /healthz" || fail "nginx :443"

# 2. api healthz (inside API container; public /api/* is auth-protected)
docker compose exec -T api python -c 'import urllib.request,sys; sys.exit(0 if urllib.request.urlopen("http://localhost:8000/healthz",timeout=3).status==200 else 1)' \
  && pass "api container /healthz" || fail "api"

# 3. OpenSearch cluster green
status=$(docker compose exec -T search curl -sf http://localhost:9200/_cluster/health \
            | python3 -c 'import sys,json;print(json.load(sys.stdin).get("status"))')
[ "$status" = "green" ] && pass "OpenSearch status=green" || fail "OpenSearch status=$status"

# 4. ClickHouse SELECT 1
ch=$(docker compose exec -T analytics clickhouse-client --query "SELECT 1")
[ "$ch" = "1" ] && pass "ClickHouse SELECT 1" || fail "ClickHouse"

# 5. dabi DB exists
ch=$(docker compose exec -T analytics clickhouse-client --query "SELECT count() FROM system.databases WHERE name='dabi'")
[ "$ch" = "1" ] && pass "ClickHouse dabi DB exists" || fail "ClickHouse dabi DB"

# 6. dabi.rdns table exists
ch=$(docker compose exec -T analytics clickhouse-client --query "SELECT count() FROM system.tables WHERE database='dabi' AND name='rdns'")
[ "$ch" = "1" ] && pass "ClickHouse dabi.rdns table" || fail "ClickHouse dabi.rdns missing"

# 7. dabi.apex_ct_aggregates_mv exists
ch=$(docker compose exec -T analytics clickhouse-client --query "SELECT count() FROM system.tables WHERE database='dabi' AND name='apex_ct_aggregates_mv'")
[ "$ch" = "1" ] && pass "ClickHouse apex_ct_aggregates_mv" || fail "MV missing"

# 8. Redis PING
[ "$(docker compose exec -T redis redis-cli ping)" = "PONG" ] && pass "Redis PING" || fail "Redis"

# 9. all containers reporting healthy (no 'unhealthy' or 'starting' after warmup)
unhealthy=$(docker compose ps --format '{{.Service}} {{.Status}}' | grep -E 'unhealthy|exited' || true)
[ -z "$unhealthy" ] && pass "all containers healthy" || fail "containers: $unhealthy"

# 10. ingest --help works
docker compose --profile scheduled run --rm ingest --help >/dev/null 2>&1 \
  && pass "ingest --help" || fail "ingest container"

echo "[smoke] 10/10 PASS"
