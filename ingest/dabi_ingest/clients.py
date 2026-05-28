"""Centralized client factories for OpenSearch, ClickHouse, Redis."""
from __future__ import annotations

import os
from functools import lru_cache


@lru_cache(maxsize=1)
def opensearch():
    from opensearchpy import OpenSearch
    url = os.environ.get("DABI_OS_URL", "http://search:9200")
    return OpenSearch(hosts=[url], use_ssl=False, verify_certs=False, timeout=60)


@lru_cache(maxsize=1)
def clickhouse():
    """clickhouse-connect client for HTTP interface (port 8123)."""
    import clickhouse_connect
    url = os.environ.get("DABI_CH_URL", "http://analytics:8123")
    host = url.split("//", 1)[-1].split(":", 1)[0]
    port = int(url.rsplit(":", 1)[-1]) if ":" in url.split("//", 1)[-1] else 8123
    user = os.environ.get("DABI_CH_USER", "ingest")
    password = _read_secret("dabi-ch-ingest-password") or ""
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password,
        database="dabi",
    )


@lru_cache(maxsize=1)
def redis():
    import redis as _r
    return _r.Redis.from_url(
        os.environ.get("DABI_REDIS_URL", "redis://redis:6379"),
        decode_responses=True,
    )


def _read_secret(name: str) -> str | None:
    """Read /run/secrets/<name> (tmpfs from Secret Manager via pull-secrets.sh)."""
    path = f"/run/secrets/{name}"
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None
