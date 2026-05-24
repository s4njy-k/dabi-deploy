# DABI ingest container — multi-stage build
# Built for ARM64 (linux/arm64) by CI; runs on C4A VM
# Single image, argparse subcommands (czds, openintel-*, rir, zonestream, ...)

FROM python:3.14-slim AS base

# OS dependencies for DuckDB, aria2c (parallel HTTP downloads), curl
RUN apt-get update && apt-get install -y --no-install-recommends \
        aria2 \
        curl \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Python deps in their own layer
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Source
COPY dabi_ingest/ ./dabi_ingest/
COPY pyproject.toml ./

# Default workspace mounts (compose binds these)
RUN mkdir -p /parquet /checkpoints /scratch /run/secrets \
    && groupadd -g 1000 dabi && useradd -u 1000 -g 1000 -m dabi
USER dabi

ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "dabi_ingest"]
CMD ["--help"]
