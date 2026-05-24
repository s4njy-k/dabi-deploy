"""Argparse dispatcher for every DABI ingest pipeline.

Each pipeline lives in dabi_ingest.pipelines.<name> and exposes:
    PIPELINE = "<name>"
    def add_args(parser: argparse.ArgumentParser) -> None: ...
    def run(args: argparse.Namespace) -> int: ...
"""
from __future__ import annotations

import argparse
import importlib
import logging
import sys

import structlog

PIPELINES = [
    "czds",
    "openintel-toplist",
    "openintel-ctlog",
    "openintel-zonefile",
    "openintel-infra",
    "openintel-rdns",
    "openintel-cctld",
    "openintel-prefix",
    "rir",
    "zonestream",
    "consolidate",
    "archive",
]


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, stream=sys.stdout, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dabi-ingest",
        description="DABI unified ingest dispatcher",
    )
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = parser.add_subparsers(dest="pipeline", required=True,
                                metavar="<pipeline>")
    for name in PIPELINES:
        # convert kebab-case CLI name to snake_case module name
        mod_name = name.replace("-", "_")
        mod = importlib.import_module(f"dabi_ingest.pipelines.{mod_name}")
        sp = sub.add_parser(name, help=getattr(mod, "DESCRIPTION", name))
        mod.add_args(sp)
        sp.set_defaults(run=mod.run)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.log_level)
    log = structlog.get_logger("dabi_ingest").bind(pipeline=args.pipeline)
    log.info("pipeline.start")
    try:
        rc = int(args.run(args))
    except KeyboardInterrupt:
        log.warning("pipeline.interrupted")
        return 130
    except Exception:
        log.exception("pipeline.failed")
        return 1
    log.info("pipeline.end", rc=rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
