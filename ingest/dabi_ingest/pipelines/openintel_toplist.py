"""OpenINTEL forward-DNS top-lists — daily resolved records for ranked popular-domain lists."""

from __future__ import annotations

import argparse

from dabi_ingest.pipelines import _fdns_common as fc

PIPELINE = "openintel-toplist"
DESCRIPTION = (
    "OpenINTEL forward-DNS top-lists — resolved records for alexa/crux/majestic/radar/tranco/umbrella."
)

DEFAULT_SOURCES = ["alexa", "crux", "majestic", "radar", "tranco", "umbrella"]


def add_args(parser: argparse.ArgumentParser) -> None:
    fc.add_fdns_args(parser, DEFAULT_SOURCES)


def run(args: argparse.Namespace) -> int:
    return fc.run_fdns("toplist", DEFAULT_SOURCES, args)
