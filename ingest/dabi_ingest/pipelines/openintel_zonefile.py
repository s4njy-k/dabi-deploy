"""OpenINTEL forward-DNS zone-based — daily resolved records for whole public TLD zones."""

from __future__ import annotations

import argparse

from dabi_ingest.pipelines import _fdns_common as fc

PIPELINE = "openintel-zonefile"
DESCRIPTION = "OpenINTEL forward-DNS zone-based — resolved A/AAAA/NS/MX/CNAME/TXT/SOA per apex for public zones."

# Public zone-based sources on object.openintel.nl (auto-discovery is preferred; this is the fallback).
DEFAULT_SOURCES = ["ch", "ee", "fr", "se", "sk", "li", "nu", "gov", "fed.us", "root"]


def add_args(parser: argparse.ArgumentParser) -> None:
    fc.add_fdns_args(parser, DEFAULT_SOURCES)


def run(args: argparse.Namespace) -> int:
    return fc.run_fdns("zonefile", DEFAULT_SOURCES, args)
