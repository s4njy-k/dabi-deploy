"""Smoke tests — verify dispatcher and all pipelines import cleanly."""

import subprocess
import sys


def test_help_succeeds():
    """`python -m dabi_ingest --help` should exit 0 and list all pipelines."""
    result = subprocess.run(
        [sys.executable, "-m", "dabi_ingest", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    for name in ["czds", "openintel-ctlog", "openintel-rdns", "zonestream"]:
        assert name in result.stdout, f"missing pipeline {name} in --help"


def test_all_pipelines_importable():
    """Each pipeline module exposes PIPELINE, DESCRIPTION, add_args, run."""
    import importlib

    from dabi_ingest.__main__ import PIPELINES

    for name in PIPELINES:
        mod = importlib.import_module(f"dabi_ingest.pipelines.{name.replace('-', '_')}")
        assert hasattr(mod, "PIPELINE")
        assert hasattr(mod, "DESCRIPTION")
        assert callable(getattr(mod, "add_args", None))
        assert callable(getattr(mod, "run", None))
