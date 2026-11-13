"""Tests run on the committed real fixture (fixtures/raw) in a temp data dir. The env vars must be set before
any p360 module is imported, because paths are resolved at import time."""

import os
import shutil
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
_TMP = Path(tempfile.mkdtemp(prefix="p360-test-"))
shutil.copytree(REPO / "fixtures" / "raw", _TMP / "data" / "raw")
os.environ["P360_DATA_DIR"] = str(_TMP / "data")
os.environ["P360_RESULTS_DIR"] = str(_TMP / "results")
os.environ["P360_SCOPE"] = "fixture"
os.environ["DAGSTER_HOME"] = str(_TMP / "dagster")
os.environ.setdefault("P360_SPARK_DRIVER_MEMORY", "2g")
os.environ.setdefault("P360_SPARK_SHUFFLE_PARTITIONS", "4")
os.makedirs(os.environ["DAGSTER_HOME"], exist_ok=True)

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def tmp_root() -> Path:
    return _TMP


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(_TMP, ignore_errors=True)
