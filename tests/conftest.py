import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.spark_job import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = build_spark("tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
