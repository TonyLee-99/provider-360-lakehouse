import pytest

pytestmark = pytest.mark.spark


def test_npi_check_digit():
    from pyspark.sql import functions as F

    from p360.identity import npi_valid
    from p360.spark import get_spark

    rows = [("1234567893",), ("1234567890",), ("123",), (None,), ("12345678a3",)]
    df = get_spark().createDataFrame(rows, ["n"])
    got = [r.ok for r in df.select(npi_valid(F.col("n")).alias("ok")).collect()]
    assert got == [True, False, False, False, False]


def test_name_normalization():
    from pyspark.sql import functions as F

    from p360.identity import norm_name, zip5
    from p360.spark import get_spark

    df = get_spark().createDataFrame([("  o'Brien-Smith jr ", "60637-1447")], ["n", "z"])
    r = df.select(norm_name(F.col("n")).alias("n"), zip5(F.col("z")).alias("z")).first()
    assert r.n == "OBRIENSMITH" and r.z == "60637"


def test_wilson_interval():
    from p360.identity import wilson

    lo, hi = wilson(990, 1000)
    assert lo < 0.99 < hi and hi <= 1
