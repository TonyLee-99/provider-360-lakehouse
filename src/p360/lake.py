"""Small helpers for path-based Delta tables (bronze/silver/gold) and contract-driven typing."""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from p360.contracts import Contract
from p360.paths import LAKE
from p360.spark import get_spark


def path(layer: str, name: str, lake_dir: Path | None = None) -> str:
    return str((lake_dir or LAKE) / layer / name)


def exists(p: str) -> bool:
    return DeltaTable.isDeltaTable(get_spark(), p)


def read(layer: str, name: str, version: int | None = None) -> DataFrame:
    reader = get_spark().read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path(layer, name))


def latest_version(p: str) -> int:
    return DeltaTable.forPath(get_spark(), p).history(1).collect()[0]["version"]


def last_operation_metrics(p: str) -> dict:
    row = DeltaTable.forPath(get_spark(), p).history(1).collect()[0]
    return {"version": row["version"], "operation": row["operation"], **(row["operationMetrics"] or {})}


def raw_row_hash(cols: list[str]) -> Column:
    """Content hash for landed rows. Landing never produces NULL (all strings, "" kept as ""), so a single
    concat_ws is exact; it is ~10x faster than the null-safe version on 91 columns (docs/INCIDENTS.md #2)."""
    return F.sha2(F.concat_ws("\u241f", *[F.col(f"`{c}`") for c in cols]), 256)


def row_hash(cols: list[str]) -> Column:
    """Null-safe content hash: NULL and "" hash differently, column order is fixed by the caller."""
    parts = [F.coalesce(F.concat(F.lit("v:"), F.col(f"`{c}`").cast("string")), F.lit("<null>")) for c in cols]
    return F.sha2(F.concat_ws("␟", *parts), 256)


def _cast(col: str, ctype: str, fmt: str | None) -> Column:
    c = F.trim(F.col(f"`{col}`"))
    c = F.when(c == "", None).otherwise(c)
    if ctype == "string":
        return c
    if ctype == "date":
        return F.to_date(c, fmt or "yyyy-MM-dd")  # unparseable -> NULL (CORRECTED parser policy)
    return c.cast(ctype)  # non-ANSI cast: a bad value becomes NULL, which apply_contract flags


def apply_contract(df: DataFrame, contract: Contract) -> DataFrame:
    """Typed, renamed columns + `_reasons` (array of contract failures for that row; empty = good)."""
    get_spark().conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    typed, checks = [], []
    for c in contract.columns:
        raw = F.trim(F.col(f"`{c.name}`"))
        val = _cast(c.name, c.type, c.format)
        typed.append(val.alias(c.alias))
        present = raw.isNotNull() & (raw != "")
        if c.type != "string":
            checks.append(F.when(present & val.isNull(), F.lit(f"bad_{c.type.split('(')[0]}:{c.alias}")))
        if c.required:
            checks.append(F.when(~present, F.lit(f"missing:{c.alias}")))
        if c.accepted:
            checks.append(F.when(present & ~raw.isin(c.accepted), F.lit(f"not_accepted:{c.alias}")))
    meta = [F.col(m) for m in df.columns if m.startswith("_")]
    reasons = F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array()
    return df.select(*typed, *meta, reasons.alias("_reasons"))


def split_good_bad(typed: DataFrame, key: list[str]) -> tuple[DataFrame, DataFrame]:
    """Rows with contract failures or a duplicated primary key go to quarantine (all copies of the key:
    we cannot know which copy is right, so neither is published)."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*key)
    flagged = typed.withColumn("_key_count", F.count(F.lit(1)).over(w)).withColumn(
        "_reasons",
        F.when(F.col("_key_count") > 1, F.array_union("_reasons", F.array(F.lit("duplicate_key")))).otherwise(
            F.col("_reasons")
        ),
    )
    good = flagged.where(F.size("_reasons") == 0).drop("_reasons", "_key_count")
    bad = flagged.where(F.size("_reasons") > 0).drop("_key_count")
    return good, bad
