"""Bronze: landed Parquet -> Delta, unchanged values plus lineage columns.

One bronze table per landed file (e.g. op_payment_p2026_01 and op_payment_p2026_06 are two tables): each
publication is an immutable snapshot, and a reload overwrites only its own table, so re-running is idempotent.
(A single table partitioned by publication was tried first: Spark sorts every row by the partition column
before a partitioned write, which for 15M wide rows meant a 15 GB spill. See docs/INCIDENTS.md.)
"""

import json
from pathlib import Path

from pyspark.sql import functions as F

from p360 import contracts, lake
from p360.landing import BY_NAME, DATASETS
from p360.paths import LANDING
from p360.spark import get_spark

BRONZE_TABLES = [d.name for d in DATASETS]


def landed(dataset: str, landing_dir: Path = LANDING) -> bool:
    return (landing_dir / dataset / "_landing.json").exists()


def load_bronze(name: str, landing_dir: Path = LANDING) -> dict:
    """Landing dataset `name` -> bronze.<name> (overwrite). Row count must equal the landing count."""
    spark = get_spark()
    ds = BY_NAME[name]
    meta = json.loads((landing_dir / name / "_landing.json").read_text())
    contract = contracts.load(ds.contract)
    df = spark.read.parquet(str(landing_dir / name / "data.parquet"))
    hash_cols = [c for c in df.columns if c not in contract.hash_exclude]
    df = df.select(
        "*",
        lake.raw_row_hash(hash_cols).alias("_row_hash"),
        F.lit(ds.publication).alias("_publication"),
        F.lit(meta["member"]).alias("_source_member"),
        F.current_timestamp().alias("_loaded_at"),
    )
    target = lake.path("bronze", name)
    if lake.exists(target):  # skip a reload of the same landed file (same member, same row count)
        cur = spark.read.format("delta").load(target)
        same = cur.select("_source_member").limit(1).collect()
        if same and same[0][0] == meta["member"] and cur.count() == meta["rows"]:
            return {"table": name, "publication": ds.publication, "rows": meta["rows"], "skipped": True}
    # bronze is replaced per file, nobody reads its change feed
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    w = w.option("delta.enableChangeDataFeed", "false")
    if any(not c.replace("_", "").isalnum() for c in df.columns):
        # NPPES headers contain spaces and parentheses; Delta column mapping keeps the published names as-is
        w = (w.option("delta.columnMapping.mode", "name").option("delta.minReaderVersion", "2")
             .option("delta.minWriterVersion", "5"))
    w.save(target)
    rows = spark.read.format("delta").load(target).count()
    if rows != meta["rows"]:  # landing and bronze must agree row for row
        raise RuntimeError(f"bronze.{name} has {rows} rows, landing had {meta['rows']}")
    return {"table": name, "publication": ds.publication, "rows": rows}
