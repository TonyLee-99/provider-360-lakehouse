"""What did a republication change, and does CMS agree?

CMS labels every record of a publication with Change_Type (NEW, ADD, CHANGED, UNCHANGED, relative to the
previous publication) and ships a separate list of DELETED / REMOVED record ids. We never use those labels to
drive the MERGE (the MERGE compares content hashes). Here we compare our MERGE's own classification, read back
from the Delta change data feed, with CMS's labels. They need not agree 1:1 (e.g. CMS may label a record
CHANGED when only a field outside the published file changed), so the result is a cross-tab, and each
off-diagonal cell is reported, not hidden.
"""

from pyspark.sql import functions as F

from p360 import lake, silver
from p360.spark import get_spark


def merge_classification(publication: str):
    entry = [e for e in silver.ledger() if e["table"] == "op_payment" and e["publication"] == publication][0]
    v = entry["delta_version"]
    cdf = (
        get_spark()
        .read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", v)
        .option("endingVersion", v)
        .load(lake.path("silver", "op_payment"))
    )
    pre = cdf.where(F.col("_change_type") == "update_preimage").select(
        "record_id", F.col("is_deleted").alias("pre_deleted")
    )
    post = cdf.where(F.col("_change_type").isin("insert", "update_postimage")).select(
        "record_id", "_change_type", F.col("is_deleted").alias("post_deleted")
    )
    j = post.join(pre, "record_id", "left")
    return j.select(
        "record_id",
        F.when(F.col("_change_type") == "insert", "inserted")
        .when(F.col("post_deleted"), "soft_deleted")
        .when(F.col("pre_deleted") & ~F.col("post_deleted"), "undeleted")
        .otherwise("updated")
        .alias("ours"),
    )


def compare_with_cms(publication: str = "2026-06") -> dict:
    ours = merge_classification(publication)
    vendor_live = lake.read("bronze", silver.OP_DATASETS[publication]).select(
        F.col("Record_ID").cast("bigint").alias("record_id"), F.col("Change_Type").alias("cms")
    )
    vendor_removed = lake.read("silver", "op_removed").select("record_id", F.col("change_type").alias("cms"))
    vendor = vendor_live.unionByName(vendor_removed)
    entry = [e for e in silver.ledger() if e["table"] == "op_payment" and e["publication"] == publication][0]
    before = lake.read("silver", "op_payment", version=entry["delta_version"] - 1).select(
        "record_id", F.lit(True).alias("was_in_table")
    )
    j = (
        vendor.join(ours, "record_id", "full_outer")
        .join(before, "record_id", "left")
        .select(
            F.coalesce("cms", F.lit("(not in publication or removed list)")).alias("cms"),
            F.coalesce(
                "ours",
                F.when(F.col("was_in_table"), F.lit("unchanged")).otherwise(F.lit("never_in_our_table")),
            ).alias("ours"),
        )
    )
    rows = j.groupBy("cms", "ours").count().collect()
    table = {}
    for r in rows:
        table.setdefault(r["cms"], {})[r["ours"]] = r["count"]
    expected = {
        ("NEW", "inserted"),
        ("ADD", "inserted"),
        ("CHANGED", "updated"),
        ("UNCHANGED", "unchanged"),
        ("DELETED", "soft_deleted"),
        ("REMOVED", "soft_deleted"),
    }
    agree = sum(r["count"] for r in rows if (r["cms"], r["ours"]) in expected)
    total = sum(r["count"] for r in rows)
    return {
        "publication": publication,
        "crosstab": table,
        "records": total,
        "agree": agree,
        "agree_pct": round(100 * agree / total, 4) if total else None,
    }
