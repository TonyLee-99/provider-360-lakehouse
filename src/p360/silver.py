"""Silver: typed, contract-checked tables. Two of them are maintained by MERGE from republished sources.

- op_payment: CMS republishes the whole program year every ~6 months. Each publication is applied with
  one MERGE: new records inserted, changed records updated (content hash differs), records absent from the
  new publication soft-deleted. Guards: publications must be applied in order; re-applying the latest one is a
  no-op; a publication that is much smaller than the live table is refused (a truncated file must not delete
  half the table); rows that fail the contract are kept at their previous version, not deleted.
- nppes_provider: monthly full file, then weekly incremental files. Upsert only (a provider absent from a
  weekly file is not deleted), and an older record never overwrites a newer one (Last Update Date guard).
"""

import json
import os
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from p360 import contracts, lake
from p360.landing import BY_NAME
from p360.paths import LAKE
from p360.spark import get_spark

MIN_REPUBLICATION_RATIO = 0.9  # a new publication must have >= 90% of the live rows to be allowed to delete


class PublicationRefused(Exception):
    pass


# ---------- ledger: which publication was applied to which table, in what order ----------
def _ledger_path():
    return LAKE / "silver" / "_publication_ledger.json"


def ledger() -> list[dict]:
    p = _ledger_path()
    return json.loads(p.read_text()) if p.exists() else []


def _append_ledger(entry: dict) -> None:
    p = _ledger_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    entries = ledger() + [entry]
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(entries, indent=2))
    os.replace(tmp, p)  # atomic: the ledger is either the old list or the new one


def applied(table: str) -> list[str]:
    return [e["publication"] for e in ledger() if e["table"] == table]


# ---------- quarantine ----------
def write_quarantine(bad: DataFrame, table: str, publication: str, key: list[str]) -> int:
    q = bad.select(
        F.lit(table).alias("table"),
        F.lit(publication).alias("publication"),
        F.to_json(F.struct(*key)).alias("record_key"),
        F.col("_reasons").alias("reasons"),
        F.to_json(F.struct(*[c for c in bad.columns if not c.startswith("_")])).alias("record"),
    )
    target = lake.path("silver", "quarantine")
    cond = f"table = '{table}' AND publication = '{publication}'"
    if lake.exists(target):  # idempotent: replace this table+publication's quarantine rows only
        DeltaTable.forPath(get_spark(), target).delete(cond)
    q.write.format("delta").mode("append").save(target)
    return lake.read("silver", "quarantine").where(cond).count()


def typed_dataset(dataset: str):
    """bronze.<dataset> -> (contract, good typed rows, quarantined rows with reasons)."""
    contract = contracts.load(BY_NAME[dataset].contract)
    src = lake.read("bronze", dataset)
    typed = lake.apply_contract(src.select(*contract.source_names, "_row_hash"), contract)
    good, bad = lake.split_good_bad(typed, contract.primary_key)
    return contract, good, bad


# ---------- reference tables: replace per publication ----------
STATIC = {  # silver table -> bronze dataset
    "op_profile": "op_profile_supplement",
    "op_removed": "op_removed_p2026_06",
    "op_recipient_nature_summary": "op_recipient_nature_summary",
    "partd_prescriber_drug": "partd_prescriber_drug",
    "partd_prescriber": "partd_prescriber",
}


def build_static(table: str) -> dict:
    ds = BY_NAME[STATIC[table]]
    contract, good, bad = typed_dataset(ds.name)
    good.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        lake.path("silver", table)
    )
    n_bad = write_quarantine(bad, table, ds.publication, contract.primary_key)
    n_good = lake.read("silver", table).count()
    return {"table": table, "publication": ds.publication, "rows": n_good, "quarantined": n_bad}


# ---------- Open Payments: apply a publication with MERGE ----------
OP_TABLE = "op_payment"
OP_DATASETS = {"2026-01": "op_payment_p2026_01", "2026-06": "op_payment_p2026_06"}  # publication -> dataset


def apply_op_publication(publication: str) -> dict:
    spark = get_spark()
    done = applied(OP_TABLE)
    if done and publication < done[-1]:
        raise PublicationRefused(f"{publication} is older than the last applied publication {done[-1]}")
    contract, good, bad = typed_dataset(OP_DATASETS[publication])
    target_path = lake.path("silver", OP_TABLE)
    program_year = good.select(F.max("program_year")).first()[0]

    n_source = good.count()
    if lake.exists(target_path):
        live = lake.read("silver", OP_TABLE).where(~F.col("is_deleted")).count()
        if n_source < MIN_REPUBLICATION_RATIO * live:
            raise PublicationRefused(
                f"{publication} has {n_source:,} valid rows vs {live:,} live rows "
                f"(< {MIN_REPUBLICATION_RATIO:.0%}); refusing to soft-delete the difference"
            )

    data_cols = [c.alias for c in contract.columns if c.alias != "change_type"]
    src = good.select(*data_cols, "_row_hash").withColumn("_quarantined", F.lit(False))
    # keys that failed the contract this time: keep their previous version instead of deleting them
    stubs = (
        bad.where(F.col("record_id").isNotNull())
        .select("record_id")
        .distinct()
        .join(good.select("record_id"), "record_id", "left_anti")
    )
    for c in data_cols + ["_row_hash"]:
        if c != "record_id":
            stubs = stubs.withColumn(c, F.lit(None).cast(src.schema[c].dataType))
    src = src.unionByName(stubs.withColumn("_quarantined", F.lit(True)))

    if not lake.exists(target_path):
        (
            src.where(~F.col("_quarantined"))
            .drop("_quarantined")
            .withColumn("first_published_in", F.lit(publication))
            .withColumn("changed_in", F.lit(publication))
            .withColumn("is_deleted", F.lit(False))
            .withColumn("deleted_in", F.lit(None).cast("string"))
            .write.format("delta")
            .save(target_path)
        )
    else:
        values = {c: f"s.`{c}`" for c in data_cols + ["_row_hash"]}
        (
            DeltaTable.forPath(spark, target_path)
            .alias("t")
            .merge(src.alias("s"), "t.record_id = s.record_id")
            .whenMatchedUpdate(
                condition="NOT s._quarantined AND (t._row_hash <> s._row_hash OR t.is_deleted)",
                set={**values, "changed_in": f"'{publication}'", "is_deleted": "false", "deleted_in": "null"},
            )
            .whenNotMatchedInsert(
                condition="NOT s._quarantined",
                values={
                    **values,
                    "first_published_in": f"'{publication}'",
                    "changed_in": f"'{publication}'",
                    "is_deleted": "false",
                    "deleted_in": "null",
                },
            )
            .whenNotMatchedBySourceUpdate(
                condition=f"NOT t.is_deleted AND t.program_year = {program_year}",
                set={"is_deleted": "true", "deleted_in": f"'{publication}'"},
            )
            .execute()
        )
    metrics = lake.last_operation_metrics(target_path)
    n_bad = write_quarantine(bad, OP_TABLE, publication, contract.primary_key)
    entry = {
        "table": OP_TABLE,
        "publication": publication,
        "delta_version": metrics["version"],
        "operation": metrics["operation"],
        "metrics": {k: v for k, v in metrics.items() if k.startswith("num")},
        "source_rows_valid": n_source,
        "quarantined": n_bad,
        "applied_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _append_ledger(entry)
    return entry


# ---------- NPPES: monthly full + weekly upserts ----------
NPPES_TABLE = "nppes_provider"


def apply_nppes(dataset: str) -> dict:
    spark = get_spark()
    ds = BY_NAME[dataset]
    contract, good, bad = typed_dataset(dataset)
    target_path = lake.path("silver", NPPES_TABLE)
    src = good.withColumn("source_publication", F.lit(ds.publication))
    if not lake.exists(target_path):
        src.write.format("delta").save(target_path)
        stale = 0
    else:
        t = lake.read("silver", NPPES_TABLE).select(
            "npi", F.col("last_update_date").alias("t_lud"), F.col("_row_hash").alias("t_hash")
        )
        stale = (
            src.join(t, "npi")
            .where((F.col("_row_hash") != F.col("t_hash")) & (F.col("last_update_date") < F.col("t_lud")))
            .count()
        )
        cols = {c: f"s.`{c}`" for c in src.columns}
        (
            DeltaTable.forPath(spark, target_path)
            .alias("t")
            .merge(src.alias("s"), "t.npi = s.npi")
            .whenMatchedUpdate(
                # never let an older record overwrite a newer one; identical content is a no-op
                condition="t._row_hash <> s._row_hash AND "
                "coalesce(s.last_update_date, date'1900-01-01') >= coalesce(t.last_update_date, date'1900-01-01')",
                set=cols,
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    metrics = lake.last_operation_metrics(target_path)
    n_bad = write_quarantine(bad, NPPES_TABLE, ds.publication, contract.primary_key)
    entry = {
        "table": NPPES_TABLE,
        "publication": ds.publication,
        "delta_version": metrics["version"],
        "operation": metrics["operation"],
        "metrics": {k: v for k, v in metrics.items() if k.startswith("num")},
        "source_rows_valid": good.count(),
        "stale_rows_skipped": stale,
        "quarantined": n_bad,
        "applied_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _append_ledger(entry)
    return entry
