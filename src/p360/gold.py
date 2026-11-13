"""Gold: the provider master and the tables an analyst uses.

- recipient_resolution  one row per distinct recipient identity on payment records -> NPI + method
- fct_payment           live payments to people, with resolved NPI (MERGE by record_id, so it has a clean CDF)
- agg_provider_payments per-NPI payment totals, maintained INCREMENTALLY from fct_payment's change data feed
- partd_provider        per-NPI Medicare Part D: CMS totals (by-provider file) + observed by-drug subtotal
- provider_360          one row per NPI seen in Open Payments or Part D: registry identity + both activity
                        summaries + cross-source conflicts, each attribute labelled with its source
"""

import json
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from p360 import identity, lake
from p360.paths import LAKE
from p360.spark import get_spark

FCT_COLS = [
    "record_id",
    "resolved_npi",
    "resolution_method",
    "recipient_profile_id",
    "recipient_type",
    "manufacturer_id",
    "manufacturer_name",
    "amount_usd",
    "payment_date",
    "payment_nature",
    "payment_form",
    "product_name_1",
    "dispute_status",
]


def _merge_by_key(df: DataFrame, table: str, key: str) -> dict:
    """Make gold.<table> equal to df with one MERGE: update changed rows (by content hash), insert new ones,
    delete rows no longer present. Unchanged rows are not rewritten, so the change feed only shows real
    changes."""
    p = lake.path("gold", table)
    df = df.withColumn("_hash", lake.row_hash([c for c in df.columns if c != key]))
    if not lake.exists(p):
        df.write.format("delta").save(p)
    else:
        (
            DeltaTable.forPath(get_spark(), p)
            .alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition="t._hash <> s._hash")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    return lake.last_operation_metrics(p)


def build_recipient_resolution() -> dict:
    payments = lake.read("silver", "op_payment").where(~F.col("is_deleted"))
    res = identity.resolve(identity.distinct_recipients(payments))
    p = lake.path("gold", "recipient_resolution")
    res.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(p)
    return {"rows": lake.read("gold", "recipient_resolution").count()}


def build_fct_payment() -> dict:
    pay = lake.read("silver", "op_payment").where(
        ~F.col("is_deleted") & F.col("recipient_type").isin(identity.PERSON_TYPES)
    )
    keyed = pay.select(
        "*",
        identity.norm_name(F.col("recipient_first_name")).alias("fn"),
        identity.norm_name(F.col("recipient_last_name")).alias("ln"),
        identity.zip5(F.col("recipient_zip")).alias("zip5"),
        F.upper(F.col("recipient_state")).alias("state"),
    )
    res = lake.read("gold", "recipient_resolution").select(
        "recipient_profile_id",
        "recipient_npi_raw",
        "fn",
        "ln",
        "zip5",
        "state",
        "resolved_npi",
        F.col("method").alias("resolution_method"),
    )
    # null-safe join: missing profile ids / NPIs / ZIPs are part of the identity
    cond = [
        keyed[c].eqNullSafe(res[c])
        for c in ["recipient_profile_id", "recipient_npi_raw", "fn", "ln", "zip5", "state"]
    ]
    fct = (
        keyed.join(res, cond, "left")
        .select(
            *[keyed[c] for c in FCT_COLS if c not in ("resolved_npi", "resolution_method")],
            res["resolved_npi"],
            res["resolution_method"],
        )
        .select(*FCT_COLS)
    )
    return _merge_by_key(fct, "fct_payment", "record_id")


# ---------- incremental aggregate from the change data feed ----------
def _checkpoint_path() -> Path:
    return LAKE / "gold" / "_cdf_checkpoints.json"


def _checkpoints() -> dict:
    p = _checkpoint_path()
    return json.loads(p.read_text()) if p.exists() else {}


def _save_checkpoint(name: str, version: int) -> None:
    cps = _checkpoints()
    cps[name] = version
    p = _checkpoint_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(cps, indent=2))
    os.replace(tmp, p)


def _provider_aggregate(fct: DataFrame) -> DataFrame:
    return (
        fct.where(F.col("resolved_npi").isNotNull())
        .groupBy(F.col("resolved_npi").alias("npi"))
        .agg(
            F.count("*").alias("payment_records"),
            F.sum("amount_usd").alias("payment_usd"),
            F.countDistinct("manufacturer_id").alias("paying_companies"),
            F.min("payment_date").alias("first_payment_date"),
            F.max("payment_date").alias("last_payment_date"),
        )
    )


def build_agg_full(table: str = "agg_provider_payments_full") -> dict:
    """Reference implementation: recompute from scratch. Used to prove the incremental path is exact."""
    agg = _provider_aggregate(lake.read("gold", "fct_payment"))
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        lake.path("gold", table)
    )
    return {"rows": lake.read("gold", table).count()}


def build_agg_incremental(table: str = "agg_provider_payments") -> dict:
    """Read fct_payment changes since the last consumed version, recompute only the NPIs they touch
    (both before- and after-images, so an NPI that lost a payment is updated too), MERGE the result.

    Replay safety: the checkpoint is written only after the MERGE commits. If the process dies in between,
    the next run re-reads the same changes and recomputes the same NPIs: recompute-by-key is idempotent.
    """
    spark = get_spark()
    src = lake.path("gold", "fct_payment")
    target = lake.path("gold", table)
    latest = lake.latest_version(src)
    name = f"{table}<-fct_payment"
    last = _checkpoints().get(name)
    if last is None or not lake.exists(target):  # first run: full build, then start consuming the feed
        agg = _provider_aggregate(lake.read("gold", "fct_payment"))
        agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
        _save_checkpoint(name, latest)
        return {"mode": "full", "source_version": latest, "npis_recomputed": None}
    if last >= latest:
        return {"mode": "noop", "source_version": latest, "npis_recomputed": 0}
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last + 1)
        .option("endingVersion", latest)
        .load(src)
    )
    touched = (
        changes.where(F.col("resolved_npi").isNotNull()).select(F.col("resolved_npi").alias("npi")).distinct()
    )
    touched = touched.cache()  # small (NPIs only) and used twice
    n_touched = touched.count()
    fct_now = lake.read("gold", "fct_payment").join(
        touched.withColumnRenamed("npi", "resolved_npi"), "resolved_npi", "left_semi"
    )
    fresh = _provider_aggregate(fct_now)
    # NPIs touched but with no payments left must be deleted: carry them as tombstones
    upd = touched.join(fresh, "npi", "left").withColumn("_gone", F.col("payment_records").isNull())
    (
        DeltaTable.forPath(spark, target)
        .alias("t")
        .merge(upd.alias("s"), "t.npi = s.npi")
        .whenMatchedDelete(condition="s._gone")
        .whenMatchedUpdate(condition="NOT s._gone", set={c: f"s.{c}" for c in fresh.columns})
        .whenNotMatchedInsert(condition="NOT s._gone", values={c: f"s.{c}" for c in fresh.columns})
        .execute()
    )
    touched.unpersist()
    if os.environ.get("P360_CRASH_BEFORE_CHECKPOINT") == "1":  # used by the replay-safety test
        raise RuntimeError("injected crash after MERGE, before checkpoint")
    _save_checkpoint(name, latest)
    return {
        "mode": "incremental",
        "from_version": last + 1,
        "to_version": latest,
        "npis_recomputed": n_touched,
    }


def compare_agg(a: str = "agg_provider_payments", b: str = "agg_provider_payments_full") -> dict:
    x, y = lake.read("gold", a), lake.read("gold", b)
    cols = [c for c in y.columns]
    only_x = x.select(cols).exceptAll(y.select(cols)).count()
    only_y = y.select(cols).exceptAll(x.select(cols)).count()
    return {
        "rows_incremental": x.count(),
        "rows_full": y.count(),
        "only_in_incremental": only_x,
        "only_in_full": only_y,
    }


# ---------- Part D and the provider master ----------
def build_partd_provider() -> dict:
    """Per-NPI Part D: CMS's own totals from the by-provider file, plus what the by-drug rows add up to.

    The by-drug file omits drug rows with fewer than 11 claims, so `partd_drug_row_claims` is an observed
    subtotal (about 87% of claims nationally), never the provider's total. `partd_total_claims` is the total.
    """
    d = lake.read("silver", "partd_prescriber_drug")
    drugs = d.groupBy("npi").agg(
        F.sum("total_claims").alias("partd_drug_row_claims"),
        F.countDistinct("brand_name").alias("partd_brands_observed"),
        F.first("state", ignorenulls=True).alias("partd_state"),
        F.first("last_name", ignorenulls=True).alias("partd_last_name"),
    )
    prov = lake.read("silver", "partd_prescriber").select(
        "npi",
        F.col("total_claims").alias("partd_total_claims"),
        F.col("total_drug_cost").alias("partd_total_drug_cost"),
        F.col("prescriber_type").alias("partd_prescriber_type"),
    )
    agg = prov.join(drugs, "npi", "full_outer")
    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        lake.path("gold", "partd_provider")
    )
    return {"rows": lake.read("gold", "partd_provider").count()}


def primary_taxonomy(df: DataFrame) -> DataFrame:
    slots = [F.when(F.col(f"taxonomy_primary_{i}") == "Y", F.col(f"taxonomy_{i}")) for i in range(1, 16)]
    return df.withColumn("primary_taxonomy", F.coalesce(*slots, F.col("taxonomy_1")))


def build_provider_360() -> dict:
    n = primary_taxonomy(lake.read("silver", "nppes_provider"))
    pay = lake.read("gold", "agg_provider_payments")
    rx = lake.read("gold", "partd_provider")
    ids = pay.select("npi").unionByName(rx.select("npi")).distinct()
    reg = n.select(
        "npi",
        "entity_type",
        "first_name",
        "last_name",
        "org_name",
        "credential",
        "primary_taxonomy",
        "practice_city",
        "practice_state",
        identity.zip5(F.col("practice_zip")).alias("practice_zip5"),
        "practice_address",
        "enumeration_date",
        "last_update_date",
        "deactivation_date",
        "reactivation_date",
        F.col("source_publication").alias("registry_publication"),
        F.lit(True).alias("_in_registry"),  # sentinel: deactivated rows have no entity type or name
    )
    p360 = (
        ids.join(reg, "npi", "left")
        .join(pay, "npi", "left")
        .join(rx, "npi", "left")
        .withColumn("in_registry", F.coalesce(F.col("_in_registry"), F.lit(False)))
        .drop("_in_registry")
        .withColumn(
            "registry_status",
            F.when(~F.col("in_registry"), "not_in_registry")
            .when(
                F.col("deactivation_date").isNotNull()
                & (
                    F.col("reactivation_date").isNull()
                    | (F.col("reactivation_date") < F.col("deactivation_date"))
                ),
                "deactivated",
            )
            .otherwise("active"),
        )
        .withColumn("has_payments", F.col("payment_records").isNotNull())
        .withColumn("has_partd", F.col("partd_total_claims").isNotNull())
        # conflicts between sources (NPPES is the golden source for identity; the others are evidence)
        .withColumn(
            "conflict_state_partd",
            F.col("partd_state").isNotNull()
            & F.col("practice_state").isNotNull()
            & (F.upper("partd_state") != F.upper("practice_state")),
        )
        .withColumn(
            "conflict_name_partd",
            F.col("partd_last_name").isNotNull()
            & F.col("last_name").isNotNull()
            & (identity.norm_name(F.col("partd_last_name")) != identity.norm_name(F.col("last_name"))),
        )
        .withColumn(
            "provenance",
            F.lit(
                json.dumps(
                    {
                        "identity, taxonomy, location, status": "NPPES",
                        "payment_*": "Open Payments (resolved via recipient_resolution)",
                        "partd_total_*, partd_prescriber_type": "Medicare Part D by provider (CMS totals)",
                        "partd_drug_row_claims, partd_brands_observed": "Part D by provider and drug (unsuppressed rows)",
                    }
                )
            ),
        )
    )
    p360.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        lake.path("gold", "provider_360")
    )
    return {"rows": lake.read("gold", "provider_360").count()}
