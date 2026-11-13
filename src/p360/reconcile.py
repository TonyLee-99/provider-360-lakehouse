"""Referees: compare what we built with totals CMS publishes itself.

R1  Open Payments general payments, program year 2024: our live record count and dollar total vs the CMS
    summary dashboard of the same publication (general-payments rows only, not the all-category headline).
R2  Per recipient: our totals per covered recipient vs CMS's "payments grouped by covered recipient and nature
    of payments" file, summed over natures. Key = (recipient type, id): physicians by Profile_ID, teaching
    hospitals by CCN (the first version keyed hospitals by Teaching_Hospital_ID and collided with profile ids,
    see docs/INCIDENTS.md #5). Report exact / mismatched, never "close enough".
R3  Part D: per-NPI claims summed from the by-provider-and-drug file vs CMS's by-provider file. Drug rows
    with fewer than 11 claims are suppressed in the by-drug file, so our sum can only be LOWER; anything else
    is unexplained and is reported as such.
"""

import csv
from decimal import Decimal

from pyspark.sql import functions as F

from p360 import lake
from p360.download import load_sources, raw_path


def _dashboard_value(metric: str, year: str = "PY_2024") -> str:
    spec = load_sources()["op_summary_dashboard_p2026_06"]
    with raw_path("op_summary_dashboard_p2026_06", spec["url"]).open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["Data_Metrics"].strip() == metric:
                return row[year]
    raise KeyError(metric)


def r1_open_payments_totals() -> dict:
    cms_n = int(_dashboard_value("Total Number of all Payment Records - General Payments"))
    cms_usd = Decimal(_dashboard_value("Total Dollar Amount of all Payment Records - General Payments"))
    live = lake.read("silver", "op_payment").where(~F.col("is_deleted") & (F.col("program_year") == 2024))
    r = live.agg(F.count("*").alias("n"), F.sum("amount_usd").alias("usd")).first()
    q = lake.read("silver", "quarantine").where(
        (F.col("table") == "op_payment") & (F.col("publication") == "2026-06")
    )
    n_q = q.count()
    ours_usd = Decimal(r["usd"])
    return {
        "cms_records": cms_n,
        "our_live_records": r["n"],
        "our_quarantined_records": n_q,
        "records_diff": r["n"] - cms_n,
        "cms_usd": str(cms_usd),
        "our_usd": str(ours_usd),
        "usd_diff": str(ours_usd - cms_usd),
        "exact": r["n"] == cms_n and ours_usd == cms_usd,
    }


def r2_recipient_totals() -> dict:
    s = lake.read("silver", "op_recipient_nature_summary")
    cms = s.groupBy("recipient_type", "recipient_id").agg(
        F.sum("n_transactions").alias("cms_n"),
        F.sum("amount_usd").alias("cms_usd"),
    )
    pay = lake.read("silver", "op_payment").where(~F.col("is_deleted"))
    ours = (
        pay.withColumn(
            "recipient_id",
            F.when(
                F.col("recipient_type") == "Covered Recipient Teaching Hospital",
                F.col("teaching_hospital_ccn").cast("bigint"),
            ).otherwise(F.col("recipient_profile_id")),
        )
        .groupBy("recipient_type", "recipient_id")
        .agg(F.count("*").alias("our_n"), F.sum("amount_usd").alias("our_usd"))
    )
    j = cms.join(ours, ["recipient_type", "recipient_id"], "full_outer")
    cls = (
        F.when(F.col("our_n").isNull(), "only_in_cms")
        .when(F.col("cms_n").isNull(), "only_in_ours")
        .when((F.col("our_n") == F.col("cms_n")) & (F.col("our_usd") == F.col("cms_usd")), "exact")
        .when(F.col("our_usd") == F.col("cms_usd"), "amount_exact_count_differs")
        .otherwise("amount_differs")
    )
    rows = (
        j.withColumn("cls", cls)
        .groupBy("cls")
        .agg(
            F.count("*").alias("recipients"),
            F.sum(F.coalesce(F.col("cms_usd"), F.lit(0))).alias("cms_usd"),
            F.sum(F.coalesce(F.col("our_usd"), F.lit(0))).alias("our_usd"),
        )
        .collect()
    )
    out = {
        r["cls"]: {"recipients": r["recipients"], "cms_usd": str(r["cms_usd"]), "our_usd": str(r["our_usd"])}
        for r in rows
    }
    total = sum(v["recipients"] for v in out.values())
    return {
        "classes": out,
        "recipients_compared": total,
        "exact_pct": round(100 * out.get("exact", {}).get("recipients", 0) / total, 3) if total else None,
    }


def r3_partd_by_provider() -> dict:
    d = (
        lake.read("silver", "partd_prescriber_drug")
        .groupBy("npi")
        .agg(F.sum("total_claims").alias("drug_claims"), F.count("*").alias("drug_rows"))
    )
    p = lake.read("silver", "partd_prescriber").select("npi", F.col("total_claims").alias("prov_claims"))
    j = d.join(p, "npi", "full_outer")
    cls = (
        F.when(F.col("drug_claims").isNull(), "provider_has_no_drug_rows")
        .when(F.col("prov_claims").isNull(), "drug_rows_but_no_provider_row")
        .when(F.col("drug_claims") == F.col("prov_claims"), "exact")
        .when(F.col("drug_claims") < F.col("prov_claims"), "lower_consistent_with_suppression")
        .otherwise("higher_unexplained")
    )
    rows = (
        j.withColumn("cls", cls)
        .groupBy("cls")
        .agg(
            F.count("*").alias("npis"),
            F.sum(F.coalesce("prov_claims", F.lit(0))).alias("prov_claims"),
            F.sum(F.coalesce("drug_claims", F.lit(0))).alias("drug_claims"),
            F.max(F.col("prov_claims") - F.col("drug_claims")).alias("max_gap"),
        )
        .collect()
    )
    out = {
        r["cls"]: {
            "npis": r["npis"],
            "provider_file_claims": r["prov_claims"],
            "drug_file_claims": r["drug_claims"],
            "max_gap": r["max_gap"],
        }
        for r in rows
    }
    total = sum(v["npis"] for v in out.values())
    unexplained = sum(
        out.get(k, {}).get("npis", 0) for k in ("higher_unexplained", "drug_rows_but_no_provider_row")
    )
    return {
        "classes": out,
        "npis_compared": total,
        "exact_pct": round(100 * out.get("exact", {}).get("npis", 0) / total, 3) if total else None,
        "unexplained_npis": unexplained,
    }
