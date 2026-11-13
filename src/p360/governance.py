"""PII policy and column lineage for the published provider table.

Every column of gold.provider_360 must declare where it comes from (contract + column, or "derived").
The public table keeps only columns whose sources are not direct PII. Both rules are checked mechanically
(an undeclared column or a direct-PII column in the public table fails the Dagster asset check and CI).

What this is NOT: anonymization. NPI is a public identifier and stays in the public table; all of this data is
already published by CMS. The point is the mechanism: classification in the contract drives what each
audience can see, and a new column cannot reach consumers without someone declaring its lineage.
"""

from p360 import contracts, lake

# gold.provider_360 column -> list of (contract, source alias); [] means computed from other gold columns
PROVIDER_360_LINEAGE: dict[str, list[tuple[str, str]]] = {
    "npi": [("nppes_provider", "npi"), ("partd_prescriber", "npi")],
    "entity_type": [("nppes_provider", "entity_type")],
    "first_name": [("nppes_provider", "first_name")],
    "last_name": [("nppes_provider", "last_name")],
    "org_name": [("nppes_provider", "org_name")],
    "credential": [("nppes_provider", "credential")],
    "primary_taxonomy": [("nppes_provider", f"taxonomy_{i}") for i in range(1, 16)],
    "practice_city": [("nppes_provider", "practice_city")],
    "practice_state": [("nppes_provider", "practice_state")],
    "practice_zip5": [("nppes_provider", "practice_zip")],
    "practice_address": [("nppes_provider", "practice_address")],
    "enumeration_date": [("nppes_provider", "enumeration_date")],
    "last_update_date": [("nppes_provider", "last_update_date")],
    "deactivation_date": [("nppes_provider", "deactivation_date")],
    "reactivation_date": [("nppes_provider", "reactivation_date")],
    "registry_publication": [],
    "payment_records": [("op_general_payment", "record_id")],
    "payment_usd": [("op_general_payment", "amount_usd")],
    "paying_companies": [("op_general_payment", "manufacturer_id")],
    "first_payment_date": [("op_general_payment", "payment_date")],
    "last_payment_date": [("op_general_payment", "payment_date")],
    "partd_total_claims": [("partd_prescriber", "total_claims")],
    "partd_total_drug_cost": [("partd_prescriber", "total_drug_cost")],
    "partd_prescriber_type": [("partd_prescriber", "prescriber_type")],
    "partd_drug_row_claims": [("partd_prescriber_drug", "total_claims")],
    "partd_brands_observed": [("partd_prescriber_drug", "brand_name")],
    "partd_state": [("partd_prescriber_drug", "state")],
    "partd_last_name": [("partd_prescriber_drug", "last_name")],
    "in_registry": [],
    "registry_status": [],
    "has_payments": [],
    "has_partd": [],
    "conflict_state_partd": [],
    "conflict_name_partd": [],
    "provenance": [],
}


def pii_level(contract_name: str, alias: str) -> str | None:
    c = contracts.load(contract_name)
    return next((col.pii for col in c.columns if col.alias == alias), None)


def column_pii(column: str) -> str | None:
    """Strictest PII level among a column's sources (direct > quasi > none)."""
    levels = {pii_level(c, a) for c, a in PROVIDER_360_LINEAGE[column]}
    return "direct" if "direct" in levels else "quasi" if "quasi" in levels else None


def undeclared_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if c not in PROVIDER_360_LINEAGE]


def public_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if c in PROVIDER_360_LINEAGE and column_pii(c) != "direct"]


def build_public() -> dict:
    df = lake.read("gold", "provider_360")
    missing = undeclared_columns(df.columns)
    if missing:
        raise ValueError(f"provider_360 has columns without declared lineage: {missing}")
    keep = public_columns(df.columns)
    df.select(*keep).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        lake.path("gold", "provider_360_public")
    )
    dropped = sorted(set(df.columns) - set(keep))
    return {"columns_public": len(keep), "columns_dropped_direct_pii": dropped}


def lineage_table() -> str:
    """Column-level lineage of provider_360 as a markdown table (docs/LINEAGE.md is generated from this)."""
    rows = [
        "| provider_360 column | sources (contract.column) | PII | in public table |",
        "|---|---|---|---|",
    ]
    for col, srcs in PROVIDER_360_LINEAGE.items():
        src = ", ".join(f"{c}.{a}" for c, a in srcs[:3]) + (" ..." if len(srcs) > 3 else "") or "derived"
        pii = column_pii(col) or "-"
        rows.append(f"| {col} | {src} | {pii} | {'no' if pii == 'direct' else 'yes'} |")
    return "\n".join(rows) + "\n"
