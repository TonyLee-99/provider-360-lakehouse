"""Dagster definitions: every table is an asset, every contract/referee rule is an asset check.

Why Dagster here: the pipeline is a graph of tables with quality rules between them. Dagster models exactly
that (assets + checks), shows the dependency graph as table-level lineage, and a failing *blocking* check
stops everything downstream of it in the same run, so a bad table is never used to build the next one.
Gold is published only by the last asset (`published`), which records the Delta version of each gold table
that passed; readers use those versions (write-audit-publish with Delta time travel).

All assets run in one process (in_process_executor) because they share one local Spark session.
"""

import json
import os
import time

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    Config,
    Definitions,
    MaterializeResult,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
)
from pyspark.sql import functions as F

from p360 import bronze, gold, governance, lake, landing, reconcile, silver
from p360.paths import LAKE
from p360.results import record

QUARANTINE_BUDGET = 0.001  # at most 0.1% of a publication may fail its contract


def _timed(fn, *a, **k):
    t0 = time.time()
    out = fn(*a, **k)
    return out, round(time.time() - t0, 1)


def _log(name: str, payload: dict) -> None:
    run_log = LAKE / "_run_log.jsonl"
    run_log.parent.mkdir(parents=True, exist_ok=True)
    with run_log.open("a") as f:
        f.write(json.dumps({"asset": name, **payload}, default=str) + "\n")


# ---------------- landing + bronze ----------------
@asset(group_name="bronze", compute_kind="pyarrow+spark")
def bronze_tables(context: AssetExecutionContext) -> MaterializeResult:
    """Land every downloaded file (zip -> all-string Parquet, skipped if already landed) and load bronze."""
    meta = {}
    for ds in landing.DATASETS:
        landing.land_all([ds.name])
        out, secs = _timed(bronze.load_bronze, ds.name)
        meta[ds.name] = out["rows"]
        _log(f"bronze.{ds.name}", {"rows": out["rows"], "seconds": secs})
    return MaterializeResult(metadata={k: int(v) for k, v in meta.items()})


# ---------------- silver ----------------
class OpConfig(Config):
    publications: list[str] = ["2026-01", "2026-06"]  # applied in order; already-applied ones are skipped


@asset(deps=[bronze_tables], group_name="silver", compute_kind="spark+delta")
def silver_op_payment(context: AssetExecutionContext, config: OpConfig) -> MaterializeResult:
    done = silver.applied(silver.OP_TABLE)
    entries = []
    for pub in config.publications:
        if pub in done:
            continue
        entry, secs = _timed(silver.apply_op_publication, pub)
        entry["seconds"] = secs
        entries.append(entry)
        _log("silver.op_payment", entry)
        context.log.info(f"applied {pub}: {entry['metrics']}")
    live = lake.read("silver", "op_payment").where(~F.col("is_deleted")).count()
    return MaterializeResult(
        metadata={"live_rows": live, "applied_now": ", ".join(e["publication"] for e in entries)}
    )


@asset(deps=[bronze_tables], group_name="silver", compute_kind="spark+delta")
def silver_nppes_provider(context: AssetExecutionContext) -> MaterializeResult:
    done = silver.applied(silver.NPPES_TABLE)
    for ds, pub in [("nppes_full", "2026-11-02"), ("nppes_weekly_2026_11_08", "2026-11-08")]:
        if pub in done:
            continue
        entry, secs = _timed(silver.apply_nppes, ds)
        entry["seconds"] = secs
        _log("silver.nppes_provider", entry)
    return MaterializeResult(metadata={"rows": lake.read("silver", "nppes_provider").count()})


@asset(deps=[bronze_tables], group_name="silver", compute_kind="spark+delta")
def silver_reference_tables(context: AssetExecutionContext) -> MaterializeResult:
    meta = {}
    for t in silver.STATIC:
        out, secs = _timed(silver.build_static, t)
        meta[t] = out["rows"]
        _log(f"silver.{t}", {**out, "seconds": secs})
    return MaterializeResult(metadata=meta)


def _quarantine_rate(table: str, publication: str, rows: int) -> float:
    q = lake.read("silver", "quarantine").where(
        (F.col("table") == table) & (F.col("publication") == publication)
    )
    return q.count() / max(rows, 1)


@asset_check(asset=silver_op_payment, blocking=True, description="every applied publication within budget")
def op_contract_quarantine_budget() -> AssetCheckResult:
    rates = {
        e["publication"]: _quarantine_rate("op_payment", e["publication"], e["source_rows_valid"])
        for e in silver.ledger()
        if e["table"] == "op_payment"
    }
    return AssetCheckResult(
        passed=all(r <= QUARANTINE_BUDGET for r in rates.values()),
        metadata={k: float(v) for k, v in rates.items()},
    )


@asset_check(asset=silver_op_payment, blocking=True, description="record_id unique in silver")
def op_primary_key_unique() -> AssetCheckResult:
    df = lake.read("silver", "op_payment")
    dupes = df.count() - df.select("record_id").distinct().count()
    return AssetCheckResult(passed=dupes == 0, metadata={"duplicate_keys": dupes})


@asset_check(
    asset=silver_op_payment,
    blocking=True,
    description="R1: live count and USD total equal the CMS dashboard for the latest publication",
)
def op_matches_cms_totals() -> AssetCheckResult:
    if silver.applied(silver.OP_TABLE)[-1:] != ["2026-06"]:
        return AssetCheckResult(passed=True, metadata={"skipped": "dashboard is for the 2026-06 publication"})
    if os.environ.get("P360_SCOPE") == "fixture":  # national totals cannot match a two-ZIP sample
        return AssetCheckResult(passed=True, metadata={"skipped": "fixture scope"})
    r = reconcile.r1_open_payments_totals()
    record("r1_open_payments_totals", r)
    return AssetCheckResult(passed=r["exact"], metadata={k: str(v) for k, v in r.items()})


@asset_check(
    asset=silver_reference_tables,
    blocking=True,
    description="each reference table within the quarantine budget",
)
def reference_quarantine_budget() -> AssetCheckResult:
    rates = {}
    for t in silver.STATIC:
        good = lake.read("silver", t).count()
        q = lake.read("silver", "quarantine").where(F.col("table") == t).count()
        rates[t] = q / max(good + q, 1)
    return AssetCheckResult(
        passed=all(r <= QUARANTINE_BUDGET for r in rates.values()),
        metadata={k: float(v) for k, v in rates.items()},
    )


@asset_check(
    asset=silver_nppes_provider, blocking=True, description="every NPPES file within the quarantine budget"
)
def nppes_quarantine_budget() -> AssetCheckResult:
    rates = {
        e["publication"]: _quarantine_rate("nppes_provider", e["publication"], e["source_rows_valid"])
        for e in silver.ledger()
        if e["table"] == "nppes_provider"
    }
    return AssetCheckResult(
        passed=all(r <= QUARANTINE_BUDGET for r in rates.values()),
        metadata={k: float(v) for k, v in rates.items()},
    )


@asset_check(asset=silver_nppes_provider, blocking=True, description="npi unique")
def nppes_primary_key_unique() -> AssetCheckResult:
    df = lake.read("silver", "nppes_provider")
    dupes = df.count() - df.select("npi").distinct().count()
    return AssetCheckResult(passed=dupes == 0, metadata={"duplicate_keys": dupes})


# ---------------- gold ----------------
@asset(
    deps=[silver_op_payment, silver_nppes_provider, silver_reference_tables],
    group_name="gold",
    compute_kind="spark+delta",
)
def gold_recipient_resolution() -> MaterializeResult:
    out, secs = _timed(gold.build_recipient_resolution)
    _log("gold.recipient_resolution", {**out, "seconds": secs})
    by = lake.read("gold", "recipient_resolution").groupBy("method").count().collect()
    return MaterializeResult(metadata={r["method"]: r["count"] for r in by})


@asset(deps=[gold_recipient_resolution], group_name="gold", compute_kind="spark+delta")
def gold_fct_payment() -> MaterializeResult:
    out, secs = _timed(gold.build_fct_payment)
    _log("gold.fct_payment", {**out, "seconds": secs})
    return MaterializeResult(metadata={k: str(v) for k, v in out.items()})


@asset(deps=[gold_fct_payment], group_name="gold", compute_kind="spark+delta")
def gold_agg_provider_payments() -> MaterializeResult:
    out, secs = _timed(gold.build_agg_incremental)
    _log("gold.agg_provider_payments", {**out, "seconds": secs})
    return MaterializeResult(metadata={k: str(v) for k, v in out.items()})


@asset_check(
    asset=gold_agg_provider_payments,
    blocking=True,
    description="incremental (change data feed) result equals a full recompute, row for row",
)
def agg_incremental_equals_full() -> AssetCheckResult:
    _, secs = _timed(gold.build_agg_full)
    cmp = gold.compare_agg()
    _log("check.agg_incremental_equals_full", {**cmp, "full_rebuild_seconds": secs})
    ok = cmp["only_in_incremental"] == 0 and cmp["only_in_full"] == 0
    return AssetCheckResult(passed=ok, metadata=cmp)


@asset(deps=[silver_reference_tables], group_name="gold", compute_kind="spark+delta")
def gold_partd_provider() -> MaterializeResult:
    out, secs = _timed(gold.build_partd_provider)
    _log("gold.partd_provider", {**out, "seconds": secs})
    return MaterializeResult(metadata=out)


@asset_check(
    asset=gold_partd_provider,
    blocking=True,
    description="R3: by-drug claims never exceed CMS's by-provider totals",
)
def partd_matches_provider_file() -> AssetCheckResult:
    r = reconcile.r3_partd_by_provider()
    record("r3_partd_by_provider", r)
    return AssetCheckResult(
        passed=r["unexplained_npis"] == 0,
        metadata={"exact_pct": r["exact_pct"], "unexplained_npis": r["unexplained_npis"]},
    )


@asset(
    deps=[gold_agg_provider_payments, gold_partd_provider, silver_nppes_provider],
    group_name="gold",
    compute_kind="spark+delta",
)
def gold_provider_360() -> MaterializeResult:
    out, secs = _timed(gold.build_provider_360)
    _log("gold.provider_360", {**out, "seconds": secs})
    return MaterializeResult(metadata=out)


@asset(deps=[gold_provider_360], group_name="gold", compute_kind="spark+delta")
def gold_provider_360_public() -> MaterializeResult:
    out = governance.build_public()
    return MaterializeResult(
        metadata={
            "columns_public": out["columns_public"],
            "dropped": ", ".join(out["columns_dropped_direct_pii"]),
        }
    )


@asset_check(
    asset=gold_provider_360_public,
    blocking=True,
    description="no column derived from direct PII; every column has declared lineage",
)
def public_has_no_direct_pii() -> AssetCheckResult:
    cols = lake.read("gold", "provider_360_public").columns
    bad = [c for c in cols if governance.column_pii(c) == "direct"]
    undeclared = governance.undeclared_columns(lake.read("gold", "provider_360").columns)
    return AssetCheckResult(
        passed=not bad and not undeclared,
        metadata={
            "direct_pii_columns": ", ".join(bad) or "none",
            "undeclared": ", ".join(undeclared) or "none",
        },
    )


PUBLISHED_TABLES = [
    "fct_payment",
    "agg_provider_payments",
    "partd_provider",
    "provider_360",
    "provider_360_public",
]


@asset(deps=[gold_provider_360_public, gold_agg_provider_payments, gold_partd_provider], group_name="publish")
def published() -> MaterializeResult:
    """Runs only if every blocking check upstream passed. Records the Delta version consumers should read."""
    versions = {t: lake.latest_version(lake.path("gold", t)) for t in PUBLISHED_TABLES}
    manifest = LAKE / "gold" / "_published.json"
    tmp = manifest.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"versions": versions, "published_at": time.strftime("%Y-%m-%dT%H:%M:%S")}, indent=2)
    )
    os.replace(tmp, manifest)
    return MaterializeResult(metadata={t: v for t, v in versions.items()})


ALL_ASSETS = [
    bronze_tables,
    silver_op_payment,
    silver_nppes_provider,
    silver_reference_tables,
    gold_recipient_resolution,
    gold_fct_payment,
    gold_agg_provider_payments,
    gold_partd_provider,
    gold_provider_360,
    gold_provider_360_public,
    published,
]
ALL_CHECKS = [
    op_contract_quarantine_budget,
    op_primary_key_unique,
    op_matches_cms_totals,
    nppes_primary_key_unique,
    nppes_quarantine_budget,
    reference_quarantine_budget,
    agg_incremental_equals_full,
    partd_matches_provider_file,
    public_has_no_direct_pii,
]

pipeline = define_asset_job("pipeline", selection="*", executor_def=in_process_executor)

defs = Definitions(assets=ALL_ASSETS, asset_checks=ALL_CHECKS, jobs=[pipeline])

__all__ = ["defs", "AssetKey"]
