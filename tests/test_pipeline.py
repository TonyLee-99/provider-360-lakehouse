"""End to end on the real two-ZIP fixture: two Dagster runs (January publication, then June), then the
properties the project claims: blocking checks pass, incremental == full, re-apply is a no-op, guards refuse
bad inputs, and a crash between MERGE and checkpoint is replay-safe."""

import json

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def pipeline():
    from p360.cli import _run_pipeline

    first = _run_pipeline(["2026-01"], "test1")
    second = _run_pipeline(["2026-01", "2026-06"], "test2")
    return first, second


def test_both_runs_succeed_with_all_checks_passing(pipeline):
    for run in pipeline:
        assert run["success"], run
        failed = [c for c in run["checks"] if not c["passed"]]
        assert not failed, failed
    names = {c["check"] for c in pipeline[1]["checks"]}
    assert {"agg_incremental_equals_full", "public_has_no_direct_pii", "op_primary_key_unique"} <= names


def test_second_run_was_incremental(pipeline):
    from p360 import gold

    cps = json.loads(gold._checkpoint_path().read_text())
    assert cps["agg_provider_payments<-fct_payment"] > 0
    cmp = gold.compare_agg()
    assert cmp["only_in_incremental"] == 0 and cmp["only_in_full"] == 0


def test_silver_has_soft_deletes_and_live_rows(pipeline):
    from p360 import lake

    op = lake.read("silver", "op_payment")
    assert op.where(F.col("is_deleted")).count() > 0
    assert op.where(~F.col("is_deleted")).count() == lake.read("bronze", "op_payment_p2026_06").count() - (
        lake.read("silver", "quarantine").where("table = 'op_payment' AND publication = '2026-06'").count()
    )


def test_restatement_crosstab_mostly_agrees_with_cms(pipeline):
    from p360 import restatement

    r = restatement.compare_with_cms("2026-06")
    assert r["agree_pct"] > 90, r


def test_reapply_is_a_noop_and_older_publication_is_refused(pipeline):
    from p360 import silver

    e = silver.apply_op_publication("2026-06")
    m = e["metrics"]
    assert int(m["numTargetRowsInserted"]) == 0 and int(m["numTargetRowsUpdated"]) == 0
    w = silver.apply_nppes("nppes_weekly_2026_11_08")
    assert int(w["metrics"].get("numTargetRowsUpdated", 0)) == 0
    assert int(w["metrics"].get("numTargetRowsInserted", 0)) == 0
    with pytest.raises(silver.PublicationRefused):
        silver.apply_op_publication("2026-01")


def test_truncated_publication_is_refused(pipeline, monkeypatch):
    from p360 import silver

    monkeypatch.setattr(silver, "MIN_REPUBLICATION_RATIO", 1.5)  # as if the file were a third short
    with pytest.raises(silver.PublicationRefused, match="refusing to soft-delete"):
        silver.apply_op_publication("2026-06")


def test_public_table_has_no_direct_pii(pipeline):
    from p360 import governance, lake

    cols = lake.read("gold", "provider_360_public").columns
    assert "first_name" not in cols and "practice_address" not in cols and "npi" in cols
    assert not [c for c in cols if governance.column_pii(c) == "direct"]


def test_crash_between_merge_and_checkpoint_is_replay_safe(pipeline, monkeypatch):
    """Change fct_payment, crash the incremental aggregate after its MERGE, rerun, compare with full."""
    from delta.tables import DeltaTable

    from p360 import gold, lake
    from p360.spark import get_spark

    fct = DeltaTable.forPath(get_spark(), lake.path("gold", "fct_payment"))
    npi = lake.read("gold", "fct_payment").where(F.col("resolved_npi").isNotNull()).first()["resolved_npi"]
    fct.update(F.col("resolved_npi") == npi, {"amount_usd": F.col("amount_usd") + F.lit(1)})
    monkeypatch.setenv("P360_CRASH_BEFORE_CHECKPOINT", "1")
    with pytest.raises(RuntimeError, match="injected crash"):
        gold.build_agg_incremental()
    monkeypatch.delenv("P360_CRASH_BEFORE_CHECKPOINT")
    out = gold.build_agg_incremental()  # replays the same changes
    assert out["mode"] == "incremental"
    gold.build_agg_full()
    cmp = gold.compare_agg()
    assert cmp["only_in_incremental"] == 0 and cmp["only_in_full"] == 0


def test_bad_rows_are_quarantined_not_dropped(pipeline):
    """Inject contract violations into a copy of a real bronze table and type it."""
    from p360 import contracts, lake

    c = contracts.load("partd_prescriber")
    src = lake.read("bronze", "partd_prescriber").limit(5)
    bad = src.withColumn("Tot_Clms", F.lit("not-a-number")).limit(2)
    dup = src.limit(1)
    typed = lake.apply_contract(src.unionByName(bad).unionByName(dup).select(*c.source_names), c)
    good, quarantined = lake.split_good_bad(typed, c.primary_key)
    reasons = {r for row in quarantined.collect() for r in row["_reasons"]}
    assert "bad_bigint:total_claims" in reasons and "duplicate_key" in reasons
    assert good.count() + quarantined.count() == 8


def test_crash_after_silver_commit_recovers_ledger_from_delta_history(pipeline):
    """Simulate a crash after the June MERGE committed but before the ledger was written: drop the June
    entries from the ledger and re-apply. The retry must find the tagged commit, not merge again."""
    import json as _json

    from p360 import lake, silver

    p = silver._ledger_path()
    saved = p.read_text()
    entries = _json.loads(saved)
    p.write_text(
        _json.dumps(
            [e for e in entries if not (e["table"] == "op_payment" and e["publication"] == "2026-06")]
        )
    )
    before = lake.latest_version(lake.path("silver", "op_payment"))
    try:
        e = silver.apply_op_publication("2026-06")
        assert e["recovered_from_commit"] is True
        assert lake.latest_version(lake.path("silver", "op_payment")) == before  # no new MERGE commit
    finally:
        p.write_text(saved)
