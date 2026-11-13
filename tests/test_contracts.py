import pytest

from p360 import contracts


def test_missing_contract_column_is_breaking():
    c = contracts.load("partd_prescriber")
    header = [n for n in c.source_names if n != "Tot_Clms"]
    with pytest.raises(contracts.ContractViolation, match="Tot_Clms"):
        contracts.check_header(c, header)


def test_new_source_column_is_additive():
    c = contracts.load("partd_prescriber")
    report = contracts.check_header(c, c.source_names + ["Brand_New_Column"])
    assert report["added"] == ["Brand_New_Column"] and report["missing"] == []


def test_every_contract_loads_and_has_a_primary_key():
    from p360.landing import DATASETS

    for ds in DATASETS:
        c = contracts.load(ds.contract)
        aliases = {col.alias for col in c.columns}
        assert c.primary_key and set(c.primary_key) <= aliases, ds.contract


def test_renamed_column_in_a_real_file_stops_landing(tmp_root):
    """Take the real fixture file, rename one column in its header, and land it: must raise."""
    import shutil

    from p360 import landing
    from p360.download import load_sources

    ds = landing.BY_NAME["partd_prescriber"]
    raw = tmp_root / "drift_raw"
    src_dir = tmp_root / "data" / "raw" / ds.source
    shutil.copytree(src_dir, raw / ds.source)
    f = next((raw / ds.source).iterdir())
    text = f.read_text()
    f.write_text(text.replace("Tot_Clms", "Total_Claims", 1))
    assert load_sources()[ds.source]
    with pytest.raises(contracts.ContractViolation):
        landing.land(ds, raw_dir=raw, landing_dir=tmp_root / "drift_landing")
