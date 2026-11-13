from p360 import governance


def test_direct_pii_columns_are_classified():
    assert governance.column_pii("first_name") == "direct"
    assert governance.column_pii("practice_address") == "direct"
    assert governance.column_pii("partd_last_name") == "direct"
    assert governance.column_pii("practice_zip5") == "quasi"
    assert governance.column_pii("payment_usd") is None


def test_public_columns_drop_direct_pii_and_undeclared():
    cols = ["npi", "first_name", "payment_usd", "sneaky_new_column"]
    assert governance.public_columns(cols) == ["npi", "payment_usd"]
    assert governance.undeclared_columns(cols) == ["sneaky_new_column"]


def test_lineage_sources_exist_in_contracts():
    from p360 import contracts

    for col, srcs in governance.PROVIDER_360_LINEAGE.items():
        for c, alias in srcs:
            assert alias in {x.alias for x in contracts.load(c).columns}, (col, c, alias)
