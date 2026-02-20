def test_import():
    import fundingscape
    assert fundingscape.DB_PATH == "data/db/fundingscape.duckdb"
