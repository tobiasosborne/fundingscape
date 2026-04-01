import os
import pytest
import duckdb

# Use in-memory DuckDB for tests
@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    from fundingscape.db import create_tables
    create_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def qa_db():
    """In-memory DuckDB for quantum applications tests."""
    conn = duckdb.connect(":memory:")
    from fundingscape.qa_db import create_tables
    create_tables(conn)
    yield conn
    conn.close()
