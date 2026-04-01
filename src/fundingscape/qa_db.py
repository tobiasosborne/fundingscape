"""Database layer for the Quantum Applications database.

Separate from the fundingscape grants DB — this catalogues every proposed
quantum computing application with advantage classification and funding links.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import duckdb

from fundingscape import QA_DB_PATH
from fundingscape.qa_models import Application, FundingLink, IndustrySector, Reference


def get_connection(path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the quantum applications DB."""
    path = path or QA_DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    create_tables(conn)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't exist."""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS application (
            id INTEGER PRIMARY KEY,
            domain TEXT NOT NULL,
            subdomain TEXT NOT NULL,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            quantum_approaches TEXT[],
            advantage_type TEXT NOT NULL DEFAULT 'unknown',
            advantage_status TEXT NOT NULL DEFAULT 'unknown',
            classical_baseline TEXT,
            quantum_complexity TEXT,
            maturity TEXT NOT NULL DEFAULT 'theoretical',
            year_first_proposed INTEGER,
            seminal_reference TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_application START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reference (
            id INTEGER PRIMARY KEY,
            application_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            authors TEXT,
            year INTEGER,
            doi TEXT,
            arxiv_id TEXT,
            contribution_type TEXT NOT NULL DEFAULT 'first_proposal',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_reference START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS industry_sector (
            id INTEGER PRIMARY KEY,
            application_id INTEGER NOT NULL,
            sector TEXT NOT NULL,
            relevance_notes TEXT,
            UNIQUE (application_id, sector)
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_industry_sector START 1")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_link (
            id INTEGER PRIMARY KEY,
            application_id INTEGER NOT NULL UNIQUE,
            query_pattern TEXT NOT NULL,
            grant_count INTEGER DEFAULT 0,
            total_funding_eur DOUBLE DEFAULT 0.0,
            top_funders TEXT,
            last_computed TIMESTAMP
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_funding_link START 1")

    # Indexes for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_app_domain
        ON application (domain)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_app_subdomain
        ON application (domain, subdomain)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_app_advantage
        ON application (advantage_type, advantage_status)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ref_app
        ON reference (application_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sector_app
        ON industry_sector (application_id)
    """)

    # Summary view: roll up to subdomain level for non-specialist queries
    conn.execute("""
        CREATE OR REPLACE VIEW application_summary AS
        SELECT
            domain,
            subdomain,
            COUNT(*) AS application_count,
            LIST(name ORDER BY name) AS applications,
            -- best advantage in the subdomain
            CASE
                WHEN BOOL_OR(advantage_type = 'exponential') THEN 'exponential'
                WHEN BOOL_OR(advantage_type = 'superpolynomial') THEN 'superpolynomial'
                WHEN BOOL_OR(advantage_type = 'polynomial') THEN 'polynomial'
                WHEN BOOL_OR(advantage_type = 'quadratic') THEN 'quadratic'
                WHEN BOOL_OR(advantage_type = 'subquadratic') THEN 'subquadratic'
                WHEN BOOL_OR(advantage_type = 'constant') THEN 'constant'
                ELSE 'unknown'
            END AS best_advantage_type,
            -- strongest evidence in the subdomain
            CASE
                WHEN BOOL_OR(advantage_status = 'proven') THEN 'proven'
                WHEN BOOL_OR(advantage_status = 'proven_with_caveats') THEN 'proven_with_caveats'
                WHEN BOOL_OR(advantage_status = 'conjectured') THEN 'conjectured'
                WHEN BOOL_OR(advantage_status = 'heuristic_only') THEN 'heuristic_only'
                ELSE 'unknown'
            END AS strongest_evidence,
            MIN(year_first_proposed) AS earliest_year
        FROM application
        GROUP BY domain, subdomain
        ORDER BY domain, subdomain
    """)

    # Domain-level rollup for executive summaries
    conn.execute("""
        CREATE OR REPLACE VIEW domain_summary AS
        SELECT
            domain,
            COUNT(*) AS application_count,
            COUNT(DISTINCT subdomain) AS subdomain_count,
            LIST(DISTINCT subdomain ORDER BY subdomain) AS subdomains,
            CASE
                WHEN BOOL_OR(advantage_type = 'exponential') THEN 'exponential'
                WHEN BOOL_OR(advantage_type = 'superpolynomial') THEN 'superpolynomial'
                WHEN BOOL_OR(advantage_type = 'polynomial') THEN 'polynomial'
                WHEN BOOL_OR(advantage_type = 'quadratic') THEN 'quadratic'
                ELSE 'unknown'
            END AS best_advantage_type,
            MIN(year_first_proposed) AS earliest_year
        FROM application
        GROUP BY domain
        ORDER BY domain
    """)


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------


def insert_application(
    conn: duckdb.DuckDBPyConnection, app: Application
) -> int:
    """Insert a new application. Returns the new ID."""
    app_id = conn.execute("SELECT nextval('seq_application')").fetchone()[0]
    conn.execute(
        """INSERT INTO application
           (id, domain, subdomain, name, description, quantum_approaches,
            advantage_type, advantage_status, classical_baseline,
            quantum_complexity, maturity, year_first_proposed,
            seminal_reference, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            app_id,
            app.domain,
            app.subdomain,
            app.name,
            app.description,
            app.quantum_approaches,
            app.advantage_type,
            app.advantage_status,
            app.classical_baseline,
            app.quantum_complexity,
            app.maturity,
            app.year_first_proposed,
            app.seminal_reference,
            app.notes,
        ],
    )
    return app_id


def upsert_application(
    conn: duckdb.DuckDBPyConnection, app: Application
) -> int:
    """Insert or update an application by name. Returns the ID."""
    existing = conn.execute(
        "SELECT id FROM application WHERE name = ?", [app.name]
    ).fetchone()
    if existing:
        app_id = existing[0]
        conn.execute(
            """UPDATE application SET
               domain=?, subdomain=?, description=?, quantum_approaches=?,
               advantage_type=?, advantage_status=?, classical_baseline=?,
               quantum_complexity=?, maturity=?, year_first_proposed=?,
               seminal_reference=?, notes=?, updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            [
                app.domain,
                app.subdomain,
                app.description,
                app.quantum_approaches,
                app.advantage_type,
                app.advantage_status,
                app.classical_baseline,
                app.quantum_complexity,
                app.maturity,
                app.year_first_proposed,
                app.seminal_reference,
                app.notes,
                app_id,
            ],
        )
        return app_id
    return insert_application(conn, app)


def insert_reference(
    conn: duckdb.DuckDBPyConnection, ref: Reference
) -> int:
    """Insert a reference. Returns the new ID."""
    ref_id = conn.execute("SELECT nextval('seq_reference')").fetchone()[0]
    conn.execute(
        """INSERT INTO reference
           (id, application_id, title, authors, year, doi, arxiv_id,
            contribution_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ref_id,
            ref.application_id,
            ref.title,
            ref.authors,
            ref.year,
            ref.doi,
            ref.arxiv_id,
            ref.contribution_type,
        ],
    )
    return ref_id


def insert_industry_sector(
    conn: duckdb.DuckDBPyConnection, sector: IndustrySector
) -> int:
    """Insert an industry sector link. Returns the new ID."""
    sid = conn.execute(
        "SELECT nextval('seq_industry_sector')"
    ).fetchone()[0]
    conn.execute(
        """INSERT INTO industry_sector
           (id, application_id, sector, relevance_notes)
           VALUES (?, ?, ?, ?)
           ON CONFLICT (application_id, sector) DO UPDATE SET
               relevance_notes = EXCLUDED.relevance_notes""",
        [sid, sector.application_id, sector.sector, sector.relevance_notes],
    )
    return sid


def upsert_funding_link(
    conn: duckdb.DuckDBPyConnection, link: FundingLink
) -> int:
    """Insert or update a funding link for an application."""
    fid = conn.execute("SELECT nextval('seq_funding_link')").fetchone()[0]
    conn.execute(
        """INSERT INTO funding_link
           (id, application_id, query_pattern, grant_count,
            total_funding_eur, top_funders, last_computed)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (application_id) DO UPDATE SET
               query_pattern = EXCLUDED.query_pattern,
               grant_count = EXCLUDED.grant_count,
               total_funding_eur = EXCLUDED.total_funding_eur,
               top_funders = EXCLUDED.top_funders,
               last_computed = EXCLUDED.last_computed""",
        [
            fid,
            link.application_id,
            link.query_pattern,
            link.grant_count,
            link.total_funding_eur,
            link.top_funders,
            link.last_computed,
        ],
    )
    return fid


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_all_applications(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Return all applications as dicts."""
    rows = conn.execute(
        "SELECT * FROM application ORDER BY domain, subdomain, name"
    ).fetchall()
    cols = [d[0] for d in conn.description]
    return [dict(zip(cols, row)) for row in rows]


def get_summary_by_subdomain(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Roll-up view for non-specialist audiences."""
    rows = conn.execute("SELECT * FROM application_summary").fetchall()
    cols = [d[0] for d in conn.description]
    return [dict(zip(cols, row)) for row in rows]


def get_summary_by_domain(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Executive-level domain rollup."""
    rows = conn.execute("SELECT * FROM domain_summary").fetchall()
    cols = [d[0] for d in conn.description]
    return [dict(zip(cols, row)) for row in rows]
