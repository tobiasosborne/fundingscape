"""DuckDB database layer for the funding landscape system."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import duckdb

from fundingscape import DB_PATH
from fundingscape.models import Call, Funder, FundingInstrument, GrantAward


def get_connection(path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection. Creates directory if needed."""
    path = path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    create_tables(conn)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funder (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            short_name TEXT,
            country TEXT,
            type TEXT NOT NULL,
            website TEXT,
            contact TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_funder START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_instrument (
            id INTEGER PRIMARY KEY,
            funder_id INTEGER,
            name TEXT NOT NULL,
            short_name TEXT,
            description TEXT,
            url TEXT,
            eligibility_criteria TEXT,
            typical_duration_months INTEGER,
            typical_amount_min DOUBLE,
            typical_amount_max DOUBLE,
            currency TEXT DEFAULT 'EUR',
            success_rate DOUBLE,
            recurrence TEXT,
            next_deadline DATE,
            deadline_type TEXT,
            relevance_tags TEXT[],
            sme_eligible BOOLEAN DEFAULT FALSE,
            source TEXT NOT NULL,
            source_id TEXT,
            raw_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_instrument START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS call (
            id INTEGER PRIMARY KEY,
            instrument_id INTEGER,
            call_identifier TEXT,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT,
            opening_date DATE,
            deadline DATE,
            deadline_timezone TEXT DEFAULT 'Europe/Brussels',
            status TEXT NOT NULL,
            budget_total DOUBLE,
            currency TEXT DEFAULT 'EUR',
            expected_grants INTEGER,
            topic_keywords TEXT[],
            framework_programme TEXT,
            programme_division TEXT,
            source TEXT NOT NULL,
            source_id TEXT,
            raw_data JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_call START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS grant_award (
            id INTEGER PRIMARY KEY,
            instrument_id INTEGER,
            call_id INTEGER,
            project_title TEXT NOT NULL,
            project_id TEXT,
            acronym TEXT,
            abstract TEXT,
            pi_name TEXT,
            pi_institution TEXT,
            pi_country TEXT,
            start_date DATE,
            end_date DATE,
            total_funding DOUBLE,
            eu_contribution DOUBLE,
            currency TEXT DEFAULT 'EUR',
            status TEXT,
            partners JSON,
            topic_keywords TEXT[],
            source TEXT NOT NULL,
            source_id TEXT,
            dedup_of INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migration: add dedup_of column if missing (existing databases)
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='grant_award' AND table_schema='main'"
        ).fetchall()
    }
    if "dedup_of" not in cols:
        conn.execute("ALTER TABLE grant_award ADD COLUMN dedup_of INTEGER")

    # Indexes for dedup matching
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_grant_project_id
        ON grant_award (project_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_grant_dedup_of
        ON grant_award (dedup_of)
    """)

    # Deduplicated view: only canonical records
    conn.execute("CREATE OR REPLACE VIEW grant_award_deduped AS SELECT * FROM grant_award WHERE dedup_of IS NULL")

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_grant START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS eligibility_profile (
            id INTEGER PRIMARY KEY,
            profile_name TEXT NOT NULL,
            pi_career_stage TEXT,
            years_since_phd INTEGER,
            nationality TEXT,
            institution TEXT,
            institution_country TEXT,
            orcid TEXT,
            research_keywords TEXT[],
            is_sme BOOLEAN DEFAULT FALSE,
            company_name TEXT,
            company_country TEXT,
            notes TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_source (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            last_fetch TIMESTAMP,
            last_success TIMESTAMP,
            records_fetched INTEGER DEFAULT 0,
            etag TEXT,
            last_modified TEXT,
            status TEXT DEFAULT 'never_fetched',
            error_message TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS change_log (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_grant'),
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            change_type TEXT NOT NULL,
            field_changed TEXT,
            old_value TEXT,
            new_value TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def insert_funder(conn: duckdb.DuckDBPyConnection, funder: Funder) -> int:
    """Insert a funder and return its ID."""
    funder_id = conn.execute("SELECT nextval('seq_funder')").fetchone()[0]
    conn.execute(
        """INSERT INTO funder (id, name, short_name, country, type, website, contact)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [funder_id, funder.name, funder.short_name, funder.country,
         funder.type, funder.website, funder.contact],
    )
    return funder_id


def insert_call(conn: duckdb.DuckDBPyConnection, call: Call) -> int:
    """Insert a call and return its ID."""
    call_id = conn.execute("SELECT nextval('seq_call')").fetchone()[0]
    conn.execute(
        """INSERT INTO call (id, instrument_id, call_identifier, title, description,
           url, opening_date, deadline, deadline_timezone, status, budget_total,
           currency, expected_grants, topic_keywords, framework_programme,
           programme_division, source, source_id, raw_data)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [call_id, call.instrument_id, call.call_identifier, call.title,
         call.description, call.url, call.opening_date, call.deadline,
         call.deadline_timezone, call.status,
         float(call.budget_total) if call.budget_total else None,
         call.currency, call.expected_grants, call.topic_keywords,
         call.framework_programme, call.programme_division,
         call.source, call.source_id,
         json.dumps(call.raw_data) if call.raw_data else None],
    )
    return call_id


def insert_grant(conn: duckdb.DuckDBPyConnection, grant: GrantAward) -> int:
    """Insert a grant and return its ID."""
    grant_id = conn.execute("SELECT nextval('seq_grant')").fetchone()[0]
    conn.execute(
        """INSERT INTO grant_award (id, instrument_id, call_id, project_title,
           project_id, acronym, abstract, pi_name, pi_institution, pi_country,
           start_date, end_date, total_funding, eu_contribution, currency,
           status, partners, topic_keywords, source, source_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [grant_id, grant.instrument_id, grant.call_id, grant.project_title,
         grant.project_id, grant.acronym, grant.abstract, grant.pi_name,
         grant.pi_institution, grant.pi_country, grant.start_date,
         grant.end_date,
         float(grant.total_funding) if grant.total_funding else None,
         float(grant.eu_contribution) if grant.eu_contribution else None,
         grant.currency, grant.status,
         json.dumps(grant.partners) if grant.partners else None,
         grant.topic_keywords, grant.source, grant.source_id],
    )
    return grant_id


def upsert_grant(conn: duckdb.DuckDBPyConnection, grant: GrantAward) -> int:
    """Insert or update a grant by source + source_id. Returns the ID."""
    existing = conn.execute(
        "SELECT id FROM grant_award WHERE source = ? AND source_id = ?",
        [grant.source, grant.source_id],
    ).fetchone()
    if existing:
        gid = existing[0]
        conn.execute(
            """UPDATE grant_award SET project_title=?, project_id=?, acronym=?,
               abstract=?, pi_name=?, pi_institution=?, pi_country=?,
               start_date=?, end_date=?, total_funding=?, eu_contribution=?,
               status=?, partners=?, topic_keywords=?, updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            [grant.project_title, grant.project_id, grant.acronym,
             grant.abstract, grant.pi_name, grant.pi_institution,
             grant.pi_country, grant.start_date, grant.end_date,
             float(grant.total_funding) if grant.total_funding else None,
             float(grant.eu_contribution) if grant.eu_contribution else None,
             grant.status,
             json.dumps(grant.partners) if grant.partners else None,
             grant.topic_keywords, gid],
        )
        return gid
    return insert_grant(conn, grant)


def upsert_call(conn: duckdb.DuckDBPyConnection, call: Call) -> int:
    """Insert or update a call by source + source_id. Returns the ID."""
    existing = conn.execute(
        "SELECT id FROM call WHERE source = ? AND source_id = ?",
        [call.source, call.source_id],
    ).fetchone()
    if existing:
        cid = existing[0]
        conn.execute(
            """UPDATE call SET title=?, description=?, deadline=?, status=?,
               budget_total=?, topic_keywords=?, updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            [call.title, call.description, call.deadline, call.status,
             float(call.budget_total) if call.budget_total else None,
             call.topic_keywords, cid],
        )
        return cid
    return insert_call(conn, call)


def update_data_source(
    conn: duckdb.DuckDBPyConnection,
    source_id: str,
    name: str,
    records: int,
    status: str = "ok",
    error: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
) -> None:
    """Update data source tracking record."""
    now = datetime.now(UTC)
    conn.execute(
        """INSERT INTO data_source (id, name, last_fetch, last_success,
           records_fetched, etag, last_modified, status, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (id) DO UPDATE SET
               last_fetch = EXCLUDED.last_fetch,
               last_success = CASE WHEN EXCLUDED.status = 'ok'
                   THEN EXCLUDED.last_fetch ELSE data_source.last_success END,
               records_fetched = EXCLUDED.records_fetched,
               etag = EXCLUDED.etag,
               last_modified = EXCLUDED.last_modified,
               status = EXCLUDED.status,
               error_message = EXCLUDED.error_message""",
        [source_id, name, now,
         now if status == "ok" else None,
         records, etag, last_modified, status, error],
    )


def init_db(path: str | None = None) -> None:
    """Initialize the database with schema and seed data."""
    conn = get_connection(path)
    _seed_funders(conn)
    _seed_profiles(conn)
    conn.close()


def _seed_funders(conn: duckdb.DuckDBPyConnection) -> None:
    """Seed the funders table with known funding bodies."""
    existing = conn.execute("SELECT COUNT(*) FROM funder").fetchone()[0]
    if existing > 0:
        return

    funders = [
        Funder(name="European Commission", short_name="EC", country="EU", type="eu",
               website="https://ec.europa.eu"),
        Funder(name="European Research Council", short_name="ERC", country="EU", type="eu",
               website="https://erc.europa.eu"),
        Funder(name="Deutsche Forschungsgemeinschaft", short_name="DFG", country="DE",
               type="federal_de", website="https://www.dfg.de"),
        Funder(name="Bundesministerium für Bildung und Forschung", short_name="BMBF",
               country="DE", type="federal_de", website="https://www.bmbf.de"),
        Funder(name="VolkswagenStiftung", short_name="VWS", country="DE",
               type="foundation", website="https://www.volkswagenstiftung.de"),
        Funder(name="Alexander von Humboldt Stiftung", short_name="AvH", country="DE",
               type="foundation", website="https://www.humboldt-foundation.de"),
        Funder(name="Air Force Office of Scientific Research", short_name="AFOSR",
               country="US", type="foreign_gov", website="https://www.afrl.af.mil/AFOSR/"),
        Funder(name="Office of Naval Research", short_name="ONR", country="US",
               type="foreign_gov", website="https://www.onr.navy.mil"),
        Funder(name="Army Research Laboratory", short_name="ARL", country="US",
               type="foreign_gov", website="https://arl.devcom.army.mil"),
        Funder(name="NATO Science for Peace and Security", short_name="NATO SPS",
               country="INT", type="foreign_gov",
               website="https://www.nato.int/cps/en/natohq/topics_85373.htm"),
        Funder(name="Carl-Zeiss-Stiftung", short_name="CZS", country="DE",
               type="foundation", website="https://www.carl-zeiss-stiftung.de"),
        Funder(name="Fritz Thyssen Stiftung", short_name="Thyssen", country="DE",
               type="foundation", website="https://www.fritz-thyssen-stiftung.de"),
        Funder(name="DAAD", short_name="DAAD", country="DE", type="federal_de",
               website="https://www.daad.de"),
        Funder(name="MWK Niedersachsen", short_name="MWK-NDS", country="DE",
               type="state_de"),
    ]
    for f in funders:
        insert_funder(conn, f)


def _seed_profiles(conn: duckdb.DuckDBPyConnection) -> None:
    """Seed eligibility profiles."""
    existing = conn.execute("SELECT COUNT(*) FROM eligibility_profile").fetchone()[0]
    if existing > 0:
        return

    conn.execute(
        """INSERT INTO eligibility_profile
           (id, profile_name, pi_career_stage, institution, institution_country,
            research_keywords, is_sme, company_name, company_country)
           VALUES
           (1, 'PI - Quantum Research Group', 'senior',
            'Leibniz Universität Hannover', 'DE',
            ['quantum_computing', 'many_body_quantum', 'topological_quantum',
             'fusion_categories', 'quantum_boolean_functions', 'formal_verification',
             'lean4', 'mobile_anyons'],
            FALSE, NULL, NULL),
           (2, 'Innovailia UG', NULL,
            'Leibniz Universität Hannover', 'DE',
            ['quantum_computing', 'deep_tech'],
            TRUE, 'Innovailia UG', 'DE')""",
    )
