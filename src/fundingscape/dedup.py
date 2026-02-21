"""Cross-source deduplication for grant_award records.

Identifies and flags duplicate grants across CORDIS, OpenAIRE API,
and OpenAIRE bulk data sources. Uses soft dedup via a `dedup_of` column
(no rows are deleted).

Canonical priority:
  1. CORDIS (cordis_bulk) — richest metadata for EC grants
  2. OpenAIRE Bulk (openaire_bulk) — complete coverage
  3. OpenAIRE API (openaire) — keyword-filtered subset
"""

from __future__ import annotations

import logging

import duckdb

logger = logging.getLogger(__name__)


def run_dedup(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Run all deduplication steps. Idempotent — clears flags first.

    Returns dict with counts of enriched and flagged records.
    """
    # Clear all existing dedup flags so we can re-apply cleanly
    conn.execute("UPDATE grant_award SET dedup_of = NULL WHERE dedup_of IS NOT NULL")

    enriched = _enrich_cordis_from_openaire(conn)
    ec_flagged = _flag_openaire_ec_duplicates(conn)
    api_flagged = _flag_openaire_api_duplicates(conn)

    stats = {
        "enriched": enriched,
        "ec_duplicates_flagged": ec_flagged,
        "api_duplicates_flagged": api_flagged,
    }
    logger.info("Dedup complete: %s", stats)
    return stats


def _enrich_cordis_from_openaire(conn: duckdb.DuckDBPyConnection) -> int:
    """Copy total_funding and pi_country from OpenAIRE → CORDIS where CORDIS is NULL.

    Uses COALESCE semantics: never overwrites existing CORDIS values.
    Returns count of enriched records.
    """
    # Count how many CORDIS records have gaps that OpenAIRE can fill
    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award c
        WHERE c.source = 'cordis_bulk'
          AND c.project_id IS NOT NULL
          AND c.project_id != ''
          AND (c.total_funding IS NULL OR c.pi_country IS NULL)
          AND EXISTS (
              SELECT 1 FROM grant_award o
              WHERE o.source = 'openaire_bulk'
                AND o.project_id = c.project_id
          )
    """).fetchone()[0]

    conn.execute("""
        UPDATE grant_award AS c
        SET
            total_funding = COALESCE(c.total_funding, o.total_funding),
            pi_country = COALESCE(c.pi_country, o.pi_country),
            updated_at = CURRENT_TIMESTAMP
        FROM grant_award AS o
        WHERE c.source = 'cordis_bulk'
          AND o.source = 'openaire_bulk'
          AND c.project_id IS NOT NULL
          AND c.project_id != ''
          AND c.project_id = o.project_id
          AND (c.total_funding IS NULL OR c.pi_country IS NULL)
    """)
    logger.info("Enriched %d CORDIS records from OpenAIRE", count)
    return count


def _flag_openaire_ec_duplicates(conn: duckdb.DuckDBPyConnection) -> int:
    """Flag OpenAIRE EC-funded records that duplicate CORDIS records.

    Matches by project_id. Sets dedup_of = CORDIS record id.
    Only flags records from openaire_bulk where the source_id starts
    with 'oaire_EC_' (EC-funded), plus openaire API records starting
    with 'openaire_EC_'.

    Returns count of flagged records.
    """
    # Flag openaire_bulk EC records that match CORDIS by project_id
    conn.execute("""
        UPDATE grant_award AS o
        SET dedup_of = (
            SELECT c.id FROM grant_award c
            WHERE c.source = 'cordis_bulk'
              AND c.project_id = o.project_id
            LIMIT 1
        )
        WHERE o.source = 'openaire_bulk'
          AND o.source_id LIKE 'oaire\\_EC\\_%' ESCAPE '\\'
          AND o.project_id IS NOT NULL
          AND o.project_id != ''
          AND EXISTS (
              SELECT 1 FROM grant_award c
              WHERE c.source = 'cordis_bulk'
                AND c.project_id = o.project_id
          )
    """)

    # Flag openaire API EC records that match CORDIS by project_id
    conn.execute("""
        UPDATE grant_award AS o
        SET dedup_of = (
            SELECT c.id FROM grant_award c
            WHERE c.source = 'cordis_bulk'
              AND c.project_id = o.project_id
            LIMIT 1
        )
        WHERE o.source = 'openaire'
          AND o.source_id LIKE 'openaire\\_EC\\_%' ESCAPE '\\'
          AND o.project_id IS NOT NULL
          AND o.project_id != ''
          AND EXISTS (
              SELECT 1 FROM grant_award c
              WHERE c.source = 'cordis_bulk'
                AND c.project_id = o.project_id
          )
    """)

    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE dedup_of IS NOT NULL
          AND source IN ('openaire_bulk', 'openaire')
          AND (source_id LIKE 'oaire\\_EC\\_%' ESCAPE '\\'
               OR source_id LIKE 'openaire\\_EC\\_%' ESCAPE '\\')
    """).fetchone()[0]
    logger.info("Flagged %d OpenAIRE EC duplicates of CORDIS", count)
    return count


def _flag_openaire_api_duplicates(conn: duckdb.DuckDBPyConnection) -> int:
    """Flag OpenAIRE API records that duplicate OpenAIRE bulk records.

    Matches by project_id. Sets dedup_of = bulk record id.
    Only flags non-EC API records (EC ones already flagged above).

    Returns count of flagged records.
    """
    conn.execute("""
        UPDATE grant_award AS api
        SET dedup_of = (
            SELECT b.id FROM grant_award b
            WHERE b.source = 'openaire_bulk'
              AND b.project_id = api.project_id
              AND b.dedup_of IS NULL
            LIMIT 1
        )
        WHERE api.source = 'openaire'
          AND api.dedup_of IS NULL
          AND api.project_id IS NOT NULL
          AND api.project_id != ''
          AND EXISTS (
              SELECT 1 FROM grant_award b
              WHERE b.source = 'openaire_bulk'
                AND b.project_id = api.project_id
                AND b.dedup_of IS NULL
          )
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM grant_award "
        "WHERE source = 'openaire' AND dedup_of IS NOT NULL"
    ).fetchone()[0]
    logger.info("Flagged %d OpenAIRE API duplicates of bulk", count)
    return count
