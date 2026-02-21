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

    dates_fixed = _clean_date_anomalies(conn)
    countries_fixed = _normalize_country_codes(conn)
    currencies_fixed = _normalize_currency_codes(conn)
    funders_linked = _link_funders(conn)
    enriched = _enrich_cordis_from_openaire(conn)
    ec_flagged = _flag_openaire_ec_duplicates(conn)
    api_flagged = _flag_openaire_api_duplicates(conn)
    within_flagged = _flag_within_source_duplicates(conn)

    stats = {
        "dates_fixed": dates_fixed,
        "countries_fixed": countries_fixed,
        "currencies_fixed": currencies_fixed,
        "funders_linked": funders_linked,
        "enriched": enriched,
        "ec_duplicates_flagged": ec_flagged,
        "api_duplicates_flagged": api_flagged,
        "within_source_flagged": within_flagged,
    }
    logger.info("Dedup complete: %s", stats)
    return stats


def _clean_date_anomalies(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Fix date anomalies in grant_award.

    1. NULL out sentinel start dates (1900-01-01)
    2. NULL out sentinel end dates (9999-12-31)
    3. NULL out implausible ancient start dates (before 1950)
    4. NULL out implausible far-future end dates (after 2040)
    5. Swap start/end dates where start > end

    Returns dict with counts per fix type.
    """
    counts = {}

    # 1. Swap start > end dates first (before cleaning, so swapped values
    #    get caught by the subsequent sentinel/range checks)
    counts["swapped"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award "
        "WHERE start_date IS NOT NULL AND end_date IS NOT NULL AND start_date > end_date"
    ).fetchone()[0]
    conn.execute("""
        UPDATE grant_award
        SET start_date = end_date, end_date = start_date
        WHERE start_date IS NOT NULL AND end_date IS NOT NULL
          AND start_date > end_date
    """)

    # 2. NULL out sentinel and implausible dates
    counts["sentinel_start"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE start_date = '1900-01-01'"
    ).fetchone()[0]
    conn.execute("UPDATE grant_award SET start_date = NULL WHERE start_date = '1900-01-01'")

    counts["sentinel_end"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE end_date = '9999-12-31'"
    ).fetchone()[0]
    conn.execute("UPDATE grant_award SET end_date = NULL WHERE end_date = '9999-12-31'")

    counts["ancient_start"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE start_date IS NOT NULL AND YEAR(start_date) < 1950"
    ).fetchone()[0]
    conn.execute(
        "UPDATE grant_award SET start_date = NULL "
        "WHERE start_date IS NOT NULL AND YEAR(start_date) < 1950"
    )

    counts["ancient_end"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE end_date IS NOT NULL AND YEAR(end_date) < 1950"
    ).fetchone()[0]
    conn.execute(
        "UPDATE grant_award SET end_date = NULL "
        "WHERE end_date IS NOT NULL AND YEAR(end_date) < 1950"
    )

    counts["future_end"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE end_date IS NOT NULL AND YEAR(end_date) > 2040"
    ).fetchone()[0]
    conn.execute(
        "UPDATE grant_award SET end_date = NULL "
        "WHERE end_date IS NOT NULL AND YEAR(end_date) > 2040"
    )

    total = sum(counts.values())
    logger.info("Date cleanup: %d fixes (%s)", total, counts)
    return counts


# Non-standard country codes → ISO 3166-1 alpha-2
_COUNTRY_CODE_MAP = {
    "UK": "GB",  # CORDIS uses UK, ISO uses GB
    "EL": "GR",  # EU convention for Greece, ISO uses GR
}

# Non-standard currency codes → ISO 4217
_CURRENCY_CODE_MAP = {
    "$": "AUD",  # NHMRC (Australia) uses "$" instead of "AUD"
}


def _normalize_country_codes(conn: duckdb.DuckDBPyConnection) -> int:
    """Normalize non-standard country codes to ISO 3166-1 alpha-2.

    Returns count of fixed records.
    """
    total = 0
    for old_code, new_code in _COUNTRY_CODE_MAP.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award WHERE pi_country = ?", [old_code]
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET pi_country = ? WHERE pi_country = ?",
                [new_code, old_code],
            )
            logger.info("Country code %s → %s: %d records", old_code, new_code, count)
            total += count
    return total


# OpenAIRE funder codes → (full name, country, type)
_OPENAIRE_FUNDERS = {
    "NIH": ("National Institutes of Health", "US", "foreign_gov"),
    "NSF": ("National Science Foundation", "US", "foreign_gov"),
    "UKRI": ("UK Research and Innovation", "GB", "foreign_gov"),
    "SNSF": ("Swiss National Science Foundation", "CH", "foreign_gov"),
    "FCT": ("Fundação para a Ciência e a Tecnologia", "PT", "foreign_gov"),
    "NWO": ("Dutch Research Council", "NL", "foreign_gov"),
    "NHMRC": ("National Health and Medical Research Council", "AU", "foreign_gov"),
    "ARC": ("Australian Research Council", "AU", "foreign_gov"),
    "AKA": ("Academy of Finland", "FI", "foreign_gov"),
    "ANR": ("Agence Nationale de la Recherche", "FR", "foreign_gov"),
    "RCN": ("Research Council of Norway", "NO", "foreign_gov"),
    "WT": ("Wellcome Trust", "GB", "foundation"),
    "FWF": ("Austrian Science Fund", "AT", "foreign_gov"),
    "TUBITAK": ("Scientific and Technological Research Council of Turkey", "TR", "foreign_gov"),
    "SFI": ("Science Foundation Ireland", "IE", "foreign_gov"),
    "IRFD": ("Independent Research Fund Denmark", "DK", "foreign_gov"),
    "NNF": ("Novo Nordisk Foundation", "DK", "foundation"),
    "MZOS": ("Ministry of Science and Education (Croatia)", "HR", "foreign_gov"),
    "HRZZ": ("Croatian Science Foundation", "HR", "foreign_gov"),
    "INCa": ("Institut National du Cancer", "FR", "foreign_gov"),
    "MESTD": ("Ministry of Education, Science and Tech. Dev. (Serbia)", "RS", "foreign_gov"),
}


def _link_funders(conn: duckdb.DuckDBPyConnection) -> int:
    """Link grants to funders via funder_id column.

    1. Ensures all known OpenAIRE funder codes exist in the funder table
    2. Sets funder_id for CORDIS grants → EC (id=1)
    3. Sets funder_id for OpenAIRE grants by extracting funder code from source_id

    Returns count of grants linked.
    """
    # Ensure OpenAIRE funders exist in funder table
    for code, (name, country, ftype) in _OPENAIRE_FUNDERS.items():
        existing = conn.execute(
            "SELECT id FROM funder WHERE short_name = ?", [code]
        ).fetchone()
        if not existing:
            fid = conn.execute("SELECT nextval('seq_funder')").fetchone()[0]
            conn.execute(
                "INSERT INTO funder (id, name, short_name, country, type) VALUES (?, ?, ?, ?, ?)",
                [fid, name, code, country, ftype],
            )

    # Build funder short_name → id mapping
    funder_map = {
        r[0]: r[1]
        for r in conn.execute("SELECT short_name, id FROM funder WHERE short_name IS NOT NULL").fetchall()
    }

    total = 0

    # Link CORDIS grants to EC
    ec_id = funder_map.get("EC")
    if ec_id:
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'cordis_bulk' AND funder_id IS NULL"
        ).fetchone()[0]
        conn.execute(
            "UPDATE grant_award SET funder_id = ? WHERE source = 'cordis_bulk' AND funder_id IS NULL",
            [ec_id],
        )
        total += count

    # Link OpenAIRE bulk grants by funder code in source_id
    for code, fid in funder_map.items():
        prefix = f"oaire_{code}_"
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award "
            "WHERE source = 'openaire_bulk' AND funder_id IS NULL AND source_id LIKE ?",
            [prefix + "%"],
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET funder_id = ? "
                "WHERE source = 'openaire_bulk' AND funder_id IS NULL AND source_id LIKE ?",
                [fid, prefix + "%"],
            )
            total += count

    # Link OpenAIRE API grants by funder code in source_id
    for code, fid in funder_map.items():
        prefix = f"openaire_{code}_"
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award "
            "WHERE source = 'openaire' AND funder_id IS NULL AND source_id LIKE ?",
            [prefix + "%"],
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET funder_id = ? "
                "WHERE source = 'openaire' AND funder_id IS NULL AND source_id LIKE ?",
                [fid, prefix + "%"],
            )
            total += count

    logger.info("Linked %d grants to funders", total)
    return total


def _normalize_currency_codes(conn: duckdb.DuckDBPyConnection) -> int:
    """Normalize non-standard currency codes to ISO 4217.

    Returns count of fixed records.
    """
    total = 0
    for old_code, new_code in _CURRENCY_CODE_MAP.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award WHERE currency = ?", [old_code]
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET currency = ? WHERE currency = ?",
                [new_code, old_code],
            )
            logger.info("Currency code %s → %s: %d records", old_code, new_code, count)
            total += count
    return total


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


def _flag_within_source_duplicates(conn: duckdb.DuckDBPyConnection) -> int:
    """Flag within-source duplicates (same source + source_id).

    Keeps the row with the lowest id as canonical.
    Note: project_id collisions across funders (e.g. NHMRC #222910 vs
    RCN #222910) are NOT duplicates — source_id includes the funder prefix.

    Returns count of flagged records.
    """
    conn.execute("""
        UPDATE grant_award AS dup
        SET dedup_of = (
            SELECT MIN(g2.id) FROM grant_award g2
            WHERE g2.source = dup.source
              AND g2.source_id = dup.source_id
        )
        WHERE dup.dedup_of IS NULL
          AND dup.id != (
              SELECT MIN(g3.id) FROM grant_award g3
              WHERE g3.source = dup.source
                AND g3.source_id = dup.source_id
          )
          AND EXISTS (
              SELECT 1 FROM grant_award g4
              WHERE g4.source = dup.source
                AND g4.source_id = dup.source_id
                AND g4.id != dup.id
          )
    """)

    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE dedup_of IS NOT NULL
          AND source = (
              SELECT source FROM grant_award g2 WHERE g2.id = grant_award.dedup_of
          )
          AND source_id = (
              SELECT source_id FROM grant_award g2 WHERE g2.id = grant_award.dedup_of
          )
    """).fetchone()[0]
    logger.info("Flagged %d within-source duplicates", count)
    return count
