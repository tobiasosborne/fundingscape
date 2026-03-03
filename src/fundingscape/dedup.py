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
import re
from pathlib import Path

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
    eu_country_fixed = _normalize_pi_country_eu(conn)
    currencies_fixed = _normalize_currency_codes(conn)
    pi_names_fixed = _normalize_pi_names(conn)
    institutions_fixed = _normalize_institutions(conn)
    funders_linked = _link_funders(conn)
    enriched = _enrich_cordis_from_openaire(conn)
    erc_pis = _enrich_cordis_erc_pis(conn)
    ec_flagged = _flag_openaire_ec_duplicates(conn)
    api_flagged = _flag_openaire_api_duplicates(conn)
    gepris_flagged = _flag_gepris_openaire_duplicates(conn)
    within_flagged = _flag_within_source_duplicates(conn)
    aggregates_flagged = _flag_aggregate_records(conn)
    funding_estimated = _estimate_gepris_funding(conn)

    stats = {
        "dates_fixed": dates_fixed,
        "countries_fixed": countries_fixed,
        "eu_country_fixed": eu_country_fixed,
        "currencies_fixed": currencies_fixed,
        "pi_names_fixed": pi_names_fixed,
        "institutions_fixed": institutions_fixed,
        "funders_linked": funders_linked,
        "enriched": enriched,
        "erc_pis_enriched": erc_pis,
        "ec_duplicates_flagged": ec_flagged,
        "api_duplicates_flagged": api_flagged,
        "gepris_duplicates_flagged": gepris_flagged,
        "within_source_flagged": within_flagged,
        "aggregates_flagged": aggregates_flagged,
        "funding_estimated": funding_estimated,
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

def _normalize_pi_country_eu(conn: duckdb.DuckDBPyConnection) -> int:
    """NULL out pi_country='EU' — not a valid ISO 3166-1 alpha-2 code.

    OpenAIRE uses 'EU' as the funder jurisdiction for EC-funded grants,
    but pi_country should represent the coordinator's country, not the funder's.
    Since we can't determine the actual coordinator country from OpenAIRE data,
    we NULL it out (CORDIS records already have the real country codes).

    Returns count of fixed records.
    """
    count = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE pi_country = 'EU'"
    ).fetchone()[0]
    if count:
        conn.execute("UPDATE grant_award SET pi_country = NULL WHERE pi_country = 'EU'")
        logger.info("Nulled pi_country='EU': %d records", count)
    return count


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

    # Link GEPRIS grants to DFG
    dfg_id = funder_map.get("DFG")
    if dfg_id:
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'gepris' AND funder_id IS NULL"
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET funder_id = ? WHERE source = 'gepris' AND funder_id IS NULL",
                [dfg_id],
            )
            total += count

    # Link Förderkatalog grants to BMBF
    bmbf_id = funder_map.get("BMBF")
    if bmbf_id:
        count = conn.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'foerderkatalog' AND funder_id IS NULL"
        ).fetchone()[0]
        if count:
            conn.execute(
                "UPDATE grant_award SET funder_id = ? WHERE source = 'foerderkatalog' AND funder_id IS NULL",
                [bmbf_id],
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


def _normalize_pi_names(conn: duckdb.DuckDBPyConnection) -> int:
    """Normalize PI names: collapse whitespace, strip titles, remove deceased markers.

    Applied to all sources but mainly affects GEPRIS which has patterns like:
      "Professor Dr. Max  Mustermann" → "Max Mustermann"
      "Professorin Dr. Anna  Fischer(†)" → "Anna Fischer"
      "Klaus  Müller, Ph.D." → "Klaus Müller"

    Returns count of fixed records.
    """
    # Count records that will change (have titles, double-spaces, or markers)
    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE pi_name IS NOT NULL AND pi_name != ''
          AND (
            pi_name LIKE '%  %'
            OR pi_name LIKE 'Professor %' OR pi_name LIKE 'Professorin %'
            OR pi_name LIKE 'Privatdozent%' OR pi_name LIKE 'Dr. %'
            OR pi_name LIKE '%(%' OR pi_name LIKE '%, Ph.D.'
          )
    """).fetchone()[0]

    # Step 1: Strip academic titles from the beginning
    # Order matters: longest prefixes first to avoid partial matches
    title_prefixes = [
        "Professorin Dr. ",
        "Professor Dr. ",
        "Privatdozentin Dr. ",
        "Privatdozent Dr. ",
        "Professorin ",
        "Professor ",
        "Privatdozentin ",
        "Privatdozent ",
        "Dr. ",
    ]
    for prefix in title_prefixes:
        conn.execute(
            "UPDATE grant_award SET pi_name = substr(pi_name, ?) "
            "WHERE pi_name IS NOT NULL AND pi_name LIKE ?",
            [len(prefix) + 1, prefix + "%"],
        )

    # Step 2: Remove ", Ph.D." suffix
    conn.execute("""
        UPDATE grant_award
        SET pi_name = regexp_replace(pi_name, ',\\s*Ph\\.D\\.\\s*$', '')
        WHERE pi_name IS NOT NULL AND pi_name LIKE '%, Ph.D.'
    """)

    # Step 3: Remove deceased markers "(†)" or "(+)"
    conn.execute("""
        UPDATE grant_award
        SET pi_name = regexp_replace(pi_name, '\\s*\\([†+]\\)\\s*$', '')
        WHERE pi_name IS NOT NULL AND pi_name LIKE '%(%'
    """)

    # Step 4: Collapse multiple whitespace to single space + trim
    conn.execute("""
        UPDATE grant_award
        SET pi_name = trim(regexp_replace(pi_name, '\\s+', ' ', 'g'))
        WHERE pi_name IS NOT NULL
          AND (pi_name LIKE '%  %' OR pi_name LIKE ' %' OR pi_name LIKE '% ')
    """)

    logger.info("Normalized %d PI names", count)
    return count


def _normalize_institutions(conn: duckdb.DuckDBPyConnection) -> int:
    """Normalize institution names across sources.

    1. CORDIS: Convert ALL-CAPS to Title Case
    2. GEPRIS: Strip "shared X and Y through:" prefix
    3. Förderkatalog: NULL out privacy placeholders
    4. All: Trim whitespace

    Returns count of fixed records.
    """
    total = 0

    # 1. CORDIS ALL-CAPS → Title Case
    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE source = 'cordis_bulk'
          AND pi_institution IS NOT NULL
          AND pi_institution = upper(pi_institution)
          AND length(pi_institution) > 1
    """).fetchone()[0]
    conn.execute("""
        UPDATE grant_award
        SET pi_institution = array_to_string(
            list_transform(
                string_split(lower(pi_institution), ' '),
                x -> concat(upper(x[1]), x[2:])
            ),
            ' '
        )
        WHERE source = 'cordis_bulk'
          AND pi_institution IS NOT NULL
          AND pi_institution = upper(pi_institution)
          AND length(pi_institution) > 1
    """)
    total += count

    # 2. GEPRIS: Strip "shared ... through:" prefix
    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE source = 'gepris'
          AND pi_institution LIKE 'shared %through:%'
    """).fetchone()[0]
    conn.execute("""
        UPDATE grant_award
        SET pi_institution = trim(substr(pi_institution,
            position(':' IN pi_institution) + 1))
        WHERE source = 'gepris'
          AND pi_institution LIKE 'shared %through:%'
    """)
    total += count

    # 3. Förderkatalog: NULL out privacy placeholders
    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE source = 'foerderkatalog'
          AND pi_institution LIKE 'Keine Anzeige%'
    """).fetchone()[0]
    conn.execute("""
        UPDATE grant_award
        SET pi_institution = NULL
        WHERE source = 'foerderkatalog'
          AND pi_institution LIKE 'Keine Anzeige%'
    """)
    total += count

    # 4. Trim whitespace on all
    conn.execute("""
        UPDATE grant_award
        SET pi_institution = trim(pi_institution)
        WHERE pi_institution IS NOT NULL
          AND (pi_institution LIKE ' %' OR pi_institution LIKE '% ')
    """)

    logger.info("Normalized %d institution names", total)
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


_ERC_PI_URL = "https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx"
_ERC_PI_CACHE = Path("data/cache/cordis/cordis-h2020-erc-pi.xlsx")


def _enrich_cordis_erc_pis(conn: duckdb.DuckDBPyConnection) -> int:
    """Enrich CORDIS records with PI names from the H2020 ERC PI dataset.

    Downloads the ERC PI XLSX from CORDIS (cached locally), parses it,
    and updates pi_name for matching CORDIS records that lack PI names.

    Returns count of enriched records.
    """
    # Download if not cached
    cache_path = _ERC_PI_CACHE
    if not cache_path.exists():
        try:
            import httpx

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            resp = httpx.get(_ERC_PI_URL, follow_redirects=True, timeout=60.0)
            resp.raise_for_status()
            cache_path.write_bytes(resp.content)
            logger.info("Downloaded ERC PI data: %d bytes", len(resp.content))
        except Exception as e:
            logger.warning("Could not download ERC PI data: %s", e)
            return 0

    # Parse XLSX
    try:
        import openpyxl

        wb = openpyxl.load_workbook(str(cache_path), read_only=True)
        ws = wb.active
    except Exception as e:
        logger.warning("Could not parse ERC PI XLSX: %s", e)
        return 0

    pi_data = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        if len(row) < 7:
            continue
        _, project_id, _acronym, _scheme, _title, first_name, last_name = row[:7]
        if project_id and first_name and last_name:
            pid = str(int(project_id))
            pi_data[pid] = f"{first_name} {last_name}"
    wb.close()

    if not pi_data:
        logger.warning("No ERC PI data parsed")
        return 0

    # Count before
    before = conn.execute(
        "SELECT COUNT(*) FROM grant_award "
        "WHERE source = 'cordis_bulk' AND pi_name IS NOT NULL AND pi_name != ''"
    ).fetchone()[0]

    # Batch update via temp table for efficiency
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS erc_pi (project_id TEXT, pi_name TEXT)")
    conn.execute("DELETE FROM erc_pi")
    conn.executemany(
        "INSERT INTO erc_pi VALUES (?, ?)",
        list(pi_data.items()),
    )
    conn.execute("""
        UPDATE grant_award AS g
        SET pi_name = e.pi_name
        FROM erc_pi AS e
        WHERE g.source = 'cordis_bulk'
          AND g.project_id = e.project_id
          AND (g.pi_name IS NULL OR g.pi_name = '')
    """)
    conn.execute("DROP TABLE IF EXISTS erc_pi")

    # Count after
    after = conn.execute(
        "SELECT COUNT(*) FROM grant_award "
        "WHERE source = 'cordis_bulk' AND pi_name IS NOT NULL AND pi_name != ''"
    ).fetchone()[0]

    count = after - before
    logger.info("Enriched %d CORDIS records with ERC PI names", count)
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


def _flag_gepris_openaire_duplicates(conn: duckdb.DuckDBPyConnection) -> int:
    """Flag OpenAIRE DFG records that duplicate GEPRIS records.

    GEPRIS is canonical for DFG since it has PI names, institutions,
    and funding amounts that OpenAIRE lacks. Matches by project_id.

    Returns count of flagged records.
    """
    # Flag openaire_bulk DFG records
    conn.execute("""
        UPDATE grant_award AS o
        SET dedup_of = (
            SELECT g.id FROM grant_award g
            WHERE g.source = 'gepris' AND g.project_id = o.project_id
            LIMIT 1
        )
        WHERE o.source = 'openaire_bulk'
          AND o.source_id LIKE 'oaire\\_DFG\\_%' ESCAPE '\\'
          AND o.project_id IS NOT NULL AND o.project_id != ''
          AND o.dedup_of IS NULL
          AND EXISTS (
              SELECT 1 FROM grant_award g
              WHERE g.source = 'gepris' AND g.project_id = o.project_id
          )
    """)

    # Flag openaire API DFG records
    conn.execute("""
        UPDATE grant_award AS o
        SET dedup_of = (
            SELECT g.id FROM grant_award g
            WHERE g.source = 'gepris' AND g.project_id = o.project_id
            LIMIT 1
        )
        WHERE o.source = 'openaire'
          AND o.source_id LIKE 'openaire\\_DFG\\_%' ESCAPE '\\'
          AND o.project_id IS NOT NULL AND o.project_id != ''
          AND o.dedup_of IS NULL
          AND EXISTS (
              SELECT 1 FROM grant_award g
              WHERE g.source = 'gepris' AND g.project_id = o.project_id
          )
    """)

    count = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE dedup_of IS NOT NULL
          AND (
              (source = 'openaire_bulk' AND source_id LIKE 'oaire\\_DFG\\_%' ESCAPE '\\')
              OR (source = 'openaire' AND source_id LIKE 'openaire\\_DFG\\_%' ESCAPE '\\')
          )
    """).fetchone()[0]
    logger.info("Flagged %d OpenAIRE DFG duplicates of GEPRIS", count)
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


# Funding threshold above which Förderkatalog records are almost certainly
# program-level aggregates rather than individual research grants.
_AGGREGATE_FUNDING_THRESHOLD = 50_000_000  # 50M EUR


def _flag_aggregate_records(conn: duckdb.DuckDBPyConnection) -> int:
    """Flag program-level aggregate records that skew funding analyses.

    Förderkatalog contains umbrella entries for entire funding programs
    (e.g. "Exzellenzinitiative", "Verwaltungsvereinbarung Bund/Länder",
    "Begabtenförderung") with billions in funding. These are not individual
    research grants and should be excluded from per-grant analyses.

    Also flags records with negative funding (correction entries).

    Returns count of flagged records.
    """
    # Reset aggregate flags first (idempotent)
    conn.execute("UPDATE grant_award SET is_aggregate = FALSE WHERE is_aggregate = TRUE")

    # Flag Förderkatalog records above threshold
    conn.execute(
        "UPDATE grant_award SET is_aggregate = TRUE "
        "WHERE source = 'foerderkatalog' AND total_funding > ?",
        [_AGGREGATE_FUNDING_THRESHOLD],
    )

    # Flag negative funding records (correction entries)
    conn.execute(
        "UPDATE grant_award SET is_aggregate = TRUE "
        "WHERE total_funding < 0"
    )

    count = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE is_aggregate = TRUE"
    ).fetchone()[0]
    logger.info("Flagged %d aggregate/correction records", count)
    return count


# DFG programme type → typical annual funding in EUR.
# Sources: DFG funding guidelines, annual reports, typical grant sizes.
# These are rough averages — individual grants vary widely.
_DFG_PROGRAMME_FUNDING_PER_YEAR = {
    "Research Grants": 80_000,             # Sachbeihilfe: ~200-400K over 3 years
    "Research Fellowships": 70_000,        # ~140K over 2 years
    "Fellowship": 70_000,                  # Same as above
    "Emmy Noether": 250_000,              # ~1.5M over 6 years
    "Heisenberg": 150_000,                # Professorship funding ~150K/yr
    "Priority Programmes": 100_000,        # SPP subprojects: ~200-400K over 3 years
    "Research Units": 100_000,             # FOR subprojects: similar to SPP
    "Collaborative Research Centres": 200_000,  # SFB subprojects: ~800K over 4 years
    "CRC/Transregios": 200_000,           # Same as SFB
    "Research Training Groups": 300_000,   # GRK: ~3-5M over 4.5 years, shared
    "Clinical Research Units": 150_000,    # KFO subprojects
    "Clusters of Excellence": 1_500_000,   # EXC: ~5-40M over 7 years (highly variable)
    "International Research Training Groups": 300_000,  # IRTG
    "Major Research Instrumentation": 300_000,  # Großgeräte: one-time, avg ~300K
    "DFG Research Centres": 500_000,       # Forschungszentren: ~5M/yr
    "Graduate Schools": 500_000,           # GSC: ~5M/yr
    "Publication Grants": 10_000,          # Small, one-time
    "Scientific Networks": 15_000,         # ~60K over 4 years
    "Infrastructure Priority Programmes": 100_000,  # Similar to SPP
    "DIP Programme": 100_000,              # German-Israeli projects
    "Advanced Studies Centres in SSH": 100_000,  # Kolleg-Forschungsgruppe
}

# Default for unrecognized programme types
_DFG_DEFAULT_FUNDING_PER_YEAR = 80_000  # Conservative estimate


def _extract_programme_type(abstract: str | None) -> str | None:
    """Extract DFG programme type from GEPRIS abstract text."""
    if not abstract:
        return None
    m = re.search(
        r"DFG Programme\s*(.+?)(?:Subject Area|Subproject of|Participating|"
        r"Term |International Connection|Applicant|Spokesperson|Major|"
        r"Project Identifier|Co-Applicant|Further|Instrumentation Group)",
        abstract,
    )
    if m:
        prog = m.group(1).strip()
        # Clean up compound entries like "Research GrantsParticipating..."
        prog = re.sub(r"(Grants|Programmes|Centres|Units|Groups|Schools|Fellowships).*", r"\1", prog)
        return prog
    return None


def _estimate_gepris_funding(conn: duckdb.DuckDBPyConnection) -> int:
    """Estimate funding for GEPRIS records that have no reported total_funding.

    Uses DFG programme type (extracted from abstract) and grant duration to
    calculate: estimated_amount = annual_rate * duration_years.

    Only fills total_funding_estimated — never overwrites total_funding.
    Sets funding_estimate_method to 'programme_type' for traceability.

    Returns count of estimated records.
    """
    # Clear previous estimates (idempotent)
    conn.execute("""
        UPDATE grant_award
        SET total_funding_estimated = NULL, funding_estimate_method = NULL
        WHERE source = 'gepris' AND funding_estimate_method IS NOT NULL
    """)

    # Fetch GEPRIS records with no funding but with abstract and dates
    rows = conn.execute("""
        SELECT id, abstract, start_date, end_date
        FROM grant_award
        WHERE source = 'gepris'
          AND (total_funding IS NULL OR total_funding = 0)
          AND abstract IS NOT NULL
    """).fetchall()

    updates = []
    for row_id, abstract, start_date, end_date in rows:
        prog = _extract_programme_type(abstract)
        if not prog:
            continue

        annual_rate = _DFG_PROGRAMME_FUNDING_PER_YEAR.get(
            prog, _DFG_DEFAULT_FUNDING_PER_YEAR
        )

        if start_date and end_date:
            duration = (end_date - start_date).days / 365.25
            duration = max(duration, 1.0)  # At least 1 year
        else:
            duration = 3.0  # Default assumption: 3-year grant

        estimated = annual_rate * duration
        updates.append((estimated, row_id))

    # Batch update
    if updates:
        conn.executemany(
            "UPDATE grant_award "
            "SET total_funding_estimated = ?, funding_estimate_method = 'programme_type' "
            "WHERE id = ?",
            updates,
        )

    logger.info("Estimated funding for %d GEPRIS records", len(updates))
    return len(updates)
