"""Analytical queries for the funding landscape system."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb


def open_calls_by_deadline(
    conn: duckdb.DuckDBPyConnection,
    months_ahead: int = 6,
    quantum_only: bool = True,
) -> list[dict]:
    """Get open/forthcoming calls ranked by deadline.

    Returns list of dicts with call details + relevance info.
    """
    cutoff = date.today() + timedelta(days=months_ahead * 30)
    quantum_filter = """
        AND (
            title ILIKE '%quantum%'
            OR title ILIKE '%ERC%'
            OR title ILIKE '%topolog%'
            OR title ILIKE '%many-body%'
            OR title ILIKE '%entangle%'
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%quantum%'
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%physics%'
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%computing%'
            OR call_identifier ILIKE '%ERC%'
            OR call_identifier ILIKE '%MSCA%'
            OR call_identifier ILIKE '%quantum%'
        )
    """ if quantum_only else ""

    rows = conn.execute(f"""
        SELECT
            call_identifier,
            title,
            deadline,
            status,
            budget_total,
            currency,
            framework_programme,
            source,
            url,
            topic_keywords
        FROM call
        WHERE status IN ('open', 'forthcoming')
        AND (deadline >= CURRENT_DATE OR deadline IS NULL)
        AND (deadline <= ? OR deadline IS NULL)
        {quantum_filter}
        ORDER BY deadline ASC NULLS LAST
    """, [cutoff]).fetchall()

    return [
        {
            "identifier": r[0],
            "title": r[1],
            "deadline": r[2],
            "status": r[3],
            "budget": r[4],
            "currency": r[5],
            "programme": r[6],
            "source": r[7],
            "url": r[8],
            "keywords": r[9],
        }
        for r in rows
    ]


def funding_landscape_summary(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Total active funding by source/programme."""
    rows = conn.execute("""
        SELECT
            source,
            COUNT(*) as num_grants,
            SUM(total_funding) as total_funding,
            SUM(eu_contribution) as total_eu,
            MIN(start_date) as earliest,
            MAX(end_date) as latest
        FROM grant_award
        WHERE status = 'active'
        GROUP BY source
        ORDER BY total_funding DESC NULLS LAST
    """).fetchall()

    return [
        {
            "source": r[0],
            "num_grants": r[1],
            "total_funding": r[2],
            "total_eu": r[3],
            "earliest": r[4],
            "latest": r[5],
        }
        for r in rows
    ]


def income_projection(
    conn: duckdb.DuckDBPyConnection,
    institution_pattern: str = "%HANNOVER%",
) -> list[dict]:
    """Project future income from active grants by year.

    Assumes linear burn rate across grant duration.
    """
    rows = conn.execute("""
        WITH grant_years AS (
            SELECT
                project_title,
                acronym,
                total_funding,
                start_date,
                end_date,
                DATEDIFF('month', start_date, end_date) as duration_months,
                total_funding / NULLIF(DATEDIFF('month', start_date, end_date), 0) as monthly_rate
            FROM grant_award
            WHERE pi_institution ILIKE ?
            AND status = 'active'
            AND start_date IS NOT NULL
            AND end_date IS NOT NULL
            AND total_funding > 0
        ),
        years AS (
            SELECT UNNEST(GENERATE_SERIES(2024, 2032)) as year
        )
        SELECT
            y.year,
            COUNT(DISTINCT g.acronym) as active_grants,
            SUM(
                g.monthly_rate * LEAST(12,
                    GREATEST(0,
                        DATEDIFF('month',
                            GREATEST(g.start_date, MAKE_DATE(y.year, 1, 1)),
                            LEAST(g.end_date, MAKE_DATE(y.year, 12, 31))
                        )
                    )
                )
            ) as projected_income
        FROM years y
        CROSS JOIN grant_years g
        WHERE y.year >= YEAR(g.start_date)
        AND y.year <= YEAR(g.end_date)
        GROUP BY y.year
        ORDER BY y.year
    """, [institution_pattern]).fetchall()

    return [
        {
            "year": r[0],
            "active_grants": r[1],
            "projected_income": r[2],
        }
        for r in rows
    ]


def top_pis_by_field(
    conn: duckdb.DuckDBPyConnection,
    field_keyword: str = "quantum",
    limit: int = 20,
) -> list[dict]:
    """Find top PIs in a field by total grant funding."""
    rows = conn.execute("""
        SELECT
            pi_institution,
            pi_country,
            COUNT(*) as num_grants,
            SUM(total_funding) as total_funding,
            ARRAY_AGG(DISTINCT acronym) FILTER (WHERE acronym IS NOT NULL) as projects
        FROM grant_award
        WHERE (
            project_title ILIKE ?
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE ?
        )
        AND pi_institution IS NOT NULL
        GROUP BY pi_institution, pi_country
        ORDER BY total_funding DESC NULLS LAST
        LIMIT ?
    """, [f"%{field_keyword}%", f"%{field_keyword}%", limit]).fetchall()

    return [
        {
            "institution": r[0],
            "country": r[1],
            "num_grants": r[2],
            "total_funding": r[3],
            "projects": r[4],
        }
        for r in rows
    ]


def gap_analysis(
    conn: duckdb.DuckDBPyConnection,
    institution_pattern: str = "%HANNOVER%",
) -> list[dict]:
    """Find funding instruments we haven't used.

    Compares our grants against available instruments/calls.
    """
    rows = conn.execute("""
        WITH our_sources AS (
            SELECT DISTINCT source, framework_programme
            FROM (
                SELECT source, NULL as framework_programme FROM grant_award
                WHERE pi_institution ILIKE ?
                UNION ALL
                SELECT 'ft_portal', framework_programme FROM call
                WHERE call_identifier IN (
                    SELECT DISTINCT call_identifier FROM call
                    WHERE status = 'open'
                )
            )
        ),
        available_programmes AS (
            SELECT DISTINCT
                framework_programme,
                COUNT(*) as open_calls,
                MIN(deadline) as next_deadline
            FROM call
            WHERE status IN ('open', 'forthcoming')
            AND deadline >= CURRENT_DATE
            GROUP BY framework_programme
        )
        SELECT
            ap.framework_programme,
            ap.open_calls,
            ap.next_deadline,
            CASE WHEN EXISTS (
                SELECT 1 FROM grant_award
                WHERE pi_institution ILIKE ?
                AND source_id LIKE '%' || LOWER(COALESCE(ap.framework_programme, '')) || '%'
            ) THEN 'Applied' ELSE 'Never applied' END as our_status
        FROM available_programmes ap
        ORDER BY ap.open_calls DESC
    """, [institution_pattern, institution_pattern]).fetchall()

    return [
        {
            "programme": r[0],
            "open_calls": r[1],
            "next_deadline": r[2],
            "status": r[3],
        }
        for r in rows
    ]


def historical_trends(
    conn: duckdb.DuckDBPyConnection,
    field_keyword: str = "quantum",
) -> list[dict]:
    """Funding for a field over time by year."""
    rows = conn.execute("""
        SELECT
            YEAR(start_date) as start_year,
            COUNT(*) as num_grants,
            SUM(total_funding) as total_funding,
            SUM(eu_contribution) as total_eu
        FROM grant_award
        WHERE (
            project_title ILIKE ?
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE ?
        )
        AND start_date IS NOT NULL
        GROUP BY YEAR(start_date)
        ORDER BY start_year
    """, [f"%{field_keyword}%", f"%{field_keyword}%"]).fetchall()

    return [
        {
            "year": r[0],
            "num_grants": r[1],
            "total_funding": r[2],
            "total_eu": r[3],
        }
        for r in rows
    ]


def sme_instruments(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Find instruments available for Innovailia UG (SME-specific)."""
    rows = conn.execute("""
        SELECT
            call_identifier,
            title,
            deadline,
            status,
            budget_total,
            framework_programme,
            source,
            url
        FROM call
        WHERE (
            ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%sme%'
            OR title ILIKE '%SME%'
            OR title ILIKE '%accelerator%'
            OR title ILIKE '%innovation%'
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%company%'
            OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%tax_credit%'
            OR call_identifier ILIKE '%EIC%'
        )
        AND status IN ('open', 'forthcoming')
        ORDER BY deadline ASC NULLS LAST
    """).fetchall()

    return [
        {
            "identifier": r[0],
            "title": r[1],
            "deadline": r[2],
            "status": r[3],
            "budget": r[4],
            "programme": r[5],
            "source": r[6],
            "url": r[7],
        }
        for r in rows
    ]
