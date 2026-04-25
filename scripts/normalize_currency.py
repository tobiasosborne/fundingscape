"""One-shot currency normalization: populate total_funding_eur column.

Adds (if missing) a `total_funding_eur` column to grant_award and fills it
using fundingscape.currency.to_eur. Idempotent — safe to re-run.

Strategy:
  - For each row with total_funding IS NOT NULL:
      year = YEAR(start_date) or YEAR(end_date) or 2024
      eur = to_eur(total_funding, currency, year)
  - Stores result in total_funding_eur.
  - Bulk operation: pulls rates into a temp table for SQL-side conversion
    where possible; falls back to Python loop for unusual currencies.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fundingscape import DB_PATH
from fundingscape.currency import _RATES, get_rate


def ensure_column(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'grant_award'"
        ).fetchall()
    }
    if "total_funding_eur" not in cols:
        print("Adding column total_funding_eur ...")
        conn.execute("ALTER TABLE grant_award ADD COLUMN total_funding_eur DOUBLE")
    else:
        print("Column total_funding_eur already exists")


def build_rate_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Materialize the (currency, year, rate) table inside DuckDB for SQL join."""
    rows = []
    for cur, table in _RATES.items():
        for year, rate in table.items():
            rows.append((cur, year, rate))
    conn.execute("DROP TABLE IF EXISTS _fx_rate")
    conn.execute("""
        CREATE TEMP TABLE _fx_rate (
            currency TEXT, year INTEGER, rate DOUBLE
        )
    """)
    conn.executemany(
        "INSERT INTO _fx_rate VALUES (?, ?, ?)",
        rows,
    )
    print(f"Loaded {len(rows)} FX rates across {len(_RATES)} currencies")


def populate_eur(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Fill total_funding_eur for all rows with total_funding IS NOT NULL.

    Uses SQL window for nearest-year fallback per currency.
    """
    # Reset to start fresh (idempotency)
    conn.execute("UPDATE grant_award SET total_funding_eur = NULL")

    # 1. EUR records: 1:1 copy
    conn.execute("""
        UPDATE grant_award
        SET total_funding_eur = total_funding
        WHERE total_funding IS NOT NULL
          AND (currency = 'EUR' OR currency IS NULL OR currency = '')
    """)
    n_eur = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE (currency = 'EUR' OR currency IS NULL OR currency = '')
          AND total_funding_eur IS NOT NULL
    """).fetchone()[0]
    print(f"EUR rows copied: {n_eur:,}")

    # 2. Foreign currency records — join with FX rates, with nearest-year fallback.
    # Strategy: for each non-EUR row, find rate for its grant year; if no exact
    # match for that year, use the nearest year for that currency.
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW _grant_year AS
        SELECT id, total_funding, currency,
               COALESCE(YEAR(start_date), YEAR(end_date), 2024) AS y
        FROM grant_award
        WHERE total_funding IS NOT NULL
          AND currency IS NOT NULL
          AND currency != 'EUR'
          AND currency != ''
    """)

    # For each (currency, target_year), find nearest year with a rate.
    # Materialize the lookup of "nearest rate per (currency, year)".
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _grant_rate AS
        WITH candidates AS (
            SELECT g.id, g.total_funding, g.currency, g.y,
                   r.year AS rate_year, r.rate,
                   ABS(g.y - r.year) AS gap,
                   ROW_NUMBER() OVER (
                       PARTITION BY g.id
                       ORDER BY ABS(g.y - r.year), r.year DESC
                   ) AS rn
            FROM _grant_year g
            JOIN _fx_rate r ON g.currency = r.currency
        )
        SELECT id, total_funding, currency, y, rate_year, rate
        FROM candidates WHERE rn = 1
    """)

    matched = conn.execute("SELECT COUNT(*) FROM _grant_rate").fetchone()[0]
    print(f"Foreign rows with rate match: {matched:,}")

    # Update grant_award using the lookup table
    conn.execute("""
        UPDATE grant_award g
        SET total_funding_eur = (gr.total_funding / gr.rate)
        FROM _grant_rate gr
        WHERE g.id = gr.id
    """)

    # Stats
    stats = {}
    stats["total_funded"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE total_funding IS NOT NULL"
    ).fetchone()[0]
    stats["eur_set"] = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE total_funding_eur IS NOT NULL"
    ).fetchone()[0]
    stats["unmapped"] = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE total_funding IS NOT NULL
          AND total_funding_eur IS NULL
    """).fetchone()[0]
    return stats


def report_unmapped(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("""
        SELECT currency, COUNT(*), SUM(total_funding)
        FROM grant_award
        WHERE total_funding IS NOT NULL AND total_funding_eur IS NULL
        GROUP BY currency ORDER BY 2 DESC
    """).fetchall()
    if not rows:
        print("✓ all funded rows have EUR conversion")
        return
    print("\nUnmapped currencies (no EUR conversion):")
    for cur, cnt, total in rows:
        print(f"  {cur or 'NULL':10s} {cnt:>10,} rows  total={total:>15,.0f}")


def report_summary(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n=== Per-currency conversion summary (canonical only) ===")
    for r in conn.execute("""
        SELECT
            currency,
            COUNT(*) FILTER (WHERE total_funding IS NOT NULL) as n,
            ROUND(SUM(total_funding) / 1e9, 2) as orig_total_B,
            ROUND(SUM(total_funding_eur) / 1e9, 2) as eur_total_B
        FROM grant_award_deduped
        WHERE currency IS NOT NULL
        GROUP BY currency
        ORDER BY n DESC LIMIT 20
    """).fetchall():
        print(f"  {r[0]:8s}  n={r[1]:>10,}  orig={r[2] or 0:>9.2f}B  eur={r[3] or 0:>9.2f}B")

    grand_eur = conn.execute("""
        SELECT SUM(total_funding_eur) FROM grant_award_deduped
    """).fetchone()[0]
    print(f"\n  Grand total (canonical, EUR): {grand_eur/1e9 if grand_eur else 0:.2f}B EUR")


def main() -> None:
    conn = duckdb.connect(DB_PATH)
    try:
        ensure_column(conn)
        build_rate_table(conn)
        stats = populate_eur(conn)
        print(f"\nFunded rows:           {stats['total_funded']:,}")
        print(f"EUR-converted:         {stats['eur_set']:,}")
        print(f"Unmapped (NULL eur):   {stats['unmapped']:,}")
        report_unmapped(conn)
        report_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
