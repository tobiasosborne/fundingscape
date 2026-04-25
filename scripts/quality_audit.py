"""Comprehensive data quality audit for fundingscape + quantum_applications DBs.

Reports:
  A. QA matching quality (per-application coverage, language gap)
  B. Duplicate detection (already-flagged + residual candidates)
  C. Missing data (per-field NULL rates, by source)
  D. Anomalies (dates, currencies, countries, funding outliers)
  E. Funder linkage coverage
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

# Allow running from project root or scripts/
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fundingscape import DB_PATH, QA_DB_PATH


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n  {title}\n{'=' * 78}")


def subsection(title: str) -> None:
    print(f"\n--- {title} ---")


def fmt_pct(n: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{100*n/total:5.1f}%"


def audit_qa_matching(fs: duckdb.DuckDBPyConnection, qa_path: str) -> None:
    section("A. QA APPLICATION MATCHING QUALITY")
    qa = duckdb.connect(qa_path, read_only=True)

    apps = qa.execute("""
        SELECT a.id, a.name, a.domain, fl.grant_count, fl.total_funding_eur
        FROM application a
        LEFT JOIN funding_link fl ON fl.application_id = a.id
        ORDER BY COALESCE(fl.grant_count, 0) ASC
    """).fetchall()

    print(f"Total applications in QA DB: {len(apps)}")

    matched = [a for a in apps if a[3] and a[3] > 0]
    unmatched = [a for a in apps if not a[3] or a[3] == 0]
    under = [a for a in apps if a[3] and 0 < a[3] < 5]
    over = [a for a in apps if a[3] and a[3] > 200]

    print(f"  Matched (>=1 grant):       {len(matched)}")
    print(f"  Unmatched (0 grants):      {len(unmatched)}")
    print(f"  Under-matched (1-4):       {len(under)}")
    print(f"  Over-matched (>200, FP risk): {len(over)}")

    if unmatched:
        subsection("UNMATCHED applications (need keyword work)")
        for a in unmatched:
            print(f"  [{a[2]:25s}] {a[1]}")

    if under:
        subsection("UNDER-MATCHED (1-4 grants — likely keyword gap)")
        for a in under:
            print(f"  [{a[2]:25s}] {a[1]:55s} {a[3]:>3} grants")

    if over:
        subsection("OVER-MATCHED (>200 grants — possible false positives)")
        for a in over:
            funding_m = (a[4] or 0) / 1e6
            print(f"  [{a[2]:25s}] {a[1]:55s} {a[3]:>5} grants  {funding_m:>7.1f}M EUR")

    qa.close()


def audit_german_gap(fs: duckdb.DuckDBPyConnection, qa_path: str) -> None:
    section("A2. GERMAN-LANGUAGE COVERAGE GAP")

    # Roughly classify grants by language using common German stopwords/chars
    de_count = fs.execute("""
        SELECT COUNT(*) FROM grant_award_deduped
        WHERE source IN ('gepris', 'foerderkatalog')
           OR project_title ILIKE '%quanten%'
           OR project_title ILIKE '%forschung%'
           OR project_title ILIKE '%verfahren%'
    """).fetchone()[0]

    en_quantum = fs.execute("""
        SELECT COUNT(*) FROM grant_award_deduped
        WHERE project_title ILIKE '%quantum%'
    """).fetchone()[0]

    de_quantum = fs.execute("""
        SELECT COUNT(*) FROM grant_award_deduped
        WHERE project_title ILIKE '%quanten%'
           OR abstract ILIKE '%quanten%'
    """).fetchone()[0]

    print(f"German-language grants (heuristic):  {de_count:>10,}")
    print(f"Grants with 'quantum' in title (EN): {en_quantum:>10,}")
    print(f"Grants with 'quanten' anywhere (DE): {de_quantum:>10,}")
    print(f"  → If German variants were added, ~{de_quantum} extra grants enter the matching pool.")

    # Check abstract availability per source
    subsection("Abstract availability by source")
    for source in ['cordis_bulk', 'gepris', 'foerderkatalog', 'openaire_bulk']:
        row = fs.execute(f"""
            SELECT COUNT(*),
                   COUNT(CASE WHEN abstract IS NOT NULL AND abstract != '' THEN 1 END)
            FROM grant_award WHERE source = ?
        """, [source]).fetchone()
        if row[0] > 0:
            print(f"  {source:25s} {row[0]:>10,} grants  {row[1]:>10,} with abstract  ({fmt_pct(row[1], row[0])})")


def audit_duplicates(fs: duckdb.DuckDBPyConnection) -> None:
    section("B. DUPLICATE DETECTION")

    total = fs.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
    deduped = fs.execute("SELECT COUNT(*) FROM grant_award WHERE dedup_of IS NOT NULL").fetchone()[0]
    aggregates = fs.execute("SELECT COUNT(*) FROM grant_award WHERE is_aggregate = TRUE").fetchone()[0]

    print(f"Total grant_award rows:    {total:>10,}")
    print(f"Already flagged dedup_of:  {deduped:>10,}  ({fmt_pct(deduped, total)})")
    print(f"Flagged is_aggregate:      {aggregates:>10,}  ({fmt_pct(aggregates, total)})")
    print(f"Canonical (deduped view):  {total - deduped - aggregates:>10,}")

    subsection("RESIDUAL: duplicate (source, source_id) — true uniqueness violations")
    # Note: (source, project_id) is NOT unique in OpenAIRE bulk because the same
    # project_id is used independently across funder slices (e.g. AKA/SNSF/WT all
    # use project_id 211082 for unrelated grants). The unique key is source_id
    # which encodes funder.
    residual = fs.execute("""
        SELECT source, source_id, COUNT(*) c
        FROM grant_award
        WHERE source_id IS NOT NULL AND dedup_of IS NULL
        GROUP BY source, source_id HAVING c > 1
        ORDER BY c DESC LIMIT 10
    """).fetchall()
    if not residual:
        print("  ✓ none found — (source, source_id) unique constraint holds")
    else:
        for r in residual:
            print(f"  {r[0]:25s} {r[1]:30s} x{r[2]}")
        total_resid = fs.execute("""
            SELECT COUNT(*) FROM (
                SELECT source, source_id FROM grant_award
                WHERE source_id IS NOT NULL AND dedup_of IS NULL
                GROUP BY source, source_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f"  TOTAL residual (source, source_id) violations: {total_resid:,}")

    subsection("RESIDUAL: same title + same year + same funder across canonical rows")
    cross = fs.execute("""
        SELECT COUNT(*) FROM (
            SELECT LOWER(TRIM(project_title)) t,
                   YEAR(start_date) y,
                   funder_id,
                   COUNT(*) c
            FROM grant_award_deduped
            WHERE project_title IS NOT NULL AND start_date IS NOT NULL
              AND LENGTH(project_title) > 20
            GROUP BY t, y, funder_id HAVING c > 1
        )
    """).fetchone()[0]
    print(f"  Canonical groups with same title+year+funder: {cross:,}")

    sample = fs.execute("""
        SELECT LOWER(TRIM(project_title)) t,
               YEAR(start_date) y,
               funder_id,
               COUNT(*) c
        FROM grant_award_deduped
        WHERE project_title IS NOT NULL AND start_date IS NOT NULL
          AND LENGTH(project_title) > 20
        GROUP BY t, y, funder_id HAVING c > 1
        ORDER BY c DESC LIMIT 5
    """).fetchall()
    for r in sample:
        print(f"    [{r[1]} fid={r[2]}] x{r[3]}: {r[0][:80]}")


def audit_missing_data(fs: duckdb.DuckDBPyConnection) -> None:
    section("C. MISSING DATA (per source, canonical rows only)")

    sources = fs.execute("""
        SELECT source, COUNT(*) FROM grant_award_deduped
        GROUP BY source ORDER BY 2 DESC
    """).fetchall()

    fields = ['pi_name', 'pi_institution', 'pi_country',
              'start_date', 'end_date', 'total_funding',
              'abstract', 'ror_id', 'funder_id']

    print(f"\n{'source':<25} {'rows':>10}  " + "  ".join(f"{f:>13}" for f in fields))
    print("-" * (25 + 10 + len(fields)*15 + 2))

    for source, n in sources:
        if n < 100:  # skip tiny sources
            continue
        cells = []
        for field in fields:
            row = fs.execute(f"""
                SELECT COUNT(*) FROM grant_award_deduped
                WHERE source = ? AND ({field} IS NULL OR ({field}::TEXT = '' AND ? != 'pi_country'))
            """, [source, field]).fetchone()
            null_pct = 100 * row[0] / n
            cells.append(f"{null_pct:>12.1f}%")
        print(f"{source:<25} {n:>10,}  " + "  ".join(cells))

    subsection("Funding totals: real vs estimated vs missing")
    row = fs.execute("""
        SELECT
            COUNT(*) FILTER (WHERE total_funding IS NOT NULL) as real_funding,
            COUNT(*) FILTER (WHERE total_funding IS NULL AND total_funding_estimated IS NOT NULL) as estimated_only,
            COUNT(*) FILTER (WHERE total_funding IS NULL AND total_funding_estimated IS NULL) as missing,
            COUNT(*) as total
        FROM grant_award_deduped
    """).fetchone()
    print(f"  Real total_funding:        {row[0]:>10,} ({fmt_pct(row[0], row[3])})")
    print(f"  Estimated only:            {row[1]:>10,} ({fmt_pct(row[1], row[3])})")
    print(f"  Missing entirely:          {row[2]:>10,} ({fmt_pct(row[2], row[3])})")


def audit_anomalies(fs: duckdb.DuckDBPyConnection) -> None:
    section("D. ANOMALIES")

    subsection("Date sanity")
    rows = fs.execute("""
        SELECT
            COUNT(*) FILTER (WHERE start_date < '1900-01-01') as ancient_start,
            COUNT(*) FILTER (WHERE start_date > CURRENT_DATE + INTERVAL 5 YEAR) as far_future_start,
            COUNT(*) FILTER (WHERE end_date < start_date) as end_before_start,
            COUNT(*) FILTER (WHERE EXTRACT(YEAR FROM end_date) - EXTRACT(YEAR FROM start_date) > 30) as too_long
        FROM grant_award_deduped
    """).fetchone()
    print(f"  start_date < 1900:           {rows[0]:>10,}")
    print(f"  start_date > today + 5y:     {rows[1]:>10,}")
    print(f"  end_date < start_date:       {rows[2]:>10,}")
    print(f"  duration > 30 years:         {rows[3]:>10,}")

    subsection("Currency distribution (canonical)")
    for r in fs.execute("""
        SELECT currency, COUNT(*)
        FROM grant_award_deduped
        WHERE total_funding IS NOT NULL
        GROUP BY currency ORDER BY 2 DESC LIMIT 15
    """).fetchall():
        print(f"  {r[0] or 'NULL':10s} {r[1]:>10,}")

    subsection("Country code distribution (top 20 + suspicious)")
    for r in fs.execute("""
        SELECT pi_country, COUNT(*)
        FROM grant_award_deduped
        WHERE pi_country IS NOT NULL
        GROUP BY pi_country ORDER BY 2 DESC LIMIT 20
    """).fetchall():
        marker = ""
        if r[0] and (len(r[0]) != 2 or not r[0].isupper() or not r[0].isalpha()):
            marker = "  ⚠ non-ISO2"
        print(f"  {r[0] or 'NULL':10s} {r[1]:>10,}{marker}")

    suspicious = fs.execute("""
        SELECT COUNT(DISTINCT pi_country) FROM grant_award_deduped
        WHERE pi_country IS NOT NULL
          AND (LENGTH(pi_country) != 2 OR NOT regexp_matches(pi_country, '^[A-Z]{2}$'))
    """).fetchone()[0]
    print(f"\n  Distinct non-ISO2 country codes: {suspicious}")

    subsection("Funding amount outliers (EUR-normalized)")
    rows = fs.execute("""
        SELECT
            COUNT(*) FILTER (WHERE total_funding_eur < 0) as negative,
            COUNT(*) FILTER (WHERE total_funding_eur = 0) as zero,
            COUNT(*) FILTER (WHERE total_funding_eur > 1e9) as gt_1B,
            COUNT(*) FILTER (WHERE total_funding_eur > 1e8 AND total_funding_eur <= 1e9) as gt_100M
        FROM grant_award_deduped
    """).fetchone()
    print(f"  Negative funding:        {rows[0]:>10,}")
    print(f"  Zero funding:            {rows[1]:>10,}")
    print(f"  > 1B EUR (normalized):   {rows[2]:>10,}")
    print(f"  100M-1B EUR:             {rows[3]:>10,}")

    if rows[2] > 0:
        print("\n  Sample > 1B EUR (post-normalization — should be real big grants):")
        for r in fs.execute("""
            SELECT source, project_title, total_funding, currency, total_funding_eur
            FROM grant_award_deduped
            WHERE total_funding_eur > 1e9
            ORDER BY total_funding_eur DESC LIMIT 5
        """).fetchall():
            print(f"    [{r[0]:20s}] {r[2]:>15,.0f} {r[3] or '':3s} → {r[4]:>15,.0f} EUR  {(r[1] or '')[:55]}")


def audit_funder_linkage(fs: duckdb.DuckDBPyConnection) -> None:
    section("E. FUNDER LINKAGE")
    rows = fs.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE funder_id IS NOT NULL) as linked
        FROM grant_award_deduped
    """).fetchone()
    print(f"  Total canonical:    {rows[0]:>10,}")
    print(f"  Linked to funder:   {rows[1]:>10,} ({fmt_pct(rows[1], rows[0])})")

    subsection("Top funders by grant count")
    for r in fs.execute("""
        SELECT f.short_name, f.country, COUNT(*) as cnt,
               SUM(COALESCE(g.total_funding, g.total_funding_estimated, 0)) as funding
        FROM grant_award_deduped g
        LEFT JOIN funder f ON g.funder_id = f.id
        GROUP BY f.short_name, f.country
        ORDER BY cnt DESC LIMIT 15
    """).fetchall():
        funding_m = (r[3] or 0) / 1e6
        print(f"  {(r[0] or 'UNLINKED'):15s} {(r[1] or '?'):3s} {r[2]:>10,}  {funding_m:>10,.0f}M EUR")


def main() -> None:
    print(f"Auditing: {DB_PATH}")
    print(f"          {QA_DB_PATH}")
    fs = duckdb.connect(DB_PATH, read_only=True)
    try:
        audit_qa_matching(fs, QA_DB_PATH)
        audit_german_gap(fs, QA_DB_PATH)
        audit_duplicates(fs)
        audit_missing_data(fs)
        audit_anomalies(fs)
        audit_funder_linkage(fs)
    finally:
        fs.close()


if __name__ == "__main__":
    main()
