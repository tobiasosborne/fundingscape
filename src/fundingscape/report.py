"""Report generation for the funding landscape system."""

from __future__ import annotations

import logging
from datetime import date, datetime

import duckdb

from fundingscape.db import get_connection
from fundingscape.queries import (
    funding_landscape_summary,
    historical_trends,
    income_projection,
    open_calls_by_deadline,
    sme_instruments,
    top_pis_by_field,
)

logger = logging.getLogger(__name__)


def generate_report(conn: duckdb.DuckDBPyConnection | None = None) -> str:
    """Generate a comprehensive funding landscape report in Markdown."""
    if conn is None:
        conn = get_connection()

    sections = []
    sections.append(_header())
    sections.append(_executive_summary(conn))
    sections.append(_deadline_calendar(conn))
    sections.append(_top_recommended_calls(conn))
    sections.append(_income_projection(conn))
    sections.append(_quantum_landscape(conn))
    sections.append(_sme_section(conn))
    sections.append(_data_quality(conn))

    return "\n\n".join(sections)


def _header() -> str:
    return f"""# EU Research Funding Landscape Intelligence Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
**Institution**: Leibniz Universität Hannover
**Research Group**: Quantum Information Theory
**Company**: Innovailia UG"""


def _executive_summary(conn: duckdb.DuckDBPyConnection) -> str:
    total_grants = conn.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
    deduped_grants = conn.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
    duplicate_grants = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE dedup_of IS NOT NULL"
    ).fetchone()[0]
    total_calls = conn.execute("SELECT COUNT(*) FROM call").fetchone()[0]
    open_calls = conn.execute(
        "SELECT COUNT(*) FROM call WHERE status IN ('open', 'forthcoming')"
    ).fetchone()[0]
    active_luh = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(total_funding), 0) FROM grant_award_deduped "
        "WHERE pi_institution ILIKE '%HANNOVER%' AND status = 'active'"
    ).fetchone()
    quantum_grants = conn.execute(
        "SELECT COUNT(*) FROM grant_award_deduped WHERE project_title ILIKE '%quantum%'"
    ).fetchone()[0]

    sources = conn.execute(
        "SELECT id, name, records_fetched, status FROM data_source ORDER BY id"
    ).fetchall()

    src_lines = "\n".join(
        f"| {r[0]:20s} | {r[1]:25s} | {r[2] or 0:>8,} | {r[3]:12s} |"
        for r in sources
    )

    return f"""## Executive Summary

| Metric | Value |
|--------|-------|
| Total grants in database | {total_grants:,} |
| Unique grants (deduplicated) | {deduped_grants:,} |
| Duplicate grants flagged | {duplicate_grants:,} |
| Total calls in database | {total_calls:,} |
| Open/forthcoming calls | {open_calls:,} |
| Active LUH grants (Horizon) | {active_luh[0]:,} |
| Active LUH grant funding | {active_luh[1]:,.0f} EUR |
| Quantum-related grants | {quantum_grants:,} |

### Data Sources

| Source ID | Name | Records | Status |
|-----------|------|---------|--------|
{src_lines}"""


def _deadline_calendar(conn: duckdb.DuckDBPyConnection) -> str:
    calls = open_calls_by_deadline(conn, months_ahead=6, quantum_only=True)

    if not calls:
        return "## Deadline Calendar (Next 6 Months)\n\nNo relevant open calls found."

    lines = ["## Deadline Calendar (Next 6 Months)\n"]
    lines.append("| Deadline | Identifier | Title | Programme | Status |")
    lines.append("|----------|------------|-------|-----------|--------|")
    for c in calls[:30]:
        deadline = str(c["deadline"]) if c["deadline"] else "Rolling"
        ident = (c["identifier"] or "")[:45]
        title = (c["title"] or "")[:55]
        prog = (c["programme"] or "")[:10]
        lines.append(f"| {deadline} | {ident} | {title} | {prog} | {c['status']} |")

    return "\n".join(lines)


def _top_recommended_calls(conn: duckdb.DuckDBPyConnection) -> str:
    calls = open_calls_by_deadline(conn, months_ahead=12, quantum_only=True)

    if not calls:
        return "## Top Recommended Calls\n\nNo relevant calls found."

    lines = ["## Top 20 Recommended Calls (Next 12 Months)\n"]
    lines.append("Ranked by deadline (soonest first), filtered to quantum/physics/ERC/MSCA relevance.\n")
    for i, c in enumerate(calls[:20], 1):
        deadline = str(c["deadline"]) if c["deadline"] else "Rolling"
        budget_str = f"{c['budget']:,.0f} EUR" if c["budget"] else "N/A"
        lines.append(f"**{i}. {c['title'][:70]}**")
        lines.append(f"   - ID: `{c['identifier']}`")
        lines.append(f"   - Deadline: {deadline}")
        lines.append(f"   - Budget: {budget_str}")
        lines.append(f"   - Programme: {c['programme']}")
        if c.get("url"):
            lines.append(f"   - URL: {c['url']}")
        lines.append("")

    return "\n".join(lines)


def _income_projection(conn: duckdb.DuckDBPyConnection) -> str:
    projection = income_projection(conn, "%HANNOVER%")

    if not projection:
        return "## Income Projection\n\nNo active grants found for LUH."

    lines = ["## Income Projection (LUH Horizon Europe Grants)\n"]
    lines.append("Based on linear burn rate across grant duration.\n")
    lines.append("| Year | Active Grants | Projected Income (EUR) |")
    lines.append("|------|--------------|----------------------|")
    for p in projection:
        income = p["projected_income"] or 0
        lines.append(f"| {p['year']} | {p['active_grants']:3d} | {income:>15,.0f} |")

    return "\n".join(lines)


def _quantum_landscape(conn: duckdb.DuckDBPyConnection) -> str:
    trends = historical_trends(conn, "quantum")
    top_inst = top_pis_by_field(conn, "quantum", limit=15)

    lines = ["## Quantum Computing Funding Landscape\n"]

    lines.append("### Historical Trends\n")
    lines.append("| Year | Grants | Total Funding (EUR) |")
    lines.append("|------|--------|-------------------|")
    for t in trends:
        funding = t["total_funding"] or 0
        lines.append(f"| {t['year']} | {t['num_grants']:4d} | {funding:>15,.0f} |")

    lines.append("\n### Top 15 Institutions by Quantum Grant Funding\n")
    lines.append("| Rank | Institution | Country | Grants | Total Funding (EUR) |")
    lines.append("|------|-------------|---------|--------|-------------------|")
    for i, inst in enumerate(top_inst, 1):
        name = (inst["institution"] or "")[:45]
        funding = inst["total_funding"] or 0
        lines.append(f"| {i:2d} | {name} | {inst['country'] or '??':2s} | {inst['num_grants']:4d} | {funding:>15,.0f} |")

    return "\n".join(lines)


def _sme_section(conn: duckdb.DuckDBPyConnection) -> str:
    instruments = sme_instruments(conn)

    lines = ["## Innovailia UG — SME Funding Opportunities\n"]
    if not instruments:
        lines.append("No SME-specific open calls found. Consider:")
        lines.append("- Forschungszulage (R&D tax credit): 25% of R&D personnel costs")
        lines.append("- EIC Accelerator: for deep-tech startups")
        lines.append("- EIC Pathfinder: for breakthrough research (consortium)")
        return "\n".join(lines)

    lines.append("| Deadline | Identifier | Title | Programme |")
    lines.append("|----------|------------|-------|-----------|")
    for inst in instruments[:15]:
        deadline = str(inst["deadline"]) if inst["deadline"] else "Rolling"
        lines.append(f"| {deadline} | {inst['identifier'] or ''} | {inst['title'][:55]} | {inst['programme'] or ''} |")

    return "\n".join(lines)


def _data_quality(conn: duckdb.DuckDBPyConnection) -> str:
    sources = conn.execute("""
        SELECT id, name, records_fetched, last_success, status, error_message
        FROM data_source
        ORDER BY id
    """).fetchall()

    lines = ["## Data Quality Report\n"]

    # Coverage
    total_grants = conn.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
    deduped_grants = conn.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
    duplicate_grants = total_grants - deduped_grants
    total_calls = conn.execute("SELECT COUNT(*) FROM call").fetchone()[0]

    lines.append(f"- **Total grants loaded**: {total_grants:,}")
    lines.append(f"- **Unique grants (deduplicated)**: {deduped_grants:,}")
    lines.append(f"- **Duplicate grants flagged**: {duplicate_grants:,}")
    lines.append(f"- **Total calls loaded**: {total_calls:,}")
    lines.append("")

    # Source status
    lines.append("| Source | Records | Last Success | Status | Error |")
    lines.append("|--------|---------|-------------|--------|-------|")
    for s in sources:
        last = str(s[3])[:19] if s[3] else "Never"
        error = (s[5] or "")[:40]
        lines.append(f"| {s[1]:25s} | {s[2] or 0:>7,} | {last} | {s[4]:12s} | {error} |")

    # Known gaps
    lines.append("\n### Known Gaps\n")
    lines.append("- BMBF Förderkatalog: Currently under maintenance (until 23 Feb 2026)")
    lines.append("- DFG GEPRIS: No API available, requires web scraping (rate-limited)")
    lines.append("- VolkswagenStiftung: Manual entries only")
    lines.append("- NATO SPS: Manual entries only")
    lines.append("- AFOSR/ONR/ARL: Manual entries for BAA details")

    return "\n".join(lines)


if __name__ == "__main__":
    report = generate_report()
    print(report)

    # Also save to file
    with open("REPORT.md", "w") as f:
        f.write(report)
    print("\nReport saved to REPORT.md")
