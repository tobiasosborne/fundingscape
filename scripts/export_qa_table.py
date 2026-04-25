#!/usr/bin/env python3
"""Export quantum applications DB as a structured markdown table for LLM ingestion.

Usage:
    uv run python scripts/export_qa_table.py
"""

from __future__ import annotations

import duckdb


def main() -> None:
    qa = duckdb.connect("data/db/quantum_applications.duckdb", read_only=True)
    fs = duckdb.connect("data/db/fundingscape.duckdb", read_only=True)

    n_apps = qa.execute("SELECT COUNT(*) FROM application").fetchone()[0]
    n_grants = fs.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
    fs.close()

    lines: list[str] = []
    lines.append("# Quantum Computing Applications: Funding & Advantage Classification")
    lines.append("")
    lines.append(
        f"Generated from Fundingscape ({n_grants:,} grants) "
        f"× QC Applications DB ({n_apps} applications). "
        f"All funding amounts EUR-normalized via ECB annual reference rates."
    )

    totals = qa.execute("""
        SELECT SUM(fl.grant_count), SUM(fl.total_funding_eur) / 1e9
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
    """).fetchone()
    lines.append(
        f"Total addressable funding matched: {totals[1]:.2f}B EUR "
        f"across {totals[0]:,.0f} grants."
    )
    lines.append("")

    # --- Domain summary ---
    lines.append("## Domain Summary")
    lines.append("")
    lines.append(
        "| Domain | Apps | Grants | Funding (M EUR) "
        "| Best Advantage | Strongest Evidence |"
    )
    lines.append(
        "|--------|------|--------|-----------------|"
        "----------------|-------------------|"
    )

    for row in qa.execute("""
        SELECT
            a.domain,
            COUNT(*) as apps,
            SUM(fl.grant_count) as grants,
            SUM(fl.total_funding_eur) / 1e6 as funding_m,
            CASE
                WHEN BOOL_OR(a.advantage_type = 'exponential') THEN 'exponential'
                WHEN BOOL_OR(a.advantage_type = 'superpolynomial') THEN 'superpolynomial'
                WHEN BOOL_OR(a.advantage_type = 'polynomial') THEN 'polynomial'
                WHEN BOOL_OR(a.advantage_type = 'quadratic') THEN 'quadratic'
                ELSE 'unknown'
            END as best_adv,
            CASE
                WHEN BOOL_OR(a.advantage_status = 'proven') THEN 'proven'
                WHEN BOOL_OR(a.advantage_status = 'proven_with_caveats') THEN 'proven (caveats)'
                WHEN BOOL_OR(a.advantage_status = 'conjectured') THEN 'conjectured'
                WHEN BOOL_OR(a.advantage_status = 'heuristic_only') THEN 'heuristic only'
                ELSE 'unknown'
            END as best_status
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
        GROUP BY a.domain
        ORDER BY funding_m DESC
    """).fetchall():
        lines.append(
            f"| {row[0]} | {row[1]} | {row[2]:,} | {row[3]:,.1f} "
            f"| {row[4]} | {row[5]} |"
        )

    lines.append("")

    # --- Full application table ---
    lines.append("## All Applications")
    lines.append("")
    lines.append(
        "| Domain | Subdomain | Application | Advantage Type "
        "| Advantage Status | Maturity | Grants | Funding (M EUR) "
        "| Year | Top Funders |"
    )
    lines.append(
        "|--------|-----------|-------------|----------------"
        "|-----------------|----------|--------|-----------------|"
        "------|-------------|"
    )

    for row in qa.execute("""
        SELECT
            a.domain, a.subdomain, a.name,
            a.advantage_type, a.advantage_status, a.maturity,
            fl.grant_count, fl.total_funding_eur / 1e6 as funding_m,
            a.year_first_proposed,
            fl.top_funders
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
        ORDER BY a.domain, a.subdomain, fl.total_funding_eur DESC
    """).fetchall():
        year = str(row[8]) if row[8] else ""
        funders = row[9] if row[9] else ""
        funder_parts = funders.split(", ")[:3]
        short_funders = ", ".join(funder_parts)
        lines.append(
            f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} "
            f"| {row[4]} | {row[5]} | {row[6]:,} | {row[7]:,.1f} "
            f"| {year} | {short_funders} |"
        )

    lines.append("")

    # --- Advantage classification matrix ---
    lines.append("## Advantage Classification Matrix")
    lines.append("")
    lines.append(
        "| Advantage Type | proven | proven (caveats) | conjectured "
        "| heuristic only | debated | disproven |"
    )
    lines.append(
        "|---------------|--------|-----------------|-------------"
        "|----------------|---------|-----------|"
    )

    for row in qa.execute("""
        SELECT
            advantage_type,
            COUNT(*) FILTER (WHERE advantage_status = 'proven') as proven,
            COUNT(*) FILTER (WHERE advantage_status = 'proven_with_caveats') as caveats,
            COUNT(*) FILTER (WHERE advantage_status = 'conjectured') as conj,
            COUNT(*) FILTER (WHERE advantage_status = 'heuristic_only') as heur,
            COUNT(*) FILTER (WHERE advantage_status = 'debated') as debated,
            COUNT(*) FILTER (WHERE advantage_status = 'disproven') as disproven
        FROM application
        GROUP BY advantage_type
        ORDER BY CASE advantage_type
            WHEN 'exponential' THEN 1
            WHEN 'superpolynomial' THEN 2
            WHEN 'polynomial' THEN 3
            WHEN 'quadratic' THEN 4
            WHEN 'subquadratic' THEN 5
            WHEN 'unknown' THEN 6
            ELSE 7
        END
    """).fetchall():
        lines.append(
            f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} "
            f"| {row[4]} | {row[5]} | {row[6]} |"
        )

    lines.append("")

    # --- Maturity distribution ---
    lines.append("## Maturity Distribution")
    lines.append("")
    lines.append("| Maturity Level | Count | Total Funding (M EUR) |")
    lines.append("|---------------|-------|-----------------------|")

    for row in qa.execute("""
        SELECT a.maturity, COUNT(*),
               SUM(fl.total_funding_eur) / 1e6 as funding_m
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
        GROUP BY a.maturity
        ORDER BY CASE a.maturity
            WHEN 'production' THEN 1
            WHEN 'industry_pilot' THEN 2
            WHEN 'small_device_demo' THEN 3
            WHEN 'numerical_evidence' THEN 4
            WHEN 'theoretical' THEN 5
        END
    """).fetchall():
        lines.append(f"| {row[0]} | {row[1]} | {row[2]:,.1f} |")

    lines.append("")

    # --- Timeline: earliest proposals by decade ---
    lines.append("## Timeline: When Applications Were First Proposed")
    lines.append("")
    lines.append("| Decade | Applications |")
    lines.append("|--------|-------------|")

    for row in qa.execute("""
        SELECT
            (year_first_proposed / 10) * 10 as decade,
            LIST(name ORDER BY year_first_proposed) as apps
        FROM application
        WHERE year_first_proposed IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """).fetchall():
        decade_label = f"{int(row[0])}s"
        app_list = "; ".join(row[1][:8])
        if len(row[1]) > 8:
            app_list += f" (+{len(row[1]) - 8} more)"
        lines.append(f"| {decade_label} | {app_list} |")

    qa.close()

    output = "\n".join(lines)
    path = "data/qa_applications_table.md"
    with open(path, "w") as f:
        f.write(output)
    print(f"Written {len(lines)} lines to {path}")


if __name__ == "__main__":
    main()
