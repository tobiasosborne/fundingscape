#!/usr/bin/env python3
"""Plot quantum computing application funding by domain.

Follows Kieran Healy's data visualisation principles:
- Show the data directly (Cleveland dot plot, not bars)
- Minimise non-data ink (no gridlines, no box, no legend clutter)
- Use position rather than area/colour for primary encoding
- Label directly rather than using legends
- Sort by the variable of interest
- Use a clean, readable typeface at sufficient size

Usage:
    uv run python scripts/plot_qa_funding.py
"""

from __future__ import annotations

import duckdb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

matplotlib.use("Agg")


def main() -> None:
    qa = duckdb.connect("data/db/quantum_applications.duckdb", read_only=True)

    # --- Data: domain-level TAM ---
    domain_data = qa.execute("""
        SELECT a.domain,
               SUM(fl.total_funding_eur) / 1e6 AS funding_m,
               SUM(fl.grant_count) AS grants,
               COUNT(*) AS apps
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
        GROUP BY a.domain
        ORDER BY funding_m ASC
    """).fetchall()

    # --- Data: application-level with advantage status ---
    app_data = qa.execute("""
        SELECT a.domain, a.name, a.advantage_status,
               fl.total_funding_eur / 1e6 AS funding_m,
               fl.grant_count
        FROM application a
        JOIN funding_link fl ON a.id = fl.application_id
        WHERE fl.grant_count > 0
        ORDER BY fl.total_funding_eur ASC
    """).fetchall()

    qa.close()

    # =====================================================================
    # FIGURE 1: Domain-level Cleveland dot plot
    # =====================================================================
    domains = [r[0] for r in domain_data]
    funding = [r[1] for r in domain_data]
    grants = [r[2] for r in domain_data]
    apps = [r[3] for r in domain_data]

    fig, ax = plt.subplots(figsize=(8, 7))

    # Healy: horizontal dot plot, sorted by value
    y_pos = range(len(domains))
    ax.scatter(funding, y_pos, s=50, color="#2c3e50", zorder=3, clip_on=False)

    # Thin connector lines from axis to dot (lollipop)
    for y, f in zip(y_pos, funding):
        ax.plot([0, f], [y, y], color="#bdc3c7", linewidth=0.6, zorder=1)

    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(domains, fontsize=10)
    ax.set_xlabel("Total addressable funding (EUR millions)", fontsize=11)

    # Healy: remove chart junk
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.tick_params(left=False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x:,.0f}"
    ))

    # Direct annotation on top dots
    for y, f, g, a in zip(y_pos, funding, grants, apps):
        ax.annotate(
            f"  {g:,} grants · {a} apps",
            (f, y),
            fontsize=7.5,
            color="#7f8c8d",
            va="center",
        )

    ax.set_title(
        "Quantum Computing Applications:\nTotal Addressable Funding by Domain",
        fontsize=13,
        fontweight="bold",
        loc="left",
        pad=12,
    )
    ax.annotate(
        "Source: Fundingscape (4M grants) × QC Applications DB (94 applications)",
        xy=(0, -0.08),
        xycoords="axes fraction",
        fontsize=8,
        color="#95a5a6",
    )

    fig.tight_layout()
    fig.savefig("data/qa_funding_by_domain.png", dpi=200, bbox_inches="tight")
    print("Saved: data/qa_funding_by_domain.png")
    plt.close(fig)

    # =====================================================================
    # FIGURE 2: Application-level dot plot, coloured by advantage status
    # =====================================================================

    # Healy: use colour sparingly, for a single meaningful variable
    status_colours = {
        "proven": "#27ae60",
        "proven_with_caveats": "#2ecc71",
        "conjectured": "#f39c12",
        "heuristic_only": "#e74c3c",
        "debated": "#9b59b6",
        "disproven": "#95a5a6",
        "unknown": "#bdc3c7",
    }

    # Top 30 by funding for readability
    top_apps = app_data[-30:]
    names = [r[1] for r in top_apps]
    fund = [r[3] for r in top_apps]
    statuses = [r[2] for r in top_apps]
    colours = [status_colours.get(s, "#bdc3c7") for s in statuses]

    fig2, ax2 = plt.subplots(figsize=(9, 9))

    y_pos2 = range(len(names))
    ax2.scatter(fund, y_pos2, s=40, c=colours, zorder=3, clip_on=False)

    for y, f in zip(y_pos2, fund):
        ax2.plot([0, f], [y, y], color="#ecf0f1", linewidth=0.5, zorder=1)

    # Truncate long names
    short_names = [n[:55] + "..." if len(n) > 58 else n for n in names]
    ax2.set_yticks(list(y_pos2))
    ax2.set_yticklabels(short_names, fontsize=8.5)

    ax2.set_xlabel("Total addressable funding (EUR millions)", fontsize=11)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.spines["left"].set_visible(False)
    ax2.tick_params(left=False)
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x:,.0f}"
    ))

    ax2.set_title(
        "Top 30 Quantum Computing Applications by Funding\n"
        "Coloured by strength of quantum advantage evidence",
        fontsize=12,
        fontweight="bold",
        loc="left",
        pad=12,
    )

    # Healy: direct legend, minimal
    legend_y = -0.06
    legend_x = 0.0
    for status, colour in [
        ("Proven", "#27ae60"),
        ("Proven (caveats)", "#2ecc71"),
        ("Conjectured", "#f39c12"),
        ("Heuristic only", "#e74c3c"),
        ("Debated", "#9b59b6"),
    ]:
        ax2.annotate(
            f"● {status}",
            xy=(legend_x, legend_y),
            xycoords="axes fraction",
            fontsize=8,
            color=colour,
            fontweight="bold",
        )
        legend_x += 0.2

    ax2.annotate(
        "Source: Fundingscape × QC Applications DB",
        xy=(0, legend_y - 0.04),
        xycoords="axes fraction",
        fontsize=8,
        color="#95a5a6",
    )

    fig2.tight_layout()
    fig2.savefig("data/qa_top30_applications.png", dpi=200, bbox_inches="tight")
    print("Saved: data/qa_top30_applications.png")
    plt.close(fig2)


if __name__ == "__main__":
    main()
