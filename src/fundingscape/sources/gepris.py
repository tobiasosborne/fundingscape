"""DFG GEPRIS scraper for German research grants."""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import date

import duckdb
from bs4 import BeautifulSoup

from fundingscape import CACHE_DIR
from fundingscape.cache import CachedHttpClient
from fundingscape.db import upsert_grant, update_data_source
from fundingscape.models import GrantAward

logger = logging.getLogger(__name__)

SOURCE_ID = "gepris"
GEPRIS_BASE = "https://gepris.dfg.de"
SEARCH_URL = f"{GEPRIS_BASE}/gepris/OCTOPUS"
PROJECT_URL = f"{GEPRIS_BASE}/gepris/projekt"

# Keywords relevant to our research group
QUANTUM_KEYWORDS = [
    "quantum computing",
    "quantum information",
    "topological quantum",
    "many-body quantum",
    "quantum entanglement",
    "quantum error correction",
    "Quantencomputer",
    "Quanteninformation",
]


def _search_projects(
    client: CachedHttpClient,
    keyword: str,
    max_results: int = 500,
) -> list[dict]:
    """Search GEPRIS for projects matching a keyword.

    Returns list of {id, title, institution, funding_programme} dicts.
    """
    url = (
        f"{SEARCH_URL}?task=doSearchSimple&context=projekt"
        f"&keywords_criterion={keyword}"
        f"&results_per_page={max_results}"
        f"&language=en"
    )

    try:
        html = client.fetch_text(url)
    except Exception as e:
        logger.error("GEPRIS search failed for '%s': %s", keyword, e)
        return []

    soup = BeautifulSoup(html, "lxml")
    results = []

    # Parse search results - each project is in a div with class "result_item"
    for item in soup.select(".result_item, .resultContainer .results .item, #liste .details"):
        link = item.select_one("a[href*='/gepris/projekt/']")
        if not link:
            continue

        href = link.get("href", "")
        match = re.search(r"/gepris/projekt/(\d+)", href)
        if not match:
            continue

        project_id = match.group(1)
        title = link.get_text(strip=True)

        # Try to get institution and programme from surrounding text
        text_parts = item.get_text(separator="|", strip=True).split("|")

        results.append({
            "id": project_id,
            "title": title,
            "text_parts": text_parts,
        })

    logger.info("GEPRIS search '%s': found %d results", keyword, len(results))
    return results


def _fetch_project_detail(
    client: CachedHttpClient,
    project_id: str,
) -> GrantAward | None:
    """Fetch and parse a single project detail page."""
    url = f"{PROJECT_URL}/{project_id}?language=en"

    try:
        html = client.fetch_text(url)
    except Exception as e:
        logger.warning("Failed to fetch GEPRIS project %s: %s", project_id, e)
        return None

    soup = BeautifulSoup(html, "lxml")

    # Extract title
    title_el = soup.select_one("h1, .detail_head h3, #detailseite h1")
    title = title_el.get_text(strip=True) if title_el else f"GEPRIS Project {project_id}"

    # Extract details from the detail table
    details: dict[str, str] = {}
    for row in soup.select(".detail_content .intern dt, .detail_content dt"):
        key = row.get_text(strip=True).rstrip(":")
        dd = row.find_next_sibling("dd")
        if dd:
            details[key] = dd.get_text(strip=True)

    # Also try <span class="name"> pattern
    for span in soup.select("span.name"):
        key = span.get_text(strip=True).rstrip(":")
        value_el = span.find_next_sibling()
        if value_el:
            details[key] = value_el.get_text(strip=True)

    # Extract abstract/description
    abstract = None
    for section in soup.select(".abstract, #projektbeschreibung, .description"):
        abstract = section.get_text(strip=True)
        break

    # Parse funding amount
    total_funding = None
    for key in ["DFG Programme", "Funding", "Overall Funding", "Gesamtförderung"]:
        if key in details:
            amount_match = re.search(r"([\d.,]+)\s*(?:EUR|€)", details[key])
            if amount_match:
                amount_str = amount_match.group(1).replace(".", "").replace(",", ".")
                try:
                    total_funding = float(amount_str)
                except ValueError:
                    pass

    # Parse PI name
    pi_name = details.get("Applicant", details.get("Spokesperson", details.get("Antragsteller")))

    # Parse institution
    institution = details.get("Institution", details.get("Einrichtung"))

    # Parse dates
    term = details.get("Term", details.get("Förderung", ""))
    start_date = None
    end_date = None
    date_match = re.search(r"(\d{4})\s*(?:to|-)\s*(\d{4})", term)
    if date_match:
        try:
            start_date = date(int(date_match.group(1)), 1, 1)
            end_date = date(int(date_match.group(2)), 12, 31)
        except ValueError:
            pass

    # Determine funding scheme
    programme = details.get("DFG Programme", details.get("DFG-Verfahren", ""))

    keywords = []
    kw_text = details.get("Subject Area", details.get("Fachgebiet", ""))
    if kw_text:
        keywords = [k.strip() for k in kw_text.split(";") if k.strip()]

    return GrantAward(
        project_title=title,
        project_id=project_id,
        abstract=abstract,
        pi_name=pi_name,
        pi_institution=institution,
        pi_country="DE",
        start_date=start_date,
        end_date=end_date,
        total_funding=total_funding,
        status="active" if end_date and end_date >= date.today() else "completed",
        topic_keywords=keywords,
        source=SOURCE_ID,
        source_id=f"gepris_{project_id}",
    )


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    client: CachedHttpClient | None = None,
    keywords: list[str] | None = None,
    fetch_details: bool = True,
    max_detail_pages: int = 200,
) -> int:
    """Search GEPRIS and load matching projects into database.

    Returns total number of grants loaded.
    """
    if client is None:
        client = CachedHttpClient(
            cache_dir=os.path.join(CACHE_DIR, "gepris"),
            delay=2.5,  # Be respectful to GEPRIS servers
        )

    if keywords is None:
        keywords = QUANTUM_KEYWORDS

    # Collect unique project IDs from all keyword searches
    seen_ids: set[str] = set()
    all_results: list[dict] = []

    for kw in keywords:
        results = _search_projects(client, kw)
        for r in results:
            if r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                all_results.append(r)

    logger.info("Found %d unique GEPRIS projects across %d keyword searches",
                len(all_results), len(keywords))

    if not fetch_details:
        # Just load basic info from search results
        loaded = 0
        for r in all_results:
            grant = GrantAward(
                project_title=r["title"],
                project_id=r["id"],
                pi_country="DE",
                source=SOURCE_ID,
                source_id=f"gepris_{r['id']}",
            )
            upsert_grant(conn, grant)
            loaded += 1
        update_data_source(conn, SOURCE_ID, "DFG GEPRIS", loaded, status="ok")
        return loaded

    # Fetch detail pages (with limit to be respectful)
    loaded = 0
    for r in all_results[:max_detail_pages]:
        grant = _fetch_project_detail(client, r["id"])
        if grant:
            upsert_grant(conn, grant)
            loaded += 1

    update_data_source(conn, SOURCE_ID, "DFG GEPRIS", loaded, status="ok")
    return loaded
