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
    hits_per_page: int = 50,
    max_pages: int = 20,
) -> list[dict]:
    """Search GEPRIS for projects matching a keyword.

    Paginates through results using hitsPerPage and index parameters.
    Returns list of {id, title} dicts.
    """
    all_results = []
    index = 0

    while True:
        if index == 0:
            url = (
                f"{SEARCH_URL}?task=doSearchSimple&context=projekt"
                f"&keywords_criterion={keyword}"
                f"&hitsPerPage={hits_per_page}"
                f"&language=en"
            )
        else:
            url = (
                f"{SEARCH_URL}?context=projekt"
                f"&findButton=historyCall"
                f"&keywords_criterion={keyword}"
                f"&hitsPerPage={hits_per_page}"
                f"&index={index}"
                f"&language=en"
            )

        try:
            html = client.fetch_text(url)
        except Exception as e:
            logger.error("GEPRIS search failed for '%s' at index %d: %s", keyword, index, e)
            break

        soup = BeautifulSoup(html, "html.parser")
        page_results = []

        # Parse search results — each project is in a div.results > h2 > a
        for item in soup.select("div.results"):
            link = item.select_one("h2 a[href*='/gepris/projekt/']")
            if not link:
                continue

            href = link.get("href", "")
            match = re.search(r"/gepris/projekt/(\d+)", href)
            if not match:
                continue

            project_id = match.group(1)
            # Strip image alt text from title
            title = link.get_text(strip=True)

            page_results.append({
                "id": project_id,
                "title": title,
            })

        all_results.extend(page_results)

        # Stop if no results on this page or we've hit max pages
        if len(page_results) < hits_per_page or len(all_results) // hits_per_page >= max_pages:
            break

        index += hits_per_page

    logger.info("GEPRIS search '%s': found %d results", keyword, len(all_results))
    return all_results


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

    soup = BeautifulSoup(html, "html.parser")

    # Extract title — h1.facelift is the project title on GEPRIS detail pages.
    # Try most specific selector first, then fall back.
    title_el = (
        soup.select_one("h1.facelift")
        or soup.select_one("#detailseite h1:not(.hidden)")
        or soup.select_one(".detail_head h3")
    )
    title = title_el.get_text(strip=True) if title_el else f"GEPRIS Project {project_id}"

    # Extract details — GEPRIS uses <span class="name"> / sibling pairs
    details: dict[str, str] = {}
    for span in soup.select("span.name"):
        key = span.get_text(strip=True).rstrip(":")
        value_el = span.find_next_sibling()
        if value_el:
            details[key] = value_el.get_text(strip=True)

    # Also try dt/dd pattern (older GEPRIS pages)
    for row in soup.select(".detail_content .intern dt, .detail_content dt"):
        key = row.get_text(strip=True).rstrip(":")
        dd = row.find_next_sibling("dd")
        if dd and key not in details:
            details[key] = dd.get_text(strip=True)

    # Extract abstract/description from content_frame
    abstract = None
    for section in soup.select(".content_frame, .abstract, #projektbeschreibung, .description"):
        text = section.get_text(strip=True)
        # Skip very short sections (nav labels, tab headers)
        if len(text) > 50:
            abstract = text
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

    # Parse PI name — try multiple field names
    pi_name = (
        details.get("Applicant")
        or details.get("Spokesperson")
        or details.get("Spokespersons")
        or details.get("Antragsteller")
        or details.get("Sprecher")
    )
    # Clean up "since/until" annotations from spokesperson fields
    if pi_name and ";" in pi_name:
        pi_name = pi_name.split(";")[0].strip()
    if pi_name and ", since " in pi_name:
        pi_name = pi_name.split(", since ")[0].strip()
    if pi_name and ", until " in pi_name:
        pi_name = pi_name.split(", until ")[0].strip()

    # Parse institution — try multiple field names (in priority order)
    institution = (
        details.get("Applicant Institution")
        or details.get("Institution")
        or details.get("Einrichtung")
        or details.get("Antragstellende Institution")
        or details.get("Co-Applicant Institution")
        or details.get("Host")
        or details.get("Participating Institution")
        or details.get("Participating University")
        or details.get("Partner Organisation")
    )

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
