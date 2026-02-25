"""BMBF Förderkatalog scraper for German federal research projects.

Scrapes the Förderkatalog at foerderportal.bund.de, a JSP-based system
with POST form submission and session cookies. Contains 260K+ projects
from five federal ministries (BMBF, BMWK, BMDV, BMU, BMEL).

Uses httpx.Client directly (not CachedHttpClient) because:
- Search requires POST form submission with jsessionid cookies
- POST results don't fit ETag-based caching
- Detail pages are fetched within a session context
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import date
from pathlib import Path

import duckdb
import httpx
from bs4 import BeautifulSoup

from fundingscape import CACHE_DIR
from fundingscape.db import upsert_grant, update_data_source
from fundingscape.models import GrantAward

logger = logging.getLogger(__name__)

SOURCE_ID = "foerderkatalog"
FOEKAT_BASE = "https://foerderportal.bund.de/foekat/jsp"
SEARCH_URL = f"{FOEKAT_BASE}/SucheAction.do"

REQUEST_DELAY = 2.5  # seconds between requests

QUANTUM_KEYWORDS = [
    "%Quantencomputer%",
    "%Quantentechnologie%",
    "%Quanteninformation%",
    "%Quantensensorik%",
    "%Quantenkommunikation%",
    "%quantum computing%",
    "%Quantensimulation%",
]

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
}


def _parse_german_date(s: str) -> date | None:
    """Parse German date string (DD.MM.YYYY) to Python date."""
    if not s:
        return None
    match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", s.strip())
    if match:
        try:
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
        except ValueError:
            return None
    return None


def _parse_german_amount(s: str) -> float | None:
    """Parse German currency amount (e.g., '1.250.000,00 €') to float."""
    if not s:
        return None
    # Remove currency symbol and whitespace
    cleaned = re.sub(r"\s*[€$]\s*", "", s).strip()
    # Remove EUR/Euro text
    cleaned = re.sub(r"\s*(EUR|Euro)\s*", "", cleaned, flags=re.IGNORECASE).strip()
    if not cleaned:
        return None
    # German format: period=thousands separator, comma=decimal
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _create_session() -> httpx.Client:
    """Create an HTTP session with browser-like headers."""
    return httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        headers=_BROWSER_HEADERS,
    )


def _init_session(session: httpx.Client) -> str:
    """GET the search page to establish jsessionid. Returns session ID."""
    resp = session.get(f"{SEARCH_URL}?actionMode=searchmask")
    resp.raise_for_status()
    return session.cookies.get("JSESSIONID", "")


def _submit_search(session: httpx.Client, keyword: str, jsessionid: str) -> str:
    """Submit a search form and return the results HTML."""
    resp = session.post(
        f"{SEARCH_URL};jsessionid={jsessionid}",
        data={
            "actionMode": "searchlist",
            "suche.detailSuche": "true",
            "suche.themaSuche[0]": keyword,
            "suche.lfdVhb": "N",  # Include completed projects (default J = only running)
            "submitAction": "Detailsuche starten",
        },
    )
    resp.raise_for_status()
    return resp.text


def _parse_total_count(html: str) -> int:
    """Extract total result count from search results page.

    The count appears as "(132&nbsp;Treffer)" or "(132 Treffer)".
    """
    # Replace &nbsp; and \xa0 with regular space for matching
    normalized = html.replace("\xa0", " ").replace("&nbsp;", " ")
    match = re.search(r"\((\d+)\s*Treffer\)", normalized)
    return int(match.group(1)) if match else 0


def _fetch_results_page(
    session: httpx.Client,
    jsessionid: str,
    row_from: int,
    rows_per_page: int = 100,
) -> str:
    """Fetch a specific page of results by posting pagination form."""
    resp = session.post(
        f"{SEARCH_URL};jsessionid={jsessionid}",
        data={
            "suche.listrowfrom": str(row_from),
            "suche.listrowpersite": str(rows_per_page),
            "suche.orderby": "1",
            "suche.order": "asc",
        },
    )
    resp.raise_for_status()
    return resp.text


def _parse_search_results(html: str) -> list[dict]:
    """Parse Förderkatalog search results table.

    Returns list of dicts with keys:
      fkz, ministry, institution, executing_institution, title,
      start_date, end_date, total_funding, is_verbund
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", attrs={"aria-label": "Suchergebnis"})
    if not table:
        return []

    results = []
    for tr in table.find_all("tr")[1:]:  # Skip header row
        cells = tr.find_all("td")
        if len(cells) < 7:
            continue

        fkz = cells[0].get_text(strip=True)
        if not fkz:
            continue

        # Parse dates — concatenated as "DD.MM.YYYYDD.MM.YYYY"
        date_text = cells[5].get_text(strip=True)
        start_date = _parse_german_date(date_text[:10]) if len(date_text) >= 10 else None
        end_date = _parse_german_date(date_text[10:]) if len(date_text) >= 20 else None

        results.append({
            "fkz": fkz,
            "ministry": cells[1].get_text(strip=True),
            "institution": cells[2].get_text(strip=True),
            "executing_institution": cells[3].get_text(strip=True),
            "title": cells[4].get_text(strip=True),
            "start_date": start_date,
            "end_date": end_date,
            "total_funding": _parse_german_amount(cells[6].get_text(strip=True)),
            "is_verbund": cells[7].get_text(strip=True) == "J" if len(cells) > 7 else False,
        })

    return results


def _fetch_project_detail(
    session: httpx.Client,
    fkz: str,
    jsessionid: str,
    cache_dir: str,
) -> dict | None:
    """Fetch and parse a single project detail page.

    Returns dict with additional fields (abstract, PI, etc.) or None on error.
    Detail pages are cached to filesystem to avoid refetching.
    """
    cache_path = Path(cache_dir) / f"{hashlib.sha256(fkz.encode()).hexdigest()[:16]}.html"

    if cache_path.exists():
        html = cache_path.read_text(encoding="utf-8", errors="replace")
    else:
        url = f"{SEARCH_URL};jsessionid={jsessionid}?actionMode=view&fkz={fkz}"
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(url)
            resp.raise_for_status()
            html = resp.text
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(html, encoding="utf-8")
        except Exception as e:
            logger.warning("Failed to fetch Förderkatalog detail for %s: %s", fkz, e)
            return None

    return _parse_detail_page(html, fkz)


def _parse_detail_page(html: str, fkz: str) -> dict | None:
    """Parse a Förderkatalog detail page.

    Returns dict with fields: title, abstract, topic, ministry,
    institution, executing_institution, start_date, end_date,
    total_funding, leistungsplan, foerderart.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Extract key-value pairs from the detail page
    # The structure is: label text followed by value text in adjacent elements
    details: dict[str, str] = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).rstrip(":")
                value = cells[1].get_text(strip=True)
                if key and value:
                    details[key] = value

    # Also extract from text blocks (detail pages use various formats)
    text = soup.get_text()
    if not details:
        # Try line-by-line parsing
        for line in text.split("\n"):
            line = line.strip()
            if ":" in line and len(line) < 200:
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                if key and value:
                    details[key] = value

    return details if details else None


def _result_to_grant(result: dict, detail: dict | None = None) -> GrantAward:
    """Convert a search result (+ optional detail page data) to GrantAward."""
    fkz = result["fkz"]
    title = result["title"]

    # Use detail page for richer title if available
    if detail:
        full_title = detail.get(
            "Thema des geförderten Vorhabens",
            detail.get("Thema", ""),
        )
        if full_title and len(full_title) > len(title):
            title = full_title

    # Get abstract from detail page
    abstract = None
    if detail:
        abstract = detail.get("Kurzbeschreibung", detail.get("Projektbeschreibung"))

    # Compute status from dates
    status = "completed"
    if result["end_date"] and result["end_date"] >= date.today():
        status = "active"

    # Build topic keywords from Leistungsplansystematik
    keywords = []
    if detail:
        lp = detail.get("Leistungsplansystematik", "")
        if lp:
            keywords = [k.strip() for k in lp.split(":") if k.strip()]

    return GrantAward(
        project_title=title,
        project_id=fkz,
        abstract=abstract,
        pi_name=None,  # Förderkatalog doesn't list PI names
        pi_institution=result.get("executing_institution") or result.get("institution"),
        pi_country="DE",
        start_date=result["start_date"],
        end_date=result["end_date"],
        total_funding=result["total_funding"],
        currency="EUR",
        status=status,
        topic_keywords=keywords,
        source=SOURCE_ID,
        source_id=f"foekat_{fkz}",
    )


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    keywords: list[str] | None = None,
    max_results: int = 5000,
    fetch_details: bool = False,
    max_detail_pages: int = 500,
) -> int:
    """Search Förderkatalog and load matching projects into database.

    The search results table already contains most useful fields
    (FKZ, institution, title, dates, funding amount), so detail page
    fetching is optional and off by default.

    Returns total number of grants loaded.
    """
    if keywords is None:
        keywords = QUANTUM_KEYWORDS

    cache_dir = os.path.join(CACHE_DIR, "foerderkatalog")
    os.makedirs(cache_dir, exist_ok=True)

    session = _create_session()
    try:
        jsessionid = _init_session(session)
    except Exception as e:
        logger.error("Failed to initialize Förderkatalog session: %s", e)
        update_data_source(
            conn, SOURCE_ID, "BMBF Förderkatalog", 0,
            status="error",
        )
        return 0

    # Collect unique projects from all keyword searches
    seen_fkzs: set[str] = set()
    all_results: list[dict] = []

    for kw in keywords:
        logger.info("Searching Förderkatalog for '%s'", kw)
        time.sleep(REQUEST_DELAY)

        try:
            html = _submit_search(session, kw, jsessionid)
        except Exception as e:
            logger.warning("Förderkatalog search failed for '%s': %s", kw, e)
            continue

        total_count = _parse_total_count(html)
        logger.info("Förderkatalog '%s': %d total results", kw, total_count)

        if total_count == 0:
            continue

        # Parse first page of results
        page_results = _parse_search_results(html)

        # Paginate if needed (100 per page)
        rows_per_page = 100
        if total_count > len(page_results):
            # Re-fetch with 100 per page
            time.sleep(REQUEST_DELAY)
            html = _fetch_results_page(session, jsessionid, 1, rows_per_page)
            page_results = _parse_search_results(html)

            # Fetch remaining pages
            fetched = len(page_results)
            while fetched < total_count and fetched < max_results:
                time.sleep(REQUEST_DELAY)
                html = _fetch_results_page(
                    session, jsessionid, fetched + 1, rows_per_page,
                )
                next_page = _parse_search_results(html)
                if not next_page:
                    break
                page_results.extend(next_page)
                fetched += len(next_page)

        # Deduplicate across keyword searches
        for r in page_results:
            if r["fkz"] not in seen_fkzs:
                seen_fkzs.add(r["fkz"])
                all_results.append(r)

    logger.info(
        "Found %d unique Förderkatalog projects across %d keywords",
        len(all_results), len(keywords),
    )

    # Optionally fetch detail pages for richer metadata
    details_map: dict[str, dict] = {}
    if fetch_details:
        for r in all_results[:max_detail_pages]:
            detail = _fetch_project_detail(
                session, r["fkz"], jsessionid, cache_dir,
            )
            if detail:
                details_map[r["fkz"]] = detail

    # Load into database
    loaded = 0
    for r in all_results:
        grant = _result_to_grant(r, details_map.get(r["fkz"]))
        upsert_grant(conn, grant)
        loaded += 1

    session.close()
    update_data_source(conn, SOURCE_ID, "BMBF Förderkatalog", loaded, status="ok")
    return loaded
