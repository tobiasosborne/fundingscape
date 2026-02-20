"""OpenAIRE API integration for cross-funder grant metadata.

OpenAIRE aggregates grants from 100K+ funders worldwide including
DFG, UKRI, NSF, SNSF, ANR, FWF, NWO, ARC, and EC (CORDIS).
API docs: https://api.openaire.eu/
No authentication required for basic access.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import duckdb
import httpx

from fundingscape import CACHE_DIR
from fundingscape.cache import CachedHttpClient
from fundingscape.db import upsert_grant, update_data_source
from fundingscape.models import GrantAward

logger = logging.getLogger(__name__)

SOURCE_ID = "openaire"
BASE_URL = "https://api.openaire.eu/search/projects"
PAGE_SIZE = 100  # max allowed by API

# Funders to query (OpenAIRE funder shortnames)
# EC is excluded since we already have CORDIS data
FUNDERS = [
    "DFG",    # Deutsche Forschungsgemeinschaft
    "UKRI",   # UK Research and Innovation (EPSRC, STFC, etc.)
    "NSF",    # US National Science Foundation
    "SNSF",   # Swiss National Science Foundation
    "ANR",    # Agence Nationale de la Recherche (France)
    "FWF",    # Austrian Science Fund
    "NWO",    # Dutch Research Council
    "ARC",    # Australian Research Council
    "FCT",    # Portuguese Foundation for Science and Technology
    "SFI",    # Science Foundation Ireland
]

# Keywords for quantum/deep-tech research
QUANTUM_KEYWORDS = [
    "quantum computing",
    "quantum information",
    "quantum technology",
    "quantum entanglement",
    "topological quantum",
    "many-body quantum",
    "quantum error correction",
    "quantum simulation",
    "quantum sensing",
    "quantum communication",
    "quantum cryptography",
    "quantum algorithms",
]

# Broader deep-tech keywords
DEEPTECH_KEYWORDS = [
    "quantum",
    "photonics",
    "superconducting",
    "cryogenic",
    "topological",
]


def _parse_project(result: dict) -> GrantAward | None:
    """Parse a single OpenAIRE project result into a GrantAward."""
    try:
        entity = result["metadata"]["oaf:entity"]["oaf:project"]
    except (KeyError, TypeError):
        return None

    project_id = entity.get("code", {}).get("$", "")
    title = entity.get("title", {}).get("$", "")
    if not title:
        return None

    # Extract funder info
    collected_from = entity.get("collectedfrom", {})
    funder_name = collected_from.get("@name", "")

    # Funding tree contains funder hierarchy
    funding_tree = entity.get("fundingtree", {})
    funder_short = ""
    if isinstance(funding_tree, dict):
        funder_info = funding_tree.get("funder", {})
        funder_short = funder_info.get("shortname", {}).get("$", "")
    elif isinstance(funding_tree, list) and funding_tree:
        funder_info = funding_tree[0].get("funder", {})
        funder_short = funder_info.get("shortname", {}).get("$", "")

    # Parse dates
    start_str = entity.get("startdate", {}).get("$", "")
    end_str = entity.get("enddate", {}).get("$", "")
    start_date = _parse_date(start_str)
    end_date = _parse_date(end_str)

    # Parse funding amount
    funded_amount = entity.get("fundedamount", {}).get("$")
    total_funding = None
    if funded_amount and funded_amount != 0.0:
        try:
            total_funding = Decimal(str(funded_amount))
        except (InvalidOperation, ValueError):
            pass

    total_cost = entity.get("totalcost", {}).get("$")
    if not total_funding and total_cost and total_cost != "0.0":
        try:
            total_funding = Decimal(str(total_cost))
        except (InvalidOperation, ValueError):
            pass

    currency = entity.get("currency", {}).get("$", "EUR")

    # Abstract
    abstract = entity.get("summary", {}).get("$", "")

    # OpenAIRE internal ID for dedup
    obj_id = result.get("header", {}).get("dri:objIdentifier", {}).get("$", "")

    # Determine status
    status = "completed"
    if end_date and end_date >= date.today():
        status = "active"
    elif not end_date and start_date:
        status = "active"

    # Source ID uses funder prefix for uniqueness
    source_id = f"openaire_{funder_short}_{project_id}" if funder_short else f"openaire_{obj_id[:20]}"

    return GrantAward(
        project_title=title,
        project_id=project_id,
        abstract=abstract if len(abstract) < 10000 else abstract[:10000],
        pi_country=_funder_to_country(funder_short),
        start_date=start_date,
        end_date=end_date,
        total_funding=total_funding,
        currency=currency if currency else "EUR",
        status=status,
        topic_keywords=[funder_short, funder_name] if funder_short else [],
        source=SOURCE_ID,
        source_id=source_id,
    )


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _funder_to_country(funder: str) -> str | None:
    """Map funder shortname to country code."""
    mapping = {
        "DFG": "DE", "BMBF": "DE",
        "UKRI": "GB", "EPSRC": "GB",
        "NSF": "US", "DOE": "US", "NIH": "US",
        "SNSF": "CH",
        "ANR": "FR",
        "FWF": "AT",
        "NWO": "NL",
        "ARC": "AU",
        "FCT": "PT",
        "SFI": "IE",
        "EC": "EU",
    }
    return mapping.get(funder)


def _fetch_page(
    keywords: str,
    funder: str | None = None,
    page: int = 1,
    size: int = PAGE_SIZE,
) -> dict:
    """Fetch a single page from the OpenAIRE API."""
    params = {
        "keywords": keywords,
        "format": "json",
        "size": size,
        "page": page,
    }
    if funder:
        params["funder"] = funder

    resp = httpx.get(BASE_URL, params=params, timeout=30.0, follow_redirects=True)
    resp.raise_for_status()
    return resp.json()


def fetch_grants_for_funder(
    funder: str,
    keywords: list[str] | None = None,
    max_pages: int = 50,
    delay: float = 1.0,
) -> list[GrantAward]:
    """Fetch all quantum-related grants for a specific funder.

    Returns list of parsed GrantAward objects.
    """
    if keywords is None:
        keywords = DEEPTECH_KEYWORDS

    all_grants: dict[str, GrantAward] = {}  # deduplicate by source_id

    for kw in keywords:
        page = 1
        while page <= max_pages:
            try:
                data = _fetch_page(kw, funder=funder, page=page)
            except Exception as e:
                logger.warning("OpenAIRE fetch failed for %s/%s page %d: %s",
                             funder, kw, page, e)
                break

            header = data.get("response", {}).get("header", {})
            total = int(header.get("total", {}).get("$", 0))
            results = data.get("response", {}).get("results", {})

            if not results or "result" not in results:
                break

            result_list = results["result"]
            if not isinstance(result_list, list):
                result_list = [result_list]

            for r in result_list:
                grant = _parse_project(r)
                if grant and grant.source_id not in all_grants:
                    all_grants[grant.source_id] = grant

            logger.debug("OpenAIRE %s/%s: page %d, got %d results (total: %d)",
                        funder, kw, page, len(result_list), total)

            # Check if we've fetched all pages
            if page * PAGE_SIZE >= total:
                break
            page += 1
            time.sleep(delay)

    logger.info("OpenAIRE %s: fetched %d unique grants across %d keywords",
               funder, len(all_grants), len(keywords))
    return list(all_grants.values())


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    funders: list[str] | None = None,
    keywords: list[str] | None = None,
    max_pages_per_funder: int = 50,
    delay: float = 1.0,
) -> int:
    """Fetch grants from OpenAIRE for all configured funders and load into DB.

    Returns total number of grants loaded.
    """
    if funders is None:
        funders = FUNDERS
    if keywords is None:
        keywords = DEEPTECH_KEYWORDS

    total_loaded = 0

    for funder in funders:
        logger.info("Fetching OpenAIRE grants for funder: %s", funder)
        try:
            grants = fetch_grants_for_funder(
                funder, keywords=keywords,
                max_pages=max_pages_per_funder,
                delay=delay,
            )
        except Exception as e:
            logger.error("OpenAIRE %s failed: %s", funder, e)
            continue

        for grant in grants:
            upsert_grant(conn, grant)
        total_loaded += len(grants)

        logger.info("OpenAIRE %s: loaded %d grants into DB", funder, len(grants))

    update_data_source(conn, SOURCE_ID, "OpenAIRE", total_loaded, status="ok")
    return total_loaded
