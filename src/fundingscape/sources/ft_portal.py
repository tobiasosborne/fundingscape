"""EU Funding & Tenders Portal ingestion from the JSON data dump."""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime
from decimal import Decimal

import duckdb

from fundingscape import CACHE_DIR
from fundingscape.cache import CachedHttpClient
from fundingscape.db import upsert_call, update_data_source
from fundingscape.models import Call

logger = logging.getLogger(__name__)

FT_URL = "https://ec.europa.eu/info/funding-tenders/opportunities/data/referenceData/grantsTenders.json"
SOURCE_ID = "ft_portal"

# Framework programmes relevant to quantum research
RELEVANT_PROGRAMMES = {
    "HORIZON", "HE", "H2020", "ERC", "MSCA", "EIC",
    "EURATOM", "COST", "ERASMUS+",
}


def _epoch_ms_to_date(ms: int | None) -> date | None:
    """Convert Unix epoch milliseconds to date."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=UTC).date()
    except (ValueError, OSError):
        return None


def _map_status(status_obj: dict | None) -> str:
    """Map F&T Portal status object to our status enum."""
    if not status_obj:
        return "closed"
    abbr = status_obj.get("abbreviation", "").lower()
    mapping = {
        "open": "open",
        "closed": "closed",
        "forthcoming": "forthcoming",
        "under evaluation": "under_evaluation",
    }
    return mapping.get(abbr, "closed")


def _extract_tags(entry: dict) -> list[str]:
    """Extract topic keywords from tags field."""
    tags = entry.get("tags", [])
    if isinstance(tags, list):
        return [t for t in tags if isinstance(t, str)]
    return []


def parse_calls(data: dict) -> list[Call]:
    """Parse the grantsTenders.json into Call models."""
    calls = []
    entries = data.get("fundingData", {}).get("GrantTenderObj", [])
    logger.info("Total entries in F&T Portal JSON: %d", len(entries))

    for entry in entries:
        # Extract framework programme
        fp = entry.get("frameworkProgramme", {})
        fp_abbr = fp.get("abbreviation", "") if fp else ""

        # Get earliest deadline
        deadlines = entry.get("deadlineDatesLong", [])
        deadline = _epoch_ms_to_date(deadlines[0]) if deadlines else None

        # Budget info
        budget = None
        actions = entry.get("actions", [])
        if actions:
            for action in actions:
                # Some actions have budget info
                pass  # Budget not always at topic level

        call = Call(
            call_identifier=entry.get("identifier", ""),
            title=entry.get("title", "Untitled"),
            description=entry.get("callTitle"),
            url=f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{entry.get('identifier', '')}",
            opening_date=_epoch_ms_to_date(entry.get("plannedOpeningDateLong")),
            deadline=deadline,
            status=_map_status(entry.get("status")),
            budget_total=None,
            topic_keywords=_extract_tags(entry),
            framework_programme=fp_abbr,
            programme_division=None,
            source=SOURCE_ID,
            source_id=str(entry.get("ccm2Id", entry.get("identifier", ""))),
            raw_data=entry,
        )
        calls.append(call)

    return calls


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    client: CachedHttpClient | None = None,
    filter_programmes: set[str] | None = None,
) -> int:
    """Fetch the F&T Portal JSON and load calls into database.

    Returns the number of calls loaded.
    """
    if client is None:
        client = CachedHttpClient(
            cache_dir=os.path.join(CACHE_DIR, "ft_portal"),
            delay=0.0,
        )
    if filter_programmes is None:
        filter_programmes = RELEVANT_PROGRAMMES

    logger.info("Fetching F&T Portal data from %s", FT_URL)
    try:
        data = client.fetch_json(FT_URL)
    except Exception as e:
        logger.error("Failed to fetch F&T Portal: %s", e)
        update_data_source(conn, SOURCE_ID, "EU F&T Portal", 0,
                          status="error", error=str(e))
        raise

    all_calls = parse_calls(data)
    logger.info("Parsed %d total calls", len(all_calls))

    # Filter to relevant programmes
    if filter_programmes:
        # Match if the framework programme starts with any relevant prefix
        filtered = []
        for c in all_calls:
            fp = c.framework_programme or ""
            if any(fp.startswith(rp) for rp in filter_programmes):
                filtered.append(c)
        logger.info("Filtered to %d calls in relevant programmes", len(filtered))
    else:
        filtered = all_calls

    # Load into database
    for call in filtered:
        upsert_call(conn, call)

    update_data_source(conn, SOURCE_ID, "EU F&T Portal", len(filtered), status="ok")
    return len(filtered)
