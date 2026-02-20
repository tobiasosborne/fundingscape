"""CORDIS bulk CSV ingestion for Horizon Europe and H2020 projects."""

from __future__ import annotations

import csv
import io
import logging
import os
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb

from fundingscape import CACHE_DIR
from fundingscape.cache import CachedHttpClient
from fundingscape.db import upsert_grant, update_data_source
from fundingscape.models import GrantAward

logger = logging.getLogger(__name__)

CORDIS_URLS = {
    "horizon": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
    "h2020": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
}

SOURCE_ID = "cordis_bulk"


def _parse_date(s: str) -> date | None:
    """Parse CORDIS date string (YYYY-MM-DD)."""
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _parse_decimal(s: str) -> Decimal | None:
    """Parse CORDIS numeric string to Decimal."""
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_status(s: str) -> str | None:
    """Map CORDIS status to our model."""
    mapping = {
        "SIGNED": "active",
        "TERMINATED": "terminated",
        "CLOSED": "completed",
    }
    return mapping.get(s)


def _extract_csv_from_zip(zip_path: str | Path, csv_name: str) -> str:
    """Extract a CSV file from a zip and return its text content."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_name) as f:
            return f.read().decode("utf-8")


def _parse_projects_csv(csv_text: str, framework: str) -> list[GrantAward]:
    """Parse CORDIS project.csv into GrantAward models."""
    grants = []
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    for row in reader:
        project_id = row.get("id", "")
        if not project_id:
            continue

        keywords_raw = row.get("keywords", "")
        keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()] if keywords_raw else []

        # Include the call topic (e.g. "ERC-2023-STG") and funding scheme
        topics = row.get("topics", "")
        if topics:
            keywords.append(topics)
        funding_scheme = row.get("fundingScheme", "")
        if funding_scheme:
            keywords.append(funding_scheme)

        grant = GrantAward(
            project_title=row.get("title", "Unknown"),
            project_id=project_id,
            acronym=row.get("acronym"),
            abstract=row.get("objective"),
            start_date=_parse_date(row.get("startDate", "")),
            end_date=_parse_date(row.get("endDate", "")),
            total_funding=_parse_decimal(row.get("totalCost", "")),
            eu_contribution=_parse_decimal(row.get("ecMaxContribution", "")),
            status=_parse_status(row.get("status", "")),
            topic_keywords=keywords,
            source=SOURCE_ID,
            source_id=f"{framework}_{project_id}",
        )
        grants.append(grant)

    return grants


def _parse_organizations_csv(csv_text: str) -> dict[str, dict]:
    """Parse organization.csv to extract coordinator info per project.

    Returns {project_id: {pi_name: ..., pi_institution: ..., pi_country: ...}}
    """
    coordinators: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    for row in reader:
        if row.get("role") == "coordinator":
            proj_id = row.get("projectID", "")
            if proj_id:
                coordinators[proj_id] = {
                    "pi_institution": row.get("name", ""),
                    "pi_country": row.get("country", ""),
                }
    return coordinators


def _enrich_with_organizations(
    grants: list[GrantAward], coordinators: dict[str, dict],
) -> None:
    """Add PI/institution info from organization data."""
    for grant in grants:
        proj_id = grant.project_id
        if proj_id and proj_id in coordinators:
            info = coordinators[proj_id]
            grant.pi_institution = info.get("pi_institution")
            grant.pi_country = info.get("pi_country")


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    client: CachedHttpClient | None = None,
    frameworks: list[str] | None = None,
) -> int:
    """Fetch CORDIS bulk data and load into database.

    Returns the total number of grants loaded.
    """
    if client is None:
        client = CachedHttpClient(
            cache_dir=os.path.join(CACHE_DIR, "cordis"),
            delay=0.0,  # bulk downloads don't need rate limiting
        )

    frameworks = frameworks or list(CORDIS_URLS.keys())
    total = 0

    for fw in frameworks:
        url = CORDIS_URLS.get(fw)
        if not url:
            logger.warning("Unknown framework: %s", fw)
            continue

        logger.info("Fetching CORDIS %s bulk data from %s", fw, url)
        try:
            entry = client.fetch(url)
        except Exception as e:
            logger.error("Failed to fetch CORDIS %s: %s", fw, e)
            update_data_source(
                conn, f"{SOURCE_ID}_{fw}", f"CORDIS {fw.upper()}", 0,
                status="error", error=str(e),
            )
            continue

        # Write zip to known location for extraction
        zip_path = Path(client.cache_dir) / f"{fw}-projects.zip"
        zip_path.write_bytes(entry.body)

        # Parse projects
        try:
            project_csv = _extract_csv_from_zip(zip_path, "project.csv")
            grants = _parse_projects_csv(project_csv, fw)
            logger.info("Parsed %d %s projects", len(grants), fw)
        except Exception as e:
            logger.error("Failed to parse CORDIS %s projects: %s", fw, e)
            update_data_source(
                conn, f"{SOURCE_ID}_{fw}", f"CORDIS {fw.upper()}", 0,
                status="error", error=str(e),
            )
            continue

        # Parse organizations for coordinator info
        try:
            org_csv = _extract_csv_from_zip(zip_path, "organization.csv")
            coordinators = _parse_organizations_csv(org_csv)
            _enrich_with_organizations(grants, coordinators)
            logger.info("Enriched with %d coordinator records", len(coordinators))
        except Exception as e:
            logger.warning("Could not parse organizations: %s", e)

        # Load into database
        for grant in grants:
            upsert_grant(conn, grant)
        total += len(grants)

        update_data_source(
            conn, f"{SOURCE_ID}_{fw}", f"CORDIS {fw.upper()}", len(grants),
            status="ok", etag=entry.etag, last_modified=entry.last_modified,
        )

    return total
