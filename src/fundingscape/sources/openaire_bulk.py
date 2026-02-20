"""OpenAIRE bulk data loader from Zenodo project dump.

Downloads and parses the full OpenAIRE Graph project.tar (~620 MB)
containing ALL projects from ALL funders (~3.8M records).

Uses DuckDB batch loading for performance instead of row-by-row upserts.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import tarfile
import tempfile
import time
from datetime import date
from pathlib import Path

import duckdb

from fundingscape import CACHE_DIR
from fundingscape.db import create_tables, update_data_source

logger = logging.getLogger(__name__)

SOURCE_ID = "openaire_bulk"
ZENODO_URL = "https://zenodo.org/api/records/17725827/files/project.tar/content"
TAR_PATH = os.path.join(CACHE_DIR, "openaire", "project.tar")


def _parse_date(s: str | None) -> str | None:
    """Return ISO date string or None."""
    if not s:
        return None
    try:
        date.fromisoformat(s[:10])
        return s[:10]
    except ValueError:
        return None


def _extract_to_csv(tar_path: str, csv_path: str) -> int:
    """Extract the tar into a flat CSV for DuckDB bulk loading.

    Returns the number of records written.
    """
    count = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            "source_id", "project_title", "project_id", "acronym",
            "pi_country", "start_date", "end_date",
            "total_funding", "currency", "status",
            "funder_short", "funder_name", "funding_stream",
            "keywords",
        ])

        with tarfile.open(tar_path, "r") as tar:
            for member in sorted(tar.getmembers(), key=lambda m: m.name):
                if not member.name.endswith(".gz"):
                    continue
                logger.info("Processing %s", member.name)
                f = tar.extractfile(member)
                if not f:
                    continue

                with gzip.open(f, "rt", encoding="utf-8") as gz:
                    for line in gz:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            proj = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        title = proj.get("title")
                        if not title or title == "unidentified":
                            continue

                        project_id = proj.get("code", "")
                        openaire_id = proj.get("id", "")
                        fundings = proj.get("fundings") or []
                        granted = proj.get("granted") or {}

                        # Funder info
                        funder_short = ""
                        funder_name = ""
                        funding_stream = ""
                        country = None
                        for fund in fundings:
                            funder_short = fund.get("shortName", "") or ""
                            funder_name = fund.get("name", "") or ""
                            country = fund.get("jurisdiction")
                            stream = fund.get("fundingStream") or {}
                            funding_stream = stream.get("description", "") or ""
                            break  # Use first funder

                        # Funding amount
                        amount = None
                        currency = granted.get("currency", "EUR") or "EUR"
                        funded = granted.get("fundedAmount")
                        if funded and float(funded) > 0:
                            amount = float(funded)
                        else:
                            total = granted.get("totalCost")
                            if total and float(total) > 0:
                                amount = float(total)

                        # Dates
                        start_date = _parse_date(proj.get("startDate"))
                        end_date = _parse_date(proj.get("endDate"))

                        # Status
                        status = "completed"
                        if end_date:
                            try:
                                if date.fromisoformat(end_date) >= date.today():
                                    status = "active"
                            except ValueError:
                                pass
                        elif start_date:
                            try:
                                if date.fromisoformat(start_date).year >= 2023:
                                    status = "active"
                            except ValueError:
                                pass

                        # Keywords
                        kw_str = proj.get("keywords") or ""
                        keywords = funder_short
                        if kw_str:
                            keywords = f"{funder_short};{kw_str[:200]}"

                        # Source ID
                        sid = f"oaire_{funder_short}_{project_id}" if project_id and project_id != "unidentified" else f"oaire_{openaire_id[:24]}"

                        # Sanitize text fields: remove tabs/newlines
                        clean_title = title.replace("\t", " ").replace("\n", " ").replace("\r", "")[:500]
                        clean_acronym = (proj.get("acronym") or "").replace("\t", " ")
                        clean_funder = funder_name.replace("\t", " ")[:200]
                        clean_stream = funding_stream.replace("\t", " ")[:200]
                        clean_kw = keywords.replace("\t", " ").replace("\n", " ")[:300]

                        writer.writerow([
                            sid, clean_title, project_id, clean_acronym,
                            country or "", start_date or "", end_date or "",
                            amount or "", currency, status,
                            funder_short, clean_funder, clean_stream,
                            clean_kw,
                        ])
                        count += 1

                        if count % 100000 == 0:
                            logger.info("Extracted %d records...", count)

    logger.info("Extracted %d total records to CSV", count)
    return count


def load_csv_to_db(
    conn: duckdb.DuckDBPyConnection,
    csv_path: str,
) -> int:
    """Bulk-load the extracted CSV into DuckDB.

    This is MUCH faster than row-by-row upserts.
    """
    # First, drop existing openaire_bulk data
    conn.execute("DELETE FROM grant_award WHERE source = ?", [SOURCE_ID])

    # Load via DuckDB's CSV reader directly into the table
    conn.execute(f"""
        INSERT INTO grant_award (
            id, project_title, project_id, acronym,
            pi_country, start_date, end_date,
            total_funding, currency, status,
            topic_keywords, source, source_id
        )
        SELECT
            nextval('seq_grant'),
            project_title,
            project_id,
            CASE WHEN acronym = '' THEN NULL ELSE acronym END,
            CASE WHEN pi_country = '' THEN NULL ELSE pi_country END,
            CASE WHEN start_date = '' THEN NULL ELSE CAST(start_date AS DATE) END,
            CASE WHEN end_date = '' THEN NULL ELSE CAST(end_date AS DATE) END,
            CASE WHEN total_funding = '' THEN NULL ELSE CAST(total_funding AS DOUBLE) END,
            currency,
            status,
            string_split(keywords, ';'),
            '{SOURCE_ID}',
            source_id
        FROM read_csv('{csv_path}',
            header=true,
            auto_detect=false,
            delim='\t',
            quote='',
            ignore_errors=true,
            columns={{
                'source_id': 'VARCHAR',
                'project_title': 'VARCHAR',
                'project_id': 'VARCHAR',
                'acronym': 'VARCHAR',
                'pi_country': 'VARCHAR',
                'start_date': 'VARCHAR',
                'end_date': 'VARCHAR',
                'total_funding': 'VARCHAR',
                'currency': 'VARCHAR',
                'status': 'VARCHAR',
                'funder_short': 'VARCHAR',
                'funder_name': 'VARCHAR',
                'funding_stream': 'VARCHAR',
                'keywords': 'VARCHAR'
            }}
        )
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE source = ?", [SOURCE_ID]
    ).fetchone()[0]
    return count


def fetch_and_load(
    conn: duckdb.DuckDBPyConnection,
    tar_path: str | None = None,
) -> int:
    """Extract OpenAIRE bulk data to CSV, then bulk-load into DuckDB.

    Returns total number of grants loaded.
    """
    tar_path = tar_path or TAR_PATH

    if not os.path.exists(tar_path):
        logger.info("Downloading OpenAIRE bulk data from Zenodo...")
        import httpx
        os.makedirs(os.path.dirname(tar_path), exist_ok=True)
        with httpx.stream("GET", ZENODO_URL, follow_redirects=True, timeout=600) as resp:
            resp.raise_for_status()
            with open(tar_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        logger.info("Downloaded %.1f MB", os.path.getsize(tar_path) / 1e6)

    # Extract to temporary CSV
    csv_path = tar_path.replace(".tar", "_staging.csv")
    logger.info("Extracting tar to staging CSV...")
    t0 = time.time()
    num_extracted = _extract_to_csv(tar_path, csv_path)
    t1 = time.time()
    logger.info("Extraction: %d records in %.1f sec", num_extracted, t1 - t0)

    # Bulk load into DuckDB
    logger.info("Bulk loading CSV into DuckDB...")
    t2 = time.time()
    num_loaded = load_csv_to_db(conn, csv_path)
    t3 = time.time()
    logger.info("Bulk load: %d records in %.1f sec", num_loaded, t3 - t2)

    # Clean up staging file
    try:
        os.remove(csv_path)
    except OSError:
        pass

    update_data_source(conn, SOURCE_ID, "OpenAIRE Bulk (Zenodo)", num_loaded, status="ok")
    return num_loaded
