#!/usr/bin/env python3
"""Resumable scraper for GEPRIS person pages to extract institutions.

Fetches person sub-pages from GEPRIS to get the institution for PIs
whose project pages don't include institution information.

Usage:
  uv run python scripts/scrape_gepris_persons.py          # start or resume
  uv run python scripts/scrape_gepris_persons.py --status  # show progress
  uv run python scripts/scrape_gepris_persons.py --reset   # start over
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path

import click
import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from fundingscape import CACHE_DIR
from fundingscape.db import get_connection

console = Console()
logger = logging.getLogger("scrape_gepris_persons")

CACHE_SUBDIR = os.path.join(CACHE_DIR, "gepris", "persons")
CHECKPOINT_PATH = os.path.join(CACHE_DIR, "gepris", "persons_checkpoint.json")
GEPRIS_PERSON_URL = "https://gepris.dfg.de/gepris/person/{person_id}?language=en"

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    console.print("\n[yellow]Shutdown requested — finishing current request...[/yellow]")


def load_checkpoint() -> dict | None:
    try:
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_checkpoint(data: dict) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, CHECKPOINT_PATH)


def _extract_person_ids_from_cache() -> dict[str, list[str]]:
    """Extract person IDs from cached GEPRIS project pages.

    Returns dict mapping person_id -> list of project_ids.
    """
    cache_dir = os.path.join(CACHE_DIR, "gepris")
    person_to_projects: dict[str, list[str]] = {}

    count = 0
    for fname in os.listdir(cache_dir):
        if not fname.endswith(".meta.json"):
            continue

        meta_path = os.path.join(cache_dir, fname)
        try:
            with open(meta_path) as f:
                meta = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        url = meta.get("url", "")
        pid_match = re.search(r"/gepris/projekt/(\d+)", url)
        if not pid_match:
            continue

        project_id = pid_match.group(1)
        data_path = meta_path.replace(".meta.json", ".data")
        if not os.path.exists(data_path):
            continue

        try:
            with open(data_path, "r", errors="replace") as f:
                html = f.read()
        except OSError:
            continue

        # Find person links in the HTML
        for person_id in re.findall(r'href="/gepris/person/(\d+)"', html):
            if person_id not in person_to_projects:
                person_to_projects[person_id] = []
            person_to_projects[person_id].append(project_id)

        count += 1
        if count % 10000 == 0:
            logger.info("Scanned %d cached pages, found %d unique persons", count, len(person_to_projects))

    return person_to_projects


def _parse_person_page(html: str) -> str | None:
    """Extract institution name from a GEPRIS person page."""
    soup = BeautifulSoup(html, "html.parser")

    # Look for institution links
    for a in soup.select('a[href*="/gepris/institution/"]'):
        name = a.get_text(strip=True)
        if name and len(name) > 3:
            return name

    # Fallback: look for name/value pairs with institution-related keys
    for span in soup.select("span.name"):
        key = span.get_text(strip=True)
        if key in ("Institution", "Address", "Einrichtung", "Adresse"):
            value_el = span.find_next_sibling()
            if value_el:
                text = value_el.get_text(strip=True)
                if text and len(text) > 3:
                    return text

    return None


def _fetch_person(
    session: httpx.Client,
    person_id: str,
    delay: float = 2.5,
    max_retries: int = 3,
) -> str | None:
    """Fetch a person page, with caching and retries. Returns institution or None."""
    # Check cache
    cache_path = os.path.join(CACHE_SUBDIR, f"{person_id}.html")
    if os.path.exists(cache_path):
        with open(cache_path, "r", errors="replace") as f:
            html = f.read()
        return _parse_person_page(html)

    url = GEPRIS_PERSON_URL.format(person_id=person_id)
    delays = [2, 4, 8]

    for attempt in range(max_retries + 1):
        try:
            time.sleep(delay)
            resp = session.get(url, follow_redirects=True)
            resp.raise_for_status()
            html = resp.text

            # Detect GEPRIS downtime / error pages
            if "vorübergehend nicht erreichbar" in html or "temporarily unavailable" in html:
                raise RuntimeError("GEPRIS is temporarily unavailable")

            # Cache the response
            os.makedirs(CACHE_SUBDIR, exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(html)

            return _parse_person_page(html)
        except Exception as e:
            if attempt < max_retries:
                d = delays[min(attempt, len(delays) - 1)]
                logger.warning("Person %s attempt %d/%d failed: %s. Retry in %ds",
                               person_id, attempt + 1, max_retries, e, d)
                time.sleep(d)
            else:
                logger.error("Person %s failed after %d retries: %s", person_id, max_retries, e)

    return None


@click.command()
@click.option("--reset", is_flag=True, help="Discard checkpoint, start from scratch")
@click.option("--status", is_flag=True, help="Show progress and exit")
@click.option("--delay", default=2.5, type=float, help="Seconds between requests")
def main(reset: bool, status: bool, delay: float) -> None:
    """Scrape GEPRIS person pages for institution names."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    if status:
        cp = load_checkpoint()
        if cp:
            done = cp.get("completed", 0)
            total = cp.get("total", 0)
            pct = done / total * 100 if total else 0
            console.print(f"[bold]GEPRIS person scrape[/bold]")
            console.print(f"  Completed: {done:,} / {total:,} ({pct:.1f}%)")
            console.print(f"  Institutions found: {cp.get('found', 0):,}")
            console.print(f"  Updated in DB: {cp.get('updated', 0):,}")
        else:
            console.print("[dim]No checkpoint found.[/dim]")
        return

    if reset:
        if os.path.exists(CHECKPOINT_PATH):
            os.remove(CHECKPOINT_PATH)
        console.print("[yellow]Checkpoint cleared.[/yellow]")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn = get_connection()

    # Step 1: Get list of projects needing institutions
    console.print("[bold]Scanning cached GEPRIS pages for person links...[/bold]")
    person_to_projects = _extract_person_ids_from_cache()

    # Filter to only persons whose projects need institutions
    rows = conn.execute("""
        SELECT project_id FROM grant_award
        WHERE source = 'gepris'
          AND pi_name IS NOT NULL AND pi_name != ''
          AND (pi_institution IS NULL OR pi_institution = '')
    """).fetchall()
    projects_needing_inst = {r[0] for r in rows}

    # Filter person IDs to those linked to projects needing institutions
    relevant_persons = {
        pid: projs
        for pid, projs in person_to_projects.items()
        if any(p in projects_needing_inst for p in projs)
    }

    person_ids = sorted(relevant_persons.keys())
    console.print(f"[bold green]Found {len(person_ids):,} unique persons to fetch[/bold green]")

    # Resume from checkpoint
    cp = load_checkpoint()
    start_idx = 0
    completed = 0
    found = 0
    updated = 0

    if cp and not reset:
        completed = cp.get("completed", 0)
        found = cp.get("found", 0)
        updated = cp.get("updated", 0)
        start_idx = completed
        console.print(f"[green]Resuming from {completed:,}[/green]")

    with httpx.Client(timeout=60.0) as session:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching person pages", total=len(person_ids), completed=completed)

            for i in range(start_idx, len(person_ids)):
                if _shutdown:
                    console.print("[yellow]Shutdown — progress saved.[/yellow]")
                    break

                person_id = person_ids[i]
                institution = _fetch_person(session, person_id, delay=delay)
                completed += 1

                if institution:
                    found += 1
                    # Update all linked projects that need institution
                    linked_projects = relevant_persons[person_id]
                    for proj_id in linked_projects:
                        if proj_id in projects_needing_inst:
                            conn.execute(
                                "UPDATE grant_award SET pi_institution = ? "
                                "WHERE source = 'gepris' AND project_id = ? "
                                "AND (pi_institution IS NULL OR pi_institution = '')",
                                [institution, proj_id],
                            )
                            updated += 1

                if completed % 10 == 0 or i == len(person_ids) - 1:
                    save_checkpoint({
                        "completed": completed,
                        "found": found,
                        "updated": updated,
                        "total": len(person_ids),
                    })
                    progress.update(task, completed=completed)

    console.print(f"\n[bold green]Done![/bold green]")
    console.print(f"  Persons fetched: {completed:,}")
    console.print(f"  Institutions found: {found:,}")
    console.print(f"  DB records updated: {updated:,}")

    conn.close()


if __name__ == "__main__":
    main()
