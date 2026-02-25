#!/usr/bin/env python3
"""Resumable bulk scraper for DFG GEPRIS (152K+ projects).

Two-phase approach:
  Phase 1 (listing): Browse catalogue to collect all project IDs + titles
  Phase 2 (details): Fetch detail page for each project

Usage:
  uv run python scripts/scrape_gepris.py                  # listing + details
  uv run python scripts/scrape_gepris.py --listing-only    # just collect IDs
  uv run python scripts/scrape_gepris.py --details-only    # just fetch details
  uv run python scripts/scrape_gepris.py --status          # show progress
  uv run python scripts/scrape_gepris.py --reset           # start over
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import tempfile
import time
from pathlib import Path

import click
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
from fundingscape.cache import CachedHttpClient
from fundingscape.db import get_connection, update_data_source, upsert_grant
from fundingscape.models import GrantAward
from fundingscape.sources.gepris import (
    GEPRIS_BASE,
    PROJECT_URL,
    SEARCH_URL,
    SOURCE_ID,
    _fetch_project_detail,
)

console = Console()
logger = logging.getLogger("scrape_gepris")

CACHE_SUBDIR = os.path.join(CACHE_DIR, "gepris")
CHECKPOINT_PATH = os.path.join(CACHE_SUBDIR, "bulk_checkpoint.json")

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    console.print("\n[yellow]Shutdown requested — finishing current page...[/yellow]")


def load_checkpoint() -> dict | None:
    """Load checkpoint from disk."""
    try:
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_checkpoint(data: dict) -> None:
    """Atomically save checkpoint to disk."""
    os.makedirs(CACHE_SUBDIR, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=CACHE_SUBDIR, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, CHECKPOINT_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def remove_checkpoint() -> None:
    """Remove checkpoint file."""
    try:
        os.unlink(CHECKPOINT_PATH)
    except FileNotFoundError:
        pass


def fetch_with_retry(
    client: CachedHttpClient,
    url: str,
    max_retries: int = 3,
) -> str:
    """Fetch a URL with retries and exponential backoff.

    Returns HTML text.
    """
    delays = [2, 4, 8]
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            return client.fetch_text(url, force=True)
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                delay = delays[min(attempt, len(delays) - 1)]
                logger.warning(
                    "Request failed (attempt %d/%d): %s. Retrying in %ds...",
                    attempt + 1, max_retries, e, delay,
                )
                time.sleep(delay)
            else:
                logger.error("Request failed after %d retries: %s", max_retries, e)

    raise last_error  # type: ignore[misc]


def _parse_catalogue_page(html: str) -> list[dict]:
    """Parse a GEPRIS catalogue page for project IDs and titles.

    Returns list of {id, title} dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for item in soup.select("div.results"):
        link = item.select_one("h2 a[href*='/gepris/projekt/']")
        if not link:
            continue

        href = link.get("href", "")
        match = re.search(r"/gepris/projekt/(\d+)", href)
        if not match:
            continue

        results.append({
            "id": match.group(1),
            "title": link.get_text(strip=True),
        })

    return results


def _extract_total_from_catalogue(html: str) -> int:
    """Extract total result count from GEPRIS catalogue page.

    Looks for patterns like "152,700 results" or "152.700 Treffer".
    """
    # Try English pattern first
    match = re.search(r"([\d,.\s]+)\s*(?:results|Treffer|Projects)", html)
    if match:
        num_str = match.group(1).replace(",", "").replace(".", "").replace(" ", "")
        try:
            return int(num_str)
        except ValueError:
            pass

    # Try extracting from pagination info
    match = re.search(r"of\s+([\d,.\s]+)", html)
    if match:
        num_str = match.group(1).replace(",", "").replace(".", "").replace(" ", "")
        try:
            return int(num_str)
        except ValueError:
            pass

    return 0


def run_listing(
    client: CachedHttpClient,
    conn,
    hits_per_page: int = 50,
    delay: float = 2.5,
) -> int:
    """Phase 1: Catalogue browse to collect all project IDs.

    Returns number of projects listed.
    """
    checkpoint = load_checkpoint()
    start_index = 0
    listed = 0

    if checkpoint and checkpoint.get("phase") == "listing":
        start_index = checkpoint["index"]
        listed = checkpoint.get("listed", 0)
        console.print(
            f"[green]Resuming listing from index {start_index:,} "
            f"({listed:,} listed so far)[/green]"
        )

    # First page to get total count
    console.print("[bold]Fetching GEPRIS catalogue...[/bold]")
    url = (
        f"{SEARCH_URL}?task=doKatalog&context=projekt"
        f"&hitsPerPage={hits_per_page}"
        f"&findButton=Finden"
        f"&language=en"
    )
    try:
        html = fetch_with_retry(client, url)
    except Exception as e:
        console.print(f"[red]Failed to fetch catalogue: {e}[/red]")
        return listed

    total = _extract_total_from_catalogue(html)
    if total == 0:
        # Try parsing first page — if we get results, estimate total later
        first_results = _parse_catalogue_page(html)
        if not first_results:
            console.print("[red]No results found — check GEPRIS accessibility.[/red]")
            return listed
        # We'll discover the total as we go — set a large estimate
        total = 200_000
        console.print(f"[yellow]Could not extract total count, using estimate: {total:,}[/yellow]")
    else:
        console.print(f"[bold green]Found {total:,} total projects[/bold green]")

    # Process first page if starting fresh
    if start_index == 0 and listed == 0:
        first_results = _parse_catalogue_page(html)
        for r in first_results:
            try:
                grant = GrantAward(
                    project_title=r["title"],
                    project_id=r["id"],
                    pi_country="DE",
                    source=SOURCE_ID,
                    source_id=f"gepris_{r['id']}",
                )
                upsert_grant(conn, grant)
                listed += 1
            except Exception as e:
                logger.warning("Failed to upsert project %s: %s", r["id"], e)

        start_index = hits_per_page
        save_checkpoint({"phase": "listing", "index": start_index, "total": total, "listed": listed})

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
        task = progress.add_task("Listing projects", total=total, completed=listed)

        index = start_index
        consecutive_empty = 0

        while index < total + hits_per_page:
            if _shutdown:
                console.print("[yellow]Shutdown — progress saved.[/yellow]")
                break

            url = (
                f"{SEARCH_URL}?context=projekt"
                f"&findButton=historyCall"
                f"&hitsPerPage={hits_per_page}"
                f"&index={index}"
                f"&language=en"
            )

            try:
                html = fetch_with_retry(client, url)
            except Exception as e:
                logger.error("Catalogue page at index %d failed: %s", index, e)
                index += hits_per_page
                save_checkpoint({"phase": "listing", "index": index, "total": total, "listed": listed})
                continue

            results = _parse_catalogue_page(html)
            if not results:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("3 consecutive empty pages at index %d — end of data", index)
                    break
                index += hits_per_page
                save_checkpoint({"phase": "listing", "index": index, "total": total, "listed": listed})
                time.sleep(delay)
                continue

            consecutive_empty = 0

            for r in results:
                try:
                    grant = GrantAward(
                        project_title=r["title"],
                        project_id=r["id"],
                        pi_country="DE",
                        source=SOURCE_ID,
                        source_id=f"gepris_{r['id']}",
                    )
                    upsert_grant(conn, grant)
                    listed += 1
                except Exception as e:
                    logger.warning("Failed to upsert project %s: %s", r["id"], e)

            index += hits_per_page
            save_checkpoint({"phase": "listing", "index": index, "total": total, "listed": listed})
            progress.update(task, completed=listed)

            time.sleep(delay)

    return listed


def run_details(
    client: CachedHttpClient,
    conn,
    delay: float = 2.5,
) -> int:
    """Phase 2: Fetch detail pages for all listed projects.

    Returns number of details fetched.
    """
    # Get all project IDs that need detail fetching
    # Projects needing details: have source='gepris' and pi_name IS NULL
    rows = conn.execute(
        "SELECT project_id FROM grant_award WHERE source = ? AND pi_name IS NULL",
        [SOURCE_ID],
    ).fetchall()

    project_ids = [r[0] for r in rows if r[0]]

    if not project_ids:
        console.print("[dim]No projects need detail fetching.[/dim]")
        return 0

    # Check checkpoint for resume position
    checkpoint = load_checkpoint()
    start_idx = 0
    completed = 0

    if checkpoint and checkpoint.get("phase") == "details":
        completed = checkpoint.get("completed", 0)
        # Find where we left off by checking last completed project
        last_id = checkpoint.get("last_project_id")
        if last_id and last_id in project_ids:
            start_idx = project_ids.index(last_id) + 1
        else:
            start_idx = completed  # approximate
        console.print(
            f"[green]Resuming details from project {start_idx:,} "
            f"({completed:,} completed)[/green]"
        )

    total = len(project_ids)
    console.print(f"[bold]Fetching details for {total:,} projects[/bold]")

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
        task = progress.add_task("Fetching details", total=total, completed=completed)
        failed = 0

        for i in range(start_idx, total):
            if _shutdown:
                console.print("[yellow]Shutdown — progress saved.[/yellow]")
                break

            project_id = project_ids[i]

            try:
                grant = _fetch_project_detail(client, project_id)
                if grant:
                    upsert_grant(conn, grant)
                    completed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning("Detail fetch failed for %s: %s", project_id, e)
                failed += 1

            # Save checkpoint every 10 projects
            if (i + 1) % 10 == 0 or i == total - 1:
                save_checkpoint({
                    "phase": "details",
                    "completed": completed,
                    "failed": failed,
                    "total": total,
                    "last_project_id": project_id,
                })
                progress.update(task, completed=completed + failed)

    return completed


@click.command()
@click.option("--reset", is_flag=True, help="Discard checkpoint, start from scratch")
@click.option("--status", is_flag=True, help="Show progress and exit")
@click.option("--listing-only", is_flag=True, help="Only collect project IDs (Phase 1)")
@click.option("--details-only", is_flag=True, help="Only fetch detail pages (Phase 2)")
@click.option("--hits-per-page", default=50, help="Results per catalogue page")
@click.option("--delay", default=2.5, type=float, help="Seconds between requests")
def main(
    reset: bool,
    status: bool,
    listing_only: bool,
    details_only: bool,
    hits_per_page: int,
    delay: float,
) -> None:
    """Scrape all GEPRIS projects into the database."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # Handle --status
    if status:
        cp = load_checkpoint()
        if cp:
            phase = cp.get("phase", "?")
            console.print(f"[bold]GEPRIS scrape — phase: {phase}[/bold]")
            if phase == "listing":
                pct = cp.get("listed", 0) / cp.get("total", 1) * 100
                console.print(f"  Index: {cp.get('index', 0):,} / {cp.get('total', 0):,}")
                console.print(f"  Listed: {cp.get('listed', 0):,} ({pct:.1f}%)")
            elif phase == "details":
                total = cp.get("total", 1)
                done = cp.get("completed", 0) + cp.get("failed", 0)
                pct = done / total * 100 if total else 0
                console.print(f"  Completed: {cp.get('completed', 0):,} / {total:,} ({pct:.1f}%)")
                console.print(f"  Failed: {cp.get('failed', 0):,}")
                console.print(f"  Last project: {cp.get('last_project_id', '?')}")
        else:
            # Check DB for existing GEPRIS records
            try:
                conn = get_connection()
                count = conn.execute(
                    "SELECT COUNT(*) FROM grant_award WHERE source = ?", [SOURCE_ID]
                ).fetchone()[0]
                with_details = conn.execute(
                    "SELECT COUNT(*) FROM grant_award WHERE source = ? AND pi_name IS NOT NULL",
                    [SOURCE_ID],
                ).fetchone()[0]
                conn.close()
                console.print(f"[dim]No active checkpoint. DB has {count:,} GEPRIS records ({with_details:,} with details).[/dim]")
            except Exception:
                console.print("[dim]No checkpoint found.[/dim]")
        return

    # Handle --reset
    if reset:
        remove_checkpoint()
        console.print("[yellow]Checkpoint cleared.[/yellow]")

    # Set up signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Create cached HTTP client (reuses GEPRIS filesystem cache)
    client = CachedHttpClient(
        cache_dir=CACHE_SUBDIR,
        delay=delay,
    )

    conn = get_connection()

    # Phase 1: Listing
    if not details_only:
        console.print("\n[bold cyan]═══ Phase 1: Catalogue Listing ═══[/bold cyan]\n")
        listed = run_listing(client, conn, hits_per_page=hits_per_page, delay=delay)
        console.print(f"\n[green]Listed {listed:,} projects[/green]\n")

        if _shutdown:
            conn.close()
            return

        # Clear listing checkpoint to prepare for details phase
        if not listing_only:
            remove_checkpoint()

    # Phase 2: Details
    if not listing_only and not _shutdown:
        console.print("\n[bold cyan]═══ Phase 2: Detail Pages ═══[/bold cyan]\n")
        completed = run_details(client, conn, delay=delay)

        if not _shutdown:
            # Count total in DB
            total_in_db = conn.execute(
                "SELECT COUNT(*) FROM grant_award WHERE source = ?", [SOURCE_ID]
            ).fetchone()[0]
            update_data_source(conn, SOURCE_ID, "DFG GEPRIS", total_in_db, status="ok")
            remove_checkpoint()
            console.print(f"\n[bold green]Complete! {total_in_db:,} projects in DB ({completed:,} with details).[/bold green]")
        else:
            console.print(f"\n[yellow]Paused. {completed:,} details fetched. Run again to resume.[/yellow]")

    conn.close()


if __name__ == "__main__":
    main()
