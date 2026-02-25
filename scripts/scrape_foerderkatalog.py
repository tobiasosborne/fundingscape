#!/usr/bin/env python3
"""Resumable bulk scraper for BMBF Förderkatalog (268K+ projects).

Searches with '%' wildcard to get all projects, paginates 100/page,
upserts each page immediately for crash safety.

Usage:
  uv run python scripts/scrape_foerderkatalog.py          # start or resume
  uv run python scripts/scrape_foerderkatalog.py --reset   # start over
  uv run python scripts/scrape_foerderkatalog.py --status  # show progress
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import tempfile
import time
from pathlib import Path

import click
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
from fundingscape.db import get_connection, update_data_source, upsert_grant
from fundingscape.sources.foerderkatalog import (
    FOEKAT_BASE,
    SEARCH_URL,
    SOURCE_ID,
    _BROWSER_HEADERS,
    _create_session,
    _fetch_results_page,
    _init_session,
    _parse_search_results,
    _parse_total_count,
    _result_to_grant,
    _submit_search,
)

console = Console()
logger = logging.getLogger("scrape_foerderkatalog")

CHECKPOINT_DIR = os.path.join(CACHE_DIR, "foerderkatalog")
CHECKPOINT_PATH = os.path.join(CHECKPOINT_DIR, "checkpoint.json")

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    console.print("\n[yellow]Shutdown requested — finishing current page...[/yellow]")


def load_checkpoint() -> dict | None:
    """Load checkpoint from disk. Returns None if no checkpoint."""
    try:
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_checkpoint(row_from: int, total: int, loaded: int, failed: int) -> None:
    """Atomically save checkpoint to disk."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    data = {
        "source": "foerderkatalog",
        "row_from": row_from,
        "total": total,
        "loaded": loaded,
        "failed": failed,
    }
    # Atomic write: write to temp file, then rename
    fd, tmp_path = tempfile.mkstemp(dir=CHECKPOINT_DIR, suffix=".tmp")
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
    """Remove checkpoint file after successful completion."""
    try:
        os.unlink(CHECKPOINT_PATH)
    except FileNotFoundError:
        pass


def fetch_with_retry(
    session,
    jsessionid: str,
    row_from: int,
    rows_per_page: int,
    max_retries: int = 3,
    reinit_session_fn=None,
) -> tuple[str, str]:
    """Fetch a results page with retries and session recovery.

    Returns (html, jsessionid) — jsessionid may change if session was refreshed.
    """
    delays = [2, 4, 8]
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            html = _fetch_results_page(session, jsessionid, row_from, rows_per_page)

            # Check for session expiry (redirect to search mask or empty results)
            if "actionMode=searchmask" in html or "Suchmaske" in html[:500]:
                if reinit_session_fn and attempt < max_retries:
                    logger.warning("Session expired, re-initializing...")
                    jsessionid = reinit_session_fn(session)
                    continue
                raise RuntimeError("Session expired and could not re-initialize")

            return html, jsessionid

        except Exception as e:
            last_error = e
            if attempt < max_retries:
                delay = delays[min(attempt, len(delays) - 1)]
                logger.warning(
                    "Request failed (attempt %d/%d): %s. Retrying in %ds...",
                    attempt + 1, max_retries, e, delay,
                )
                time.sleep(delay)

                # Try session recovery on HTTP errors
                if reinit_session_fn and ("403" in str(e) or "session" in str(e).lower()):
                    try:
                        jsessionid = reinit_session_fn(session)
                        logger.info("Session re-initialized successfully")
                    except Exception as re_err:
                        logger.warning("Session re-init failed: %s", re_err)
            else:
                logger.error("Request failed after %d retries: %s", max_retries, e)

    raise last_error  # type: ignore[misc]


def reinit_and_search(session) -> str:
    """Re-initialize session and re-submit the wildcard search.

    Returns new jsessionid.
    """
    jsessionid = _init_session(session)
    _submit_search(session, "%", jsessionid)
    return jsessionid


@click.command()
@click.option("--reset", is_flag=True, help="Discard checkpoint, start from scratch")
@click.option("--status", is_flag=True, help="Show progress and exit")
@click.option("--batch-size", default=100, help="Rows per page (max 100)")
@click.option("--delay", default=2.5, type=float, help="Seconds between requests")
def main(reset: bool, status: bool, batch_size: int, delay: float) -> None:
    """Scrape all Förderkatalog projects into the database."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # Handle --status
    if status:
        cp = load_checkpoint()
        if cp:
            pct = cp["loaded"] / cp["total"] * 100 if cp["total"] else 0
            console.print(f"[bold]Förderkatalog scrape in progress[/bold]")
            console.print(f"  Row: {cp['row_from']:,} / {cp['total']:,}")
            console.print(f"  Loaded: {cp['loaded']:,} ({pct:.1f}%)")
            console.print(f"  Failed: {cp['failed']:,}")
        else:
            console.print("[dim]No checkpoint found — scrape not started or already complete.[/dim]")
        return

    # Handle --reset
    if reset:
        remove_checkpoint()
        console.print("[yellow]Checkpoint cleared.[/yellow]")

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Load checkpoint
    checkpoint = load_checkpoint()
    start_row = 1
    loaded = 0
    failed = 0

    if checkpoint and not reset:
        start_row = checkpoint["row_from"]
        loaded = checkpoint["loaded"]
        failed = checkpoint["failed"]
        console.print(
            f"[green]Resuming from row {start_row:,} "
            f"({loaded:,} loaded, {failed:,} failed)[/green]"
        )

    # Connect to database
    conn = get_connection()

    # Initialize HTTP session
    console.print("[bold]Initializing Förderkatalog session...[/bold]")
    session = _create_session()
    try:
        jsessionid = _init_session(session)
    except Exception as e:
        console.print(f"[red]Failed to initialize session: {e}[/red]")
        return

    # Submit wildcard search to get all projects
    console.print("[bold]Submitting wildcard search...[/bold]")
    try:
        html = _submit_search(session, "%", jsessionid)
    except Exception as e:
        console.print(f"[red]Search failed: {e}[/red]")
        return

    total = _parse_total_count(html)
    if total == 0:
        console.print("[red]No results found — check if Förderkatalog is accessible.[/red]")
        return

    console.print(f"[bold green]Found {total:,} total projects[/bold green]")

    # If this is a fresh start, process the first page from the search response
    if start_row == 1 and loaded == 0:
        first_results = _parse_search_results(html)
        if first_results:
            for r in first_results:
                try:
                    grant = _result_to_grant(r)
                    upsert_grant(conn, grant)
                    loaded += 1
                except Exception as e:
                    logger.warning("Failed to process result %s: %s", r.get("fkz", "?"), e)
                    failed += 1

            start_row = loaded + 1
            save_checkpoint(start_row, total, loaded, failed)

        # Now re-fetch page 1 with proper pagination (100/page)
        time.sleep(delay)

    # Main pagination loop
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
        task = progress.add_task("Scraping", total=total, completed=loaded)

        row_from = start_row
        while row_from <= total:
            if _shutdown:
                console.print("[yellow]Shutdown — progress saved.[/yellow]")
                break

            try:
                html, jsessionid = fetch_with_retry(
                    session, jsessionid, row_from, batch_size,
                    reinit_session_fn=reinit_and_search,
                )
            except Exception as e:
                logger.error("Page at row %d failed permanently: %s", row_from, e)
                failed += batch_size  # approximate
                row_from += batch_size
                save_checkpoint(row_from, total, loaded, failed)
                continue

            results = _parse_search_results(html)
            if not results:
                # Empty page — we've reached the end
                logger.info("Empty page at row %d — end of data", row_from)
                break

            # Upsert each result
            page_loaded = 0
            for r in results:
                try:
                    grant = _result_to_grant(r)
                    upsert_grant(conn, grant)
                    page_loaded += 1
                except Exception as e:
                    logger.warning("Failed to process %s: %s", r.get("fkz", "?"), e)
                    failed += 1

            loaded += page_loaded
            row_from += batch_size
            save_checkpoint(row_from, total, loaded, failed)
            progress.update(task, completed=loaded)

            # Rate limiting
            time.sleep(delay)

    # Final status
    session.close()

    if not _shutdown and loaded > 0:
        update_data_source(conn, SOURCE_ID, "BMBF Förderkatalog", loaded, status="ok")
        remove_checkpoint()
        console.print(f"\n[bold green]Complete! Loaded {loaded:,} projects ({failed:,} failed).[/bold green]")
    elif loaded > 0:
        console.print(f"\n[yellow]Paused. Loaded {loaded:,} so far ({failed:,} failed). Run again to resume.[/yellow]")

    conn.close()


if __name__ == "__main__":
    main()
