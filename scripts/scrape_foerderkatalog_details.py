#!/usr/bin/env python3
"""Resumable detail page scraper for Förderkatalog (268K+ projects).

Fetches individual project detail pages to extract abstracts
(Kurzbeschreibung) and other metadata not available in search results.

Usage:
  uv run python scripts/scrape_foerderkatalog_details.py          # start or resume
  uv run python scripts/scrape_foerderkatalog_details.py --status  # show progress
  uv run python scripts/scrape_foerderkatalog_details.py --reset   # start over
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
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
from fundingscape.db import get_connection
from fundingscape.sources.foerderkatalog import (
    SOURCE_ID,
    _create_session,
    _fetch_project_detail,
    _init_session,
)

console = Console()
logger = logging.getLogger("scrape_foerderkatalog_details")

DETAIL_CACHE_DIR = os.path.join(CACHE_DIR, "foerderkatalog", "details")
CHECKPOINT_PATH = os.path.join(CACHE_DIR, "foerderkatalog", "detail_checkpoint.json")

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


@click.command()
@click.option("--reset", is_flag=True, help="Discard checkpoint, start from scratch")
@click.option("--status", is_flag=True, help="Show progress and exit")
@click.option("--delay", default=2.5, type=float, help="Seconds between requests")
def main(reset: bool, status: bool, delay: float) -> None:
    """Fetch detail pages for all Förderkatalog projects."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    if status:
        cp = load_checkpoint()
        if cp:
            done = cp.get("completed", 0) + cp.get("failed", 0)
            total = cp.get("total", 0)
            pct = done / total * 100 if total else 0
            console.print(f"[bold]Förderkatalog detail scrape[/bold]")
            console.print(f"  Progress: {done:,} / {total:,} ({pct:.1f}%)")
            console.print(f"  With abstract: {cp.get('with_abstract', 0):,}")
            console.print(f"  Failed: {cp.get('failed', 0):,}")
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

    # Get all Förderkatalog FKZ codes that need detail fetching
    # Focus on records without abstracts
    rows = conn.execute("""
        SELECT project_id FROM grant_award
        WHERE source = ? AND (abstract IS NULL OR abstract = '')
        ORDER BY project_id
    """, [SOURCE_ID]).fetchall()

    fkz_list = [r[0] for r in rows if r[0]]
    total = len(fkz_list)
    console.print(f"[bold]{total:,} projects need detail pages[/bold]")

    if not fkz_list:
        console.print("[dim]All projects already have abstracts.[/dim]")
        conn.close()
        return

    # Resume from checkpoint
    cp = load_checkpoint()
    start_idx = 0
    completed = 0
    failed = 0
    with_abstract = 0

    if cp and not reset:
        completed = cp.get("completed", 0)
        failed = cp.get("failed", 0)
        with_abstract = cp.get("with_abstract", 0)
        start_idx = completed + failed
        console.print(f"[green]Resuming from {start_idx:,}[/green]")

    os.makedirs(DETAIL_CACHE_DIR, exist_ok=True)

    # Create session
    session = _create_session()
    jsessionid = _init_session(session)
    console.print(f"[dim]Session: {jsessionid[:12]}...[/dim]")

    # Session refresh counter (Förderkatalog sessions expire)
    requests_since_refresh = 0
    SESSION_REFRESH_INTERVAL = 500

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
        task = progress.add_task("Fetching details", total=total, completed=start_idx)

        for i in range(start_idx, total):
            if _shutdown:
                console.print("[yellow]Shutdown — progress saved.[/yellow]")
                break

            fkz = fkz_list[i]

            # Refresh session periodically
            requests_since_refresh += 1
            if requests_since_refresh >= SESSION_REFRESH_INTERVAL:
                try:
                    jsessionid = _init_session(session)
                    requests_since_refresh = 0
                    logger.info("Session refreshed: %s...", jsessionid[:12])
                except Exception as e:
                    logger.warning("Session refresh failed: %s", e)

            try:
                detail = _fetch_project_detail(session, fkz, jsessionid, DETAIL_CACHE_DIR)
                if detail:
                    completed += 1
                    abstract = detail.get("Kurzbeschreibung", detail.get("Projektbeschreibung"))
                    if abstract:
                        with_abstract += 1
                        conn.execute(
                            "UPDATE grant_award SET abstract = ? "
                            "WHERE source = ? AND project_id = ? "
                            "AND (abstract IS NULL OR abstract = '')",
                            [abstract, SOURCE_ID, fkz],
                        )
                else:
                    failed += 1
            except Exception as e:
                logger.warning("Detail fetch failed for %s: %s", fkz, e)
                failed += 1

            if (i + 1) % 10 == 0 or i == total - 1:
                save_checkpoint({
                    "completed": completed,
                    "failed": failed,
                    "with_abstract": with_abstract,
                    "total": total,
                    "last_fkz": fkz,
                })
                progress.update(task, completed=completed + failed)

    session.close()

    console.print(f"\n[bold green]Done![/bold green]")
    console.print(f"  Completed: {completed:,}")
    console.print(f"  With abstract: {with_abstract:,}")
    console.print(f"  Failed: {failed:,}")

    conn.close()


if __name__ == "__main__":
    main()
