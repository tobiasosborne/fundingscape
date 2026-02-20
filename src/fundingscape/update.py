"""Pipeline orchestrator — runs all data source fetchers."""

from __future__ import annotations

import logging
import sys

from fundingscape.db import get_connection, _seed_funders, _seed_profiles
from fundingscape.sources import cordis, ft_portal, manual

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_update() -> None:
    """Run the full data pipeline update."""
    conn = get_connection()
    _seed_funders(conn)
    _seed_profiles(conn)

    logger.info("=== Starting data pipeline update ===")

    # 1. CORDIS bulk data (Horizon Europe + H2020)
    logger.info("--- CORDIS Bulk Data ---")
    try:
        total = cordis.fetch_and_load(conn)
        logger.info("CORDIS: loaded %d grants", total)
    except Exception as e:
        logger.error("CORDIS failed: %s", e)

    # 2. EU Funding & Tenders Portal
    logger.info("--- EU F&T Portal ---")
    try:
        total = ft_portal.fetch_and_load(conn)
        logger.info("F&T Portal: loaded %d calls", total)
    except Exception as e:
        logger.error("F&T Portal failed: %s", e)

    # 3. Manual entries
    logger.info("--- Manual Entries ---")
    try:
        total = manual.load_yaml_instruments(conn)
        logger.info("Manual: loaded %d entries", total)
    except Exception as e:
        logger.error("Manual entries failed: %s", e)

    # Summary
    total_grants = conn.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
    total_calls = conn.execute("SELECT COUNT(*) FROM call").fetchone()[0]
    logger.info("=== Update complete: %d grants, %d calls ===", total_grants, total_calls)

    conn.close()


if __name__ == "__main__":
    run_update()
