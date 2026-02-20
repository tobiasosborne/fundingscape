"""Manual entry system for funding sources without APIs."""

from __future__ import annotations

import logging
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import yaml
import duckdb

from fundingscape.db import insert_call, upsert_call, update_data_source
from fundingscape.models import Call, FundingInstrument

logger = logging.getLogger(__name__)

SOURCE_ID = "manual"
MANUAL_DIR = "manual"


def load_yaml_instruments(
    conn: duckdb.DuckDBPyConnection,
    manual_dir: str | None = None,
) -> int:
    """Load funding instruments and calls from YAML files.

    Returns total number of calls loaded.
    """
    manual_dir = manual_dir or MANUAL_DIR
    yaml_dir = Path(manual_dir)
    if not yaml_dir.exists():
        logger.warning("Manual directory %s does not exist", manual_dir)
        return 0

    total = 0
    for yaml_file in sorted(yaml_dir.glob("*.yaml")):
        logger.info("Loading manual entries from %s", yaml_file.name)
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if not data:
            continue

        calls = data.get("calls", [])
        for call_data in calls:
            try:
                call = Call(
                    call_identifier=call_data.get("id"),
                    title=call_data["title"],
                    description=call_data.get("description"),
                    url=call_data.get("url"),
                    opening_date=call_data.get("opening_date"),
                    deadline=call_data.get("deadline"),
                    status=call_data.get("status", "open"),
                    budget_total=Decimal(str(call_data["budget"])) if call_data.get("budget") else None,
                    topic_keywords=call_data.get("keywords", []),
                    framework_programme=call_data.get("programme"),
                    source=SOURCE_ID,
                    source_id=f"manual_{call_data.get('id', call_data['title'][:30])}",
                )
                upsert_call(conn, call)
                total += 1
            except Exception as e:
                logger.error("Failed to load manual call '%s': %s",
                           call_data.get("title", "unknown"), e)

    update_data_source(conn, SOURCE_ID, "Manual Entries", total, status="ok")
    return total
