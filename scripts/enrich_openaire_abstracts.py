"""Re-extract abstracts from the cached OpenAIRE bulk tar and backfill them.

The original openaire_bulk.py loader skipped the `summary` field entirely.
This script re-reads the cached project.tar (~620 MB), extracts summaries
+ keywords + subjects for records that have them, and updates the existing
grant_award rows by source_id.

UKRI (~175K) and ARC (~32K) records have 100% summaries in the dump.
NIH/NSF/DFG/SNSF have 0% (their abstracts aren't in the OpenAIRE bulk
format — would need API enrichment). Net expected: ~250-360K new abstracts.

Idempotent — only updates rows where abstract is currently NULL or empty.
"""

from __future__ import annotations

import csv
import gzip
import json
import logging
import os
import sys
import tarfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb

from fundingscape import CACHE_DIR, DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TAR_PATH = os.path.join(CACHE_DIR, "openaire", "project.tar")


def extract_abstracts_to_csv(tar_path: str, csv_path: str) -> tuple[int, int, dict[str, int]]:
    """Stream the tar, write source_id + abstract + keywords + subjects to TSV.

    Returns (records_with_summary, records_total, per_funder_counts).
    """
    total = 0
    with_summary = 0
    by_funder: dict[str, int] = {}

    with open(csv_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["source_id", "abstract", "extra_keywords"])

        with tarfile.open(tar_path, "r") as tar:
            members = sorted(
                [m for m in tar.getmembers() if m.name.endswith(".gz")],
                key=lambda m: m.name,
            )
            for member in members:
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
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        total += 1

                        title = obj.get("title")
                        if not title or title == "unidentified":
                            continue

                        summary = obj.get("summary")
                        if not summary:
                            continue
                        # Normalize whitespace, strip tabs/newlines
                        summary = " ".join(summary.split())
                        if not summary:
                            continue
                        # Truncate very long summaries
                        if len(summary) > 10000:
                            summary = summary[:10000]

                        project_id = obj.get("code", "")
                        if not project_id or project_id == "unidentified":
                            continue

                        funder_short = ""
                        for fund in obj.get("fundings") or []:
                            funder_short = fund.get("shortName", "") or ""
                            break

                        # Match the source_id format used by openaire_bulk.py
                        sid = f"oaire_{funder_short}_{project_id}"

                        # Combine keywords + subjects into a single field
                        kw = obj.get("keywords") or ""
                        subjects = obj.get("subjects")
                        extra_kw_parts = []
                        if kw:
                            extra_kw_parts.append(" ".join(str(kw).split())[:500])
                        if subjects:
                            if isinstance(subjects, list):
                                subj_str = "; ".join(str(s) for s in subjects[:20])
                            else:
                                subj_str = str(subjects)
                            extra_kw_parts.append(" ".join(subj_str.split())[:500])
                        extra_kw = " | ".join(extra_kw_parts)

                        writer.writerow([sid, summary, extra_kw])
                        with_summary += 1
                        by_funder[funder_short] = by_funder.get(funder_short, 0) + 1

                        if with_summary % 50000 == 0:
                            logger.info("  ... %d summaries extracted (of %d total)",
                                        with_summary, total)

    return with_summary, total, by_funder


def update_grant_award(conn: duckdb.DuckDBPyConnection, csv_path: str) -> dict[str, int]:
    """Update grant_award.abstract for rows where source_id matches and abstract is NULL/empty.

    Uses a temp table for the join.
    """
    logger.info("Loading TSV into temp table...")
    conn.execute("DROP TABLE IF EXISTS _oaire_abstracts")
    conn.execute(f"""
        CREATE TEMP TABLE _oaire_abstracts AS
        SELECT * FROM read_csv('{csv_path}',
            header=true,
            delim='\t',
            quote='',
            ignore_errors=true,
            columns={{
                'source_id': 'VARCHAR',
                'abstract': 'VARCHAR',
                'extra_keywords': 'VARCHAR'
            }})
    """)
    n_loaded = conn.execute("SELECT COUNT(*) FROM _oaire_abstracts").fetchone()[0]
    logger.info("Loaded %d abstract records into temp table", n_loaded)

    # Index for fast join
    conn.execute("CREATE INDEX idx_oaire_sid ON _oaire_abstracts (source_id)")

    # Pre-update: count grant_award rows that will be touched
    matched = conn.execute("""
        SELECT COUNT(*) FROM grant_award g
        JOIN _oaire_abstracts t ON g.source_id = t.source_id
        WHERE g.source = 'openaire_bulk'
          AND (g.abstract IS NULL OR g.abstract = '')
    """).fetchone()[0]
    logger.info("Will update %d grant_award rows (currently null abstracts)", matched)

    # Apply update
    conn.execute("""
        UPDATE grant_award g
        SET abstract = t.abstract,
            updated_at = CURRENT_TIMESTAMP
        FROM _oaire_abstracts t
        WHERE g.source_id = t.source_id
          AND g.source = 'openaire_bulk'
          AND (g.abstract IS NULL OR g.abstract = '')
    """)

    # Report
    after_with_abs = conn.execute("""
        SELECT COUNT(*) FROM grant_award
        WHERE source = 'openaire_bulk' AND abstract IS NOT NULL AND abstract != ''
    """).fetchone()[0]
    total_oaire = conn.execute(
        "SELECT COUNT(*) FROM grant_award WHERE source = 'openaire_bulk'"
    ).fetchone()[0]

    return {
        "tsv_records": n_loaded,
        "matched_for_update": matched,
        "openaire_total": total_oaire,
        "openaire_with_abstract_after": after_with_abs,
    }


def main() -> None:
    if not os.path.exists(TAR_PATH):
        logger.error("Tar not found at %s. Run the OpenAIRE bulk pipeline first.", TAR_PATH)
        sys.exit(1)

    csv_path = TAR_PATH.replace(".tar", "_abstracts.tsv")

    logger.info("Step 1: extract abstracts from cached tar...")
    t0 = time.time()
    with_summary, total, by_funder = extract_abstracts_to_csv(TAR_PATH, csv_path)
    t1 = time.time()
    logger.info("Step 1 done in %.1fs: %d summaries from %d records (%.1f%%)",
                t1 - t0, with_summary, total, 100*with_summary/max(total, 1))
    logger.info("Top funders by extracted summaries:")
    for f, n in sorted(by_funder.items(), key=lambda x: -x[1])[:15]:
        logger.info("  %-15s %10d", f, n)

    logger.info("Step 2: update grant_award table...")
    t2 = time.time()
    conn = duckdb.connect(DB_PATH)
    try:
        stats = update_grant_award(conn, csv_path)
    finally:
        conn.close()
    t3 = time.time()
    logger.info("Step 2 done in %.1fs", t3 - t2)

    logger.info("=== Final stats ===")
    for k, v in stats.items():
        logger.info("  %-30s %10d", k, v)

    pct = 100 * stats["openaire_with_abstract_after"] / stats["openaire_total"]
    logger.info("OpenAIRE bulk abstract coverage: %.1f%% (was 0%%)", pct)

    # Clean up TSV
    try:
        os.remove(csv_path)
    except OSError:
        pass


if __name__ == "__main__":
    main()
