"""Sample QA-matcher results to spot-check false positives.

For each application with > N matches (over-matched), sample K random grants
and show title + abstract snippet + which pattern(s) matched.

Output is a markdown report at data/qa_fp_audit.md that can be reviewed by
hand to classify true positives vs false positives.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fundingscape import DB_PATH, QA_DB_PATH
from fundingscape.qa_funding import APPLICATION_KEYWORDS, _build_where_clause

# Tunables
THRESHOLD = 100   # only audit apps with > this many matches
SAMPLE_PER_APP = 8  # sample size
ABSTRACT_SNIPPET = 250  # chars of abstract to show


def find_matching_patterns(title: str | None, abstract: str | None,
                           patterns: list[str]) -> list[str]:
    """Return the list of patterns that match the given grant text."""
    if not title and not abstract:
        return []
    title = (title or "").lower()
    abstract = (abstract or "").lower()
    matches = []
    for p in patterns:
        # Convert SQL ILIKE pattern to a Python substring match
        # %foo%bar% means "foo" then anything then "bar"
        # Strip leading/trailing %, split on %, all parts must appear in order.
        parts = [s for s in p.strip("%").lower().split("%") if s]
        if not parts:
            continue
        for text in (title, abstract):
            pos = 0
            ok = True
            for part in parts:
                idx = text.find(part, pos)
                if idx < 0:
                    ok = False
                    break
                pos = idx + len(part)
            if ok:
                matches.append(p)
                break
    return matches


def main() -> None:
    fs = duckdb.connect(DB_PATH, read_only=True)
    fs.execute("SET enable_progress_bar = false")
    qa = duckdb.connect(QA_DB_PATH, read_only=True)

    # Find over-matched apps
    over_matched = qa.execute("""
        SELECT a.id, a.name, a.domain, fl.grant_count
        FROM application a JOIN funding_link fl ON a.id = fl.application_id
        WHERE fl.grant_count > ?
        ORDER BY fl.grant_count DESC
    """, [THRESHOLD]).fetchall()

    print(f"Auditing {len(over_matched)} over-matched apps (>{THRESHOLD} matches)")

    out_path = Path(__file__).parent.parent / "data" / "qa_fp_audit.md"
    lines: list[str] = [
        "# QA Matcher False Positive Audit",
        "",
        f"For each application with >{THRESHOLD} matches, {SAMPLE_PER_APP} random grants are sampled.",
        "Review each: ✓ = true positive, ✗ = false positive.",
        "",
    ]

    for app_id, app_name, domain, count in over_matched:
        patterns = APPLICATION_KEYWORDS.get(app_name)
        if not patterns:
            continue
        where = _build_where_clause(patterns)

        rows = fs.execute(f"""
            SELECT project_title, abstract, source, source_id
            FROM grant_award_deduped
            WHERE {where}
            ORDER BY RANDOM()
            LIMIT {SAMPLE_PER_APP}
        """).fetchall()

        lines.append(f"## [{domain}] {app_name} — {count} matches")
        lines.append("")
        lines.append(f"<details>")
        lines.append(f"<summary>{len(patterns)} patterns</summary>")
        for p in patterns:
            lines.append(f"- `{p}`")
        lines.append(f"</details>")
        lines.append("")

        for i, (title, abstract, source, sid) in enumerate(rows, 1):
            matched = find_matching_patterns(title, abstract, patterns)
            snippet = (abstract or "")[:ABSTRACT_SNIPPET]
            if abstract and len(abstract) > ABSTRACT_SNIPPET:
                snippet += "..."

            # Highlight which fields matched
            in_title = any(p for p in matched if find_matching_patterns(title, "", [p]))
            in_abs = any(p for p in matched if find_matching_patterns("", abstract, [p]))
            field = []
            if in_title:
                field.append("title")
            if in_abs:
                field.append("abstract")
            field_str = "+".join(field)

            lines.append(f"**{i}.** [ ] [{source}] {sid}  *(via {field_str})*")
            lines.append(f"")
            lines.append(f"  - **title**: {title or '*(no title)*'}")
            if snippet:
                lines.append(f"  - **abstract**: {snippet}")
            if matched:
                lines.append(f"  - **matched patterns**: {', '.join(f'`{p}`' for p in matched[:3])}")
            lines.append("")

        lines.append("---")
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {len(lines)} lines to {out_path}")
    fs.close()
    qa.close()


if __name__ == "__main__":
    main()
