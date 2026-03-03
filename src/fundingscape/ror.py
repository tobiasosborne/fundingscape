"""ROR (Research Organization Registry) integration for institution matching.

Downloads the ROR data dump and matches institution names to canonical
ROR IDs using exact + fuzzy matching. The ROR dump is cached locally.

Usage:
    from fundingscape.ror import build_ror_index, match_institution

    index = build_ror_index("data/cache/ror/ror-data.json")
    result = match_institution(index, "Technische Universität München")
    # -> {"ror_id": "https://ror.org/02kkvpp62", "name": "Technical University of Munich", "score": 100}
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path

from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


def _normalize(name: str) -> str:
    """Normalize an institution name for matching.

    Lowercases, strips accents, removes punctuation, collapses whitespace.
    """
    # NFKD decomposition + strip combining marks (accents)
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # Remove common noise words and punctuation
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class RORIndex:
    """In-memory index of ROR organizations for fast matching."""

    def __init__(self, orgs: list[dict]):
        # Map normalized name -> (ror_id, display_name, country_code)
        self.exact: dict[str, tuple[str, str, str | None]] = {}
        # List of (normalized_name, ror_id, display_name, country_code) for fuzzy
        self.candidates: list[tuple[str, str, str, str | None]] = []

        for org in orgs:
            ror_id = org["id"]
            country = None
            for loc in org.get("locations", []):
                gd = loc.get("geonames_details", {})
                country = gd.get("country_code")
                break

            display_name = ""
            for name_entry in org.get("names", []):
                if "ror_display" in name_entry.get("types", []):
                    display_name = name_entry["value"]
                    break

            for name_entry in org.get("names", []):
                val = name_entry["value"]
                norm = _normalize(val)
                if not norm:
                    continue

                entry = (ror_id, display_name or val, country)
                self.exact[norm] = entry
                self.candidates.append((norm, ror_id, display_name or val, country))

        # Deduplicate candidates
        seen = set()
        unique = []
        for item in self.candidates:
            if item[0] not in seen:
                seen.add(item[0])
                unique.append(item)
        self.candidates = unique

        # Pre-extract candidate names for rapidfuzz
        self._candidate_names = [c[0] for c in self.candidates]

        logger.info(
            "ROR index: %d orgs, %d exact entries, %d fuzzy candidates",
            len(orgs), len(self.exact), len(self.candidates),
        )

    def match_exact(self, name: str) -> dict | None:
        """Fast exact match only (after normalization)."""
        norm = _normalize(name)
        if not norm:
            return None
        if norm in self.exact:
            ror_id, display, country = self.exact[norm]
            return {"ror_id": ror_id, "name": display, "score": 100, "country": country}
        return None

    def match(
        self,
        name: str,
        country_hint: str | None = None,
        score_cutoff: int = 85,
    ) -> dict | None:
        """Match an institution name to a ROR organization.

        Returns dict with ror_id, name, score, country or None.
        """
        # Try exact first
        result = self.match_exact(name)
        if result:
            return result

        norm = _normalize(name)
        if not norm or len(norm) < 5:
            return None

        # Fuzzy match
        result = process.extractOne(
            norm,
            self._candidate_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_cutoff,
        )
        if result:
            matched_name, score, idx = result
            _, ror_id, display, country = self.candidates[idx]
            return {"ror_id": ror_id, "name": display, "score": score, "country": country}

        return None


def build_ror_index(json_path: str | Path) -> RORIndex:
    """Load ROR JSON dump and build an in-memory matching index."""
    path = Path(json_path)
    logger.info("Loading ROR data from %s", path)
    with open(path) as f:
        orgs = json.load(f)
    return RORIndex(orgs)
