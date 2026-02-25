"""Tests for DFG GEPRIS scraper."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from fundingscape.sources.gepris import (
    _search_projects,
    _fetch_project_detail,
    SOURCE_ID,
    QUANTUM_KEYWORDS,
)

SAMPLE_SEARCH_HTML = """
<html>
<body>
<div class="content_frame search_results">
  <div class="results">
    <h2><a href="/gepris/projekt/12345678">Quantum Error Correction in Topological Codes</a></h2>
  </div>
  <div class="results">
    <h2><a href="/gepris/projekt/87654321">Many-Body Quantum Entanglement Dynamics</a></h2>
  </div>
  <div class="results">
    <h2><span>No link here — should be skipped</span></h2>
  </div>
</div>
</body>
</html>
"""

SAMPLE_DETAIL_HTML = """
<html>
<body>
<div id="detailseite">
  <div class="contentWrap">
    <h1 class="facelift">Quantum Error Correction in Topological Codes</h1>
    <div class="details">
      <span class="name">Applicant</span>
      <span class="value">Professor Dr. Max Mustermann</span>
      <span class="name">Institution</span>
      <span class="value">Leibniz Universität Hannover</span>
      <span class="name">Subject Area</span>
      <span class="value">Theoretical Condensed Matter Physics; Optics, Quantum Optics</span>
      <span class="name">DFG Programme</span>
      <span class="value">Research Grants</span>
      <span class="name">Term</span>
      <span class="value">from 2020 to 2025</span>
    </div>
    <div class="content_frame">
      This project investigates topological approaches to quantum error correction
      using surface codes and color codes for fault-tolerant quantum computing.
    </div>
  </div>
</div>
</body>
</html>
"""

SAMPLE_DETAIL_HTML_WITH_FUNDING = """
<html>
<body>
<div id="detailseite">
  <div class="contentWrap">
    <h1 class="facelift">Quantum Computing Platform</h1>
    <div class="details">
      <span class="name">Spokesperson</span>
      <span class="value">Dr. Anna Schmidt</span>
      <span class="name">Einrichtung</span>
      <span class="value">Technische Universität München</span>
      <span class="name">Overall Funding</span>
      <span class="value">1.250.000 EUR</span>
      <span class="name">Förderung</span>
      <span class="value">2019 - 2024</span>
      <span class="name">Fachgebiet</span>
      <span class="value">Computer Science; Physics</span>
    </div>
  </div>
</div>
</body>
</html>
"""

SAMPLE_DETAIL_HTML_MINIMAL = """
<html>
<body>
<h1 class="facelift">Minimal Project</h1>
</body>
</html>
"""


def _make_mock_client(html: str) -> MagicMock:
    client = MagicMock()
    client.fetch_text.return_value = html
    return client


class TestSearchResultsParsing:
    def test_parse_search_results(self):
        client = _make_mock_client(SAMPLE_SEARCH_HTML)
        results = _search_projects(client, "quantum computing")
        assert len(results) == 2

    def test_project_ids_extracted(self):
        client = _make_mock_client(SAMPLE_SEARCH_HTML)
        results = _search_projects(client, "quantum")
        ids = {r["id"] for r in results}
        assert "12345678" in ids
        assert "87654321" in ids

    def test_titles_extracted(self):
        client = _make_mock_client(SAMPLE_SEARCH_HTML)
        results = _search_projects(client, "quantum")
        titles = {r["title"] for r in results}
        assert "Quantum Error Correction in Topological Codes" in titles

    def test_empty_search_results(self):
        client = _make_mock_client("<html><body><div id='liste'></div></body></html>")
        results = _search_projects(client, "nonexistent")
        assert results == []

    def test_search_error_returns_empty(self):
        client = MagicMock()
        client.fetch_text.side_effect = Exception("Connection refused")
        results = _search_projects(client, "quantum")
        assert results == []


class TestDetailPageParsing:
    def test_parse_detail_page(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert grant is not None
        assert grant.project_title == "Quantum Error Correction in Topological Codes"
        assert grant.project_id == "12345678"
        assert grant.source == SOURCE_ID
        assert grant.source_id == "gepris_12345678"

    def test_pi_name_from_applicant(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert grant.pi_name == "Professor Dr. Max Mustermann"

    def test_pi_name_from_spokesperson_fallback(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        assert grant.pi_name == "Dr. Anna Schmidt"

    def test_institution_extracted(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert grant.pi_institution == "Leibniz Universität Hannover"

    def test_institution_german_fallback(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        assert grant.pi_institution == "Technische Universität München"

    def test_abstract_extracted(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert "topological approaches" in grant.abstract

    def test_date_parsing_from_to(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert grant.start_date == date(2020, 1, 1)
        assert grant.end_date == date(2025, 12, 31)

    def test_date_parsing_dash(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        assert grant.start_date == date(2019, 1, 1)
        assert grant.end_date == date(2024, 12, 31)

    def test_funding_amount_german_format(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        assert grant.total_funding == 1250000.0

    def test_country_hardcoded_de(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert grant.pi_country == "DE"

    def test_status_computed_from_dates(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        # end_date is 2024-12-31, which is in the past
        assert grant.status == "completed"

    def test_subject_area_parsed(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        assert "Theoretical Condensed Matter Physics" in grant.topic_keywords
        assert "Optics, Quantum Optics" in grant.topic_keywords

    def test_subject_area_german_fallback(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_WITH_FUNDING)
        grant = _fetch_project_detail(client, "99999")
        assert "Computer Science" in grant.topic_keywords

    def test_minimal_detail_page(self):
        client = _make_mock_client(SAMPLE_DETAIL_HTML_MINIMAL)
        grant = _fetch_project_detail(client, "11111")
        assert grant is not None
        assert grant.project_title == "Minimal Project"
        assert grant.pi_name is None
        assert grant.total_funding is None

    def test_fetch_error_returns_none(self):
        client = MagicMock()
        client.fetch_text.side_effect = Exception("Timeout")
        grant = _fetch_project_detail(client, "12345678")
        assert grant is None


class TestDatabaseLoading:
    def test_load_gepris_grant(self, db):
        from fundingscape.db import upsert_grant

        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        upsert_grant(db, grant)

        count = db.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'gepris'"
        ).fetchone()[0]
        assert count == 1

        row = db.execute(
            "SELECT project_title, pi_name, pi_country FROM grant_award WHERE source_id = 'gepris_12345678'"
        ).fetchone()
        assert "Topological" in row[0]
        assert "Mustermann" in row[1]
        assert row[2] == "DE"

    def test_upsert_idempotent(self, db):
        from fundingscape.db import upsert_grant

        client = _make_mock_client(SAMPLE_DETAIL_HTML)
        grant = _fetch_project_detail(client, "12345678")
        upsert_grant(db, grant)
        upsert_grant(db, grant)

        count = db.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'gepris'"
        ).fetchone()[0]
        assert count == 1
