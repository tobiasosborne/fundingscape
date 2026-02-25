"""Tests for BMBF Förderkatalog scraper."""

from datetime import date

import pytest

from fundingscape.sources.foerderkatalog import (
    _parse_german_date,
    _parse_german_amount,
    _parse_search_results,
    _parse_total_count,
    _result_to_grant,
    SOURCE_ID,
)


SAMPLE_RESULTS_HTML = """
<html><body>
<div>(132 Treffer)</div>
<table cellpadding="3" cellspacing="0" border="0" width="100%" aria-label="Suchergebnis">
<tr>
  <th>FKZ</th><th>Ressort</th><th>Förderempfänger</th>
  <th>Ausführende Stelle</th><th>Thema</th><th>Laufzeit</th>
  <th>Summe</th><th>Projekt</th>
</tr>
<tr>
  <td><a href="SucheAction.do?actionMode=view&fkz=13N15521">13N15521</a></td>
  <td><a href="#">BMFTR PT-VDI</a></td>
  <td><a href="#">Universität Siegen</a></td>
  <td><a href="#">Universität Siegen - Fakultät IV</a></td>
  <td><a href="#">Verbundprojekt: Skalierbarer Quantencomputer</a></td>
  <td><a href="#">01.05.202130.04.2026</a></td>
  <td><a href="#">7.142.500,00 €</a></td>
  <td><a href="#">J</a></td>
</tr>
<tr>
  <td><a href="SucheAction.do?actionMode=view&fkz=13N15522">13N15522</a></td>
  <td><a href="#">BMFTR PT-VDI</a></td>
  <td><a href="#">TU Hamburg</a></td>
  <td><a href="#">TU Hamburg - Institut für Nanostruktur</a></td>
  <td><a href="#">Verbundprojekt: Quantencomputer MIQRO</a></td>
  <td><a href="#">01.05.202130.04.2026</a></td>
  <td><a href="#">1.337.347,00 €</a></td>
  <td><a href="#">J</a></td>
</tr>
<tr>
  <td><a href="SucheAction.do?actionMode=view&fkz=99Z99999">99Z99999</a></td>
  <td><a href="#">BMBF</a></td>
  <td><a href="#">Max Planck Gesellschaft</a></td>
  <td><a href="#">MPI für Quantenoptik</a></td>
  <td><a href="#">Quantum Computing Research</a></td>
  <td><a href="#">01.01.201831.12.2022</a></td>
  <td><a href="#">500.000,00 €</a></td>
  <td><a href="#">N</a></td>
</tr>
</table>
</body></html>
"""

SAMPLE_EMPTY_RESULTS_HTML = """
<html><body>
<div>Die Suche liefert keine Ergebnisse, bitte ändern Sie die Suchkriterien.</div>
</body></html>
"""


class TestGermanDateParsing:
    def test_valid_date(self):
        assert _parse_german_date("15.03.2024") == date(2024, 3, 15)

    def test_first_of_month(self):
        assert _parse_german_date("01.01.2020") == date(2020, 1, 1)

    def test_end_of_year(self):
        assert _parse_german_date("31.12.2025") == date(2025, 12, 31)

    def test_empty_string(self):
        assert _parse_german_date("") is None

    def test_none_input(self):
        assert _parse_german_date(None) is None

    def test_invalid_date(self):
        assert _parse_german_date("32.13.2024") is None

    def test_wrong_format(self):
        assert _parse_german_date("2024-01-01") is None

    def test_with_whitespace(self):
        assert _parse_german_date("  01.06.2023  ") == date(2023, 6, 1)


class TestGermanAmountParsing:
    def test_standard_amount(self):
        assert _parse_german_amount("7.142.500,00 €") == 7142500.0

    def test_small_amount(self):
        assert _parse_german_amount("500.000,00 €") == 500000.0

    def test_amount_no_currency(self):
        assert _parse_german_amount("250000") == 250000.0

    def test_amount_with_euro_text(self):
        assert _parse_german_amount("1.250.000 EUR") == 1250000.0

    def test_amount_with_decimal_comma(self):
        assert _parse_german_amount("12.345,67") == 12345.67

    def test_empty_string(self):
        assert _parse_german_amount("") is None

    def test_none_input(self):
        assert _parse_german_amount(None) is None

    def test_just_euro_sign(self):
        assert _parse_german_amount("€") is None

    def test_amount_with_spaces(self):
        assert _parse_german_amount("  1.337.347,00 €  ") == 1337347.0


class TestTotalCount:
    def test_parse_total_count(self):
        assert _parse_total_count(SAMPLE_RESULTS_HTML) == 132

    def test_parse_count_with_nbsp(self):
        html = "<h1>(132&nbsp;Treffer)</h1>"
        assert _parse_total_count(html) == 132

    def test_parse_count_with_unicode_nbsp(self):
        html = "<h1>(132\xa0Treffer)</h1>"
        assert _parse_total_count(html) == 132

    def test_no_count(self):
        assert _parse_total_count("<html><body></body></html>") == 0


class TestSearchResultsParsing:
    def test_parse_results(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        assert len(results) == 3

    def test_fkz_extracted(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        fkzs = {r["fkz"] for r in results}
        assert "13N15521" in fkzs
        assert "13N15522" in fkzs
        assert "99Z99999" in fkzs

    def test_institution_extracted(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        r = results[0]
        assert r["institution"] == "Universität Siegen"
        assert "Fakultät IV" in r["executing_institution"]

    def test_title_extracted(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        assert "Quantencomputer" in results[0]["title"]

    def test_dates_parsed(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        r = results[0]
        assert r["start_date"] == date(2021, 5, 1)
        assert r["end_date"] == date(2026, 4, 30)

    def test_funding_parsed(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        assert results[0]["total_funding"] == 7142500.0
        assert results[1]["total_funding"] == 1337347.0

    def test_completed_project_dates(self):
        results = _parse_search_results(SAMPLE_RESULTS_HTML)
        r = results[2]  # 2018-2022 project
        assert r["start_date"] == date(2018, 1, 1)
        assert r["end_date"] == date(2022, 12, 31)

    def test_empty_results(self):
        results = _parse_search_results(SAMPLE_EMPTY_RESULTS_HTML)
        assert results == []


class TestResultToGrant:
    def test_basic_conversion(self):
        result = {
            "fkz": "13N15521",
            "ministry": "BMFTR",
            "institution": "Universität Siegen",
            "executing_institution": "Universität Siegen - Fakultät IV",
            "title": "Verbundprojekt: Skalierbarer Quantencomputer",
            "start_date": date(2021, 5, 1),
            "end_date": date(2026, 4, 30),
            "total_funding": 7142500.0,
            "is_verbund": True,
        }
        grant = _result_to_grant(result)
        assert grant.project_title == "Verbundprojekt: Skalierbarer Quantencomputer"
        assert grant.project_id == "13N15521"
        assert grant.source == SOURCE_ID
        assert grant.source_id == "foekat_13N15521"
        assert grant.pi_country == "DE"
        assert grant.currency == "EUR"
        assert grant.total_funding == 7142500.0
        assert grant.status == "active"
        assert grant.pi_institution == "Universität Siegen - Fakultät IV"

    def test_completed_project(self):
        result = {
            "fkz": "99Z99999",
            "ministry": "BMBF",
            "institution": "Max Planck Gesellschaft",
            "executing_institution": "MPI für Quantenoptik",
            "title": "Quantum Computing Research",
            "start_date": date(2018, 1, 1),
            "end_date": date(2022, 12, 31),
            "total_funding": 500000.0,
            "is_verbund": False,
        }
        grant = _result_to_grant(result)
        assert grant.status == "completed"

    def test_with_detail_enrichment(self):
        result = {
            "fkz": "13N15521",
            "ministry": "BMFTR",
            "institution": "Uni Siegen",
            "executing_institution": "Uni Siegen - Physik",
            "title": "Short title",
            "start_date": date(2021, 5, 1),
            "end_date": date(2026, 4, 30),
            "total_funding": 7142500.0,
            "is_verbund": True,
        }
        detail = {
            "Thema des geförderten Vorhabens": "Verbundprojekt: Skalierbarer Quantencomputer mit Hochfrequenz-gesteuerten gespeicherten Ionen (MIQRO) - Teilvorhaben",
            "Leistungsplansystematik": "Quantentechnologien: Quantencomputing und -simulation",
        }
        grant = _result_to_grant(result, detail)
        # Detail title is longer, should be used
        assert "MIQRO" in grant.project_title
        assert "Quantentechnologien" in grant.topic_keywords


class TestDatabaseLoading:
    def test_load_foerderkatalog_grant(self, db):
        from fundingscape.db import upsert_grant

        result = {
            "fkz": "13N15521",
            "ministry": "BMFTR",
            "institution": "Universität Siegen",
            "executing_institution": "Universität Siegen - Fakultät IV",
            "title": "Verbundprojekt: Quantencomputer",
            "start_date": date(2021, 5, 1),
            "end_date": date(2026, 4, 30),
            "total_funding": 7142500.0,
            "is_verbund": True,
        }
        grant = _result_to_grant(result)
        upsert_grant(db, grant)

        count = db.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'foerderkatalog'"
        ).fetchone()[0]
        assert count == 1

        row = db.execute(
            "SELECT project_title, pi_country, total_funding "
            "FROM grant_award WHERE source_id = 'foekat_13N15521'"
        ).fetchone()
        assert "Quantencomputer" in row[0]
        assert row[1] == "DE"
        assert row[2] == 7142500.0

    def test_upsert_idempotent(self, db):
        from fundingscape.db import upsert_grant

        result = {
            "fkz": "13N15521",
            "ministry": "BMFTR",
            "institution": "Universität Siegen",
            "executing_institution": "Universität Siegen",
            "title": "Quantencomputer Test",
            "start_date": date(2021, 5, 1),
            "end_date": date(2026, 4, 30),
            "total_funding": 100000.0,
            "is_verbund": False,
        }
        grant = _result_to_grant(result)
        upsert_grant(db, grant)
        upsert_grant(db, grant)

        count = db.execute(
            "SELECT COUNT(*) FROM grant_award WHERE source = 'foerderkatalog'"
        ).fetchone()[0]
        assert count == 1
