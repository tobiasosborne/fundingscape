"""Tests for OpenAIRE API integration."""

from datetime import date
from decimal import Decimal

import pytest

from fundingscape.sources.openaire import (
    _parse_date,
    _parse_project,
    _funder_to_country,
)


SAMPLE_RESULT = {
    "header": {
        "dri:objIdentifier": {
            "$": "ukri________::578c1c01bdce5a597c95f681775c6386"
        },
    },
    "metadata": {
        "oaf:entity": {
            "oaf:project": {
                "collectedfrom": {
                    "@name": "UK Research and Innovation",
                    "@id": "openaire____::92ed35af5f7f859de570d1d2919c09a4",
                },
                "code": {"$": "EP/W032643/1"},
                "title": {"$": "Distributed Quantum Computing and Applications"},
                "startdate": {"$": "2022-03-31"},
                "enddate": {"$": "2026-03-30"},
                "summary": {"$": "Quantum computing project for distributed systems..."},
                "currency": {"$": "GBP"},
                "totalcost": {"$": "0.0"},
                "fundedamount": {"$": 3049360.0},
                "fundingtree": {
                    "funder": {
                        "shortname": {"$": "UKRI"},
                        "name": {"$": "UK Research and Innovation"},
                    },
                    "funding_level_0": {
                        "name": {"$": "EPSRC"},
                    },
                },
            }
        }
    },
}

SAMPLE_DFG_RESULT = {
    "header": {
        "dri:objIdentifier": {"$": "dfgf________::abc123"},
    },
    "metadata": {
        "oaf:entity": {
            "oaf:project": {
                "collectedfrom": {
                    "@name": "DFG",
                    "@id": "openaire____::dfg123",
                },
                "code": {"$": "239028562"},
                "title": {"$": "Quantum Error Correction for Topological Codes"},
                "startdate": {"$": "2020-01-01"},
                "enddate": {"$": "2024-12-31"},
                "summary": {"$": "Research on topological quantum error correction."},
                "currency": {"$": "EUR"},
                "fundedamount": {"$": 450000.0},
                "fundingtree": {
                    "funder": {
                        "shortname": {"$": "DFG"},
                        "name": {"$": "Deutsche Forschungsgemeinschaft"},
                    },
                },
            }
        }
    },
}


class TestParseDate:
    def test_valid_date(self):
        assert _parse_date("2022-03-31") == date(2022, 3, 31)

    def test_empty(self):
        assert _parse_date("") is None

    def test_invalid(self):
        assert _parse_date("not-a-date") is None


class TestFunderToCountry:
    def test_known_funders(self):
        assert _funder_to_country("DFG") == "DE"
        assert _funder_to_country("UKRI") == "GB"
        assert _funder_to_country("NSF") == "US"
        assert _funder_to_country("SNSF") == "CH"
        assert _funder_to_country("ANR") == "FR"
        assert _funder_to_country("FWF") == "AT"
        assert _funder_to_country("NWO") == "NL"

    def test_unknown_funder(self):
        assert _funder_to_country("UNKNOWN") is None


class TestParseProject:
    def test_ukri_project(self):
        grant = _parse_project(SAMPLE_RESULT)
        assert grant is not None
        assert grant.project_title == "Distributed Quantum Computing and Applications"
        assert grant.project_id == "EP/W032643/1"
        assert grant.start_date == date(2022, 3, 31)
        assert grant.end_date == date(2026, 3, 30)
        assert grant.total_funding == Decimal("3049360")
        assert grant.currency == "GBP"
        assert grant.source == "openaire"
        assert "UKRI" in grant.source_id
        assert grant.status == "active"  # ends 2026

    def test_dfg_project(self):
        grant = _parse_project(SAMPLE_DFG_RESULT)
        assert grant is not None
        assert grant.project_id == "239028562"
        assert grant.total_funding == Decimal("450000")
        assert grant.pi_country == "DE"
        assert grant.status == "completed"  # ended 2024

    def test_empty_result(self):
        assert _parse_project({}) is None

    def test_no_title(self):
        result = {
            "metadata": {
                "oaf:entity": {
                    "oaf:project": {
                        "code": {"$": "123"},
                        "title": {"$": ""},
                    }
                }
            }
        }
        assert _parse_project(result) is None


class TestDatabaseLoading:
    def test_load_parsed_grants(self, db):
        from fundingscape.db import upsert_grant

        grant = _parse_project(SAMPLE_RESULT)
        assert grant is not None
        gid = upsert_grant(db, grant)
        assert gid >= 1

        row = db.execute(
            "SELECT project_title, total_funding FROM grant_award WHERE id = ?",
            [gid],
        ).fetchone()
        assert "Distributed Quantum" in row[0]
        assert row[1] == 3049360.0
