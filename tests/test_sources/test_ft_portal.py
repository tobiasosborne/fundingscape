"""Tests for EU Funding & Tenders Portal ingestion."""

from datetime import date

import pytest

from fundingscape.sources.ft_portal import (
    _epoch_ms_to_date,
    _map_status,
    _extract_tags,
    parse_calls,
)


SAMPLE_FT_DATA = {
    "fundingData": {
        "GrantTenderObj": [
            {
                "type": 1,
                "ccm2Id": 99999001,
                "identifier": "HORIZON-CL4-2025-QUANTUM-01-01",
                "title": "Quantum Computing Platforms",
                "publicationDateLong": 1700000000000,
                "plannedOpeningDateLong": 1700000000000,
                "callIdentifier": "HORIZON-CL4-2025-QUANTUM-01",
                "callTitle": "Quantum Technologies Call 2025",
                "deadlineDatesLong": [1726358400000],  # 2024-09-15
                "frameworkProgramme": {
                    "id": 1,
                    "abbreviation": "HORIZON",
                    "description": "Horizon Europe",
                },
                "status": {
                    "id": 1,
                    "abbreviation": "Open",
                    "description": "Open",
                },
                "tags": ["quantum computing", "quantum technology", "digital"],
                "sme": False,
            },
            {
                "type": 1,
                "ccm2Id": 99999002,
                "identifier": "ERC-2025-STG",
                "title": "ERC Starting Grants 2025",
                "plannedOpeningDateLong": 1700000000000,
                "deadlineDatesLong": [1728950400000],
                "frameworkProgramme": {
                    "id": 2,
                    "abbreviation": "HORIZON",
                    "description": "Horizon Europe",
                },
                "status": {
                    "abbreviation": "Forthcoming",
                },
                "tags": ["research", "frontier"],
            },
            {
                "type": 1,
                "ccm2Id": 99999003,
                "identifier": "AGRIP-SIMPLE-2025",
                "title": "Agriculture Promotion",
                "plannedOpeningDateLong": 1700000000000,
                "deadlineDatesLong": [],
                "frameworkProgramme": {
                    "abbreviation": "AGRIP2027",
                },
                "status": {
                    "abbreviation": "Closed",
                },
                "tags": ["agriculture"],
            },
        ]
    }
}


class TestHelpers:
    def test_epoch_ms_to_date(self):
        # 1726358400000 = 2024-09-15 UTC
        d = _epoch_ms_to_date(1726358400000)
        assert d == date(2024, 9, 15)

    def test_epoch_ms_none(self):
        assert _epoch_ms_to_date(None) is None

    def test_map_status_open(self):
        assert _map_status({"abbreviation": "Open"}) == "open"

    def test_map_status_closed(self):
        assert _map_status({"abbreviation": "Closed"}) == "closed"

    def test_map_status_forthcoming(self):
        assert _map_status({"abbreviation": "Forthcoming"}) == "forthcoming"

    def test_map_status_none(self):
        assert _map_status(None) == "closed"

    def test_extract_tags(self):
        entry = {"tags": ["quantum", "computing", "digital"]}
        assert _extract_tags(entry) == ["quantum", "computing", "digital"]

    def test_extract_tags_empty(self):
        assert _extract_tags({}) == []


class TestParseCalls:
    def test_parse_all_calls(self):
        calls = parse_calls(SAMPLE_FT_DATA)
        assert len(calls) == 3

    def test_quantum_call_fields(self):
        calls = parse_calls(SAMPLE_FT_DATA)
        qc = calls[0]
        assert qc.call_identifier == "HORIZON-CL4-2025-QUANTUM-01-01"
        assert qc.title == "Quantum Computing Platforms"
        assert qc.status == "open"
        assert qc.framework_programme == "HORIZON"
        assert "quantum computing" in qc.topic_keywords
        assert qc.source == "ft_portal"

    def test_erc_call(self):
        calls = parse_calls(SAMPLE_FT_DATA)
        erc = calls[1]
        assert erc.call_identifier == "ERC-2025-STG"
        assert erc.status == "forthcoming"

    def test_call_with_no_deadline(self):
        calls = parse_calls(SAMPLE_FT_DATA)
        agri = calls[2]
        assert agri.deadline is None
        assert agri.status == "closed"


class TestDatabaseLoading:
    def test_load_calls_into_db(self, db):
        from fundingscape.db import upsert_call

        calls = parse_calls(SAMPLE_FT_DATA)
        for c in calls:
            upsert_call(db, c)

        count = db.execute("SELECT COUNT(*) FROM call").fetchone()[0]
        assert count == 3

    def test_filter_relevant_programmes(self):
        """Test that filtering by programme works."""
        calls = parse_calls(SAMPLE_FT_DATA)
        relevant = {"HORIZON", "ERC"}
        filtered = [c for c in calls if any(
            (c.framework_programme or "").startswith(rp) for rp in relevant
        )]
        # AGRIP should be filtered out
        assert len(filtered) == 2
        assert all(c.framework_programme == "HORIZON" for c in filtered)
