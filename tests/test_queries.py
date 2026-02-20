"""Tests for analytical queries."""

from datetime import date, timedelta
from decimal import Decimal

import pytest

from fundingscape.db import create_tables, insert_call, upsert_grant
from fundingscape.models import Call, GrantAward
from fundingscape.queries import (
    funding_landscape_summary,
    historical_trends,
    income_projection,
    open_calls_by_deadline,
    top_pis_by_field,
)


@pytest.fixture
def loaded_db(db):
    """DB with sample data for query testing."""
    # Insert some grants
    grants = [
        GrantAward(
            project_title="Quantum Computing Platform",
            project_id="QC1",
            acronym="QCP",
            pi_institution="LEIBNIZ UNIVERSITAT HANNOVER",
            pi_country="DE",
            start_date=date(2024, 1, 1),
            end_date=date(2028, 12, 31),
            total_funding=Decimal("2000000"),
            eu_contribution=Decimal("2000000"),
            status="active",
            topic_keywords=["quantum", "computing"],
            source="cordis_bulk",
            source_id="test_1",
        ),
        GrantAward(
            project_title="Topological Anyons Research",
            project_id="TA1",
            acronym="TOPO",
            pi_institution="LEIBNIZ UNIVERSITAT HANNOVER",
            pi_country="DE",
            start_date=date(2023, 6, 1),
            end_date=date(2026, 5, 31),
            total_funding=Decimal("1500000"),
            eu_contribution=Decimal("1500000"),
            status="active",
            topic_keywords=["quantum", "topology"],
            source="cordis_bulk",
            source_id="test_2",
        ),
        GrantAward(
            project_title="Classical AI Project",
            project_id="AI1",
            acronym="CLAAI",
            pi_institution="TU BERLIN",
            pi_country="DE",
            start_date=date(2024, 1, 1),
            end_date=date(2026, 12, 31),
            total_funding=Decimal("500000"),
            status="active",
            topic_keywords=["AI", "machine_learning"],
            source="cordis_bulk",
            source_id="test_3",
        ),
    ]
    for g in grants:
        upsert_grant(db, g)

    # Insert some calls
    calls = [
        Call(
            call_identifier="HORIZON-QUANTUM-2025",
            title="Quantum Technologies Call",
            status="open",
            deadline=date.today() + timedelta(days=90),
            topic_keywords=["quantum", "computing"],
            framework_programme="HORIZON",
            source="ft_portal",
            source_id="call_1",
        ),
        Call(
            call_identifier="ERC-2025-STG",
            title="ERC Starting Grants",
            status="open",
            deadline=date.today() + timedelta(days=60),
            topic_keywords=["research", "frontier"],
            framework_programme="HORIZON",
            source="ft_portal",
            source_id="call_2",
        ),
        Call(
            call_identifier="AGRI-2025",
            title="Agriculture Innovation",
            status="open",
            deadline=date.today() + timedelta(days=30),
            topic_keywords=["agriculture"],
            framework_programme="AGRIP",
            source="ft_portal",
            source_id="call_3",
        ),
    ]
    for c in calls:
        insert_call(db, c)

    return db


class TestOpenCalls:
    def test_quantum_calls(self, loaded_db):
        calls = open_calls_by_deadline(loaded_db, months_ahead=6, quantum_only=True)
        # Should find quantum call and ERC, but not agriculture
        identifiers = [c["identifier"] for c in calls]
        assert "HORIZON-QUANTUM-2025" in identifiers
        assert "ERC-2025-STG" in identifiers
        assert "AGRI-2025" not in identifiers

    def test_all_calls(self, loaded_db):
        calls = open_calls_by_deadline(loaded_db, months_ahead=6, quantum_only=False)
        assert len(calls) == 3


class TestFundingLandscape:
    def test_summary(self, loaded_db):
        summary = funding_landscape_summary(loaded_db)
        assert len(summary) > 0
        assert summary[0]["source"] == "cordis_bulk"
        assert summary[0]["num_grants"] == 3


class TestIncomeProjection:
    def test_luh_projection(self, loaded_db):
        proj = income_projection(loaded_db, "%HANNOVER%")
        assert len(proj) > 0
        # Should have entries for 2024-2028 (grant periods)
        years = [p["year"] for p in proj]
        assert 2024 in years
        assert 2026 in years


class TestTopPIs:
    def test_quantum_pis(self, loaded_db):
        pis = top_pis_by_field(loaded_db, "quantum", limit=10)
        assert len(pis) > 0
        # LUH should be top (2 quantum grants)
        assert pis[0]["institution"] == "LEIBNIZ UNIVERSITAT HANNOVER"
        assert pis[0]["num_grants"] == 2


class TestHistoricalTrends:
    def test_quantum_trends(self, loaded_db):
        trends = historical_trends(loaded_db, "quantum")
        assert len(trends) > 0
        years = [t["year"] for t in trends]
        assert 2024 in years
