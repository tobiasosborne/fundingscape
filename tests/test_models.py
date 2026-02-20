"""Tests for pydantic data models."""

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from fundingscape.models import (
    Call,
    EligibilityProfile,
    Funder,
    FundingInstrument,
    GrantAward,
)


class TestFunder:
    def test_valid_funder(self):
        f = Funder(name="European Commission", type="eu", country="EU")
        assert f.name == "European Commission"
        assert f.type == "eu"

    def test_invalid_type(self):
        with pytest.raises(ValidationError):
            Funder(name="Test", type="invalid")

    def test_minimal_funder(self):
        f = Funder(name="DFG", type="federal_de")
        assert f.short_name is None
        assert f.website is None


class TestFundingInstrument:
    def test_valid_instrument(self):
        fi = FundingInstrument(
            name="ERC Starting Grant",
            source="cordis",
            typical_amount_min=Decimal("1000000"),
            typical_amount_max=Decimal("1500000"),
            success_rate=0.12,
            recurrence="annual",
            relevance_tags=["quantum", "physics"],
        )
        assert fi.name == "ERC Starting Grant"
        assert fi.success_rate == 0.12
        assert fi.currency == "EUR"

    def test_success_rate_bounds(self):
        with pytest.raises(ValidationError):
            FundingInstrument(name="Bad", source="test", success_rate=1.5)
        with pytest.raises(ValidationError):
            FundingInstrument(name="Bad", source="test", success_rate=-0.1)

    def test_zero_success_rate(self):
        fi = FundingInstrument(name="Hard", source="test", success_rate=0.0)
        assert fi.success_rate == 0.0


class TestCall:
    def test_valid_call(self):
        c = Call(
            title="Quantum Computing Call 2025",
            status="open",
            deadline=date(2025, 9, 15),
            call_identifier="HORIZON-CL4-2025-DIGITAL-01-22",
            source="ft_portal",
            source_id="12345",
            budget_total=Decimal("50000000"),
        )
        assert c.status == "open"
        assert c.deadline == date(2025, 9, 15)

    def test_invalid_status(self):
        with pytest.raises(ValidationError):
            Call(title="Bad", status="maybe", source="test")

    def test_with_keywords(self):
        c = Call(
            title="Test",
            status="forthcoming",
            source="test",
            topic_keywords=["quantum", "topology", "computing"],
        )
        assert len(c.topic_keywords) == 3


class TestGrantAward:
    def test_valid_grant(self):
        g = GrantAward(
            project_title="Topological Quantum Computing",
            source="cordis",
            source_id="101234567",
            pi_name="Jane Doe",
            pi_institution="LUH",
            total_funding=Decimal("1500000"),
            start_date=date(2023, 1, 1),
            end_date=date(2028, 12, 31),
            status="active",
        )
        assert g.total_funding == Decimal("1500000")

    def test_coerce_decimal_from_string(self):
        g = GrantAward(
            project_title="Test",
            source="test",
            source_id="1",
            total_funding="1234567.89",
        )
        assert g.total_funding == Decimal("1234567.89")

    def test_coerce_decimal_from_int(self):
        g = GrantAward(
            project_title="Test",
            source="test",
            source_id="1",
            total_funding=1000000,
        )
        assert g.total_funding == Decimal("1000000")

    def test_empty_string_funding(self):
        g = GrantAward(
            project_title="Test",
            source="test",
            source_id="1",
            total_funding="",
        )
        assert g.total_funding is None

    def test_with_partners(self):
        g = GrantAward(
            project_title="Collaborative",
            source="cordis",
            source_id="2",
            partners=[
                {"name": "TU Delft", "country": "NL", "funding": 500000},
                {"name": "ETH Zurich", "country": "CH", "funding": 300000},
            ],
        )
        assert len(g.partners) == 2

    def test_serialization_roundtrip(self):
        g = GrantAward(
            project_title="Roundtrip Test",
            source="test",
            source_id="rt1",
            total_funding=Decimal("999.99"),
        )
        data = g.model_dump(mode="json")
        g2 = GrantAward.model_validate(data)
        assert g2.project_title == g.project_title
        assert g2.source_id == g.source_id


class TestEligibilityProfile:
    def test_default_profile(self):
        p = EligibilityProfile(
            profile_name="PI Profile",
            pi_career_stage="mid",
            research_keywords=["quantum", "many-body", "formal_methods"],
        )
        assert p.institution == "Leibniz Universität Hannover"
        assert p.institution_country == "DE"
        assert not p.is_sme

    def test_sme_profile(self):
        p = EligibilityProfile(
            profile_name="Innovailia",
            is_sme=True,
            company_name="Innovailia UG",
            company_country="DE",
        )
        assert p.is_sme
        assert p.company_name == "Innovailia UG"
