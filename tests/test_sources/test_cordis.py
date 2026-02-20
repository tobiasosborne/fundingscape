"""Tests for CORDIS bulk CSV ingestion."""

import csv
import io
import zipfile
from datetime import date
from decimal import Decimal

import pytest

from fundingscape.sources.cordis import (
    _parse_date,
    _parse_decimal,
    _parse_status,
    _parse_projects_csv,
    _parse_organizations_csv,
    _enrich_with_organizations,
)


SAMPLE_PROJECT_CSV = '''"id";"acronym";"status";"title";"startDate";"endDate";"totalCost";"ecMaxContribution";"legalBasis";"topics";"ecSignatureDate";"frameworkProgramme";"masterCall";"subCall";"fundingScheme";"objective";"contentUpdateDate";"rcn";"grantDoi";"keywords"
"101234567";"QUANTOP";"SIGNED";"Topological Quantum Computing with Anyons";"2024-01-01";"2028-12-31";"2500000";"2500000";"HORIZON.1.1";"ERC-2023-STG";"2023-12-01";"HORIZON";"ERC-2023-STG";"ERC-2023-STG";"HORIZON-ERC";"This project explores topological quantum computing...";"2024-01-15 10:00:00";"123456";"10.3030/101234567";"quantum computing, topology, anyons"
"101234568";"MBQM";"CLOSED";"Many-Body Quantum Mechanics";"2020-03-01";"2024-02-28";"1500000";"1200000";"HORIZON.1.1";"H2020-MSCA-IF";"2020-02-15";"HORIZON";"H2020-MSCA-IF";"H2020-MSCA-IF";"HORIZON-MSCA";"Research on many-body systems...";"2024-03-01 09:00:00";"123457";"";"quantum, many-body, physics"
"101234569";"AGRIBOT";"SIGNED";"Agricultural Robotics";"2024-06-01";"2027-05-31";"3000000";"3000000";"HORIZON.2.6";"CL6-2023-01";"2023-11-01";"HORIZON";"CL6-2023";"CL6-2023-01";"HORIZON-RIA";"Developing robots for agriculture...";"2024-06-15 14:00:00";"123458";"";"agriculture, robotics"
'''

SAMPLE_ORG_CSV = '''"projectID";"projectAcronym";"organisationID";"vatNumber";"name";"shortName";"SME";"activityType";"street";"postCode";"city";"country";"nutsCode";"geolocation";"organizationURL";"contactForm";"contentUpdateDate";"rcn";"order";"role";"ecContribution";"netEcContribution";"totalCost";"endOfParticipation";"active"
"101234567";"QUANTOP";"999619146";"";"LEIBNIZ UNIVERSITAET HANNOVER";"LUH";"false";"HES";"WELFENGARTEN 1";"30167";"HANNOVER";"DE";"DE92";"52.38,9.72";"";"";"";"100001";"1";"coordinator";"2500000";"2500000";"2500000";"";"true"
"101234568";"MBQM";"999619146";"";"LEIBNIZ UNIVERSITAET HANNOVER";"LUH";"false";"HES";"WELFENGARTEN 1";"30167";"HANNOVER";"DE";"DE92";"52.38,9.72";"";"";"";"100002";"1";"coordinator";"1200000";"1200000";"1500000";"";"true"
"101234569";"AGRIBOT";"999643492";"";"SOME OTHER UNIVERSITY";"SOU";"false";"HES";"MAIN ST 1";"12345";"BERLIN";"DE";"DE30";"52.52,13.40";"";"";"";"100003";"1";"coordinator";"3000000";"3000000";"3000000";"";"true"
'''


class TestParsers:
    def test_parse_date_valid(self):
        assert _parse_date("2024-01-01") == date(2024, 1, 1)

    def test_parse_date_empty(self):
        assert _parse_date("") is None

    def test_parse_date_invalid(self):
        assert _parse_date("not-a-date") is None

    def test_parse_decimal_valid(self):
        assert _parse_decimal("2500000") == Decimal("2500000")

    def test_parse_decimal_empty(self):
        assert _parse_decimal("") is None

    def test_parse_status(self):
        assert _parse_status("SIGNED") == "active"
        assert _parse_status("CLOSED") == "completed"
        assert _parse_status("TERMINATED") == "terminated"
        assert _parse_status("UNKNOWN") is None


class TestProjectParsing:
    def test_parse_projects_csv(self):
        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        assert len(grants) == 3

    def test_project_fields(self):
        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        qt = grants[0]  # QUANTOP
        assert qt.acronym == "QUANTOP"
        assert qt.project_title == "Topological Quantum Computing with Anyons"
        assert qt.total_funding == Decimal("2500000")
        assert qt.eu_contribution == Decimal("2500000")
        assert qt.start_date == date(2024, 1, 1)
        assert qt.end_date == date(2028, 12, 31)
        assert qt.status == "active"
        assert qt.source == "cordis_bulk"
        assert qt.source_id == "horizon_101234567"
        assert "quantum computing" in qt.topic_keywords

    def test_closed_project(self):
        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        mb = grants[1]  # MBQM
        assert mb.status == "completed"
        assert mb.total_funding == Decimal("1500000")
        assert mb.eu_contribution == Decimal("1200000")

    def test_keywords_parsed(self):
        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        qt = grants[0]
        assert "topology" in qt.topic_keywords
        assert "anyons" in qt.topic_keywords


class TestOrganizationParsing:
    def test_parse_organizations(self):
        coordinators = _parse_organizations_csv(SAMPLE_ORG_CSV)
        assert "101234567" in coordinators
        assert coordinators["101234567"]["pi_institution"] == "LEIBNIZ UNIVERSITAET HANNOVER"
        assert coordinators["101234567"]["pi_country"] == "DE"

    def test_enrich_grants(self):
        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        coordinators = _parse_organizations_csv(SAMPLE_ORG_CSV)
        _enrich_with_organizations(grants, coordinators)

        qt = grants[0]  # QUANTOP
        assert qt.pi_institution == "LEIBNIZ UNIVERSITAET HANNOVER"
        assert qt.pi_country == "DE"


class TestDatabaseLoading:
    def test_load_into_db(self, db):
        """Test that parsed grants can be loaded into the database."""
        from fundingscape.db import upsert_grant

        grants = _parse_projects_csv(SAMPLE_PROJECT_CSV, "horizon")
        for g in grants:
            upsert_grant(db, g)

        count = db.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
        assert count == 3

        qt = db.execute(
            "SELECT project_title FROM grant_award WHERE source_id = 'horizon_101234567'"
        ).fetchone()
        assert "Topological" in qt[0]
