"""Tests for OpenAIRE bulk data loader."""

import json

import pytest

from fundingscape.sources.openaire_bulk import _parse_date


SAMPLE_PROJECT_LINE = json.dumps({
    "id": "fwf_________::8b1f2314ba8a643ad9b2383f88f1ae43",
    "code": "Y 1067",
    "acronym": "ParityQC",
    "title": "ParityQC: Parity Constraints as a Quantum Computing Toolbox",
    "websiteUrl": "https://fwf.ac.at/forschungsradar/10.55776/Y1067",
    "startDate": "2017-09-04",
    "endDate": "2024-09-03",
    "callIdentifier": None,
    "keywords": "Quantum Computing; Quantum Simulation; Many-body Physics",
    "openAccessMandateForPublications": False,
    "openAccessMandateForDataset": False,
    "subjects": None,
    "fundings": [
        {
            "shortName": "FWF",
            "name": "Austrian Science Fund (FWF)",
            "jurisdiction": "AT",
            "fundingStream": {
                "id": "FWF::FWF START Award",
                "description": "FWF START Award",
            },
        }
    ],
    "summary": "A project about parity quantum computing...",
    "granted": {
        "currency": "EUR",
        "totalCost": 0.0,
        "fundedAmount": 1168240.0,
    },
    "h2020Programmes": None,
})


class TestParseDate:
    def test_valid(self):
        assert _parse_date("2024-01-15") == "2024-01-15"

    def test_none(self):
        assert _parse_date(None) is None

    def test_empty(self):
        assert _parse_date("") is None

    def test_invalid(self):
        assert _parse_date("not-a-date") is None

    def test_truncates_time(self):
        assert _parse_date("2024-01-15T10:00:00") == "2024-01-15"


class TestCSVExtraction:
    def test_sample_project_parseable(self):
        """Verify the sample project JSON is valid."""
        proj = json.loads(SAMPLE_PROJECT_LINE)
        assert proj["title"] == "ParityQC: Parity Constraints as a Quantum Computing Toolbox"
        assert proj["fundings"][0]["shortName"] == "FWF"
        assert proj["granted"]["fundedAmount"] == 1168240.0
