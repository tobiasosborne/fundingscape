"""Pydantic models for the Quantum Applications database."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


AdvantageType = Literal[
    "exponential",
    "superpolynomial",
    "polynomial",
    "quadratic",
    "subquadratic",
    "constant",
    "none",
    "unknown",
]

AdvantageStatus = Literal[
    "proven",
    "proven_with_caveats",
    "conjectured",
    "heuristic_only",
    "disproven",
    "debated",
    "unknown",
]

Maturity = Literal[
    "theoretical",
    "numerical_evidence",
    "small_device_demo",
    "industry_pilot",
    "production",
]


class Application(BaseModel):
    """A specific computational problem where quantum advantage has been
    proposed or studied."""

    domain: str
    subdomain: str
    name: str
    description: str | None = None
    quantum_approaches: list[str] = Field(default_factory=list)
    advantage_type: AdvantageType = "unknown"
    advantage_status: AdvantageStatus = "unknown"
    classical_baseline: str | None = None
    quantum_complexity: str | None = None
    maturity: Maturity = "theoretical"
    year_first_proposed: int | None = None
    seminal_reference: str | None = None
    notes: str | None = None


class Reference(BaseModel):
    """A key paper associated with an application."""

    application_id: int
    title: str
    authors: str | None = None
    year: int | None = None
    doi: str | None = None
    arxiv_id: str | None = None
    contribution_type: Literal[
        "first_proposal",
        "speedup_proof",
        "experimental_demo",
        "refutation",
        "survey",
        "improvement",
    ] = "first_proposal"


class IndustrySector(BaseModel):
    """Link between an application and an industry sector."""

    application_id: int
    sector: str
    relevance_notes: str | None = None


class FundingLink(BaseModel):
    """Cached result of matching an application against fundingscape grants."""

    application_id: int
    query_pattern: str
    grant_count: int = 0
    total_funding_eur: float = 0.0
    top_funders: str | None = None
    last_computed: str | None = None
