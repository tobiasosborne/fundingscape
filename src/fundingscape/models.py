"""Pydantic models for the funding landscape data."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


FunderType = Literal["eu", "federal_de", "state_de", "foundation", "foreign_gov"]
Recurrence = Literal["annual", "continuous", "one-off", "biennial"]
DeadlineType = Literal["fixed", "rolling", "continuous"]
CallStatus = Literal["open", "forthcoming", "closed", "under_evaluation"]
GrantStatus = Literal["active", "completed", "terminated"]


class Funder(BaseModel):
    name: str
    short_name: str | None = None
    country: str | None = None
    type: FunderType
    website: str | None = None
    contact: str | None = None


class FundingInstrument(BaseModel):
    funder_id: int | None = None
    name: str
    short_name: str | None = None
    description: str | None = None
    url: str | None = None
    eligibility_criteria: str | None = None
    typical_duration_months: int | None = None
    typical_amount_min: Decimal | None = None
    typical_amount_max: Decimal | None = None
    currency: str = "EUR"
    success_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    recurrence: Recurrence | None = None
    next_deadline: date | None = None
    deadline_type: DeadlineType | None = None
    relevance_tags: list[str] = Field(default_factory=list)
    sme_eligible: bool = False
    source: str
    source_id: str | None = None


class Call(BaseModel):
    instrument_id: int | None = None
    call_identifier: str | None = None
    title: str
    description: str | None = None
    url: str | None = None
    opening_date: date | None = None
    deadline: date | None = None
    deadline_timezone: str = "Europe/Brussels"
    status: CallStatus
    budget_total: Decimal | None = None
    currency: str = "EUR"
    expected_grants: int | None = None
    topic_keywords: list[str] = Field(default_factory=list)
    framework_programme: str | None = None
    programme_division: str | None = None
    source: str
    source_id: str | None = None
    raw_data: dict[str, Any] | None = None


class GrantAward(BaseModel):
    instrument_id: int | None = None
    call_id: int | None = None
    project_title: str
    project_id: str | None = None
    acronym: str | None = None
    abstract: str | None = None
    pi_name: str | None = None
    pi_institution: str | None = None
    pi_country: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    total_funding: Decimal | None = None
    eu_contribution: Decimal | None = None
    currency: str = "EUR"
    status: GrantStatus | None = None
    partners: list[dict[str, Any]] = Field(default_factory=list)
    topic_keywords: list[str] = Field(default_factory=list)
    source: str
    source_id: str

    @field_validator("total_funding", "eu_contribution", mode="before")
    @classmethod
    def coerce_decimal(cls, v: Any) -> Decimal | None:
        if v is None or v == "":
            return None
        return Decimal(str(v))


class EligibilityProfile(BaseModel):
    profile_name: str
    pi_career_stage: Literal["early", "mid", "senior"] | None = None
    years_since_phd: int | None = None
    nationality: str | None = None
    institution: str = "Leibniz Universität Hannover"
    institution_country: str = "DE"
    orcid: str | None = None
    research_keywords: list[str] = Field(default_factory=list)
    is_sme: bool = False
    company_name: str | None = None
    company_country: str | None = None


class DataSourceStatus(BaseModel):
    source_id: str
    name: str
    last_fetch: datetime | None = None
    last_success: datetime | None = None
    records_fetched: int = 0
    etag: str | None = None
    last_modified: str | None = None
    status: Literal["ok", "error", "stale", "never_fetched"] = "never_fetched"
    error_message: str | None = None
