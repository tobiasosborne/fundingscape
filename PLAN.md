# EU Research Funding Landscape Intelligence System — PLAN

## 0.1 Data Source Inventory

### Tier 1: Structured APIs / Bulk Downloads (Best data quality)

#### CORDIS (Community Research and Development Information Service)
- **URL**: https://cordis.europa.eu/datalab
- **Access mechanisms**:
  - **DET REST API**: https://cordis.europa.eu/dataextractions/api-docs-ui (OpenAPI spec)
    - Auth: API key required (free, register at CORDIS)
    - Rate limits: Enforced but undocumented; be conservative (1 req/sec)
    - Format: JSON, XML, CSV
  - **Bulk CSV/XML downloads** (NO auth, NO rate limit):
    - Horizon Europe projects: `https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip` (~30 MB)
    - H2020 projects: `https://cordis.europa.eu/data/cordis-h2020projects-csv.zip` (~55 MB)
    - FP7 projects also available
    - Reference data (programmes, topics, countries): `https://cordis.europa.eu/data/reference/cordisref-*.zip`
  - **SPARQL endpoint**: https://cordis.europa.eu/datalab/sparql-endpoint (verified accessible, HTTP 200)
- **Data**: All EU framework programme projects (FP6, FP7, H2020, Horizon Europe). Includes project title, abstract, PI, organisations, funding amounts, start/end dates, topics, partners.
- **Update frequency**: Bulk downloads updated periodically (~monthly). API is real-time.
- **Gotchas**: DET API key registration may take time. Bulk downloads are the practical choice for historical data.
- **Strategy**: Use bulk CSV for initial load + historical data. Use DET API for incremental updates.

#### EU Funding & Tenders Portal
- **URL**: https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/support/apis
- **Access mechanism**: JSON data dump
  - `https://ec.europa.eu/info/funding-tenders/opportunities/data/referenceData/grantsTenders.json`
  - No auth required. No documented rate limits.
  - Format: JSON (single large file containing all calls/topics)
- **Data**: All open, forthcoming, and closed calls across Horizon Europe, MSCA, ERC, EIC, and other EU programmes. Includes call identifiers, deadlines, statuses, framework programme, topic keywords, tags.
- **Update frequency**: Real-time (reflects portal state)
- **Gotchas**: File is large (tens of MB). Timestamps are Unix epoch milliseconds. Some fields nested deeply. Includes ALL EU programmes (agriculture, etc.) — needs filtering.
- **Strategy**: Download full JSON, filter to relevant framework programmes (Horizon Europe, ERC, MSCA, EIC, COST), parse into Call entities.

#### Simpler Grants API (US Federal — AFOSR, ONR, ARL)
- **URL**: https://api.simpler.grants.gov (OpenAPI spec at `/openapi.json`)
- **Auth**: API key (free)
- **Format**: JSON REST API
- **Data**: All US federal grant opportunities including DoD BAAs
- **Key opportunities**:
  - AFOSR FY2026 Research Interests: NOFOAFRLAFOSR20250006
  - ONR Long Range BAA: N00014-25-S-B001 (rolling until Sep 2026)
  - ARL Foundational Research BAA: W911NF-23-S-0001 (rolling until Nov 2027)
- **Strategy**: Query API for DoD/AFOSR/ONR/ARL opportunities with quantum keywords.

### Tier 2: Semi-structured (Scraping with predictable structure)

#### DFG GEPRIS
- **URL**: https://gepris.dfg.de
- **Access mechanism**: Web scraping only. No official API.
  - Search URL: `https://gepris.dfg.de/gepris/OCTOPUS?task=doSearchSimple&context=projekt&keywords_criterion=quantum&results_per_page=1000`
  - Project detail: `https://gepris.dfg.de/gepris/projekt/{id}?language=en`
  - HTML pages with structured content (title, PI, institution, funding scheme, abstract, duration, amount)
- **Community tool**: [dfg-gepris-crawler](https://github.com/primeapple/dfg-gepris-crawler) — Scrapy-based, we can learn from its approach
- **Data**: ~150,000+ DFG-funded projects. Includes PI, institution, funding scheme (Sachbeihilfe, SFB, Emmy Noether, etc.), abstract, funding period.
- **Update frequency**: Continuous (new projects added as funded)
- **Gotchas**: No official API. Rate-limit scraping heavily (2-3 sec delays). German-language interface; `?language=en` param exists. No bulk download.
- **Strategy**: Build a Scrapy-style crawler. Start with quantum/physics keyword searches. Cache all HTML responses.

#### BMBF Förderkatalog
- **URL**: https://foerderportal.bund.de/foekat/jsp/StartAction.do
- **Access mechanism**: Web scraping only. JSP-based search interface.
  - Search: `https://foerderportal.bund.de/foekat/jsp/SucheAction.do?actionMode=searchmask`
  - 110,000+ completed and ongoing federal projects
- **Data**: Project title, Förderkennzeichen (grant ID), PI, institution, funding ministry, amount, duration, abstract.
- **Update frequency**: Continuous
- **Gotchas**: **Currently down for maintenance until 23 Feb 2026**. Notoriously poorly structured JSP site. No API. Search interface is slow and paginated. May need form-based POST requests. HTML structure may change without notice.
- **Also**: Förderdatenbank at https://www.foerderdatenbank.de/ provides programme-level info (not project-level).
- **Strategy**: Build robust scraper after maintenance window. Add staleness detection tests. Store raw HTML. Consider förderdatenbank.de as supplementary programme-level source.

#### VolkswagenStiftung
- **URL**: https://www.volkswagenstiftung.de
- **Access mechanism**: Web scraping. Funding initiatives listed on website.
- **Data**: Current and past funding initiatives, deadlines, eligibility criteria.
- **Gotchas**: German-language. No API. Relatively few entries (manageable manually if needed).
- **Strategy**: Scrape funding initiatives pages. Supplement with manual entry for Niedersächsisches Vorab details.

#### COST Actions
- **URL**: https://www.cost.eu/cost-actions-event/browse-actions/
- **Access mechanism**: Web browsing/scraping. No API found.
- **Data**: Action titles, participating countries, chairs, duration, topic area.
- **Strategy**: Scrape the browse actions page. COST Actions also appear in CORDIS data.

### Tier 3: Static / Manual Entry Sources

#### ERC, MSCA, EIC (specific programme data)
- ERC Dashboard: Qlik Sense — https://dashboard.tech.ec.europa.eu/... (Excel export only)
- MSCA Dashboard: Qlik Sense — https://webgate.ec.europa.eu/eacdashboard/... (web-only)
- **Strategy**: These are subsets of CORDIS + F&T Portal data. Use Tier 1 sources as primary. Dashboard data for success rate statistics only (manual Excel download if needed).

#### QuantERA
- **URL**: https://quantera.eu
- **Access mechanism**: Website scraping for calls. Project data also in CORDIS.
- **Data**: ERA-NET calls for quantum technologies, funded projects.
- **Strategy**: Scrape call pages. Cross-reference with CORDIS for funded projects.

#### Quantum Flagship
- **URL**: https://qt.eu
- **Strategy**: Projects are in CORDIS under Horizon Europe. Use CORDIS data. Scrape qt.eu for programme-level context.

#### Alexander von Humboldt Stiftung
- **URL**: https://www.humboldt-foundation.de
- **Data**: Fellowships (postdoc, experienced), Humboldt professorships, prizes.
- **Strategy**: Manual/semi-manual entry. Programmes are stable and well-documented. Few enough to curate.

#### Fritz Thyssen Stiftung
- **URL**: https://www.fritz-thyssen-stiftung.de
- **Strategy**: Manual entry. Limited number of programmes.

#### Carl-Zeiss-Stiftung
- **URL**: https://www.carl-zeiss-stiftung.de
- **Strategy**: Manual entry. Check eligibility (they tend to fund specific partner universities).

#### DAAD
- **URL**: https://www.daad.de
- **Strategy**: Scrape programme database. DAAD has a searchable database of programmes but no API.

#### NATO SPS (Science for Peace and Security)
- **URL**: https://www.nato.int/cps/en/natohq/topics_85373.htm
- **Data**: Quantum technologies is a priority area (2024 quantum strategy). SPS funds Multi-Year Projects (up to €400K/3yr) and Advanced Study Institutes.
- **Strategy**: Manual entry. Few calls per year. Monitor NATO SPS hub page.

#### MWK Niedersachsen / QuantumFrontiers / Niedersächsisches Vorab
- **Strategy**: Manual entry with periodic web checks. State-level programmes change infrequently. QuantumFrontiers cluster may have internal calls communicated via email.

#### Forschungszulage (R&D Tax Credit)
- **URL**: https://www.forschungszulage.de
- **Data**: Not a grant programme — it's a 25% tax credit on R&D personnel costs (up to €1M/year base). Relevant for Innovailia UG.
- **Strategy**: Static entry in FundingInstrument table. No ongoing data feed needed.

#### AFOSR / ONR Global / ARL (detail)
- All accessible via Simpler Grants API (Tier 1) or SAM.gov
- **AFOSR**: Publishes annual BAA with research interest areas. Quantum computing/information is under Physics & Biological Sciences division.
- **ONR Global**: Has European offices (London). Accepts proposals from non-US institutions.
- **ARL**: BAA W911NF-23-S-0001 explicitly includes "Mathematics for Quantum Information Systems" topic.
- **Strategy**: Query Simpler Grants API. For BAA topic details, download PDF attachments and parse.

---

## 0.2 Toolchain Decision

### Primary Language: **Python 3.12**
- Best ecosystem for web scraping (beautifulsoup4, lxml), HTTP (httpx), data processing
- Excellent database drivers (duckdb, sqlite3)
- Strong validation (pydantic v2)
- Maximum development velocity and my highest fluency
- Available on system with `uv` package manager

### Database: **DuckDB**
- Analytical queries are the primary use case (aggregations, joins, window functions for trends, rankings)
- Single-file database — no server needed, trivially portable
- Native CSV/JSON import for bulk loading
- SQL dialect is Postgres-compatible — richer than SQLite for analytics
- Excellent Python integration via `duckdb` package
- Can query Parquet/CSV files directly without import

### HTTP Client: **httpx** (async-capable)
- Modern, async-native HTTP client
- Built-in HTTP/2 support
- Clean API for headers, auth, redirects
- Pair with `hishel` for HTTP caching (ETags, Last-Modified, Cache-Control)

### Caching: **hishel** (HTTP cache) + **filesystem** (raw responses)
- `hishel` provides RFC-compliant HTTP caching on top of httpx
- Additionally store raw responses as files: `cache/{source}/{hash}.json` with metadata
- Idempotent: re-run skips unchanged data

### Testing: **pytest**
- De facto standard. No reason to use anything else.
- `pytest-httpx` for mocking HTTP requests
- `pytest-asyncio` for async test support

### Schema Validation: **pydantic v2**
- Strict mode for data validation
- JSON serialization/deserialization built-in
- Type safety with Python type hints
- Model validators for complex business rules

### Scraping: **beautifulsoup4 + lxml**
- BS4 for HTML parsing with lxml backend (fast)
- No headless browser needed unless JavaScript rendering required (unlikely for these government sites)

### Data Serialisation (raw cache): **JSON**
- Human-readable for debugging
- Schema-validatable with pydantic
- Native to most APIs

### Package Manager: **uv**
- Already installed on system
- Fast dependency resolution
- Handles virtual environments automatically

### Build/Run: **Makefile**
- `make update` — refresh all sources
- `make test` — run all tests
- `make report` — generate funding landscape report
- `make init` — initialize database and project

---

## 0.3 Data Model Design

```sql
-- Funders: the funding bodies
CREATE TABLE funder (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    short_name TEXT,
    country TEXT,  -- ISO 3166-1 alpha-2
    type TEXT NOT NULL,  -- 'eu', 'federal_de', 'state_de', 'foundation', 'foreign_gov'
    website TEXT,
    contact TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Funding instruments: programme/scheme (e.g. "ERC Starting Grant")
CREATE TABLE funding_instrument (
    id INTEGER PRIMARY KEY,
    funder_id INTEGER REFERENCES funder(id),
    name TEXT NOT NULL,
    short_name TEXT,
    description TEXT,
    url TEXT,
    eligibility_criteria TEXT,  -- structured JSON or free text
    typical_duration_months INTEGER,
    typical_amount_min DECIMAL,
    typical_amount_max DECIMAL,
    currency TEXT DEFAULT 'EUR',
    success_rate DECIMAL,  -- historical, 0.0-1.0
    recurrence TEXT,  -- 'annual', 'continuous', 'one-off', 'biennial'
    next_deadline DATE,
    deadline_type TEXT,  -- 'fixed', 'rolling', 'continuous'
    relevance_tags TEXT[],  -- ['quantum', 'formal_methods', 'many_body', ...]
    sme_eligible BOOLEAN DEFAULT FALSE,
    source TEXT NOT NULL,  -- data source identifier
    source_id TEXT,  -- ID in source system
    raw_data JSON,  -- original source record
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Calls: specific open or forthcoming call for proposals
CREATE TABLE call (
    id INTEGER PRIMARY KEY,
    instrument_id INTEGER REFERENCES funding_instrument(id),
    call_identifier TEXT,  -- e.g. "HORIZON-CL4-2025-DIGITAL-01-22"
    title TEXT NOT NULL,
    description TEXT,
    url TEXT,
    opening_date DATE,
    deadline DATE,
    deadline_timezone TEXT DEFAULT 'Europe/Brussels',
    status TEXT NOT NULL,  -- 'open', 'forthcoming', 'closed', 'under_evaluation'
    budget_total DECIMAL,
    currency TEXT DEFAULT 'EUR',
    expected_grants INTEGER,
    topic_keywords TEXT[],
    framework_programme TEXT,  -- 'HORIZON', 'H2020', etc.
    programme_division TEXT,
    source TEXT NOT NULL,
    source_id TEXT,
    raw_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grants: awarded grants (for benchmarking, network mapping)
CREATE TABLE grant_award (
    id INTEGER PRIMARY KEY,
    instrument_id INTEGER REFERENCES funding_instrument(id),
    call_id INTEGER REFERENCES call(id),
    project_title TEXT NOT NULL,
    project_id TEXT,  -- e.g. CORDIS project number, DFG Förderkennzeichen
    acronym TEXT,
    abstract TEXT,
    pi_name TEXT,
    pi_institution TEXT,
    pi_country TEXT,
    start_date DATE,
    end_date DATE,
    total_funding DECIMAL,
    eu_contribution DECIMAL,
    currency TEXT DEFAULT 'EUR',
    status TEXT,  -- 'active', 'completed', 'terminated'
    partners JSON,  -- [{name, country, funding}]
    topic_keywords TEXT[],
    source TEXT NOT NULL,
    source_id TEXT UNIQUE,
    raw_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Eligibility profile: our group's profile for matching
CREATE TABLE eligibility_profile (
    id INTEGER PRIMARY KEY,
    profile_name TEXT NOT NULL,
    pi_career_stage TEXT,  -- 'early', 'mid', 'senior'
    years_since_phd INTEGER,
    nationality TEXT,
    institution TEXT,
    institution_country TEXT,
    orcid TEXT,
    research_keywords TEXT[],
    is_sme BOOLEAN DEFAULT FALSE,
    company_name TEXT,
    company_country TEXT,
    notes TEXT
);

-- Data source tracking
CREATE TABLE data_source (
    id TEXT PRIMARY KEY,  -- e.g. 'cordis_bulk', 'ft_portal', 'gepris'
    name TEXT NOT NULL,
    last_fetch TIMESTAMP,
    last_success TIMESTAMP,
    records_fetched INTEGER,
    etag TEXT,
    last_modified TEXT,
    status TEXT,  -- 'ok', 'error', 'stale'
    error_message TEXT
);

-- Change log for diffs
CREATE TABLE change_log (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,  -- 'call', 'grant', 'instrument'
    entity_id INTEGER NOT NULL,
    change_type TEXT NOT NULL,  -- 'new', 'updated', 'closed'
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Pydantic Models (Python side)

```python
class Funder(BaseModel):
    name: str
    short_name: str | None
    country: str | None
    type: Literal['eu', 'federal_de', 'state_de', 'foundation', 'foreign_gov']
    website: str | None

class FundingInstrument(BaseModel):
    funder_id: int
    name: str
    description: str | None
    url: str | None
    eligibility_criteria: str | None
    typical_duration_months: int | None
    typical_amount_min: Decimal | None
    typical_amount_max: Decimal | None
    currency: str = 'EUR'
    success_rate: float | None  # 0.0-1.0
    recurrence: Literal['annual', 'continuous', 'one-off', 'biennial'] | None
    next_deadline: date | None
    deadline_type: Literal['fixed', 'rolling', 'continuous'] | None
    relevance_tags: list[str] = []
    sme_eligible: bool = False
    source: str
    source_id: str | None

class Call(BaseModel):
    instrument_id: int | None
    call_identifier: str | None
    title: str
    description: str | None
    url: str | None
    opening_date: date | None
    deadline: date | None
    status: Literal['open', 'forthcoming', 'closed', 'under_evaluation']
    budget_total: Decimal | None
    currency: str = 'EUR'
    expected_grants: int | None
    topic_keywords: list[str] = []
    framework_programme: str | None
    source: str
    source_id: str | None

class GrantAward(BaseModel):
    instrument_id: int | None
    call_id: int | None
    project_title: str
    project_id: str | None
    acronym: str | None
    abstract: str | None
    pi_name: str | None
    pi_institution: str | None
    pi_country: str | None
    start_date: date | None
    end_date: date | None
    total_funding: Decimal | None
    eu_contribution: Decimal | None
    currency: str = 'EUR'
    status: str | None
    partners: list[dict] | None
    topic_keywords: list[str] = []
    source: str
    source_id: str
```

---

## 0.4 Granular Implementation Plan

### Step 1: Project Scaffolding (15 min)
- **Input**: Nothing
- **Output**: Project directory structure, `pyproject.toml`, Makefile, virtual environment
- **Test**: `uv run pytest` passes (with one dummy test)
- **Details**:
  ```
  fundingscape/
  ├── pyproject.toml
  ├── Makefile
  ├── src/
  │   └── fundingscape/
  │       ├── __init__.py
  │       ├── models.py          # Pydantic models
  │       ├── db.py              # DuckDB schema + operations
  │       ├── cache.py           # HTTP caching layer
  │       ├── sources/           # One module per data source
  │       │   ├── __init__.py
  │       │   ├── cordis.py
  │       │   ├── ft_portal.py
  │       │   ├── gepris.py
  │       │   ├── foerderkatalog.py
  │       │   ├── grants_gov.py
  │       │   └── manual.py      # Manual entry sources
  │       ├── enrichment.py      # Dedup, scoring, network
  │       ├── queries.py         # Analytical queries
  │       └── report.py          # Report generation
  ├── tests/
  │   ├── conftest.py
  │   ├── test_models.py
  │   ├── test_db.py
  │   ├── test_cache.py
  │   └── test_sources/
  ├── data/
  │   ├── cache/                 # Raw response cache
  │   └── db/                    # DuckDB database file
  └── manual/                    # Manual entry YAML files
  ```

### Step 2: Data Models + Validation (20 min)
- **Input**: Schema design from 0.3
- **Output**: `models.py` with pydantic models, `test_models.py` with validation tests
- **Test**: Models accept valid data, reject invalid data, serialize to/from JSON
- **Depends on**: Step 1

### Step 3: Database Layer (20 min)
- **Input**: Schema design, models
- **Output**: `db.py` with DuckDB schema creation, CRUD operations, `test_db.py`
- **Test**: Create tables, insert records, query records, verify schema constraints
- **Depends on**: Step 2

### Step 4: HTTP Cache Layer (20 min)
- **Input**: Nothing
- **Output**: `cache.py` with cached HTTP client (ETag, Last-Modified, filesystem cache), `test_cache.py`
- **Test**: First fetch downloads, second fetch uses cache, ETag support, cache invalidation
- **Depends on**: Step 1

### Step 5: CORDIS Bulk Ingestion (30 min)
- **Input**: CORDIS CSV download URLs
- **Output**: `sources/cordis.py` with bulk CSV download + parse, `test_sources/test_cordis.py`
- **Test**: Download succeeds, CSV parses correctly, records match expected schema, quantum projects found
- **Depends on**: Steps 3, 4

### Step 6: EU F&T Portal Ingestion (25 min)
- **Input**: grantsTenders.json URL
- **Output**: `sources/ft_portal.py` with JSON download + parse, `test_sources/test_ft_portal.py`
- **Test**: JSON downloads, calls parse correctly, can filter by programme, deadlines are valid dates
- **Depends on**: Steps 3, 4

### Step 7: DFG GEPRIS Scraper (30 min)
- **Input**: GEPRIS search + detail page URLs
- **Output**: `sources/gepris.py` with search + detail scraper, `test_sources/test_gepris.py`
- **Test**: Search returns results, detail pages parse correctly, rate limiting works
- **Depends on**: Steps 3, 4

### Step 8: Simpler Grants API (US DoD) (20 min)
- **Input**: Grants.gov API
- **Output**: `sources/grants_gov.py` with API client, `test_sources/test_grants_gov.py`
- **Test**: API queries return results, AFOSR/ONR/ARL BAAs found, data parses correctly
- **Depends on**: Steps 3, 4

### Step 9: Manual Entry System (15 min)
- **Input**: YAML format for manual sources
- **Output**: `sources/manual.py` + YAML files for foundations/NATO/state-level, `test_sources/test_manual.py`
- **Test**: YAML files validate, load into database correctly
- **Depends on**: Steps 3
- **Sources covered**: Humboldt, Thyssen, Carl-Zeiss, NATO SPS, MWK Niedersachsen, QuantumFrontiers, Forschungszulage

### Step 10: BMBF Förderkatalog Scraper (30 min)
- **Input**: foerderportal.bund.de (after maintenance ends Feb 23)
- **Output**: `sources/foerderkatalog.py` with scraper, staleness tests
- **Test**: Search works, results parse, staleness detection (HTML structure checks)
- **Depends on**: Steps 3, 4
- **Note**: May need to defer if maintenance extends. Lower priority — CORDIS covers many BMBF co-funded projects.

### Step 11: Supplementary Scrapers (25 min)
- **Output**: VolkswagenStiftung, COST Actions, QuantERA, DAAD scrapers
- **Test**: Each returns valid data, staleness detection
- **Depends on**: Steps 3, 4

### Step 12: Deduplication (20 min)
- **Input**: Loaded database with multiple sources
- **Output**: `enrichment.py` dedup logic — match grants across CORDIS, GEPRIS, Förderkatalog
- **Test**: Known duplicates detected, no false positives on test data
- **Depends on**: Steps 5-11

### Step 13: Relevance Scoring (20 min)
- **Input**: Eligibility profile, keyword lists
- **Output**: `enrichment.py` scoring function — keyword match score for each instrument/call
- **Test**: Known-relevant calls score high, irrelevant calls score low
- **Depends on**: Step 12

### Step 14: Success Rate Calculation (15 min)
- **Input**: Call data + award data
- **Output**: `enrichment.py` success rate computation
- **Test**: Known instruments (ERC StG) match published success rates (~10-15%)
- **Depends on**: Step 12

### Step 15: Network Mapping (15 min)
- **Input**: Grant award PI/partner data
- **Output**: `enrichment.py` network analysis — top PIs, institutional collaborations
- **Test**: Can find top-funded PIs in quantum computing, LUH-affiliated grants
- **Depends on**: Step 12

### Step 16: Analytical Queries (30 min)
- **Input**: Enriched database
- **Output**: `queries.py` with SQL queries for all Phase 4 views
- **Test**: Each query returns sensible results
- **Depends on**: Steps 12-15
- **Queries**:
  1. Open calls ranked by relevance × deadline
  2. Funding landscape by source/year
  3. Income projection from active grants
  4. Gap analysis (unused instruments)
  5. Success rate benchmarking
  6. Partner network / top PIs
  7. Historical trends
  8. Innovailia UG eligible instruments

### Step 17: Report Generation (20 min)
- **Input**: Query results
- **Output**: `report.py` generating Markdown report
- **Test**: Report renders correctly, contains all sections
- **Depends on**: Step 16
- **Sections**:
  - Executive summary
  - Deadline calendar (next 6 months)
  - Top 20 recommended calls
  - Active grants + income projection
  - Data quality report

### Step 18: Makefile + CLI (15 min)
- **Input**: All modules
- **Output**: `Makefile` with targets, CLI entry point
- **Test**: `make init`, `make update`, `make report`, `make test` all work
- **Depends on**: Step 17

### Step 19: Integration Testing + Polish (20 min)
- **Input**: Complete system
- **Output**: End-to-end test, README, data quality checks
- **Test**: Full pipeline runs, answers all 5 success criteria questions
- **Depends on**: Step 18

---

## Execution Order

```
Step 1 (scaffolding)
  ├── Step 2 (models)
  │     └── Step 3 (database)
  └── Step 4 (cache)
        ├── Step 5 (CORDIS) ─────────────────┐
        ├── Step 6 (F&T Portal) ─────────────┤
        ├── Step 7 (GEPRIS) ─────────────────┤
        ├── Step 8 (Grants.gov) ─────────────┤
        ├── Step 9 (Manual) ─────────────────┤
        ├── Step 10 (Förderkatalog) ─────────┤
        └── Step 11 (Supplementary) ─────────┘
                                              │
                                    Step 12 (Dedup)
                                     ├── Step 13 (Scoring)
                                     ├── Step 14 (Success rates)
                                     └── Step 15 (Network)
                                              │
                                    Step 16 (Queries)
                                              │
                                    Step 17 (Report)
                                              │
                                    Step 18 (CLI/Makefile)
                                              │
                                    Step 19 (Integration)
```

---

## Dependencies (pyproject.toml)

```toml
[project]
name = "fundingscape"
version = "0.1.0"
requires-python = ">=3.12"

dependencies = [
    "duckdb>=1.0",
    "httpx>=0.27",
    "hishel>=0.0.31",
    "pydantic>=2.7",
    "beautifulsoup4>=4.12",
    "lxml>=5.0",
    "pyyaml>=6.0",
    "rich>=13.0",        # CLI output formatting
    "click>=8.1",        # CLI framework
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-httpx>=0.30",
    "pytest-asyncio>=0.23",
]
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| CORDIS API key takes days to get | Medium | Use bulk downloads (no auth needed) as primary |
| BMBF Förderkatalog maintenance extends | Low | CORDIS covers many BMBF projects. Defer scraper. |
| GEPRIS structure changes | Medium | Staleness detection tests. Use dfg-gepris-crawler patterns. |
| F&T Portal JSON format changes | Medium | Schema validation will catch immediately |
| Rate limiting from GEPRIS/Förderkatalog | Low | Conservative delays (2-3 sec). Cache everything. |
| Grants.gov API key takes time | Low | SAM.gov as backup. Manual entry as fallback. |
| DuckDB migration needed later | Low | SQL is standard. Migration to Postgres straightforward. |
