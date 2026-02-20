# Claude Code Prompt: EU Research Funding Landscape Intelligence System

## Context

You are building a comprehensive research funding intelligence system for a quantum information theory research group at a German university (Leibniz Universität Hannover). The group works on quantum computing, many-body quantum mechanics, topological quantum computation (mobile anyons, fusion categories), quantum Boolean functions, and formal verification (Lean4). The PI also co-founded a two-person deep-tech company (Innovailia UG).

This tool replaces a service that consulting firms charge €5,000–80,000 for and deliver in weeks. We are building it in an afternoon.

## Commandments

1. **You choose the toolchain.** Research what is best for this task — language, libraries, database, testing framework. Justify your choices briefly, then commit. Do not ask me. Optimise for: speed of development, robustness of data ingestion, query flexibility, and your own fluency.
2. **Free and open source software only.** Every dependency must be FOSS. No exceptions unless literally no alternative exists, in which case document why.
3. **TDD throughout.** Write tests before implementation for every module. Tests are not optional or deferred. Each data source gets validation tests for schema conformance, completeness, and staleness.
4. **Idempotent and cached.** Every data fetch must be cached locally. Re-running the pipeline must not re-download unchanged data. Use ETags/Last-Modified headers where available. Store raw responses before transformation.
5. **Fail loud, log everything.** If a scraper breaks or an API changes schema, the system must fail with a clear error, not silently produce garbage.

## Phase 0: Research & Plan (DO THIS FIRST)

Before writing any code, research and produce a `PLAN.md` containing:

### 0.1 Data Source Inventory

For EACH of the following sources, research the actual API/download mechanism available today. Record: URL, auth requirements, rate limits, data format, update frequency, and any gotchas.

**EU-level sources:**
- CORDIS (Community Research and Development Information Service) — all Horizon Europe / Horizon 2020 projects and calls
- EU Funding & Tenders Portal — open and forthcoming calls, topics, deadlines
- ERC (European Research Council) — Starting, Consolidator, Advanced, Synergy, Proof of Concept grants
- MSCA (Marie Skłodowska-Curie Actions) — Doctoral Networks, Postdoctoral Fellowships, COFUND, Staff Exchanges
- EIC (European Innovation Council) — Pathfinder, Transition, Accelerator
- QuantERA — ERA-NET for quantum technologies
- Quantum Flagship calls (if distinct from above)
- COST Actions — networking grants
- ERASMUS+ (if relevant for research mobility)

**German federal sources:**
- BMBF Förderkatalog (foerderportal.bund.de) — all BMBF-funded projects and open calls
- DFG GEPRIS — all DFG-funded projects, and DFG funding schemes (Sachbeihilfe, SFB, Emmy Noether, Heisenberg, EXC, etc.)
- BMBF Quantum Computing research programme specifically
- VDI/VDE-IT as Projektträger for quantum technology calls
- Forschungszulage (tax credit for R&D — relevant for the company Innovailia UG)

**State-level (Niedersachsen):**
- MWK Niedersachsen funding programmes
- QuantumFrontiers cluster (if they have internal calls)
- Niedersächsisches Vorab (VolkswagenStiftung allocation for Niedersachsen)

**Foundations & other:**
- VolkswagenStiftung — all current funding initiatives
- Alexander von Humboldt Stiftung — professorships, fellowships, prizes
- Thyssen Stiftung
- Carl-Zeiss-Stiftung
- Werner Heisenberg Programme (DFG)
- NATO Science for Peace and Security (SPS) — quantum relevant calls
- DAAD — research-related programmes

**International (non-EU):**
- AFOSR (Air Force Office of Scientific Research) — they fund EU-based quantum researchers
- ONR Global (Office of Naval Research) — same
- ARL (Army Research Lab) — International Technology Alliance programmes

### 0.2 Toolchain Decision

Research and decide:
- Primary language and why
- Database (consider: DuckDB, SQLite, PostgreSQL — think about query complexity needed)
- HTTP client with robust retry/cache
- Testing framework
- Schema validation approach
- Scraping library for the inevitably terrible websites (BMBF Förderkatalog, I'm looking at you)
- Data serialisation format for raw cache

### 0.3 Data Model Design

Design the schema. At minimum, the following entities and their relationships:

**FundingInstrument** — a programme/scheme (e.g., "ERC Starting Grant", "DFG Sachbeihilfe")
- name, funder, description, url
- eligibility_criteria (structured if possible, free text otherwise)
- typical_duration, typical_amount_min, typical_amount_max, currency
- success_rate (historical, if available)
- recurrence (annual, continuous, one-off)
- next_deadline, deadline_type (fixed, rolling, continuous)
- relevance_tags (quantum, formal_methods, many_body, etc.)

**Call** — a specific open or forthcoming call for proposals
- parent instrument
- call_id, title, description, url
- opening_date, deadline, status (open, forthcoming, closed)
- budget_total, expected_number_of_grants
- topic_keywords

**Grant** — an awarded grant (for benchmarking, network mapping, success rate calculation)
- instrument, call (if linkable)
- project_title, project_id, abstract
- PI_name, PI_institution, PI_country
- start_date, end_date, total_funding
- partners (for collaborative grants)
- status (active, completed)

**Funder** — the funding body
- name, country, type (EU, federal, state, foundation, foreign_gov)
- website, contact

**EligibilityProfile** — our group's profile for matching
- PI career stage, nationality, institutional affiliation
- research keywords, ORCID
- company details (for SME-specific instruments)

### 0.4 Granular Implementation Plan

Break the build into steps no larger than 30 minutes each. Each step must have:
- Clear input and output
- Test criteria (what does "done" look like)
- Dependencies on previous steps

## Phase 1: Infrastructure

- Set up project structure, dependencies, database schema
- Implement the caching/fetch layer with tests
- Implement the data model with tests
- Build a test harness that validates the database against the schema

## Phase 2: Data Acquisition (one source at a time)

For EACH data source, in order of data quality (best APIs first):

1. **Research** the actual endpoint/download mechanism (curl it, read the docs)
2. **Write tests** for expected data shape
3. **Implement** the fetcher/parser
4. **Validate** — check record counts, spot-check against the website, verify no silent failures
5. **Load** into the database
6. **Verify** with a query: "show me all quantum-related entries from this source"

Suggested order (adjust based on Phase 0 findings):
1. CORDIS (REST API, well-documented, high-quality data)
2. EU Funding & Tenders Portal
3. DFG GEPRIS
4. BMBF Förderkatalog
5. VolkswagenStiftung
6. ERC/MSCA/EIC (may be subsets of #1 and #2)
7. QuantERA
8. Everything else

For sources without APIs: build robust scrapers with explicit selectors, and add staleness tests that detect when the page structure has changed.

## Phase 3: Enrichment & Scoring

Once raw data is loaded:

1. **Deduplication** — same grants appear in multiple databases (CORDIS + national DBs)
2. **Relevance scoring** — score each instrument and call against our research profile. Use keyword matching as a baseline; optionally use an LLM for semantic matching of call descriptions against our research topics
3. **Success rate calculation** — for instruments where we have both call data and award data, compute historical success rates
4. **Network mapping** — who at our university or in our field has received grants from each instrument? (from GEPRIS and CORDIS PI data)
5. **Timeline construction** — build a calendar of upcoming deadlines

## Phase 4: Query Interface & Analytics

Build queries / views for:

1. **Dashboard view**: all open calls relevant to our group, sorted by deadline
2. **Funding landscape**: total available funding by source, by year, with trends
3. **Income projection**: based on current active grants, model future income (grant amounts, start/end dates, burn rate)
4. **Gap analysis**: which funders have we never applied to? Which instruments are we eligible for but haven't used?
5. **Success rate benchmarking**: for each instrument, what's the overall success rate vs. our field vs. our institution?
6. **Partner network**: who are the most common co-PIs in quantum computing grants? Who should we collaborate with?
7. **Historical trends**: funding for quantum computing over time, by funder, by sub-topic
8. **Company-specific**: which instruments are available for Innovailia UG (SME-specific, Forschungszulage, EIC Accelerator)?

These can be SQL queries, a CLI tool, a simple web dashboard, or Jupyter-style notebooks — you decide what's most practical.

## Phase 5: Export & Reporting

Generate a structured report (markdown or HTML) containing:
- Executive summary of the funding landscape
- Calendar of upcoming deadlines
- Top 20 recommended calls ranked by fit × expected value
- Active grants summary with income projections
- Data quality report (coverage, freshness, known gaps)

## Constraints & Notes

- The BMBF Förderkatalog at foerderportal.bund.de is notoriously poorly structured. Expect pain. Document workarounds.
- Some sources (NATO SPS, AFOSR) may not have programmatic access. A manual entry mechanism is acceptable for these — design the schema so it supports both automated and manual data entry.
- DFG GEPRIS has a web interface but check if there's a bulk download or API.
- CORDIS has both a REST API and bulk CSV/XML downloads. The bulk download may be more practical for historical data.
- The EU Funding & Tenders Portal has an API — research its current state.
- Rate-limit all scrapers. Be a good citizen. Add delays between requests.
- Store everything in UTC. Convert deadlines to Europe/Berlin for display.
- The system should be re-runnable. `make update` should refresh all sources and report what changed.

## Success Criteria

The system is done when I can:

1. Ask "What quantum computing grants can I apply for in the next 6 months?" and get a ranked list with deadlines, amounts, success rates, and relevance scores.
2. Ask "How much grant income will my group have in 2026 and 2027?" and get a projection based on active grants.
3. Ask "Who are the top 10 PIs in topological quantum computing in Europe by grant funding?" and get an answer.
4. Ask "What's the historical success rate for ERC Consolidator Grants in PE2 (physics)?" and get a number.
5. Run `make update` weekly and get a diff of new calls, closed calls, and changed deadlines.

## Begin

Start with Phase 0. Research everything. Produce PLAN.md. Then proceed to implementation. Do not ask me for decisions — make them, document them, and move.
