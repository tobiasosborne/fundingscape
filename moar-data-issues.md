# Tier 1: Highest Impact Data Sources

## [epic] MOAR DATA: 100% quantum funding coverage
- type: epic
- priority: 0
- labels: moar-data
- description: |
    Expand fundingscape to cover ALL possible funding sources for quantum technology
    and deep-tech research. Target: go from 4 sources to 30+ sources. Currently we
    have ~55K grants and ~7K calls. Goal: 200K+ records from 30+ funders worldwide.

## [feature] Integrate OpenAIRE API
- type: feature
- priority: 1
- labels: data-source, tier-1, api
- estimate: 120
- description: |
    OpenAIRE aggregates grants from 100K+ funders worldwide, cross-linked to publications.
    REST API at https://api.openaire.eu/ — FREE, no auth required for basic access.
    Single highest-impact addition for coverage. Covers most European national funders.

## [feature] Integrate OpenAlex API
- type: feature
- priority: 1
- labels: data-source, tier-1, api
- estimate: 90
- description: |
    OpenAlex (https://openalex.org/) is a free, open REST API that replaced Microsoft Academic Graph.
    Has funder and grant metadata linked to works/publications. Excellent for linking grants to outputs.
    Covers funders globally. No auth needed, 100K requests/day.

## [feature] Integrate Dimensions API (grants module)
- type: feature
- priority: 1
- labels: data-source, tier-1, api
- estimate: 90
- description: |
    Dimensions (https://www.dimensions.ai/) has the most comprehensive commercial grants database.
    Freemium API covers 100+ funders globally. Grants module includes quantum-specific data.
    Would provide massive coverage of national funders (ANR, NWO, SNSF, FWF, ARC, etc.) in one integration.

## [feature] Integrate DFG GEPRIS scraper (live)
- type: feature
- priority: 1
- labels: data-source, tier-1, scraper
- estimate: 60
- description: |
    Scraper is built (sources/gepris.py) but not yet run against live site.
    GEPRIS covers ALL DFG-funded projects (~200K+). Critical for German coverage.
    Rate-limit to 2.5s between requests. Target: all quantum/physics projects.
    Existing community crawler: github.com/primeapple/dfg-gepris-crawler

## [feature] Integrate BMBF Förderkatalog
- type: feature
- priority: 1
- labels: data-source, tier-1, scraper
- estimate: 120
- description: |
    BMBF Förderkatalog at foerderportal.bund.de has 110K+ federal research projects.
    Site was under maintenance until Feb 23. Scraper framework exists (sources/foerderkatalog.py placeholder).
    JSP-based interface, needs form-based POST requests. No API available.
    Reportedly has bulk XML export — investigate.

## [feature] Integrate UKRI Gateway to Research API
- type: feature
- priority: 1
- labels: data-source, tier-1, api
- estimate: 90
- description: |
    UK Research & Innovation Gateway to Research (https://gtr.ukri.org/) has a REST API (JSON/XML).
    UK is the second-largest quantum funder in Europe (EPSRC Quantum Technology Hubs, NQTP).
    Excellent structured data. Covers EPSRC, BBSRC, STFC, Innovate UK.

# Tier 2: High Impact

## [feature] Integrate NSF Award Search API
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 60
- description: |
    NSF Award Search API returns JSON. Covers QLCI, quantum information science.
    https://www.nsf.gov/ — major quantum funder. Some international collaborations funded.

## [feature] Integrate SNSF P3 database API
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 60
- description: |
    Swiss National Science Foundation P3 database (https://p3.snf.ch/) has REST API (JSON).
    Excellent structured data. Switzerland is a top quantum research country (ETH, EPFL, Basel).
    NCCR SPIN and other quantum programmes.

## [feature] Integrate ANR (French) project database
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 60
- description: |
    Agence Nationale de la Recherche (https://anr.fr/) recently launched API.
    France has the largest national quantum programme in continental Europe (Plan Quantique / France 2030).

## [feature] Integrate FWF (Austrian) project database API
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 60
- description: |
    Austrian Science Fund FWF (https://www.fwf.ac.at/) has Research Radar with API.
    Austria has strong quantum research (Vienna, Innsbruck — Zeilinger group, Blatt group).
    SFB BeyondC and other quantum programmes.

## [feature] Integrate DOE/OSTI grant database
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 60
- description: |
    DOE Office of Science (https://science.osti.gov/) has OSTI API.
    National Quantum Information Science Research Centers. Major US quantum funder.

## [feature] Integrate Simpler.Grants.gov API (replace manual DoD entries)
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 90
- description: |
    Replace manual YAML entries for AFOSR/ONR/ARL with proper API integration.
    POST https://api.simpler.grants.gov/v1/opportunities/search
    API key required (free), 60 req/min, 10K req/day.
    Search for quantum/physics DoD BAAs automatically.

## [feature] Integrate ESA EMITS/OSIP tender system
- type: feature
- priority: 2
- labels: data-source, tier-2, scraper
- estimate: 90
- description: |
    European Space Agency is a major quantum funder (QKD satellites, quantum clocks, quantum sensors).
    EMITS for tenders (login may be required). OSIP for open calls.
    Not in CORDIS. https://www.esa.int/

## [feature] Integrate Crossref Funder Registry + ROR for data normalization
- type: feature
- priority: 2
- labels: infrastructure, tier-2, api
- estimate: 60
- description: |
    Crossref Funder Registry (REST API, free) maps funder IDs and links grants to DOIs.
    ROR (Research Organization Registry, REST API, free) normalizes institution names.
    Essential for deduplication and cross-referencing across databases.

## [feature] Integrate Swedish SWECRIS database API
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 45
- description: |
    Swedish Research Council SWECRIS (https://swecris.se/) has API.
    Wallenberg Foundation's WACQT is a major quantum programme.

## [feature] Add ERC PI-specific download
- type: feature
- priority: 2
- labels: data-source, tier-2, api
- estimate: 30
- description: |
    CORDIS has H2020 ERC Principal Investigators file: cordis-h2020-erc-pi.xlsx
    Download and parse for PI-level data (names, institutions, panels).
    Enables ERC success rate analysis by panel (PE2 for physics).

# Tier 3: Medium Impact

## [feature] Integrate Simons Foundation grants database
- type: feature
- priority: 3
- labels: data-source, tier-3, scraper
- estimate: 60
- description: |
    Simons Foundation (https://www.simonsfoundation.org/) has awarded grants database online.
    Major quantum funder (Simons Collaboration on Ultra-Quantum Matter, It from Qubit, etc.).
    No formal API but scrapeable.

## [feature] Integrate Moore Foundation grants database
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 45
- description: |
    Gordon and Betty Moore Foundation (https://www.moore.org/) has grants database with structured data.
    Filterable, some download capability. Funds quantum science.

## [feature] Integrate NWO/NARCIS (Dutch) database
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 60
- description: |
    NWO (Dutch Research Council) + NARCIS aggregator (https://www.narcis.nl/).
    NARCIS has OAI-PMH API. Quantum Delta NL is a major programme.

## [feature] Integrate Japan KAKEN database API
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 60
- description: |
    KAKEN (https://kaken.nii.ac.jp/) has API for JSPS/JST grants.
    Japan Moonshot R&D Programme includes quantum computing.

## [feature] Add Munich Quantum Valley projects
- type: feature
- priority: 3
- labels: data-source, tier-3, manual
- estimate: 30
- description: |
    Munich Quantum Valley (https://www.munich-quantum-valley.de/) — Bavarian quantum programme.
    Project portfolio on website. Manual/scrape. Includes MCQST cluster.

## [feature] Integrate Keep.eu for Interreg quantum projects
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 45
- description: |
    Keep.eu (https://keep.eu/) has API covering all Interreg/ETC projects.
    May contain cross-border quantum initiatives.

## [feature] Add VC deal tracking (Crunchbase/Dealroom/Quantum Insider)
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 120
- description: |
    Track quantum startup funding rounds via Crunchbase (REST API, paid),
    Dealroom (API, paid), or The Quantum Insider investment tracker.
    Relevant for Innovailia UG competitive intelligence and partnership mapping.

## [feature] Integrate Helmholtz quantum programme data
- type: feature
- priority: 3
- labels: data-source, tier-3, scraper
- estimate: 45
- description: |
    Helmholtz Association quantum programmes including Helmholtz Quantum Center at FZJ.
    POF IV topics. Programme info on website, no API.

## [feature] Batch insert optimization for DuckDB
- type: task
- priority: 2
- labels: infrastructure, performance
- estimate: 60
- description: |
    Current row-by-row upserts take ~15 min for 55K records.
    Implement batch insert using DuckDB's native CSV/Parquet import
    or COPY statements. Target: < 30 seconds for full reload.

## [feature] CORDIS SPARQL integration for network mapping
- type: feature
- priority: 3
- labels: data-source, tier-3, api
- estimate: 90
- description: |
    CORDIS SPARQL endpoint at https://cordis.europa.eu/datalab/sparql-endpoint
    uses EURIO ontology. Supports federated queries with Wikidata.
    Use for PI network mapping, institutional collaboration analysis.
