"""Tests for cross-source deduplication."""

from datetime import date
from decimal import Decimal

from fundingscape.db import create_tables, insert_grant
from fundingscape.dedup import (
    run_dedup,
    _clean_date_anomalies,
    _normalize_country_codes,
    _normalize_pi_country_eu,
    _normalize_currency_codes,
    _normalize_pi_names,
    _normalize_institutions,
    _link_funders,
    _enrich_cordis_from_openaire,
    _enrich_cordis_erc_pis,
    _flag_openaire_ec_duplicates,
    _flag_openaire_api_duplicates,
    _flag_within_source_duplicates,
    _flag_aggregate_records,
    _estimate_gepris_funding,
    _extract_programme_type,
)
from fundingscape.models import GrantAward


def _cordis_grant(db, project_id, total_funding=None, pi_country=None, **kw):
    """Helper: insert a CORDIS grant."""
    g = GrantAward(
        project_title=kw.get("project_title", f"CORDIS {project_id}"),
        project_id=project_id,
        source="cordis_bulk",
        source_id=f"cordis_{project_id}",
        total_funding=Decimal(str(total_funding)) if total_funding else None,
        pi_country=pi_country,
        status="active",
    )
    return insert_grant(db, g)


def _openaire_bulk_grant(db, project_id, funder="EC", total_funding=None,
                         pi_country=None, **kw):
    """Helper: insert an OpenAIRE bulk grant."""
    g = GrantAward(
        project_title=kw.get("project_title", f"OABulk {project_id}"),
        project_id=project_id,
        source="openaire_bulk",
        source_id=f"oaire_{funder}_{project_id}",
        total_funding=Decimal(str(total_funding)) if total_funding else None,
        pi_country=pi_country,
        status="active",
    )
    return insert_grant(db, g)


def _openaire_api_grant(db, project_id, funder="DFG", total_funding=None, **kw):
    """Helper: insert an OpenAIRE API grant."""
    g = GrantAward(
        project_title=kw.get("project_title", f"OAAPI {project_id}"),
        project_id=project_id,
        source="openaire",
        source_id=f"openaire_{funder}_{project_id}",
        total_funding=Decimal(str(total_funding)) if total_funding else None,
        status="active",
    )
    return insert_grant(db, g)


def _gepris_grant(db, project_id, pi_name=None, **kw):
    """Helper: insert a GEPRIS grant."""
    g = GrantAward(
        project_title=kw.get("project_title", f"GEPRIS {project_id}"),
        project_id=project_id,
        source="gepris",
        source_id=f"gepris_{project_id}",
        pi_name=pi_name,
        pi_country="DE",
        status="active",
    )
    return insert_grant(db, g)


def _foerderkatalog_grant(db, project_id, total_funding=None, **kw):
    """Helper: insert a Förderkatalog grant."""
    g = GrantAward(
        project_title=kw.get("project_title", f"FK {project_id}"),
        project_id=project_id,
        source="foerderkatalog",
        source_id=f"fk_{project_id}",
        total_funding=Decimal(str(total_funding)) if total_funding else None,
        pi_country="DE",
        status="active",
    )
    return insert_grant(db, g)


class TestCleanDateAnomalies:
    def test_nulls_sentinel_start(self, db):
        """1900-01-01 start date is set to NULL."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id, start_date, end_date)
            VALUES (nextval('seq_grant'), 'Old Grant', 'openaire_bulk', 'test_s1',
                    '1900-01-01', '1980-12-31')
        """)
        counts = _clean_date_anomalies(db)
        row = db.execute("SELECT start_date FROM grant_award WHERE source_id='test_s1'").fetchone()
        assert row[0] is None
        assert counts["sentinel_start"] >= 1

    def test_nulls_sentinel_end(self, db):
        """9999-12-31 end date is set to NULL."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id, start_date, end_date)
            VALUES (nextval('seq_grant'), 'Ongoing Grant', 'openaire_bulk', 'test_s2',
                    '2023-01-01', '9999-12-31')
        """)
        counts = _clean_date_anomalies(db)
        row = db.execute("SELECT end_date FROM grant_award WHERE source_id='test_s2'").fetchone()
        assert row[0] is None
        assert counts["sentinel_end"] >= 1

    def test_nulls_ancient_start(self, db):
        """Dates before 1950 are set to NULL."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id, start_date)
            VALUES (nextval('seq_grant'), 'Ancient', 'openaire_bulk', 'test_s3', '0009-05-28')
        """)
        counts = _clean_date_anomalies(db)
        row = db.execute("SELECT start_date FROM grant_award WHERE source_id='test_s3'").fetchone()
        assert row[0] is None

    def test_swaps_inverted_dates(self, db):
        """start > end dates are swapped."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id, start_date, end_date)
            VALUES (nextval('seq_grant'), 'Swapped', 'openaire_bulk', 'test_s4',
                    '2025-12-31', '2023-01-01')
        """)
        counts = _clean_date_anomalies(db)
        row = db.execute(
            "SELECT start_date, end_date FROM grant_award WHERE source_id='test_s4'"
        ).fetchone()
        assert str(row[0]) == "2023-01-01"
        assert str(row[1]) == "2025-12-31"
        assert counts["swapped"] >= 1

    def test_preserves_valid_dates(self, db):
        """Normal dates are not modified."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id, start_date, end_date)
            VALUES (nextval('seq_grant'), 'Normal', 'openaire_bulk', 'test_s5',
                    '2023-01-01', '2027-12-31')
        """)
        _clean_date_anomalies(db)
        row = db.execute(
            "SELECT start_date, end_date FROM grant_award WHERE source_id='test_s5'"
        ).fetchone()
        assert str(row[0]) == "2023-01-01"
        assert str(row[1]) == "2027-12-31"


class TestNormalizeCountryCodes:
    def test_uk_to_gb(self, db):
        """UK is normalized to GB."""
        _cordis_grant(db, "CC001", pi_country="UK")
        count = _normalize_country_codes(db)
        row = db.execute(
            "SELECT pi_country FROM grant_award WHERE project_id='CC001'"
        ).fetchone()
        assert row[0] == "GB"
        assert count >= 1

    def test_el_to_gr(self, db):
        """EL (Greece EU convention) is normalized to GR."""
        _cordis_grant(db, "CC002", pi_country="EL")
        _normalize_country_codes(db)
        row = db.execute(
            "SELECT pi_country FROM grant_award WHERE project_id='CC002'"
        ).fetchone()
        assert row[0] == "GR"

    def test_preserves_standard_codes(self, db):
        """Standard ISO codes are not modified."""
        _cordis_grant(db, "CC003", pi_country="DE")
        _normalize_country_codes(db)
        row = db.execute(
            "SELECT pi_country FROM grant_award WHERE project_id='CC003'"
        ).fetchone()
        assert row[0] == "DE"


class TestNormalizePiCountryEU:
    def test_eu_nulled(self, db):
        """pi_country='EU' is set to NULL."""
        _openaire_bulk_grant(db, "EU001", funder="EC", pi_country="EU")
        count = _normalize_pi_country_eu(db)
        row = db.execute(
            "SELECT pi_country FROM grant_award WHERE project_id='EU001'"
        ).fetchone()
        assert row[0] is None
        assert count >= 1

    def test_real_country_preserved(self, db):
        """Real ISO country codes are not affected."""
        _cordis_grant(db, "EU002", pi_country="DE")
        _normalize_pi_country_eu(db)
        row = db.execute(
            "SELECT pi_country FROM grant_award WHERE project_id='EU002'"
        ).fetchone()
        assert row[0] == "DE"


class TestNormalizeCurrencyCodes:
    def test_dollar_sign_to_aud(self, db):
        """'$' currency is normalized to AUD."""
        db.execute("""
            INSERT INTO grant_award (id, project_title, source, source_id,
                                     total_funding, currency)
            VALUES (nextval('seq_grant'), 'NHMRC Grant', 'openaire_bulk',
                    'oaire_NHMRC_test1', 100000, '$')
        """)
        count = _normalize_currency_codes(db)
        row = db.execute(
            "SELECT currency FROM grant_award WHERE source_id='oaire_NHMRC_test1'"
        ).fetchone()
        assert row[0] == "AUD"
        assert count >= 1

    def test_preserves_standard_codes(self, db):
        """Standard ISO currency codes are not modified."""
        _cordis_grant(db, "CUR001", total_funding=500000)
        _normalize_currency_codes(db)
        row = db.execute(
            "SELECT currency FROM grant_award WHERE project_id='CUR001'"
        ).fetchone()
        assert row[0] == "EUR"


class TestNormalizePiNames:
    def test_double_spaces_collapsed(self, db):
        """Double spaces between first and last name are collapsed."""
        _gepris_grant(db, "PI001", pi_name="Max  Mustermann")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI001'").fetchone()
        assert row[0] == "Max Mustermann"

    def test_professor_dr_stripped(self, db):
        """'Professor Dr.' title is stripped."""
        _gepris_grant(db, "PI002", pi_name="Professor Dr. Edgar  Hösch")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI002'").fetchone()
        assert row[0] == "Edgar Hösch"

    def test_professorin_dr_stripped(self, db):
        """'Professorin Dr.' title is stripped."""
        _gepris_grant(db, "PI003", pi_name="Professorin Dr. Viola  König")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI003'").fetchone()
        assert row[0] == "Viola König"

    def test_privatdozent_stripped(self, db):
        """'Privatdozent Dr.' title is stripped."""
        _gepris_grant(db, "PI004", pi_name="Privatdozent Dr. Roger  Schallreuter")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI004'").fetchone()
        assert row[0] == "Roger Schallreuter"

    def test_dr_only_stripped(self, db):
        """'Dr.' title (without Professor) is stripped."""
        _gepris_grant(db, "PI005", pi_name="Dr. Hans-Jürgen  Rüger")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI005'").fetchone()
        assert row[0] == "Hans-Jürgen Rüger"

    def test_deceased_marker_removed(self, db):
        """Deceased marker '(†)' is removed."""
        _gepris_grant(db, "PI006", pi_name="Professor Dr. Hanns  Ruder(†)")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI006'").fetchone()
        assert row[0] == "Hanns Ruder"

    def test_phd_suffix_removed(self, db):
        """', Ph.D.' suffix is removed."""
        _gepris_grant(db, "PI007", pi_name="Aaron  Greicius, Ph.D.")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI007'").fetchone()
        assert row[0] == "Aaron Greicius"

    def test_plain_name_preserved(self, db):
        """Names without titles or markers are preserved (except whitespace)."""
        _gepris_grant(db, "PI008", pi_name="Carmen Gransee")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI008'").fetchone()
        assert row[0] == "Carmen Gransee"

    def test_compound_name_preserved(self, db):
        """Compound surnames like 'von Humboldt' are preserved."""
        _gepris_grant(db, "PI009", pi_name="Professor Dr. Alexander  von Humboldt")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI009'").fetchone()
        assert row[0] == "Alexander von Humboldt"

    def test_professor_no_dr(self, db):
        """'Professor' without 'Dr.' is stripped."""
        _gepris_grant(db, "PI010", pi_name="Professor Eric von Elert, Ph.D.")
        _normalize_pi_names(db)
        row = db.execute("SELECT pi_name FROM grant_award WHERE project_id='PI010'").fetchone()
        assert row[0] == "Eric von Elert"


class TestNormalizeInstitutions:
    def test_cordis_allcaps_to_titlecase(self, db):
        """CORDIS ALL-CAPS institutions get converted to Title Case."""
        _cordis_grant(db, "INST001")
        db.execute(
            "UPDATE grant_award SET pi_institution = 'TECHNISCHE UNIVERSITAET MUENCHEN' "
            "WHERE project_id = 'INST001'"
        )
        _normalize_institutions(db)
        row = db.execute(
            "SELECT pi_institution FROM grant_award WHERE project_id='INST001'"
        ).fetchone()
        assert row[0] == "Technische Universitaet Muenchen"

    def test_gepris_shared_prefix_stripped(self, db):
        """GEPRIS 'shared X through:' prefix is stripped."""
        _gepris_grant(db, "INST002")
        db.execute(
            "UPDATE grant_award SET pi_institution = "
            "'shared FU Berlin and HU Berlin through:Charité - Universitätsmedizin Berlin' "
            "WHERE project_id = 'INST002'"
        )
        _normalize_institutions(db)
        row = db.execute(
            "SELECT pi_institution FROM grant_award WHERE project_id='INST002'"
        ).fetchone()
        assert row[0] == "Charité - Universitätsmedizin Berlin"

    def test_foerderkatalog_privacy_nulled(self, db):
        """Förderkatalog privacy placeholder is NULLed."""
        _foerderkatalog_grant(db, "INST003")
        db.execute(
            "UPDATE grant_award SET pi_institution = "
            "'Keine Anzeige aufgrund datenschutzrechtlicher Regelungen.' "
            "WHERE project_id = 'INST003'"
        )
        _normalize_institutions(db)
        row = db.execute(
            "SELECT pi_institution FROM grant_award WHERE project_id='INST003'"
        ).fetchone()
        assert row[0] is None

    def test_normal_institution_preserved(self, db):
        """Normal institution names are not modified."""
        _gepris_grant(db, "INST004")
        db.execute(
            "UPDATE grant_award SET pi_institution = 'Ludwig-Maximilians-Universität München' "
            "WHERE project_id = 'INST004'"
        )
        _normalize_institutions(db)
        row = db.execute(
            "SELECT pi_institution FROM grant_award WHERE project_id='INST004'"
        ).fetchone()
        assert row[0] == "Ludwig-Maximilians-Universität München"


class TestLinkFunders:
    def test_links_cordis_to_ec(self, db):
        """CORDIS grants get funder_id = EC."""
        from fundingscape.db import _seed_funders
        _seed_funders(db)
        _cordis_grant(db, "FL001")

        count = _link_funders(db)

        row = db.execute(
            "SELECT funder_id FROM grant_award WHERE project_id='FL001'"
        ).fetchone()
        ec_id = db.execute("SELECT id FROM funder WHERE short_name='EC'").fetchone()[0]
        assert row[0] == ec_id
        assert count >= 1

    def test_links_openaire_bulk_by_funder_code(self, db):
        """OpenAIRE bulk grants linked by funder code in source_id."""
        from fundingscape.db import _seed_funders
        _seed_funders(db)
        _openaire_bulk_grant(db, "FL002", funder="DFG")

        _link_funders(db)

        row = db.execute(
            "SELECT funder_id FROM grant_award WHERE source_id='oaire_DFG_FL002'"
        ).fetchone()
        dfg_id = db.execute("SELECT id FROM funder WHERE short_name='DFG'").fetchone()[0]
        assert row[0] == dfg_id

    def test_creates_missing_funders(self, db):
        """Funders not in seed data are auto-created."""
        from fundingscape.db import _seed_funders
        _seed_funders(db)
        _openaire_bulk_grant(db, "FL003", funder="NSF")

        _link_funders(db)

        nsf = db.execute("SELECT id, name FROM funder WHERE short_name='NSF'").fetchone()
        assert nsf is not None
        assert "National Science Foundation" in nsf[1]
        row = db.execute(
            "SELECT funder_id FROM grant_award WHERE source_id='oaire_NSF_FL003'"
        ).fetchone()
        assert row[0] == nsf[0]


class TestEnrichCordisFromOpenaire:
    def test_fills_missing_funding(self, db):
        """CORDIS record with NULL funding gets enriched from OpenAIRE."""
        _cordis_grant(db, "100001", total_funding=None)
        _openaire_bulk_grant(db, "100001", total_funding=500000)

        _enrich_cordis_from_openaire(db)

        row = db.execute(
            "SELECT total_funding FROM grant_award WHERE source='cordis_bulk' AND project_id='100001'"
        ).fetchone()
        assert row[0] == 500000.0

    def test_does_not_overwrite_existing(self, db):
        """CORDIS record with existing funding is NOT overwritten."""
        _cordis_grant(db, "100002", total_funding=1000000, pi_country="DE")
        _openaire_bulk_grant(db, "100002", total_funding=999999, pi_country="FR")

        _enrich_cordis_from_openaire(db)

        row = db.execute(
            "SELECT total_funding, pi_country FROM grant_award "
            "WHERE source='cordis_bulk' AND project_id='100002'"
        ).fetchone()
        assert row[0] == 1000000.0  # NOT overwritten
        assert row[1] == "DE"  # NOT overwritten


class TestFlagOpenAIREECDuplicates:
    def test_flags_bulk_ec_duplicate(self, db):
        """OpenAIRE bulk EC record matching CORDIS is flagged."""
        cordis_id = _cordis_grant(db, "200001")
        _openaire_bulk_grant(db, "200001", funder="EC")

        count = _flag_openaire_ec_duplicates(db)

        assert count >= 1
        row = db.execute(
            "SELECT dedup_of FROM grant_award WHERE source='openaire_bulk' AND project_id='200001'"
        ).fetchone()
        assert row[0] == cordis_id

    def test_flags_api_ec_duplicate(self, db):
        """OpenAIRE API EC record matching CORDIS is flagged."""
        cordis_id = _cordis_grant(db, "200002")
        _openaire_api_grant(db, "200002", funder="EC")

        count = _flag_openaire_ec_duplicates(db)

        assert count >= 1
        row = db.execute(
            "SELECT dedup_of FROM grant_award WHERE source='openaire' AND project_id='200002'"
        ).fetchone()
        assert row[0] == cordis_id

    def test_preserves_non_ec_records(self, db):
        """Non-EC OpenAIRE records are NOT flagged by EC dedup."""
        _cordis_grant(db, "200003")
        _openaire_bulk_grant(db, "200003", funder="DFG")

        _flag_openaire_ec_duplicates(db)

        row = db.execute(
            "SELECT dedup_of FROM grant_award WHERE source='openaire_bulk' AND project_id='200003'"
        ).fetchone()
        assert row[0] is None  # DFG record not flagged


class TestFlagOpenAIREAPIDuplicates:
    def test_flags_api_duplicate_of_bulk(self, db):
        """OpenAIRE API record matching bulk is flagged."""
        bulk_id = _openaire_bulk_grant(db, "300001", funder="DFG")
        _openaire_api_grant(db, "300001", funder="DFG")

        count = _flag_openaire_api_duplicates(db)

        assert count >= 1
        row = db.execute(
            "SELECT dedup_of FROM grant_award WHERE source='openaire' AND project_id='300001'"
        ).fetchone()
        assert row[0] == bulk_id


class TestWithinSourceDuplicates:
    def test_flags_duplicate_source_id(self, db):
        """Two rows with same source + source_id: second is flagged."""
        id1 = _openaire_bulk_grant(db, "700001", funder="EC")
        # Insert a second row with the same source_id manually
        db.execute("""
            INSERT INTO grant_award (id, project_title, project_id, source, source_id, status)
            VALUES (nextval('seq_grant'), 'Duplicate EC 700001', '700001',
                    'openaire_bulk', 'oaire_EC_700001', 'active')
        """)

        count = _flag_within_source_duplicates(db)

        assert count == 1
        # The lower id should be canonical
        rows = db.execute("""
            SELECT id, dedup_of FROM grant_award
            WHERE source_id = 'oaire_EC_700001'
            ORDER BY id
        """).fetchall()
        assert rows[0][1] is None  # first = canonical
        assert rows[1][1] == id1   # second = flagged

    def test_different_project_id_same_funder_not_flagged(self, db):
        """Different project_ids from same source are NOT duplicates."""
        _openaire_bulk_grant(db, "800001", funder="DFG")
        _openaire_bulk_grant(db, "800002", funder="DFG")

        count = _flag_within_source_duplicates(db)

        assert count == 0


class TestFlagAggregateRecords:
    def test_flags_umbrella_record(self, db):
        """Förderkatalog record above 50M is flagged as aggregate."""
        _foerderkatalog_grant(db, "AGG001", total_funding=100_000_000)
        _flag_aggregate_records(db)
        row = db.execute(
            "SELECT is_aggregate FROM grant_award WHERE project_id='AGG001'"
        ).fetchone()
        assert row[0] is True

    def test_preserves_normal_grant(self, db):
        """Normal-sized Förderkatalog grant is not flagged."""
        _foerderkatalog_grant(db, "AGG002", total_funding=500_000)
        _flag_aggregate_records(db)
        row = db.execute(
            "SELECT is_aggregate FROM grant_award WHERE project_id='AGG002'"
        ).fetchone()
        assert row[0] is False

    def test_flags_negative_funding(self, db):
        """Records with negative funding are flagged as aggregate."""
        _foerderkatalog_grant(db, "AGG003", total_funding=-100_000)
        _flag_aggregate_records(db)
        row = db.execute(
            "SELECT is_aggregate FROM grant_award WHERE project_id='AGG003'"
        ).fetchone()
        assert row[0] is True

    def test_aggregate_excluded_from_deduped_view(self, db):
        """Aggregate records are excluded from grant_award_deduped view."""
        _foerderkatalog_grant(db, "AGG004", total_funding=200_000_000)
        _foerderkatalog_grant(db, "AGG005", total_funding=1_000_000)
        _flag_aggregate_records(db)
        total = db.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
        deduped = db.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
        assert total == 2
        assert deduped == 1  # Only the normal grant


class TestExtractProgrammeType:
    def test_research_grants(self):
        abstract = "Some text hereDFG ProgrammeResearch GrantsSubject AreaAtmospheric Science"
        assert _extract_programme_type(abstract) == "Research Grants"

    def test_emmy_noether(self):
        abstract = "DescriptionDFG ProgrammeEmmy Noether Independent Junior Research GroupsTerm from 2020"
        assert _extract_programme_type(abstract) == "Emmy Noether Independent Junior Research Groups"

    def test_sfb(self):
        abstract = "TextDFG ProgrammeCollaborative Research CentresSubproject ofSFB 999"
        assert _extract_programme_type(abstract) == "Collaborative Research Centres"

    def test_no_programme(self):
        assert _extract_programme_type("No programme info here") is None

    def test_none_abstract(self):
        assert _extract_programme_type(None) is None


class TestEstimateGeprisFunding:
    def test_estimates_research_grant(self, db):
        """GEPRIS record with Research Grants programme gets estimated funding."""
        _gepris_grant(db, "EST001")
        db.execute("""
            UPDATE grant_award
            SET abstract = 'TextDFG ProgrammeResearch GrantsSubject AreaMath',
                start_date = '2020-01-01', end_date = '2022-12-31',
                total_funding = NULL
            WHERE project_id = 'EST001'
        """)
        count = _estimate_gepris_funding(db)
        assert count == 1
        row = db.execute("""
            SELECT total_funding_estimated, funding_estimate_method
            FROM grant_award WHERE project_id = 'EST001'
        """).fetchone()
        assert row[0] is not None
        assert row[0] > 0
        assert row[1] == "programme_type"

    def test_does_not_overwrite_actual_funding(self, db):
        """Records with actual funding are not estimated."""
        _gepris_grant(db, "EST002")
        db.execute("""
            UPDATE grant_award
            SET abstract = 'TextDFG ProgrammeResearch GrantsSubject AreaMath',
                start_date = '2020-01-01', end_date = '2022-12-31',
                total_funding = 500000
            WHERE project_id = 'EST002'
        """)
        count = _estimate_gepris_funding(db)
        assert count == 0

    def test_uses_default_duration_when_no_dates(self, db):
        """Records without dates use 3-year default duration."""
        _gepris_grant(db, "EST003")
        db.execute("""
            UPDATE grant_award
            SET abstract = 'TextDFG ProgrammeResearch GrantsSubject AreaMath',
                start_date = NULL, end_date = NULL,
                total_funding = NULL
            WHERE project_id = 'EST003'
        """)
        _estimate_gepris_funding(db)
        row = db.execute(
            "SELECT total_funding_estimated FROM grant_award WHERE project_id = 'EST003'"
        ).fetchone()
        # 80,000/yr * 3 years = 240,000
        assert row[0] == 240_000.0


class TestRunDedup:
    def test_idempotent(self, db):
        """Running dedup twice produces the same result."""
        _cordis_grant(db, "400001", total_funding=None)
        _openaire_bulk_grant(db, "400001", funder="EC", total_funding=750000)

        stats1 = run_dedup(db)
        stats2 = run_dedup(db)

        assert stats1["ec_duplicates_flagged"] == stats2["ec_duplicates_flagged"]
        flagged = db.execute(
            "SELECT COUNT(*) FROM grant_award WHERE dedup_of IS NOT NULL"
        ).fetchone()[0]
        assert flagged == 1

    def test_view_excludes_duplicates(self, db):
        """grant_award_deduped view excludes flagged records."""
        _cordis_grant(db, "500001")
        _openaire_bulk_grant(db, "500001", funder="EC")

        run_dedup(db)

        total = db.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
        deduped = db.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
        assert total == 2
        assert deduped == 1  # Only CORDIS canonical record

    def test_full_scenario(self, db):
        """End-to-end: CORDIS + OpenAIRE bulk EC + OpenAIRE API DFG + bulk DFG."""
        # CORDIS grant (canonical for EC)
        _cordis_grant(db, "600001", total_funding=None)
        # OpenAIRE bulk EC duplicate of CORDIS
        _openaire_bulk_grant(db, "600001", funder="EC", total_funding=1200000)
        # OpenAIRE bulk DFG grant (unique, canonical)
        _openaire_bulk_grant(db, "600002", funder="DFG", total_funding=300000)
        # OpenAIRE API DFG duplicate of bulk
        _openaire_api_grant(db, "600002", funder="DFG", total_funding=300000)
        # OpenAIRE bulk NSF grant (unique, no duplicate)
        _openaire_bulk_grant(db, "600003", funder="NSF", total_funding=200000)

        stats = run_dedup(db)

        assert stats["ec_duplicates_flagged"] == 1
        assert stats["api_duplicates_flagged"] == 1

        total = db.execute("SELECT COUNT(*) FROM grant_award").fetchone()[0]
        deduped = db.execute("SELECT COUNT(*) FROM grant_award_deduped").fetchone()[0]
        assert total == 5
        assert deduped == 3  # CORDIS + DFG bulk + NSF bulk

        # Check CORDIS was enriched with funding from OpenAIRE
        cordis_funding = db.execute(
            "SELECT total_funding FROM grant_award WHERE source='cordis_bulk' AND project_id='600001'"
        ).fetchone()[0]
        assert cordis_funding == 1200000.0
