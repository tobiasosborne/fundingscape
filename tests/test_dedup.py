"""Tests for cross-source deduplication."""

from datetime import date
from decimal import Decimal

from fundingscape.db import create_tables, insert_grant
from fundingscape.dedup import (
    run_dedup,
    _clean_date_anomalies,
    _normalize_country_codes,
    _enrich_cordis_from_openaire,
    _flag_openaire_ec_duplicates,
    _flag_openaire_api_duplicates,
    _flag_within_source_duplicates,
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
