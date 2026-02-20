"""Tests for the database layer."""

from datetime import date
from decimal import Decimal

from fundingscape.db import (
    create_tables,
    insert_call,
    insert_funder,
    insert_grant,
    upsert_call,
    upsert_grant,
    update_data_source,
    _seed_funders,
    _seed_profiles,
)
from fundingscape.models import Call, Funder, FundingInstrument, GrantAward


class TestSchema:
    def test_tables_created(self, db):
        tables = db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "funder" in table_names
        assert "funding_instrument" in table_names
        assert "call" in table_names
        assert "grant_award" in table_names
        assert "eligibility_profile" in table_names
        assert "data_source" in table_names
        assert "change_log" in table_names

    def test_create_tables_idempotent(self, db):
        create_tables(db)
        create_tables(db)
        tables = db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        assert len(tables) >= 7


class TestFunderCrud:
    def test_insert_funder(self, db):
        f = Funder(name="Test Funder", type="eu", country="EU")
        fid = insert_funder(db, f)
        assert fid >= 1

        row = db.execute("SELECT name, type FROM funder WHERE id = ?", [fid]).fetchone()
        assert row[0] == "Test Funder"
        assert row[1] == "eu"

    def test_seed_funders(self, db):
        _seed_funders(db)
        count = db.execute("SELECT COUNT(*) FROM funder").fetchone()[0]
        assert count >= 10

    def test_seed_idempotent(self, db):
        _seed_funders(db)
        _seed_funders(db)
        count = db.execute("SELECT COUNT(*) FROM funder").fetchone()[0]
        assert count >= 10


class TestCallCrud:
    def test_insert_call(self, db):
        c = Call(
            title="Quantum Call 2025",
            status="open",
            deadline=date(2025, 9, 15),
            source="ft_portal",
            source_id="QC2025",
            budget_total=Decimal("50000000"),
            topic_keywords=["quantum", "computing"],
        )
        cid = insert_call(db, c)
        assert cid >= 1

        row = db.execute("SELECT title, status, budget_total FROM call WHERE id = ?", [cid]).fetchone()
        assert row[0] == "Quantum Call 2025"
        assert row[1] == "open"
        assert row[2] == 50000000.0

    def test_upsert_call_insert(self, db):
        c = Call(title="New Call", status="open", source="test", source_id="new1")
        cid = upsert_call(db, c)
        assert cid >= 1

    def test_upsert_call_update(self, db):
        c1 = Call(title="Original", status="open", source="test", source_id="up1")
        cid1 = upsert_call(db, c1)

        c2 = Call(title="Updated", status="closed", source="test", source_id="up1")
        cid2 = upsert_call(db, c2)

        assert cid1 == cid2
        row = db.execute("SELECT title, status FROM call WHERE id = ?", [cid1]).fetchone()
        assert row[0] == "Updated"
        assert row[1] == "closed"


class TestGrantCrud:
    def test_insert_grant(self, db):
        g = GrantAward(
            project_title="Quantum Project",
            source="cordis",
            source_id="12345",
            pi_name="Jane Doe",
            total_funding=Decimal("1500000"),
            start_date=date(2023, 1, 1),
            end_date=date(2027, 12, 31),
            status="active",
        )
        gid = insert_grant(db, g)
        assert gid >= 1

        row = db.execute(
            "SELECT project_title, pi_name, total_funding FROM grant_award WHERE id = ?",
            [gid],
        ).fetchone()
        assert row[0] == "Quantum Project"
        assert row[1] == "Jane Doe"
        assert row[2] == 1500000.0

    def test_upsert_grant_insert(self, db):
        g = GrantAward(project_title="New Grant", source="test", source_id="g1")
        gid = upsert_grant(db, g)
        assert gid >= 1

    def test_upsert_grant_update(self, db):
        g1 = GrantAward(
            project_title="Original Grant",
            source="test",
            source_id="gu1",
            status="active",
        )
        gid1 = upsert_grant(db, g1)

        g2 = GrantAward(
            project_title="Updated Grant",
            source="test",
            source_id="gu1",
            status="completed",
        )
        gid2 = upsert_grant(db, g2)

        assert gid1 == gid2
        row = db.execute(
            "SELECT project_title, status FROM grant_award WHERE id = ?", [gid1]
        ).fetchone()
        assert row[0] == "Updated Grant"
        assert row[1] == "completed"


class TestDataSource:
    def test_update_data_source(self, db):
        update_data_source(db, "test_src", "Test Source", records=42, status="ok")
        row = db.execute("SELECT * FROM data_source WHERE id = 'test_src'").fetchone()
        assert row is not None
        assert row[1] == "Test Source"

    def test_update_data_source_upsert(self, db):
        update_data_source(db, "src1", "Source 1", records=10, status="ok")
        update_data_source(db, "src1", "Source 1", records=20, status="ok")
        count = db.execute("SELECT COUNT(*) FROM data_source WHERE id = 'src1'").fetchone()[0]
        assert count == 1
        records = db.execute("SELECT records_fetched FROM data_source WHERE id = 'src1'").fetchone()[0]
        assert records == 20


class TestProfiles:
    def test_seed_profiles(self, db):
        _seed_profiles(db)
        count = db.execute("SELECT COUNT(*) FROM eligibility_profile").fetchone()[0]
        assert count == 2

        pi = db.execute(
            "SELECT profile_name, institution FROM eligibility_profile WHERE id = 1"
        ).fetchone()
        assert pi[0] == "PI - Quantum Research Group"
        assert "Hannover" in pi[1]
