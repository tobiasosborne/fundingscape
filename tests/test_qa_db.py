"""Tests for the quantum applications database."""

from fundingscape.qa_db import (
    create_tables,
    get_all_applications,
    get_summary_by_domain,
    get_summary_by_subdomain,
    insert_application,
    insert_industry_sector,
    insert_reference,
    upsert_application,
    upsert_funding_link,
)
from fundingscape.qa_models import (
    Application,
    FundingLink,
    IndustrySector,
    Reference,
)


def _factoring() -> Application:
    return Application(
        domain="Cryptography",
        subdomain="Public-Key Cryptanalysis",
        name="Integer factorisation",
        description="Factor large integers into primes",
        quantum_approaches=["Shor's algorithm", "QPE"],
        advantage_type="exponential",
        advantage_status="proven",
        classical_baseline="General number field sieve, sub-exponential",
        quantum_complexity="O(n^3) with QPE",
        maturity="small_device_demo",
        year_first_proposed=1994,
        seminal_reference="Shor, Algorithms for quantum computation (FOCS 1994)",
    )


def _grover_search() -> Application:
    return Application(
        domain="Optimisation",
        subdomain="Unstructured Search",
        name="Unstructured database search",
        description="Find a marked item in an unsorted database",
        quantum_approaches=["Grover's algorithm"],
        advantage_type="quadratic",
        advantage_status="proven",
        classical_baseline="O(N) linear scan",
        quantum_complexity="O(sqrt(N))",
        maturity="small_device_demo",
        year_first_proposed=1996,
        seminal_reference="Grover, A fast quantum mechanical algorithm (STOC 1996)",
    )


class TestInsert:
    def test_insert_application(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        assert app_id >= 1
        row = qa_db.execute(
            "SELECT name, advantage_type FROM application WHERE id = ?",
            [app_id],
        ).fetchone()
        assert row[0] == "Integer factorisation"
        assert row[1] == "exponential"

    def test_insert_two_applications(self, qa_db):
        id1 = insert_application(qa_db, _factoring())
        id2 = insert_application(qa_db, _grover_search())
        assert id1 != id2
        count = qa_db.execute("SELECT COUNT(*) FROM application").fetchone()[0]
        assert count == 2

    def test_quantum_approaches_stored_as_array(self, qa_db):
        insert_application(qa_db, _factoring())
        row = qa_db.execute(
            "SELECT quantum_approaches FROM application WHERE name = ?",
            ["Integer factorisation"],
        ).fetchone()
        assert "Shor's algorithm" in row[0]
        assert "QPE" in row[0]


class TestUpsert:
    def test_upsert_inserts_new(self, qa_db):
        app_id = upsert_application(qa_db, _factoring())
        assert app_id >= 1

    def test_upsert_updates_existing(self, qa_db):
        id1 = upsert_application(qa_db, _factoring())
        updated = _factoring()
        updated.maturity = "numerical_evidence"
        id2 = upsert_application(qa_db, updated)
        assert id1 == id2
        row = qa_db.execute(
            "SELECT maturity FROM application WHERE id = ?", [id1]
        ).fetchone()
        assert row[0] == "numerical_evidence"

    def test_upsert_does_not_duplicate(self, qa_db):
        upsert_application(qa_db, _factoring())
        upsert_application(qa_db, _factoring())
        count = qa_db.execute("SELECT COUNT(*) FROM application").fetchone()[0]
        assert count == 1


class TestReferences:
    def test_insert_reference(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        ref = Reference(
            application_id=app_id,
            title="Polynomial-time algorithms for prime factorization",
            authors="Peter W. Shor",
            year=1997,
            doi="10.1137/S0097539795293172",
            contribution_type="first_proposal",
        )
        ref_id = insert_reference(qa_db, ref)
        assert ref_id >= 1


class TestIndustrySector:
    def test_insert_sector(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        sector = IndustrySector(
            application_id=app_id,
            sector="Cybersecurity",
            relevance_notes="Breaking RSA/ECC encryption",
        )
        sid = insert_industry_sector(qa_db, sector)
        assert sid >= 1

    def test_upsert_sector_on_conflict(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        s1 = IndustrySector(application_id=app_id, sector="Cybersecurity")
        s2 = IndustrySector(
            application_id=app_id,
            sector="Cybersecurity",
            relevance_notes="Updated note",
        )
        insert_industry_sector(qa_db, s1)
        insert_industry_sector(qa_db, s2)
        count = qa_db.execute(
            "SELECT COUNT(*) FROM industry_sector WHERE application_id = ?",
            [app_id],
        ).fetchone()[0]
        assert count == 1


class TestFundingLink:
    def test_upsert_funding_link(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        link = FundingLink(
            application_id=app_id,
            query_pattern="project_title ILIKE '%factor%' OR project_title ILIKE '%RSA%'",
            grant_count=42,
            total_funding_eur=15_000_000.0,
            top_funders="EC, NSF, DFG",
        )
        fid = upsert_funding_link(qa_db, link)
        assert fid >= 1

    def test_funding_link_updates_on_recompute(self, qa_db):
        app_id = insert_application(qa_db, _factoring())
        link1 = FundingLink(
            application_id=app_id,
            query_pattern="test",
            grant_count=10,
            total_funding_eur=1_000_000.0,
        )
        link2 = FundingLink(
            application_id=app_id,
            query_pattern="test_v2",
            grant_count=20,
            total_funding_eur=2_000_000.0,
        )
        upsert_funding_link(qa_db, link1)
        upsert_funding_link(qa_db, link2)
        row = qa_db.execute(
            "SELECT grant_count, query_pattern FROM funding_link WHERE application_id = ?",
            [app_id],
        ).fetchone()
        assert row[0] == 20
        assert row[1] == "test_v2"


class TestViews:
    def _seed(self, qa_db):
        insert_application(qa_db, _factoring())
        insert_application(qa_db, _grover_search())
        insert_application(
            qa_db,
            Application(
                domain="Cryptography",
                subdomain="Public-Key Cryptanalysis",
                name="Discrete logarithm",
                description="Compute discrete logs in finite groups",
                quantum_approaches=["Shor's algorithm"],
                advantage_type="exponential",
                advantage_status="proven",
                maturity="small_device_demo",
                year_first_proposed=1994,
            ),
        )

    def test_subdomain_summary_rolls_up(self, qa_db):
        self._seed(qa_db)
        rows = get_summary_by_subdomain(qa_db)
        crypto = [r for r in rows if r["subdomain"] == "Public-Key Cryptanalysis"]
        assert len(crypto) == 1
        assert crypto[0]["application_count"] == 2
        assert crypto[0]["best_advantage_type"] == "exponential"

    def test_domain_summary_rolls_up(self, qa_db):
        self._seed(qa_db)
        rows = get_summary_by_domain(qa_db)
        domains = {r["domain"] for r in rows}
        assert "Cryptography" in domains
        assert "Optimisation" in domains

    def test_get_all_applications(self, qa_db):
        self._seed(qa_db)
        apps = get_all_applications(qa_db)
        assert len(apps) == 3
