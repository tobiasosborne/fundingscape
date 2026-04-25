"""Tests for fundingscape.currency."""

from __future__ import annotations

import pytest

from fundingscape.currency import _RATES, get_rate, to_eur


class TestGetRate:
    def test_eur_returns_one(self):
        assert get_rate("EUR", 2024) == 1.0
        assert get_rate("eur", 2024) == 1.0  # case-insensitive

    def test_usd_known_year(self):
        assert get_rate("USD", 2020) == pytest.approx(1.142)

    def test_unknown_currency_returns_none(self):
        assert get_rate("XYZ", 2020) is None

    def test_empty_currency_returns_none(self):
        assert get_rate("", 2020) is None
        assert get_rate(None, 2020) is None

    def test_year_outside_range_uses_nearest(self):
        # USD table covers 1995-2026
        # Year 1990 should fall back to 1995
        assert get_rate("USD", 1990) == _RATES["USD"][1995]
        # Year 2030 should fall back to 2026
        assert get_rate("USD", 2030) == _RATES["USD"][2026]

    def test_no_year_uses_most_recent(self):
        latest_year = max(_RATES["USD"])
        assert get_rate("USD", None) == _RATES["USD"][latest_year]

    def test_case_insensitive(self):
        assert get_rate("usd", 2020) == get_rate("USD", 2020)
        assert get_rate("Usd", 2020) == get_rate("USD", 2020)

    def test_whitespace_stripped(self):
        assert get_rate(" USD ", 2020) == get_rate("USD", 2020)


class TestToEur:
    def test_none_amount(self):
        assert to_eur(None, "USD", 2020) is None

    def test_eur_passthrough(self):
        assert to_eur(1000.0, "EUR", 2020) == 1000.0
        assert to_eur(1000, "EUR", None) == 1000.0

    def test_null_currency_treated_as_eur(self):
        # Legacy rows with NULL currency are EUR by DB default
        assert to_eur(500.0, None, 2020) == 500.0

    def test_usd_conversion(self):
        # 2020 rate: 1 EUR = 1.142 USD
        # So 1142 USD = 1000 EUR
        result = to_eur(1142.0, "USD", 2020)
        assert result == pytest.approx(1000.0, rel=1e-3)

    def test_gbp_conversion(self):
        # 2020 rate: 1 EUR = 0.890 GBP
        # So 890 GBP = 1000 EUR
        result = to_eur(890.0, "GBP", 2020)
        assert result == pytest.approx(1000.0, rel=1e-3)

    def test_aud_conversion(self):
        # 2020 rate: 1 EUR = 1.660 AUD
        result = to_eur(1660.0, "AUD", 2020)
        assert result == pytest.approx(1000.0, rel=1e-3)

    def test_inr_conversion(self):
        # The historical bug: 3.64B INR was treated as 3.64B EUR.
        # 2020 rate: 1 EUR = 84.64 INR. So 3.64B INR = ~43M EUR, not 3.64B EUR.
        result = to_eur(3_640_000_000, "INR", 2020)
        assert result == pytest.approx(43_006_852, rel=1e-3)
        assert result < 50_000_000  # sanity: this is millions, not billions

    def test_unknown_currency_returns_none(self):
        assert to_eur(1000.0, "XYZ", 2020) is None

    def test_year_fallback(self):
        # Year 1990 not in table; should still convert via nearest year
        result = to_eur(1000.0, "USD", 1990)
        assert result is not None
        assert result > 0

    def test_zero_amount(self):
        assert to_eur(0.0, "USD", 2020) == 0.0


class TestRatesTable:
    def test_all_currencies_have_rates(self):
        for cur, rates in _RATES.items():
            assert rates, f"{cur} has no rates"

    def test_rates_are_positive(self):
        for cur, rates in _RATES.items():
            for year, rate in rates.items():
                assert rate > 0, f"{cur} {year}: rate = {rate}"

    def test_major_currencies_present(self):
        # These must be in the table — they cover 99.99% of records
        for cur in ["USD", "GBP", "AUD", "CHF"]:
            assert cur in _RATES

    def test_usd_rate_plausible(self):
        # USD/EUR rate should be in [0.7, 1.7] for any modern year
        for year, rate in _RATES["USD"].items():
            assert 0.7 < rate < 1.7, f"USD {year} rate {rate} implausible"
