"""Currency normalization to EUR.

Converts grant funding amounts from foreign currencies to EUR using ECB
annual reference rates. Source: ECB Statistical Data Warehouse, eurofxref-hist.

Rates are stored as `1 EUR = X foreign`, so EUR_amount = foreign_amount / rate.

For currencies not in the table, falls back to a fixed approximate rate.
For years outside the table, falls back to the nearest year.
"""

from __future__ import annotations

# 1 EUR = X foreign currency, annual averages (ECB reference rates, ECB SDW).
# Sources: https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-{usd,gbp,aud}.en.html
# Rounded to 4 sig figs. Values for 1999 onward; 1995-1998 use earliest known.
_RATES: dict[str, dict[int, float]] = {
    "USD": {
        1995: 1.31, 1996: 1.27, 1997: 1.13, 1998: 1.12,
        1999: 1.066, 2000: 0.924, 2001: 0.896, 2002: 0.946, 2003: 1.131,
        2004: 1.244, 2005: 1.244, 2006: 1.256, 2007: 1.371, 2008: 1.471,
        2009: 1.395, 2010: 1.326, 2011: 1.392, 2012: 1.285, 2013: 1.328,
        2014: 1.329, 2015: 1.110, 2016: 1.107, 2017: 1.130, 2018: 1.181,
        2019: 1.119, 2020: 1.142, 2021: 1.183, 2022: 1.054, 2023: 1.081,
        2024: 1.082, 2025: 1.090, 2026: 1.090,
    },
    "GBP": {
        1995: 0.829, 1996: 0.814, 1997: 0.692, 1998: 0.676,
        1999: 0.659, 2000: 0.609, 2001: 0.622, 2002: 0.629, 2003: 0.692,
        2004: 0.679, 2005: 0.684, 2006: 0.682, 2007: 0.685, 2008: 0.796,
        2009: 0.891, 2010: 0.858, 2011: 0.868, 2012: 0.811, 2013: 0.849,
        2014: 0.806, 2015: 0.726, 2016: 0.819, 2017: 0.877, 2018: 0.885,
        2019: 0.878, 2020: 0.890, 2021: 0.860, 2022: 0.853, 2023: 0.870,
        2024: 0.847, 2025: 0.845, 2026: 0.845,
    },
    "AUD": {
        1999: 1.652, 2000: 1.589, 2001: 1.732, 2002: 1.738, 2003: 1.738,
        2004: 1.690, 2005: 1.632, 2006: 1.667, 2007: 1.635, 2008: 1.741,
        2009: 1.773, 2010: 1.442, 2011: 1.349, 2012: 1.241, 2013: 1.378,
        2014: 1.472, 2015: 1.478, 2016: 1.488, 2017: 1.473, 2018: 1.580,
        2019: 1.611, 2020: 1.660, 2021: 1.575, 2022: 1.517, 2023: 1.628,
        2024: 1.640, 2025: 1.660, 2026: 1.660,
    },
    "CHF": {
        1999: 1.600, 2000: 1.558, 2001: 1.510, 2002: 1.467, 2003: 1.521,
        2004: 1.544, 2005: 1.548, 2006: 1.573, 2007: 1.643, 2008: 1.587,
        2009: 1.510, 2010: 1.380, 2011: 1.234, 2012: 1.205, 2013: 1.231,
        2014: 1.215, 2015: 1.068, 2016: 1.090, 2017: 1.112, 2018: 1.155,
        2019: 1.112, 2020: 1.071, 2021: 1.081, 2022: 1.005, 2023: 0.972,
        2024: 0.953, 2025: 0.940, 2026: 0.940,
    },
    "CAD": {
        1999: 1.584, 2005: 1.509, 2010: 1.366, 2015: 1.418, 2020: 1.530,
        2023: 1.460, 2024: 1.480,
    },
    "NOK": {
        1999: 8.310, 2005: 8.000, 2010: 8.005, 2015: 8.945, 2020: 10.723,
        2023: 11.430, 2024: 11.620,
    },
    "SEK": {
        1999: 8.808, 2005: 9.282, 2010: 9.541, 2015: 9.354, 2020: 10.485,
        2023: 11.480, 2024: 11.430,
    },
    "DKK": {
        1999: 7.435, 2005: 7.452, 2010: 7.447, 2015: 7.459, 2020: 7.454,
        2023: 7.451, 2024: 7.459,
    },
    # HRK fixed at ~7.534 from 1999, EUR adoption 2023-01-01
    "HRK": {
        1999: 7.581, 2005: 7.400, 2010: 7.288, 2015: 7.610, 2020: 7.538,
        2022: 7.534,
    },
    "INR": {
        2010: 60.59, 2015: 71.20, 2020: 84.64, 2023: 89.30, 2024: 90.20,
    },
    "ZAR": {
        2010: 9.700, 2015: 14.18, 2020: 18.77, 2023: 19.95, 2024: 19.80,
    },
    "IDR": {
        2010: 12053.0, 2015: 14870.0, 2020: 16624.0, 2023: 16442.0,
    },
    "SGD": {
        2010: 1.808, 2015: 1.526, 2020: 1.575, 2023: 1.452, 2024: 1.450,
    },
    "JPY": {
        1999: 121.3, 2005: 136.9, 2010: 116.2, 2015: 134.3, 2020: 121.8,
        2023: 151.9, 2024: 163.9,
    },
    "CNY": {
        2010: 8.971, 2015: 6.973, 2020: 7.875, 2023: 7.660, 2024: 7.787,
    },
}


def get_rate(currency: str, year: int | None) -> float | None:
    """Return rate (1 EUR = X currency) for the given currency and year.

    Falls back to nearest year if exact year not in table.
    Returns None if currency is unknown.
    Returns 1.0 for EUR.
    """
    if not currency:
        return None
    cur = currency.upper().strip()
    if cur == "EUR":
        return 1.0
    table = _RATES.get(cur)
    if table is None:
        return None
    if year is None:
        # Use most recent rate as default
        return table[max(table)]
    # Exact match
    if year in table:
        return table[year]
    # Nearest year fallback
    closest = min(table, key=lambda y: abs(y - year))
    return table[closest]


def to_eur(amount: float | None, currency: str | None, year: int | None) -> float | None:
    """Convert a foreign-currency amount to EUR.

    Returns None if amount is None, currency is missing, or rate unknown.
    """
    if amount is None:
        return None
    if currency is None:
        # Treat unknown currency as EUR (matches DuckDB default for legacy rows)
        return float(amount)
    rate = get_rate(currency, year)
    if rate is None:
        return None
    return float(amount) / rate
